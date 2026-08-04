package main

import (
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	dashboardExportJobTTL       = 15 * time.Minute
	dashboardExportJobMaxActive = 2
	dashboardExportJobMaxStored = 16
	dashboardExportJobPageSize  = 5000
)

const (
	dashboardExportJobQueued    = "queued"
	dashboardExportJobRunning   = "running"
	dashboardExportJobSucceeded = "succeeded"
	dashboardExportJobFailed    = "failed"
)

var dashboardExportJobs = newDashboardExportJobManager()

type dashboardExportJobManager struct {
	mu     sync.Mutex
	jobs   map[string]*dashboardExportJob
	closed bool
}

type dashboardExportJob struct {
	ID          string
	Status      string
	Params      EventsQuery
	Options     dashboardEventsExportOptions
	CreatedAt   time.Time
	SnapshotAt  time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	ExpiresAt   time.Time
	Error       string
	Total       int
	Exported    int
	Truncated   bool
	RawBytes    int
	BodyBytes   int
	ContentType string
	ETag        string
	FilePath    string
}

type dashboardExportFileResult struct {
	Total       int
	Exported    int
	Truncated   bool
	RawBytes    int
	BodyBytes   int
	ContentType string
	GeneratedAt string
}

type countingWriter struct {
	w io.Writer
	n int
}

type dashboardExportJobResponse struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	Format       string `json:"format"`
	Gzip         bool   `json:"gzip"`
	Limit        int    `json:"limit,omitempty"`
	CreatedAt    string `json:"created_at"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	Error        string `json:"error,omitempty"`
	Total        int    `json:"total,omitempty"`
	Exported     int    `json:"exported,omitempty"`
	Truncated    bool   `json:"truncated,omitempty"`
	RawBytes     int    `json:"raw_bytes,omitempty"`
	BodyBytes    int    `json:"body_bytes,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	DownloadPath string `json:"download_path,omitempty"`
}

type dashboardExportJobListResponse struct {
	Jobs []dashboardExportJobResponse `json:"jobs"`
}

type dashboardExportJobErrorResponse struct {
	Error string `json:"error"`
}

func newDashboardExportJobManager() *dashboardExportJobManager {
	return &dashboardExportJobManager{jobs: make(map[string]*dashboardExportJob)}
}

func handleDashboardEventsExportJobCreate(query map[string][]string) ([]byte, error) {
	params := normalizeEventsQuery(dashboardEventsQuery(query), false)
	opts := dashboardEventsExportOptionsFromQuery(query)
	opts.Limit = effectiveDashboardExportLimit(opts.Limit, stats.ExportMaxRecords())
	job, statusCode, message := dashboardExportJobs.create(params, opts)
	if message != "" {
		return dashboardExportJobJSON(statusCode, dashboardExportJobErrorResponse{Error: message})
	}
	return dashboardExportJobJSON(statusCode, dashboardExportJobSnapshot(job))
}

func handleDashboardEventsExportJobStatus(query map[string][]string) ([]byte, error) {
	id := dashboardExportJobID(query)
	if id == "" {
		return dashboardExportJobJSON(http.StatusOK, dashboardExportJobListResponse{Jobs: dashboardExportJobs.list()})
	}
	job, ok := dashboardExportJobs.get(id)
	if !ok {
		return dashboardExportJobJSON(http.StatusNotFound, dashboardExportJobErrorResponse{Error: "export job not found"})
	}
	return dashboardExportJobJSON(http.StatusOK, dashboardExportJobSnapshot(job))
}

func handleDashboardEventsExportJobDelete(query map[string][]string) ([]byte, error) {
	id := dashboardExportJobID(query)
	if id == "" {
		return dashboardExportJobJSON(http.StatusBadRequest, dashboardExportJobErrorResponse{Error: "missing export job id"})
	}
	if !dashboardExportJobs.delete(id) {
		return dashboardExportJobJSON(http.StatusNotFound, dashboardExportJobErrorResponse{Error: "export job not found"})
	}
	return dashboardExportJobJSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func handleDashboardEventsExportDownload(query map[string][]string) ([]byte, error) {
	id := dashboardExportJobID(query)
	if id == "" {
		return dashboardExportJobJSON(http.StatusBadRequest, dashboardExportJobErrorResponse{Error: "missing export job id"})
	}
	job, ok := dashboardExportJobs.get(id)
	if !ok {
		return dashboardExportJobJSON(http.StatusNotFound, dashboardExportJobErrorResponse{Error: "export job not found"})
	}
	switch job.Status {
	case dashboardExportJobSucceeded:
	case dashboardExportJobFailed:
		return dashboardExportJobJSON(http.StatusInternalServerError, dashboardExportJobSnapshot(job))
	default:
		return dashboardExportJobJSON(http.StatusAccepted, dashboardExportJobSnapshot(job))
	}

	chunked := queryValue(query, "offset") != "" || queryValue(query, "chunk_size") != ""
	if !chunked {
		body, err := os.ReadFile(job.FilePath)
		if err != nil {
			return dashboardExportJobJSON(http.StatusGone, dashboardExportJobErrorResponse{Error: "export job file is no longer available"})
		}
		resp := ManagementResponse{
			StatusCode: http.StatusOK,
			Headers:    dashboardExportJobDownloadHeaders(job),
			Body:       body,
		}
		return okEnvelopeJSON(string(mustMarshal(resp)))
	}

	offsetValue, err := dashboardExportDownloadInt(query, "offset", 0)
	if err != nil {
		return dashboardExportJobJSON(http.StatusBadRequest, dashboardExportJobErrorResponse{Error: "invalid export offset"})
	}
	offset := int64(offsetValue)
	chunkSize, err := dashboardExportDownloadInt(query, "chunk_size", dashboardExportDownloadChunkSize)
	if err != nil || chunkSize <= 0 {
		return dashboardExportJobJSON(http.StatusBadRequest, dashboardExportJobErrorResponse{Error: "invalid export chunk size"})
	}
	if chunkSize > dashboardExportDownloadMaxChunkSize {
		chunkSize = dashboardExportDownloadMaxChunkSize
	}
	if !job.Options.Gzip && chunkSize < utf8.UTFMax {
		chunkSize = utf8.UTFMax
	}
	file, err := os.Open(job.FilePath)
	if err != nil {
		return dashboardExportJobJSON(http.StatusGone, dashboardExportJobErrorResponse{Error: "export job file is no longer available"})
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return dashboardExportJobJSON(http.StatusGone, dashboardExportJobErrorResponse{Error: "export job file is no longer available"})
	}
	totalBytes := info.Size()
	if offset < 0 || offset >= totalBytes {
		if offset == totalBytes {
			return okEnvelopeJSON(string(mustMarshal(ManagementResponse{
				StatusCode: http.StatusOK,
				Headers:    dashboardExportJobChunkHeaders(job, offset, 0, totalBytes),
				Body:       []byte{},
			})))
		}
		return dashboardExportJobJSON(http.StatusRequestedRangeNotSatisfiable, dashboardExportJobErrorResponse{Error: "export offset is outside the file"})
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return dashboardExportJobJSON(http.StatusGone, dashboardExportJobErrorResponse{Error: "export job file is no longer available"})
	}
	body := make([]byte, chunkSize)
	read, readErr := io.ReadFull(file, body)
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		return dashboardExportJobJSON(http.StatusGone, dashboardExportJobErrorResponse{Error: "read export job file failed"})
	}
	body = body[:read]
	// JSON and CSV exports are UTF-8 text. Keep chunk boundaries valid so the
	// management envelope can decode each response independently. gzip files
	// are opaque binary data and must retain every byte.
	if !job.Options.Gzip {
		for len(body) > 0 && !utf8.Valid(body) {
			body = body[:len(body)-1]
		}
	}
	resp := ManagementResponse{
		StatusCode: http.StatusPartialContent,
		Headers:    dashboardExportJobChunkHeaders(job, offset, int64(len(body)), totalBytes),
		Body:       body,
	}
	return okEnvelopeJSON(string(mustMarshal(resp)))
}

const (
	dashboardExportDownloadChunkSize    = 1 << 20
	dashboardExportDownloadMaxChunkSize = 4 << 20
)

func dashboardExportDownloadInt(query map[string][]string, key string, fallback int) (int, error) {
	value := strings.TrimSpace(queryValue(query, key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, err
	}
	return parsed, nil
}

func dashboardExportJobChunkHeaders(job dashboardExportJob, offset, chunkBytes, totalBytes int64) map[string][]string {
	headers := dashboardExportJobDownloadHeaders(job)
	headers["Content-Length"] = []string{strconv.FormatInt(chunkBytes, 10)}
	if chunkBytes > 0 {
		headers["Content-Range"] = []string{fmt.Sprintf("bytes %d-%d/%d", offset, offset+chunkBytes-1, totalBytes)}
	} else {
		headers["Content-Range"] = []string{fmt.Sprintf("bytes */%d", totalBytes)}
	}
	headers["Accept-Ranges"] = []string{"bytes"}
	headers["X-Export-Chunk-Offset"] = []string{strconv.FormatInt(offset, 10)}
	headers["X-Export-Chunk-Bytes"] = []string{strconv.FormatInt(chunkBytes, 10)}
	headers["X-Export-Total-Bytes"] = []string{strconv.FormatInt(totalBytes, 10)}
	return headers
}

func (m *dashboardExportJobManager) create(params EventsQuery, opts dashboardEventsExportOptions) (dashboardExportJob, int, string) {
	now := time.Now()
	m.mu.Lock()
	m.cleanupLocked(now)
	if m.closed {
		m.mu.Unlock()
		return dashboardExportJob{}, http.StatusServiceUnavailable, "export jobs are shutting down"
	}
	if m.activeLocked() >= dashboardExportJobMaxActive {
		m.mu.Unlock()
		return dashboardExportJob{}, http.StatusTooManyRequests, "too many active export jobs"
	}
	if len(m.jobs) >= dashboardExportJobMaxStored {
		m.mu.Unlock()
		return dashboardExportJob{}, http.StatusTooManyRequests, "too many retained export jobs"
	}

	id := newDashboardExportJobID()
	filePath := filepath.Join(os.TempDir(), "cpa-usage-events-export-"+id)
	job := &dashboardExportJob{
		ID:         id,
		Status:     dashboardExportJobQueued,
		Params:     params,
		Options:    opts,
		CreatedAt:  now,
		SnapshotAt: now,
		ExpiresAt:  now.Add(dashboardExportJobTTL),
		FilePath:   filePath,
	}
	m.jobs[id] = job
	snapshot := *job
	m.mu.Unlock()

	go m.run(id, params, opts, filePath, now)
	return snapshot, http.StatusAccepted, ""
}

func (m *dashboardExportJobManager) run(id string, params EventsQuery, opts dashboardEventsExportOptions, filePath string, snapshotAt time.Time) {
	startedAt := time.Now()
	if snapshotAt.IsZero() {
		snapshotAt = startedAt
	}
	m.update(id, func(job *dashboardExportJob) {
		job.Status = dashboardExportJobRunning
		job.StartedAt = startedAt
	})

	if m.isClosed() {
		_ = os.Remove(filePath + ".tmp")
		_ = os.Remove(filePath)
		return
	}
	tmpPath := filePath + ".tmp"
	encoded, err := encodeDashboardEventsExportFile(params, opts, tmpPath, snapshotAt)
	if err != nil {
		_ = os.Remove(tmpPath)
		m.fail(id, err)
		return
	}
	if m.isClosed() {
		_ = os.Remove(tmpPath)
		_ = os.Remove(filePath)
		return
	}

	sum, err := fileSHA256(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		m.fail(id, err)
		return
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		m.fail(id, err)
		return
	}
	finishedAt := time.Now()
	stats.RecordEventsExportSummary(string(opts.Format), opts.Gzip, encoded.Total, encoded.Exported, encoded.Truncated, encoded.RawBytes, encoded.BodyBytes, finishedAt.Sub(startedAt))
	m.update(id, func(job *dashboardExportJob) {
		job.Status = dashboardExportJobSucceeded
		job.FinishedAt = finishedAt
		job.ExpiresAt = finishedAt.Add(dashboardExportJobTTL)
		job.Total = encoded.Total
		job.Exported = encoded.Exported
		job.Truncated = encoded.Truncated
		job.RawBytes = encoded.RawBytes
		job.BodyBytes = encoded.BodyBytes
		job.ContentType = encoded.ContentType
		job.ETag = `W/"events-export-job-` + hex.EncodeToString(sum[:]) + `"`
	})
}

func encodeDashboardEventsExportFile(params EventsQuery, opts dashboardEventsExportOptions, filePath string, snapshotAt time.Time) (dashboardExportFileResult, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return dashboardExportFileResult{}, err
	}
	bodyCounter := &countingWriter{w: file}
	rawWriter := io.Writer(bodyCounter)
	var gzipWriter *gzip.Writer
	if opts.Gzip {
		gzipWriter = gzip.NewWriter(bodyCounter)
		rawWriter = gzipWriter
	}
	rawCounter := &countingWriter{w: rawWriter}

	result, encodeErr := encodeDashboardEventsExportPaged(rawCounter, params, opts, snapshotAt)
	if gzipWriter != nil {
		if closeErr := gzipWriter.Close(); encodeErr == nil {
			encodeErr = closeErr
		}
	}
	if closeErr := file.Close(); encodeErr == nil {
		encodeErr = closeErr
	}
	if encodeErr != nil {
		return dashboardExportFileResult{}, encodeErr
	}
	result.RawBytes = rawCounter.n
	result.BodyBytes = bodyCounter.n
	return result, nil
}

func encodeDashboardEventsExportPaged(writer io.Writer, params EventsQuery, opts dashboardEventsExportOptions, snapshotAt time.Time) (dashboardExportFileResult, error) {
	contentType := dashboardExportContentType(opts.Format)
	firstPage := stats.QueryExportEventsPage(params, 0, dashboardExportJobPageSize, opts.Limit, snapshotAt)
	localizeEventsResultForResource(&firstPage)
	result := dashboardExportFileResult{
		Total:       firstPage.Total,
		Truncated:   firstPage.Truncated,
		ContentType: contentType,
		GeneratedAt: firstPage.GeneratedAt,
	}
	switch opts.Format {
	case dashboardExportJSONL:
		exported, err := encodeDashboardEventsJSONLPaged(writer, params, opts, snapshotAt, firstPage)
		result.Exported = exported
		return result, err
	case dashboardExportCSV:
		exported, err := encodeDashboardEventsCSVPaged(writer, params, opts, snapshotAt, firstPage)
		result.Exported = exported
		return result, err
	default:
		exported, err := encodeDashboardEventsJSONPaged(writer, params, opts, snapshotAt, firstPage)
		result.Exported = exported
		return result, err
	}
}

func encodeDashboardEventsJSONPaged(writer io.Writer, params EventsQuery, opts dashboardEventsExportOptions, snapshotAt time.Time, firstPage EventsResult) (int, error) {
	if _, err := io.WriteString(writer, `{"events":[`); err != nil {
		return 0, err
	}
	first := true
	exported, err := encodeDashboardEventsPaged(params, opts, snapshotAt, firstPage, func(event RequestDetail) error {
		if !first {
			if _, err := io.WriteString(writer, ","); err != nil {
				return err
			}
		}
		first = false
		raw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		_, err = writer.Write(raw)
		return err
	})
	if err != nil {
		return exported, err
	}
	generatedAt, err := json.Marshal(firstPage.GeneratedAt)
	if err != nil {
		return exported, err
	}
	if _, err := fmt.Fprintf(writer, `],"total":%d,"limit":%d,"offset":0`, firstPage.Total, firstPage.Limit); err != nil {
		return exported, err
	}
	if firstPage.Truncated {
		if _, err := io.WriteString(writer, `,"truncated":true`); err != nil {
			return exported, err
		}
	}
	if _, err := fmt.Fprintf(writer, `,"generated_at":%s}`, generatedAt); err != nil {
		return exported, err
	}
	return exported, nil
}

func encodeDashboardEventsJSONLPaged(writer io.Writer, params EventsQuery, opts dashboardEventsExportOptions, snapshotAt time.Time, firstPage EventsResult) (int, error) {
	encoder := json.NewEncoder(writer)
	return encodeDashboardEventsPaged(params, opts, snapshotAt, firstPage, func(event RequestDetail) error {
		return encoder.Encode(event)
	})
}

func encodeDashboardEventsCSVPaged(writer io.Writer, params EventsQuery, opts dashboardEventsExportOptions, snapshotAt time.Time, firstPage EventsResult) (int, error) {
	csvWriter := csv.NewWriter(writer)
	if err := csvWriter.Write(dashboardEventsCSVHeader()); err != nil {
		return 0, err
	}
	exported, err := encodeDashboardEventsPaged(params, opts, snapshotAt, firstPage, func(event RequestDetail) error {
		return csvWriter.Write(dashboardEventCSVRecord(event))
	})
	csvWriter.Flush()
	if err == nil {
		err = csvWriter.Error()
	}
	return exported, err
}

func encodeDashboardEventsPaged(params EventsQuery, opts dashboardEventsExportOptions, snapshotAt time.Time, firstPage EventsResult, consume func(RequestDetail) error) (int, error) {
	page := firstPage
	exported := 0
	for {
		for _, event := range page.Events {
			event = sanitizeRequestDetailAPIKeyForOutput(event)
			if err := consume(event); err != nil {
				return exported, err
			}
			exported++
		}
		if exported >= page.Limit || len(page.Events) == 0 {
			return exported, nil
		}
		page = stats.QueryExportEventsPage(params, exported, dashboardExportJobPageSize, opts.Limit, snapshotAt)
		localizeEventsResultForResource(&page)
	}
}

func (m *dashboardExportJobManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for id, job := range m.jobs {
		delete(m.jobs, id)
		_ = os.Remove(job.FilePath + ".tmp")
		_ = os.Remove(job.FilePath)
	}
}

func (m *dashboardExportJobManager) fail(id string, err error) {
	now := time.Now()
	m.update(id, func(job *dashboardExportJob) {
		job.Status = dashboardExportJobFailed
		job.FinishedAt = now
		job.ExpiresAt = now.Add(dashboardExportJobTTL)
		job.Error = err.Error()
		_ = os.Remove(job.FilePath + ".tmp")
		_ = os.Remove(job.FilePath)
	})
}

func (m *dashboardExportJobManager) get(id string) (dashboardExportJob, bool) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	job, ok := m.jobs[id]
	if !ok {
		return dashboardExportJob{}, false
	}
	return *job, true
}

func (m *dashboardExportJobManager) list() []dashboardExportJobResponse {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	jobs := make([]dashboardExportJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, *job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	responses := make([]dashboardExportJobResponse, 0, len(jobs))
	for _, job := range jobs {
		responses = append(responses, dashboardExportJobSnapshot(job))
	}
	return responses
}

func (m *dashboardExportJobManager) delete(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return false
	}
	delete(m.jobs, id)
	_ = os.Remove(job.FilePath + ".tmp")
	_ = os.Remove(job.FilePath)
	return true
}

func (m *dashboardExportJobManager) update(id string, update func(*dashboardExportJob)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	if job, ok := m.jobs[id]; ok {
		update(job)
	}
}

func (m *dashboardExportJobManager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *dashboardExportJobManager) activeLocked() int {
	active := 0
	for _, job := range m.jobs {
		if job.Status == dashboardExportJobQueued || job.Status == dashboardExportJobRunning {
			active++
		}
	}
	return active
}

func (m *dashboardExportJobManager) cleanupLocked(now time.Time) {
	for id, job := range m.jobs {
		if job.Status == dashboardExportJobQueued || job.Status == dashboardExportJobRunning {
			continue
		}
		expiresAt := job.ExpiresAt
		if expiresAt.IsZero() {
			expiresAt = job.CreatedAt.Add(dashboardExportJobTTL)
		}
		if now.Before(expiresAt) {
			continue
		}
		delete(m.jobs, id)
		_ = os.Remove(job.FilePath + ".tmp")
		_ = os.Remove(job.FilePath)
	}
}

func dashboardExportJobSnapshot(job dashboardExportJob) dashboardExportJobResponse {
	response := dashboardExportJobResponse{
		ID:          job.ID,
		Status:      job.Status,
		Format:      string(job.Options.Format),
		Gzip:        job.Options.Gzip,
		Limit:       job.Options.Limit,
		CreatedAt:   formatExportJobTime(job.CreatedAt),
		StartedAt:   formatExportJobTime(job.StartedAt),
		FinishedAt:  formatExportJobTime(job.FinishedAt),
		ExpiresAt:   formatExportJobTime(job.ExpiresAt),
		Error:       job.Error,
		Total:       job.Total,
		Exported:    job.Exported,
		Truncated:   job.Truncated,
		RawBytes:    job.RawBytes,
		BodyBytes:   job.BodyBytes,
		ContentType: job.ContentType,
	}
	if job.Status == dashboardExportJobSucceeded {
		response.DownloadPath = "/dashboard-events-export-download?id=" + job.ID
	}
	return response
}

func dashboardExportJobDownloadHeaders(job dashboardExportJob) map[string][]string {
	headers := dashboardExportHeaders(job.ETag, job.ContentType, job.Options.Gzip)
	headers["X-Total-Count"] = []string{strconv.Itoa(job.Total)}
	headers["X-Exported-Count"] = []string{strconv.Itoa(job.Exported)}
	headers["X-Export-Truncated"] = []string{strconv.FormatBool(job.Truncated)}
	headers["Content-Disposition"] = []string{fmt.Sprintf(`attachment; filename="usage-events-%s.%s"`, job.ID, dashboardExportJobFileExtension(job.Options))}
	return headers
}

func dashboardExportJobFileExtension(opts dashboardEventsExportOptions) string {
	ext := "json"
	switch opts.Format {
	case dashboardExportCSV:
		ext = "csv"
	case dashboardExportJSONL:
		ext = "jsonl"
	}
	if opts.Gzip {
		return ext + ".gz"
	}
	return ext
}

func dashboardExportJobJSON(statusCode int, body interface{}) ([]byte, error) {
	responseJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp := ManagementResponse{
		StatusCode: statusCode,
		Headers: map[string][]string{
			"Content-Type":  {"application/json; charset=utf-8"},
			"Cache-Control": {"no-store"},
		},
		Body: responseJSON,
	}
	return okEnvelopeJSON(string(mustMarshal(resp)))
}

func dashboardExportJobID(query map[string][]string) string {
	for _, key := range []string{"id", "job_id", "job"} {
		if values, ok := query[key]; ok && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func formatExportJobTime(t time.Time) string {
	return dashboardResourceTime(t)
}

func newDashboardExportJobID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += n
	return n, err
}

func fileSHA256(path string) ([32]byte, error) {
	var sum [32]byte
	file, err := os.Open(path)
	if err != nil {
		return sum, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return sum, err
	}
	copy(sum[:], hash.Sum(nil))
	return sum, nil
}
