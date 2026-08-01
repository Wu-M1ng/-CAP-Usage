package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const eventStoreSchemaVersion = 1

const eventStoreSchema = `
CREATE TABLE IF NOT EXISTS request_events (
	id INTEGER PRIMARY KEY,
	timestamp_ns INTEGER NOT NULL,
	timestamp_zero INTEGER NOT NULL DEFAULT 0 CHECK (timestamp_zero IN (0, 1)),
	api TEXT NOT NULL,
	model TEXT NOT NULL,
	source TEXT NOT NULL,
	source_key TEXT NOT NULL,
	provider TEXT NOT NULL,
	auth_id TEXT NOT NULL,
	auth_index TEXT NOT NULL,
	auth_type TEXT NOT NULL,
	api_key TEXT NOT NULL,
	api_key_hash TEXT NOT NULL,
	api_key_label_hash TEXT NOT NULL,
	endpoint TEXT NOT NULL,
	base_url TEXT NOT NULL,
	stream INTEGER NOT NULL DEFAULT 0 CHECK (stream IN (0, 1)),
	thinking_json TEXT NOT NULL,
	headers_json TEXT NOT NULL,
	input_tokens INTEGER NOT NULL,
	output_tokens INTEGER NOT NULL,
	reasoning_tokens INTEGER NOT NULL,
	cached_tokens INTEGER NOT NULL,
	cache_tokens INTEGER NOT NULL,
	cache_write_tokens INTEGER NOT NULL,
	total_tokens INTEGER NOT NULL,
	latency_ms INTEGER NOT NULL,
	ttft_ms INTEGER NOT NULL,
	failed INTEGER NOT NULL DEFAULT 0 CHECK (failed IN (0, 1)),
	status_code INTEGER NOT NULL,
	failure TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL,
	CHECK (timestamp_zero = 0 OR timestamp_ns = 0)
);

CREATE TABLE IF NOT EXISTS aggregate_state (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	version INTEGER NOT NULL,
	state_json TEXT NOT NULL,
	updated_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_enrichments (
	fingerprint TEXT NOT NULL,
	timestamp_ns INTEGER NOT NULL,
	timestamp_zero INTEGER NOT NULL DEFAULT 0 CHECK (timestamp_zero IN (0, 1)),
	update_json TEXT NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	PRIMARY KEY (fingerprint, timestamp_ns, timestamp_zero),
	CHECK (timestamp_zero = 0 OR timestamp_ns = 0)
);

CREATE INDEX IF NOT EXISTS idx_events_time ON request_events(timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_api_time ON request_events(api, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_model_time ON request_events(model, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_source_time ON request_events(source_key, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_auth_time ON request_events(auth_index, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_client_hash ON request_events(api_key_hash, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_client_label_hash ON request_events(api_key_label_hash, timestamp_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_fingerprint ON request_events(fingerprint, timestamp_ns DESC, id DESC);
`

// eventStore owns the SQLite database that retains request event details and
// the durable aggregate snapshot used to restore lightweight runtime state.
type eventStore struct {
	mu        sync.Mutex
	db        *sql.DB
	path      string
	temporary bool
}

// eventRow is the storage representation supplied by the statistics layer.
// API and Model preserve their grouping keys even when Detail is incomplete.
type eventRow struct {
	API       string
	Model     string
	Detail    RequestDetail
	SourceKey string
}

type eventPruneResult struct {
	Removed          int64
	RetentionDetails []RequestDetail
}

func eventRowFromDetail(api, model string, detail RequestDetail) eventRow {
	if model == "" {
		model = detail.Model
	}
	return eventRow{
		API:       api,
		Model:     model,
		Detail:    detail,
		SourceKey: dashboardEventDetailSourceKey(&detail),
	}
}

func openEventStore(path string, temporary bool) (*eventStore, error) {
	dsn := strings.TrimSpace(path)
	if strings.EqualFold(filepath.Ext(dsn), ".jsonl") || strings.EqualFold(filepath.Base(dsn), "snapshot.json") {
		return nil, errors.New("event store path must be a SQLite database, not a JSONL or snapshot path")
	}
	if temporary {
		file, err := os.CreateTemp("", "cpa-usage-statistics-*.db")
		if err != nil {
			return nil, fmt.Errorf("create temporary event store: %w", err)
		}
		dsn = file.Name()
		if err := file.Close(); err != nil {
			_ = os.Remove(dsn)
			return nil, fmt.Errorf("close temporary event store: %w", err)
		}
	} else if dsn == "" {
		return nil, errors.New("event store path is empty")
	} else {
		parent := filepath.Dir(dsn)
		if parent != "." && parent != "" {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return nil, fmt.Errorf("create event store directory: %w", err)
			}
		}
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}
	// A single writer connection makes exact import deduplication and pending
	// enrichment handoff atomic while SQLite WAL still serves readers safely.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, statement := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA cache_size = -4096",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure event store: %w", err)
		}
	}
	var storedVersion int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&storedVersion); err != nil {
		db.Close()
		return nil, fmt.Errorf("read event store schema version: %w", err)
	}
	if storedVersion > eventStoreSchemaVersion {
		db.Close()
		return nil, fmt.Errorf("event store schema version %d is newer than supported version %d", storedVersion, eventStoreSchemaVersion)
	}
	if storedVersion < eventStoreSchemaVersion {
		if _, err := db.ExecContext(ctx, eventStoreSchema); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize event store schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", eventStoreSchemaVersion)); err != nil {
			db.Close()
			return nil, fmt.Errorf("write event store schema version: %w", err)
		}
	}
	return &eventStore{db: db, path: dsn, temporary: temporary}, nil
}

func sameEventStorePath(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return left == right
	}
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
}

func (s *eventStore) insertEvent(ctx context.Context, row eventRow, fingerprint string, exact bool, cutoff time.Time) (int64, bool, error) {
	db, err := s.database()
	if err != nil {
		return 0, false, err
	}
	if strings.TrimSpace(fingerprint) == "" {
		fingerprint = eventFingerprint(row.API, eventRowModel(row), row.Detail)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin event insert: %w", err)
	}
	defer tx.Rollback()
	id, added, err := s.insertEventTx(ctx, tx, row, fingerprint, exact, cutoff)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit event insert: %w", err)
	}
	return id, added, nil
}

func (s *eventStore) insertEventTx(ctx context.Context, tx *sql.Tx, row eventRow, fingerprint string, exact bool, cutoff time.Time) (int64, bool, error) {
	if exact {
		var existingID int64
		query := "SELECT id FROM request_events WHERE fingerprint = ?"
		args := []any{fingerprint}
		if !cutoff.IsZero() {
			query += " AND timestamp_zero = 0 AND timestamp_ns >= ?"
			args = append(args, cutoff.UTC().UnixNano())
		}
		query += " ORDER BY id DESC LIMIT 1"
		err := tx.QueryRowContext(ctx, query, args...).Scan(&existingID)
		if err == nil {
			return existingID, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("check exact event duplicate: %w", err)
		}
	}

	timestampNS, timestampZero := eventTimestampColumns(row.Detail.Timestamp)
	if update, ok, err := pendingEnrichment(ctx, tx, fingerprint, timestampNS, timestampZero); err != nil {
		return 0, false, err
	} else if ok {
		enrichRequestDetailMetadata(&row.Detail, update)
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM pending_enrichments WHERE fingerprint = ? AND timestamp_ns = ? AND timestamp_zero = ?",
			fingerprint, timestampNS, timestampZero,
		); err != nil {
			return 0, false, fmt.Errorf("delete pending enrichment: %w", err)
		}
	}

	args, err := eventInsertArgs(row, fingerprint)
	if err != nil {
		return 0, false, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO request_events (
	timestamp_ns, timestamp_zero, api, model, source, source_key, provider,
	auth_id, auth_index, auth_type, api_key, api_key_hash, api_key_label_hash,
	endpoint, base_url, stream, thinking_json, headers_json,
	input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_tokens,
	cache_write_tokens, total_tokens, latency_ms, ttft_ms, failed, status_code,
	failure, fingerprint, created_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, args...)
	if err != nil {
		return 0, false, fmt.Errorf("insert event: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("read inserted event id: %w", err)
	}
	return id, true, nil
}

func (s *eventStore) enrichEvent(ctx context.Context, fingerprint string, timestamp time.Time, update RequestDetail) (bool, error) {
	db, err := s.database()
	if err != nil {
		return false, err
	}
	if !hasEventEnrichment(update) {
		return false, nil
	}

	timestampNS, timestampZero := eventTimestampColumns(timestamp)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin event enrichment: %w", err)
	}
	defer tx.Rollback()

	var (
		id       int64
		endpoint string
		thinking string
		stream   int64
	)
	err = tx.QueryRowContext(ctx, `
SELECT id, endpoint, thinking_json, stream
FROM request_events
WHERE fingerprint = ? AND timestamp_ns = ? AND timestamp_zero = ?
ORDER BY id DESC
LIMIT 1`, fingerprint, timestampNS, timestampZero).Scan(&id, &endpoint, &thinking, &stream)
	if errors.Is(err, sql.ErrNoRows) {
		if err := upsertPendingEnrichment(ctx, tx, fingerprint, timestampNS, timestampZero, update); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pending enrichment: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("find event to enrich: %w", err)
	}

	detail := RequestDetail{Endpoint: endpoint, Stream: stream != 0}
	if err := decodeEventThinking(thinking, &detail.Thinking); err != nil {
		return false, err
	}
	if !enrichRequestDetailMetadata(&detail, update) {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit unchanged enrichment: %w", err)
		}
		return false, nil
	}
	thinkingJSON, err := encodeEventThinking(detail.Thinking)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE request_events
SET endpoint = ?, thinking_json = ?, stream = ?
WHERE id = ?`, detail.Endpoint, thinkingJSON, boolToInt64(detail.Stream), id); err != nil {
		return false, fmt.Errorf("update event enrichment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit event enrichment: %w", err)
	}
	return true, nil
}

func (s *eventStore) queryEvents(ctx context.Context, q EventsQuery, now time.Time) (EventsResult, error) {
	q = normalizeEventsQuery(q, true)
	return s.queryEventsPage(ctx, q, now, q.Limit, q.Offset)
}

func (s *eventStore) queryEventsPage(ctx context.Context, q EventsQuery, now time.Time, limit, offset int) (EventsResult, error) {
	db, err := s.database()
	if err != nil {
		return EventsResult{}, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	queryLimit := limit
	if queryLimit <= 0 {
		queryLimit = -1
	}
	if offset < 0 {
		offset = 0
	}
	where, args := eventQueryWhere(q, now)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return EventsResult{}, fmt.Errorf("begin event query: %w", err)
	}
	defer tx.Rollback()

	var total int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM request_events"+where, args...).Scan(&total); err != nil {
		return EventsResult{}, fmt.Errorf("count filtered events: %w", err)
	}

	queryArgs := append(append([]any{}, args...), queryLimit, offset)
	rows, err := tx.QueryContext(ctx, eventSelectColumns+where+" ORDER BY timestamp_ns DESC, id DESC LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return EventsResult{}, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]RequestDetail, 0, q.Limit)
	for rows.Next() {
		detail, err := scanEvent(rows)
		if err != nil {
			return EventsResult{}, err
		}
		events = append(events, detail)
	}
	if err := rows.Err(); err != nil {
		return EventsResult{}, fmt.Errorf("iterate events: %w", err)
	}
	return EventsResult{
		Events:      events,
		Total:       nonNegativeIntFromInt64(total),
		Limit:       limit,
		Offset:      offset,
		GeneratedAt: now.UTC().Format(time.RFC3339),
	}, nil
}

func (s *eventStore) forEachEvent(ctx context.Context, q EventsQuery, now time.Time, fn func(RequestDetail) error) error {
	if fn == nil {
		return nil
	}
	q = normalizeEventsQuery(q, false)
	db, err := s.database()
	if err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now()
	}
	where, args := eventQueryWhere(q, now)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event scan: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, eventSelectColumns+where+" ORDER BY timestamp_ns DESC, id DESC", args...)
	if err != nil {
		return fmt.Errorf("scan events: %w", err)
	}
	for rows.Next() {
		detail, scanErr := scanEvent(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		if callbackErr := fn(detail); callbackErr != nil {
			rows.Close()
			return callbackErr
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate scanned events: %w", err)
	}
	rows.Close()
	return nil
}

func (s *eventStore) queryAPIDetail(ctx context.Context, api, rangeKey, clientAPI string, recentLimit, recentOffset, errorLimit int, now time.Time) (APIDetailResponse, error) {
	db, err := s.database()
	if err != nil {
		return APIDetailResponse{}, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	recentLimit, errorLimit = normalizeDashboardAPIDetailLimits(recentLimit, errorLimit)
	if recentOffset < 0 {
		recentOffset = 0
	}
	q := EventsQuery{API: strings.TrimSpace(api), Range: rangeKey, ClientAPI: strings.TrimSpace(clientAPI)}
	where, args := eventQueryWhere(q, now)
	if q.API == "" {
		where = " WHERE 1 = 0"
		args = nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return APIDetailResponse{}, fmt.Errorf("begin api detail query: %w", err)
	}
	defer tx.Rollback()

	result := APIDetailResponse{
		API:          api,
		RecentEvents: []RequestDetail{},
		RecentLimit:  recentLimit,
		RecentOffset: recentOffset,
		GeneratedAt:  now.UTC().Format(time.RFC3339),
	}
	var (
		totalRequests    int64
		successCount     int64
		failureCount     int64
		totalTokens      int64
		inputTokens      int64
		outputTokens     int64
		cachedTokens     int64
		cacheWriteTokens int64
		reasoningTokens  int64
		latencySum       int64
		latencyCount     int64
	)
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN failed <> 0 THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(%s), 0),
       COALESCE(SUM(MAX(input_tokens, 0)), 0),
       COALESCE(SUM(MAX(output_tokens, 0)), 0),
       COALESCE(SUM(%s), 0),
       COALESCE(SUM(MAX(cache_write_tokens, 0)), 0),
       COALESCE(SUM(MAX(reasoning_tokens, 0)), 0),
       COALESCE(SUM(CASE WHEN latency_ms > 0 THEN latency_ms ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN latency_ms > 0 THEN 1 ELSE 0 END), 0)
FROM request_events%s`, eventTotalTokensSQL, eventCachedTokensSQL, where), args...).Scan(
		&totalRequests, &successCount, &failureCount, &totalTokens, &inputTokens,
		&outputTokens, &cachedTokens, &cacheWriteTokens, &reasoningTokens,
		&latencySum, &latencyCount,
	); err != nil {
		return APIDetailResponse{}, fmt.Errorf("aggregate api detail summary: %w", err)
	}
	result.TotalEvents = nonNegativeIntFromInt64(totalRequests)
	result.RecentTotal = result.TotalEvents
	result.Summary = APIDetailSummary{
		TotalRequests:    totalRequests,
		SuccessCount:     successCount,
		FailureCount:     failureCount,
		TotalTokens:      totalTokens,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		CachedTokens:     cachedTokens,
		CacheWriteTokens: cacheWriteTokens,
		ReasoningTokens:  reasoningTokens,
	}
	if latencyCount > 0 {
		result.Summary.AvgLatencyMs = float64(latencySum) / float64(latencyCount)
	}

	modelAgg := make(map[string]*ModelStat)
	modelRows, err := tx.QueryContext(ctx, fmt.Sprintf(`
SELECT model, provider,
       COUNT(*),
       SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN failed <> 0 THEN 1 ELSE 0 END),
       SUM(%s),
       SUM(MAX(input_tokens, 0)),
       SUM(MAX(output_tokens, 0)),
       SUM(%s),
       SUM(MAX(cache_write_tokens, 0)),
       SUM(MAX(reasoning_tokens, 0)),
       SUM(CASE WHEN latency_ms > 0 THEN latency_ms ELSE 0 END),
       SUM(CASE WHEN latency_ms > 0 THEN 1 ELSE 0 END)
FROM request_events%s
GROUP BY model, provider
ORDER BY COUNT(*) DESC, model ASC, provider ASC`, eventTotalTokensSQL, eventCachedTokensSQL, where), args...)
	if err != nil {
		return APIDetailResponse{}, fmt.Errorf("aggregate api detail models: %w", err)
	}
	for modelRows.Next() {
		var (
			modelName, provider                          string
			requests, successes, failures                int64
			modelTokens, modelInput, modelOutput         int64
			modelCached, modelCacheWrite, modelReasoning int64
			modelLatency, modelLatencyCount              int64
		)
		if err := modelRows.Scan(
			&modelName, &provider, &requests, &successes, &failures,
			&modelTokens, &modelInput, &modelOutput, &modelCached,
			&modelCacheWrite, &modelReasoning, &modelLatency, &modelLatencyCount,
		); err != nil {
			modelRows.Close()
			return APIDetailResponse{}, fmt.Errorf("scan api detail model aggregate: %w", err)
		}
		modelName = normalizeModelName(modelName)
		stat := modelAgg[modelName]
		if stat == nil {
			stat = &ModelStat{Model: modelName}
			modelAgg[modelName] = stat
		}
		stat.TotalRequests += requests
		stat.SuccessCount += successes
		stat.FailureCount += failures
		stat.TotalTokens += modelTokens
		stat.InputTokens += modelInput
		stat.OutputTokens += modelOutput
		stat.CachedTokens += modelCached
		stat.CacheWriteTokens += modelCacheWrite
		stat.ReasoningTokens += modelReasoning
		stat.latencySum += modelLatency
		stat.latencyN += modelLatencyCount
		mergeAPIDetailProviderStat(stat, provider, requests, successes, failures, modelTokens, modelInput, modelOutput, modelCached, modelCacheWrite, modelReasoning)
	}
	if err := modelRows.Err(); err != nil {
		modelRows.Close()
		return APIDetailResponse{}, fmt.Errorf("iterate api detail model aggregates: %w", err)
	}
	modelRows.Close()

	result.ModelStats = make([]ModelStat, 0, len(modelAgg))
	for _, stat := range modelAgg {
		result.ModelStats = append(result.ModelStats, finalizeModelStat(*stat))
	}
	sort.SliceStable(result.ModelStats, func(i, j int) bool {
		if result.ModelStats[i].TotalRequests != result.ModelStats[j].TotalRequests {
			return result.ModelStats[i].TotalRequests > result.ModelStats[j].TotalRequests
		}
		return result.ModelStats[i].Model < result.ModelStats[j].Model
	})
	if (strings.TrimSpace(rangeKey) == "" || strings.EqualFold(strings.TrimSpace(rangeKey), "all")) && strings.TrimSpace(clientAPI) == "" {
		if aggregate, ok, aggregateErr := storedAPIAggregateForDetail(ctx, tx, api); aggregateErr != nil {
			return APIDetailResponse{}, aggregateErr
		} else if ok {
			applyStoredAPIAggregateToDetail(&result, aggregate)
		}
	}

	sourceAgg := make(map[string]*SourceStat)
	sourceRows, err := tx.QueryContext(ctx, fmt.Sprintf(`
SELECT source_key, provider,
       COUNT(*),
       SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN failed <> 0 THEN 1 ELSE 0 END),
       SUM(%s)
FROM request_events%s
GROUP BY source_key, provider
ORDER BY COUNT(*) DESC, source_key ASC, provider ASC`, eventTotalTokensSQL, where), args...)
	if err != nil {
		return APIDetailResponse{}, fmt.Errorf("aggregate api detail sources: %w", err)
	}
	for sourceRows.Next() {
		var source, provider string
		var requests, successes, failures, tokens int64
		if err := sourceRows.Scan(&source, &provider, &requests, &successes, &failures, &tokens); err != nil {
			sourceRows.Close()
			return APIDetailResponse{}, fmt.Errorf("scan api detail source aggregate: %w", err)
		}
		source = summarySourceKey(RequestDetail{Source: source})
		stat := sourceAgg[source]
		if stat == nil {
			stat = &SourceStat{Source: source, Provider: strings.TrimSpace(provider)}
			sourceAgg[source] = stat
		} else if stat.Provider == "" {
			stat.Provider = strings.TrimSpace(provider)
		}
		stat.TotalRequests += requests
		stat.SuccessCount += successes
		stat.FailureCount += failures
		stat.TotalTokens += tokens
	}
	if err := sourceRows.Err(); err != nil {
		sourceRows.Close()
		return APIDetailResponse{}, fmt.Errorf("iterate api detail source aggregates: %w", err)
	}
	sourceRows.Close()
	result.SourceStats = make([]SourceStat, 0, len(sourceAgg))
	for _, stat := range sourceAgg {
		result.SourceStats = append(result.SourceStats, *stat)
	}
	sort.SliceStable(result.SourceStats, func(i, j int) bool {
		if result.SourceStats[i].TotalRequests != result.SourceStats[j].TotalRequests {
			return result.SourceStats[i].TotalRequests > result.SourceStats[j].TotalRequests
		}
		return result.SourceStats[i].Source < result.SourceStats[j].Source
	})

	errorRows, err := tx.QueryContext(ctx, fmt.Sprintf(`
	SELECT status_code,
	       CASE WHEN TRIM(failure) = '' THEN 'unknown failure' ELSE TRIM(failure) END,
	       COUNT(*) AS error_count
FROM request_events%s AND failed <> 0
GROUP BY status_code, CASE WHEN TRIM(failure) = '' THEN 'unknown failure' ELSE TRIM(failure) END
ORDER BY error_count DESC, status_code ASC, failure ASC
LIMIT ?`, where), append(append([]any{}, args...), errorLimit)...)
	if err != nil {
		return APIDetailResponse{}, fmt.Errorf("aggregate api detail errors: %w", err)
	}
	result.ErrorStats = make([]APIDetailErrorStat, 0, errorLimit)
	for errorRows.Next() {
		var stat APIDetailErrorStat
		if err := errorRows.Scan(&stat.StatusCode, &stat.Failure, &stat.Count); err != nil {
			errorRows.Close()
			return APIDetailResponse{}, fmt.Errorf("scan api detail error aggregate: %w", err)
		}
		if strings.TrimSpace(stat.Failure) == "" {
			stat.Failure = "unknown failure"
		}
		result.ErrorStats = append(result.ErrorStats, stat)
	}
	if err := errorRows.Err(); err != nil {
		errorRows.Close()
		return APIDetailResponse{}, fmt.Errorf("iterate api detail error aggregates: %w", err)
	}
	errorRows.Close()

	recentArgs := append(append([]any{}, args...), recentLimit, recentOffset)
	recentRows, err := tx.QueryContext(ctx, eventSelectColumns+where+" ORDER BY timestamp_ns DESC, id DESC LIMIT ? OFFSET ?", recentArgs...)
	if err != nil {
		return APIDetailResponse{}, fmt.Errorf("query recent api events: %w", err)
	}
	for recentRows.Next() {
		detail, scanErr := scanEvent(recentRows)
		if scanErr != nil {
			recentRows.Close()
			return APIDetailResponse{}, scanErr
		}
		result.RecentEvents = append(result.RecentEvents, detail)
	}
	if err := recentRows.Err(); err != nil {
		recentRows.Close()
		return APIDetailResponse{}, fmt.Errorf("iterate recent api events: %w", err)
	}
	recentRows.Close()
	return result, nil
}

func storedAPIAggregateForDetail(ctx context.Context, tx *sql.Tx, api string) (APISnapshot, bool, error) {
	var encoded string
	err := tx.QueryRowContext(ctx, "SELECT state_json FROM aggregate_state WHERE id = 1").Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return APISnapshot{}, false, nil
	}
	if err != nil {
		return APISnapshot{}, false, fmt.Errorf("read api detail aggregate state: %w", err)
	}
	var snapshot StatisticsSnapshot
	if err := json.Unmarshal([]byte(encoded), &snapshot); err != nil {
		return APISnapshot{}, false, fmt.Errorf("decode api detail aggregate state: %w", err)
	}
	apiSnapshot, ok := snapshot.APIs[strings.TrimSpace(api)]
	return apiSnapshot, ok, nil
}

func applyStoredAPIAggregateToDetail(result *APIDetailResponse, apiSnapshot APISnapshot) {
	if result == nil {
		return
	}
	result.Summary = APIDetailSummary{
		TotalRequests:    apiSnapshot.TotalRequests,
		SuccessCount:     apiSnapshot.SuccessCount,
		FailureCount:     apiSnapshot.FailureCount,
		TotalTokens:      apiSnapshot.TotalTokens,
		InputTokens:      apiSnapshot.InputTokens,
		OutputTokens:     apiSnapshot.OutputTokens,
		CachedTokens:     apiSnapshot.CachedTokens,
		CacheWriteTokens: apiSnapshot.CacheWriteTokens,
		ReasoningTokens:  apiSnapshot.ReasoningTokens,
		AvgLatencyMs:     apiSnapshot.AvgLatencyMs,
	}
	result.ModelStats = make([]ModelStat, 0, len(apiSnapshot.Models))
	for modelName, modelSnapshot := range apiSnapshot.Models {
		stat := ModelStat{
			Model:            normalizeModelName(modelName),
			TotalRequests:    modelSnapshot.TotalRequests,
			SuccessCount:     modelSnapshot.SuccessCount,
			FailureCount:     modelSnapshot.FailureCount,
			TotalTokens:      modelSnapshot.TotalTokens,
			InputTokens:      modelSnapshot.InputTokens,
			OutputTokens:     modelSnapshot.OutputTokens,
			CachedTokens:     modelSnapshot.CachedTokens,
			CacheWriteTokens: modelSnapshot.CacheWriteTokens,
			ReasoningTokens:  modelSnapshot.ReasoningTokens,
			latencySum:       restoredLatencySum(modelSnapshot.AvgLatencyMs, modelSnapshot.TotalRequests),
			latencyN:         restoredLatencyCount(modelSnapshot.AvgLatencyMs, modelSnapshot.TotalRequests),
			providerStats:    modelProviderStatsFromSnapshot(modelSnapshot.Providers),
		}
		result.ModelStats = append(result.ModelStats, finalizeModelStat(stat))
	}
	sort.SliceStable(result.ModelStats, func(i, j int) bool {
		if result.ModelStats[i].TotalRequests != result.ModelStats[j].TotalRequests {
			return result.ModelStats[i].TotalRequests > result.ModelStats[j].TotalRequests
		}
		return result.ModelStats[i].Model < result.ModelStats[j].Model
	})
}

func restoredLatencySum(avg float64, count int64) int64 {
	if !(avg > 0) || count <= 0 {
		return 0
	}
	return int64(math.Round(avg * float64(count)))
}

func restoredLatencyCount(avg float64, count int64) int64 {
	if !(avg > 0) || count <= 0 {
		return 0
	}
	return count
}

func (s *eventStore) populateSnapshotDetails(ctx context.Context, snapshot *StatisticsSnapshot) error {
	if snapshot == nil {
		return nil
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	if snapshot.APIs == nil {
		snapshot.APIs = make(map[string]APISnapshot)
	}
	rows, err := db.QueryContext(ctx, eventSelectColumns+" ORDER BY timestamp_ns ASC, id ASC")
	if err != nil {
		return fmt.Errorf("query snapshot events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		detail, err := scanEvent(rows)
		if err != nil {
			return err
		}
		apiName := strings.TrimSpace(detail.UpstreamAPI)
		if apiName == "" {
			apiName = "未知接口"
		}
		apiSnapshot, ok := snapshot.APIs[apiName]
		if !ok {
			apiSnapshot = APISnapshot{Models: make(map[string]ModelSnapshot)}
		}
		if apiSnapshot.Models == nil {
			apiSnapshot.Models = make(map[string]ModelSnapshot)
		}
		modelName := normalizeModelName(detail.Model)
		modelSnapshot := apiSnapshot.Models[modelName]
		modelSnapshot.Details = append(modelSnapshot.Details, detail)
		apiSnapshot.Models[modelName] = modelSnapshot
		snapshot.APIs[apiName] = apiSnapshot
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot events: %w", err)
	}
	return nil
}

func (s *eventStore) count(ctx context.Context) (int64, error) {
	db, err := s.database()
	if err != nil {
		return 0, err
	}
	var total int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM request_events").Scan(&total); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return total, nil
}

func (s *eventStore) prune(ctx context.Context, maxDetailsPerModel int, retention time.Duration, now time.Time) (eventPruneResult, error) {
	db, err := s.database()
	if err != nil {
		return eventPruneResult{}, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return eventPruneResult{}, fmt.Errorf("begin event prune: %w", err)
	}
	defer tx.Rollback()
	result, err := s.pruneTx(ctx, tx, maxDetailsPerModel, retention, now)
	if err != nil {
		return eventPruneResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return eventPruneResult{}, fmt.Errorf("commit event prune: %w", err)
	}
	return result, nil
}

func (s *eventStore) pruneTx(ctx context.Context, tx *sql.Tx, maxDetailsPerModel int, retention time.Duration, now time.Time) (eventPruneResult, error) {
	result := eventPruneResult{}
	deleteRows := func(query string, args ...any) error {
		execResult, execErr := tx.ExecContext(ctx, query, args...)
		if execErr != nil {
			return execErr
		}
		count, countErr := execResult.RowsAffected()
		if countErr == nil {
			result.Removed += count
		}
		return nil
	}
	if retention > 0 {
		cutoff := now.Add(-retention).UTC().UnixNano()
		rows, err := tx.QueryContext(ctx, eventSelectColumns+" WHERE timestamp_zero = 0 AND timestamp_ns < ? ORDER BY id ASC", cutoff)
		if err != nil {
			return eventPruneResult{}, fmt.Errorf("query expired events: %w", err)
		}
		for rows.Next() {
			detail, scanErr := scanEvent(rows)
			if scanErr != nil {
				rows.Close()
				return eventPruneResult{}, fmt.Errorf("scan expired event: %w", scanErr)
			}
			result.RetentionDetails = append(result.RetentionDetails, detail)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return eventPruneResult{}, fmt.Errorf("iterate expired events: %w", err)
		}
		rows.Close()
		if err := deleteRows("DELETE FROM request_events WHERE timestamp_zero = 0 AND timestamp_ns < ?", cutoff); err != nil {
			return eventPruneResult{}, fmt.Errorf("prune expired events: %w", err)
		}
	}
	if maxDetailsPerModel >= 0 {
		if err := deleteRows(`DELETE FROM request_events
WHERE id IN (
	SELECT id FROM (
		SELECT id, ROW_NUMBER() OVER (PARTITION BY api, model ORDER BY timestamp_ns DESC, id DESC) AS row_number
		FROM request_events
	) ranked
	WHERE row_number > ?
)`, maxDetailsPerModel); err != nil {
			return eventPruneResult{}, fmt.Errorf("prune model event limit: %w", err)
		}
	}
	if retention > 0 {
		if _, err := tx.ExecContext(ctx, "DELETE FROM pending_enrichments WHERE timestamp_zero = 0 AND timestamp_ns < ?", now.Add(-retention).UTC().UnixNano()); err != nil {
			return eventPruneResult{}, fmt.Errorf("prune pending enrichments: %w", err)
		}
	}
	return result, nil
}

func (s *eventStore) close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	db := s.db
	s.db = nil
	s.mu.Unlock()
	if db == nil {
		return nil
	}
	err := db.Close()
	if s.temporary && s.path != "" {
		for _, path := range []string{s.path, s.path + "-wal", s.path + "-shm"} {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) && err == nil {
				err = fmt.Errorf("remove temporary event store: %w", removeErr)
			}
		}
	}
	return err
}

func (s *eventStore) loadAggregate(ctx context.Context) (StatisticsSnapshot, bool, error) {
	db, err := s.database()
	if err != nil {
		return StatisticsSnapshot{}, false, err
	}
	var encoded string
	err = db.QueryRowContext(ctx, "SELECT state_json FROM aggregate_state WHERE id = 1").Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return StatisticsSnapshot{}, false, nil
	}
	if err != nil {
		return StatisticsSnapshot{}, false, fmt.Errorf("read aggregate state: %w", err)
	}
	var snapshot StatisticsSnapshot
	if err := json.Unmarshal([]byte(encoded), &snapshot); err != nil {
		return StatisticsSnapshot{}, false, fmt.Errorf("decode aggregate state: %w", err)
	}
	for apiName, apiSnapshot := range snapshot.APIs {
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelSnapshot.Details = nil
			apiSnapshot.Models[modelName] = modelSnapshot
		}
		snapshot.APIs[apiName] = apiSnapshot
	}
	return snapshot, true, nil
}

func (s *eventStore) isEmpty(ctx context.Context) (bool, error) {
	db, err := s.database()
	if err != nil {
		return false, err
	}
	var events, aggregates, pending int64
	if err := db.QueryRowContext(ctx, `
SELECT
  (SELECT COUNT(*) FROM request_events),
  (SELECT COUNT(*) FROM aggregate_state),
  (SELECT COUNT(*) FROM pending_enrichments)`).Scan(&events, &aggregates, &pending); err != nil {
		return false, fmt.Errorf("inspect event store contents: %w", err)
	}
	return events == 0 && aggregates == 0 && pending == 0, nil
}

// copyFrom copies an old SQLite store row by row. It is only used when a
// newly selected database is empty, so an existing destination is never
// overwritten during configuration reload.
func (s *eventStore) copyFrom(ctx context.Context, source *eventStore) error {
	if s == nil || source == nil || s == source {
		return nil
	}
	sourceDB, err := source.database()
	if err != nil {
		return err
	}
	destinationDB, err := s.database()
	if err != nil {
		return err
	}
	sourceTx, err := sourceDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event store copy read: %w", err)
	}
	defer sourceTx.Rollback()
	destinationTx, err := destinationDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event store copy write: %w", err)
	}
	defer destinationTx.Rollback()

	rows, err := sourceTx.QueryContext(ctx, `
SELECT timestamp_ns, timestamp_zero, api, model, source, source_key, provider,
       auth_id, auth_index, auth_type, api_key, api_key_hash, api_key_label_hash,
       endpoint, base_url, stream, thinking_json, headers_json,
       input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_tokens,
       cache_write_tokens, total_tokens, latency_ms, ttft_ms, failed, status_code,
       failure, fingerprint, created_at_ns
FROM request_events
ORDER BY id ASC`)
	if err != nil {
		return fmt.Errorf("read event store rows for copy: %w", err)
	}
	for rows.Next() {
		var row storedEventRow
		if err := rows.Scan(
			&row.timestampNS, &row.timestampZero, &row.api, &row.model, &row.source,
			&row.sourceKey, &row.provider, &row.authID, &row.authIndex, &row.authType,
			&row.apiKey, &row.apiKeyHash, &row.apiKeyLabelHash, &row.endpoint, &row.baseURL,
			&row.stream, &row.thinking, &row.headers, &row.inputTokens, &row.outputTokens,
			&row.reasoningTokens, &row.cachedTokens, &row.cacheTokens, &row.cacheWriteTokens,
			&row.totalTokens, &row.latencyMS, &row.ttftMS, &row.failed, &row.statusCode,
			&row.failure, &row.fingerprint, &row.createdAtNS,
		); err != nil {
			rows.Close()
			return fmt.Errorf("scan event store row for copy: %w", err)
		}
		if _, err := destinationTx.ExecContext(ctx, storedEventInsertSQL,
			row.timestampNS, row.timestampZero, row.api, row.model, row.source, row.sourceKey,
			row.provider, row.authID, row.authIndex, row.authType, row.apiKey, row.apiKeyHash,
			row.apiKeyLabelHash, row.endpoint, row.baseURL, row.stream, row.thinking, row.headers,
			row.inputTokens, row.outputTokens, row.reasoningTokens, row.cachedTokens, row.cacheTokens,
			row.cacheWriteTokens, row.totalTokens, row.latencyMS, row.ttftMS, row.failed,
			row.statusCode, row.failure, row.fingerprint, row.createdAtNS,
		); err != nil {
			rows.Close()
			return fmt.Errorf("write copied event store row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate event store rows for copy: %w", err)
	}
	rows.Close()

	pendingRows, err := sourceTx.QueryContext(ctx, `
SELECT fingerprint, timestamp_ns, timestamp_zero, update_json, updated_at_ns
FROM pending_enrichments`)
	if err != nil {
		return fmt.Errorf("read pending enrichments for copy: %w", err)
	}
	for pendingRows.Next() {
		var fingerprint, updateJSON string
		var timestampNS, timestampZero, updatedAtNS int64
		if err := pendingRows.Scan(&fingerprint, &timestampNS, &timestampZero, &updateJSON, &updatedAtNS); err != nil {
			pendingRows.Close()
			return fmt.Errorf("scan pending enrichment for copy: %w", err)
		}
		if _, err := destinationTx.ExecContext(ctx, `
INSERT INTO pending_enrichments (fingerprint, timestamp_ns, timestamp_zero, update_json, updated_at_ns)
VALUES (?, ?, ?, ?, ?)`, fingerprint, timestampNS, timestampZero, updateJSON, updatedAtNS); err != nil {
			pendingRows.Close()
			return fmt.Errorf("write copied pending enrichment: %w", err)
		}
	}
	if err := pendingRows.Err(); err != nil {
		pendingRows.Close()
		return fmt.Errorf("iterate pending enrichments for copy: %w", err)
	}
	pendingRows.Close()

	var aggregateVersion int
	var aggregateJSON string
	var aggregateUpdatedAt int64
	err = sourceTx.QueryRowContext(ctx, `
SELECT version, state_json, updated_at_ns
FROM aggregate_state
WHERE id = 1`).Scan(&aggregateVersion, &aggregateJSON, &aggregateUpdatedAt)
	if err == nil {
		if _, err := destinationTx.ExecContext(ctx, `
INSERT INTO aggregate_state (id, version, state_json, updated_at_ns)
VALUES (1, ?, ?, ?)`, aggregateVersion, aggregateJSON, aggregateUpdatedAt); err != nil {
			return fmt.Errorf("write copied aggregate state: %w", err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read aggregate state for copy: %w", err)
	}

	if err := destinationTx.Commit(); err != nil {
		return fmt.Errorf("commit event store copy: %w", err)
	}
	return nil
}

type storedEventRow struct {
	timestampNS, timestampZero                               int64
	api, model, source, sourceKey, provider                  string
	authID, authIndex, authType, apiKey, apiKeyHash          string
	apiKeyLabelHash, endpoint, baseURL                       string
	stream, inputTokens, outputTokens, reasoningTokens       int64
	cachedTokens, cacheTokens, cacheWriteTokens, totalTokens int64
	latencyMS, ttftMS, failed, statusCode, createdAtNS       int64
	thinking, headers, failure, fingerprint                  string
}

const storedEventInsertSQL = `
INSERT INTO request_events (
timestamp_ns, timestamp_zero, api, model, source, source_key, provider,
auth_id, auth_index, auth_type, api_key, api_key_hash, api_key_label_hash,
endpoint, base_url, stream, thinking_json, headers_json,
input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_tokens,
cache_write_tokens, total_tokens, latency_ms, ttft_ms, failed, status_code,
failure, fingerprint, created_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func (s *eventStore) saveAggregate(ctx context.Context, snapshot StatisticsSnapshot) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin aggregate state write: %w", err)
	}
	defer tx.Rollback()
	if err := s.saveAggregateTx(ctx, tx, snapshot); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit aggregate state write: %w", err)
	}
	return nil
}

func (s *eventStore) saveAggregateTx(ctx context.Context, tx *sql.Tx, snapshot StatisticsSnapshot) error {
	for apiName, apiSnapshot := range snapshot.APIs {
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelSnapshot.Details = nil
			apiSnapshot.Models[modelName] = modelSnapshot
		}
		snapshot.APIs[apiName] = apiSnapshot
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode aggregate state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO aggregate_state (id, version, state_json, updated_at_ns)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	version = excluded.version,
	state_json = excluded.state_json,
	updated_at_ns = excluded.updated_at_ns`, eventStoreSchemaVersion, string(encoded), time.Now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("write aggregate state: %w", err)
	}
	return nil
}

func (s *eventStore) database() (*sql.DB, error) {
	if s == nil {
		return nil, errors.New("event store is nil")
	}
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()
	if db == nil {
		return nil, errors.New("event store is closed")
	}
	return db, nil
}

func eventFingerprint(api, model string, detail RequestDetail) string {
	return fingerprintRequestDedupKey(dedupKey(api, model, detail))
}

func fingerprintRequestDedupKey(key requestDedupKey) string {
	var encoded []byte
	appendString := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, value...)
	}
	appendInt64 := func(value int64) {
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], uint64(value))
		encoded = append(encoded, raw[:]...)
	}
	appendBool := func(value bool) {
		if value {
			encoded = append(encoded, 1)
		} else {
			encoded = append(encoded, 0)
		}
	}

	appendString(key.apiName)
	appendString(key.modelName)
	if key.timestamp.IsZero() {
		encoded = append(encoded, 1)
		appendInt64(0)
	} else {
		encoded = append(encoded, 0)
		appendInt64(key.timestamp.UTC().UnixNano())
	}
	appendString(key.source)
	appendString(key.authIndex)
	appendString(key.clientAPIHash)
	appendString(key.clientAPIKey)
	appendString(key.failure)
	appendBool(key.failed)
	appendInt64(key.latencyMs)
	appendInt64(key.ttftMs)
	appendInt64(int64(key.statusCode))
	appendInt64(key.inputTokens)
	appendInt64(key.outputTokens)
	appendInt64(key.reasoning)
	appendInt64(key.cachedTokens)
	appendInt64(key.cacheTokens)
	appendInt64(key.cacheWriteTokens)
	appendInt64(key.totalTokens)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func eventInsertArgs(row eventRow, fingerprint string) ([]any, error) {
	detail := row.Detail
	timestampNS, timestampZero := eventTimestampColumns(detail.Timestamp)
	thinkingJSON, err := encodeEventThinking(detail.Thinking)
	if err != nil {
		return nil, err
	}
	headersJSON, err := encodeEventHeaders(detail.Headers)
	if err != nil {
		return nil, err
	}
	model := row.Model
	if model == "" {
		model = detail.Model
	}
	sourceKey := row.SourceKey
	if sourceKey == "" {
		sourceKey = dashboardEventDetailSourceKey(&detail)
	}
	return []any{
		timestampNS, timestampZero, row.API, model, detail.Source, sourceKey, detail.Provider,
		detail.AuthID, detail.AuthIndex, detail.AuthType, detail.APIKey, detail.APIKeyHash, hashAPIKey(detail.APIKey),
		detail.Endpoint, detail.BaseURL, boolToInt64(detail.Stream), thinkingJSON, headersJSON,
		detail.Tokens.InputTokens, detail.Tokens.OutputTokens, detail.Tokens.ReasoningTokens, detail.Tokens.CachedTokens,
		detail.Tokens.CacheTokens, detail.Tokens.CacheWriteTokens, detail.Tokens.TotalTokens, detail.LatencyMs, detail.TTFTMs,
		boolToInt64(detail.Failed), detail.StatusCode, detail.Failure, fingerprint, time.Now().UTC().UnixNano(),
	}, nil
}

func eventRowModel(row eventRow) string {
	if row.Model != "" {
		return row.Model
	}
	return row.Detail.Model
}

func eventTimestampColumns(timestamp time.Time) (int64, int64) {
	if timestamp.IsZero() {
		return 0, 1
	}
	return timestamp.UTC().UnixNano(), 0
}

func eventTimestamp(timestampNS, timestampZero int64) time.Time {
	if timestampZero != 0 {
		return time.Time{}
	}
	return time.Unix(0, timestampNS).UTC()
}

func encodeEventThinking(thinking UsageThinking) (string, error) {
	if thinking == (UsageThinking{}) {
		return "", nil
	}
	encoded, err := json.Marshal(thinking)
	if err != nil {
		return "", fmt.Errorf("encode event thinking: %w", err)
	}
	return string(encoded), nil
}

func decodeEventThinking(encoded string, thinking *UsageThinking) error {
	if strings.TrimSpace(encoded) == "" || strings.TrimSpace(encoded) == "null" || strings.TrimSpace(encoded) == "{}" {
		return nil
	}
	if err := json.Unmarshal([]byte(encoded), thinking); err != nil {
		return fmt.Errorf("decode event thinking: %w", err)
	}
	return nil
}

func encodeEventHeaders(headers map[string][]string) (string, error) {
	if len(headers) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("encode event headers: %w", err)
	}
	return string(encoded), nil
}

func decodeEventHeaders(encoded string) (map[string][]string, error) {
	if strings.TrimSpace(encoded) == "" || strings.TrimSpace(encoded) == "null" || strings.TrimSpace(encoded) == "{}" {
		return nil, nil
	}
	var headers map[string][]string
	if err := json.Unmarshal([]byte(encoded), &headers); err != nil {
		return nil, fmt.Errorf("decode event headers: %w", err)
	}
	return headers, nil
}

func hasEventEnrichment(update RequestDetail) bool {
	return strings.TrimSpace(update.Endpoint) != "" || update.Thinking != (UsageThinking{}) || update.Stream
}

func pendingEnrichment(ctx context.Context, tx *sql.Tx, fingerprint string, timestampNS, timestampZero int64) (RequestDetail, bool, error) {
	var encoded string
	err := tx.QueryRowContext(ctx,
		"SELECT update_json FROM pending_enrichments WHERE fingerprint = ? AND timestamp_ns = ? AND timestamp_zero = ?",
		fingerprint, timestampNS, timestampZero,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return RequestDetail{}, false, nil
	}
	if err != nil {
		return RequestDetail{}, false, fmt.Errorf("read pending enrichment: %w", err)
	}
	var update RequestDetail
	if err := json.Unmarshal([]byte(encoded), &update); err != nil {
		return RequestDetail{}, false, fmt.Errorf("decode pending enrichment: %w", err)
	}
	return update, true, nil
}

func upsertPendingEnrichment(ctx context.Context, tx *sql.Tx, fingerprint string, timestampNS, timestampZero int64, update RequestDetail) error {
	pending, found, err := pendingEnrichment(ctx, tx, fingerprint, timestampNS, timestampZero)
	if err != nil {
		return err
	}
	if found {
		enrichRequestDetailMetadata(&pending, update)
	} else {
		pending = RequestDetail{}
		enrichRequestDetailMetadata(&pending, update)
	}
	encoded, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("encode pending enrichment: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO pending_enrichments (fingerprint, timestamp_ns, timestamp_zero, update_json, updated_at_ns)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(fingerprint, timestamp_ns, timestamp_zero) DO UPDATE SET
	update_json = excluded.update_json,
	updated_at_ns = excluded.updated_at_ns`, fingerprint, timestampNS, timestampZero, string(encoded), time.Now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("write pending enrichment: %w", err)
	}
	return nil
}

const eventSelectColumns = `SELECT
	timestamp_ns, timestamp_zero, api, model, source, provider, auth_id, auth_index,
	auth_type, api_key, api_key_hash, endpoint, base_url, stream, thinking_json,
	headers_json, input_tokens, output_tokens, reasoning_tokens, cached_tokens,
	cache_tokens, cache_write_tokens, total_tokens, latency_ms, ttft_ms, failed,
	status_code, failure
FROM request_events`

// These expressions mirror detailTotalsFromRequest while keeping aggregate
// queries inside SQLite. Event rows are normalized on write, but the lower
// bounds also make older or manually repaired rows harmless to summaries.
const eventCachedTokensSQL = `CASE
WHEN cached_tokens > 0 THEN cached_tokens
ELSE MAX(cache_tokens - MAX(cache_write_tokens, 0), 0)
END`

const eventNormalizedCacheTokensSQL = `CASE
WHEN cache_tokens > 0 THEN MAX(cache_tokens, MAX(cached_tokens, MAX(cache_write_tokens, 0)))
ELSE MAX(cached_tokens, 0) + MAX(cache_write_tokens, 0)
END`

const eventTotalTokensSQL = `MAX(
MAX(total_tokens, 0),
MAX(input_tokens, 0) + MAX(output_tokens, 0),
CASE
WHEN LOWER(TRIM(provider)) = 'claude'
  OR LOWER(TRIM(provider)) LIKE 'claude-%'
  OR LOWER(TRIM(provider)) = 'anthropic'
  OR LOWER(TRIM(provider)) LIKE 'anthropic-%'
  OR (TRIM(provider) = '' AND MAX(total_tokens, 0) >= MAX(input_tokens, 0) + MAX(output_tokens, 0) + (` + eventNormalizedCacheTokensSQL + `))
THEN MAX(input_tokens, 0) + MAX(output_tokens, 0) + (` + eventNormalizedCacheTokensSQL + `)
ELSE 0
END
)`

func mergeAPIDetailProviderStat(stat *ModelStat, provider string, requests, successes, failures, totalTokens, inputTokens, outputTokens, cachedTokens, cacheWriteTokens, reasoningTokens int64) {
	if stat == nil {
		return
	}
	if stat.providerStats == nil {
		stat.providerStats = make(map[string]*ModelProviderStat)
	}
	key := modelProviderStatsKey(provider)
	providerStat := stat.providerStats[key]
	if providerStat == nil {
		providerStat = &ModelProviderStat{Provider: strings.TrimSpace(provider)}
		stat.providerStats[key] = providerStat
	}
	providerStat.TotalRequests += requests
	providerStat.SuccessCount += successes
	providerStat.FailureCount += failures
	providerStat.TotalTokens += totalTokens
	providerStat.InputTokens += inputTokens
	providerStat.OutputTokens += outputTokens
	providerStat.CachedTokens += cachedTokens
	providerStat.CacheWriteTokens += cacheWriteTokens
	providerStat.ReasoningTokens += reasoningTokens
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanEvent(scanner eventScanner) (RequestDetail, error) {
	var (
		timestampNS   int64
		timestampZero int64
		api           string
		model         string
		source        string
		provider      string
		authID        string
		authIndex     string
		authType      string
		apiKey        string
		apiKeyHash    string
		endpoint      string
		baseURL       string
		stream        int64
		thinking      string
		headers       string
		inputTokens   int64
		outputTokens  int64
		reasoning     int64
		cached        int64
		cache         int64
		cacheWrite    int64
		total         int64
		latency       int64
		ttft          int64
		failed        int64
		statusCode    int
		failure       string
	)
	if err := scanner.Scan(
		&timestampNS, &timestampZero, &api, &model, &source, &provider, &authID, &authIndex,
		&authType, &apiKey, &apiKeyHash, &endpoint, &baseURL, &stream, &thinking, &headers,
		&inputTokens, &outputTokens, &reasoning, &cached, &cache, &cacheWrite, &total, &latency,
		&ttft, &failed, &statusCode, &failure,
	); err != nil {
		return RequestDetail{}, fmt.Errorf("scan event: %w", err)
	}
	detail := RequestDetail{
		UpstreamAPI: api,
		Model:       model,
		Timestamp:   eventTimestamp(timestampNS, timestampZero),
		LatencyMs:   latency,
		TTFTMs:      ttft,
		APIKey:      apiKey,
		APIKeyHash:  apiKeyHash,
		Source:      source,
		Provider:    provider,
		AuthID:      authID,
		AuthIndex:   authIndex,
		AuthType:    authType,
		Endpoint:    endpoint,
		BaseURL:     baseURL,
		Stream:      stream != 0,
		Tokens: TokenStats{
			InputTokens:      inputTokens,
			OutputTokens:     outputTokens,
			ReasoningTokens:  reasoning,
			CachedTokens:     cached,
			CacheTokens:      cache,
			CacheWriteTokens: cacheWrite,
			TotalTokens:      total,
		},
		Failed:     failed != 0,
		StatusCode: statusCode,
		Failure:    failure,
	}
	if err := decodeEventThinking(thinking, &detail.Thinking); err != nil {
		return RequestDetail{}, err
	}
	headersValue, err := decodeEventHeaders(headers)
	if err != nil {
		return RequestDetail{}, err
	}
	detail.Headers = headersValue
	return detail, nil
}

func eventQueryWhere(q EventsQuery, now time.Time) (string, []any) {
	conditions := make([]string, 0, 7)
	args := make([]any, 0, 7)
	if cutoff := dashboardRangeCutoff(q.Range, now); !cutoff.IsZero() {
		conditions = append(conditions, "timestamp_zero = 0 AND timestamp_ns >= ?")
		args = append(args, cutoff.UTC().UnixNano())
	}
	if !q.Before.IsZero() {
		conditions = append(conditions, "(timestamp_zero = 1 OR timestamp_ns <= ?)")
		args = append(args, q.Before.UTC().UnixNano())
	}
	if q.Model != "" {
		conditions = append(conditions, "model = ?")
		args = append(args, q.Model)
	}
	if q.Source != "" {
		conditions = append(conditions, "source_key = ?")
		args = append(args, q.Source)
	}
	if q.AuthIndex != "" {
		conditions = append(conditions, "auth_index = ?")
		args = append(args, q.AuthIndex)
	}
	if q.API != "" {
		conditions = append(conditions, "api = ?")
		args = append(args, q.API)
	}
	if strings.TrimSpace(q.ClientAPI) != "" {
		selector, ok := parseClientAPISelector(q.ClientAPI)
		if !ok {
			conditions = append(conditions, "1 = 0")
		} else {
			switch selector.kind {
			case 'u':
				conditions = append(conditions, "api_key_hash = '' AND api_key = ''")
			case 'm':
				conditions = append(conditions, "api_key_label_hash = ?")
				args = append(args, selector.labelHash)
			case 'h':
				if selector.labelHash == "" {
					conditions = append(conditions, "api_key_hash = ?")
					args = append(args, selector.hash)
				} else {
					conditions = append(conditions, "(api_key_hash = ? OR (api_key_hash = '' AND api_key_label_hash = ?))")
					args = append(args, selector.hash, selector.labelHash)
				}
			}
		}
	}
	if len(conditions) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
