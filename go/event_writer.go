package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const eventSpoolMaxBytes int64 = 64 << 20

// eventWriteTask is the smallest unit accepted by the SQLite writer. The
// RequestDetail has already been normalized and the in-memory aggregate has
// already been published before a task enters this queue.
type eventWriteTask struct {
	store       *eventStore
	apiName     string
	modelName   string
	detail      RequestDetail
	enqueuedAt  time.Time
	maxDetails  int
	retention   time.Duration
	fingerprint string
	exact       bool
	replay      *eventSpoolReplay
}

type eventSpoolReplay struct {
	mu        sync.Mutex
	path      string
	remaining int
	failed    []persistedDetail
}

type eventWriterState struct {
	snapshotRecords int64
	lastSnapshot    time.Time
	forceSnapshot   bool
}

func (s *RequestStatistics) startEventWriterLocked() {
	if s == nil || s.eventWriterQueue != nil {
		return
	}
	capacity := s.eventWriterQueueCapacity
	if capacity <= 0 {
		capacity = defaultStorageWriteQueueSize
		s.eventWriterQueueCapacity = capacity
	}
	queue := make(chan eventWriteTask, capacity)
	stop := make(chan struct{})
	done := make(chan struct{})
	s.eventWriterQueue = queue
	s.eventWriterStop = stop
	s.eventWriterDone = done
	s.eventWriterStopping = false
	s.eventWriterRunning = true
	s.eventWriterQueueLength = 0
	s.eventWriterSnapshotRecords = 0
	s.eventWriterLastSnapshot = time.Now()
	go s.eventWriterLoop(queue, stop, done)
	go s.replayEventSpool(s.eventStore, true)
}

// enqueueEventWriteLocked is non-blocking and must be called with
// storageControlMu held. The wait groups protect the referenced SQLite store
// from configuration switches while the task is queued or being written.
func (s *RequestStatistics) enqueueEventWriteLocked(task eventWriteTask) bool {
	if s == nil || task.store == nil || s.eventWriterQueue == nil || s.eventWriterStopping {
		return false
	}
	if task.enqueuedAt.IsZero() {
		task.enqueuedAt = time.Now()
	}
	s.storageWriteWG.Add(1)
	s.storageQueryWG.Add(1)
	select {
	case s.eventWriterQueue <- task:
		s.mu.Lock()
		s.eventWriterQueueLength = len(s.eventWriterQueue)
		s.mu.Unlock()
		return true
	default:
		s.storageWriteWG.Done()
		s.storageQueryWG.Done()
		return false
	}
}

func (s *RequestStatistics) stopEventWriter() {
	if s == nil {
		return
	}
	s.storageControlMu.Lock()
	done := s.eventWriterDone
	if done == nil {
		s.storageControlMu.Unlock()
		return
	}
	if !s.eventWriterStopping {
		s.eventWriterStopping = true
		close(s.eventWriterStop)
	}
	s.storageControlMu.Unlock()

	<-done

	s.storageControlMu.Lock()
	if s.eventWriterDone == done {
		s.eventWriterQueue = nil
		s.eventWriterStop = nil
		s.eventWriterDone = nil
		s.eventWriterStopping = false
		s.eventWriterRunning = false
		s.eventWriterQueueLength = 0
	}
	s.storageControlMu.Unlock()
}

func (s *RequestStatistics) eventWriterLoop(queue <-chan eventWriteTask, stop <-chan struct{}, done chan<- struct{}) {
	state := &eventWriterState{lastSnapshot: time.Now()}
	defer close(done)
	defer func() {
		s.mu.Lock()
		s.eventWriterRunning = false
		s.eventWriterQueueLength = 0
		s.mu.Unlock()
	}()

	for {
		select {
		case first := <-queue:
			batch := collectEventWriteBatch(queue, first)
			s.updateEventWriterQueueLength(len(queue))
			s.processEventWriteBatch(state, batch)
		case <-stop:
			for {
				select {
				case first := <-queue:
					batch := collectEventWriteBatch(queue, first)
					s.updateEventWriterQueueLength(len(queue))
					s.processEventWriteBatch(state, batch)
				default:
					s.saveFinalEventWriterSnapshot(state)
					return
				}
			}
		}
	}
}

func collectEventWriteBatch(queue <-chan eventWriteTask, first eventWriteTask) []eventWriteTask {
	batch := make([]eventWriteTask, 0, defaultStorageWriteBatchSize)
	batch = append(batch, first)
	for len(batch) < defaultStorageWriteBatchSize {
		select {
		case task := <-queue:
			batch = append(batch, task)
		default:
			return batch
		}
	}
	return batch
}

func (s *RequestStatistics) updateEventWriterQueueLength(length int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.eventWriterQueueLength = length
	s.mu.Unlock()
}

func (s *RequestStatistics) processEventWriteBatch(state *eventWriterState, batch []eventWriteTask) {
	if s == nil || len(batch) == 0 {
		return
	}
	groups := make([][]eventWriteTask, 0, 1)
	groupStores := make([]*eventStore, 0, 1)
	groupIndex := make(map[*eventStore]int, 1)
	for _, task := range batch {
		if task.store == nil {
			s.finishEventWriteTask()
			continue
		}
		index, ok := groupIndex[task.store]
		if !ok {
			index = len(groups)
			groupIndex[task.store] = index
			groupStores = append(groupStores, task.store)
			groups = append(groups, nil)
		}
		groups[index] = append(groups[index], task)
	}
	for index, tasks := range groups {
		writeErr := s.persistEventWriteGroup(state, groupStores[index], tasks)
		if writeErr != nil {
			s.recordEventStoreFailure(writeErr, true)
		}
		for _, task := range tasks {
			if task.replay != nil {
				task.replay.complete(s, task, writeErr == nil)
			} else if writeErr != nil {
				s.spoolEventWrite(task)
			}
			s.finishEventWriteTask()
		}
	}
}

func (s *RequestStatistics) finishEventWriteTask() {
	if s == nil {
		return
	}
	s.storageWriteWG.Done()
	s.storageQueryWG.Done()
}

func (s *RequestStatistics) persistEventWriteGroup(state *eventWriterState, store *eventStore, tasks []eventWriteTask) error {
	if s == nil || store == nil || len(tasks) == 0 {
		return nil
	}
	db, err := store.database()
	if err != nil {
		return err
	}
	ctx, cancel := eventStoreContext(context.Background(), eventStoreBulkWriteTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event batch: %w", err)
	}
	defer tx.Rollback()

	requests := make([]eventInsertRequest, 0, len(tasks))
	for _, task := range tasks {
		requests = append(requests, eventInsertRequest{
			Row:         eventRowFromDetail(task.apiName, task.modelName, task.detail),
			Fingerprint: task.fingerprint,
			Exact:       task.exact,
		})
	}
	inserted, err := store.insertEventsTx(ctx, tx, requests)
	if err != nil {
		return fmt.Errorf("insert event batch: %w", err)
	}

	s.mu.RLock()
	maxDetails := s.maxDetailsPerModel
	retention := s.retention
	lastRetentionCutoff := s.eventStoreLastRetentionCutoff
	s.mu.RUnlock()
	if maxDetails < 0 {
		maxDetails = tasks[0].maxDetails
	}
	if retention < 0 {
		retention = tasks[0].retention
	}
	pruneNow := time.Now()
	retentionCutoff := retentionPruneCutoff(pruneNow, retention)
	retentionDue := retentionCutoff > 0 && (lastRetentionCutoff == 0 || retentionCutoff > lastRetentionCutoff)
	var retentionRemoved []RequestDetail
	var removedCount int64
	var addedCount int64
	var lastEventID int64
	for index, task := range tasks {
		if index < len(inserted) && inserted[index].Added {
			addedCount++
		}
		if index < len(inserted) && inserted[index].ID > lastEventID {
			lastEventID = inserted[index].ID
		}
		pruned, pruneErr := store.pruneTxScoped(ctx, tx, maxDetails, retention, pruneNow, eventPruneScope{
			API:            task.apiName,
			Model:          task.modelName,
			ApplyRetention: retentionDue && index == 0,
		})
		if pruneErr != nil {
			return fmt.Errorf("prune event batch: %w", pruneErr)
		}
		removedCount += pruned.Removed
		retentionRemoved = append(retentionRemoved, pruned.RetentionDetails...)
	}

	previousEvictedTotal := int64(0)
	previousLastRecordedAt := time.Time{}
	previousSummaryVersion := uint64(0)
	previousLastEventID := int64(0)
	var aggregate *StatisticsSnapshot
	s.eventRecordMu.Lock()
	s.mu.Lock()
	previousEvictedTotal = s.evictedTotal
	previousLastRecordedAt = s.lastRecordedAt
	previousSummaryVersion = s.summaryVersion
	previousLastEventID = s.eventStoreLastEventID
	s.applyRetentionRemovalsLocked(retentionRemoved)
	if removedCount > 0 {
		s.evictedTotal += removedCount
	}
	shouldSnapshot := state != nil && (state.forceSnapshot ||
		(state.snapshotRecords+addedCount >= int64(defaultStorageSnapshotRecords)) ||
		(!state.lastSnapshot.IsZero() && time.Since(state.lastSnapshot) >= time.Duration(defaultStorageSnapshotSeconds)*time.Second))
	queueEmpty := s.eventWriterQueue == nil || len(s.eventWriterQueue) == 0
	spoolEmpty := s.eventWriterSpoolPending <= 0
	if shouldSnapshot && queueEmpty && spoolEmpty && s.eventStore == store {
		snapshot := s.aggregateSnapshotLocked()
		aggregate = &snapshot
	}
	s.mu.Unlock()
	s.eventRecordMu.Unlock()

	rollback := func() {
		s.eventRecordMu.Lock()
		s.mu.Lock()
		for _, removed := range retentionRemoved {
			s.recordDetailLocked(strings.TrimSpace(removed.UpstreamAPI), normalizeModelName(removed.Model), removed, requestDedupKey{}, time.Now(), false, false)
		}
		s.evictedTotal = previousEvictedTotal
		s.lastRecordedAt = previousLastRecordedAt
		s.summaryVersion = previousSummaryVersion
		s.mu.Unlock()
		s.eventRecordMu.Unlock()
	}
	if aggregate != nil {
		watermark := previousLastEventID
		if lastEventID > watermark {
			watermark = lastEventID
		}
		if err := store.saveAggregateTxWithWatermark(ctx, tx, *aggregate, watermark); err != nil {
			rollback()
			return fmt.Errorf("save event aggregate: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return fmt.Errorf("commit event batch: %w", err)
	}

	finished := time.Now()
	s.mu.Lock()
	s.eventStoreEventCount += addedCount - removedCount
	if s.eventStoreEventCount < 0 {
		s.eventStoreEventCount = 0
	}
	if retentionDue {
		s.eventStoreLastRetentionCutoff = retentionCutoff
	}
	if lastEventID > s.eventStoreLastEventID {
		s.eventStoreLastEventID = lastEventID
	}
	s.eventStoreLastError = ""
	s.eventStoreLastWrite = finished
	s.eventStoreSizeBytes = eventStoreFileSize(store.path)
	s.mu.Unlock()
	if state != nil {
		state.snapshotRecords += addedCount
		if len(retentionRemoved) > 0 {
			state.forceSnapshot = true
		}
		if aggregate != nil {
			state.snapshotRecords = 0
			state.lastSnapshot = finished
			state.forceSnapshot = false
		}
	}
	return nil
}

func (s *RequestStatistics) saveFinalEventWriterSnapshot(state *eventWriterState) {
	if s == nil {
		return
	}
	s.eventRecordMu.Lock()
	s.storageControlMu.Lock()
	s.mu.RLock()
	store := s.eventStore
	s.mu.RUnlock()
	if store == nil || s.eventWriterSpoolPending > 0 || (s.eventWriterQueue != nil && len(s.eventWriterQueue) > 0) {
		s.storageControlMu.Unlock()
		s.eventRecordMu.Unlock()
		return
	}
	s.mu.Lock()
	snapshot := s.aggregateSnapshotLocked()
	watermark := s.eventStoreLastEventID
	s.mu.Unlock()
	ctx, cancel := eventStoreContext(context.Background(), eventStoreBulkWriteTimeout)
	err := store.saveAggregateWithWatermark(ctx, snapshot, watermark)
	cancel()
	if err != nil {
		s.recordEventStoreFailure(err, true)
	} else {
		now := time.Now()
		s.mu.Lock()
		s.eventWriterSnapshotRecords = 0
		s.eventWriterLastSnapshot = now
		s.mu.Unlock()
		if state != nil {
			state.snapshotRecords = 0
			state.lastSnapshot = now
			state.forceSnapshot = false
		}
	}
	s.storageControlMu.Unlock()
	s.eventRecordMu.Unlock()
}

func (s *RequestStatistics) spoolEventWrite(task eventWriteTask) {
	if s == nil || task.store == nil {
		return
	}
	payload := persistedDetail{API: task.apiName, Model: task.modelName, Detail: task.detail}
	raw, err := json.Marshal(payload)
	if err != nil {
		s.recordEventStoreFailure(fmt.Errorf("encode event spool record: %w", err), true)
		return
	}
	lineBytes := int64(len(raw) + 1)
	path := eventSpoolPath(task.store.path)
	s.eventSpoolMu.Lock()
	defer s.eventSpoolMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.recordEventStoreFailure(fmt.Errorf("create event spool directory: %w", err), true)
		return
	}
	currentSize := int64(0)
	if info, statErr := os.Stat(path); statErr == nil {
		currentSize = info.Size()
	} else if !os.IsNotExist(statErr) {
		s.recordEventStoreFailure(fmt.Errorf("stat event spool: %w", statErr), true)
		return
	}
	if currentSize+lineBytes > eventSpoolMaxBytes {
		s.recordEventStoreFailure(fmt.Errorf("event spool limit reached"), true)
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.recordEventStoreFailure(fmt.Errorf("open event spool: %w", err), true)
		return
	}
	_, writeErr := file.Write(append(raw, '\n'))
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		s.recordEventStoreFailure(fmt.Errorf("write event spool: %w", writeErr), true)
		return
	}
	if closeErr != nil {
		s.recordEventStoreFailure(fmt.Errorf("close event spool: %w", closeErr), true)
		return
	}
	s.mu.Lock()
	s.eventWriterSpoolPending++
	s.mu.Unlock()
	s.scheduleEventSpoolReplay(task.store)
}

func eventSpoolPath(storePath string) string {
	return strings.TrimSpace(storePath) + ".spool.jsonl"
}

func eventSpoolReplayPath(storePath string) string {
	return eventSpoolPath(storePath) + ".replay"
}

func (s *RequestStatistics) scheduleEventSpoolReplay(store *eventStore) {
	if s == nil || store == nil {
		return
	}
	s.eventSpoolRetryMu.Lock()
	if s.eventSpoolRetryRunning {
		s.eventSpoolRetryMu.Unlock()
		return
	}
	s.eventSpoolRetryRunning = true
	s.eventSpoolRetryMu.Unlock()
	go func() {
		defer func() {
			s.eventSpoolRetryMu.Lock()
			s.eventSpoolRetryRunning = false
			s.eventSpoolRetryMu.Unlock()
		}()
		for {
			if s.eventWriterIsStopping() {
				return
			}
			s.replayEventSpool(store, false)
			if !s.eventSpoolFilesExist(store.path) {
				return
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-s.eventWriterStopSignal():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
	}()
}

func (s *RequestStatistics) eventWriterIsStopping() bool {
	if s == nil {
		return true
	}
	s.storageControlMu.Lock()
	stopping := s.eventWriterStopping || s.eventWriterQueue == nil
	s.storageControlMu.Unlock()
	return stopping
}

func (s *RequestStatistics) eventWriterStopSignal() <-chan struct{} {
	if s == nil {
		return nil
	}
	s.storageControlMu.Lock()
	stop := s.eventWriterStop
	s.storageControlMu.Unlock()
	if stop == nil {
		return make(chan struct{})
	}
	return stop
}

func (s *RequestStatistics) eventSpoolFilesExist(storePath string) bool {
	if strings.TrimSpace(storePath) == "" {
		return false
	}
	s.eventSpoolMu.Lock()
	defer s.eventSpoolMu.Unlock()
	for _, path := range []string{eventSpoolPath(storePath), eventSpoolReplayPath(storePath)} {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return true
		}
	}
	return false
}

func (s *RequestStatistics) replayEventSpool(store *eventStore, restoreMemory bool) {
	if s == nil || store == nil || strings.TrimSpace(store.path) == "" {
		return
	}
	path := eventSpoolPath(store.path)
	replayPath := eventSpoolReplayPath(store.path)
	s.eventSpoolMu.Lock()
	if _, err := os.Stat(replayPath); os.IsNotExist(err) {
		if renameErr := os.Rename(path, replayPath); renameErr != nil && !os.IsNotExist(renameErr) {
			s.eventSpoolMu.Unlock()
			s.recordEventStoreFailure(fmt.Errorf("rotate event spool for replay: %w", renameErr), true)
			return
		}
	} else if err == nil {
		if active, readErr := os.ReadFile(path); readErr == nil && len(active) > 0 {
			if file, openErr := os.OpenFile(replayPath, os.O_APPEND|os.O_WRONLY, 0o600); openErr == nil {
				_, _ = file.Write(active)
				_ = file.Close()
				_ = os.Remove(path)
			}
		}
	}
	s.eventSpoolMu.Unlock()

	file, err := os.Open(replayPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		s.recordEventStoreFailure(fmt.Errorf("open event spool replay: %w", err), true)
		return
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 32<<10), 2<<20)
	records := make([]persistedDetail, 0)
	for scanner.Scan() {
		var persisted persistedDetail
		if err := json.Unmarshal(scanner.Bytes(), &persisted); err != nil {
			continue
		}
		if strings.TrimSpace(persisted.API) == "" || strings.TrimSpace(persisted.Model) == "" {
			continue
		}
		records = append(records, persisted)
	}
	scanErr := scanner.Err()
	_ = file.Close()
	if scanErr != nil {
		s.recordEventStoreFailure(fmt.Errorf("read event spool replay: %w", scanErr), true)
		return
	}
	if len(records) == 0 {
		_ = os.Remove(replayPath)
		return
	}

	replay := &eventSpoolReplay{path: replayPath, remaining: len(records)}
	s.mu.Lock()
	if restoreMemory {
		s.eventWriterSpoolPending = int64(len(records))
	}
	s.mu.Unlock()
	for _, persisted := range records {
		for {
			if s.eventWriterIsStopping() {
				return
			}
			s.eventRecordMu.Lock()
			s.storageControlMu.Lock()
			s.mu.RLock()
			currentStore := s.eventStore
			queue := s.eventWriterQueue
			stopping := s.eventWriterStopping
			s.mu.RUnlock()
			queued := false
			if currentStore == store && !stopping && queue != nil && len(queue) < cap(queue) {
				if restoreMemory {
					s.mu.Lock()
					s.recordDetailLocked(persisted.API, persisted.Model, persisted.Detail, requestDedupKey{}, time.Now(), false, false)
					s.mu.Unlock()
				}
				queued = s.enqueueEventWriteLocked(eventWriteTask{
					store:       store,
					apiName:     persisted.API,
					modelName:   persisted.Model,
					detail:      persisted.Detail,
					enqueuedAt:  time.Now(),
					fingerprint: eventFingerprint(persisted.API, persisted.Model, persisted.Detail),
					exact:       true,
					replay:      replay,
				})
			}
			s.storageControlMu.Unlock()
			s.eventRecordMu.Unlock()
			if queued {
				break
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-timer.C:
			case <-s.eventWriterStopSignal():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
	}
}

func (r *eventSpoolReplay) complete(s *RequestStatistics, task eventWriteTask, success bool) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	if !success {
		r.failed = append(r.failed, persistedDetail{API: task.apiName, Model: task.modelName, Detail: task.detail})
	}
	r.remaining--
	done := r.remaining <= 0
	failed := append([]persistedDetail(nil), r.failed...)
	r.mu.Unlock()
	if success {
		s.mu.Lock()
		if s.eventWriterSpoolPending > 0 {
			s.eventWriterSpoolPending--
		}
		s.mu.Unlock()
	}
	if !done {
		return
	}
	if len(failed) == 0 {
		_ = os.Remove(r.path)
		return
	}
	_ = writeEventSpoolFile(r.path, failed)
}

func writeEventSpoolFile(path string, records []persistedDetail) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			_ = file.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
