package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	eventSpoolMaxBytes                int64 = 64 << 20
	eventSpoolMaxLineBytes            int   = 16 << 20
	eventRetentionMaintenanceInterval       = time.Minute
	eventWriteBatchMaxWait                  = 20 * time.Millisecond
	eventWriteBatchMaxSize                  = 128
	eventStoreSizeCheckInterval             = 30 * time.Second
	// Count queued and currently-processing tasks so a large detail cannot
	// turn the writer channel into an unbounded heap anchor.
	eventWriterQueueMaxBytes int64 = 16 << 20
	eventSpoolQueueMaxBytes  int64 = 16 << 20
)

// eventWriteTask is the smallest unit accepted by the SQLite writer. The
// RequestDetail has already been normalized and the in-memory aggregate has
// already been published before a task enters this queue.
type eventWriteTask struct {
	store         *eventStore
	apiName       string
	modelName     string
	detail        RequestDetail
	enqueuedAt    time.Time
	maxDetails    int
	retention     time.Duration
	fingerprint   string
	exact         bool
	replay        *eventSpoolReplay
	retainedBytes int64
}

type eventSpoolReplay struct {
	mu            sync.Mutex
	path          string
	store         *eventStore
	remaining     int
	readComplete  bool
	failedFile    *os.File
	failedPath    string
	failedBytes   int64
	failedRecords int
	failureErr    error
	aborted       bool
	finalized     bool
}

type eventWriterState struct {
	snapshotRecords int64
	lastSnapshot    time.Time
	forceSnapshot   bool
	lastSizeCheck   time.Time
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
	s.eventWriterQueueBytes.Store(0)
	s.eventSpoolQueueBytes.Store(0)
	spoolQueue := make(chan eventWriteTask, capacity)
	spoolStop := make(chan struct{})
	spoolDone := make(chan struct{})
	s.eventSpoolQueue = spoolQueue
	s.eventSpoolStop = spoolStop
	s.eventSpoolDone = spoolDone
	s.eventSpoolQueueCapacity = capacity
	s.eventSpoolQueueLength = 0
	go s.eventWriterLoop(queue, stop, done)
	go s.eventSpoolLoop(spoolQueue, spoolStop, spoolDone)
	go s.replayEventSpool(s.eventStore, true)
}

// enqueueEventWriteLocked is non-blocking and must be called with
// storageControlMu held. The wait groups protect the referenced SQLite store
// from configuration switches while the task is queued or being written.
func (s *RequestStatistics) enqueueEventWriteLocked(task eventWriteTask) bool {
	if s == nil || task.store == nil || s.eventWriterQueue == nil || s.eventWriterStopping {
		return false
	}
	if task.retainedBytes <= 0 {
		task.retainedBytes = eventWriteTaskRetainedBytes(task)
	}
	if !reserveEventQueueBytes(&s.eventWriterQueueBytes, task.retainedBytes, eventWriterQueueMaxBytes) {
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
		releaseEventQueueBytes(&s.eventWriterQueueBytes, task.retainedBytes)
		return false
	}
}

func (s *RequestStatistics) stopEventWriter() {
	if s == nil {
		return
	}
	s.storageControlMu.Lock()
	done := s.eventWriterDone
	spoolDone := s.eventSpoolDone
	if done == nil {
		s.storageControlMu.Unlock()
		return
	}
	if !s.eventWriterStopping {
		s.eventWriterStopping = true
		close(s.eventWriterStop)
		if spoolDone != nil && s.eventSpoolStop != nil {
			close(s.eventSpoolStop)
		}
	}
	s.storageControlMu.Unlock()

	<-done
	if spoolDone != nil {
		<-spoolDone
	}

	s.storageControlMu.Lock()
	if s.eventWriterDone == done {
		s.eventWriterQueue = nil
		s.eventWriterStop = nil
		s.eventWriterDone = nil
		s.eventWriterStopping = false
		s.eventSpoolQueue = nil
		s.eventSpoolStop = nil
		s.eventSpoolDone = nil
		s.mu.Lock()
		s.eventWriterRunning = false
		s.eventWriterQueueLength = 0
		s.eventSpoolQueueLength = 0
		s.mu.Unlock()
		s.eventWriterQueueBytes.Store(0)
		s.eventSpoolQueueBytes.Store(0)
	}
	s.storageControlMu.Unlock()
}

func (s *RequestStatistics) eventWriterLoop(queue <-chan eventWriteTask, stop <-chan struct{}, done chan<- struct{}) {
	state := &eventWriterState{lastSnapshot: time.Now()}
	maintenance := time.NewTicker(eventRetentionMaintenanceInterval)
	defer maintenance.Stop()
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
			batch := collectEventWriteBatchUntil(queue, first, eventWriteBatchMaxWait, eventWriteBatchMaxSize)
			s.updateEventWriterQueueLength(len(queue))
			s.processEventWriteBatch(state, batch)
			s.releaseEventWriterTaskBytes(batch)
		case <-maintenance.C:
			s.runEventRetentionMaintenance()
		case <-stop:
			for {
				select {
				case first := <-queue:
					batch := collectEventWriteBatchUntil(queue, first, 0, eventWriteBatchMaxSize)
					s.updateEventWriterQueueLength(len(queue))
					s.processEventWriteBatch(state, batch)
					s.releaseEventWriterTaskBytes(batch)
				default:
					s.saveFinalEventWriterSnapshot(state)
					return
				}
			}
		}
	}
}

// runEventRetentionMaintenance keeps retention enforcement independent from
// request traffic. It takes the storage control lock while pruning so a
// concurrent storage-path switch cannot close the store mid-transaction.
func (s *RequestStatistics) runEventRetentionMaintenance() {
	if s == nil {
		return
	}
	s.storageControlMu.Lock()
	defer s.storageControlMu.Unlock()
	s.mu.RLock()
	store := s.eventStore
	maxDetails := s.maxDetailsPerModel
	retention := s.retention
	lastCutoff := s.eventStoreLastRetentionCutoff
	s.mu.RUnlock()
	if store == nil || retention <= 0 {
		return
	}
	cutoff := retentionPruneCutoff(time.Now(), retention)
	if cutoff <= 0 || cutoff <= lastCutoff {
		return
	}
	if err := s.pruneSQLite(store, maxDetails, retention, time.Now()); err != nil {
		s.mu.Lock()
		s.eventStoreLastError = err.Error()
		s.mu.Unlock()
	}
}

func collectEventWriteBatch(queue <-chan eventWriteTask, first eventWriteTask) []eventWriteTask {
	return collectEventWriteBatchUntil(queue, first, eventWriteBatchMaxWait, eventWriteBatchMaxSize)
}

func collectEventWriteBatchUntil(queue <-chan eventWriteTask, first eventWriteTask, maxWait time.Duration, maxSize int) []eventWriteTask {
	if maxSize <= 0 {
		maxSize = eventWriteBatchMaxSize
	}
	batch := make([]eventWriteTask, 0, maxSize)
	batch = append(batch, first)
	if len(batch) >= maxSize {
		return batch
	}
	if maxWait <= 0 {
		for len(batch) < maxSize {
			select {
			case task := <-queue:
				batch = append(batch, task)
			default:
				return batch
			}
		}
		return batch
	}
	timer := time.NewTimer(maxWait)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	for len(batch) < maxSize {
		select {
		case task := <-queue:
			batch = append(batch, task)
		case <-timer.C:
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

func (s *RequestStatistics) eventSpoolLoop(queue <-chan eventWriteTask, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case task := <-queue:
			s.updateEventSpoolQueueLength(len(queue))
			s.writeEventSpoolNow(task)
			s.releaseEventSpoolTaskBytes(task)
		case <-stop:
			for {
				select {
				case task := <-queue:
					s.updateEventSpoolQueueLength(len(queue))
					s.writeEventSpoolNow(task)
					s.releaseEventSpoolTaskBytes(task)
				default:
					return
				}
			}
		}
	}
}

func (s *RequestStatistics) updateEventSpoolQueueLength(length int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.eventSpoolQueueLength = length
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
			s.recordStorageWriteFailure(writeErr)
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
	for index := range tasks {
		if index < len(inserted) && inserted[index].Added {
			addedCount++
		}
		if index < len(inserted) && inserted[index].ID > lastEventID {
			lastEventID = inserted[index].ID
		}
	}
	if retentionDue {
		pruned, pruneErr := store.pruneTxScoped(ctx, tx, maxDetails, retention, pruneNow, eventPruneScope{
			ApplyRetention: true,
		})
		if pruneErr != nil {
			return fmt.Errorf("prune event batch: %w", pruneErr)
		}
		removedCount += pruned.Removed
		retentionRemoved = append(retentionRemoved, pruned.RetentionDetails...)
	}
	if maxDetails >= 0 && retention <= 0 {
		type eventScopeKey struct {
			api   string
			model string
		}
		scopes := make(map[eventScopeKey]struct{}, len(tasks))
		for _, task := range tasks {
			scopes[eventScopeKey{api: task.apiName, model: task.modelName}] = struct{}{}
		}
		for scope := range scopes {
			pruned, pruneErr := store.pruneTxScoped(ctx, tx, maxDetails, retention, pruneNow, eventPruneScope{
				API:   scope.api,
				Model: scope.model,
			})
			if pruneErr != nil {
				return fmt.Errorf("prune event batch: %w", pruneErr)
			}
			removedCount += pruned.Removed
			retentionRemoved = append(retentionRemoved, pruned.RetentionDetails...)
		}
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
	if state == nil || state.lastSizeCheck.IsZero() || finished.Sub(state.lastSizeCheck) >= eventStoreSizeCheckInterval {
		s.eventStoreSizeBytes = eventStoreFileSize(store.path)
		if state != nil {
			state.lastSizeCheck = finished
		}
	}
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
	s.storageControlMu.Lock()
	s.eventRecordMu.Lock()
	s.mu.RLock()
	store := s.eventStore
	spoolPending := s.eventWriterSpoolPending
	queueEmpty := s.eventWriterQueue == nil || len(s.eventWriterQueue) == 0
	s.mu.RUnlock()
	if store == nil || spoolPending > 0 || !queueEmpty {
		s.eventRecordMu.Unlock()
		s.storageControlMu.Unlock()
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
		s.recordEventStoreFailure(err, false)
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
	s.eventRecordMu.Unlock()
	s.storageControlMu.Unlock()
}

// tryEnqueueEventSpool is the callback-safe spool handoff. It only attempts
// to put a compact event task on the worker queue and never performs file I/O
// or waits for storageControlMu. The caller owns the explicit drop decision
// when the bounded handoff is unavailable.
func (s *RequestStatistics) tryEnqueueEventSpool(task eventWriteTask) bool {
	if s == nil || task.store == nil || !s.storageControlMu.TryLock() {
		return false
	}
	queue := s.eventSpoolQueue
	stop := s.eventSpoolStop
	stopping := s.eventWriterStopping
	if queue == nil || stopping {
		s.storageControlMu.Unlock()
		return false
	}
	if task.retainedBytes <= 0 {
		task.retainedBytes = eventWriteTaskRetainedBytes(task)
	}
	if !reserveEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes, eventSpoolQueueMaxBytes) {
		s.storageControlMu.Unlock()
		return false
	}
	s.mu.Lock()
	s.eventWriterSpoolPending++
	s.mu.Unlock()
	select {
	case queue <- task:
		s.storageControlMu.Unlock()
		s.updateEventSpoolQueueLength(len(queue))
		return true
	case <-stop:
		releaseEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes)
		s.storageControlMu.Unlock()
		s.finishSpoolEvent(false)
		return false
	default:
		releaseEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes)
		s.storageControlMu.Unlock()
		s.finishSpoolEvent(false)
		return false
	}
}

func (s *RequestStatistics) spoolEventWrite(task eventWriteTask) {
	if s == nil || task.store == nil {
		return
	}
	// A storage-path switch holds storageControlMu while it drains the old
	// writer queue. If the old write failed, waiting for that same lock here
	// would deadlock the switch before the task can be marked complete. The
	// synchronous fallback still runs on the writer goroutine, never on the
	// API callback goroutine, and preserves the event durably.
	if s.storageControlMu.TryLock() {
		queue := s.eventSpoolQueue
		stop := s.eventSpoolStop
		stopping := s.eventWriterStopping
		if queue != nil && !stopping {
			if task.retainedBytes <= 0 {
				task.retainedBytes = eventWriteTaskRetainedBytes(task)
			}
			if reserveEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes, eventSpoolQueueMaxBytes) {
				s.mu.Lock()
				s.eventWriterSpoolPending++
				s.mu.Unlock()
				select {
				case queue <- task:
					s.storageControlMu.Unlock()
					s.updateEventSpoolQueueLength(len(queue))
					return
				case <-stop:
					releaseEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes)
					s.finishSpoolEvent(false)
				}
			}
		}
		s.storageControlMu.Unlock()
	}
	// This path is limited to shutdown or a spool-worker failure. It is kept
	// for durability, while the normal callback overflow path is always
	// handled by eventSpoolLoop above.
	s.mu.Lock()
	s.eventWriterSpoolPending++
	s.mu.Unlock()
	s.writeEventSpoolNow(task)
}

func (s *RequestStatistics) writeEventSpoolNow(task eventWriteTask) {
	if s == nil || task.store == nil {
		return
	}
	payload := persistedDetail{API: task.apiName, Model: task.modelName, Detail: task.detail}
	raw, err := json.Marshal(payload)
	if err != nil {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("encode event spool record: %w", err))
		return
	}
	lineBytes := int64(len(raw) + 1)
	if lineBytes > int64(eventSpoolMaxLineBytes) {
		s.finishSpoolEvent(false)
		s.recordSpoolLimitDrop(fmt.Errorf("event spool record exceeds %d bytes", eventSpoolMaxLineBytes))
		return
	}
	path := eventSpoolPath(task.store.path)
	s.eventSpoolMu.Lock()
	defer s.eventSpoolMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("create event spool directory: %w", err))
		return
	}
	currentSize := int64(0)
	if info, statErr := os.Stat(path); statErr == nil {
		currentSize = info.Size()
	} else if !os.IsNotExist(statErr) {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("stat event spool: %w", statErr))
		return
	}
	if currentSize+lineBytes > eventSpoolMaxBytes {
		s.finishSpoolEvent(false)
		s.recordSpoolLimitDrop(fmt.Errorf("event spool limit reached"))
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("open event spool: %w", err))
		return
	}
	written, writeErr := file.Write(append(raw, '\n'))
	if writeErr == nil && written != len(raw)+1 {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("write event spool: %w", writeErr))
		return
	}
	if closeErr != nil {
		s.finishSpoolEvent(false)
		s.recordPermanentDrop(fmt.Errorf("close event spool: %w", closeErr))
		return
	}
	s.recordStorageSpooled()
	// Once the record is durably appended to the spool file it is no longer
	// queued work. Replay accounts for the on-disk record when it is scanned.
	s.finishSpoolEvents(1)
	s.scheduleEventSpoolReplay(task.store)
}

func (s *RequestStatistics) finishSpoolEvent(success bool) {
	if s == nil || success {
		return
	}
	s.finishSpoolEvents(1)
}

func (s *RequestStatistics) finishSpoolEvents(count int64) {
	if s == nil || count <= 0 {
		return
	}
	s.mu.Lock()
	if count >= s.eventWriterSpoolPending {
		s.eventWriterSpoolPending = 0
	} else {
		s.eventWriterSpoolPending -= count
	}
	s.mu.Unlock()
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
	mergeErr := mergeEventSpoolFiles(path, replayPath)
	s.eventSpoolMu.Unlock()
	if mergeErr != nil {
		s.recordStorageWriteFailure(fmt.Errorf("rotate event spool for replay: %w", mergeErr))
	}

	file, err := os.Open(replayPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		s.recordEventStoreFailure(fmt.Errorf("open event spool replay: %w", err), false)
		return
	}
	if closeErr := file.Close(); closeErr != nil {
		s.recordStorageWriteFailure(fmt.Errorf("close event spool replay: %w", closeErr))
		return
	}
	validRecords, invalidRecords, scanErr := scanEventSpoolFile(replayPath, nil)
	if scanErr != nil {
		s.recordStorageWriteFailure(fmt.Errorf("read event spool replay: %w", scanErr))
		return
	}
	if invalidRecords > 0 {
		compactedRecords, compactErr := compactEventSpoolFile(replayPath)
		if compactErr != nil {
			s.recordStorageWriteFailure(fmt.Errorf("compact invalid event spool records: %w", compactErr))
			return
		}
		validRecords = compactedRecords
		s.recordPermanentDrops(invalidRecords, fmt.Errorf("discarded %d invalid event spool records", invalidRecords))
	}
	if validRecords == 0 {
		if removeErr := os.Remove(replayPath); removeErr != nil && !os.IsNotExist(removeErr) {
			s.recordStorageWriteFailure(fmt.Errorf("remove empty event spool replay: %w", removeErr))
		}
		return
	}

	replay := &eventSpoolReplay{path: replayPath, store: store, remaining: validRecords}
	s.mu.Lock()
	s.eventWriterSpoolPending += int64(validRecords)
	s.mu.Unlock()
	queuedRecords := 0
	_, _, scanErr = scanEventSpoolFile(replayPath, func(persisted persistedDetail) error {
		for {
			if s.eventWriterIsStopping() {
				return errEventSpoolReplayStopped
			}
			s.storageControlMu.Lock()
			s.eventRecordMu.Lock()
			s.mu.RLock()
			currentStore := s.eventStore
			queue := s.eventWriterQueue
			stopping := s.eventWriterStopping
			s.mu.RUnlock()
			queued := false
			if currentStore == store && !stopping && queue != nil && len(queue) < cap(queue) {
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
				if queued && restoreMemory {
					s.mu.Lock()
					s.recordDetailLocked(persisted.API, persisted.Model, persisted.Detail, requestDedupKey{}, time.Now(), false, false)
					s.mu.Unlock()
				}
			}
			s.eventRecordMu.Unlock()
			s.storageControlMu.Unlock()
			if queued {
				queuedRecords++
				return nil
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
				return errEventSpoolReplayStopped
			}
		}
	})
	if scanErr != nil {
		if !errors.Is(scanErr, errEventSpoolReplayStopped) {
			s.recordStorageWriteFailure(fmt.Errorf("read event spool replay: %w", scanErr))
		}
		replay.cancelUnqueued(s, validRecords-queuedRecords)
		return
	}
	replay.markReadComplete(s)
}

var errEventSpoolReplayStopped = errors.New("event spool replay stopped")

func scanEventSpoolFile(path string, visit func(persistedDetail) error) (int, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	if info.Size() > eventSpoolMaxBytes {
		return 0, 0, fmt.Errorf("event spool file exceeds %d bytes", eventSpoolMaxBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	scanner := bufio.NewScanner(file)
	// The configured line limit includes the trailing newline written by the
	// spool writer, while Scanner's token excludes it.
	scanner.Buffer(make([]byte, 32<<10), eventSpoolMaxLineBytes+1)
	validRecords := 0
	var invalidRecords int64
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var persisted persistedDetail
		if err := json.Unmarshal(line, &persisted); err != nil || strings.TrimSpace(persisted.API) == "" || strings.TrimSpace(persisted.Model) == "" {
			invalidRecords++
			continue
		}
		validRecords++
		if visit != nil {
			if err := visit(persisted); err != nil {
				closeErr := file.Close()
				if closeErr != nil {
					return validRecords, invalidRecords, errors.Join(err, fmt.Errorf("close event spool scan: %w", closeErr))
				}
				return validRecords, invalidRecords, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return validRecords, invalidRecords, errors.Join(err, fmt.Errorf("close event spool scan: %w", closeErr))
		}
		return validRecords, invalidRecords, err
	}
	if err := file.Close(); err != nil {
		return validRecords, invalidRecords, fmt.Errorf("close event spool scan: %w", err)
	}
	return validRecords, invalidRecords, nil
}

// compactEventSpoolFile removes invalid lines without materializing the valid
// records. The source remains intact until the normalized temporary file is
// synced, closed, and atomically renamed into place.
func compactEventSpoolFile(path string) (int, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".compact-*")
	if err != nil {
		return 0, fmt.Errorf("create compacted event spool: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func(cause error) (int, error) {
		closeErr := tmp.Close()
		removeErr := removeEventSpoolTemp(tmpPath)
		if closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close compacted event spool: %w", closeErr))
		}
		if removeErr != nil {
			cause = errors.Join(cause, removeErr)
		}
		return 0, cause
	}

	var writtenBytes int64
	var validRecords int
	_, _, scanErr := scanEventSpoolFile(path, func(record persistedDetail) error {
		raw, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return fmt.Errorf("encode compacted event spool record: %w", marshalErr)
		}
		lineBytes := int64(len(raw) + 1)
		if lineBytes > int64(eventSpoolMaxLineBytes) {
			return fmt.Errorf("compacted event spool record exceeds %d bytes", eventSpoolMaxLineBytes)
		}
		if writtenBytes+lineBytes > eventSpoolMaxBytes {
			return fmt.Errorf("compacted event spool exceeds %d bytes", eventSpoolMaxBytes)
		}
		written, writeErr := tmp.Write(append(raw, '\n'))
		if writeErr == nil && written != len(raw)+1 {
			writeErr = io.ErrShortWrite
		}
		if writeErr != nil {
			return fmt.Errorf("write compacted event spool: %w", writeErr)
		}
		writtenBytes += lineBytes
		validRecords++
		return nil
	})
	if scanErr != nil {
		return cleanup(fmt.Errorf("scan event spool for compaction: %w", scanErr))
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync compacted event spool: %w", err))
	}
	if err := tmp.Close(); err != nil {
		if removeErr := removeEventSpoolTemp(tmpPath); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return 0, fmt.Errorf("close compacted event spool: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if removeErr := removeEventSpoolTemp(tmpPath); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return 0, fmt.Errorf("replace compacted event spool: %w", err)
	}
	return validRecords, nil
}

func (r *eventSpoolReplay) complete(s *RequestStatistics, task eventWriteTask, success bool) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	if !success && r.failureErr == nil {
		if err := r.appendFailureLocked(task); err != nil {
			r.failureErr = err
		}
	}
	if r.remaining > 0 {
		r.remaining--
	}
	finalize := r.remaining <= 0 && r.readComplete && !r.finalized
	if finalize {
		r.finalized = true
	}
	r.mu.Unlock()
	s.finishSpoolEvents(1)
	if finalize {
		r.finalize(s)
	}
}

func (r *eventSpoolReplay) markReadComplete(s *RequestStatistics) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	r.readComplete = true
	finalize := r.remaining <= 0 && !r.finalized
	if finalize {
		r.finalized = true
	}
	r.mu.Unlock()
	if finalize {
		r.finalize(s)
	}
}

func (r *eventSpoolReplay) cancelUnqueued(s *RequestStatistics, count int) {
	if r == nil || s == nil || count <= 0 {
		return
	}
	r.mu.Lock()
	if count > r.remaining {
		count = r.remaining
	}
	r.remaining -= count
	// The scan will not enqueue any more records after this point. Keep the
	// original replay file authoritative, but let already queued tasks finish
	// before closing and removing the temporary failure stream.
	r.aborted = true
	r.readComplete = true
	finalize := r.remaining <= 0 && !r.finalized
	if finalize {
		r.finalized = true
	}
	r.mu.Unlock()
	s.finishSpoolEvents(int64(count))
	if finalize {
		r.finalize(s)
	}
}

func mergeEventSpoolFiles(activePath, replayPath string) error {
	replayInfo, replayErr := os.Stat(replayPath)
	if errors.Is(replayErr, os.ErrNotExist) {
		if err := os.Rename(activePath, replayPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if replayErr != nil {
		return replayErr
	}
	if replayInfo.Size() > eventSpoolMaxBytes {
		return fmt.Errorf("event spool replay exceeds %d bytes", eventSpoolMaxBytes)
	}
	activeInfo, activeErr := os.Stat(activePath)
	if errors.Is(activeErr, os.ErrNotExist) {
		return nil
	}
	if activeErr != nil {
		return activeErr
	}
	if activeInfo.Size() == 0 {
		if err := os.Remove(activePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove empty event spool: %w", err)
		}
		return nil
	}
	if activeInfo.Size() > eventSpoolMaxBytes || replayInfo.Size()+activeInfo.Size() > eventSpoolMaxBytes {
		return fmt.Errorf("merged event spool exceeds %d bytes", eventSpoolMaxBytes)
	}

	active, err := os.Open(activePath)
	if err != nil {
		return fmt.Errorf("open active event spool: %w", err)
	}
	replay, err := os.OpenFile(replayPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		if closeErr := active.Close(); closeErr != nil {
			return errors.Join(fmt.Errorf("open event spool replay for append: %w", err), fmt.Errorf("close active event spool: %w", closeErr))
		}
		return fmt.Errorf("open event spool replay for append: %w", err)
	}
	copied, copyErr := io.Copy(replay, active)
	syncErr := replay.Sync()
	activeCloseErr := active.Close()
	replayCloseErr := replay.Close()
	var errs []error
	if copyErr != nil {
		errs = append(errs, fmt.Errorf("copy active event spool: %w", copyErr))
	}
	if copied != activeInfo.Size() {
		errs = append(errs, fmt.Errorf("copy active event spool: copied %d of %d bytes", copied, activeInfo.Size()))
	}
	if syncErr != nil {
		errs = append(errs, fmt.Errorf("sync event spool replay: %w", syncErr))
	}
	if activeCloseErr != nil {
		errs = append(errs, fmt.Errorf("close active event spool: %w", activeCloseErr))
	}
	if replayCloseErr != nil {
		errs = append(errs, fmt.Errorf("close event spool replay: %w", replayCloseErr))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := os.Remove(activePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove merged active event spool: %w", err)
	}
	return nil
}

func (r *eventSpoolReplay) appendFailureLocked(task eventWriteTask) error {
	payload := persistedDetail{API: task.apiName, Model: task.modelName, Detail: task.detail}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode failed event spool record: %w", err)
	}
	lineBytes := int64(len(raw) + 1)
	if lineBytes > int64(eventSpoolMaxLineBytes) {
		return fmt.Errorf("failed event spool record exceeds %d bytes", eventSpoolMaxLineBytes)
	}
	if r.failedBytes+lineBytes > eventSpoolMaxBytes {
		return fmt.Errorf("failed event spool rewrite exceeds %d bytes", eventSpoolMaxBytes)
	}
	if r.failedFile == nil {
		file, err := os.CreateTemp(filepath.Dir(r.path), filepath.Base(r.path)+".failed-*")
		if err != nil {
			return fmt.Errorf("create failed event spool rewrite: %w", err)
		}
		r.failedFile = file
		r.failedPath = file.Name()
	}
	written, err := r.failedFile.Write(append(raw, '\n'))
	if err != nil {
		return fmt.Errorf("write failed event spool rewrite: %w", err)
	}
	if written != len(raw)+1 {
		return fmt.Errorf("write failed event spool rewrite: wrote %d of %d bytes", written, len(raw)+1)
	}
	r.failedBytes += lineBytes
	r.failedRecords++
	return nil
}

func (r *eventSpoolReplay) finalize(s *RequestStatistics) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	failureErr := r.failureErr
	failedFile := r.failedFile
	failedPath := r.failedPath
	failedRecords := r.failedRecords
	aborted := r.aborted
	r.failedFile = nil
	r.mu.Unlock()

	var finalizeErr error
	if failedFile != nil {
		if err := failedFile.Sync(); err != nil {
			finalizeErr = fmt.Errorf("sync failed event spool rewrite: %w", err)
		}
		if err := failedFile.Close(); err != nil {
			finalizeErr = errors.Join(finalizeErr, fmt.Errorf("close failed event spool rewrite: %w", err))
		}
	}
	if failureErr != nil {
		finalizeErr = errors.Join(finalizeErr, failureErr)
	}
	if finalizeErr != nil {
		if removeErr := removeEventSpoolTemp(failedPath); removeErr != nil {
			finalizeErr = errors.Join(finalizeErr, removeErr)
		}
		s.recordStorageWriteFailure(fmt.Errorf("rewrite event spool replay: %w", finalizeErr))
		s.scheduleEventSpoolReplay(r.store)
		return
	}
	if aborted {
		if removeErr := removeEventSpoolTemp(failedPath); removeErr != nil {
			s.recordStorageWriteFailure(fmt.Errorf("discard aborted event spool rewrite: %w", removeErr))
		}
		s.scheduleEventSpoolReplay(r.store)
		return
	}
	if failedRecords == 0 {
		if err := os.Remove(r.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.recordStorageWriteFailure(fmt.Errorf("remove completed event spool replay: %w", err))
			s.scheduleEventSpoolReplay(r.store)
		}
		return
	}
	if err := os.Rename(failedPath, r.path); err != nil {
		cleanupErr := removeEventSpoolTemp(failedPath)
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
		s.recordStorageWriteFailure(fmt.Errorf("replace event spool replay: %w", err))
		s.scheduleEventSpoolReplay(r.store)
		return
	}
	s.scheduleEventSpoolReplay(r.store)
}

func removeEventSpoolTemp(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove temporary event spool rewrite: %w", err)
	}
	return nil
}

func (s *RequestStatistics) releaseEventWriterTaskBytes(batch []eventWriteTask) {
	if s == nil {
		return
	}
	for _, task := range batch {
		if task.retainedBytes > 0 {
			releaseEventQueueBytes(&s.eventWriterQueueBytes, task.retainedBytes)
		}
	}
}

func (s *RequestStatistics) releaseEventSpoolTaskBytes(task eventWriteTask) {
	if s == nil || task.retainedBytes <= 0 {
		return
	}
	releaseEventQueueBytes(&s.eventSpoolQueueBytes, task.retainedBytes)
}

func reserveEventQueueBytes(counter *atomic.Int64, amount, limit int64) bool {
	if counter == nil || amount <= 0 || limit <= 0 || amount > limit {
		return false
	}
	for {
		current := counter.Load()
		if current < 0 || current > limit-amount {
			return false
		}
		if counter.CompareAndSwap(current, current+amount) {
			return true
		}
	}
}

func releaseEventQueueBytes(counter *atomic.Int64, amount int64) {
	if counter == nil || amount <= 0 {
		return
	}
	for {
		current := counter.Load()
		if current <= 0 {
			counter.Store(0)
			return
		}
		next := current - amount
		if next < 0 {
			next = 0
		}
		if counter.CompareAndSwap(current, next) {
			return
		}
	}
}

func eventWriteTaskRetainedBytes(task eventWriteTask) int64 {
	retained := int64(512 + len(task.apiName) + len(task.modelName))
	detail := task.detail
	for _, value := range []string{
		detail.UpstreamAPI, detail.Model, detail.APIKey, detail.APIKeyHash,
		detail.Source, detail.Provider, detail.AuthID, detail.AuthIndex,
		detail.AuthType, detail.Endpoint, detail.BaseURL, detail.Failure,
		detail.Thinking.Intensity, detail.Thinking.Mode, detail.Thinking.Level,
	} {
		retained += int64(len(value))
	}
	for key, values := range detail.Headers {
		retained += int64(len(key) + 32)
		for _, value := range values {
			retained += int64(len(value))
		}
	}
	if retained < 512 {
		return 512
	}
	return retained
}
