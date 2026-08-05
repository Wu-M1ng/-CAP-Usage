package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEventWriterWaitsBrieflyToCoalesceBatch(t *testing.T) {
	queue := make(chan eventWriteTask, 1)
	started := time.Now()
	batch := collectEventWriteBatchUntil(queue, eventWriteTask{}, eventWriteBatchMaxWait, eventWriteBatchMaxSize)
	elapsed := time.Since(started)
	if len(batch) != 1 {
		t.Fatalf("coalesced batch size = %d, want 1", len(batch))
	}
	if elapsed < eventWriteBatchMaxWait/2 {
		t.Fatalf("coalescing wait = %s, want at least %s", elapsed, eventWriteBatchMaxWait/2)
	}
}

func TestEventWriterFlushesImmediatelyAtBatchLimit(t *testing.T) {
	queue := make(chan eventWriteTask, eventWriteBatchMaxSize)
	for i := 0; i < eventWriteBatchMaxSize-1; i++ {
		queue <- eventWriteTask{}
	}
	started := time.Now()
	batch := collectEventWriteBatchUntil(queue, eventWriteTask{}, eventWriteBatchMaxWait, eventWriteBatchMaxSize)
	if len(batch) != eventWriteBatchMaxSize {
		t.Fatalf("full batch size = %d, want %d", len(batch), eventWriteBatchMaxSize)
	}
	if elapsed := time.Since(started); elapsed >= eventWriteBatchMaxWait/2 {
		t.Fatalf("full batch wait = %s, want less than %s", elapsed, eventWriteBatchMaxWait/2)
	}
}

func TestEventWriterShutdownFlushesPartialBatch(t *testing.T) {
	queue := make(chan eventWriteTask, 4)
	queue <- eventWriteTask{}
	queue <- eventWriteTask{}
	queue <- eventWriteTask{}
	batch := collectEventWriteBatchUntil(queue, eventWriteTask{}, 0, eventWriteBatchMaxSize)
	if len(batch) != 4 || len(queue) != 0 {
		t.Fatalf("shutdown batch = %d with queue length %d, want 4/0", len(batch), len(queue))
	}
}

// BenchmarkRecordSQLiteBatch128 measures the worker's transaction shape. The
// repeated batches intentionally use exact fingerprints, so the database does
// not grow with b.N while every batch still exercises the prepared statement,
// duplicate checks, and one transaction boundary.
func BenchmarkRecordSQLiteBatch128(b *testing.B) {
	store, err := openEventStore(b.TempDir()+"/usage-statistics.db", false)
	if err != nil {
		b.Fatalf("open event store: %v", err)
	}
	defer store.close()

	statistics := NewRequestStatistics()
	statistics.eventStore = store
	statistics.maxDetailsPerModel = -1
	statistics.retention = 0
	tasks := make([]eventWriteTask, eventWriteBatchMaxSize)
	when := time.Now().UTC()
	for i := range tasks {
		model := fmt.Sprintf("benchmark-model-%d", i)
		detail := RequestDetail{
			Model:     model,
			Timestamp: when,
			Provider:  "openai",
			Tokens:    TokenStats{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
		}
		tasks[i] = eventWriteTask{
			store:       store,
			apiName:     "openai",
			modelName:   model,
			detail:      detail,
			fingerprint: fmt.Sprintf("benchmark-fingerprint-%d", i),
			exact:       true,
			maxDetails:  -1,
			retention:   0,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := statistics.persistEventWriteGroup(nil, store, tasks); err != nil {
			b.Fatalf("persist event batch: %v", err)
		}
	}
	b.StopTimer()
}

func TestStartStorageWorkerLockedDoesNotReverseLockOrder(t *testing.T) {
	statistics := NewRequestStatistics()
	statistics.storageEnabled = true
	statistics.storageDir = t.TempDir()
	statistics.storageWriteQueueCapacity = 1

	statistics.mu.Lock()
	statistics.startStorageWorkerLocked()
	statistics.mu.Unlock()
	statistics.stopStorageWorker()
}

func TestEventWriterQueueBoundsRetainedBytes(t *testing.T) {
	statistics := NewRequestStatistics()
	statistics.eventWriterQueue = make(chan eventWriteTask, 4)
	statistics.eventWriterQueueCapacity = 4
	store := &eventStore{}
	largeHeader := strings.Repeat("x", int(eventWriterQueueMaxBytes/2))
	task := eventWriteTask{
		store:     store,
		apiName:   "openai",
		modelName: "gpt-5",
		detail: RequestDetail{
			Headers: map[string][]string{"x-test": {largeHeader}},
		},
	}

	statistics.storageControlMu.Lock()
	if !statistics.enqueueEventWriteLocked(task) {
		statistics.storageControlMu.Unlock()
		t.Fatal("first byte-bounded task was rejected")
	}
	if statistics.enqueueEventWriteLocked(task) {
		statistics.storageControlMu.Unlock()
		t.Fatal("second task exceeded the byte budget but was accepted")
	}
	statistics.storageControlMu.Unlock()

	if got := statistics.eventWriterQueueBytes.Load(); got > eventWriterQueueMaxBytes {
		t.Fatalf("writer queue bytes = %d, want <= %d", got, eventWriterQueueMaxBytes)
	}
	queued := <-statistics.eventWriterQueue
	statistics.finishEventWriteTask()
	statistics.releaseEventWriterTaskBytes([]eventWriteTask{queued})
	if got := statistics.eventWriterQueueBytes.Load(); got != 0 {
		t.Fatalf("writer queue bytes after release = %d, want 0", got)
	}
}
