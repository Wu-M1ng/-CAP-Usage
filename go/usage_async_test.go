package main

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestUsageCallbackProcessorBoundsQueuedTasks(t *testing.T) {
	processor := newUsageCallbackProcessor()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer func() {
		releaseWorker()
		processor.shutdown()
	}()

	if !processor.enqueue(func() {
		close(started)
		<-release
	}, 0) {
		t.Fatal("enqueue() rejected the initial task")
	}
	<-started

	ready := make(chan struct{})
	done := make(chan int, 1)
	go func() {
		accepted := 0
		for i := 0; i < usageCallbackQueueMaxTasks+1; i++ {
			if processor.enqueue(func() {}, 0) {
				accepted++
			}
			if i == usageCallbackQueueMaxTasks-1 {
				close(ready)
			}
		}
		done <- accepted
	}()
	<-ready

	select {
	case accepted := <-done:
		t.Fatalf("enqueue() accepted %d tasks before the worker drained, want backpressure", accepted)
	case <-time.After(50 * time.Millisecond):
	}

	processor.mu.Lock()
	queuedTasks := len(processor.queue)
	queuedBytes := processor.queuedBytes
	processor.mu.Unlock()
	if queuedTasks > usageCallbackQueueMaxTasks {
		t.Fatalf("queued tasks = %d, want <= %d", queuedTasks, usageCallbackQueueMaxTasks)
	}
	if queuedBytes > usageCallbackQueueMaxBytes {
		t.Fatalf("queued payload bytes = %d, want <= %d", queuedBytes, usageCallbackQueueMaxBytes)
	}

	releaseWorker()
	select {
	case accepted := <-done:
		if accepted != usageCallbackQueueMaxTasks+1 {
			t.Fatalf("accepted tasks after backpressure = %d, want %d", accepted, usageCallbackQueueMaxTasks+1)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded queue did not drain")
	}
}

func TestResponseStreamChunkWithoutCurrentUsageIsNotQueued(t *testing.T) {
	previousStats := stats
	previousCallbacks := usageCallbacks
	previousFallbacks := usageFallbacks
	statistics := NewRequestStatistics()
	statistics.eventStore = &eventStore{}
	processor := newUsageCallbackProcessor()
	stats = statistics
	usageCallbacks = processor
	usageFallbacks = nil

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer func() {
		releaseWorker()
		processor.shutdown()
		usageCallbacks = previousCallbacks
		usageFallbacks = previousFallbacks
		stats = previousStats
	}()
	if !processor.enqueue(func() {
		close(started)
		<-release
	}, 0) {
		t.Fatal("enqueue() rejected the blocker task")
	}
	<-started

	req := ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat:   "openai",
			Model:          "gpt-5.5",
			RequestedModel: "gpt-5.5",
			Body:           []byte(`data: {"choices":[{"delta":{"content":"hello"}}]}`),
			StatusCode:     200,
		},
		HistoryChunks: [][]byte{
			[]byte(`data: {"usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`),
		},
		ChunkIndex: 2,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal stream chunk: %v", err)
	}
	if _, err := handleResponseStreamChunk(body); err != nil {
		t.Fatalf("handleResponseStreamChunk() error = %v", err)
	}

	processor.mu.Lock()
	queuedTasks := len(processor.queue)
	processor.mu.Unlock()
	if queuedTasks != 0 {
		t.Fatalf("queued tasks = %d, want no task for a chunk without current usage", queuedTasks)
	}

	req.Body = []byte(`data: {"usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`)
	body, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal usage stream chunk: %v", err)
	}
	if _, err := handleResponseStreamChunk(body); err != nil {
		t.Fatalf("handleResponseStreamChunk() usage error = %v", err)
	}
	processor.mu.Lock()
	queuedTasks = len(processor.queue)
	processor.mu.Unlock()
	if queuedTasks != 1 {
		t.Fatalf("queued usage tasks = %d, want one task for the current usage chunk", queuedTasks)
	}
}

func TestPluginCallNeedsRequestCopySkipsNonUsageStreamChunks(t *testing.T) {
	streamBody := func(body string) []byte {
		raw, err := json.Marshal(ResponseStreamChunkRequest{
			ResponseInterceptRequest: ResponseInterceptRequest{
				SourceFormat: "openai",
				Body:         []byte(body),
			},
			HistoryChunks: [][]byte{make([]byte, 4<<20)},
			ChunkIndex:    42,
		})
		if err != nil {
			t.Fatalf("marshal stream chunk: %v", err)
		}
		return raw
	}

	if pluginCallNeedsRequestCopy("response.intercept_stream_chunk", streamBody(`data: {"choices":[{"delta":{"content":"done"}}]}`)) {
		t.Fatal("non-usage stream chunk should not be copied into the Go heap")
	}
	if !pluginCallNeedsRequestCopy("response.intercept_stream_chunk", streamBody(`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)) {
		t.Fatal("usage-bearing stream chunk must keep an owned copy for background processing")
	}
	if !pluginCallNeedsRequestCopy("usage.handle", []byte(`{"detail":{"total_tokens":12}}`)) {
		t.Fatal("non-stream plugin calls must keep an owned request copy")
	}
	if !pluginCallNeedsRequestCopy("response.intercept_stream_chunk", []byte(`{"Body":`)) {
		t.Fatal("malformed stream callback must fall back to the normal error path")
	}
}

func TestUsageCallbackProcessorHandlesOversizedPayloadSynchronouslyAfterDrain(t *testing.T) {
	processor := newUsageCallbackProcessor()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer func() {
		releaseWorker()
		processor.shutdown()
	}()

	if !processor.enqueue(func() {
		close(started)
		<-release
	}, 0) {
		t.Fatal("enqueue() rejected the blocker task")
	}
	<-started

	accepted := make(chan bool, 1)
	go func() {
		accepted <- processor.enqueue(func() {}, usageCallbackQueueMaxBytes)
	}()
	select {
	case <-accepted:
		t.Fatal("oversized payload bypassed an earlier active callback")
	case <-time.After(50 * time.Millisecond):
	}

	releaseWorker()
	select {
	case queued := <-accepted:
		if queued {
			t.Fatal("oversized payload was retained in the bounded queue")
		}
	case <-time.After(time.Second):
		t.Fatal("oversized payload did not fall back to synchronous processing")
	}
}
