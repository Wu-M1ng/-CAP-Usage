package main

import (
	"encoding/json"
	"fmt"
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

	accepted := 0
	for i := 0; i < usageCallbackQueueMaxTasks+1; i++ {
		if processor.enqueue(func() {}, 0) {
			accepted++
		}
	}
	if accepted != usageCallbackQueueMaxTasks {
		t.Fatalf("enqueue() accepted %d tasks, want %d", accepted, usageCallbackQueueMaxTasks)
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
	processor.drain()
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

func TestDecodeResponseStreamChunkForUsageBoundsHistoryCopy(t *testing.T) {
	history := make([][]byte, 128)
	for i := range history {
		history[i] = []byte(fmt.Sprintf(`data: {"usage":{"prompt_tokens":%d,"completion_tokens":2,"total_tokens":%d}}`, i+1, i+3))
	}
	wire, err := json.Marshal(ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat:   "openai",
			Model:          "gpt-5.5",
			RequestedModel: "gpt-5.5",
			Body:           []byte(`data: {"usage":{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105}}`),
			StatusCode:     200,
		},
		HistoryChunks: history,
	})
	if err != nil {
		t.Fatalf("marshal stream chunk: %v", err)
	}
	decoded, hasUsage, err := decodeResponseStreamChunkForUsage(wire)
	if err != nil {
		t.Fatalf("decodeResponseStreamChunkForUsage() error = %v", err)
	}
	if !hasUsage || decoded.Model != "gpt-5.5" {
		t.Fatalf("decoded stream chunk = %#v/%t, want usage-bearing gpt-5.5", decoded, hasUsage)
	}
	if len(decoded.HistoryChunks) > maxStreamHistoryUsageChunks {
		t.Fatalf("retained history chunks = %d, want <= %d", len(decoded.HistoryChunks), maxStreamHistoryUsageChunks)
	}
	var retainedBytes int
	for _, chunk := range decoded.HistoryChunks {
		retainedBytes += len(chunk)
	}
	if retainedBytes > maxStreamHistoryUsageBytes {
		t.Fatalf("retained history bytes = %d, want <= %d", retainedBytes, maxStreamHistoryUsageBytes)
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
	case queued := <-accepted:
		if queued {
			t.Fatal("oversized payload was retained in the bounded queue")
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("oversized payload enqueue blocked")
	}

	releaseWorker()
	processor.drain()
}
