package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBase64ChunkContainsUsageHandlesScanBlockBoundary(t *testing.T) {
	const encodedBlockBytes = 32 << 10
	decodedBoundary := base64.StdEncoding.DecodedLen(encodedBlockBytes)
	payload := append(bytes.Repeat([]byte{'x'}, decodedBoundary-2), []byte(`"usage":{"total_tokens":12}`)...)
	encoded := base64.StdEncoding.EncodeToString(payload)
	scratch := make([]byte, base64.StdEncoding.DecodedLen(encodedBlockBytes))
	hasUsage, err := base64ChunkContainsUsage(encoded, scratch)
	if err != nil {
		t.Fatalf("base64ChunkContainsUsage() error = %v", err)
	}
	if !hasUsage {
		t.Fatal("base64ChunkContainsUsage() missed usage marker across scan block boundary")
	}

	cleanEncoded := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, decodedBoundary+16))
	hasUsage, err = base64ChunkContainsUsage(cleanEncoded, scratch)
	if err != nil {
		t.Fatalf("base64ChunkContainsUsage() clean error = %v", err)
	}
	if hasUsage {
		t.Fatal("base64ChunkContainsUsage() reported usage in a clean payload")
	}
}

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

func TestResponseStreamChunkUsesHistoryUsageWhenCurrentBodyHasNone(t *testing.T) {
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
	if queuedTasks != 1 {
		t.Fatalf("queued history usage tasks = %d, want one task", queuedTasks)
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
	if queuedTasks != 2 {
		t.Fatalf("queued usage tasks = %d, want two history/current usage tasks", queuedTasks)
	}
}

func TestResponseInterceptCompactsLargeResponseBodyBeforeQueue(t *testing.T) {
	previousStats := stats
	previousCallbacks := usageCallbacks
	previousFallbacks := usageFallbacks
	statistics := NewRequestStatistics()
	statistics.mu.Lock()
	statistics.eventStore = &eventStore{}
	statistics.mu.Unlock()
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

	largeContent := bytes.Repeat([]byte{'x'}, 3<<20)
	body := append([]byte(`data: {"choices":[{"delta":{"content":"`), largeContent...)
	body = append(body, []byte(`"}}]}`)...)
	body = append(body, []byte("\n\ndata: {\"id\":\"resp-compact\",\"model\":\"gpt-5.5\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20,\"total_tokens\":120}}\n")...)
	request, err := json.Marshal(ResponseInterceptRequest{
		SourceFormat:   "openai-responses",
		Model:          "gpt-5.5",
		RequestedModel: "gpt-5.5",
		Body:           body,
		StatusCode:     200,
		Metadata:       map[string]any{"request_path": "/v1/responses"},
	})
	if err != nil {
		t.Fatalf("marshal response intercept request: %v", err)
	}
	if _, err := handleResponseIntercept(request); err != nil {
		t.Fatalf("handleResponseIntercept() error = %v", err)
	}

	processor.mu.Lock()
	if len(processor.queue) != 1 {
		queued := len(processor.queue)
		processor.mu.Unlock()
		t.Fatalf("queued response tasks = %d, want one compact task", queued)
	}
	retained := processor.queue[0].retained
	processor.mu.Unlock()
	if retained >= 128<<10 {
		t.Fatalf("compact response task retains %d bytes, want less than 128 KiB", retained)
	}
	if retained >= len(body)/8 {
		t.Fatalf("compact response task retains %d bytes for %d-byte body", retained, len(body))
	}

	releaseWorker()
	processor.drain()
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
	historyUsage, err := json.Marshal(ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat: "openai",
			Body:         []byte(`data: {"choices":[{"delta":{"content":"done"}}]}`),
		},
		HistoryChunks: [][]byte{[]byte(`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)},
	})
	if err != nil {
		t.Fatalf("marshal history usage stream chunk: %v", err)
	}
	if !pluginCallNeedsRequestCopy("response.intercept_stream_chunk", historyUsage) {
		t.Fatal("history usage-bearing stream chunk must keep an owned copy for background processing")
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

func TestDecodeResponseStreamChunkForUsageDoesNotRetainLargeRequestPayload(t *testing.T) {
	largePrompt := bytes.Repeat([]byte{'x'}, 3<<20)
	requestPayload := append([]byte(`{"model":"gpt-5.5","service_tier":"standard","prompt":"`), largePrompt...)
	requestPayload = append(requestPayload, []byte(`"}`)...)
	wire, err := json.Marshal(ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat:    "openai",
			Model:           "gpt-5.5",
			RequestedModel:  "gpt-5.5",
			OriginalRequest: requestPayload,
			Body:            []byte(`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`),
			StatusCode:      200,
		},
	})
	if err != nil {
		t.Fatalf("marshal stream chunk: %v", err)
	}
	decoded, hasUsage, err := decodeResponseStreamChunkForUsage(wire)
	if err != nil {
		t.Fatalf("decodeResponseStreamChunkForUsage() error = %v", err)
	}
	if !hasUsage {
		t.Fatal("decodeResponseStreamChunkForUsage() hasUsage = false, want true")
	}
	if len(decoded.OriginalRequest) != 0 || len(decoded.RequestBody) != 0 {
		t.Fatalf("decoded request payload retained: original=%d request=%d, want both zero", len(decoded.OriginalRequest), len(decoded.RequestBody))
	}
	if got := metadataString(decoded.Metadata, "service_tier"); got != "standard" {
		t.Fatalf("decoded service tier = %q, want standard", got)
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
