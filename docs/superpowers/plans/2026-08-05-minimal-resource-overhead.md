# Minimal Resource Overhead Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Minimize plugin CPU, heap usage, and API callback latency while preserving native usage, Body usage, History-only usage, deduplication, request metadata, and SQLite durability under normal bounded load.

**Architecture:** Replace full cumulative-history parsing on every stream callback with a two-stage terminal-aware parser. The synchronous C ABI path extracts only the current Body and compact stream identity; full HistoryChunks are inspected at most once when a stream terminates. Completed compact records enter a byte-bounded ordered queue, and SQLite persists them with short delayed batching. Dashboard queries and caches are kept outside the API path and reduced to demand-driven work.

**Tech Stack:** Go, cgo C ABI, `github.com/buger/jsonparser` v1.1.1, `encoding/json`, `encoding/base64`, `database/sql`, `modernc.org/sqlite`, browser JavaScript, Go benchmarks, Go race tests.

## Global Constraints

- A normal non-settlement stream callback must not copy the complete C request buffer into the Go heap.
- A normal non-settlement stream callback must not decode `OriginalRequest`, `RequestBody`, headers, metadata maps, or any HistoryChunks payload.
- History-only final usage remains supported for OpenAI Responses, OpenAI Chat Completions, Codex, Anthropic, Gemini, and compatible SSE variants already covered by tests.
- Request correlation must prefer response/request ID. Weak model/token fingerprints must never merge concurrent streams when a strong ID exists.
- No raw response transcript, prompt, OriginalRequest, RequestBody, or complete HistoryChunks array may be retained after the callback returns.
- SQLite and spool I/O remain worker-only operations. API callback goroutines must not open, write, sync, query, prune, or snapshot files.
- Queues must be bounded by both item count and retained bytes.
- Under impossible overload, fixed memory and API availability take priority. Overflow must be counted explicitly; it must never silently execute disk I/O on the API goroutine.
- `go test ./...`, JavaScript dashboard tests, `go vet ./...`, and Linux `go test -race ./...` must pass before release.

---

### Task 1: Establish Reproducible CPU, Allocation, and Latency Baselines

**Files:**
- Modify: `go/usage_async_test.go`
- Create: `go/performance_test.go`
- Modify: `go/types.go`
- Modify: `go/stats.go`
- Modify: `go/management.go`

**Interfaces:**
- Produces: `StreamRuntimeMetrics`, `streamCallbackObservation`, `RequestStatistics.RecordStreamCallbackObservation`.
- Produces benchmark names used as fixed before/after comparisons by later tasks.

- [ ] **Step 1: Add runtime metric types without placing timers on unrelated code paths**

```go
type StreamRuntimeMetrics struct {
	Callbacks             int64   `json:"callbacks"`
	FastPathCallbacks     int64   `json:"fast_path_callbacks"`
	SettlementCallbacks   int64   `json:"settlement_callbacks"`
	TerminalHistoryScans  int64   `json:"terminal_history_scans"`
	InputBytes            int64   `json:"input_bytes"`
	BodyBytesDecoded      int64   `json:"body_bytes_decoded"`
	HistoryBytesDecoded   int64   `json:"history_bytes_decoded"`
	CallbackDurationMsAvg float64 `json:"callback_duration_ms_avg"`
	CallbackDurationMsMax float64 `json:"callback_duration_ms_max"`
}

type streamCallbackObservation struct {
	inputBytes          int
	bodyBytesDecoded    int
	historyBytesDecoded int
	fastPath            bool
	settlement          bool
	terminalHistoryScan bool
	duration            time.Duration
}
```

Store only counters, duration sum/count, and maximum in `RequestStatistics`; do not retain per-callback samples.

- [ ] **Step 2: Add a failing runtime-status test**

Add `TestRuntimeStatusReportsBoundedStreamMetrics` that records two observations and checks exact totals, average, and maximum. It must also verify that runtime status does not expose request bodies, headers, API keys, or HistoryChunks.

- [ ] **Step 3: Run the metric test and verify it fails**

Run: `cd go && go test -run TestRuntimeStatusReportsBoundedStreamMetrics -count=1`

Expected: FAIL because the stream metric fields and recorder do not exist.

- [ ] **Step 4: Implement the counter-only recorder and status output**

```go
func (s *RequestStatistics) RecordStreamCallbackObservation(o streamCallbackObservation) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.streamCallbacks++
	s.streamInputBytes += int64(max(o.inputBytes, 0))
	s.streamBodyBytesDecoded += int64(max(o.bodyBytesDecoded, 0))
	s.streamHistoryBytesDecoded += int64(max(o.historyBytesDecoded, 0))
	if o.fastPath {
		s.streamFastPathCallbacks++
	}
	if o.settlement {
		s.streamSettlementCallbacks++
	}
	if o.terminalHistoryScan {
		s.streamTerminalHistoryScans++
	}
	if o.duration > 0 {
		s.streamCallbackDurationSum += o.duration
		s.streamCallbackDurationCount++
		if o.duration > s.streamCallbackDurationMax {
			s.streamCallbackDurationMax = o.duration
		}
	}
	s.mu.Unlock()
}
```

- [ ] **Step 5: Add fixed benchmarks for the actual problematic shape**

Create these benchmarks in `go/performance_test.go`:

```go
func BenchmarkStreamCallbackNoUsage4MiBHistory(b *testing.B)
func BenchmarkStreamCallbackHistoryOnlyFinal4MiB(b *testing.B)
func BenchmarkStreamCallbackCurrentBodyUsage4MiBHistory(b *testing.B)
func BenchmarkRecordSQLiteBatch128(b *testing.B)
```

The stream fixture must contain a 256 KiB OriginalRequest, a 256 KiB RequestBody, 128 cumulative history chunks totaling at least 4 MiB, and a small current Body. Run benchmarks with `-benchmem` and retain their output in the implementation report rather than committing generated profiles.

- [ ] **Step 6: Capture the baseline**

Run: `cd go && go test -run '^$' -bench 'BenchmarkStreamCallback|BenchmarkRecordSQLiteBatch128' -benchmem -count=5`

Expected: PASS. Record median `ns/op`, `B/op`, and `allocs/op` for each benchmark.

- [ ] **Step 7: Commit the baseline and metrics**

```bash
git add go/performance_test.go go/usage_async_test.go go/types.go go/stats.go go/management.go
git commit -m "test: add stream and storage performance baselines"
```

---

### Task 2: Add a Two-Stage Stream Callback Envelope Parser

**Files:**
- Create: `go/stream_callback.go`
- Create: `go/stream_callback_test.go`
- Modify: `go/main.go`
- Modify: `go/response_intercept.go`
- Modify: `go/go.mod`
- Modify: `go/go.sum`

**Interfaces:**
- Produces: `inspectStreamCallbackEnvelope([]byte) (streamCallbackEnvelope, error)`.
- Produces: `decodeStreamSettlement(streamCallbackEnvelope) (ResponseStreamChunkRequest, bool, error)`.
- Consumes: existing `usageRecordFromResponseStreamChunk` and `handleResponseStreamChunkRequest`.

- [ ] **Step 1: Add failing fast-path tests**

Add these tests:

```go
func TestInspectStreamCallbackSkipsLargeHistoryOnOrdinaryChunk(t *testing.T)
func TestInspectStreamCallbackDetectsOpenAIResponsesTerminal(t *testing.T)
func TestInspectStreamCallbackDetectsChatCompletionsDone(t *testing.T)
func TestInspectStreamCallbackDetectsAnthropicMessageStop(t *testing.T)
func TestInspectStreamCallbackDetectsCurrentBodyUsage(t *testing.T)
func TestInspectStreamCallbackAcceptsPascalSnakeAndCamelFields(t *testing.T)
func TestInspectStreamCallbackRejectsMalformedEnvelope(t *testing.T)
```

The ordinary-chunk test must prove that a deliberately invalid Base64 item inside `HistoryChunks` does not fail inspection. That demonstrates the fast path did not decode history.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `cd go && go test -run 'TestInspectStreamCallback' -count=1`

Expected: FAIL because `inspectStreamCallbackEnvelope` does not exist.

- [ ] **Step 3: Implement a compact envelope type**

Add the exact in-place field parser dependency:

```bash
cd go
go get github.com/buger/jsonparser@v1.1.1
```

```go
type streamCallbackEnvelope struct {
	raw           []byte
	body          []byte
	statusCode    int
	chunkIndex    int
	correlationID string
	terminal      bool
	bodyHasUsage  bool
}
```

Use `jsonparser.Get` so returned field slices reference the host-owned input only during `cliproxyPluginCall`. Probe `Body`, `body`, status, chunk index, and compact correlation fields directly; do not call `jsonparser.ArrayEach` for HistoryChunks on an ordinary callback. Decode only the small Base64 Body into a pooled buffer. Recognize terminal markers case-sensitively after JSON/SSE normalization:

```text
[DONE]
response.completed
response.failed
response.incomplete
message_stop
message_delta with stop_reason
```

- [ ] **Step 4: Replace the C ABI full-history gate**

In `cliproxyPluginCall`, replace the unconditional `decodeResponseStreamChunkForUsage(requestView)` path with:

```go
envelope, err := inspectStreamCallbackEnvelope(requestView)
if err == nil && !envelope.bodyHasUsage && !envelope.terminal {
	stats.RecordStreamCallbackObservation(streamCallbackObservation{
		inputBytes: len(requestView),
		fastPath:   true,
	})
	raw, _ := okEnvelopeJSON("{}")
	writeResponse(response, raw)
	return 0
}
```

Settlement decoding remains synchronous only long enough to produce a compact owned record. Never enqueue `requestView` or any slice referencing it.

- [ ] **Step 5: Preserve malformed-input behavior**

If compact inspection fails, return a plugin parse error for malformed settlement callbacks. For ordinary callbacks where Body is extractable and has no usage, return success even when unrelated ignored fields are malformed. This keeps the fast path independent from oversized history data.

- [ ] **Step 6: Run parser tests and existing stream regressions**

Run: `cd go && go test -run 'TestInspectStreamCallback|TestResponseStream|TestDecodeResponseStream' -count=1`

Expected: PASS.

- [ ] **Step 7: Re-run stream benchmarks**

Run: `cd go && go test -run '^$' -bench 'BenchmarkStreamCallback' -benchmem -count=5`

Acceptance:

- Ordinary 4 MiB callback: at least 80% lower `ns/op` than Task 1 baseline.
- Ordinary 4 MiB callback: at least 90% lower `B/op` than Task 1 baseline.
- Ordinary callback allocations do not scale with OriginalRequest, RequestBody, or HistoryChunks byte size.

- [ ] **Step 8: Commit the two-stage parser**

```bash
git add go/stream_callback.go go/stream_callback_test.go go/main.go go/response_intercept.go go/go.mod go/go.sum
git commit -m "perf: add terminal-aware stream callback fast path"
```

---

### Task 3: Scan History Once and Keep Only Compact Per-Stream State

**Files:**
- Create: `go/stream_state.go`
- Create: `go/stream_state_test.go`
- Modify: `go/stream_callback.go`
- Modify: `go/response_intercept.go`
- Modify: `go/main.go`

**Interfaces:**
- Produces: `streamUsageTracker.Observe(streamObservation) []UsageRecord`.
- Produces: `latestUsageFromTerminalHistory([]byte) (UsageRecord, bool, int, error)`.
- Consumes: strong correlation IDs extracted by Task 2.

- [ ] **Step 1: Add failing correctness and bound tests**

```go
func TestStreamTrackerCommitsLatestUsageAtTerminal(t *testing.T)
func TestStreamTrackerHistoryOnlyTerminalCommitsOnce(t *testing.T)
func TestStreamTrackerConcurrentEqualTokenStreamsStaySeparate(t *testing.T)
func TestStreamTrackerRejectsStaleChunkIndex(t *testing.T)
func TestStreamTrackerExpiresIdleEntry(t *testing.T)
func TestStreamTrackerCapacityIsBounded(t *testing.T)
func TestStreamTrackerDoesNotRetainRawPayload(t *testing.T)
func TestTerminalHistorySearchStopsAtLatestValidUsage(t *testing.T)
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd go && go test -run 'TestStreamTracker|TestTerminalHistorySearch' -count=1`

Expected: FAIL because the tracker does not exist.

- [ ] **Step 3: Implement bounded compact state**

```go
const (
	maxTrackedStreams     = 2048
	maxTrackedStreamBytes = 8 << 20
	streamStateTTL        = 60 * time.Second
)

type streamObservation struct {
	id         string
	chunkIndex int
	terminal   bool
	candidate  *UsageRecord
	now        time.Time
}

type trackedStream struct {
	id         string
	chunkIndex int
	updatedAt  time.Time
	candidate  UsageRecord
	hasUsage   bool
	bytes      int
}
```

The tracker stores only strings and numeric usage fields required by the final `UsageRecord`. It must not store `[]byte`, raw JSON, headers not selected by the whitelist, maps copied from metadata, or response transcripts.

- [ ] **Step 4: Make terminal History scanning newest-first and one-shot**

Only a terminal callback with no valid current Body usage may inspect HistoryChunks. Iterate array elements from newest to oldest, Base64-decode one element at a time into a pooled scratch buffer, and stop after the first valid final usage record. Increment `terminal_history_scans` once per terminal callback, not once per history element.

- [ ] **Step 5: Keep no-ID behavior conservative without unsafe merging**

For callbacks without a strong response/request ID:

- Current Body usage is processed immediately through the existing fallback coordinator.
- Terminal History is scanned once and processed immediately.
- No weak per-stream state is created.
- Model/token equality alone never overwrites another active stream.

- [ ] **Step 6: Integrate final candidates with native/fallback reconciliation**

Feed only the finalized candidate to `usageFallbackCoordinator.ScheduleForStats`. Continue calling `HandleNativeForStats` for native records. Preserve the existing 2-second fallback delay and late-native enrichment behavior, but remove repeated scheduling caused only by cumulative HistoryChunks.

- [ ] **Step 7: Run all response and dedup tests**

Run: `cd go && go test -run 'TestStreamTracker|TestTerminalHistorySearch|TestResponseStream|TestResponseIntercept|TestFallback|TestDedup' -count=1`

Expected: PASS with exactly one record for each native/fallback pair and each completed stream.

- [ ] **Step 8: Commit one-shot History handling**

```bash
git add go/stream_state.go go/stream_state_test.go go/stream_callback.go go/response_intercept.go go/main.go
git commit -m "perf: scan cumulative stream history only at completion"
```

---

### Task 4: Remove Allocation-Heavy Usage and SSE Conversion

**Files:**
- Create: `go/usage_decode.go`
- Create: `go/usage_decode_test.go`
- Modify: `go/response_intercept.go`
- Modify: `go/protocol_helpers.go`

**Interfaces:**
- Produces: `decodeUsagePayload([]byte, usageDecodeMode) (decodedUsage, bool)`.
- Produces: `forEachSSEData([]byte, func([]byte) bool)`.
- Replaces generic `map[string]any -> json.Marshal -> json.Unmarshal` usage conversion on stream settlement paths.

- [ ] **Step 1: Add failing protocol fixtures**

Add table tests covering OpenAI Responses, Chat Completions, Anthropic cache read/write, Gemini `usageMetadata`, Codex `response.completed`, usage-only SSE, multiple independent data lines, malformed data lines, and `[DONE]`.

- [ ] **Step 2: Run the decoder tests and verify they fail**

Run: `cd go && go test -run 'TestDecodeUsagePayload|TestForEachSSEData' -count=1`

Expected: FAIL because the compact decoder does not exist.

- [ ] **Step 3: Implement byte-oriented SSE iteration**

```go
func forEachSSEData(body []byte, fn func([]byte) bool) {
	for len(body) > 0 {
		line, rest, _ := bytes.Cut(body, []byte{'\n'})
		body = rest
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		if !fn(data) {
			return
		}
	}
}
```

Do not convert the complete body to string and do not split it into a slice of strings.

- [ ] **Step 4: Decode usage into typed numeric fields directly**

Return a compact value:

```go
type decodedUsage struct {
	model         string
	correlationID string
	detail        UsageDetail
}
```

Support all existing aliases directly. Do not marshal a discovered usage map back to JSON.

- [ ] **Step 5: Pool Base64 and temporary decode buffers**

Use `sync.Pool` with a maximum retained buffer size of 256 KiB. Larger one-off payloads must be allowed but returned to GC instead of being retained in the pool.

- [ ] **Step 6: Run decoder tests and benchmarks**

Run: `cd go && go test -run 'TestDecodeUsagePayload|TestForEachSSEData|TestUsageDetail' -count=1`

Run: `cd go && go test -run '^$' -bench 'BenchmarkStreamCallback' -benchmem -count=5`

Acceptance: settlement callback `B/op` is at least 50% below the Task 1 baseline and does not scale with request prompt size.

- [ ] **Step 7: Commit compact decoding**

```bash
git add go/usage_decode.go go/usage_decode_test.go go/response_intercept.go go/protocol_helpers.go
git commit -m "perf: decode usage and SSE without generic JSON round trips"
```

---

### Task 5: Replace Raw Callback Closures with a Compact Ordered Queue

**Files:**
- Modify: `go/usage_async.go`
- Modify: `go/usage_async_test.go`
- Modify: `go/management.go`
- Modify: `go/response_intercept.go`
- Modify: `go/types.go`
- Modify: `go/stats.go`

**Interfaces:**
- Produces: `usageIngestQueue.Enqueue(usageIngestTask) usageCallbackDisposition`.
- Consumes finalized `UsageRecord` values from Tasks 3 and 4.

- [ ] **Step 1: Add failing queue-bound and ordering tests**

```go
func TestUsageIngestQueuePreservesNativeFallbackOrder(t *testing.T)
func TestUsageIngestQueueIsBoundedByCountAndBytes(t *testing.T)
func TestUsageIngestQueueDoesNotRetainRawCallback(t *testing.T)
func TestUsageIngestQueueOverflowDoesNotRunTaskInline(t *testing.T)
func TestUsageIngestQueueShutdownDrainsAcceptedTasks(t *testing.T)
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd go && go test -run 'TestUsageIngestQueue' -count=1`

Expected: FAIL because the current queue stores closures and executes overflow inline.

- [ ] **Step 3: Implement a compact queue**

```go
const (
	usageIngestQueueMaxTasks = 4096
	usageIngestQueueMaxBytes = 8 << 20
)

type usageIngestKind uint8

const (
	usageIngestNative usageIngestKind = iota
	usageIngestFallback
)

type usageIngestTask struct {
	kind   usageIngestKind
	stats  *RequestStatistics
	record UsageRecord
	bytes  int
}
```

Estimate retained bytes from compact string lengths and selected headers. Do not use original callback length after the raw payload has been discarded.

- [ ] **Step 4: Remove synchronous overflow execution**

When both count and byte limits reject a task:

- Increment `callback_queue_overflows` and `permanent_drops`.
- Return success to the host immediately.
- Do not parse again, block, sleep, open a file, write spool, or call `task()` inline.

This is the only policy compatible with fixed memory and minimum API impact under unbounded overload. Normal-load completeness is protected by making each task compact and raising capacity from 256 raw callbacks to 4096 completed records.

- [ ] **Step 5: Preserve shutdown drain and fallback ordering**

Use one worker for native and fallback ingest ordering. SQLite remains separately asynchronous through `eventWriterQueue`.

- [ ] **Step 6: Run queue, dedup, and race tests**

Run: `cd go && go test -run 'TestUsageIngestQueue|TestResponseInterceptFallback|TestResponseStreamChunkDoesNotDoubleCount|TestFallbackAuthIndex' -count=1`

Run on Linux with gcc: `cd go && go test -race ./...`

Expected: PASS with zero queue overflow in all normal-load tests.

- [ ] **Step 7: Commit compact ingestion**

```bash
git add go/usage_async.go go/usage_async_test.go go/management.go go/response_intercept.go go/types.go go/stats.go
git commit -m "perf: queue compact usage records without callback blocking"
```

---

### Task 6: Batch SQLite Writes Without Delaying API Responses

**Files:**
- Modify: `go/event_writer.go`
- Modify: `go/event_store.go`
- Create: `go/event_writer_performance_test.go`
- Modify: `go/types.go`
- Modify: `go/stats.go`

**Interfaces:**
- Produces: `collectEventWriteBatchUntil(queue, first, 20*time.Millisecond, 128)`.
- Consumes compact event tasks from `RequestStatistics.Record`.

- [ ] **Step 1: Add failing batching tests**

```go
func TestEventWriterWaitsBrieflyToCoalesceBatch(t *testing.T)
func TestEventWriterFlushesImmediatelyAtBatchLimit(t *testing.T)
func TestEventWriterShutdownFlushesPartialBatch(t *testing.T)
func TestEventWriterBatchingDoesNotRunSQLiteOnCaller(t *testing.T)
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `cd go && go test -run 'TestEventWriter.*Batch|TestEventWriterWaits|TestEventWriterShutdown' -count=1`

Expected: FAIL because the writer currently drains only immediately available tasks.

- [ ] **Step 3: Implement a maximum 20 ms coalescing window**

```go
const (
	eventWriteBatchMaxWait = 20 * time.Millisecond
	eventWriteBatchMaxSize = 128
)
```

Start the timer only after receiving the first task. Flush when the batch reaches 128, the 20 ms timer expires, or shutdown begins. Reuse the timer safely and stop/drain it before reset.

- [ ] **Step 4: Keep all durability work on workers**

The API path performs only the in-memory aggregate update and non-blocking enqueue. SQLite insertion, prune, aggregate snapshot, spool write, and file-size stat remain in `eventWriterLoop` or `eventSpoolLoop`.

- [ ] **Step 5: Avoid unnecessary work per batch**

- Prepare one insert statement per transaction, as currently implemented.
- Run retention at most once per minute.
- Run max-details pruning only when retention is disabled.
- Read database size at most once per 30 seconds instead of after every batch.
- Save aggregate state at 1000 accepted records, 300 seconds, retention changes, or clean shutdown.

- [ ] **Step 6: Run storage tests and benchmark**

Run: `cd go && go test -run 'TestStorage|TestEventWriter|TestStorageSnapshot|TestStorageReplay' -count=1`

Run: `cd go && go test -run '^$' -bench BenchmarkRecordSQLiteBatch128 -benchmem -count=5`

Acceptance: at 128 queued events, transaction count is one and per-record SQLite CPU is at least 50% below the Task 1 baseline.

- [ ] **Step 7: Commit SQLite batching**

```bash
git add go/event_writer.go go/event_store.go go/event_writer_performance_test.go go/types.go go/stats.go
git commit -m "perf: coalesce SQLite event writes off the API path"
```

---

### Task 7: Bound SQLite, Fallback, Cache, and Header Memory

**Files:**
- Modify: `go/event_store.go`
- Modify: `go/response_intercept.go`
- Modify: `go/stats.go`
- Modify: `go/dashboard_test.go`
- Modify: `go/main_test.go`

**Interfaces:**
- Produces byte-bounded fallback metrics and reduced database/cache defaults.
- Consumes the existing configured response-header whitelist.

- [ ] **Step 1: Add failing memory-bound tests**

```go
func TestFallbackCoordinatorBoundsRetainedBytes(t *testing.T)
func TestFallbackCoordinatorDropsUnselectedHeadersBeforeRetention(t *testing.T)
func TestSQLiteConnectionAndCacheLimits(t *testing.T)
func TestDashboardCachesRemainWithinReducedLimits(t *testing.T)
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd go && go test -run 'TestFallbackCoordinatorBounds|TestFallbackCoordinatorDrops|TestSQLiteConnection|TestDashboardCachesRemain' -count=1`

Expected: FAIL with current fallback records, four reader connections, and cache limits.

- [ ] **Step 3: Filter headers before fallback retention**

Apply `headerWhitelist` while constructing the compact record. When `log_response_headers` is empty, set `ResponseHeaders` to nil before the record enters pending, nativeRecent, fallbackRecent, the usage queue, or the SQLite queue.

- [ ] **Step 4: Add fallback byte limits**

Keep existing count limits as secondary guards and add:

```go
const maxUsageFallbackRetainedBytes = 8 << 20
```

Track pending, nativeRecent, and fallbackRecent retained bytes. Expire by TTL first, then evict oldest entries if the byte cap would be exceeded. Report evictions separately from confirmed permanent event drops because native usage may still arrive.

- [ ] **Step 5: Reduce SQLite per-process cache for personal workloads**

Use one writer and two reader connections:

```go
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
readDB.SetMaxOpenConns(2)
readDB.SetMaxIdleConns(2)
```

Set `PRAGMA cache_size = -2048` on every connection. Retain WAL, NORMAL synchronous mode, foreign keys, and the 5-second busy timeout.

- [ ] **Step 6: Reduce dashboard cache copies**

```go
const (
	dashboardEventCacheMax        = 8
	dashboardSummaryRangeCacheMax = 4
)
```

Do not cache synchronous exports. Continue deep-copying selected headers in cached event pages so callers cannot mutate shared state.

- [ ] **Step 7: Run focused and complete Go tests**

Run: `cd go && go test -run 'TestFallback|TestSQLite|TestDashboard.*Cache|TestRuntimeStatus' -count=1`

Run: `cd go && go test ./...`

Expected: PASS.

- [ ] **Step 8: Commit memory bounds**

```bash
git add go/event_store.go go/response_intercept.go go/stats.go go/dashboard_test.go go/main_test.go
git commit -m "perf: bound fallback SQLite and dashboard memory"
```

---

### Task 8: Make Dashboard Work Demand-Driven

**Files:**
- Modify: `go/dashboard/script.js`
- Modify: `go/dashboard/script.test.js`
- Modify: `go/dashboard.go`
- Modify: `go/dashboard_test.go`

**Interfaces:**
- Produces 60-second visible summary polling and version-gated detail refresh.
- Consumes existing ETag and `summary_version` behavior.

- [ ] **Step 1: Add failing polling tests**

```javascript
test('visible dashboard polls summary every 60 seconds', async () => {})
test('unchanged summary version does not query events or api detail', async () => {})
test('hidden dashboard polls no faster than every 5 minutes', async () => {})
test('events and api detail refresh only while their section is visible', async () => {})
```

- [ ] **Step 2: Run JavaScript tests and verify they fail**

Run: `node --test go/dashboard/script.test.js`

Expected: FAIL because the visible interval is currently 30 seconds and detail visibility is not used as a query gate.

- [ ] **Step 3: Change polling constants and detail gates**

```javascript
const visiblePollDelayMs = 60000;
const hiddenPollDelayMs = 300000;
```

Always poll the lightweight summary. Query events and API detail only when `summary_version` changes and the corresponding section is visible, or when the user explicitly refreshes, changes a filter, changes range, or changes page.

- [ ] **Step 4: Preserve cached UI while refreshing**

Do not blank charts, events, or API details during conditional refresh. Continue sending ETags and treating 304 as a cache hit.

- [ ] **Step 5: Run JavaScript and dashboard Go tests**

Run: `node --test go/dashboard/script.test.js`

Run: `cd go && go test -run 'TestDashboard|TestSummaryETag|TestEventsRangeETag|TestAPIDetailETag' -count=1`

Expected: PASS.

- [ ] **Step 6: Commit demand-driven polling**

```bash
git add go/dashboard/script.js go/dashboard/script.test.js go/dashboard.go go/dashboard_test.go
git commit -m "perf: reduce dashboard polling and hidden detail queries"
```

---

### Task 9: Configuration Cleanup, Production Profiling, and Release Gate

**Files:**
- Modify: `config.example.yaml`
- Modify: `README.md`
- Modify: `docs/releases/changelog.md`
- Modify: `go/register.go`
- Modify: `go/main_test.go`

**Interfaces:**
- Documents actual SQLite behavior and runtime metrics.
- Removes stale claims for ignored legacy JSONL timing settings.

- [ ] **Step 1: Add a failing configuration contract test**

Add `TestRegisterDoesNotAdvertiseIgnoredLegacyStorageTimingFields` and verify `storage_flush_interval_seconds`, `storage_snapshot_interval_seconds`, and `storage_snapshot_record_interval` are not advertised as active SQLite tuning controls.

- [ ] **Step 2: Run the configuration test and verify its current result**

Run: `cd go && go test -run TestRegisterDoesNotAdvertiseIgnoredLegacyStorageTimingFields -count=1`

Expected before documentation cleanup: the test exposes any stale registration or example references that imply these values control SQLite.

- [ ] **Step 3: Document the fixed low-overhead behavior**

Document:

- Stream callbacks use current Body fast path and one terminal History scan.
- SQLite batches for at most 20 ms and 128 events.
- Runtime queues are bounded by count and bytes.
- Dashboard polls summary every 60 seconds while visible.
- Overflow counters indicate extreme overload.
- Frequent `debug.FreeOSMemory`, reduced `GOGC`, and forced GC are intentionally not used because they increase CPU and latency.

- [ ] **Step 4: Run the full local verification suite**

```bash
cd go
go test -count=1 ./...
go vet ./...
go test -run '^$' -bench 'BenchmarkStreamCallback|BenchmarkRecordSQLiteBatch128' -benchmem -count=5
node --test dashboard/script.test.js
```

Expected: all tests pass and benchmark acceptance thresholds from Tasks 2, 4, and 6 are met.

- [ ] **Step 5: Run Linux race and production pprof verification**

On the VPS, temporarily enable localhost-only pprof and collect equivalent 30-second profiles with the plugin disabled, old plugin enabled, and optimized plugin enabled:

```bash
go tool pprof -top 'http://127.0.0.1:8316/debug/pprof/profile?seconds=30'
go tool pprof -top -inuse_space 'http://127.0.0.1:8316/debug/pprof/heap'
cd go && go test -race ./...
```

Use the same provider, model, request prompt, output length, concurrency, open/closed Dashboard state, and 10-minute measurement window for each comparison.

- [ ] **Step 6: Enforce release acceptance criteria**

Release only when all conditions hold:

- `detail_count`, `total_requests`, usage-origin counters, and upstream comparison show no unexplained missing normal-load events.
- No duplicate event is created for native plus fallback usage.
- Ordinary stream callback P95 is below 1 ms on the VPS.
- Plugin-enabled idle CPU is within 2 percentage points of plugin-disabled idle CPU.
- Under representative streaming load, total process CPU is at least 60% below the old-plugin measurement.
- Plugin-attributable steady heap is below 128 MiB for a personal 7-day database and does not grow monotonically after traffic stops.
- Callback, fallback, event-writer, and spool queue lengths return to zero after traffic stops.
- `callback_queue_overflows`, `permanent_drops`, and `spool_limit_drops` remain zero during representative load.
- API time-to-first-byte and stream completion latency regress by less than 1% or 1 ms, whichever is larger.

- [ ] **Step 7: Commit documentation and release evidence**

```bash
git add config.example.yaml README.md docs/releases/changelog.md go/register.go go/main_test.go
git commit -m "docs: document low-overhead usage collection behavior"
```

## Self-Review

- Spec coverage: CPU, heap, API callback latency, SQLite work, fallback correctness, dashboard load, overload behavior, observability, and release validation each have an implementing task.
- Data integrity: native, Body, History-only, malformed, no-ID, concurrent equal-token, shutdown, queue saturation, SQLite failure, and retention paths are explicitly tested.
- Resource bounds: raw payload retention is prohibited; stream state, fallback state, callback queue, SQLite pools, and dashboard caches all have exact caps.
- API-path guarantee: the normal stream path performs bounded parsing only; file and SQLite operations are worker-only; overflow never invokes synchronous persistence.
- Tradeoff review: absolute zero loss, fixed memory, and zero blocking cannot coexist under unbounded overload. The selected policy preserves fixed memory and API availability, reports every rejected record, and sizes the compact queue so normal personal workloads do not overflow.
