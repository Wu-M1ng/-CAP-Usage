# Low-Overhead Usage Statistics Implementation Plan

> **For agentic workers:** Execute the tasks in order and run the listed test gate after each task.

**Goal:** Keep usage accounting correct for the normal 2-3 concurrent request workload while reducing callback latency, CPU work, retained heap, and SQLite contention.

**Architecture:** The host callback only performs bounded inspection and publishes a compact owned record. In-memory aggregates remain the fast source for live dashboards, while one SQLite writer goroutine batches durable event writes and a separate bounded spool worker handles only writer failures. Store switches and reads use explicit lifecycle references so no database is closed while a query or write is active.

**Tech Stack:** Go, `database/sql`, `modernc.org/sqlite`, JSONL failure spool, Node dashboard tests.

## Global Constraints

- Do not perform SQLite I/O, spool file I/O, `fsync`, or unbounded parsing on an API callback goroutine.
- Normal traffic must not drop records; overload must be bounded and exposed through runtime counters instead of silently blocking the upstream callback.
- Preserve native usage, response `Body` usage, `HistoryChunks`-only usage, request-level stream correlation, deduplication, retention, import/export, and dashboard query behavior.
- Keep callback and writer memory bounded by both task count and retained payload bytes.
- Keep SQLite writes serialized, reads bounded, and all database operations under explicit context deadlines.
- Do not force `runtime.GC` or `debug.FreeOSMemory`; avoid trading CPU and latency for a lower RSS watermark.
- Preserve unrelated dirty files in the workspace and edit only files belonging to this plugin and this change.

---

### Task 1: Establish the baseline and lock-order contract

**Files:**
- Inspect: `go/stats.go`, `go/event_writer.go`, `go/event_store.go`, `go/usage_async.go`, `go/response_intercept.go`
- Test: `go/performance_test.go`, `go/event_writer_performance_test.go`, `go/usage_async_test.go`
- Document: this plan

**Interfaces:**
- The lock contract is `storageControlMu -> eventRecordMu -> s.mu` when more than one of these locks is required.
- A function that already holds `storageControlMu` must call only `*Locked` helpers that do not acquire it again.
- A function that only queries SQLite must acquire a store reference and increment `storageQueryWG` before releasing `storageControlMu`.

- [ ] Run `gofmt -w go/*.go`.
- [ ] Run `go test -count=1 ./...` and `go vet ./...` from `cpa-usage-plugin-main/go`.
- [ ] Run the stream and SQLite benchmarks with `-benchmem` and record the output.
- [ ] Use `rg` to list every `storageControlMu.Lock`, `eventRecordMu.Lock`, and `s.mu.Lock` before editing, then classify each call site by the contract above.

### Task 2: Make lock ordering consistent

**Files:**
- Modify: `go/stats.go`
- Modify: `go/event_writer.go`
- Modify: `go/event_store.go` only if a store lifecycle helper requires a lock-safe adjustment
- Test: `go/main_test.go` or a focused new Go test in the plugin package

**Interfaces:**
- `Record` continues to publish the in-memory record and enqueue the durable task atomically with respect to store switching.
- `persistEventWriteGroup` may update aggregates only after the SQL transaction is prepared, but must never hold `s.mu` while running SQLite.
- `saveFinalEventWriterSnapshot`, spool replay, retention maintenance, and configuration switching use the same lock order.

- [ ] Add a focused concurrent test that repeatedly runs `Record`, a dashboard query, `ConfigurePatch` with the same storage path, and writer shutdown against a temporary store.
- [ ] Move any `eventRecordMu -> storageControlMu` path to `storageControlMu -> eventRecordMu`; where holding both is unnecessary, release the first lock before taking the second.
- [ ] Keep `s.mu` acquisition inside the ordered pair and never call a method that can acquire `storageControlMu` while holding `s.mu`.
- [ ] Run the focused test with `go test -race -run 'Test.*(Lock|Concurrent|Storage)' ./...`.

### Task 3: Bound queued task memory, including ordinary responses

**Files:**
- Modify: `go/usage_async.go`
- Modify: `go/response_intercept.go`
- Modify: `go/stream_callback.go`, `go/stream_state.go` only where the compact record contract is shared
- Test: `go/usage_async_test.go`, `go/stream_callback_test.go`, `go/performance_test.go`

**Interfaces:**
- Stream callbacks enqueue an owned compact `UsageRecord`, never the complete request or cumulative history.
- Non-stream response callbacks enqueue a compact envelope containing only the fields required by the later parser and enrichment path; the queue accounts for its owned byte size.
- `usageCallbackQueueMaxTasks` and `usageCallbackQueueMaxBytes` remain hard upper bounds.

- [ ] Add a test proving an ordinary response with a multi-megabyte body does not leave that body referenced by the callback queue after compact extraction.
- [ ] Replace any closure capture of the original request/body with an owned compact payload or a bounded byte copy.
- [ ] Add a per-task retained-byte limit so one large response cannot consume the entire queue budget; reject it through the existing explicit drop metrics without running storage work inline.
- [ ] Verify stream and non-stream callback paths preserve model, metadata, response ID, headers needed for provider detection, native usage, body usage, and history-only usage.
- [ ] Run `go test -run 'Test(Usage|Response|Stream)' ./...` and the callback benchmarks.

### Task 4: Add a byte-bounded SQLite writer queue and durable-failure accounting

**Files:**
- Modify: `go/event_writer.go`
- Modify: `go/stats.go`
- Modify: `go/types.go` if runtime status needs a new queue-byte field
- Test: `go/event_writer_performance_test.go`, `go/main_test.go`

**Interfaces:**
- Each `eventWriteTask` has a deterministic retained-byte estimate computed before enqueue.
- Enqueue and dequeue update both task count and byte counters under the existing queue lifecycle synchronization.
- SQLite failure accounting distinguishes “accepted by spool worker”, “spool limit rejected”, and “permanently dropped”.

- [ ] Add a bounded byte budget for the SQLite writer queue and expose current/capacity values in runtime status.
- [ ] Ensure `processEventWriteBatch` releases the exact task byte reservation on every success, SQLite failure, spool handoff, and shutdown path.
- [ ] Keep file opening, JSON encoding, writes, and sync operations on `eventSpoolLoop`; the host callback must remain non-blocking.
- [ ] Make spool replay stream records in bounded batches instead of loading the full spool into `[]persistedDetail` when the file is large.
- [ ] Align spool line size and replay behavior so a valid maximum-sized record is recoverable, while preserving the 64 MiB file cap.
- [ ] Add tests for queue byte saturation, successful spool handoff not incrementing `permanent_drops`, and replay of a large valid spool.

### Task 5: Reduce SQLite read/reload and export memory peaks

**Files:**
- Modify: `go/event_store.go`
- Modify: `go/stats.go`
- Modify: `go/dashboard_export_jobs.go`
- Test: `go/dashboard_test.go`, `go/main_test.go`

**Interfaces:**
- Query methods return bounded pages and return query errors distinctly from an empty result.
- Cursor-based export remains stable at a captured `snapshotAt` and does not use `OFFSET` for the async path.
- Store reload/rebuild consumes aggregate rows in bounded batches and does not materialize every detail more than once.

- [ ] Confirm every read path uses `readDB`, a context deadline, and the existing query wait-group reference.
- [ ] Change any full-event reload or derived-state rebuild to bounded iteration/batching while preserving aggregate totals and detail trimming.
- [ ] Ensure deleting an export job cancels its context before removing its file, and make the encoder check cancellation between pages and records.
- [ ] Keep JSON/CSV/gzip export streaming so query rows, encoded bytes, and response body are not all retained simultaneously.
- [ ] Add regression tests for query failure status, export cancellation, retention deletion during cursor export, and bounded reload.

### Task 6: Verify retention, stream settlement, and no-loss behavior

**Files:**
- Modify: only the implementation files required by failing tests from Tasks 2-5
- Test: `go/stream_state_test.go`, `go/stream_callback_test.go`, `go/main_test.go`, `go/event_writer_performance_test.go`

- [ ] Verify `HistoryChunks`-only terminal usage is counted exactly once.
- [ ] Verify two concurrent streams with identical model and token counts remain separate by response/request ID.
- [ ] Verify a normal 2-3 concurrent workload records every usage event in memory and SQLite after `waitForUsageCallbacks`.
- [ ] Verify retention maintenance removes expired rows without requiring a new request.
- [ ] Verify `event_count`, `detail_count`, `total_requests`, and `permanent_drops` remain internally consistent after writer failure and spool replay.
- [ ] Run `go test -race ./...` on Linux/VPS, because the local Windows build uses `CGO_ENABLED=0` and does not replace the deployment race gate.

### Task 7: Final performance gate and release notes

**Files:**
- Modify: `docs/releases/changelog.md` only when the implementation is complete
- Modify: version declarations discovered by `rg -n '2\\.5\\.' .`

- [ ] Run `gofmt -w go/*.go`.
- [ ] Run `go test -count=1 ./...`.
- [ ] Run `go test -race ./...` on the deployment platform.
- [ ] Run `go vet ./...`.
- [ ] Run `go test -run '^$' -bench 'BenchmarkStreamCallback|BenchmarkRecordSQLiteBatch128' -benchmem -count=5`.
- [ ] Run `node --test dashboard/script.test.js`.
- [ ] Compare callback benchmark allocations and duration with the Task 1 baseline; reject changes that increase callback work or retained memory without a correctness reason.
- [ ] Record the final queue limits, spool behavior, and expected loss semantics in the changelog.

## Self-review checklist

- Every requested goal has a task: CPU, memory, callback latency, queue bounds, SQLite contention, persistence, and no-loss behavior under normal concurrency.
- The plan does not change the data model semantics of native, Body, or History usage.
- All tests exercise both correctness and bounded-resource behavior.
- The only accepted loss path is explicit overload or unrecoverable storage/spool failure, and each such event is surfaced by counters.
