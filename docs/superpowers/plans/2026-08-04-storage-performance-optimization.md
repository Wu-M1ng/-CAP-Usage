# Storage Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce SQLite contention, write amplification, range-query CPU, and fallback timer memory while preserving every accepted usage record, import compatibility, aggregate accuracy, and native/fallback deduplication.

**Architecture:** Keep `RequestStatistics.mu` exclusively for in-memory state and keep SQLite transactions independent of it. A record is inserted and its durable aggregate watermark is committed as one SQLite unit, while the in-memory aggregate is updated outside the database critical section with rollback/rebuild behavior for failures. SQLite remains the source of truth for event details; aggregate state is a compact recovery checkpoint with an explicit event-id watermark. Dashboard range summaries use SQL aggregate queries for scalar totals and only materialize bounded rows for dimensions that cannot be represented by the existing aggregate tables. The response fallback coordinator uses one scheduler goroutine and a deadline heap instead of one runtime timer per fallback.

**Tech Stack:** Go, `database/sql`, `modernc.org/sqlite`, Go `testing`, SQLite WAL, Node dashboard tests.

## Global Constraints

- Preserve the existing public plugin protocol, dashboard JSON shapes, resource timestamps, endpoint inference, API-key masking, import deduplication, retention semantics, and fallback/native reconciliation.
- Never hold `RequestStatistics.mu` while waiting on SQLite, beginning/committing a transaction, executing a query, or closing a store.
- Accepted usage callbacks are not dropped. Queue backpressure remains bounded by `usageCallbackQueueMaxTasks` and `usageCallbackQueueMaxBytes`.
- Existing databases at `eventStoreSchemaVersion = 1` must open without migration failure; new watermark columns/tables require an additive migration and zero-value compatibility for legacy rows.
- Keep `go/event_store.go` user changes, including endpoint inference and event-query compatibility, and do not revert unrelated worktree files.
- Every task must be formatted with `gofmt`; run focused tests before the full suite.

---

### Task 1: Remove SQLite and Statistics Lock Inversion

**Files:**
- Modify: `go/stats.go` in `Record`, `EnrichRecordedUsage`, `pruneSQLiteLocked`, `mergeSnapshotSQLite`, `SummaryWithoutDetailsForRangeAndClientAPIAt`, `buildSummaryWithoutDetailsLocked`, `buildSummaryWithoutDetailsForRangeLocked`, `currentDetailCountLocked`, and `storageStatusLocked`.
- Modify: `go/event_store.go` only where a read-only helper is needed to return counts or metadata without coupling to `RequestStatistics.mu`.
- Test: `go/dashboard_test.go` and `go/main_test.go` for concurrent recording and dashboard/status reads.

**Interfaces:**
- `Record` must use a two-phase flow: snapshot configuration under `s.mu.RLock`, perform SQLite work without `s.mu`, then apply the in-memory delta under `s.mu.Lock`.
- `currentDetailCountLocked` and `storageStatusLocked` must read an atomically maintained in-memory event count, not query SQLite. The count is updated after a successful SQLite commit and restored on rollback.
- Slow storage metadata reads used by dashboard responses must happen before acquiring the final summary lock or after copying the in-memory state, then be injected into the response.

- [x] **Step 1: Add a failing lock-order regression test.**

Create a test that starts a recording goroutine and repeatedly calls `SummaryWithoutDetailsAt` and `RuntimeStatus` while a one-connection SQLite store is active. Use a bounded timeout and assert both the recorder and readers finish. Add a test-only event-store hook or a blocked transaction to force the two paths to overlap; the test must fail against the current lock order by timing out.

- [x] **Step 2: Add the in-memory event count and explicit lock-free store status snapshot.**

Add fields such as `eventCount int64` and a small `storageStatusSnapshot` value to `RequestStatistics`. Update `eventCount` only when an event transaction commits, subtract rows removed by pruning, initialize it from `eventStore.count` during store activation, and make `currentDetailCountLocked` return the maintained value. `storageStatusLocked` may format already-copied values but must not call `store.count` or any other SQLite method.

- [x] **Step 3: Refactor `Record` into database phase and memory phase.**

Read `eventStore`, header whitelist, retention, and max-details configuration under short locks. Insert, prune, and prepare the aggregate checkpoint without `s.mu`; commit the transaction. After commit, apply the corresponding in-memory record and retention removals under `s.mu`. If the in-memory update fails, rebuild from the committed aggregate/event state rather than opening a nested SQLite wait while holding `s.mu`.

- [x] **Step 4: Move snapshot/status SQLite reads outside summary locks.**

Change callers to copy the store pointer and needed counters first, perform `count`/metadata reads without `s.mu`, and then build or annotate the dashboard result. Keep generated timestamps and ETags stable for the same query window.

- [x] **Step 5: Run focused and race-oriented tests.**

Run:

```powershell
go test -count=1 ./... -run 'TestDashboard|TestRuntimeStatus|TestRecord|TestStorage'
go test -count=1 -run 'TestConcurrent|TestDashboard' .
```

Expected: no timeout, no data race, and detail counts remain equal to committed SQLite rows after pruning.

### Task 2: Batch SQLite Event Writes

**Files:**
- Modify: `go/event_store.go` to add a transaction-level batch insert API and shared insert-result structure.
- Modify: `go/stats.go` in `Record`, `mergeSnapshotSQLite`, and storage/import paths that currently begin one transaction per event.
- Test: `go/main_test.go` and `go/dashboard_test.go` for batch atomicity and duplicate handling.

**Interfaces:**
- Add `insertEventsTx(ctx context.Context, tx *sql.Tx, rows []eventInsertRequest) ([]eventInsertResult, error)` where each result contains the inserted id and `Added` flag.
- The batch API must preserve exact fingerprint deduplication, pending-enrichment consumption, event timestamps, and insertion order.

- [x] **Step 1: Add batch tests.**

Insert a batch containing two unique events and one exact duplicate, assert `Added == [true, true, false]`, assert one transaction leaves exactly two rows, and force one invalid row to assert the whole batch rolls back.

- [x] **Step 2: Implement shared prepared-statement batch insertion.**

Prepare the exact-duplicate lookup and event insert statements once per transaction. Process rows in input order, consume pending enrichment in the same transaction, and return per-row ids/results. Keep the existing single-row method as a one-element wrapper.

- [x] **Step 3: Route import and queued writes through the batch API.**

Group `mergeSnapshotSQLite` imported events into batches no larger than `defaultStorageWriteBatchSize`; insert all batches before one prune and one aggregate checkpoint. Preserve result counts and rollback metadata. Keep live `Record` atomic with its own event and checkpoint.

- [x] **Step 4: Verify write reduction and semantics.**

Run:

```powershell
go test -count=1 ./... -run 'TestMergeSnapshot|TestStorage|TestRecord|TestImport'
go test -count=1 ./...
```

### Task 3: Durable Aggregate Event-ID Watermark

**Files:**
- Modify: `go/event_store.go` schema, schema migration, aggregate load/save, and event query helpers.
- Modify: `go/stats.go` startup restore/rebuild paths and aggregate checkpoint calls.
- Test: `go/main_test.go` for crash/reopen recovery, legacy databases, and trimmed details.

**Interfaces:**
- Extend `aggregate_state` with `last_event_id INTEGER NOT NULL DEFAULT 0` (or an additive companion table if the installed SQLite schema requires it).
- `loadAggregate` returns the snapshot plus `lastEventID`; `saveAggregateTx` accepts the watermark for the checkpoint.
- Recovery replays only `request_events.id > last_event_id` after restoring the checkpoint, while a zero watermark means replay all events for legacy state.

- [x] **Step 1: Add compatibility tests before implementation.**

Create a version-1 database with aggregate JSON and event rows, open it, assert it loads with watermark zero and reconstructs all rows. Create a checkpoint with watermark `N`, add rows `N+1` and `N+2`, reopen, and assert top-level and per-dimension totals include both tail rows exactly once. Trim event details and verify aggregate totals still survive.

- [x] **Step 2: Add the additive schema migration.**

Keep `eventStoreSchemaVersion` compatibility. On an existing schema, inspect `PRAGMA table_info(aggregate_state)` and run `ALTER TABLE aggregate_state ADD COLUMN last_event_id INTEGER NOT NULL DEFAULT 0` when absent. New schemas include the column directly. Do not discard or rewrite existing state JSON.

- [x] **Step 3: Persist the maximum committed event id with each aggregate.**

After batch/event insertion and pruning, obtain the transaction-local maximum event id and write it with `aggregate_state` in the same transaction. If pruning deletes the newest detail for a model, the watermark still represents the durable event log and must not move backwards.

- [x] **Step 4: Use watermark-aware startup replay.**

Restore the aggregate snapshot first, then query rows whose id exceeds the saved watermark and apply only that tail. If the aggregate is missing, damaged, or has watermark zero, rebuild from all rows. Preserve legacy JSONL/snapshot migration behavior.

- [x] **Step 5: Verify crash and migration behavior.**

Run:

```powershell
go test -count=1 ./... -run 'TestStorageSnapshot|TestStorageReplay|TestSummaryWithoutDetails|TestLegacy'
go test -count=1 ./...
```

### Task 4: Make Detail Pruning Incremental and Targeted

**Files:**
- Modify: `go/event_store.go` in `pruneTx`, indexes, and pruning helpers.
- Modify: `go/stats.go` to pass the affected API/model keys and avoid a global window query on every live record.
- Test: `go/dashboard_test.go` and `go/main_test.go` for retention, per-model caps, and eviction counts.

**Interfaces:**
- Add a prune scope containing `cutoff`, `maxDetailsPerModel`, and affected `(api, model)` pairs.
- Retention deletion remains global only when the retention deadline has advanced since the last prune; per-model cap deletion is restricted to affected groups.

- [x] **Step 1: Add targeted-prune tests.**

Record events in two API/model groups, trigger a cap prune in one group, assert the other group is untouched, and assert `evicted_total` equals the number of deleted rows. Add a retention test that proves zero-timestamp events remain and expired timestamped rows are removed.

- [x] **Step 2: Add supporting indexes and prune state.**

Use the existing `(api, model, timestamp_ns, id)` ordering index or add the smallest missing index. Track the last retention-prune cutoff in the statistics state so live inserts do not repeat the global expired-row scan until the cutoff changes.

- [x] **Step 3: Replace global `ROW_NUMBER()` work for live records.**

For each affected group, select ids after the newest `maxDetailsPerModel` rows and delete only those ids. Keep the global window query as an import/configuration fallback when no affected scope is supplied. Collect removed details only when the in-memory aggregate needs to subtract them.

- [x] **Step 4: Verify pruning behavior.**

Run:

```powershell
go test -count=1 ./... -run 'TestRecordPrunes|TestEvictedTotal|TestRetention|TestMergeSnapshot|TestStorageSnapshot'
go test -count=1 ./...
```

### Task 5: Separate SQLite Reader and Writer Pools

**Files:**
- Modify: `go/event_store.go` `eventStore`, `openEventStore`, `database`, `close`, and transaction helpers.
- Test: `go/main_test.go` for concurrent readers/writer and clean shutdown.

**Interfaces:**
- `eventStore` exposes separate read and write `*sql.DB` handles. The writer remains single-connection; the reader pool has a bounded maximum and uses the same WAL database.
- Mutations always use the writer handle; dashboard queries/counts use the reader handle. Both handles close exactly once.

- [x] **Step 1: Add a concurrency test.**

Open a temporary store, seed events, run concurrent range/event/API-detail reads with writes, and assert all complete within a bounded timeout and the final committed count is correct.

- [x] **Step 2: Open bounded read/write handles.**

Configure writer `MaxOpenConns(1)` and reader `MaxOpenConns(4)` (or the configured bounded value), enable WAL and `busy_timeout` on both, and avoid sharing a transaction across handles. Keep `database()` as the writer accessor only for mutation callers and add `readDatabase()` for queries.

- [x] **Step 3: Route read-only methods and close both handles.**

Move `queryEvents`, `forEachEvent`, `queryAPIDetail`, `loadAggregate`, `count`, `isEmpty`, and status reads to the reader handle. `close` first detaches both handles under `eventStore.mu`, then closes them outside that mutex.

- [x] **Step 4: Verify.**

Run:

```powershell
go test -count=1 ./... -run 'TestDashboard|TestRuntimeStatus|TestStorage|TestEvent'
go vet ./...
```

### Task 6: Push Range Aggregation Into SQLite

**Files:**
- Modify: `go/event_store.go` with reusable SQL aggregate queries for usage, model/provider, API, endpoint, credential, client API, health, and time-series buckets.
- Modify: `go/stats.go` in `SummaryWithoutDetailsForRangeAndClientAPIAt` and range response assembly.
- Test: `go/dashboard_test.go` for range totals, zero timestamps, client API filtering, cache fields, and trimmed details.

**Interfaces:**
- Add a `queryDashboardRange(ctx, EventsQuery, now) (dashboardRangeQueryResult, error)` result containing SQL-computed scalar/dimension aggregates and only the bounded recent-event data needed by the response.
- Use the existing `eventTotalTokensSQL`, `eventCachedTokensSQL`, `eventQueryWhere`, and `dashboardLocalDayHour` timezone semantics; zero timestamps are excluded from finite ranges but remain valid for `all`.

- [x] **Step 1: Add differential tests.**

Build a fixture with recent, old, zero-timestamp, failed, cached, cache-write, multiple-provider, multiple-endpoint, and multiple-client-API events. Compare SQL range output with the current full-detail aggregation for `all`, `24h`, `7d`, and client API filters.

- [x] **Step 2: Implement SQL scalar and grouped aggregates.**

Use one read transaction and grouped queries for totals; use SQLite `strftime`/Unix timestamp conversion only for UTC bucket extraction, then convert bucket keys through the existing dashboard timezone helper. Keep price lookup in Go after token-part aggregates are returned so model-price behavior remains unchanged.

- [x] **Step 3: Replace the per-event range scan.**

Make the range summary use `queryDashboardRange` when SQLite is active. Keep the in-memory path for storage-disabled mode. Build response maps deterministically and preserve current sorting and empty-result behavior.

- [x] **Step 4: Verify output equivalence and cache behavior.**

Run:

```powershell
go test -count=1 ./... -run 'TestSummaryRange|TestAPIDetailRange|TestDashboard.*Range|TestClientAPI|TestCost'
go test -count=1 ./...
node --test go/dashboard/*.test.js
```

### Task 7: Centralize Fallback Scheduling

**Files:**
- Modify: `go/response_intercept.go` `usageFallbackCoordinator`, pending occurrence types, `Schedule`, `HandleNativeForStats`, `Supersede`, `Flush`, `commit`, and cleanup logic.
- Test: `go/main_test.go` and a focused fallback coordinator test file if the existing tests are too coupled to `main_test.go`.

**Interfaces:**
- `usageFallbackCoordinator` owns one scheduler goroutine, a wake channel, a stop channel, and a min-heap of pending deadlines. Each pending entry stores its deadline and index; no entry owns a `*time.Timer`.
- The scheduler removes due entries, commits them outside the coordinator mutex, and wakes on a newly earlier deadline. Native matching and superseding remain O(number of matching key entries), with expired recent occurrences compacted during scheduler ticks and public calls.

- [x] **Step 1: Add scheduler lifecycle and memory-bound tests.**

Schedule thousands of fallback records with a long delay, assert the coordinator has one scheduler loop and no per-record timer objects, then flush and assert all accepted records commit once. Test native cancellation, supersede, late-native enrichment, and close idempotence.

- [x] **Step 2: Implement the deadline heap.**

Use `container/heap` with `pendingUsageFallback.deadline` and `heapIndex`. `Schedule` pushes one item and signals only when it becomes the heap head. The scheduler waits on a timer for the head deadline, drains due items under lock, records fallback occurrences, and calls `Record` after unlocking.

- [x] **Step 3: Preserve reconciliation semantics.**

Replace timer stopping with heap removal/cancellation flags. Keep `pending` lookup maps for matching, but remove entries from both the map and heap exactly once. `Flush` closes scheduling, drains pending items synchronously in deadline-independent order, and waits for the scheduler to exit.

- [x] **Step 4: Verify fallback behavior and full regressions.**

Run:

```powershell
go test -count=1 ./... -run 'TestResponse|TestFallback|TestPending|TestUsageCallback'
go test -count=1 ./...
go vet ./...
node --test go/dashboard/*.test.js
```

### Final Verification

- [x] Run `gofmt -w` on every modified Go file.
- [x] Run `git diff --check` and inspect only intended files; preserve unrelated user files.
- [x] Run `go test -count=1 ./...`.
- [x] Run `go vet ./...`.
- [x] Run `node --test go/dashboard/helpers.test.js go/dashboard/script.test.js`.
- [x] Run `go test -race ./...` when the local toolchain has cgo enabled; if Windows lacks `gcc`, report that exact environment limitation and retain the non-race verification results.
