# SQLite 事件存储与看板分页实施计划

> For agentic workers: 使用 subagent-driven-development 或 executing-plans 按任务执行。每个步骤使用 checkbox 跟踪。

**目标：** 将请求事件明细迁移到 SQLite；请求事件明细和上游接口详情的最近请求都改为服务端分页，降低 Go 内存和浏览器数据量。

**架构：** SQLite 作为单条 RequestDetail 的唯一存储和查询源，Go 内存只保留累计统计、模型/来源/凭证聚合和有限缓存。storage_enabled=true 时直接打开 storage_path 指定的 SQLite 数据库；storage_enabled=false 时使用临时 SQLite 数据库并在关闭时删除。运行时不读取或迁移其他事件存储格式。前端只保存当前页，后端通过 COUNT、LIMIT、OFFSET 返回分页结果。

**技术栈：** Go 1.26、database/sql、modernc.org/sqlite、SQLite WAL、原生 HTML/CSS/JavaScript。

## 全局约束

- 本次改造对外只保留 SQLite 存储配置：storage_enabled 和 storage_path；数据库默认路径为 data/usage-statistics.db。
- modelStats.Details、eventIndex 及 API/model/source/auth 二级事件索引不再保存全量事件。
- retention_days 继续控制时间保留；max_details_per_model 改为 SQLite 中每个 api + model 的明细上限。
- dashboard-events 保持 limit、offset、range、model、source、auth、api、client_api 参数。
- dashboard-api-detail 新增 recent_offset，保留 recent_limit、error_limit、total_events。
- 事件接口默认每页 50 条；服务端最大值继续为 500；上游最近请求默认每页 50 条。
- SQL 全部使用参数绑定；API key 仍只保存脱敏值和 hash。
- ETag/cache key 必须包含事件 offset，以及上游详情 recent_offset。
- 普通看板、轮询、摘要和健康检查不得物化全量事件。
- usage import/export 的公开 JSON 结构保持兼容；SQLite schema 通过 user_version 管理。

## 自审查结论与必须遵守的实现约束

以下约束是对后续任务的补充，优先级高于“直接把内存明细替换成 SQL 查询”的字面实现：

- **事件行和累计统计分开持久化。** 当前行为中，`max_details_per_model` 删除最老明细时不会扣减 lifetime aggregate；只保存 `request_events` 行会导致重启后总请求数、模型统计和导入导出结果变小。因此 SQLite 还必须包含一个版本化的 `aggregate_state`（不含明细数组），并在事件插入、retention 删除、导入和配置触发的清理中，与事件变更放在同一事务内提交。启动时优先恢复该状态；仅在状态缺失的空库上从事件行重建。
- **`fingerprint` 不得做唯一约束。** 实时相同 usage 记录当前允许重复，`dedup_window_minutes` 不参与实时写入；`fingerprint` 只用于 enrichment 定位。显式 JSON import 需要在同一写事务内按完整规范化 dedup key 精确跳过已有记录，不能用“相同 fingerprint 永远只保留一条”替代。
- **enrichment 必须可跨顺序到达。** 若 metadata update 先于基础事件到达，SQLite 需要暂存 pending enrichment，并在基础事件插入时合并；否则现有 fallback/interceptor 的“先补写、后记录”行为会丢失。pending 状态也必须持久化，不能只放 Go map。
- **COUNT、聚合和当前页必须使用同一 SQLite 读事务。** `/dashboard-events` 的 `total` 与 `events`、`/dashboard-api-detail` 的摘要和 recent page 不能分别看到不同写入时刻。recent page 翻页还应复用按 `summaryVersion` 缓存的聚合结果，只重新查询当前页。
- **保留现有接口语义。** `SummaryWithoutDetails`、健康检查和普通轮询不得加载明细；但 `/dashboard-data`、`/usage`、`/usage/export` 仍需提供原有 `StatisticsSnapshot` JSON 兼容结构，显式调用时再从 SQLite cursor 物化明细。`export_max_records` 只限制看板事件导出，不限制 usage export。
- **配置切换必须原子化。** `ConfigurePatch`/`handleReconfigure` 在写锁下先打开并校验新 SQLite，成功后再切换；目标库为空时用 SQL cursor 将旧库事件和 aggregate state 复制过去，失败则保留旧库继续运行。`.jsonl` 路径直接返回配置错误并保持旧文件不动，不读取、不迁移、不删除旧格式。
- **默认值与用户配置一致。** `defaultRuntimeConfig`、register 返回值和 `config.example.yaml` 必须统一为 `max_details_per_model=1000`、`retention_days=7`、`dedup_window_minutes=1440`、`storage_enabled=true`、`storage_path=data/usage-statistics.db`。旧的 flush/snapshot/fsync/writer queue 配置字段从 runtime config、register、ExportConfig、StorageStatus 和文档中删除；dashboard 事件导出的 `format=jsonl` 仍是输出格式，不属于存储配置。

## 本次配置字段

以下是本次改造后对外暴露的字段；storage_path 必须是 SQLite 数据库路径，不再使用 JSONL 路径或文件分片配置。

| 字段 | 当前值 | 说明 |
| --- | ---: | --- |
| max_details_per_model | 1000 | 每个上游接口/模型最多保留的请求明细条数。 |
| retention_days | 7 | SQLite 事件和内存统计最多保留的天数，0 表示不按时间淘汰。 |
| dedup_window_minutes | 1440 | 兼容旧配置；导入仅跳过精确重复记录，实时 usage 和 SQLite 写入不按窗口去重。 |
| log_response_headers | 空 | 允许记录的响应头名称列表，逗号分隔，支持 *；留空不记录。 |
| api_key_hash_salt | 空 | API key 分组哈希的稳定 salt；留空使用插件默认 salt。 |
| storage_enabled | true | 是否启用 SQLite 文件持久化。 |
| storage_path | data/usage-statistics.db | SQLite 数据库路径。 |
| export_max_records | 100000 | 看板事件导出最多返回的明细条数，0 表示不限制。 |

## 文件职责

创建：

- go/event_store.go：SQLite schema、打开关闭、事件写入、查询、聚合和清理。
- go/event_store_test.go：SQLite 存储层测试。

修改：

- go/go.mod、go/go.sum：增加 modernc.org/sqlite。
- go/types.go、go/register.go：配置、存储状态、API detail 分页字段。
- go/stats.go：Record、enrichment、snapshot、导入、保留和查询改用 eventStore。
- go/dashboard.go：解析 recent_offset，更新 API detail ETag。
- go/dashboard.go、go/management.go、go/dashboard_export_jobs.go：兼容快照和直接/后台导出从 SQL 分页/流式读取。
- go/dashboard/index.html、script.js、style.css、i18n.js：两个分页控件和状态。
- go/dashboard_test.go、go/main_test.go、go/dashboard/*.test.js：回归测试。
- README.md、docs/guides/cpa-usage.md：更新 SQLite 配置和分页说明。

## Task 1：建立 SQLite 存储层

文件：go/event_store.go、go/event_store_test.go、go/go.mod、go/go.sum。

新增接口：

~~~go
func openEventStore(path string, temporary bool) (*eventStore, error)
func (s *eventStore) insertEvent(ctx context.Context, row eventRow, fingerprint string, exact bool, cutoff time.Time) (int64, bool, error)
func (s *eventStore) enrichEvent(ctx context.Context, fingerprint string, timestamp time.Time, update RequestDetail) (bool, error)
func (s *eventStore) queryEvents(ctx context.Context, q EventsQuery, now time.Time) (EventsResult, error)
func (s *eventStore) count(ctx context.Context) (int64, error)
func (s *eventStore) close() error
~~~

数据库初始化设置：

~~~sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA cache_size = -4096;
~~~

创建 request_events 表，字段包括：

- id、timestamp_ns、api、model、source、source_key、provider。
- auth_id、auth_index、auth_type、api_key、api_key_hash、api_key_label_hash。
- endpoint、base_url、stream、thinking_json、headers_json。
- input_tokens、output_tokens、reasoning_tokens、cached_tokens、cache_tokens、cache_write_tokens、total_tokens。
- latency_ms、ttft_ms、failed、status_code、failure、fingerprint、created_at_ns。
- `timestamp_ns` 配合 `timestamp_zero` 必须能区分 Go zero time 与 Unix epoch（zero time 使用 `timestamp_zero=1`，真实 Unix epoch 使用 `timestamp_zero=0,timestamp_ns=0`）。

另外创建：

- `aggregate_state(id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL, state_json TEXT NOT NULL, updated_at_ns INTEGER NOT NULL)`，只保存不含明细数组的累计状态、时间序列、健康桶和 `evictedTotal`。
- `pending_enrichments(fingerprint TEXT NOT NULL, timestamp_ns INTEGER NOT NULL, timestamp_zero INTEGER NOT NULL DEFAULT 0, update_json TEXT NOT NULL, updated_at_ns INTEGER NOT NULL, PRIMARY KEY (fingerprint, timestamp_ns, timestamp_zero))`，用于 metadata update 先到的情况。

创建索引：

~~~sql
CREATE INDEX idx_events_time ON request_events(timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_api_time ON request_events(api, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_model_time ON request_events(model, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_source_time ON request_events(source_key, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_auth_time ON request_events(auth_index, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_client_hash ON request_events(api_key_hash, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_client_label_hash ON request_events(api_key_label_hash, timestamp_ns DESC, id DESC);
CREATE INDEX idx_events_fingerprint ON request_events(fingerprint, timestamp_ns DESC, id DESC);
~~~

执行顺序：

1. 先写 schema、round-trip、并发写入和去重失败测试。
2. 运行 cd go; go test ./... -run TestEventStore -count=1，确认测试先失败。
3. 添加 driver、schema、eventRow 转换和参数化 insert。
4. fingerprint 使用现有 requestDedupKey 字段固定编码后计算 SHA-256。
5. `fingerprint` 只做 enrichment 定位且不建立 UNIQUE 约束；exact=true 仅用于显式 JSON import 的规范化 dedup key 检查，实时记录不按 dedup window 去重。
6. enrichEvent 只补空 endpoint、空 thinking 和 stream=false 字段。
7. 运行 go test -race ./... -run TestEventStore -count=1。

## Task 2：接入配置和 Record

文件：go/types.go、go/register.go、go/stats.go、go/event_store.go、go/main_test.go。

配置规则：

- storage_enabled=false：创建临时 SQLite 文件，Close 时删除数据库及 WAL/SHM 文件。
- storage_enabled=true：直接打开 storage_path 对应的 SQLite 数据库；父目录不存在时自动创建。
- storage_path 为空时使用 data/usage-statistics.db。
- storage_path 不接受 *.jsonl、日期分片目录或 snapshot.json 作为存储路径。

RequestStatistics 新增 eventStore、eventStorePath、eventStoreLastError、eventStoreLastWrite、droppedEvents。

Record 改造：

1. 构造 RequestDetail。
2. 在 eventStore 中完成 dedup 和 insert。
3. 在同一写事务内提交事件行、aggregate_state、保留清理和 pending enrichment；事务成功后才更新内存累计统计。
4. 删除 modelStats.Details append、seen 全量 map 和所有事件二级索引。
5. SQL 失败时不增加累计统计，记录 LastError 和 droppedEvents；Record、Import、Configure、Close 共用明确的写锁顺序，避免数据库 I/O 与 `s.mu` 互相等待。

EnrichRecordedUsage 使用 fingerprint + timestamp 更新 SQLite，不再扫描模型明细；找不到基础行时写入 pending enrichment，基础行稍后插入时合并。

启动流程：

1. 解析以上 9 个配置字段。
2. 创建或打开 SQLite，执行 schema migration 和 WAL 参数设置。
3. 从 SQLite 聚合恢复内存统计；数据库为空时从零开始。
4. metadata enrichment 直接更新 SQLite 行；找不到基础行时写入 pending enrichment，基础行稍后插入时合并。
5. 数据库 schema 版本写入 user_version，重复启动只执行未完成的 schema migration。

测试：

- 记录 100000 条事件后所有 modelStats.Details 长度为 0。
- SQL COUNT 为 100000，分页查询只返回当前 limit。
- 并发 exact import 的相同规范化 dedup key 只接受一条；并发实时相同 fingerprint 仍允许多条。
- 空数据库启动、重启恢复、schema migration 和 metadata enrichment 均通过。
- max-details 删除后重启仍保持 lifetime aggregate 计数；pending enrichment 先到后到两种顺序都通过。

## Task 3：SQL 事件分页和上游详情聚合

文件：go/types.go、go/stats.go、go/event_store.go、go/dashboard.go、go/dashboard_test.go、go/main_test.go。

APIDetailResponse 新增：

~~~go
RecentTotal  int
RecentLimit  int
RecentOffset int
~~~

新增方法：

~~~go
func (s *RequestStatistics) QueryAPIDetailPageAt(
    api, rangeKey, clientAPI string,
    recentLimit, recentOffset, errorLimit int,
    now time.Time,
) APIDetailResponse
~~~

现有 QueryAPIDetail 和 QueryAPIDetailForClientAPIAt 保留为 recentOffset=0 的兼容 wrapper。

事件 SQL：

~~~sql
SELECT <event columns>
FROM request_events
WHERE <same parameterized filters>
ORDER BY timestamp_ns DESC, api ASC, id DESC
LIMIT ? OFFSET ?;
~~~

COUNT 使用完全相同的 WHERE。range=7h/24h/7d 使用 `timestamp_zero=0` 且 timestamp_ns 不早于 cutoff；range=all 允许 zero timestamp。model、source、auth、api、client_api 全部按 AND 组合。无效 client_api selector 追加 1=0。

上游详情执行：

1. COUNT(*) 得到只表示 SQLite 可展示明细行数的 `RecentTotal`；`TotalEvents` 继续表示原有摘要口径，不能用 recent page 行数替代。
2. COUNT/SUM/AVG 得到 Summary。
3. GROUP BY model、provider 得到 ModelStats。
4. GROUP BY source_key、provider 得到 SourceStats。
5. failed=1 按 status_code、failure 聚合并应用 errorLimit。
6. 最近请求按 timestamp_ns DESC、model ASC、id DESC 应用 recentLimit/recentOffset；同一读事务内返回 `RecentTotal` 和当前页。

测试覆盖：

- 20 条事件的 limit=5/offset=5。
- offset 超界返回空 events 但 total 不变。
- 24h、model、source、auth、api、client_api 过滤。
- 125 条上游请求的 recent_offset=0/50/100/150。
- recent page 改变不影响 summary、model、source、error。
- 同一时间戳使用稳定 tie-breaker。

dashboard.go 解析非负 recent_offset；API detail ETag 加入 api、range、client_api、recent_limit、recent_offset、error_limit 和 summary version。
`QueryAPIDetailPageAt` 需要按 `(api, range, client_api, error_limit, summaryVersion)` 缓存聚合部分，分页请求只执行 recent page 查询；`range=all` 且未按 client_api 过滤时，摘要继续使用 aggregate_state 的 lifetime 口径，而 `RecentTotal` 只统计当前可展示明细行。

## Task 4：请求事件明细分页 UI

文件：go/dashboard/index.html、go/dashboard/script.js、go/dashboard/style.css、go/dashboard/i18n.js、go/dashboard/helpers.test.js、go/dashboard/script.test.js。

新增状态：

~~~javascript
const eventsPageSize = 50;
let eventsPageOffset = 0;
~~~

新增 eventsPrev、eventsNext、eventsPageLabel 控件。renderEvents 发送 limit=50 和当前 offset；renderEventsContent 只渲染当前页，不调用 fetchAllEventPages。

行为：

- previous 在 offset=0 时 disabled。
- next 在 offset + rows.length >= total 时 disabled。
- total 变化导致当前页越界时回退到最后有效页。
- model/source/auth/range/client_api/clear filters 统一把 offset 重置为 0。
- 请求失败只复用相同 URL 的缓存，不显示别页数据。
- 文案显示当前起止行、总数和总页数。

测试首次 offset=0、next offset=50、最后页按钮、筛选重置、空数据、错误缓存和四种语言。

## Task 5：上游接口详情最近请求分页 UI

文件：go/dashboard/script.js、go/dashboard/style.css、go/dashboard/i18n.js、go/dashboard/script.test.js。

新增状态和方法：

~~~javascript
const apiDetailRecentPageSize = 50;
let apiDetailRecentOffset = 0;
fetchApiDetailData(api, recentOffset);
renderApiDetailRecentPagination(detail);
~~~

请求增加 recent_limit=50、recent_offset。api detail cache key 必须包含 API、range、client_api、limit、offset。异步响应同时校验 selectedApi 和请求 offset，防止慢请求覆盖新页。

apiDetailRecentHtml 直接渲染 recent_events，不再 slice；优先使用 recent_total/recent_limit/recent_offset，旧响应缺字段时使用 total_events 和 offset=0。

切换 API、range、client_api 时 offset 归零；翻页只刷新最近请求区域，不重算模型、来源和错误统计。测试 125 条请求的第一页、第二页、最后页、返回上一页和 API 切换。

## Task 6：保留策略、导入导出和运行状态

文件：go/stats.go、go/event_store.go、go/management.go、go/dashboard_export_jobs.go、go/types.go、相关测试。

SQL prune：

1. transaction 内查询并删除 retention cutoff 之前的 rows。
2. retention 删除的行调用现有 decrementCounters，保持累计统计语义。
3. 使用 ROW_NUMBER() OVER (PARTITION BY api, model ORDER BY timestamp_ns DESC, id DESC) 删除超过 max_details_per_model 的最老行。
4. max-details 删除只增加 evictedTotal，不扣 lifetime aggregate，保持旧行为。
5. DetailCount 改为 SQL COUNT。

普通运行路径不再在 Go 内存中保留全量 `Details`。保留 `Snapshot()`/兼容快照的公开 JSON 结构，显式调用时通过 SQL cursor 构造兼容的 `StatisticsSnapshot`；`handleDashboardData`、`handleGetUsage` 和 `handleExportUsage` 都必须切换到该 SQL-backed snapshot。后台导出和直接导出都从 SQL cursor 分页读取，单页最多 5000 条；后台任务写临时文件，直接响应只保留协议要求的响应体内存。

StorageStatus 增加 backend、database_path、event_count、database_size_bytes、last_write_at、dropped_events；删除 flush、snapshot、sync、writer queue 等旧文件存储状态字段。

## Task 7：文档、性能和构建验证

文件：README.md、docs/guides/cpa-usage.md、现有测试和 CI 配置。

文档更新内容：

- 默认明细存 SQLite；storage_enabled=false 使用临时数据库。
- storage_enabled=true 使用 storage_path 指定的 .db。
- retention_days/max_details_per_model 作用于 SQL 行。
- dashboard-events 示例增加 offset。
- dashboard-api-detail 示例增加 recent_limit/recent_offset。
- 说明 recent_total、recent_limit、recent_offset。

验证命令：

~~~bash
cd go
go test -v -race ./...
go test -run '^$' -bench='Benchmark(RecordIncremental|RecordSQLite|Snapshot|SummaryWithoutDetails|QueryEvents|QueryEventsSQLite|QueryAPIDetail)' -benchtime=100ms -count=1 ./...
cd ..
node --check go/dashboard/helpers.js
node --check go/dashboard/script.js
node --test go/dashboard/*.test.js
~~~

增加 100000 条事件回归测试，只断言 SQL COUNT、Details slice 为空、分页不超过 limit，不使用固定 RSS 阈值。按 CI 验证 Linux amd64/arm64、macOS amd64/arm64、Windows amd64 的 c-shared 构建和 cliproxy_plugin_init 导出。

补充回归项：旧 JSONL 路径不被读取或修改、`.jsonl` 配置被拒绝、默认值与 register/config.example.yaml 一致、运行时切换数据库失败时旧库仍可写、max-details 后重启保留 lifetime aggregate、zero time 与 Unix epoch 区分、pending enrichment 先到后到、exact import 去重与 realtime 重复记录语义、兼容 `/dashboard-data`/`/usage`/`/usage/export` 仍返回明细，以及直接导出和后台导出都不先构造全量 `EventsResult`。

## 验收标准

- 普通运行不保留全量 RequestDetail 或事件二级索引。
- 两个页面都能独立上一页/下一页，筛选变化回到第一页。
- recent_offset 和事件 offset 都进入 ETag/cache key。
- retention、max-details、dedup、enrichment、zero timestamp、client_api selector 和脱敏行为测试通过。
- SQLite 重启恢复，导入导出兼容。
- go test -race、Node tests 和五个平台 c-shared 构建通过。

## 执行顺序

按 Task 1 到 Task 7 顺序执行。每个 Task 通过局部测试后提交一次；Task 3 完成后后端接口可独立验证，Task 5 完成后前端分页可独立验证，Task 7 作为最终验收。
