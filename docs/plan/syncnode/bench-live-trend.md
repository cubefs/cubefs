# bench-live-trend — 测试管理中间趋势可观测增强

> 状态：草案（2026-05-24）→ 进入实现
> 范围：syncnode（指标）+ cubefs-dashboard（后端 prom 代理 + 前端独立『测试分析』页）
> 触发：上一轮 GPFS 测试在 dashboard 看不到中间趋势（latency / throughput / IOPS 随时间），仅有 stage 终态聚合结果

## 1. 背景与问题

测试管理（BenchTask/BenchRule）当前可观测能力：

| 路径 | 中间趋势 (per-op / per-interval) | 终态聚合 |
|------|-------------------------------|----------|
| S3 (`bench_s3.go`) | ✅ 已写 `/metrics/bench`：`syncnode_bench_op_latency_seconds` histogram per-op + `_bytes_total` / `_errors_total` / `_latency_class_seconds`（按 size class） | ✅ stage 聚合 → BenchStageResult |
| FIO/POSIX (`bench_posix.go`) | ❌ 仅 stdout `log.LogDebugf`，从未写 `/metrics/bench`（代码注释明确："histogram stays empty for fio paths"） | ✅ 解析 fio JSON+ 末端汇总 |
| IOR/mdtest (sidecar) | ❌ 子进程边界 + 单 shot postRun，不在本期 scope | ✅ |
| SDK (`bench_sdk.go`) | ✅ per-op | ✅ |

前端：
- `frontend/src/api/cfs/prometheus.js` 客户端期待 `/api/cfs/prometheus/query_range` —— **后端没有这个路由** (grep `backend/router/`、`backend/handler/` 均无 prometheus)
- 已有 `BenchLiveCharts.vue` / `BenchTrendChart.vue` / `BenchTrendsDrawer.vue` 接受静态 `:points` prop，未对接 PromQL
- 没有独立『测试分析』页面，drawer 空间小，多曲线 + 多 shard 对比时显得窘迫

结论：
- S3 trend 数据已经在 Prometheus，缺的只是前端把 PromQL 跑起来 + 后端代理通
- FIO trend 数据完全没有写到 Prometheus，需要 syncnode 侧把 fio `--status-interval` 已经产出的中间快照解析并 emit
- 跳出 drawer 单独做『测试分析』页，URL 锚定 `taskId`，可承载更丰富的图表

## 2. 目标与非目标

### 目标（本期）

1. FIO 路径写 `/metrics/bench`：每个 `--status-interval` 周期 emit 一次 latency p50/p95/p99 + throughput MB/s + IOPS + 累积 bytes/errors。
2. S3 路径维持现状（已有 per-op histogram 足够），不动既有指标契约。
3. dashboard 后端新增 `/api/cfs/prometheus/query_range` 透传代理（thin proxy → Prometheus HTTP `/api/v1/query_range`），走现有认证中间件。
4. dashboard 前端新增独立『测试分析』页（route `/cfs/test-analysis`）+ 顶部菜单入口，URL 带 `taskId / clusterName / shard / stage / range`，图表通过 PromQL 拉数据。
5. 既有 `BenchTrendsDrawer` / `BenchTaskDrawer` 加"展开完整分析 →"按钮跳新页。
6. GPFS 复测：能在新页上看到 latency / throughput / IOPS 中间曲线，时间分辨率 5s（与 `--status-interval` 默认一致）。

### 非目标（本期不做）

1. IOR / mdtest 的 sidecar 子进程改造（postRun 单 shot 没有 SSE / chunked 流，无法 emit 中间数据，需要更大动作，留下期）。
2. cubefs-dashboard 内置 Grafana 或图表面板嵌入 —— 我们只接 PromQL，不引入面板服务。
3. Pushgateway 集成（`BenchGlobalConfig.PushgatewayURL` 字段保留，本期不用）。
4. 历史任务回看（>30d）—— Prometheus 默认保留策略约束，本期只覆盖运行中 + 近期任务（保留期由 Prometheus 决定，本仓库不负责）。
5. 跨集群对比 —— 本期 PromQL 通过 `clusterName` URL 参定位单集群指标源。
6. 告警规则下发 / Alertmanager 配置 —— 不在本仓库范围。

## 3. 总体设计

```
┌──────────────────────────┐         ┌─────────────────────────────────┐
│ syncnode (cubefs)        │         │ cubefs-dashboard backend        │
│  - bench_posix.go       │         │  /api/cfs/prometheus/           │
│   ① fio stdout 流式解析   │         │    query_range  →  proxy        │
│   ② 调 ObserveFIOInterval│         │    → Prometheus HTTP API        │
│  - metrics.go (S3.5)     │         │                                 │
│   新增 6 个 fio gauge/cnt│         └────────────┬────────────────────┘
│  /metrics/bench (已有)   │                      │
└────────────┬─────────────┘                      │
             │ scrape (Prometheus)               │ query (PromQL)
             ▼                                    ▼
        ┌─────────────────────────────────────────────┐
        │ Prometheus (cluster-side, deploy 之外)       │
        └─────────────────────────────────────────────┘
                                                       │
                                                       ▼
                                       ┌──────────────────────────────┐
                                       │ cubefs-dashboard frontend    │
                                       │  顶部菜单：测试分析          │
                                       │  /cfs/test-analysis          │
                                       │  ?taskId=…&clusterName=…     │
                                       │  &shard=…&stage=…            │
                                       │  → queryRange(PromQL)        │
                                       │  → ECharts 折线/面积         │
                                       └──────────────────────────────┘
```

### 3.1 syncnode 指标契约（metrics.go 末尾追加 S3.5 锚点）

所有新指标 append-only、不改既有 label 集合，旧 dashboard 100% 兼容。

| 指标名 | 类型 | label | 含义 |
|--------|------|-------|------|
| `syncnode_bench_fio_interval_lat_p50_us` | Gauge | task_id, shard, stage, op | fio interval 内 clat p50（微秒） |
| `syncnode_bench_fio_interval_lat_p95_us` | Gauge | 同上 | p95 |
| `syncnode_bench_fio_interval_lat_p99_us` | Gauge | 同上 | p99 |
| `syncnode_bench_fio_interval_throughput_mbs` | Gauge | 同上 | interval 内 throughput MB/s |
| `syncnode_bench_fio_interval_iops` | Gauge | 同上 | interval 内 IOPS |
| `syncnode_bench_fio_interval_total_ios_total` | Counter | 同上 | 累积 IO 数（fio 内部计数） |
| `syncnode_bench_fio_interval_total_bytes_total` | Counter | 同上 | 累积字节数 |
| `syncnode_bench_fio_interval_errors_total` | Counter | 同上 | 累积 IO 错误数（来自 fio total_err） |

label `op` 取值：`read` / `write`（fio JSON 同时报告两路，每路独立 emit；只要任一非零就 emit）。

helper：
```go
func ObserveFIOInterval(taskID string, shard int, stage, op string,
    latP50Us, latP95Us, latP99Us float64,
    thrMBs, iops float64,
    totalIOs, totalBytes, errs int64)
```

`_total` 后缀的三个 Counter 使用 fio 累积值而非 delta —— Counter 必须单调递增；fio interval JSON 内 `total_ios` / `bw_bytes` 已经是 stage 内累积，可以直接 `Set(...)` 不可（Counter 不允许 Set）。

实际实现选 **Add(delta)** 方案：
- 进 helper 时与"上次该 (taskID, shard, stage, op) 的累积值"做 diff，diff > 0 才 Add，diff < 0（fio 重启 / 新 stage / overflow）则记录新基准不 Add；
- 模块内维护 `intervalCumState` map（key=`taskID|shard|stage|op`），mutex 保护，stage 结束时清理（通过 SetStageState Done/Failed 触发 cleanup）。

### 3.2 syncnode fio stdout 解析（bench_posix.go）

当前 `drainFIOStdout` 只 debug log。改造：

```
fio process
  --status-interval=5s
  --output-format=json+
  --output=resultFile      ← 最终 JSON 写入 resultFile（保持不变，用于 parseFIOResult）
  stdout                   ← 同时输出 interval 快照（每 5s 一个独立 JSON 对象）
   ↓
drainFIOStdout (新) brace-balanced JSON streaming parser
  ↓
   for each JSON object:
     - json.Unmarshal → fioJSONResult
     - 对每个 job, 取 Read/Write 各自的 iops/bw_bytes/total_ios/percentile
     - 调用 ObserveFIOInterval(taskID, shard, stage, op="read"|"write", ...)
```

要点：
1. **fio stdout 与 --output 的关系**：fio 文档明确 `--status-interval` 的快照走 **stderr** 还是 stdout 取决于版本与 `--eta` 设置。实测 cubefs-tools 镜像内 fio 4.x，当 `--output=` 设置时 `--status-interval` 的 JSON 快照走 **stdout**；当未设置 `--output=` 时所有 JSON 都走 stdout。当前代码 **同时** 设置了 `--output=resultFile`，所以 stdout 是干净的 interval JSON 流（最后会带一个 final summary，与 resultFile 末尾内容一致 —— 我们让 stdout 解析器忽略 final，因为 `parseFIOResult` 已读 resultFile）。
2. **JSON 分割**：interval 之间无明显分隔符。用 brace-balance 计数器，从字节流第一个 `{` 开始累加，遇到对应的 `}` 即为一个完整对象。
3. **错误鲁棒**：JSON 解析失败时 log warn 跳过（不阻塞 fio）；context cancel 时退出 goroutine。
4. **Mixed 路径暂不接**：`runFIOStageMixed` 内的 `fioRunnerImpl.run` 走 `cmd.Run()` 无 StdoutPipe，本期不动；改造单一组件路径已经覆盖最常见场景（GPFS 复测就是单一 stage）。Mixed 路径在下期统一调整。

### 3.3 dashboard 后端：Prometheus 代理 — 已落地（Phase 3 完成）

实际实现与 URL：

- `backend/config/config.go`：新增 `Prometheus *PrometheusConfig`（`Url string` + `TimeoutMs int`，`TimeoutMs=0` 走 15s 默认）。`Url` 为 base，不带 `/api/v1`。空值合法 → bench 实时曲线优雅失败，dashboard 其他功能不受影响。
- `backend/service/prometheus/client.go`：`Client.QueryRange(ctx, RangeQuery) (json.RawMessage, error)`，HTTP 直连 Prometheus `/api/v1/query_range`。响应解析够用即可：识别 `status != "success"` 与非 JSON 体并把上游 `errorType/error` 透到 Go error；`data` 块用 `json.RawMessage` 透传，零额外编解码。
- `backend/handler/prometheus/handler.go`：gin handler `QueryRange(c *gin.Context)`，按 form 校验 query/start/end/step（`step` 用 float64 兼容亚秒），用 `sync.Once` 懒构造 client（dashboard 启动顺序不依赖此项）。
- `backend/router/prometheus_router.go`：新文件，注册 `engine.Group(config.Conf.Prefix.Api + "/console").GET("/prometheus/query_range", ...)`。
- `backend/router/router.go`：`Register` 末尾追加 `new(prometheusRouter).Register(engine)`。
- **实际 URL：** `GET /api/cubefs/console/prometheus/query_range`（dashboard 的 `Prefix.Api = /api/cubefs`，cluster-independent group 是 `/console`）。原计划写的 `/api/cfs/prometheus/query_range` 是过时口径，已同步修正 `frontend/src/api/cfs/prometheus.js` 的 `RANGE_URL`。
- **响应口径调整：** 不再用 `c.Data` 原样透传 Prometheus body。dashboard 前端 `utils/ajax.js` 强校验 `{code:200,...}` 信封，对裸 Prometheus body 会直接抛错。所以 handler 走 `ginutils.Send(c, codes.OK.Code(), "", json.RawMessage(data))`，把 Prometheus 的 `data` 块（`{resultType, result}`）原封不动塞进信封 `data` 字段；前端 `queryRange()` 的"unwrap envelope"分支已经能正确识别这种 shape。
- `backend/conf/config.yml`：示例配置补 `prometheus: { url:, timeoutMs: 15000 }`，url 留空表示禁用。
- 鉴权：路由自动落在既有 auth 中间件保护下（与 `clusters` / `prometheus` 同 group）。

单元测试 `backend/service/prometheus/client_test.go`（6 个 case，全部通过）：
- `TestQueryRange_PassthroughHappyPath` — 用 `httptest.Server` mock Prometheus，断言 path、query、start、end、step 透传无误，data block 反序列化后 `resultType=matrix` + result 数组完整。
- `TestQueryRange_UpstreamError` — 上游 `status:error` + `errorType/error` 字段透到 Go error。
- `TestQueryRange_NonJSON5xx` — 502 + HTML 响应不 panic，错误信息带 snippet。
- `TestQueryRange_Validation` — 空 query / start=0 / end<start / step=0 全部被校验拒绝，且不发起 HTTP。
- `TestNew_RejectsEmptyAndInvalidURL` — 空字符串、无 scheme 的 URL 在构造期就报错。
- `TestNew_AppliesDefaultTimeout` — timeout=0 时回落到 15s 默认。

### 3.4 dashboard 前端：独立『测试分析』页

新增页面 `frontend/src/pages/cfs/testAnalysis/index.vue`：

```
┌─────────────────────────────────────────────────────────────────┐
│ 测试分析   [任务: gpfs-test-001 ▼] [shard: 0 ▼] [stage: rw ▼]    │
│           [时间范围: 最近 30 分钟 ▼]              [刷新 ▼ 15s]   │
├─────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────┐ ┌─────────────────────────┐         │
│ │ 延迟 P50/P95/P99 (µs)   │ │ 吞吐 (MB/s) 按 op 拆分  │         │
│ │  折线 × 3               │ │  read / write 区域图    │         │
│ └─────────────────────────┘ └─────────────────────────┘         │
│ ┌─────────────────────────┐ ┌─────────────────────────┐         │
│ │ IOPS 按 op              │ │ 错误率 (errors/sec)     │         │
│ │  read / write 折线      │ │  按 kind 堆叠           │         │
│ └─────────────────────────┘ └─────────────────────────┘         │
│ ┌─────────────────────────┐ ┌─────────────────────────┐         │
│ │ Stage 状态时间线        │ │ 跨 shard 对比 (selected │         │
│ │  甘特/泳道              │ │  metric, by shard)      │         │
│ └─────────────────────────┘ └─────────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

PromQL 模板（以 latency p99 为例）：
```
syncnode_bench_fio_interval_lat_p99_us{task_id="$taskId", shard="$shard", stage="$stage"}
```

（S3 路径同步给出 histogram 模板）
```
histogram_quantile(0.99, sum by (le, op) (
  rate(syncnode_bench_op_latency_seconds_bucket{task_id="$taskId", shard="$shard", stage="$stage"}[1m])
)) * 1e6   # → µs，便于和 fio 同图对齐
```

入口：
- 顶部菜单加一级『测试分析』（vue-router meta + nav config）；点击进入空状态，提示从 BenchTask 列表选任务
- BenchTrendsDrawer / BenchTaskDrawer 抽屉右上角加按钮"展开完整分析 →"，`router.push({ path: '/cfs/test-analysis', query: { taskId, clusterName, shard, stage } })`

shard 选择器：从 task detail API 拿 shard 列表（已有 BenchTaskDrawer 用到）。stage 选择器：从 task spec 取 FIOStages 名字。

刷新策略：默认 15s 轮询；时间范围在最近 5 / 30 / 60 分钟 / 24h 之间切换。轮询用 `setInterval` 守在组件 onBeforeUnmount 清掉。

## 4. 分阶段任务

| 阶段 | 仓库 | 任务 | 验收 |
|-----|------|------|------|
| Phase 1 | cubefs | metrics.go 追加 S3.5 锚点 + 6 个 fio interval gauge/counter + `ObserveFIOInterval` + cleanup helper | 单元测试：模拟 3 个连续 interval 调用，验证 gauge Set 与 counter delta 累加正确；本仓库 `go test ./syncnode/executor/...` 通过 |
| Phase 2 | cubefs | bench_posix.go: `drainFIOStdout` 改流式 brace-balance parser + 对每个 interval 对象调 ObserveFIOInterval | 单元测试：注入 fake reader 喂 2 个相邻 JSON 对象，断言 helper 调用次数与参数；端到端：test-hb 跑一次 fio stage，curl `/metrics/bench` 看到 `syncnode_bench_fio_interval_*` 序列 |
| Phase 3 | cubefs-dashboard | 后端：service/prometheus + handler/prometheus + router 接线 + config Prometheus.Url | 单元测试：service 用 httptest.Server mock Prometheus，断言 URL 与 query 透传正确；本地起 dashboard，curl `/api/cfs/prometheus/query_range?query=up&...` 返回 Prom 原生 JSON |
| Phase 4 | cubefs-dashboard | 前端：testAnalysis/index.vue + 路由 + 顶部菜单入口 + drawer 跳转按钮 | 浏览器手测：进入新页能选 task/shard/stage，6 个面板渲染；drawer 跳转 URL 带参；空数据态有友好提示 |
| Phase 5 | cubefs-deploy | bump `_envcommon/images.hcl` 中 cubefs / cubefs-dashboard tag → `make ENV=test-hb apply-syncnode` + dashboard | GPFS 复测：新页能看到 fio interval 中间曲线 |

提交策略（**carryover 硬约束**）：
- cubefs：每阶段独立 commit + push（feat / fix 前缀，附 plan 路径引用）
- cubefs-dashboard：按用户允许后再提交
- cubefs-deploy：**永远不提交** images.hcl 与其他改动；本地改、apply、丢

## 5. 风险与边界

| 风险 | 缓解 |
|------|------|
| fio 不同版本对 stdout 行为差异 | parser 鲁棒处理：JSON 解析失败 log warn 跳过；brace-balance 容错 |
| 高 cardinality (taskID × shard × stage × op = N×M×K×2)，长跑任务积累 | 既有指标已有同样维度风险，benchRegistry 隔离；stage Done/Failed 时 cleanup helper 触发 DeleteLabelValues |
| Counter delta 计算 ID 冲突（同 stage 重跑） | cumState key 加 stage-instance（暂用 (taskID|shard|stage|op)，stage 重跑前必须 SetStageState 触发 cleanup —— 既有逻辑已经在 stage 入口 SetStageRunning，在 cleanup helper 中 hook 即可） |
| Prometheus URL 多集群环境配置 | 一个 dashboard 对一个 cluster 一个 Prometheus；多集群通过 cluster 选择切换 dashboard 实例（与 dashboard 现有 cluster 模型一致），URL 走配置不走 query param |
| dashboard backend 路由权限漏配 | 落到既有 cluster_router / cfs_router 内沿用统一的 auth 中间件；新 handler 不绕开 |
| 顶部菜单字段命名 / 国际化 | 与 i18n/lang/{zh,en}/index.js 同步增加 'testAnalysis' 文案 |
| Mixed 路径 fio 子进程 `cmd.Run()` 无 stdout pipe，本期无中间趋势 | 文档明示 limitation；下期统一改造时把 mixed 切到 fioRunner 接口 + stdout pipe |

## 6. 验收清单

- [ ] cubefs：syncnode 启动后 `/metrics/bench` 包含 `syncnode_bench_fio_interval_*` 系列（任务运行时）
- [ ] cubefs：单元测试覆盖 ObserveFIOInterval 累计 delta 与 cleanup
- [ ] cubefs：bench_posix 单元测试覆盖 fake stdout 流解析
- [ ] cubefs-dashboard：`/api/cfs/prometheus/query_range` 200 + Prometheus 原生 JSON shape
- [ ] cubefs-dashboard：『测试分析』页能从顶部菜单进入；URL `?taskId=X&clusterName=Y&shard=0&stage=rw` 加载即出图
- [ ] cubefs-dashboard：BenchTrendsDrawer / BenchTaskDrawer 跳转按钮 work
- [ ] GPFS 实测：fio runtime 60s 期间，每 5s 一次新数据点出现在曲线上
