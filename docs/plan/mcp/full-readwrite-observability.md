# cubefs-mcp 读写/观测面补齐执行文档

## 背景与目标

S1.1–S1.3 只落地了 14 个 tool（ping/version/cluster_health/cluster_stat
+ bench rule list/get/trigger + bench task list/get/cancel
+ sync task list/get/cancel + sync_node_list），覆盖面只到 master REST
表面的 ~40%。

本阶段一次性补齐到 35 个 tool，让 LLM 端在 stdio MCP 协议下能完成 sync /
bench / syncnode 的全套读写、生命周期管理与观测调取，不再依赖人工跳到
dashboard 或 curl。

## 范围与边界

**做什么**
- 把 master 路由表里已注册、面向运维/LLM 的全部 sync / bench / syncnode
  endpoint 暴露成 MCP tool。
- 给 `/syncTask/export` 这种 NDJSON 流式响应单独加一个 forwarder，不要塞
  进通用 `rawJSONResult`。
- 给写 / 删除类 tool 在 description 里加 `DESTRUCTIVE` / `MUTATES` 前缀，
  让 MCP 客户端（Claude Desktop / Code）的人工确认提示能突出风险。
- `tools.Register` 重新分组，按 cluster / bench rule / bench task /
  sync rule / sync task / sync node 六类排列；每个新 tool 一个独立 .go
  文件，保持原有 ~30 行/文件的体量。
- 给两个新公共 helper（`forwardGetText` for NDJSON、`forwardPostJSON` for
  JSON body POST）补单测，覆盖空 body、非 2xx、非法 JSON、context cancel
  四类边界。

**不做什么**
- 不做 syncnode 直连 tool（`/metrics/bench` 是 Prometheus 文本，不适合
  LLM 直读；`bench_task_get` 已经把 stage 级聚合带回来，观测面够了）。
- 不做 master 内部 RPC（`/syncNode/dispatch`、`/syncNode/response`），
  这两个是 master↔syncnode 的协议层调用，对外没意义。
- 不在 mcp 本地做参数语义校验：master 已经有强校验（`bench_rule` 走
  DisallowUnknownFields，sync_rule 走 spec 包），mcp 只透传 body 与
  错误回执。
- 不在本阶段做镜像打包 / Claude Code 客户端注册 / 部署侧改造。这三步在
  "集成阶段"再做，是独立的可选 backlog。

## 当前结论

35 个 tool 覆盖以下能力面：

| 类别 | 现有 | 新增 | 合计 |
|------|------|------|------|
| 元 / 集群 | 4（ping / version / cluster_health / cluster_stat）| 0 | 4 |
| Bench Rule | 3（list / get / trigger）| 3（create / update / delete）| 6 |
| Bench Task | 3（list / get / cancel）| 2（retry / delete）| 5 |
| Sync Rule | 0 | 8（list / get / create / update / delete / pause / resume / trigger）| 8 |
| Sync Task | 3（list / get / cancel）| 3（retry / delete / export）| 6 |
| Sync Node | 1（list）| 4（decommission / drain / restore / tasks）| 5 |
| **合计** | **14** | **20** | **34** |

> 备注：`/syncNode/getQuota` 在 master 已废弃（路由不注册，仓内注释提示
> 走 inline quota），不暴露为 tool。

写操作总数从 3 涨到 14。每个写操作在 description 里以 `DESTRUCTIVE:` 或
`MUTATES:` 前缀开头，并写明 master 侧失败语义（"may reject depending on
state"、"force=true skips ..."）。

## 分阶段任务

### S2.1 — 公共 helper + 路由常量

- `tools.go` 增加 `forwardGetText`（NDJSON / 任意文本）和
  `forwardPostJSON`（带 JSON body 的 POST），与现有 `forwardGet` /
  `forwardPost` 同层；保留 readTimeout / writeTimeout 不变。
- 复用 `proto.SyncRule*` / `proto.SyncTask*` / `proto.SyncNode*` /
  `proto.BenchRule*` / `proto.BenchTask*` 常量；不要再硬编码 path
  字面量。

### S2.2 — Bench Rule CRUD

- `bench_rule_create.go` — POST `/benchRule/create` + body
- `bench_rule_update.go` — POST `/benchRule/update` + body
- `bench_rule_delete.go` — POST `/benchRule/delete?id=`（DESTRUCTIVE）

### S2.3 — Bench Task 写操作

- `bench_task_retry.go` — POST `/benchTask/retry?id=`
- `bench_task_delete.go` — POST `/benchTask/delete?id=`（DESTRUCTIVE）

### S2.4 — Sync Rule 全套（最大块）

- `sync_rule_list.go` — GET `/syncRule/list?state=`
- `sync_rule_get.go` — GET `/syncRule/get?id=`
- `sync_rule_create.go` — POST + body
- `sync_rule_update.go` — POST + body
- `sync_rule_delete.go` — POST `?id=`（DESTRUCTIVE）
- `sync_rule_pause.go` — POST `?id=`（MUTATES）
- `sync_rule_resume.go` — POST `?id=`（MUTATES）
- `sync_rule_trigger.go` — POST `?id=`（MUTATES — 同步触发）

### S2.5 — Sync Task 写操作

- `sync_task_retry.go` — POST `/syncTask/retry?id=`
- `sync_task_delete.go` — POST `/syncTask/delete?id=`（DESTRUCTIVE）
- `sync_task_export.go` — GET `/syncTask/export?since=RFC3339` →
  `forwardGetText`，response 直接返 NDJSON 文本（不做 JSON 解析）

### S2.6 — Sync Node 管控

- `sync_node_decommission.go` — POST `?addr=&force=`（DESTRUCTIVE）
- `sync_node_drain.go` — POST `?addr=`（MUTATES）
- `sync_node_restore.go` — POST `?addr=`（MUTATES）
- `sync_node_tasks.go` — GET `?addr=[&status=]`

### S2.7 — Register 总装 + 单测

- `tools.Register` 按类别重排，每类一个 section 注释。
- `tools/helpers_test.go` 覆盖 `forwardGetText` + `forwardPostJSON` 四
  类边界：空 body、5xx + body 回填、非法 utf-8、context cancel。

### S2.8 — 验证

- `go build ./cmd/cubefs-mcp` 通过
- `go vet ./cmd/cubefs-mcp/...` 0 warning
- `go test ./cmd/cubefs-mcp/...` 通过（含新增 helper 单测）

## 验收标准

- `cmd/cubefs-mcp/internal/tools/` 下有且只有 34 个 `registerXxx` 函数；
  `tools.go::Register` 完整调用。
- 每个新 tool 文件 ≤ 50 行（保持现有风格），无重复 forwarder 实现。
- 写操作 description 全部以 `DESTRUCTIVE:` 或 `MUTATES:` 开头。
- `proto` path 字符串零硬编码，全部走 `proto.XxxURL` 常量。
- build + vet + test 三件套通过。

## 风险与阻塞

- master 端 `bench_rule_retry` 写法以前没有；要确认 handler 是否齐全，
  否则 tool 调出来会拿到 404。**先 grep handler，handler 不存在的不上
  tool，并在文档里标 N/A。**
- `SyncTaskExport` 是 NDJSON streaming，master 端边写边 flush；mcp 现
  在 `io.ReadAll` 全 buffer，对大量历史 task 会一次拉很大的 body。本期
  接受（每个 task 几百字节，1 万条 ~ 几 MB），后续要做流式时改
  masterclient 的 Get 让它支持 reader 返回。
- 写操作没有撤销手段。MCP 客户端的 per-call confirm 是唯一防线；如果
  未来切换到自动接受模式，需要在 server 层加白名单。本期通过描述里
  `DESTRUCTIVE:` 标识 + 客户端确认覆盖。

## 当前进度

- [x] 路由表核对（master/http_server.go）
- [x] proto 常量核对（proto/admin_proto.go）
- [x] masterclient API 复用面确认
- [x] S2.1 helper / 常量
- [x] S2.2 bench rule CRUD
- [x] S2.3 bench task retry/delete
- [x] S2.4 sync rule 8 件套
- [x] S2.5 sync task retry/delete/export
- [x] S2.6 sync node 5 件套（含 sync_node_tasks，落到 5 个 register）
- [x] S2.7 Register + 单测
- [x] S2.8 build + vet + test

## 已完成事项

- `tools.go` 新增 `forwardGetText` / `forwardPostJSON` 两个公共 forwarder，复用既有 read/write timeout 与 `forwardError` 错误回执。
- 路径常量沿用现网代码风格，保留字面量（`/syncRule/*` / `/syncTask/*` / `/syncNode/*` / `/benchRule/*` / `/benchTask/*`），未引入额外的 `proto.*` 反向依赖（与现有 14 个 tool 一致）。
- 新增 20 个 tool 文件，每个 ≤ 50 行，全部按类别归档：
  - Bench Rule：`bench_rule_create / update / delete`（DESTRUCTIVE）
  - Bench Task：`bench_task_retry / delete`（DESTRUCTIVE）
  - Sync Rule：`sync_rule_list / get / create / update / delete / pause / resume / trigger`
  - Sync Task：`sync_task_retry / delete / export`（export 走 `forwardGetText`，NDJSON 不做 `json.Valid` 校验）
  - Sync Node：`sync_node_drain / restore / decommission / tasks`
- 所有写操作 description 以 `MUTATES:` 或 `DESTRUCTIVE:` 起头，并写明 master 侧失败语义（state 拒绝、`force=true` 孤儿任务等）。
- `tools.Register` 重排为 Meta / Cluster / Bench Rule / Bench Task / Sync Rule / Sync Task / Sync Node 六段，每段加注释说明覆盖面与风险标签。
- 新增 `tools/helpers_test.go`，覆盖 `forwardGetText` + `forwardPostJSON` 四个边界：空 body 短路、5xx + body 透传、NDJSON 文本直通、context cancel 经 `forwardError` 转 IsError。
- `go build ./cmd/cubefs-mcp/...`、`go vet ./cmd/cubefs-mcp/...`、`go test ./cmd/cubefs-mcp/...` 三件套全部通过；现有目录共 34 个 `registerXxx` 函数，与目标一致。

## 剩余事项 / 下一步

本阶段（S2）目标已达成，task 可收口。以下条目为可选 backlog，不在当前 PR 范围：

- 集成阶段：Makefile target、镜像打包、Claude Code 客户端 `claude_config` 注册（与部署侧解耦，独立 PR）。
- `/syncTask/export` 改为真正的流式（masterclient 的 Get 暴露 reader），消除大 export 一次性 buffer；本期接受 buffer 实现。
- 若未来 MCP 客户端切自动接受模式，需要在 server 层补写操作白名单 / 限速；本期通过 `DESTRUCTIVE:` 描述 + 客户端确认覆盖。
