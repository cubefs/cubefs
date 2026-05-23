# Bench Rule BackendID/BackendEndpoint 校验下沉

**作用域**：master `api_service_bench.go` 的 create / update / trigger handler；`syncnode/spec/bench_rule.go` 新增方法；`syncnode/tasks/runner.go` 替换内联检查。
**关联事项**：`docs/plan/mcp/healthcheck-findings-fixes.md`（P0/P1/P2 已完成；本文档承接 P3）。

## 背景

2026-05-23 test-k3d 集群体检发现：

- `benchTask.list` 总数 5，全部 `status=failed`。
- 5 条全部来自同一次 `bench_rule_trigger` 调用：rule `bench-smoke-test-001-mpdhdqyw`，`storageType=s3`，`backendID=1`，`parallelism=4`。
- 失败原因栈底是 `syncnode/tasks/runner.go:854`：
  `TriggerBench: rule %q requires BackendEndpoint (BackendID=%q) but it is nil`。
- master `triggerBenchRule` 在 ContentLength==0 / 没有 `backendEndpoint` body 时**静默放行**，把规则原样下发 syncnode，4 条 shard 全部在 syncnode 侧失败。

## 设计缺陷

`spec.BenchRule.BackendEndpoint` 是 transient 字段（`json:"backendEndpoint,omitempty"` 但不持久化，由 dashboard 在 trigger 时查 MySQL 凭据后注入）。当 storageType 是 S3 / SDK 时，BackendEndpoint 是 syncnode 真正发起请求的前提：

- 校验唯一发生在 syncnode 任务真正执行前；
- 失败时 master 已经创建了 BenchTaskRecord 并 fan-out 出多个 shard；
- ledger 里出现一批 `failed` task 但根本原因相同，浪费排查精力，也污染了 SLA 指标和后续 retry 重试链路。

应当在 master 层就拒绝：

- create / update 时 storageType 是 S3 / SDK 且 `BackendID` 为空 → 直接 400；
- trigger 时（rule 已落库）storageType 是 S3 / SDK 但请求体没带 `backendEndpoint` → 直接 400，不创建 BenchTaskRecord，不 dispatch。

## 单一事实来源

把"需要 BackendEndpoint"这条业务规则收敛到 `spec.BenchRule` 上：

```go
// RequiresBackendEndpoint reports whether this rule needs an
// EndpointConfig populated before dispatch / execution. True for S3
// and SDK storage types; false for POSIX / mdtest / IOR which only
// use MountPath.
func (r *BenchRule) RequiresBackendEndpoint() bool {
    return r.StorageType == BenchStorageS3 || r.StorageType == BenchStorageSDK
}
```

之后 master 和 syncnode 都调这个方法，避免两边 `storageType == "s3" || storageType == "sdk"` 各写一遍——未来新增 storageType（比如 RC9 计划的 OSS 直连）只需要改 spec 一处。

## 校验矩阵

| Handler | 检查时机 | 拒绝条件 | HTTP 状态 / 错误码 |
| --- | --- | --- | --- |
| `createBenchRule` | strict decode 后，落库前 | `rule.RequiresBackendEndpoint() && rule.BackendID == ""` | 400 / `ErrCodeParamError` |
| `updateBenchRule` | strict decode 后，落库前 | 同上 | 400 / `ErrCodeParamError` |
| `triggerBenchRule` | rule load + 可选 body decode 后，创建 BenchTaskRecord 前 | `rule.RequiresBackendEndpoint() && rule.BackendEndpoint == nil` | 400 / `ErrCodeParamError` |
| `syncnode/tasks/runner.go` `TriggerBench` | dispatch payload 进入 executor 前（保留兜底） | 同上 | error；改成调用 helper |

trigger 的错误消息要带上 rule ID 和 backendID，方便 dashboard / 排查直接定位。

## 不做什么

- 不引入"backend 注册表"——master 仍然没有 MySQL 凭据，BackendEndpoint 仍由 dashboard 在 trigger body 里注入；本次只是把"必须注入"这条约束显式化。
- 不改 BenchRuleStore 的 schema、不动 raft 持久化路径；老的（无 BackendID 的 S3 rule）老规则在 store 里**保留**，只是再次 trigger 时会被新校验拒绝。这符合"老数据可读、新行为收紧"的兼容方向。
- 不做 backendID 存在性校验（master 也无法知道哪些 backendID 合法，那是 dashboard 的职责）。

## 实现步骤

1. **spec helper**：`syncnode/spec/bench_rule.go` 末尾新增 `RequiresBackendEndpoint()`；附 table-driven 单测覆盖 5 个 storageType。
2. **master handler**：
   - `createBenchRule` / `updateBenchRule`：strict decode + `rule.ID == ""` 检查后，立即加 `RequiresBackendEndpoint && BackendID == ""` 拒绝分支。
   - `triggerBenchRule`：可选 body decode 之后，加 `RequiresBackendEndpoint && BackendEndpoint == nil` 拒绝分支；先于 `BenchTaskRecord` 创建。
3. **syncnode runner**：把 `runner.go:852-855` 的字面量比较换成 `if rule.RequiresBackendEndpoint() && rule.BackendEndpoint == nil { ... }`。错误文本保持不变（已经被 dashboard / 运行手册引用）。
4. **修存量测试**：`api_service_bench_test.go` 里 `newBenchRuleView(r)` / `newBenchRuleViews(nil)` 单参调用补上 `nil` ledger（P2 修复时遗漏，导致 test 实际从未跑过）。
5. **新增 handler 拒绝测试**：表驱动覆盖 create / update / trigger 三种拒绝路径 + 一条 happy-path（posix，不需要 backend）。
6. **构建 rc11 + bump images.hcl + apply-master**：按 `cubefs-stack-build-deploy` 记忆里的标准流程；只重启 master 即可（syncnode 改的是同一 binary，但 trigger 校验已经在 master 提前拦截，老 syncnode 也不会再收到坏 payload）。
7. **端到端验证**：在 test-k3d 上跑三次 trigger 验证拒绝路径 + 一次正常路径（带 backendEndpoint）。

## 验收标准

- 单元测试：`go test ./syncnode/spec/... ./master/...` 全绿。
- master 重启后，新建一条 storageType=s3、backendID 为空的 rule → 400。
- 对已有 storageType=s3 rule 不带 body trigger → 400（不创建 BenchTaskRecord，benchTask.list 无新增 failed）。
- 带 backendEndpoint body trigger → 进入正常 dispatch 路径，至少能成功创建 shard 记录（即使后续 S3 凭据错误也不归本次修复管）。
- syncnode 侧不再出现 `requires BackendEndpoint (BackendID=...) but it is nil` 日志。
- healthcheck-findings-fixes.md 的 P3 章节追加"已完成"标记 + 验收 hash / 时间戳。

## 风险与回滚

- 风险：dashboard 旧客户端在 create 时不传 backendID（仅依赖 trigger 时补）。如果存在，会被新校验直接 400。
  - 缓解：dashboard 当前流程 `backendID` 是 form 必填，已经强制；review 后无回滚必要。如果有遗漏环境，临时把 master 镜像回到 rc10。
- 回滚：本次改动是纯校验收紧，无 schema 变更。回滚 = 部署 rc10。

## 进度

- 2026-05-23 09:xx — 落 plan doc。
- 2026-05-23 — 实现 + 单测 + 构建 + 部署 + e2e 验证（待）。
