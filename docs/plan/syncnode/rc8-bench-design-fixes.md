# rc8 Bench 体系设计缺陷修复（合并提交）

> 日期：2026-05-22
> 起源：rc7 + bench-tools rc1 在 test-k3d e2e 暴露 4 个 "数据在边界处被静默丢弃" 的设计缺陷
> 范围：cubefs 仓库本次单次 rc，bench-tools 不变

## 背景

rc7 e2e 验证 Sprint 1/2/3 bench 平台时发现 4 个共同模式的设计缺陷：
单测全过、看似 feature 完成，但运行时在以下边界静默丢数据：
- JSON 序列化边界（master /benchRule POST）
- 子进程边界（fio-subprocess 内部完成 I/O）
- result 结构边界（BenchStageResult 缺 MixedComponents）
- Registry 注册边界（warmup / cache_drop 是 dead code）

本 doc 把这 4 个修复合并到 rc8，避免多次 rc 增量。

## 目标与边界

**做**：
- #119 master /benchRule POST 严格 JSON + 原始 raw JSON 持久化
- #120 RunWarmup / MaybeDropCaches 在 fio/S3 stage runner 中接入
- #121 BenchStageResult 增加 MixedComponents 子项 + master ledger 透传
- #122 删除 bench_posix.go:526 误导性 op-level emit + 文档明确 fio POSIX 路径不暴露 op-level

**不做**：
- 不重构 bench rule schema
- 不动 S3 path 的真 per-op emit
- 不动 bench-tools sidecar 镜像
- 不改 dashboard（dashboard 适配放下一轮，先确认 backend 正确）

## 验收

部署 rc8 到 test-k3d 后，触发 sprint3-e2e rule，期望：

1. **#119**：POST 一个含未知字段的 BenchRule → 400 Bad Request；POST 成功的 rule → GET 返回的 rawJSON 与 POST body 字节级一致
2. **#120**：rule 含 warmup + cache_drop → syncnode 日志出现 "warmup phase started" / "cache drop executed"；/metrics/bench 有 `syncnode_bench_warmup_ops_total` 和 `syncnode_bench_cache_drop_total` 非零样本
3. **#121**：mixed=[small,large] 的 stage → `/benchTask/get` 返回 JSON 含 `mixedComponents:[{name:small,...},{name:large,...}]`，每项有独立 throughputMBs / latency
4. **#122**：fio POSIX stage 跑完 → /metrics/bench 不再有 `syncnode_bench_op_latency_class_seconds` 误导样本（fio path）；S3 path 的真 op-level 仍然正常

## 分阶段任务

### #119 master 严格 JSON + raw 持久化

文件：
- `master/api_service_bench.go` — BenchRule create/update handler 用 `json.NewDecoder(body).DisallowUnknownFields()`
- `master/bench_rule_store.go` — store 接口/实现增加 `RawJSON []byte` 字段
- `syncnode/spec/bench_rule.go` — BenchRule 结构增加 `RawJSON string` 字段（getter only，POST 时填充）

要点：
- 必须保留原始 body 字节序列（不重新 marshal），避免字段顺序/空格变化
- raw JSON 在 GET 时一并返回，便于 dashboard / debug 对照
- DisallowUnknownFields 错误信息要明确指出哪个未知字段

### #120 warmup / cache_drop 接入

文件：
- `syncnode/executor/bench_posix.go` — runFIOStage / runFIOStageMixed 入口处先 RunWarmup（如果 stage.Warmup 非零）
- `syncnode/executor/bench_s3.go` — 各 S3 stage 入口同上
- `syncnode/executor/bench_ior.go` / `bench_mdtest.go` — sidecar 路径同上
- 所有上述 stage 在 warmup 完成后、正式 work 开始前调 MaybeDropCaches（如果 stage.CacheDrop 非零）

要点：
- warmup 失败：根据策略允许继续还是 fail-fast（schema 字段 stage.Warmup.AllowError 控制；不存在则默认 fail-fast）
- cache_drop 失败：log warn 但不 fail stage（语义：尽力而为）
- warmup duration 不计入 stage 总耗时；cache_drop 同理
- 单测覆盖：每个 stage 类型至少一个 warmup+drop 正常路径用例

### #121 BenchStageResult.MixedComponents 透传

文件：
- `syncnode/spec/bench_rule.go` — 增加 `BenchComponentResult` struct + `BenchStageResult.MixedComponents []BenchComponentResult`
- `syncnode/executor/bench_posix.go::runFIOStageMixed` — 每个 comp 的 sr 追加到 `agg.MixedComponents`
- `master/bench_task_ledger.go` — 确认 BenchStageResult 透传无字段丢失（一般 JSON encode 整个 struct 就行，但要确保没有 `json:"-"` tag）

BenchComponentResult 字段（最小集合）：
```go
type BenchComponentResult struct {
    Name          string        `json:"name"`
    SizeClass     string        `json:"sizeClass"`
    Weight        int           `json:"weight"`
    DurationSec   float64       `json:"durationSec"`
    ThroughputMBs float64       `json:"throughputMBs"`
    OpsPerSec     float64       `json:"opsPerSec"`
    TotalOps      int64         `json:"totalOps"`
    TotalBytes    int64         `json:"totalBytes"`
    Errors        int64         `json:"errors"`
    Latency       BenchLatency  `json:"latency"`
}
```

### #122 fio op-level emit 清理

文件：
- `syncnode/executor/bench_posix.go:526` — 删除 ObserveBenchOpClass 调用（误导性，是 stage 聚合 mean 调一次 histogram）
- `syncnode/executor/metrics.go` — 在 `ObserveBenchOpClass` 注释里加一句：仅用于 S3 path 真 per-op 调用，不要在 fio path 使用
- `docs/plan/syncnode/sprint-1.5.md` 或类似 — 标注 fio path 不提供 op-level，dashboard 应改用 stage-level + mixedComponents

## 风险

- #121 BenchStageResult schema 变化：旧 syncnode 上报的 task 不带 MixedComponents 字段 → master/dashboard 必须容忍 nil/[]，单测覆盖
- #119 RawJSON 体积：每个 rule 多存一份原文 → 估算单 rule < 4KB，可接受
- #120 warmup 调用栈错位：必须确保 stage_state metric 在 warmup/work/cache_drop 三个阶段都正确切换

## 进度

- [ ] #119 实现 + 单测
- [ ] #120 实现 + 单测
- [ ] #121 实现 + 单测
- [x] #122 实现（仅删除 + 注释）— bench_posix.go 删除 521-528 区间 8 行 → 4 行注释；metrics.go ObserveBenchOpClass 函数注释追加 3 行"设计约束"说明 fio path 不可调用
- [ ] make build + go test ./... 全过
- [ ] 单次提交 push 到 cubefs main
- [ ] dev_bd 构建 cubefs:v3.5.3.1.rc8 + push hub
- [ ] cubefs-deploy bump images.hcl rc7→rc8
- [ ] make ENV=test-k3d apply-master apply-syncnode
- [ ] e2e 重测 sprint3-e2e rule

## 下一步（不在本轮范围）

- dashboard 适配 mixedComponents 与 warmup/cache_drop 字段（rc9 dashboard 镜像）
- Sprint 1.5 op-level 文档/UI 在 fio path 的呈现方式调整：fio POSIX 路径不提供 op-level Prometheus histogram（子进程边界限制），dashboard 应改用 stage-level BenchStageResult + mixedComponents（#121）
