# bench rule master 端 raft 持久化设计

> **背景**：commit `6fba85cd4` 落地 bench 子系统时，`benchRuleStore` 选择 P0 仅内存（in-memory），原因是 bench 规则被定位为"短期实验配置"。实际部署后发现痛点：
>
> 1. master 三副本 rolling 升级 → leader 切换 → benchRuleStore 清空 → 用户已 create 的规则全部丢失，必须重建。
> 2. test-k3d / 生产环境每次镜像升级都是同样路径，对调试 / 长跑实验非常不友好。
> 3. bench task ledger 同样仅内存，task 历史也会丢，但这是预期行为（observability 数据可重建），**只持久化 rule，task 仍保持内存**。
>
> 本文是 backlog 项，等优先级排上来再实施。**不在当前 PR 范围内**。

---

## 0. 决议表

| # | 决议 | 备注 |
|---|---|---|
| B1 | 仅 `benchRule` 走 raft，`benchTaskLedger` 保持内存 LRU | task 是 observability，rule 是配置 |
| B2 | 数据模型沿用现有 `spec.BenchRule`，不拆 wire / persist 两套 schema | 减少代码 |
| B3 | key prefix `br`，op codes 0x86 / 0x87 / 0x88 紧接 sync rule (0x83-0x85) 之后 | 沿用 syncnode block 0x80-0x88 区间 |
| B4 | 启动 / leader 切换时 `loadBenchRules()` 从 rocksdb 重建缓存 | 与 `loadSyncRules()` 对称 |
| B5 | admin handler 先 raft submit，apply 成功后再更新内存缓存 | 与 sync rule 一致 |
| B6 | raft 失败返回 HTTP 503 + code=4（`PersistenceByRaft`） | 沿用现有错误约定 |
| B7 | 迁移：P0 → P1 不做数据迁移，直接发布即可（既然 P0 本就会丢，用户已知） | 一次性 |

---

## 1. 改动清单

### 1.1 新增 op codes（`master/sync_rule_store.go` 注释区扩展，或单独 const 块）

```go
const (
    benchRuleAcronym = "br"
    benchRulePrefix  = keySeparator + benchRuleAcronym + keySeparator

    opSyncAddBenchRule    uint32 = 0x86
    opSyncDeleteBenchRule uint32 = 0x87
    opSyncUpdateBenchRule uint32 = 0x88
)
```

### 1.2 改造 `master/bench_rule_store.go`

当前实现：`map[string]*spec.BenchRule` + sync.RWMutex，纯内存。

目标接口（保持原签名，内部增加 raft submit 路径）：

```go
func (s *BenchRuleStore) Add(rule *spec.BenchRule) error    // raft submit + 缓存更新
func (s *BenchRuleStore) Update(rule *spec.BenchRule) error // 同上
func (s *BenchRuleStore) Delete(id string) error            // 同上
func (s *BenchRuleStore) Get(id string) (*spec.BenchRule, error)     // 纯内存读
func (s *BenchRuleStore) List() []*spec.BenchRule                    // 纯内存读
```

需要持有 `*Cluster` 引用（或 raft submitter 接口）才能调 `c.submit(ctx, opCode, key, value)`。

### 1.3 新增 FSM apply 分支

`master/metadata_fsm_op.go`（或同等文件）handleRaftLogCommit 的 switch 增加：

```go
case opSyncAddBenchRule, opSyncUpdateBenchRule:
    // unmarshal spec.BenchRule, put into BatchDeleteAndPut, update cache
case opSyncDeleteBenchRule:
    // BatchDeleteAndPut delete, remove from cache
```

### 1.4 新增 loadBenchRules

`master/cluster.go` 或 `bench_rule_store.go`：

```go
func (c *Cluster) loadBenchRules() error {
    // rocksdb scan benchRulePrefix
    // unmarshal each value into spec.BenchRule
    // populate c.benchRuleStore.rules
}
```

调用点：在 `loadSyncRules()` 之后追加一行 `c.loadBenchRules()`。

### 1.5 admin handler 调整

`master/api_service_bench.go` 现有 `createBenchRule / updateBenchRule / deleteBenchRule` 直接调 store 的 in-memory 操作，改造后 store 内部会 submit raft；handler 只需处理 raft 错误返回 HTTP 503 + code=4。

`triggerBenchRule` 不变（task 不持久化）。

---

## 2. 实施分期与 AC

### Phase 1：FSM + store（单节点可跑）

- 新增三个 op code 常量
- `BenchRuleStore.Add/Update/Delete` 改走 `c.submit`
- FSM apply 分支处理三个 op
- `loadBenchRules` 实现 + 启动钩子接入

**AC**：
- 单 master 模式（peers=1）下创建 rule → 进程重启 → list 仍能看到
- rocksdb 命令行扫描 `/br/` 前缀能看到对应 key

### Phase 2：多节点 + leader 切换

- 三副本部署
- 在 leader 上 create 5 个 rule
- `kubectl delete pod <leader>` → 新 leader 被选出
- `list` 在新 leader 上能看到全部 5 个 rule

**AC**：
- 5/5 规则保留
- 期间 client 收到 HTTP 503 + code=4 时能正确重试（dashboard 端是否已实现待确认）

### Phase 3：dashboard 适配 503 重试

- dashboard backend 已有 sync rule 的 503 重试逻辑，bench rule 复用即可
- 验证 `cubefs-dashboard/backend/handler/benchrule/` 是否走了相同的重试封装

**AC**：master leader 切换期间 dashboard 不显示报错弹窗，自动重试成功

---

## 3. 风险与回退

- **风险 1**：raft 日志条目体积。BenchRule 含 `Stages` / `FIOStages`，单条 JSON 序列化可达几 KB。比 SyncRule 略大但远低于 raft 单条上限。无需特殊处理。
- **风险 2**：op code 冲突。当前 syncnode block 0x80-0x88 仅用了 0x80-0x85，0x86-0x88 是空的。需在合入前再 grep 一次确认无人占用。
- **回退**：本设计不破坏现有 in-memory 接口签名，必要时可在 store 层挂一个 feature flag `enableBenchRulePersist`，默认 on，出问题切回 in-memory。

---

## 4. 不做的事

- 不持久化 `benchTaskLedger`：task 历史允许丢，由 syncnode 心跳重建活跃 task 即可。如果未来需要"已完成任务的长期归档"，应该走 dashboard MySQL 这条链路（参考 sync task 的 archive 机制），不应该塞进 raft。
- 不做 bench rule 的导入 / 导出：先做持久化，导入导出是后续可选项。
- 不引入新的 raft state machine：复用 `MetadataFsm`。
