# SyncRule 分片数 / 单节点并发度 解耦

**作用域**：`proto/sync_rule.go`（新增字段）；`master/sync_rule_manager.go`（dispatch 路径）；`master/api_service_sync_rule.go`（校验）；`cubefs-dashboard` 前端规则创建/编辑 / 列表展示 + i18n。
**关联事项**：上游用户反馈"页面设置 SyncRule 时看不到分片数，似乎只能默认所有节点"。本质是 `SyncRuleConfig.Parallelism` 一个字段承载了两套互不相关的语义。

## 背景

`proto.SyncRuleConfig` 当前仅有一个 `Parallelism int json:"parallelism"`。但这个字段在调用链上承担了两件完全不同的事：

1. **单节点并发文件数**（hint：「单任务内并发文件数」）——syncnode 接到 sub-task 后，决定同一进程内同时拷贝几条 object。
2. **分片数上限**（hash / prefix / auto 模式）——master `dispatchHash` / `dispatchPrefix` 把 `parallelism` 当作 `shardTotal` 的上界，再 `min(parallelism, 在线 syncnode 数)`（hash）或 `min(parallelism, len(prefixes))`（prefix）。

证据：
- `master/sync_rule_manager.go:248` `parallelism := rule.Config.Parallelism`
- `master/sync_rule_manager.go:293-296` (prefix) `limit := parallelism; if limit <= 0 || limit > len(prefixes) { limit = len(prefixes) }; bucketsForPrefix(prefixes, limit)`
- `master/sync_rule_manager.go:327` (hash) `if parallelism <= 1 || len(cands) <= 1 { 走单 dispatch }`
- `master/sync_rule_manager.go:343-345` (hash) `shardTotal := parallelism; if shardTotal > len(cands) { shardTotal = len(cands) }`
- dashboard `SyncRuleCreateDialog.vue:57-66` 仅暴露 `parallelism`，hint 还是「单任务内并发文件数」——用户按 hint 设置时根本不知道自己同时在改分片数。

> 对照组：ad-hoc 触发用的 `SyncTaskDispatchDialog.vue:42-49` 已经把 `parallelism` 和 `shardTotal` 拆成两个独立输入。规则编辑器没有跟上，行为不一致。

## 设计缺陷

- **语义混淆**：节点内文件并发和跨节点分片是两个独立维度。前者影响单 syncnode 进程的 IO/CPU 压力，后者影响整个 cluster 的并行宽度。把它们捆在一个字段里，调任意一边都会意外影响另一边。
- **UI 误导**：hint 写「单任务内并发文件数」，但实际它还在控制分片数。运维同学只能通过看代码或踩坑反推真实含义。
- **默认值困局**：用户若希望「单节点跑多条流，但不想拆分片」，目前做不到——`parallelism > 1` 一定触发 hash 分片（前提：有 ≥2 个在线 syncnode）；反之希望「拆 4 个分片，但每个 syncnode 只跑 1 条」也做不到，分片数永远等于单节点并发。
- **与 ad-hoc 路径不一致**：手动 dispatch 已经把两者拆开，规则触发路径还是耦合的，行为难以解释。

## 不做什么

- 不动 `SyncRunSubTaskInfo.ShardTotal`：fan-out wire 协议保持不变，syncnode 看到的还是「这是第 i/N 个分片」。
- 不改 `SyncFanout.DispatchN` 接口：master 内部仍然按 `shardTotal int` 传给 fanout 引擎，只是 `shardTotal` 的来源换成新字段。
- 不引入新的分片策略（继续 `"" / "hash" / "prefix" / "auto"`）。
- 不改 BenchRule（其 `Parallelism` 含义就是「shard count」，注释里写得很清楚，不在本次范围）。
- 不改 master raft 持久化：`SyncRuleConfig` 是 JSON 落盘，新增字段对旧记录天然兼容（缺字段 = 零值）。

## 字段设计

`proto.SyncRuleConfig` 新增字段 `ShardCount`：

```go
// ShardCount controls how many parallel sub-tasks the master fans a
// single rule trigger into. Decoupled from Parallelism (per-shard,
// per-syncnode in-process file concurrency).
//
//   - hash mode:  capped by min(ShardCount, online syncnodes); 1 = no fan-out
//   - prefix mode: capped by min(ShardCount, len(ShardPrefixes))
//   - auto mode:  uses ShardCount when prefix cache hits; falls back to
//                 hash with the same ShardCount on cache miss
//
// Zero means "use legacy fallback": derive from Parallelism when
// Parallelism > 0 (backward compatibility for rules persisted before
// this field landed); otherwise default to 3.
ShardCount int `json:"shardCount,omitempty"`
```

**为什么字段名是 `ShardCount` 而不是 `ShardTotal`**：避免和 `SyncRunSubTaskInfo.ShardTotal`（运行时 wire 字段、表示已确定的分片总数）混淆。`ShardCount` 是规则期望值，`ShardTotal` 是某次 fire 实际分到的份数（仍可能被 `len(prefixes)` / `len(candidates)` 截断）。

**默认值约定**：
- 新建规则：dashboard 表单初始值 = 3，与单节点并发 `Parallelism` 默认值一致但两者独立。
- 旧规则（DB 里只有 `parallelism`）：master dispatch 阶段如果 `ShardCount == 0` 且 `Parallelism > 0`，按 `Parallelism` 当 shard 上限（沿用老行为，保证升级零回归）；若两者都是 0，分片数 = 1（单 dispatch）。
- 显式希望「单节点并发 3、不分片」：dashboard 设置 `parallelism=3, shardCount=1`。
- 显式希望「分 5 片、每个 syncnode 1 流」：dashboard 设置 `parallelism=1, shardCount=5`。

## 校验矩阵

| 入口 | 字段 | 校验规则 | 错误码 |
| --- | --- | --- | --- |
| `createSyncRule` / `updateSyncRule` | `ShardCount` | `< 0` 拒绝；`= 0` 允许（语义=未设置）；`> 0` 直接采纳 | 400 / `ErrCodeParamError` |
| `createSyncRule` / `updateSyncRule` | `Parallelism` | 同上（既有行为，不动） | 同上 |
| `triggerSyncRule` | — | 不接受 body override（保持当前实现）；shard 行为完全由 rule 字段决定 | — |

不做「`ShardCount > 1 必须搭配 ShardingStrategy != "" / "hash"`」之类硬约束——hash 模式本来就支持任意 N，prefix 模式自然被 `len(ShardPrefixes)` 截断。

## 实现步骤

1. **proto 字段**（`/Users/tao.fang/codes/cubefs/proto/sync_rule.go`）
   - 在 `SyncRuleConfig` 加 `ShardCount int json:"shardCount,omitempty"`。
   - 同步更新 `proto/sync_rule_test.go` 的 round-trip / omitempty 测例：补一条 `ShardCount=5` 的覆盖；保留一条 `ShardCount=0` 验证不出现在 JSON 中。

2. **master dispatch 改造**（`/Users/tao.fang/codes/cubefs/master/sync_rule_manager.go`）
   - 新增辅助：`func effectiveShardCount(cfg *proto.SyncRuleConfig) int`，返回 `ShardCount > 0 ? ShardCount : (Parallelism > 0 ? Parallelism : 1)`，并加一条 LogDebug 标注是 `shardCount` 来源还是 `parallelism` 来源（便于排查旧规则行为）。
   - `dispatchRule` 替换 line 248 `parallelism := rule.Config.Parallelism` 为 `shardCount := effectiveShardCount(&rule.Config)`，并把 `dispatchPrefix` / `dispatchHash` 的参数名同步改为 `shardCount`。
   - `dispatchHash` line 327 的 `if parallelism <= 1 || len(cands) <= 1 { 单 dispatch }` 改为 `if shardCount <= 1 || len(cands) <= 1 { 单 dispatch }`。
   - 注释更新：把"Parallelism"出现的位置全部替换为"ShardCount (fallback Parallelism)"或拆分语义说明。

3. **handler 校验**（`/Users/tao.fang/codes/cubefs/master/api_service_sync_rule.go`）
   - `validateSyncRuleShape` 末尾增加：`if rule.Config.ShardCount < 0 { return errors.New("invalid shardCount: must be >= 0") }`。
   - 不动其余分支。

4. **单元测试**（`/Users/tao.fang/codes/cubefs/master/sync_rule_manager_test.go` + 新文件 `sync_rule_shardcount_test.go`）
   - `effectiveShardCount` table-driven：覆盖 4 个 case（仅 ShardCount 设置 / 仅 Parallelism 设置 / 两者都设置 / 两者都为 0）。
   - `dispatchHash` 行为：mock 3 candidates，`ShardCount=2, Parallelism=4` → 期望分 2 片；`ShardCount=0, Parallelism=4` → 期望分 4 片（兼容老行为）；`ShardCount=1, Parallelism=4` → 期望走单 dispatch。
   - `dispatchPrefix` 行为：`ShardPrefixes=["a/","b/","c/","d/"], ShardCount=2` → 期望 `bucketsForPrefix(_, 2)`；`ShardCount=0, Parallelism=3` → 期望 `bucketsForPrefix(_, 3)`。
   - `validateSyncRuleShape`：`ShardCount=-1` 必须报错。

5. **dashboard 前端**（`/Users/tao.fang/codes/cubefs-dashboard/frontend/src/pages/cfs/clusterOverview/clusterInfo/syncManage/components/SyncRuleCreateDialog.vue`）
   - 在 `parallelism` form-item 之后插入 `shardCount`：
     - label 文案：`$t('sync.shardCount')`
     - hint：「分片数（跨 syncnode 并行），≤1 表示不分片；默认 3。与上方"并发度"互不影响」
     - default value: 3
   - `parallelism` 的 hint 收紧成：「单任务内并发文件数（单 syncnode 进程内）」，去掉「分片数自动截断为 min(并发度, 在线节点数)」这种暗示分片的描述。
   - `shardingStrategy` 的 hash 选项 hint 改为「按 object key hash 分片到多个 syncnode；分片数 = min(分片数, 在线节点数)」。
   - 提交逻辑：当 `form.shardCount > 0` 时把 `shardCount` 写入 payload；不写时让后端按默认/兼容逻辑取值。
   - 加载现有规则时：`this.form.shardCount = config.shardCount ?? 0`（0 = 沿用 backend 默认，方便老规则无感）。

6. **dashboard 列表展示**（`SyncRuleTab.vue`）
   - 在 `parallelism` 列旁加 `shardCount` 列：`getConfig(scope.row, 'shardCount') || '-'`。
   - i18n key：`sync.shardCount` / `sync.shardCountTip`（zh + en）。

7. **i18n**（`frontend/src/i18n/lang/{zh,en}/index.js`）
   - `sync.shardCount`: 中文「分片数」/ 英文 "Shard count"
   - `sync.parallelism` 的现有翻译保留，并配套修订 tip 文案（如有）。

8. **构建与部署**
   - 编译 cubefs 镜像（bump 到 rc12）+ dashboard 镜像（bump rc7）。
   - `_envcommon/images.hcl` 本地工作树 bump，仅 `apply-master` + `apply-dashboard` 滚动；syncnode 无需重启（行为完全在 master 决策）。
   - test-k3d e2e：
     - case1：创建 hash rule `shardCount=2, parallelism=4` → 触发 → 期望 2 个 shard 记录、每个 owner 不同。
     - case2：创建 hash rule `shardCount=1, parallelism=4` → 触发 → 期望单 dispatch，无 shard 记录。
     - case3：导入老 rule（手工 raft put 一条只有 `parallelism=3` 的 cfg）→ 触发 → 期望 3 个 shard（兼容路径）。
     - case4：prefix rule `ShardPrefixes=[a/,b/,c/], shardCount=2` → 期望 2 个 bucket。

## 验收标准

- `go test ./proto/... ./master/...` 全绿。
- master 重启后：
  - 新规则可设置 `shardCount` 独立于 `parallelism`，dispatch 行为符合上表。
  - 老规则（仅 `parallelism`）行为零回归。
- dashboard：
  - 规则创建 / 编辑表单出现"分片数"输入，与"并发度"分离，hint 互不交叉。
  - 列表能展示 shardCount 列。
- syncnode 端无任何代码改动、无任何配置改动；旧 syncnode 镜像继续工作。
- 文档：本 plan doc 标记"已完成"+ 验证 hash / 时间戳；CHANGELOG 增补一条。

## 风险与回滚

- 风险：dashboard 旧 build 仍在调 `parallelism` 字段——后端兼容路径仍按 `parallelism` 当 shard 上限，行为与改动前一致，不会破坏。
- 风险：第三方脚本直接 POST `/syncRule/create` 不带 `shardCount`——按"`shardCount=0 = 沿用 parallelism`"路径处理，行为不变。
- 回滚：本次改动是纯增量字段 + 兼容降级，无 schema 不兼容；如出现问题，把 dashboard 镜像回退到 rc6 即可（master 多一个字段对外是 backward-compatible 的）。

## 进度

- 2026-05-23 ?? — 落 plan doc。
- 待开发：proto / master / dashboard 三处改动同步推进。
