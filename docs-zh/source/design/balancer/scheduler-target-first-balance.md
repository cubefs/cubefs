# 目标优先 Balance（换盘灌空盘）

> 前置阅读：[Balancer 架构设计](balancer-architecture.md)，至少第 1、4、6、8 节和第 14 节。
> 本文复用其中的拓扑、策略、prepare 选盘和安全机制，只说明目标优先新增和改变的部分。

### 术语

| 术语 | 含义 |
|---|---|
| 目标优先 | 本功能：出现极端空盘时，collect 先确定目标盘，再找能搬进去的数据 |
| 源驱动 / 目标驱动 | collect 的两种顺序：先定源盘 S（原有路径）/ 先定目标盘 T（本文新增） |
| 相位（phase） | 某个 idc 本轮 collect 走哪条路径：`target`（目标驱动）/ `source`（源驱动）/ `idle`（不建任务）。不是任务状态 |
| 计划目标盘 | 目标驱动为任务选定的 T，记在 `PlannedTargetDiskID`，代码里也叫 pin |
| 供数盘 | 目标驱动中给 T 提供数据的源盘 S（代码里叫 feeder） |
| 空头 `T`（日志字段 `target_skew`） | 最空盘相对均值偏多少：`(max - mean) / mean` |
| 满头 `S`（日志字段 `source_skew`） | 最满盘相对均值偏多少：`(mean - min) / mean` |
| 极差 `R` | `(max - min) / mean`，`free_chunk_hysteresis` trigger 使用。恒有 `R = S + T`（见 4.1） |

注意：公式和阈值里的 `S`、`T` 是上面的统计量；正文中的「源盘 S」「目标盘 T」指盘。

---

## 1. 背景与问题

换盘后出现极空盘时，原有源驱动的顺序是「先选满盘 S → 再选 `vuid` → prepare 才定目标盘 T」。
空盘所在 host 的其它盘往往已经持有大量卷的 unit，host-aware 规则会拒绝这些卷搬到空盘上。
由于选 `vuid` 时还不知道 T，很多任务到 prepare 才发现「上不去这块空盘」：任务已经持久化、
占了 `DiskConcurrency`、锁了卷，最后因为没有合适目标而提前结束。结果是迁移额度被占用，空盘却灌不进去。

架构文档的 Target selector 和 Benefit evaluator 只能在**已经选定的 `vuid`** 上挑目标，
不能反过来为一块空盘挑 `vuid`。要解决这个问题，必须改变 collect 的顺序（详见架构文档第 14 节）。

---

## 2. 设计目标

| 目标 | 做法 |
|---|---|
| 换盘后优先灌满极空盘 | 出现极端空头时，整个 idc 走目标驱动：先定 T，再找能搬进 T 的 S 和 `vuid` |
| 不改变原有 Trigger 语义 | `balance_trigger_strategy` 仍是总开关，相位只在 Trigger 开启后决定走哪条路径 |
| collect 阶段就避开故障域冲突 | 选 `vuid` 时直接用 `BuildTargetExcludeRule.Allow(T)` 检查能否放到 T |
| prepare 规则不放松 | 计划目标盘只是偏好，仍要通过 `Allow`、收益判断和 CM 校验 |
| 普通不均衡仍走源驱动 | 空头消除后退出目标相位，复用原有 `addSourceTasks` / `genOneBalanceTask` |

**非目标**：不跨 idc 均衡；不改 prepare / worker / finish 流程；不保证一定能灌满空盘（见第 9 节）。

---

## 3. 方案概览

### 3.1 两道判断：Trigger 和相位

每轮 collect，对每个 idc 先后做两次判断：

1. **Trigger：这个 idc 要不要均衡。** 沿用原有 `ITrigger`，例如 `free_chunk_hysteresis` 看极差 `R`。
2. **相位：要均衡的话，本轮走哪条路径。** 只看空头 `target_skew`：
   - 空头足够大（≥ `target_first_enter`）→ `target`：先定空盘，往里灌；
   - 空头不大 → `source`：先定满盘，往外抽；
   - Trigger 关闭 → `idle`：不建任务。

Trigger 问「整个 idc 还匀不匀」；相位问「现在主要矛盾是不是有极空盘」。

### 3.2 总流程

collect 和 prepare 是两条独立循环：

```text
collect（每 collect_task_interval_s 一次）
  collectionTask
    ├─ need = DiskConcurrency - 正在迁出的源盘数；need ≤ 0 本轮不建任务
    ├─ 未启用相位（balance_phase_strategy = off）
    │     → addSourceTasks(selectDisks(), need)                 # 原源驱动
    └─ 启用相位（target_then_source）
          → collectByPhase(need)
                ├─ 有 idc 处于 target → 目标驱动建任务，带计划目标盘 T
                └─ 否则 source idc → 源驱动建任务
          → AddTask（state = Inited），进入 prepareQueue

prepare（独立循环，沿用 MigrateMgr）
  SelectTarget：计划目标盘排在候选最前，仍走 Allow + 收益判断
  → AllocVolumeUnitOnDisk → OnAllocResult → state = Prepared
```

入口代码：`BalanceMgr.collectionTask` → `collectByPhase` 或 `addSourceTasks`。

### 3.3 两种路径各固定一端

- **目标驱动**固定极空盘 T，再寻找与 T 故障域兼容的源盘和 `vuid`，不保证抽到全局最满盘。
- **源驱动**固定偏满盘 S，再从故障域过滤后的候选中选目标盘，不保证灌入全局最空盘。

两者都不能同时命中全局最满和最空，但目标驱动围绕极空盘主动寻找兼容数据，
换盘场景中极空盘获得数据的机会明显高于源驱动。

---

## 4. 相位

### 4.1 指标与日志

对某一 idc 的**可写盘**（`IsWritableDisk`：状态正常且非 readonly）按 `FreeChunkCnt` 统计 `mean` / `min` / `max`。
相位、`empty_disk` planner、`free_chunk_hysteresis` trigger 和相位日志都用这同一批盘，避免 readonly 把均值带偏。

| 名字 | 公式 | 含义 | 谁用 |
|---|---|---|---|
| `source_skew`（S） | `(mean - min) / mean` | 满头：最满盘相对均值偏了多少 | 日志观测 |
| `target_skew`（T） | `(max - mean) / mean` | 空头：最空盘相对均值偏了多少 | 相位进入 / 退出 |
| 相对极差（R） | `(max - min) / mean` | 整段不均 | `free_chunk_hysteresis` trigger |

每轮每个 idc 输出一条日志：

```text
balance phase: idc[...] phase[target|source|idle] triggered[...]
  source_skew[...] target_skew[...] mean[...] min[...] max[...]
```

#### 公式关系（同一批可写盘）

设 `mean` / `min` / `max` 分别为平均、最满、最空盘的 `FreeChunkCnt`（`mean > 0`）：

```text
R = (max - min) / mean     # 极差，hysteresis trigger 用
T = (max - mean) / mean    # 空头 target_skew，相位用
S = (mean - min) / mean    # 满头 source_skew，日志观测用
```

**恒等式推导**（同一组统计量、同一个 `mean`）：

```text
S + T
  = (mean - min) / mean  +  (max - mean) / mean
  = [(mean - min) + (max - mean)] / mean
  = (max - min) / mean
  = R
```

所以对任意同一批可写盘恒有 `R = S + T`。直观上：极差相对均值 =「满头偏多少」+「空头偏多少」，两头拼起来就是整段不均。

这个恒等式只说明三个统计量之间的关系，**不代表 `target_first` 阈值应设置为 Trigger 阈值的一半**。
Trigger 用来判断整体是否需要均衡；目标相位只用来识别换盘等场景产生的极端空头。两者职责不同，
阈值也应分别设置（见 8.4）。

### 4.2 状态机

相位由 `strategy.targetThenSource` 维护，每个 idc 一份，按上一轮相位和本轮的 `triggered`、`target_skew` 计算：

```text
idle（初始）:
  Trigger 关                          → idle
  Trigger 开 且 target_skew ≥ enter   → target
  Trigger 开 其它情况                  → source

source:
  Trigger 关                          → idle
  target_skew ≥ enter                 → target（又出现极空盘，例如新换盘）
  其它情况                             → source

target（粘滞）:
  target_skew > exit                  → target（不看 Trigger）
  否则：Trigger 开 → source；Trigger 关 → idle
```

其中 enter / exit 是 `target_first_enter` / `target_first_exit`。代码：`balance_strategy_phase.go` 的 `step` / `underTrigger`。

要点：

- **进入目标相位需要 Trigger 开启；进入之后只看空头。** 只要 `target_skew > exit` 就留在目标相位，即使 Trigger 已经关闭。
- **退出那一轮只会去 `source` 或 `idle`。** 此时 `target_skew ≤ exit ≤ enter`，不可能同一轮再回到 `target`；
  要重新进入，至少要在 `source` / `idle` 待一轮，并且空头重新涨到 enter 以上。
- **建不出任务不算收敛。** 相位只看统计量，不看本轮 `AddTask` 是否成功。原因是一次失败可能只是卷锁、
  并发或候选暂时不可用，不代表空头已经消失。
- 相位保存在内存中，leader 切换后从 `idle` 重新计算。

### 4.3 例子

**例子 A：刚换上一块空盘（进入 target）**

可写盘 free 为 `100, 100, 100, 100, 300`：

```text
mean = 140
min  = 100, max = 300
S = (140-100)/140 ≈ 0.29
T = (300-140)/140 ≈ 1.14
R = S+T ≈ 1.43
```

按推荐配置 `trigger_enter = 0.20`、`target_first_enter = 1.0`：`R` 很大，Trigger 开启；
`T ≥ 1.0`，即最空盘的 `FreeChunkCnt` 至少是均值的 2 倍 → 进入 `target`。

**例子 B：极端空头消除后转入 source**

灌完后 free 为 `120, 125, 130, 130, 145`：

```text
mean = 130
min = 120, max = 145
S ≈ 0.08
T ≈ 0.12
R ≈ 0.19
```

按 `target_first_exit = 0.2`：`T ≤ 0.2`，极端空头已消除，退出目标相位。
但 `R ≈ 0.19` 仍高于 `trigger_exit = 0.08`，Trigger 保持开启，下一轮转入 `source` 继续处理普通不均衡。

### 4.4 collectByPhase：整轮只走一种路径

```text
collectByPhase(need):
  for 每个 idc:
      triggered = trigger.ShouldTrigger(idc, disks)
      phase     = phase.Next(idc, disks, triggered)
      按 phase 记入 targetIDCs 或 sourceIDCs

  if targetIDCs 非空:
      for 每个 target idc: collectByTargetIDC(idc, need - added)
      return                      # 本轮不跑任何 source
  if sourceIDCs 非空:
      合并这些 idc 的 selectDisksOfIDC(idc, useTrigger=false)
      addSourceTasks(disks, need)
  return
```

约定：

1. **同一 idc 同一轮只走一条路径**：`Next` 只返回一个相位。
2. **跨 idc 整轮互斥**：只要有一个 idc 处于 `target`，本轮额度全部给目标驱动，其它 `source` idc 等到没有 idc 处于 `target` 时再执行。
   搬迁本身不跨 idc，互斥的是共享的 `DiskConcurrency`。
3. 源相位调用 `selectDisksOfIDC(..., false)`，**不再用 Trigger 筛一遍**：相位已经确认要做 source。

---

## 5. 目标驱动 collect（collectByTargetIDC）

主线：选目标盘 T → 为每个 T 排供数盘 S → 在 S 上找一个能放到 T 的 `vuid` → 建任务。

### 5.1 单轮临时状态 `refillRound`

目标驱动开始时创建 `newRefillRound()`，只在本轮有效，结束即丢弃：

```go
type refillRound struct {
    diskUnits  map[proto.DiskID][]*client.VunitInfoSimple // 盘 → unit 列表缓存
    pairedVids map[proto.Vid]struct{}                     // 本轮已建任务的卷
}
```

| 字段 | 作用 |
|---|---|
| `diskUnits` | 同一轮内同一块盘只调一次 `ListDiskVolumeUnits` |
| `pairedVids` | **本轮同一个卷只建一个任务**，与 prepare「一卷一锁」一致 |

### 5.2 选目标盘：`SelectTargets`

```text
snapshot = 浅拷贝该 idc 盘列表         # 本轮遍历顺序固定，不受 UpdateAfterChunkMove 重排影响
sources  = FilterSourceCandidates(snapshot)   # 剔除 readonly / 不健康 / 迁移中 / 冷却中
sources 按 free 升序（最满在前）

targets  = emptyDiskPlanner.SelectTargets(RefillInput{AllDisks: snapshot, Sources: sources, InBand: true})
```

`empty_disk` planner 的规则（`mean` 只算可写盘）：

- `exitAt = mean × (1 + target_first_exit)`，可写盘中 `free ≥ exitAt` 的都是目标盘候选，按 free 降序（最空在前）。
- 当前只由目标相位调用，`InBand` 恒为 `true`：已经在目标相位内，不再要求某块盘达到 `enter`，
  只要还有盘高于 `exitAt` 就继续灌。这与相位的「进入看 enter、退出看 exit」一致。

### 5.3 排供数盘：`RankFeeders`

对每个 T，从 `sources` 中挑选供数盘：

- 去掉 T 自己；
- 去掉与 T **同 host** 的盘：目标相位只从其它 host 抽数据；
- 只保留 `free < mean` 的盘（偏满侧）；
- 与 T 同 `DiskSetID` 的排在前面，其余在后。

### 5.4 选 vuid：`selectVunitForTarget`

在供数盘 S 上为固定的 T 找一个可搬的 `vuid`：

```text
取 round.diskUnits[S]（没有则 ListDiskVolumeUnits 并缓存）
按 Used 升序（先搬小的）

for 每个 unit:
    所属卷已在 pairedVids                      → 跳过
    GetVolumeInfo(vid)
    卷不空闲                                   → 跳过
    BuildTargetExcludeRule(S, vol).Allow(T)    → 不通过就跳过
    return vuid
```

与源驱动的 `selectBalanceVunit` 相比，多了一步 `Allow(T)`：故障域（同盘 / 同 host / DiskSet）在 collect 阶段就检查，
不用等到 prepare 才发现放不上去。

### 5.5 配对循环

```text
for T in targets（最空在前）:
    moved = 0
    for S in RankFeeders(T):
        S 正在迁出 / 冷却中                → 跳过
        vuid = selectVunitForTarget(S, T)
        找不到                            → 试下一块 S
        找到 →
            addBalanceTask(S, vuid, planned = T)
            pairedVids 记入该卷
            moved++，added++
        moved ≥ max_moves_per_target      → 换下一块 T
        added ≥ need                      → 本轮结束
```

建任务后 S 立即被登记为迁出中，同一轮不会再被选为供数盘，所以每个任务占用一个不同的源盘名额，与 `need` 的含义一致。
源驱动的每轮上限 `MaxTasksPerRound` 不作用于目标驱动；目标驱动受 `need` 和 `max_moves_per_target` 限制。

---

## 6. 计划目标盘如何传到 prepare

### 6.1 collect 建任务

目标驱动和源驱动最终都调用 `addBalanceTask`，区别只在是否带计划目标盘：

```go
task := &proto.MigrateTask{
    TaskID:              ...,
    TaskType:            TaskTypeBalance,
    State:               MigrateStateInited,
    SourceIDC:           S.Idc,
    SourceDiskID:        S.DiskID,
    SourceVuid:          vuid,      // 搬哪个 unit
    PlannedTargetDiskID: T.DiskID,  // 计划目标盘；源驱动传 InvalidDiskID
}
IMigrator.AddTask(ctx, task)
```

### 6.2 语义

```go
PlannedTargetDiskID DiskID `json:"-"`
```

- **只在进程内**从 collect 传到 prepare，不写入 CM 任务 KV，也不下发给 worker。
- **只是偏好，不是最终决定。**
- **不持久化是有意的，保证兼容性。** 任务 KV 格式和 worker 协议都不变，新旧版本 Scheduler 可以互相加载对方创建的任务，
  升级和回滚都不需要迁移数据。代价只是 Scheduler 重启后重新加载的任务没有计划目标盘，
  prepare 按当前分布正常选盘，正确性不受影响。

### 6.3 prepare 如何使用

```text
prepareTask
  → 锁卷，确认 SourceVuid 仍有效
  → allocMigrateVunit
       → BalanceMgr.SelectTarget(task, vol)
            PreferDiskID = task.PlannedTargetDiskID
            CandIdx = 通过 Allow 的可写盘
            targetSelector.SelectTarget
              → rankCandidates 排序
              → preferFirst：把计划目标盘提到最前
              → 逐个候选做收益判断
       → AllocVunitOnDiskSafe(SourceVuid, targetDiskID)
       → OnAllocResult（成功：乐观更新拓扑；失败：记录拒盘 / 冷却源盘）
  → 写入 Destination / Sources，进入 workQueue
```

- 计划目标盘只影响 `preferFirst` 这一步的顺序，`Allow`、收益判断、CM 校验都不跳过。
- 即使 `most_level_free` 自己的分档会把它过滤掉，只要它在 `CandIdx` 中且没有被 CM 拒过，仍排第一个试。
- 计划目标盘不可用（不在 `CandIdx`、被 CM 拒过、或收益判断不通过）时，任务**继续尝试其它候选**。
  因此目标驱动建的任务，最终可能搬到其它有收益的盘上，而不是计划的那块空盘。这是有意的：任务已经占用了额度和卷锁，
  换一个有收益的目标比直接放弃更划算。

---

## 7. 与源驱动和现有路径的关系

### 7.1 源驱动复用同一套实现

开启相位后，源相位调用的仍是原有实现：

```text
addSourceTasks → genOneBalanceTask(disk, InvalidDiskID)
  → selectBalanceVunit(S)
  → addBalanceTask(..., planned = InvalidDiskID)
```

差别只在**哪些 idc 有资格进入**：

- 未开相位：`selectDisks()` 对每个 idc 用 Trigger 筛选，再合并排序；
- 开启相位：只有处于 `source` 的 idc，并且本轮没有 idc 处于 `target`。

### 7.2 接缝

| 点 | 做法 |
|---|---|
| 任务带目标盘 | `MigrateTask.PlannedTargetDiskID`，`json:"-"` |
| prepare 使用计划目标盘 | `SelectTarget` → `PreferDiskID` → `preferFirst` |
| 故障域规则 | `BuildTargetExcludeRule`：本地镜像 CM `ValidateTargetDisk` 中 Scheduler 能评估的部分 |
| 失败处理 | CM 拒盘 → `OnAllocResult` 记录；`ErrNoTargetDisk` → 源盘冷却 |
| 停止目标驱动 | 空头降到 exit 以下，相位切走；或 `SelectTargets` 选不出目标盘 |
| `IsMigratingDisk` | 一块源盘同一时间只有一个迁移任务；两条路径共用。限制的是正在迁出的源盘，不限制计划中的目标盘 |

`TargetExcludeRule` 的规则：

- free > 0；目标盘不在该卷已有 unit 所在的盘上；
- host-aware：目标 host 不能已持有该卷的**其它** unit（源 host 自己那份豁免，允许同机换盘）；
- replicate 模式要求同 DiskSetID；没有 rack 级约束。

注意：目标相位已在 `RankFeeders` 中排除同 host 的供数盘，走不到上面的同机换盘豁免。
该豁免主要留给源驱动，以及 prepare 丢 pin / reclaim 后重新选盘时的路径。

---

## 8. 配置与配参

### 8.1 推荐配置（换盘场景）

`balance` 段的策略字段扁平展开，目标优先相关项放在 `target_first` 子块中：

```json
"balance": {
  "disk_concurrency": 8,
  "collect_task_interval_s": 10,
  "check_task_interval_s": 1,
  "work_queue_size": 60,
  "prepare_queue_retry_delay_s": 60,
  "finish_queue_retry_delay_s": 60,
  "cancel_punish_duration_s": 60,

  "balance_trigger_strategy": "free_chunk_hysteresis",
  "trigger_enter": 0.20,
  "trigger_exit": 0.08,
  "max_disk_free_chunk_cnt": 800,
  "min_disk_free_chunk_cnt": 120,
  "source_select_strategy": "most_level_full",
  "target_select_strategy": "most_level_free",
  "benefit_strategy": "free_chunk_variance",
  "balance_deviation_ratio": 0.05,
  "balance_deviation_min": 1,

  "target_first": {
    "balance_phase_strategy": "target_then_source",
    "target_first_strategy": "empty_disk",
    "target_first_enter": 1.0,
    "target_first_exit": 0.2,
    "max_moves_per_target": 4
  }
}
```

### 8.2 字段说明

`balance` 根上的策略字段含义见架构文档第 6、12 节。和目标优先有关的是：

| 字段 | 默认 | 说明 |
|---|---|---|
| `balance_trigger_strategy` | `default` | 外层总开关，相位只在它开启后才进入 `target` / `source` |
| `trigger_enter` / `trigger_exit` | 0.20 / 0.15 | `free_chunk_hysteresis` 的极差开关 |
| `target_select_strategy` | `auto` | 开启目标优先时不能是 `auto`（见 8.3） |

`balance.target_first` 子块：

| 字段 | 取值 | 默认 | 说明 |
|---|---|---|---|
| `balance_phase_strategy` | `off` / `target_then_source` | `off` | 是否启用相位。`off` 时走原源驱动 |
| `target_first_strategy` | `off` / `empty_disk` | `off` | 目标相位使用的 planner。只在相位开启时生效 |
| `target_first_enter` | float | 1.0 | 空头达到该值进入目标相位 |
| `target_first_exit` | float | 0.3 | 空头降到该值以下退出目标相位；`free ≥ mean × (1 + exit)` 的盘可作为目标盘 |
| `max_moves_per_target` | int | 4 | 一轮内同一块目标盘最多建几个任务 |

- 两个开关需要**同时打开**：`balance_phase_strategy = target_then_source` 且 `target_first_strategy = empty_disk`。
  只开 `empty_disk`、相位为 `off` 时，planner 会被创建，但没有任何调用方，配置不生效。
- `target_first_exit > target_first_enter` 时，启动会把 exit 修正为 `enter × 0.3`（Trigger 的修正系数是 0.8，两者不同）。

### 8.3 启动校验

`config.go` 的 `fixBalanceConfig` 先调用 `Config.Fix()` 填默认值，再调用 `Config.Validate()`：

- `target_first_strategy != off` 且 `target_select_strategy == auto` → 启动失败。
  目标优先在 collect 配好了目标盘，如果 prepare 又把选盘交给 CM，计划目标盘就没有人使用。
- `balance_phase_strategy == target_then_source` 且 `target_first_strategy == off` → 启动失败（相位开了却没有 planner）。

### 8.4 阈值怎么配

推荐把普通不均衡和极端空头分开处理：

```text
trigger_enter / trigger_exit           = 0.20 / 0.08   # 极差 R：何时开 / 关均衡
target_first_enter / target_first_exit = 1.0  / 0.2    # 空头 T：何时进 / 出目标相位
```

| 阈值 | 含义 |
|---|---|
| `target_first_enter = 1.0` | 最空盘的 `FreeChunkCnt ≥ 2 × mean` 才进入目标相位，只有换盘这类极端空盘才会触发 |
| `target_first_exit = 0.2` | 空头降到均值的 1.2 倍以内退出，剩余普通不均衡交给源驱动 |
| `trigger_enter / trigger_exit = 0.20 / 0.08` | 极差达到 20% 开始均衡，降到 8% 才停，中间滞回避免抖动 |

代码默认的 `target_first_exit` 是 0.3，推荐配置收紧到 0.2，让目标驱动多灌一点再退出。

配置约束：

- **`target_first_exit` 应 ≥ `trigger_exit`。** 因为 `R = S + T ≥ T`，空头还高于 `target_first_exit` 时 Trigger 关不掉，
  目标相位就不会比 Trigger 持续更久。如果反过来配，Trigger 已经认为 idc 均衡了，目标相位仍会继续搬。
- **阈值不要配得太低。** `target_first_enter` 太低时，普通水位波动也会进入目标相位，planner 会选出很多目标盘，
  目标驱动可能承担大部分甚至全部均衡工作。这些任务仍会经过收益判断，不算无收益搬迁，
  但会消耗更多迁移 IO，并偏离「只处理极端空盘」的设计目的。

---

## 9. 风险与局限

| 风险 | 说明 | 观测 / 应对 |
|---|---|---|
| 对 CM 的请求较多 | 配对失败时会遍历「目标盘 × 供数盘 × unit」。`ListDiskVolumeUnits` 每轮缓存，但 `GetVolumeInfo` 每换一个目标盘会重新请求。很多盘处在 exit 附近时，一轮请求量较大 | 代码中已标 `FIXME: cache`；可通过 `collect_task_interval_s` 降低频率 |
| 不保证一定能灌满空盘 | 如果卷分布导致与空盘故障域兼容的卷很少，目标驱动也找不到可搬的 `vuid` | 观察 `target-first pass` 日志和新建任务数 |

---

## 10. 可观测性

| 日志 | 含义 |
|---|---|
| `balance phase: idc[...] phase[...] ...` | 每轮每个 idc 的相位和统计量 |
| `target-first pass: idc[...], targets[...], sources[...], max_per_target[...]` | 目标驱动本轮选出了多少目标盘和候选源盘 |
| `select balance volume unit; ... planned_target[...]` | 建任务时的计划目标盘（debug 级别） |
| `select target disk` / `no beneficial target disk` | prepare 最终选中的目标盘 / 没有有收益的目标 |

---

## 11. 关键代码位置

| 模块 | 文件 |
|---|---|
| collect 入口 / 相位互斥 / 目标驱动配对 | `blobstore/scheduler/balancer.go` |
| 配置、接口、Fix / Validate、`TargetExcludeRule` | `blobstore/scheduler/strategy/balance_strategy.go` |
| 启动校验调用点 | `blobstore/scheduler/config.go`（`fixBalanceConfig`） |
| 相位状态机 | `blobstore/scheduler/strategy/balance_strategy_phase.go` |
| `emptyDiskPlanner` | `blobstore/scheduler/strategy/balance_strategy_target_first.go` |
| `SelectTarget`、`preferFirst` | `blobstore/scheduler/strategy/balance_strategy_target.go` |
| 统计量 | `blobstore/scheduler/strategy/balancer_stats.go` |
| 任务字段 | `blobstore/common/proto/scheduler.go`（`SourceVuid`、`PlannedTargetDiskID`） |
| prepare / alloc | `blobstore/scheduler/migrate.go`（`prepareTask`、`allocMigrateVunit`） |

---

## 12. 端到端调用栈

Balance 有 **四条互相独立的异步链路**：Collect 建任务、Prepare 选盘并 alloc、Finish 改卷映射、
垃圾任务清理。目标优先只改造了 Collect 的一条分支和 Prepare 的 pin 消费，其余与原路径共用。

栈里 **`★` 标记本 MR（`feat(scheduler): 加入目标驱动的均衡策略`）新增或修改的节点**，未标记的是
既有代码，列出来只为把链路串起来。本 MR 的改动集中在 12.1～12.3；12.4 之后三节一行未动。

### 12.1 启动装配

```
|--startup.go: NewService
|   |--conf.fixConfig -> fixBalanceConfig              【scheduler/config.go】
|   |   |--Config.Fix / Validate            ★ 增补 TargetFirstConfig 段与互斥校验
|   |--NewBalanceMgr                                   【scheduler/balancer.go】
|   |   |--conf.Config.Fix
|   |   |--strategy.NewTrigger              -> defaultTrigger | freeChunkHysteresisTrigger
|   |   |--strategy.NewSourceSelector       -> default | belowMeanFree | mostFull | mostLevelFull
|   |   |--strategy.NewTargetSelector       -> nil(auto) | mostFreeSelector | mostLevelFreeSelector
|   |   |--strategy.NewBenefitEvaluator     -> freeChunkVarianceEvaluator
|   |   |--strategy.NewRefillPlanner        -> nil(off) | emptyDiskPlanner        ★ 新增
|   |   |--strategy.NewPhaseCoordinator     -> nil(off) | targetThenSource        ★ 新增
|   |   |--NewMigrateMgr(TaskTypeBalance)              【scheduler/migrate.go】
|   |   |   |--migrator.isBalanceAlloc = true
|   |   |   |-- if targetSelector != nil
|   |   |   |   |--migrator.targetDiskAlloc = mgr      【BalanceMgr 实现 SelectTarget/OnAllocResult】
|   |   |--mgr.IMigrator = migrator
|   |--svr.waitAndLoad -> load -> balanceMgr.Load()    【重载存量任务，按 state 分流三个队列】
|   |--go svr.Run -> balanceMgr.Run()
|   |   |--go collectTaskLoop                          【建任务】
|   |   |--IMigrator.Run()
|   |   |   |--go prepareTaskLoop                      【选目标盘 + alloc】
|   |   |   |--go finishTaskLoop                       【收尾改卷映射】
|   |   |--go checkAndClearJunkTasksLoop
```

`Load` 按 state 分流：`Inited` 进 prepareQueue，`Prepared` 进 workQueue，`WorkCompleted` 进
finishQueue。注意重载的任务 **没有 pin**（`PlannedTargetDiskID` 是 `json:"-"`），prepare 回退到按
当前分布排序。

### 12.2 Collect 链路（建任务，state=1）

```
|--BalanceMgr.collectTaskLoop                          【每 CollectTaskIntervalS 一次】
|   |--IMigrator.WaitEnable
|   |--BalanceMgr.collectionTask                       ★ 改：按 mgr.phase 分流出新分支
|   |   |--need = DiskConcurrency - GetMigratingDiskNum
|   |   |-- if mgr.phase != nil                        【balance_phase_strategy=target_then_source】
|   |   |   |--BalanceMgr.collectByPhase                                          ★ 新增
|   |   |   |   |--for each idc
|   |   |   |   |   |--clusterTopology.GetIDCDisks
|   |   |   |   |   |--trigger.ShouldTrigger                 【外层 gate，沿用旧 ITrigger】
|   |   |   |   |   |--phase.Next                      【balance_strategy_phase.go】  ★ 新文件
|   |   |   |   |   |   |--freeChunkStats(FilterWritableDisks).AboveMeanRange -> target_skew ★ 与 trigger 同集合
|   |   |   |   |   |   |--targetThenSource.step             -> 读写 phases[idc]（粘滞）
|   |   |   |   |-- if 有 idc 处于 DriveTarget         【整轮互斥】
|   |   |   |   |   |--newRefillRound                                             ★ 新增
|   |   |   |   |   |--BalanceMgr.collectByTargetIDC(InBand=true)                 ★ 新增
|   |   |   |   |   |   |--FilterSourceCandidates            【剔除迁移中 / cooldown 源盘】
|   |   |   |   |   |   |--refillPlanner.SelectTargets 【target_first.go，最空优先】★ 新文件
|   |   |   |   |   |   |--for each target T
|   |   |   |   |   |   |   |--refillPlanner.RankFeeders     【偏满侧、禁同机、同 DiskSet 优先】★ 新增
|   |   |   |   |   |   |   |--for each feeder S
|   |   |   |   |   |   |   |   |--BalanceMgr.selectVunitForTarget                ★ 新增
|   |   |   |   |   |   |   |   |   |--ListDiskVolumeUnits + 按 Used 升序
|   |   |   |   |   |   |   |   |   |--round.pairedVids 去重                      ★ 本轮同 vid 只配一次
|   |   |   |   |   |   |   |   |   |--GetVolumeInfo + vol.IsIdle
|   |   |   |   |   |   |   |   |   |--BuildTargetExcludeRule + rule.Allow(T)     ★ 改签名收 srcDiskID
|   |   |   |   |   |   |   |   |--BalanceMgr.addBalanceTask(S, vuid, planned=T.DiskID)
|   |   |   |   |   |   |   |   |--round.pairedVids[vid] = {}
|   |   |   |   |-- else 只有 DriveSource 的 idc
|   |   |   |   |   |--BalanceMgr.selectDisksOfIDC(useTrigger=false)              ★ 从 selectDisks 拆出
|   |   |   |   |   |--BalanceMgr.addSourceTasks                                  ★ 从 collectionTask 拆出
|   |   |-- else                                       【phase==nil：纯源驱动，默认行为】
|   |   |   |--BalanceMgr.selectDisks
|   |   |   |   |--for each idc -> selectDisksOfIDC(useTrigger=true)
|   |   |   |   |   |--trigger.ShouldTrigger
|   |   |   |   |   |--FilterSourceCandidates          【IsMigratingDisk || isSourceCooldowned】
|   |   |   |   |   |--sortDiskByFreeChunkCnt          【free 升序，最满在前】
|   |   |   |   |   |--sourceSelector.SelectSources
|   |   |   |   |--sortDiskByFreeChunkCnt
|   |   |   |--BalanceMgr.addSourceTasks
|   |   |   |   |--sourceSelector.MaxTasksPerRound     【most_* 每轮 1 个】
|   |   |   |   |--BalanceMgr.genOneBalanceTask                                    ★ 改：多收一个 planned
|   |   |   |   |   |--BalanceMgr.selectBalanceVunit
|   |   |   |   |   |   |--ListDiskVolumeUnits + 按 Used 升序
|   |   |   |   |   |   |--GetVolumeInfo + volInfo.IsIdle
|   |   |   |   |   |--BalanceMgr.addBalanceTask(planned=InvalidDiskID)
```

目标驱动与源驱动的唯一差别就是 `planned` 传不传 T；之后共用同一个出口：

```
|--BalanceMgr.addBalanceTask                           【scheduler/balancer.go】★ 改：多收一个 planned
|   |--client.GenMigrateTaskID
|   |--MigrateTask{State: MigrateStateInited, PlannedTargetDiskID: planned}      ★ 新字段 json:"-"
|   |--MigrateMgr.AddTask                              【scheduler/migrate.go】
|   |   |--clusterMgrCli.AddMigrateTask                【落 CM KV，state=1；pin 不落盘】
|   |   |--prepareQueue.PushTask
|   |   |--addMigratingVuid
```

### 12.3 Prepare 链路（消费 pin + alloc，state=1 → 2）

```
|--MigrateMgr.prepareTaskLoop                          【scheduler/migrate.go】
|   |--workQueue 满则 sleep
|   |--MigrateMgr.prepareTask
|   |   |--prepareQueue.PopTask
|   |   |--VolTaskLockerInst().TryLock(vid)
|   |   |--clusterMgrCli.GetVolumeInfo
|   |   |-- if SourceVuid != 当前 vuid                 【unit 已被搬走】
|   |   |   |--UnlockVolume
|   |   |   |--finishTaskInAdvance("volume has migrated")          -> state=5
|   |   |--clusterMgrCli.LockVolume
|   |   |   |-- if CodeLockNotAllow
|   |   |   |   |--lockVolFailHandleFunc -> handleLockVolFail
|   |   |   |   |   |--finishTaskInAdvance("lock volume fail")     -> state=5
|   |   |--MigrateMgr.allocMigrateVunit
|   |   |   |-- if targetDiskAlloc == nil              【target_select_strategy=auto，pin 无人消费】
|   |   |   |   |--base.AllocVunitSafe
|   |   |   |   |   |--clusterMgrCli.AllocVolumeUnit(vuid, isBalance=true)
|   |   |   |   |   |--base.checkAllocNotConflict
|   |   |   |-- else                                   【most_free / most_level_free】
|   |   |   |   |--BalanceMgr.SelectTarget             【scheduler/balancer.go】★ 改：透传 pin
|   |   |   |   |   |--clusterTopology.GetIDCDisks            【< 2 块 -> ErrNoTargetDisk】
|   |   |   |   |   |--BuildTargetExcludeRule                                    ★ 改签名收 srcDiskID
|   |   |   |   |   |--IsWritableDisk 过滤 -> in.IDCDisks / in.SrcIdx
|   |   |   |   |   |--rule.Allow 过滤 -> in.CandIdx          【①硬约束】
|   |   |   |   |   |--in.PreferDiskID = task.PlannedTargetDiskID                ★ 新增入参
|   |   |   |   |   |--targetSelector.SelectTarget     【balance_strategy_target.go】
|   |   |   |   |   |   |--rankCandidates                     【②策略排序】
|   |   |   |   |   |   |   |--excludedTargets                【CM 曾拒过的 target】
|   |   |   |   |   |   |   |--most_free      : 按 FreeChunkCnt 降序
|   |   |   |   |   |   |   |--most_level_free: classifyTarget + Deviation + WeightedOrder
|   |   |   |   |   |   |   |--preferFirst                    【pin 提到最前】★ 新增
|   |   |   |   |   |   |--for each cand
|   |   |   |   |   |   |   |--benefit.Accept                 【③收益：方差须变小】
|   |   |   |   |   |   |--全不过 -> ErrNoTargetDisk（cands==0 时 forget 排除表）
|   |   |   |   |-- if SelectTarget 失败
|   |   |   |   |   |--BalanceMgr.OnAllocResult(target=InvalidDiskID, err)
|   |   |   |   |   |   |--coolDownSource(30m)
|   |   |   |   |   |   |--targetSelector.OnAllocResult       【日志 excluded[N]】
|   |   |   |   |--base.AllocVunitOnDiskSafe
|   |   |   |   |   |--clusterMgrCli.AllocVolumeUnitOnDisk(vuid, targetDiskID, isBalance=true)
|   |   |   |   |   |--base.checkAllocNotConflict
|   |   |   |   |--BalanceMgr.OnAllocResult(targetDiskID, err)
|   |   |   |   |   |-- err==nil  -> clusterTopology.UpdateAfterChunkMove
|   |   |   |   |   |-- err!=nil  -> targetSelector.OnAllocResult 记录该 target
|   |   |-- if err is ErrNoTargetDisk
|   |   |   |--UnlockVolume
|   |   |   |--finishTaskInAdvance("no beneficial target disk")    -> state=5
|   |   |--migTask.SetDestination + State = MigrateStatePrepared   -> state=2
|   |   |--clusterMgrCli.UpdateMigrateTask
|   |   |--workQueue.AddPreparedTask
|   |   |--prepareQueue.RemoveTask
```

pin 在这条链路上只影响 `preferFirst` 一步的排序，`rule.Allow`、`benefit.Accept`、CM 拒盘三道关卡
一个都不跳过；`PreferDiskID == InvalidDiskID`（源驱动任务或重启重载的任务）时 `preferFirst` 直接
原样返回。

以下 12.4～12.6 **本 MR 一行未改**，列出来只为把状态机走完、便于对照。其中
`finishTaskInAdvance("no beneficial target disk")` 与 `coolDownSource` 来自前序 MR
（`balance 无目标盘时提前结束`、`选不出目标盘时冷却源盘`），`AllocVolumeUnitOnDisk` 来自
`feature_scheduler` / `feature_clustermgr` 两个 MR。

### 12.4 Worker 执行与 Finish 链路（state=2 → 3 → 4）

```
|--〖Worker 侧〗轮询                                    【scheduler/migrate.go】
|   |--MigrateMgr.AcquireTask       -> workQueue.Acquire
|   |--MigrateMgr.RenewalTask       -> workQueue.Renewal      【租约 TaskLeaseExpiredS=10s】
|   |--MigrateMgr.ReclaimTask / CancelTask                    【失败重投】
|   |--MigrateMgr.CompleteTask      -> finishQueue.PushTask    -> state=3

|--MigrateMgr.finishTaskLoop
|   |--MigrateMgr.finishTask
|   |   |--finishQueue.PopTask（须为 MigrateStateWorkCompleted）
|   |   |--clusterMgrCli.UpdateMigrateTask
|   |   |--clusterMgrCli.UpdateVolume(dstVuid, srcVuid, dstDiskID)  【改卷映射】
|   |   |   |-- 失败 -> handleUpdateVolMappingFail
|   |   |   |   |-- ShouldAllocAndRedo -> allocMigrateVunit 重新 alloc + WorkerRedoCnt++
|   |   |--clusterMgrCli.ReleaseVolumeUnit(srcVuid, srcDiskID)
|   |   |--clusterMgrCli.UnlockVolume
|   |   |--State = MigrateStateFinished                        -> state=4
|   |   |--clusterMgrCli.DeleteMigrateTask                     【从 KV 删除】
|   |   |--taskLogger.Encode                                   【写 task_log】
|   |   |--updateVolumeCache / VolTaskLocker.Unlock / deleteMigratingVuid
|   |   |--addDeletedTask + finishTaskCallback
```

### 12.5 提前结束与垃圾清理

```
|--MigrateMgr.finishTaskInAdvance                      【state=5 的唯一出口】
|   |--State = MigrateStateFinishedInAdvance
|   |--FinishAdvanceReason = reason
|   |   |-- "volume has migrated" | "lock volume fail" | "no beneficial target disk"
|   |--clusterMgrCli.DeleteMigrateTask
|   |--taskLogger.Encode
|   |--prepareQueue.RemoveTask + addDeletedTask
|   |--VolTaskLocker.Unlock + deleteMigratingVuid

|--BalanceMgr.checkAndClearJunkTasksLoop               【scheduler/balancer.go】
|   |--BalanceMgr.checkAndClearJunkTasks
|   |   |--DeletedTasks（过 junkMigrationTaskProtectionWindow）
|   |   |--clusterMgrCli.GetMigrateTask
|   |   |   |-- 404   -> 只清内存
|   |   |   |-- 存在  -> clusterMgrCli.DeleteMigrateTask
|   |   |--ClearDeletedTaskByID
```

### 12.6 与 ClusterMgr alloc 栈的衔接

Prepare 的两个 alloc 分支对应 CM 侧不同入口，`isBalance` 恒为 `true`
（`NewBalanceMgr` 里 `migrator.isBalanceAlloc = true`）：

| Scheduler 侧 | CM 接口 | CM 侧分支 |
|---|---|---|
| `base.AllocVunitSafe`（`target_select=auto`） | `AllocVolumeUnit(vuid, isBalance)` | `allocFromBlobNodeOrDisk` → `isBalance==true` → `allocFromDiskStorages` |
| `base.AllocVunitOnDiskSafe`（Scheduler 选盘） | `AllocVolumeUnitOnDisk(vuid, targetDiskID, isBalance)` | 目标盘已定，跳过 rack/host 挑选 |

CM 侧完整栈（供对照）：

```
|--Service.VolumeUnitAlloc
|   |--VolumeMgr.AllocVolumeUnit
|   |   |-- if TargetDiskID != 0
|   |   |   |--VolumeMgr.validateTargetDiskForAlloc -> DiskMgr.ValidateTargetDisk
|   |   |--VolumeMgr.Apply
|   |   |   |--VolumeMgr.applyAllocVolumeUnit
|   |--DiskMgr.AllocChunks
|   |   |-- if TargetDiskID != 0
|   |   |   |--DiskMgr.allocOnTargetDisk
|   |   |--allocator.ReAlloc
|   |   |   |--idcAllocator.alloc
|   |   |   |   |-- if s.diffRack && s.diffHost
|   |   |   |   |   |--idcAllocator.allocFromRack
|   |   |   |   |   |   |--idcAllocator.allocFromBlobNodeOrDisk
|   |   |   |   |   |   |   |-- if isBalance == true
|   |   |   |   |   |   |   |   |--idcAllocator.allocFromDiskStorages
|   |   |   |   |   |   |   |-- if isBalance == false
|   |   |   |   |   |   |   |   |--idcAllocator.allocFromBlobNodeStorages
|   |   |   |   |   |   |   |   |   |--blobNodeAllocator.allocDisk
|   |   |   |   |-- if s.diffRack==false || s.diffHost==false
|   |   |   |   |   |--idcAllocator.allocFromBlobNodeOrDisk
|   |   |   |   |   |   |-- 同上两分支
|   |   |--DiskMgr.validateAllocRet
```

层级差异：CM 的 `DiskMgr.validateAllocRet` 在 CM 内部校验分配结果（指定目标盘时改为分配前由
`ValidateTargetDisk` 校验）；Scheduler 侧对应的是 `base.checkAllocNotConflict`，发现分配盘与卷内其它
unit 同盘时直接 `span.Panic`，属兜底断言而非常规错误路径。
