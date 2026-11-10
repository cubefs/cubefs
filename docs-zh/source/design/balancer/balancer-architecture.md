# Balancer 架构设计

Scheduler 的 **balance**（数据均衡）把同一 idc 内各盘的空闲 chunk（`FreeChunkCnt`）搬匀：
把满盘上的 volume unit（`vuid`）迁到更空的盘。
它和 repair / drop / 手工迁移共用 `MigrateMgr`，只在 Scheduler leader 上运行。

**本文范围**：说明 balance 的整体架构，以及本次对均衡决策的改造：何时触发、从哪里搬、搬到哪里、是否值得搬。
数据迁移的执行框架沿用原有实现，本文只说明决策逻辑接在哪里。

**与目标优先设计的关系**：本文是
[目标优先 Balance（换盘灌空盘）](scheduler-target-first-balance.md) 的前置背景。
目标优先复用本文的拓扑、策略、prepare 选盘和安全机制，只改变 collect 的决定顺序。
阅读目标优先前，至少需要掌握本文第 1、4、6、8 节和第 14 节。

**硬约束**：不跨 idc；不破坏卷的故障域；一个卷同时只有一个任务；`DiskConcurrency` 限制同时迁出的源盘数。

### 术语

| 术语 | 含义 |
|---|---|
| idc | 数据中心，盘按 idc 分组。均衡只在同一 idc 内进行 |
| `FreeChunkCnt` | 盘上还能分配的 chunk 数。本文所说的「满 / 空」都按它衡量 |
| volume unit / `vuid` | 卷在某块盘上的一个分片，占用一个 chunk。一次 balance 任务搬一个 unit |
| 源盘 S / 目标盘 T | 数据搬出 / 搬入的盘 |
| 可写盘 | 状态正常且非 readonly 的盘（`IsWritableDisk`） |
| 源驱动 | collect 先定 S 和 `vuid`，prepare 再定 T。当前默认路径 |
| 目标优先（目标驱动） | collect 先定 T，再找能搬进 T 的 S 和 `vuid`。见下一篇 |

---

## 1. 背景：沿用的迁移执行框架

balance 不是独立的数据通道，而是复用 `MigrateMgr` 的迁移框架。一条任务依次经过四个阶段：

```text
collect ──▶ prepare ──▶ worker ──▶ finish
建任务        选目标盘、     搬数据       改卷映射、
             分配 chunk                 释放旧 unit
```

| 阶段 | 负责组件 | 做什么 |
|---|---|---|
| collect | `BalanceMgr` | 判断是否需要均衡，选源盘 S 和 `vuid`，创建任务 |
| prepare | `MigrateMgr` | 锁卷，确定目标盘 T，向 ClusterMgr 申请新 chunk |
| worker | BlobNode | 把数据从旧 unit 拷到新 unit |
| finish | `MigrateMgr` | 切换卷映射，释放旧 unit，解锁，删除任务 |

四个阶段由各自的循环异步推进，通过三个队列和 ClusterMgr 上的任务 KV 衔接。

ClusterMgr（CM）不是其中一个阶段，而是各阶段都会调用的元数据服务：保存任务、提供卷信息和卷锁、
分配 chunk、更新卷映射。其中 **alloc chunk 发生在 prepare**。

本次没有修改 worker 和 finish 的正常路径，改动集中在 collect 和 prepare 的**决策**上。

---

## 2. 原架构的问题

### 2.1 原有决策逻辑

原有 balance 把决策写死在 `BalanceMgr` 里，没有策略层：

```text
collect：
  idc 里有没有盘 FreeChunkCnt ≥ MaxDiskFreeChunkCnt？      # 绝对阈值：有地方可灌
    → 把该 idc 里 FreeChunkCnt < MinDiskFreeChunkCnt 的盘当源   # 绝对阈值：够满
    → 源盘上按 Used 升序找第一个卷空闲的 vuid
prepare：
    → AllocVolumeUnit，由 ClusterMgr 选目标盘
```

### 2.2 四个问题

**1. 触发看「有没有空盘」，不看「匀不匀」**

`default` trigger 的条件是「某块可写盘 free 够高」。集群整体很空、各盘差不多时也会开；
集群整体较满、但盘与盘之间差很多时，只要没有一块盘跨过 `MaxDiskFreeChunkCnt` 就不开。
绝对阈值跟均值、盘数无关，换一批盘或改 chunk 规格就要重配，不同集群还得各配一套；
集群水位实时变化，阈值也得不断调整，运维负担重。

**2. 选源同样是绝对阈值，且会一把抓**

`free < MinDiskFreeChunkCnt` 的盘全部当源，没有每轮上限（只受 `DiskConcurrency` 限制）。
阈值配松会同时迁很多盘，挤占业务 IO；配紧则明明不均衡却选不出源。

**3. CM 的目标盘分配不是面向均衡收敛设计的**

`AllocVolumeUnit` 是通用的 chunk 分配机制：先按故障域过滤候选盘，再以 `FreeChunkCnt` 为权重随机选择目标盘。
它的目标是满足故障域约束并分散数据，并不保证每次 balance 迁移都能改善均衡度：

- 加权随机只表示空闲越多被选中的概率越高，不保证选中较空的盘。即使盘分布相同，不同轮次也可能选出不同目标盘。
- 不判断搬迁收益：目标可能只比源盘空一点，搬完后方差甚至不降反升。CM 分配成功，任务就继续执行，
  消耗了迁移 IO，却没有让分布明显变匀。

**4. 源驱动顺序让极空盘难以接到数据**

collect 先选定 `vuid`，prepare 才选目标盘。如果该卷已有 unit 位于极空盘所在 host，
故障域规则会排除这块极空盘。故障域限制本身正确且不能绕过；问题在于源驱动不会围绕极空盘
改找一个与它兼容的卷。于是极空盘可能连续多轮接不到数据，表现为「其它盘一直在搬，极空盘仍然很空」。

### 2.3 本文解决哪些问题

| 问题 | 本文的处理 | 结果 |
|---|---|---|
| 1. 触发用绝对阈值 | Trigger 改为相对极差 + 滞回（`free_chunk_hysteresis`） | 解决 |
| 2. 选源绝对阈值、一把抓 | Source selector 按均值分档，每轮限量 | 解决 |
| 3. CM 随机选盘、不判断搬迁收益 | Scheduler 选目标盘 + Benefit evaluator 判断收益 | 解决 |
| 4. 源驱动顺序导致极空盘接不到数据 | 本文不改变 collect 顺序 | 留给目标优先 |

前三项的「解决」指本文提供了对应策略。默认配置仅用于兼容和回退，行为与原架构一致；
实际生效需要按第 12 节的示例显式配置新策略。

前三个问题属于「每一步怎么选」，可以在现有流程里替换决策；第四个问题属于「先选什么」，
需要改变 collect 顺序，因此单独设计。第 14 节会说明为什么本文的改造不足以解决它。

---

## 3. 设计目标与思路

### 3.1 设计目标

| 目标 | 做法 |
|---|---|
| 触发不依赖集群规模和水位 | 相对极差 `(max-min)/mean` + 进入 / 退出两个阈值 |
| 搬迁方向可控 | Scheduler 根据全 idc 分布选目标盘，CM 在指定盘上分配 |
| 每次搬迁都有收益 | Benefit evaluator：搬迁后方差变小才执行 |
| 不盯死同一块盘，也不空转 | 分档 + 类内加权随机；乐观更新拓扑；选不出目标时冷却源盘 |
| 可扩展 | 四个决策维度独立成策略，主流程不随算法变化 |
| 兼容和回退 | 默认配置等价原行为；`target_select_strategy = auto` 时仍由 CM 选盘 |

**非目标**：不改迁移执行框架；不改 collect 的决定顺序（见第 14 节）；不做跨 idc 均衡。

### 3.2 核心思路：Scheduler 定目标盘，CM 做最终分配

均衡需要看 **idc 内 `FreeChunkCnt` 的整体分布**，这份信息由 Scheduler 的拓扑缓存维护，
CM 的通用分配接口并不关心它。因此在发起 alloc 前，由 `BalanceMgr` 选定目标盘，
再调用 CM 新增的 `AllocVolumeUnitOnDisk` 在指定盘上分配 chunk。

这不是绕过 CM：Scheduler 只是提前做均衡决策，CM 仍负责最终的故障域校验和实际分配。
Scheduler 本地先过滤，是为了减少注定会被拒绝的请求；CM 仍然拒盘时，Scheduler 记录结果，下次跳过该盘。

### 3.3 决策拆成四个独立维度

目标盘选择前移后，`BalanceMgr` 要同时回答四个问题。为避免规则重新耦合进主流程，拆成四个策略：

| 维度 | 回答的问题 | 特意不让它做的 |
|---|---|---|
| Trigger（触发器） | 这个 idc 要不要均衡 | 不指定搬哪块盘 |
| Source selector（源盘选择器） | 从哪几块盘抽、什么顺序 | 不管目标盘、不管收益 |
| Target selector（目标盘选择器） | 目标候选按什么顺序试 | 不判定「过不过」 |
| Benefit evaluator（收益评估器） | 这次搬迁值不值得（是 / 否） | 不重排候选 |

当前所有策略只看 `FreeChunkCnt`。后续引入 `UsedBytes` 等多维指标时，替换对应策略即可，
不用修改 `collectionTask` / `prepareTask` 主流程。各策略的实现见第 6 节。

---

## 4. 整体架构

```text
       +--------------------------------------------------------------+
       | ClusterMgr                                                   |
       | metadata / task KV / lock / alloc / volume map               |
       +--------------------------------------------------------------+
                                      |
                        metadata / task / lock / alloc
                                      v
+--------------------------- Scheduler Leader ------------------------------+
|                                                                           |
|  +----------------------+     +----------------------+     +------------+ |
|  | ClusterTopologyMgr   |<--->| BalanceMgr           |<--->| MigrateMgr | |
|  | topology cache       |     | balance decisions    |     | queues     | |
|  | sorted by free chunk |     | trigger/source/      |     | prepare    | |
|  |                      |     | target/benefit       |     | work       | |
|  +----------------------+     +----------------------+     | finish     | |
|                                                            +------+-----+ |
+---------------------------------------------------------------------------+
                                      |
                          Acquire / Renew / Complete
                                      v
                           +----------------------+
                           | BlobNode Worker      |
                           | copy data to target  |
                           +----------------------+
```

### 4.1 分层与组件

| 层 | 组件 | 职责 | 代码 |
|---|---|---|---|
| 策略层 | `strategy.*` | Trigger / Source / Target / Benefit 的接口与实现 | `blobstore/scheduler/strategy/` |
| 决策层 | `BalanceMgr` | collect 建任务；prepare 时代为选目标盘；处理 alloc 结果；源盘冷却 | `blobstore/scheduler/balancer.go` |
| 执行层 | `MigrateMgr` | 三队列、状态机；prepare 发起 alloc；finish 切换卷映射。与 repair / drop / 手工迁移共用 | `blobstore/scheduler/migrate.go` |
| 数据来源 | `ClusterTopologyMgr` | 从 CM 拉盘信息，按 idc 维护有序盘表；alloc 成功后乐观更新 | `blobstore/scheduler/cluster_topology.go` |
| 元数据服务 | ClusterMgr | 任务 KV、卷信息与卷锁、alloc chunk、卷映射 | `blobstore/clustermgr/` |
| 数据面 | BlobNode Worker | 领取任务，实际搬数据 | `blobstore/blobnode/task_runner_*` |

`BalanceMgr` 回答「该不该搬、搬哪里」，`MigrateMgr` 回答「任务怎么排队、执行、收尾」。
执行层不需要理解均衡策略：两者通过 `AddTask`、`SelectTarget`、`OnAllocResult` 交互。

### 4.2 决策点在流水线上的位置

四个策略不是新的阶段，而是插在执行流程里的决策点：

```text
collect   ① Trigger    这个 idc 要不要均衡
          ② Source     从哪几块盘抽
              → 在源盘上选 vuid，AddTask（state = Inited）
prepare   ③ Target     目标候选按什么顺序试
          ④ Benefit    这次搬迁值不值得
              → CM alloc chunk（state = Prepared）
worker        搬数据（沿用，未改）
finish        切换卷映射、释放旧 unit（沿用）；异常重做时复用 ③④ 和 alloc
```

Trigger、Source 只在 collect 中调用；Target、Benefit 和 CM alloc 只在 prepare 中调用。
所以源驱动下，选 `vuid` 时还不知道最终的 T——这正是第 2.2 节问题 4 的来源。

### 4.3 本次改动

| 阶段 / 组件 | 改动 | 说明 |
|---|---|---|
| collect | 修改 | Trigger、Source 策略化；默认顺序仍是 S → `vuid` |
| prepare | 修改 | Scheduler 选 T + 收益判断；钉盘 alloc；结果回灌；失败冷却 |
| worker | 未改 | — |
| finish 正常路径 | 未改 | — |
| finish 异常重做 | 修改 | 重新申请目标时与 prepare 共用同一入口，不再退回 CM 随机选盘 |
| ClusterMgr | 新增接口 | `AllocVolumeUnitOnDisk`：在指定盘上分配并校验 |
| `MigrateMgr` | 新增接口 | `ITargetDiskAllocator`：`SelectTarget`（alloc 前选盘）、`OnAllocResult`（回灌结果） |
| `ClusterTopologyMgr` | 新增能力 | `UpdateAfterChunkMove` 乐观更新 |

prepare 的 alloc 因此变成两条可选路径：

```text
targetDiskAlloc == nil（target_select_strategy = auto）
  → AllocVolumeUnit，由 CM 选目标盘（原行为）

targetDiskAlloc != nil
  → BalanceMgr.SelectTarget
  → CM.AllocVolumeUnitOnDisk
  → BalanceMgr.OnAllocResult
```

不配置新策略时，所有行为回退到 Redundancer 原有默认。

---

## 5. 集群拓扑：决策的数据来源

所有策略都基于 `ClusterTopologyMgr` 提供的盘分布做判断：

```text
ClusterTopology
  ├─ allDisks  map[DiskID]*DiskInfoSimple
  ├─ diskMap   map[idc][]*DiskInfoSimple     # 每个 idc 的盘表，按 FreeChunkCnt 升序（最满在前）
  └─ idcMap    map[idc]*IDC
                 └─ rackMap map[rack]*Rack
                              └─ diskMap map[host]*Host
                                           └─ disks []*DiskInfoSimple
         （cluster / idc / rack / host 各级都带 FreeChunkCnt / MaxChunkCnt 汇总）
```

| 点 | 说明 |
|---|---|
| 全量刷新（沿用） | `loopUpdate` 按 `UpdateInterval` 从 CM `ListClusterDisks` 全量重建，仅 leader |
| 乐观更新（本次新增） | alloc 成功后 `UpdateAfterChunkMove(src, dst)`：src `Free++`、dst `Free--`，各级汇总同步 ±1，并重排该 idc 盘表。下一次全量刷新以 CM 真值覆盖 |
| 为什么需要乐观更新 | 一个刷新周期内可能连续建、选多个任务；不更新的话后续决策仍基于旧分布，会反复往同一块盘搬 |
| 可写盘 | `IsWritableDisk`：状态正常且非 readonly。Trigger、统计、目标候选都只看可写盘，避免 readonly 把均值带偏 |
| broken 盘 | `loadBrokenDisks` 单独维护坏盘 / 修复中盘，不进入 balance 视野 |
| 卷信息 | balance 的 collect / prepare 直接调 CM 取卷信息，不走 `IVolumeCache` |

---

## 6. 决策策略

### 6.1 四个维度与默认值

`strategy.Config` 把决策拆成四个互相独立的维度。四个维度的默认组合等价于 Redundancer 原有行为。

| 维度 | 接口 | 调用阶段 | 实现 | 默认 |
|---|---|---|---|---|
| Trigger | `ITrigger` | collect | `default` / `free_chunk_hysteresis` | `default` |
| Source selector | `ISourceSelector` | collect | `default` / `below_mean_free` / `most_full` / `most_level_full` | `default` |
| Target selector | `ITargetSelector` | prepare | `auto`(nil) / `most_free` / `most_level_free` | `auto` |
| Benefit evaluator | `IBenefitEvaluator` | prepare | `free_chunk_variance` | 同左 |

`target_select_strategy = auto` 时 `ITargetSelector` 为 nil，prepare 把选盘交给 CM。
Benefit 只在 Scheduler 选盘路径生效；`target_select_strategy = auto` 时 prepare 和 finish 异常重做都直接走 CM 分配，
不经过该策略。因此原架构虽然没有收益判断，默认组合仍与原行为一致。
新增策略只需增加枚举、实现和工厂分支。启动时 `Config.Validate` 拒绝不存在的策略名。

常见组合：

| 场景 | trigger | source | target |
|---|---|---|---|
| Redundancer 原有配置 | `default` | `default` | `auto` |
| 按相对不均衡 + 精细选盘 | `free_chunk_hysteresis` | `most_level_full` | `most_level_free` |

另有 `IRefillPlanner` / `IPhaseCoordinator` 两个扩展维度，默认关闭，开启后走目标优先 collect，
见 [scheduler-target-first-balance.md](scheduler-target-first-balance.md)。

### 6.2 Trigger：要不要均衡

| 实现 | 条件 | 状态 |
|---|---|---|
| `default` | idc 内存在可写盘 `FreeChunkCnt ≥ max_disk_free_chunk_cnt`（有地方可搬进去） | 无状态 |
| `free_chunk_hysteresis` | 极差 `R = (max-min)/mean`：`R ≥ trigger_enter` 开，`R ≤ trigger_exit` 关 | 每 idc 一个开关，带滞回 |

相对量解决「一套 Max/Min 适配不了不同均值」；滞回解决「刚匀一点又触发、刚不匀又停下」。
`trigger_exit > trigger_enter` 时会被修正为 `enter × 0.8`。Trigger 只做总开关，不参与选源和选目标。

对同一批可写盘的 `FreeChunkCnt`，记 `mean` / `min`（最满）/ `max`（最空）：

```text
R = (max - min) / mean     # 极差：整段不均，hysteresis trigger 用
S = (mean - min) / mean    # 满头 source_skew：最满盘偏离均值多少，日志观测用
T = (max - mean) / mean    # 空头 target_skew：最空盘偏离均值多少，目标优先的相位用
```

恒有 `R = S + T`。例如 `trigger_enter = 0.20` 表示：最空盘和最满盘的空闲 chunk 之差达到均值的 20% 才开始均衡。
源驱动只使用 `R`；`T` 在目标优先中用于识别「极空盘」。

### 6.3 Source selector：从哪几块盘抽

入参已过 `FilterSourceCandidates`（剔除 readonly / 不健康 / 迁移中 / 冷却中的盘），并按 `FreeChunkCnt` 升序（最满在前）。

严格只选最满盘，会让同一块盘一直占着 `DiskConcurrency`，相邻同样偏满的盘轮不到。
`most_level_*` 借鉴 Ceph upmap：先按偏离均值的 δ 带分档，**先处理档位更差的**；
同一档内按「偏了多少」加权随机，偏得多的更常排前，但不保证每次第一。

| 实现 | 选法 | 每轮任务上限 `MaxTasksPerRound` |
|---|---|---|
| `default` | `free < min_disk_free_chunk_cnt` 的所有盘（绝对阈值） | 0（不额外限制） |
| `below_mean_free` | `free < 本轮源盘候选集均值` 的所有盘 | 0 |
| `most_full` | 整个有序候选，最满优先 | 1 |
| `most_level_full` | `overfull`（`free ≤ mean-δ`）先于 `moreOverfull`（均值与 `mean-δ` 之间），类内按「低于均值多少」加权随机 | 1 |

- 源侧 `mean` 只统计本轮源盘候选集，不是整个 idc。
- `δ = max(balance_deviation_min, balance_deviation_ratio × mean)`：随均值缩放，又不会在均值趋近 0 时塌缩。
- `most_*` 返回完整有序列表：某块盘建不出任务时，本轮顺延给下一块。
- `most_*` 每轮只出 1 个任务，让乐观更新作用到下一轮，避免同一轮多个任务按同一张过时快照往同一块盘挤。

### 6.4 Target selector：目标候选按什么顺序试

`target_select_strategy != auto` 时，prepare 里的 `SelectTarget` 按三道关卡确定目标盘。
第一关是硬约束，第二关由 Target selector 排序，第三关由 Benefit evaluator 判断：

```text
源 idc 内可写盘
   │
   ① 硬约束：BuildTargetExcludeRule(src, vol).Allow(disk)
   │     · free > 0
   │     · 目标盘不在该卷已有 unit 上（含源盘本身）
   │     · host-aware：目标 host 不能已持有该卷的「其它」unit（源 host 自己那份豁免，允许同机换盘）
   │     · replicate 模式：同 DiskSetID
   ▼
   ② Target selector 排序
   │     · 去掉本源盘上 CM 曾拒绝过的目标（failedTargets）
   │     · 按所选策略排序
   ▼
   ③ Benefit evaluator：逐个候选判断
         第一个通过的即目标；全部不通过 → ErrNoTargetDisk
```

`Allow` 镜像的是 CM `ValidateTargetDisk` 中 Scheduler 能评估的部分，CM 真正 alloc 时还会再校验一遍。

| 实现 | 排序方式 |
|---|---|
| `auto` | 不在 Scheduler 侧选盘，由 CM `AllocVolumeUnit` 加权随机分配 |
| `most_free` | `FreeChunkCnt` 降序，最空优先 |
| `most_level_free` | 只留偏空侧：`underfull`（`free ≥ mean+δ`）先于 `moreUnderfull`（均值与 `mean+δ` 之间），类内按「高于均值多少」加权随机 |

目标侧的 `mean` / `δ` 按源 idc 的**全部可写盘**计算，和源侧候选集口径不同。

### 6.5 Benefit evaluator：值不值得搬

Benefit 只返回 bool，不重排候选。当前实现 `free_chunk_variance`：仅当搬迁后 idc 可写盘 `FreeChunkCnt` 方差变小时接受。

搬一个 unit 等于 `src.Free + 1`、`dst.Free - 1`，均值不变。设 `s = src.Free`、`d = dst.Free`：

```text
平方和变化 = (s+1)² + (d-1)² - s² - d² = 2(s - d + 1)
方差变小  ⇔  d ≥ s + 2
```

即**目标盘的空闲 chunk 至少要比源盘多 2**，否则不搬。

后续引入多维指标时，只替换评估器内部公式，主流程仍只接收「值得搬 / 不值得搬」。
排序和收益判断分开，避免把「先试谁」和「值不值得」重新耦合。

### 6.6 统计量与口径

`balancer_stats.go` 提供与业务语义无关的 `Stats{Mean, Min, Max}`，以及 `RelativeRange` / `BelowMeanRange` /
`AboveMeanRange`、`Variance` / `Deviation` / `WeightedOrder`（按权重无放回随机排列，权重大的更可能排前但不保证）。

| 使用方 | 统计范围 |
|---|---|
| Trigger、Target selector、Benefit evaluator | 源 idc 的全部可写盘 |
| Source selector | 经过 `FilterSourceCandidates` 后的本轮源盘候选集 |

---

## 7. 任务模型与生命周期（沿用）

任务模型与正常生命周期沿用迁移框架的既有机制；finish 异常重做分支的改动见第 10 节。
列出来是为了说明决策结果如何落到任务上。

### 7.1 任务

balance 任务是一个 `proto.MigrateTask`（`TaskType = balance`），语义是「把 `SourceVuid` 从 `SourceDiskID` 搬走」：

| 字段 | 何时填 | 说明 |
|---|---|---|
| `TaskID` | collect | `GenMigrateTaskID(balance, srcDiskID, vid)` |
| `SourceIDC` / `SourceDiskID` / `SourceVuid` | collect | 搬哪块盘上的哪个 unit |
| `Sources` / `CodeMode` | prepare | 从卷信息填入，worker 用来定位源副本 |
| `Destination` | prepare；finish 异常重做时更新 | alloc 出来的目标 unit |
| `State` | 全程 | 见下 |

源驱动下，collect 只确定**源盘 + `vuid`**，目标盘到 prepare 才确定。

### 7.2 状态机与三队列

```text
              collect              prepare                worker               finish
  (无) ──AddTask──▶ Inited(1) ───────────▶ Prepared(2) ───────────▶ WorkCompleted(3) ──────▶ Finished(4)
                       │
                       └── 提前结束 ───────────────────────────────────────────────────▶ FinishedInAdvance(5)
                           volume has migrated / lock volume fail / no beneficial target disk
```

| 状态 | 所在队列 | 持久化 | 推进者 |
|---|---|---|---|
| `Inited` | `prepareQueue` | CM 任务 KV | `prepareTaskLoop` |
| `Prepared` | `workQueue`（按 idc 分组，worker 领取） | CM 任务 KV | BlobNode worker |
| `WorkCompleted` | `finishQueue` | `CompleteTask` 优先写 CM KV；写失败时由 finish 补偿 | `finishTaskLoop` |
| `Finished` / `FinishedInAdvance` | 无 | 从 KV 删除，写 `task_log`，内存记入 `deletedTasks` | — |

### 7.3 四条异步循环

| 循环 | 周期 | 做什么 |
|---|---|---|
| `collectTaskLoop` | `collect_task_interval_s`（默认 5s） | Trigger → 选源盘 S → 选 `vuid` → `AddTask` |
| `prepareTaskLoop` | 1s 轮询；`workQueue` 满则暂停 | 锁卷 → 选目标盘 T → alloc → `Prepared` |
| `finishTaskLoop` | 1s 轮询 | 改卷映射 → 释放源 unit → 解锁 → 删 KV |
| `checkAndClearJunkTasksLoop` | 1h | 清理网络超时后可能残留在 CM 的垃圾任务 |

BlobNode 侧 worker 轮询：`AcquireTask` → `RenewalTask` → `CompleteTask` / `CancelTask` / `ReclaimTask`。

### 7.4 重启恢复

`Service.waitAndLoad` 先等待一个租约周期（`TaskLeaseExpiredS = 10s`），确保上一任 leader 的 worker 都已释放任务；
然后 `BalanceMgr.Load()` 从 CM 取全部 balance 任务，**按 state 放回三队列**，重建 `diskMigratingVuids`，
并对运行中的任务重新加卷锁。

---

## 8. Collect：源驱动建任务

本节说明默认源驱动 collect 如何调用 Trigger 和 Source。三个决定按固定顺序做，**前一步不知道后一步的约束**：

```text
collect:  ① Trigger 开启后，由 Source selector 选出源盘 S
          ② 在 S 上选一个能搬的 vuid
                │  任务入 prepareQueue，S 占用一个 DiskConcurrency 额度
                ▼
prepare:  ③ 为这个已经确定的 vuid 选目标盘 T
```

### 8.1 入口与并发额度

```text
collectionTask
  ├─ need = DiskConcurrency - GetMigratingDiskNum()
  │     need ≤ 0 → ErrTooManyBalancingTasks（本轮不建任务）
  └─ addSourceTasks(selectDisks(), need)
```

- `need` 是本轮还能新开几个**源盘**名额，不是还差多少 chunk。
- `GetMigratingDiskNum()` 是当前有在途任务的源盘数（`AddTask` 时登记，finish / 提前结束时删除）。
- 同一块源盘在途期间不会再被选为源（`IsMigratingDisk` 过滤）。
- 任务提前结束后立即释放额度。

### 8.2 选源盘

```text
selectDisks()
  for each idc:
    ├─ trigger.ShouldTrigger(idc, disks)         # 这个 idc 要不要均衡
    ├─ FilterSourceCandidates                    # 剔除 readonly / 不健康 / 迁移中 / 冷却中的盘
    └─ sourceSelector.SelectSources(...)         # 哪些盘当源、什么顺序
  合并所有 idc 的候选
```

Trigger 和 Source selector 都按 idc 独立计算，最后合并各 idc 的候选。

> 当前实现为兼容 `default` / `most_full`，在调用 selector 前按 `FreeChunkCnt` 预排序，
> 合并各 idc 后还会再次排序；后一次排序会覆盖 `most_level_full` 的加权随机顺序。
> 后续应把最终排序权收回 Source selector，并单独处理跨 idc 候选的合并。

### 8.3 选 vuid、创建并持久化任务

```text
addSourceTasks(disks, need)
  cap = min(need, sourceSelector.MaxTasksPerRound())   # most_* 每轮只出 1 个
  for disk in disks:
      genOneBalanceTask(disk)
          ├─ ListDiskVolumeUnits(disk)         # 取盘上所有 unit
          ├─ 按 Used 升序（先搬小的）
          └─ 找第一个卷空闲（IsIdle）的 unit → addBalanceTask
      成功数达到 cap → 结束
```

`addBalanceTask` 构造 `MigrateTask`（`State = Inited`），调用 `IMigrator.AddTask`：
先写 CM 任务 KV，再进入 `prepareQueue`，并登记 `diskMigratingVuids`。

这一步**不看目标盘，也不做故障域校验**。`vuid` 能否放到某块空盘，要到 prepare 才知道。

### 8.4 源盘冷却（本次新增）

prepare 选不出目标盘（`ErrNoTargetDisk`）时，`OnAllocResult` 把源盘冷却 30 分钟（`defaultBalanceSourceCooldown`）。
否则任务提前结束、额度释放后，下一轮 collect 会立即为同一块最满盘重建同样会失败的任务，形成空转。
冷却记录在内存中，过期后惰性清理。

---

## 9. Prepare：选目标盘并 alloc

prepare 由执行层 `MigrateMgr.prepareTask` 推进，CM alloc chunk 在这一段发生。
balance 任务的特殊之处在 `allocMigrateVunit`；其中 `SelectTarget` 的三道关卡见 6.4 / 6.5。

```text
prepareTask
  ├─ PopTask(prepareQueue)
  ├─ VolTaskLocker.TryLock(vid)            失败 → 重试（一卷一任务）
  ├─ GetVolumeInfo
  ├─ SourceVuid 已不在该位置 → 提前结束 "volume has migrated"
  ├─ LockVolume                            CodeLockNotAllow → 提前结束 "lock volume fail"
  ├─ allocMigrateVunit
  │     ├─ targetDiskAlloc == nil（target_select_strategy = auto）
  │     │     └─ AllocVunitSafe → CM 自己选盘
  │     └─ 否则（most_free / most_level_free）
  │           ├─ BalanceMgr.SelectTarget(task, vol)          # 三道关卡
  │           ├─ AllocVunitOnDiskSafe(srcVuid, targetDiskID)
  │           └─ BalanceMgr.OnAllocResult(...)
  ├─ ErrNoTargetDisk → 解锁卷，提前结束 "no beneficial target disk"
  └─ 填 Sources / Destination，State = Prepared，写 KV，进入 workQueue
```

### 9.1 `OnAllocResult`：结果回灌（本次新增）

| 结果 | 动作 |
|---|---|
| 成功 | `UpdateAfterChunkMove` 乐观更新拓扑；清除该源盘的失败记录 |
| CM 拒盘 | 记录 `(src → target)`，下次 prepare 重试时跳过该目标 |
| `ErrNoTargetDisk` | 冷却源盘，prepare 随后提前结束任务 |

失败记录有上限（`maxFailedTargetSourceDisks = 1024`），超过时整体重置，避免泄漏。
候选集被失败记录全部排除时，清空该源盘的记录，下次从头选。

### 9.2 与 ClusterMgr alloc 的衔接

`NewBalanceMgr` 中设置 `migrator.isBalanceAlloc = true`，两条 alloc 入口对应 CM 不同分支：

| Scheduler 侧 | CM 接口 | CM 侧 |
|---|---|---|
| `AllocVunitSafe`（`target=auto`） | `AllocVolumeUnit(vuid, isBalance)` | `isBalance == true` 时走 `allocFromDiskStorages` |
| `AllocVunitOnDiskSafe`（Scheduler 选盘） | `AllocVolumeUnitOnDisk(vuid, targetDiskID, isBalance)` | 目标盘已定，跳过 rack / host 挑选，只做校验和分配 |

Scheduler 侧兜底检查 `base.checkAllocNotConflict`：分配出的盘与卷内其它 unit 同盘，说明安全约束被破坏，
直接 panic，不按普通可重试错误处理。

---

## 10. Finish：异常重做时复用选盘

worker 搬数据和 finish 正常路径没有改动：`UpdateVolume` 切换卷映射 → 释放源 unit → 解锁 → 删 KV。

本次只修改 finish 的异常分支。`UpdateVolume` 返回 `CodeNewVuidNotMatch` 或 `CodeStatChunkFailed` 时，
`ShouldAllocAndRedo` 判定原目标 unit 不能继续使用，需要重新申请目标并让 worker 再做一次。

原路径直接调用 `AllocVunitSafe`，由 CM 随机选择新目标盘，会绕过第 6 节的 Target 和 Benefit：
首次 prepare 按均衡策略选了目标，重做时却可能落到一个没有收益的随机目标。修改后统一走 `allocMigrateVunit`：

```text
UpdateVolume 失败且 ShouldAllocAndRedo
  → allocMigrateVunit                       # 与 prepare 同一入口
      ├─ targetDiskAlloc == nil → AllocVunitSafe（auto：仍由 CM 选盘）
      └─ 否则 → SelectTarget → AllocVunitOnDiskSafe → OnAllocResult
  → 更新 Destination，State = Prepared，WorkerRedoCnt++
  → 回到 workQueue
```

---

## 11. 互斥、安全与容错

| 机制 | 防什么 | 位置 |
|---|---|---|
| 卷锁 `VolTaskLocker.TryLock(vid)` | 同一个卷同时被 repair / drop / balance 等多个任务搬 | prepare / Load |
| CM `LockVolume` | 卷在 CM 侧被并发修改 | prepare |
| `IsMigratingDisk` / `DiskConcurrency` | 同一块源盘并发多个任务；迁移整体占用过多 | collect |
| `FilterSourceCandidates` | readonly / 不健康 / 迁移中 / 冷却中的盘当源 | collect |
| 源盘冷却 30min（新增） | 选不出目标时每轮为同一块盘空转 | `OnAllocResult` |
| 提前结束 `finishTaskInAdvance` | 任务已无意义却一直占额度 | prepare |
| `checkAllocNotConflict` | alloc 出的盘与卷内其它 unit 同盘 | `base.AllocVunit*` |
| 租约 + `waitAndLoad` | leader 切换后任务在多个 worker 上重复执行 | worker / 启动 |
| `taskswitch` | 运维动态开关 balance | collect / prepare |
| 任务持久化到 CM KV + `Load` | Scheduler 重启丢任务 | CM KV |

---

## 12. 配置

在 Scheduler 配置的 `balance` 段（`BalanceMgrConfig = strategy.Config + MigrateConfig`）：

```json
"balance": {
  "disk_concurrency": 8,
  "collect_task_interval_s": 10,
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
  "balance_deviation_min": 1
}
```

策略组合见 6.1。字段不填时的默认值：

| 字段 | 默认 | 备注 |
|---|---|---|
| `collect_task_interval_s` | 5 | |
| `check_task_interval_s` | 5 | |
| `disk_concurrency` | 1 | 同时迁出的源盘数 |
| `work_queue_size` | 20 | `workQueue` 里待领 + 执行中任务达到该值，prepare 暂停 |
| `prepare_queue_retry_delay_s` / `finish_queue_retry_delay_s` | 10 / 10 | 失败任务重试间隔 |
| `cancel_punish_duration_s` | 20 | worker 取消任务后的惩罚时间 |
| `balance_trigger_strategy` / `source_select_strategy` / `target_select_strategy` / `benefit_strategy` | `default` / `default` / `auto` / `free_chunk_variance` | 等价 Redundancer 原有行为 |
| `trigger_enter` / `trigger_exit` | 0.20 / 0.15 | 仅 hysteresis 用 |
| `max_disk_free_chunk_cnt` | 1024 | `default` trigger |
| `min_disk_free_chunk_cnt` | 20 | `default` source selector |
| `balance_deviation_ratio` / `balance_deviation_min` | 0.05 / 1 | `most_level_*` 偏差带 |

---

## 13. 可观测性

| 手段 | 内容 |
|---|---|
| 日志 | 选盘 `select target disk` / `no beneficial target disk` / `exclude target disk for next select`；`cool down balance source disk` |
| 任务统计 | Scheduler 统计接口里的 `Balance` 段：是否启用、`preparing / workerDoing / finishing` 数量、每分钟完成数、搬迁数据量 |
| 任务日志 | 每个结束的任务（含提前结束及原因 `FinishAdvanceReason`）写 `task_log` |

---

## 14. 源驱动的局限：目标优先的前提

### 14.1 本文改造之后仍然存在的问题

本文改的是每一步「怎么选」，**没有改 collect 的决定顺序**：仍是先 S、再 `vuid`、最后 T。
S、`vuid`、T 之间没有配对：

```text
S  ──▶  vuid  ──▶  T（prepare 才确定，可能配不上）
```

集群大体均匀、只是缓慢漂移时，这个顺序够用：满盘上的大多数 unit 都能找到合适的偏空盘。
换盘后出现**极空盘**时，问题就暴露出来：

1. **`vuid` 与空盘 T 互不相干。** 空盘所在 host 的其它盘往往已经持有大量卷的 unit，host-aware 下 `Allow(T)`
   会拒掉这些卷；而 collect 选 `vuid` 时不知道 T 是谁，只会选「S 上第一个卷空闲的 unit」。
2. **失败发生在 prepare，代价已经付出。** 任务已经持久化、占了 `DiskConcurrency`、锁了卷，才因
   `ErrNoTargetDisk` 提前结束；源盘还会冷却 30 分钟，这块满盘反而长时间不能再当源。
3. **Trigger 能触发，但不能保证灌入空盘。** 换上空盘后 `R` 突然增大，`free_chunk_hysteresis` 会开启；
   但源驱动仍先选满盘和 `vuid`，并不围绕空盘找兼容的卷。可兼容卷很少时，新空盘可能长期接不到数据。

本文新增的 Target selector 和 Benefit evaluator 只能在**已定的 `vuid`** 上挑目标，
无法反过来为某块空盘挑选 `vuid`；源盘冷却也只能减少空转，不能让空盘接到数据。
要解决这个问题，必须改变 collect 的顺序。

### 14.2 目标优先怎么做

collect 先确定空盘 T，再找能搬进 T 的源盘 S 和 `vuid`，并把 T 作为进程内偏好带到 prepare。
默认仍走源驱动；只有出现极端空头（`T` 统计量超过阈值）时才切换到目标优先，空头消除后再回到源驱动。

### 14.3 目标优先复用本文的哪些机制

| 本文机制 | 在目标优先中的作用 |
|---|---|
| `need` / `DiskConcurrency`（8.1） | 目标相位同样受本轮名额限制 |
| `FilterSourceCandidates`（8.2） | 目标相位的源盘候选沿用同一过滤口径 |
| R / S / T（6.2） | `T`（空头）决定是否进入目标相位；Trigger 仍是总开关 |
| `BuildTargetExcludeRule.Allow`（6.4） | 在 collect 阶段提前筛选与 T 兼容的 `vuid` |
| `SelectTarget` + Benefit（6.4 / 6.5） | prepare 仍执行三道关卡；T 只作为 `PreferDiskID` 排到候选最前，不绕过约束 |
| `OnAllocResult`、乐观更新、冷却（9.1） | 不变 |
| 源驱动 collect（第 8 节） | 退出目标相位后继续使用 |
| `target_select_strategy != auto` | 目标优先要求 Scheduler 选盘，启动校验强制 |

> **小结**：本文让 Scheduler 能指定目标盘并过滤无收益搬迁；
> 目标优先在此基础上改变 collect 顺序，解决换盘场景下极空盘难以收敛的问题。
>
> 下一篇：[目标优先 Balance（换盘灌空盘）](scheduler-target-first-balance.md)。

### 14.4 其它现状

- collect 阶段 `ListDiskVolumeUnits`、`GetVolumeInfo` 每轮每个候选都会请求 CM（代码里已有 `FIXME: cache`），盘多、卷多时成本高。
- 拓扑是快照 + 乐观更新，与 CM 真值可能有偏差，下一次刷新才纠正。
- 只均衡 `FreeChunkCnt`，不区分 chunk 实际数据量；`IBenefitEvaluator` 留了换度量的扩展点。
- 只在同 idc 内均衡。

---

## 15. 关键代码位置

| 模块 | 文件 |
|---|---|
| 启动装配、leader 判定、`Load` 顺序 | `blobstore/scheduler/startup.go` |
| BalanceMgr：collect / SelectTarget / OnAllocResult / 冷却 / 垃圾清理 | `blobstore/scheduler/balancer.go` |
| MigrateMgr：三队列、prepare / finish、`allocMigrateVunit` | `blobstore/scheduler/migrate.go` |
| 集群拓扑、乐观更新、卷缓存 | `blobstore/scheduler/cluster_topology.go` |
| 策略接口、枚举、工厂、Config / Fix / Validate、`TargetExcludeRule` | `blobstore/scheduler/strategy/balance_strategy.go` |
| Trigger | `blobstore/scheduler/strategy/balance_strategy_trigger.go` |
| Source selector | `blobstore/scheduler/strategy/balance_strategy_source.go` |
| Target selector | `blobstore/scheduler/strategy/balance_strategy_target.go` |
| Benefit evaluator | `blobstore/scheduler/strategy/balance_strategy_benefit.go` |
| 统计 / 方差 / 偏差带 / 加权随机 | `blobstore/scheduler/strategy/balancer_stats.go` |
| 任务结构、状态常量 | `blobstore/common/proto/scheduler.go` |
| 任务公共配置与默认值 | `blobstore/scheduler/base/task_types.go` |
| Worker 执行 | `blobstore/blobnode/task_runner_migrate.go`、`blobstore/blobnode/task_runner_mgr.go` |
