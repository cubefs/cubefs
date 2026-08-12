# Disk 下线迁移

本文说明 CubeFS 中 Disk 级 decommission 流程。一个 Disk decommission 任务负责把 DataNode 某块 Disk 上选中的 DataPartition replica 迁走。

## 职责

Disk 层是编排和聚合层，不直接迁移数据。它会找到一块 Disk 上的 DataPartition，逐个标记为 decommission，并聚合这些 DP 的结果。

Disk 层负责：

- 表示一个 `DataNode address + disk path` decommission 任务。
- 在需要时禁止源 Disk 继续承载新分配。
- 找到位于该 Disk 上的所有 DP replica。
- 创建 DP decommission 任务。
- 将 DP 进度聚合为 Disk 级状态。
- 处理手动、自动、pause、cancel、recommission 等流程。

## 主要触发来源

| 触发来源 | 说明 |
| --- | --- |
| 手动 Disk decommission | Admin 调用 `/disk/decommission`。 |
| 坏盘自动迁移 | DataNode 在 heartbeat 中上报坏盘信息。 |
| DataNode decommission | Node 级流程把节点拆成多个 Disk 任务。 |
| 未完成任务重试 | 已存在的 decommission disk 记录会被再次捞取。 |

## 手动 Disk Decommission 流程

```text
/disk/decommission
  -> Server.decommissionDisk
  -> Cluster.migrateDisk
  -> 创建或复用 DecommissionDisk
  -> markDecommission
  -> 持久化 DecommissionDisk
  -> 加入 NodeSet DecommissionDiskList
  -> traverseDecommissionDisk
  -> TryDecommissionDisk
  -> 找到该 Disk 上的所有 DP replica
  -> 对每个 DP 调用 MarkDecommissionStatus
  -> 进入 DP decommission 流程
```

API 只标记意图并持久化状态。实际任务展开由后台调度器完成。

## 坏盘自动 Decommission 流程

DataNode 在本地检测 disk error，并通过 heartbeat 上报给 Master。

```text
DataPartition 读写或 Raft IO error
  -> DataNode 记录 disk error
  -> 达到阈值后 disk status 变为 unavailable
  -> heartbeat 上报 BadDisks / BadDiskStats
  -> Master 的 checkBadDisk 调度器处理
  -> handleDataNodeBadDisk
  -> 选择整盘迁移或只迁移 bad DP
  -> migrateDisk 或直接标记选中的 DP
```

系统因此有两种策略：

- 当 Disk 被认为已损坏时，迁移整块 Disk。
- 当影响范围较小时，只迁移出现 IO error 的 DataPartition。

## DataPartition 选择

Disk decommission 通过扫描 volume 并匹配 replica 位置来选择 DataPartition：

```text
replica.Addr == 源 DataNode address
replica.DiskPath == 源 disk path
```

调度器也会合并同一 Disk 上之前未完成的 DP decommission 任务，用于支持 Master 重启、leader 切换或部分失败后的恢复。

最终选中的 DP 集合可能会受请求参数或集群级 decommission limit 限制。

## 队列与并发

Disk 任务存放在 NodeSet 级 Disk 队列中。

Disk 调度器负责：

- 更新 running 状态 Disk 的进度。
- 从手动或自动队列中取出已 mark 的 Disk 任务。
- 按 weight 排序。
- 应用 Disk 级并发限制。
- 调用 `TryDecommissionDisk` 将 Disk 任务展开成 DP 任务。

这一层控制同一个 NodeSet 内可以并发 decommission 的 Disk 数量。

## Disk 状态聚合

Disk 层 decommission 状态由同一 `DecommissionTerm` 下的 DP 任务聚合得出，不由独立状态机直接驱动。聚合规则与完整状态流转见下文「Master 逻辑 Decommission 状态」和「与 DP 状态聚合的关系」。

## 状态分层模型

Disk 状态在系统中分两层维护，职责不同、同步方式也不同：

| 层级 | 持有方 | 主要对象 | 职责 |
| --- | --- | --- | --- |
| 物理健康状态 | DataNode | `Disk.Status`、IO 错误计数、`.diskStatus` 探测 | 检测本地磁盘是否可用、是否应拒绝 IO |
| 逻辑下线状态 | Master | `DecommissionDisk`、`DecommissionedDisks`、`DecommissionSuccessDisks` | 编排迁移任务、禁止新分配、聚合进度 |

两层通过 heartbeat 双向同步：Master 下发 `DecommissionDisks` 列表，DataNode 上报 `BadDisks` / `BadDiskStats`。

## DataNode 物理 Disk 状态

### 状态枚举

物理 Disk 状态定义在 `proto/status.go`：

| 状态 | 含义 |
| --- | --- |
| `ReadWrite` | 磁盘正常，可读写、可承载新 DP |
| `ReadOnly` | 可用空间不足（`Available <= 0`），仍可读写已有数据，但不适合新分配 |
| `Unavailable` | 磁盘不可用，拒绝写入；DP 上的 replica 会被标记为 unavailable |
| `Recovering` | 坏盘恢复过程中 |

### 状态流转

```text
启动 / 定时 updateSpaceInfo
  -> Available > 0 且未触发 IO 错误阈值
     -> ReadWrite

  -> Available <= 0
     -> ReadOnly

DP 读写 / Raft IO / Statfs / .diskStatus 探测出错
  -> triggerDiskError
  -> ReadErrCnt / WriteErrCnt 递增
  -> 记录 DiskErrPartitionSet
  -> 错误计数或出错 DP 数达到阈值
     -> doDiskError
     -> Unavailable
```

关键阈值由 DataNode 配置控制：

- `diskUnavailableErrorCount`：磁盘级 IO 错误总次数阈值。
- `diskUnavailablePartitionErrorCount`：出错 DP 数量阈值。

任一达到阈值即将 Disk 置为 `Unavailable`，此后不再参与正常 IO 调度。

### 健康探测

DataNode 对每块 Disk 维护两类本地探测：

1. **`.diskStatus` 文件探测**（每 2 分钟）：对磁盘根目录下的 `.diskStatus` 文件执行 write/sync/read，验证底层存储是否可正常读写。
2. **业务 IO 错误采集**：DP 读写、Raft 日志 IO、`Statfs` 等路径调用 `CheckDiskError`，累计错误并关联出错 DP。

### Decommission 本地标记

DataNode 在 Disk 目录下维护 `decommissionDiskMark` 标记文件：

```text
Master heartbeat 携带 DecommissionDisks 列表
  -> DataNode.checkDecommissionDisks
  -> 列表中有、本地未标记 -> MarkDecommissionStatus(true) -> 创建 decommissionDiskMark
  -> 列表中无、本地已标记 -> MarkDecommissionStatus(false) -> 删除 decommissionDiskMark
```

DataNode 重启时通过 `initDecommissionStatus` 读取该文件，恢复本地 decommission 标记，不依赖 Master 即时下发。

### 坏盘上报

heartbeat 响应中携带坏盘信息：

```text
buildHeartBeatResponse
  -> 遍历所有 Disk
  -> Status == Unavailable 或 DiskErrPartitionSet 非空
     -> 加入 BadDisks / BadDiskStats
  -> isLost == true
     -> 加入 LostDisks
  -> 所有 Disk 汇总为 DiskStats
```

`BadDiskStat` 包含 disk path、出错 DP 列表、首次上报时间等，供 Master 的 `checkBadDisk` 调度器决策整盘迁移或单 DP 迁移。

## Master 逻辑 Decommission 状态

### DecommissionDisk 任务对象

每块待下线 Disk 对应一个 `DecommissionDisk`，key 为 `{SrcAddr}_{DiskPath}`，主要字段：

| 字段 | 含义 |
| --- | --- |
| `DecommissionStatus` | 任务状态（见下表） |
| `DecommissionTerm` | 本次 decommission 轮次，用于关联 DP 任务 |
| `DecommissionDpTotal` | 计划迁移的 DP 总数 |
| `DiskDisable` | 是否禁止该 Disk 继续承载新分配 |
| `Type` | 触发类型（Manual / Auto 等） |
| `DecommissionWeight` | 调度优先级 |
| `IgnoreDecommissionDps` | 不计入进度的 DP（如坏盘 DP 需优先处理） |

### DecommissionDisk 状态流转

Decommission 状态枚举与 DP 层共用（`master/data_partition.go`）：

| 状态 | 含义 |
| --- | --- |
| `Initial` | 无活跃任务，或部分 decommission 完成后重置 |
| `Marked` | 意图已记录，等待 NodeSet 调度器展开 |
| `Running` | 已对该 Disk 上的 DP 发起 decommission |
| `Success` | 该 term 下所有相关 DP 迁移完成 |
| `Fail` | 失败 DP 比例达到阈值或出现不可恢复错误 |
| `Pause` | 已暂停，不再推进新 DP |
| `Cancel` | 已取消，进入 DP 清理逻辑 |

```text
migrateDisk
  -> markDecommission
  -> Marked
  -> 持久化 + 加入 NodeSet 队列

traverseDecommissionDisk (NodeSet 调度器)
  -> TryDecommissionDisk
  -> 扫描 Disk 上 DP -> MarkDecommissionStatus
  -> Running

updateDecommissionStatus (定时聚合)
  -> 所有相关 DP Success -> Success
  -> 失败比例达阈值 -> Fail
  -> pause / cancel API -> Pause / Cancel
```

`TryDecommissionDisk` 通过 `beginDecommissionAttempt` / `commitDecommissionAttempt` 保证同一 Disk 任务不会被并发重复展开；若执行过程中收到 Pause 或 Cancel，未提交的展开会被丢弃。

### 分配排除列表

除 `DecommissionDisk` 任务外，Master 在 `DataNode` 上还维护两个持久化 Disk 集合：

| 集合 | 写入时机 | 作用 |
| --- | --- | --- |
| `DecommissionedDisks` | `migrateDisk` 且 `DiskDisable=true` 时立即写入；或 `TryDecommissionDisk` 成功后写入 | 禁止新 DP 分配到该 Disk |
| `DecommissionSuccessDisks` | `checkDecommissionDisk` 检测到任务 Success 且 Disk 上无剩余 DP 时写入 | 标记该 Disk 已完成 decommission，防止重复自动迁移 |

分配路径通过 `DataNode.checkDecommissionedDisks` / `availableDiskCount` 排除这些 Disk。heartbeat 请求会把 `DecommissionedDisks` 下发给 DataNode，驱动本地 `decommissionDiskMark` 同步。

### 状态清理与 recommission

```text
checkDecommissionDisk (每 10s)
  -> Success 且 Disk 上无剩余 DP
     -> addAndSyncDecommissionSuccessDisk
     -> 120 小时后删除 DecommissionDisk 记录

部分 decommission (DecommissionLimit != 0) 且 Disk 上仍有 DP
  -> Success/Fail 后
     -> deleteAndSyncDecommissionedDisk (恢复可分配)
     -> Success 时 DecommissionDisk 重置为 Initial

recommissionDisk API
  -> 删除 DecommissionDisk 记录 (若非 Marked/Running)
  -> deleteAndSyncDecommissionSuccessDisk
  -> deleteAndSyncDecommissionedDisk (坏盘不可 recommission)
  -> Disk 重新参与分配
```

## Master 与 DataNode 状态同步

```text
Master                                    DataNode
  |                                          |
  |-- heartbeat request ------------------>  |
  |   DecommissionDisks[]                  |
  |                                          |-> checkDecommissionDisks
  |                                          |-> 更新 decommissionDiskMark
  |                                          |
  |<-- heartbeat response ------------------  |
  |   BadDisks[], BadDiskStats[]           |
  |   DiskStats[], AllDisks[]              |
  |                                          |
  |-> checkBadDisk / handleDataNodeBadDisk   |
  |-> migrateDisk (Auto)                     |
  |-> addAndSyncDecommissionedDisk           |
  |                                          |
  |-- 下一轮 heartbeat ------------------>  |
  |   DecommissionDisks[] (含新禁用 Disk)   |
```

这一闭环保证：Master 侧禁止分配决策能传递到 DataNode 本地标记；DataNode 侧物理故障能反馈到 Master 触发自动迁移。

## Pause / Cancel / Recommission 控制流

| API | 作用 | 状态影响 |
| --- | --- | --- |
| `/disk/pauseDecommission` | 暂停 Disk decommission | Disk -> Pause；关联 DP 调用 `PauseDecommission`；队列中 DP 出队 |
| `/disk/cancelDecommission` | 取消 Disk decommission | Disk -> Cancel；关联 DP 进入 cancel worker 清理 |
| `/disk/recommission` | 恢复已禁用 Disk 的分配能力 | 清除 Decommissioned/Success 记录；删除 DecommissionDisk |
| `/disk/restoreStoppedAutoDecommission` | 恢复被暂停的自动坏盘迁移 | Pause -> Marked，重新进入调度队列 |

Pause 与 Cancel 的区别：Pause 保留任务上下文，可通过重新 mark 或 restore API 继续；Cancel 主动清理 DP 侧 decommission 状态，任务进入终态。

## 与 DP 状态聚合的关系

Disk 层 `DecommissionStatus` 不由独立状态机直接驱动，而是由同一 `DecommissionTerm` 下的 DP 任务聚合得出（详见 [状态模型与恢复](./state-model.md)）。调度器 `traverseDecommissionDisk` 每轮先调用 `updateDecommissionStatus` 刷新进度，再根据 Running 数量与并发限制决定是否取出新的 Marked 任务执行。

## 与 DataNode Decommission 的关系

DataNode decommission 复用 Disk decommission。Node 级任务会枚举节点上的 Disk，并对每块 Disk 调用 Disk 迁移流程。

```text
DataNode decommission
  -> 枚举 disks
  -> 对每块选中的 Disk 调用 migrateDisk
  -> Disk decommission
  -> DP decommission
```

这种复用让手动换盘、坏盘恢复、整节点下线都保持一致的执行路径。

## 重要源码文件

| 文件 | 用途 |
| --- | --- |
| `master/disk_manager.go` | DecommissionDisk 状态、进度聚合、repair 进度检查。 |
| `master/cluster.go` | `migrateDisk`、`TryDecommissionDisk`、坏盘处理。 |
| `master/topology.go` | NodeSet Disk 队列和并发控制。 |
| `master/data_node.go` | DataNode disk 元数据和 bad partition 发现辅助逻辑。 |
| `master/data_partition_map.go` | 按 disk path 扫描 DP replica。 |
| `datanode/disk.go` | 物理 Disk 状态、IO error 检测、`.diskStatus` 探测、`decommissionDiskMark`。 |
| `datanode/wrap_operator.go` | heartbeat 处理、`checkDecommissionDisks`、坏盘恢复。 |
| `datanode/space_manager.go` | heartbeat 响应构造、`BadDisks` / `DiskStats` 上报。 |
| `proto/status.go` | Disk 物理状态枚举。 |

## 设计说明

- Disk decommission 是从一个 Disk 任务展开为多个 DP 任务的中间层。
- 坏盘自动 decommission 和手动 Disk decommission 最终收敛到同一条迁移路径。
- Disk 状态不是独立得出的，而是 DP 任务结果的聚合。
