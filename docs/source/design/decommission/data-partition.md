# DataPartition 下线迁移

本文说明 DataPartition 级别的 decommission 流程。DataPartition 是 CubeFS decommission 体系中最小的执行单元。

## 职责

在这一层，系统会把一个 DataPartition 的某个 replica 从源 DataNode 或源 Disk 迁走。

DataPartition 层负责：

- 记录源端和目标端信息。
- 选择或校验目标 replica 位置。
- 进入 NodeSet decommission 队列。
- 删除旧 replica 并创建新 replica。
- 更新 Raft member。
- 等待新 replica 完成 repair。
- 持久化 decommission 状态，用于恢复。

## 主要触发来源

DataPartition decommission 可以由多个上层流程触发：

| 触发来源 | 说明 |
| --- | --- |
| 单 DP decommission | Admin 显式下线一个 replica。 |
| Disk decommission | Disk 级任务标记该 Disk 上的所有 DP replica。 |
| DataNode decommission | Node 级任务先拆成 Disk 任务，再拆成 DP 任务。 |
| 坏盘自动 decommission | Master 收到坏盘上报后调度迁移。 |
| 分布优化 | 调度器为了优化 replica 分布而迁移。 |
| 手动或自动 add-replica repair | replica 缺失或不健康时使用。 |

大多数路径最终都会收敛到同一套 DP 标记和执行流程。

## 主流程

```text
触发入口
  -> MarkDecommissionStatus
  -> 加入 DecommissionDataPartitionList
  -> NodeSet traverse 循环
  -> 获取 decommission token
  -> TryToDecommission
  -> Decommission
  -> 删除旧 replica 并新增新 replica
  -> 设置 DecommissionRunning
  -> 等待 DataNode repair
  -> checkDiskRecoveryProgress
  -> DecommissionSuccess 或 DecommissionFail
```

## 标记阶段

标记阶段用于记录 decommission 意图，通常包含：

- Source address。
- Source disk path。
- Destination address，如果指定了目标。
- Decommission type。
- Decommission term。
- Weight。
- Retry 和 rollback 状态。

DP 被标记后，会加入所在 NodeSet 的 `DecommissionDataPartitionList`。实际执行不会在 API handler 内立即完成。

## 队列与 Token 阶段

`DecommissionDataPartitionList` 会周期性遍历待处理的 DP 任务。

该队列负责：

- 按 decommission weight 排序。
- 为已完成任务释放 token。
- 跳过 pause 或 cancel 的任务。
- 获取并发 token。
- 异步启动 `TryToDecommission`。

这个设计用于避免同一个 NodeSet 内同时迁移过多 DataPartition。

## 执行阶段

常规三副本路径可以概括为：

```text
DataPartition.Decommission
  -> 校验当前状态和 replica 健康情况
  -> removeDataReplica(source)
       -> 删除 Raft member
       -> 删除旧 DP replica
  -> addDataReplica(destination)
       -> 新增 Raft member
       -> 创建新 DP replica
  -> 标记 DP 为 DecommissionRunning
  -> 把 DP 加入 repair 进度跟踪
```

对于特殊副本数场景，实际函数顺序可能不同。例如一副本或两副本分区可能使用更保守的分阶段流程：先创建新 replica，等待 repair 完成，再删除旧 replica。

## 控制面与数据面

Master 负责控制面编排：

- 创建或删除 replica。
- 新增或删除 Raft member。
- 持久化 DP decommission 状态。
- 检查 repair 进度。
- 决定 success、failure、retry 或 rollback。

DataNode 负责数据面 repair：

- 重建缺失数据。
- repair extents。
- 同步 replica 内容。
- 通过 heartbeat 上报 `IsRepairing` 和 repair 进度。

这个分离很重要：Master 不直接复制用户数据。

## 完成条件

只有当新 replica 可用并且不再 repairing 时，DP 才会被认为 decommission 成功。

典型完成闭环如下：

```text
DataNode repair 新 replica
  -> heartbeat 上报 PartitionReports
  -> Master 更新 DataPartition replica metrics
  -> checkDiskRecoveryProgress 观察到 repair 完成
  -> DP 状态变为 DecommissionSuccess
  -> NodeSet 队列释放 token
```

失败可能先触发 retry 或 rollback，直到最终被标记为 failed。

## 关键状态

| 状态 | 含义 |
| --- | --- |
| `DecommissionInitial` | 没有正在执行的 decommission。 |
| `markDecommission` | DP 已被标记，等待调度。 |
| `DecommissionPrepare` | DP 获取 token 后进入执行前准备。 |
| `DecommissionRunning` | 控制面变更已完成或已开始，repair 正在进行。 |
| `DecommissionSuccess` | 新 replica 已 repair 完成且可用。 |
| `DecommissionFail` | retry 或 rollback 后仍然失败。 |
| `DecommissionPause` | 任务已暂停。 |
| `DecommissionCancel` | 任务已取消。 |

## 重要源码文件

| 文件 | 用途 |
| --- | --- |
| `master/data_partition.go` | DP 结构、状态字段、任务创建、decommission 状态机。 |
| `master/cluster.go` | replica 增删编排，以及 API 级 DP 迁移辅助逻辑。 |
| `master/topology.go` | NodeSet decommission 队列和 token 控制。 |
| `master/disk_manager.go` | repair 进度检查和 DP 完成标记。 |
| `master/data_partition_check.go` | DP 健康检查。 |
| `datanode/data_partition_repair.go` | DataNode 侧数据 repair。 |

## 设计说明

- DataPartition 是执行、retry、rollback 和 repair 跟踪的基本单元。
- Disk 和 DataNode decommission 最终都会拆成多个 DP decommission 任务。
- 持久化的 DP decommission 状态是 Master 重启或 leader 切换后保证原子性和恢复能力的基础。
