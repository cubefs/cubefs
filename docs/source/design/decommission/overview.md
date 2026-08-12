# Decommission 调度总览

本文说明 CubeFS 中 DataPartition、Disk、DataNode 下线迁移相关的调度设计。文档关注架构层面的流程、对象职责和主要状态流转，不展开逐行代码实现。

## 覆盖范围

Decommission 体系主要覆盖以下场景：

- 下线单个 DataPartition 副本。
- 下线 DataNode 上的一块 Disk。
- 下线或迁移整个 DataNode。
- 自动迁移坏盘上的副本。
- 新副本创建后跟踪 repair 进度。

相关模块文档：

- [DataPartition 下线迁移](./data-partition.md)
- [DP Repair 与 Raft Snapshot 并发一致性](./dp-repair-raft-snapshot-consistency.md)
- [Disk 下线迁移](./disk.md)
- [DataNode 下线迁移](./datanode.md)
- [任务下发与 DataNode 执行](./task-dispatch.md)
- [状态模型与恢复](./state-model.md)

## 高层模型

Decommission 设计是一个分层调度系统：

```text
DataNode 下线
  -> 拆分为多个 Disk 下线任务
    -> 找到每块 Disk 上的 DataPartition
      -> 调度 DataPartition 下线任务
        -> 向 DataNode 下发控制任务
          -> DataNode 本地 repair 数据
            -> Master 通过 heartbeat 观察进度
              -> 聚合 DP、Disk、DataNode 状态
```

真正的执行单元是 `DataPartition`。`Disk` 和 `DataNode` 是更高层的编排和状态聚合对象。

## 核心设计原则

### API 只标记意图

Admin API 通常不会同步完成迁移。API 只负责把目标对象标记为 decommissioning，持久化状态，然后交给后台调度器持续推进。

典型入口：

- `/dataPartition/decommission`
- `/disk/decommission`
- `/dataNode/decommission`
- `/dataNode/migrate`

### Master 驱动控制面调度

Master 持有调度状态，并通过周期任务推进状态机。重要调度任务包括：

- `scheduleToCheckDataPartitions`
- `scheduleToCheckDecommissionDisk`
- `scheduleToCheckDecommissionDataNode`
- `scheduleToCheckDiskRecoveryProgress`
- `scheduleToBadDisk`
- `scheduleToCheckHeartbeat`

这些调度任务在 Master 启动时注册，并且只在当前 Raft leader 上执行。

### NodeSet 控制并发

NodeSet 是 decommission 并发控制的主要边界。它维护 Disk 级和 DataPartition 级队列，按 weight 排序，并通过 token 限制并发迁移数量。

关键队列包括：

- `DecommissionDiskList`
- `DecommissionDataPartitionList`

### DataNode 执行控制任务并 repair 数据

Master 下发控制任务，例如创建新副本、删除旧副本、调整 Raft member。DataNode 执行这些任务，并在本地或通过 DataNode-to-DataNode repair 消息完成实际数据 repair。

Master 不直接搬运 extent 数据，而是通过 heartbeat 上报观察 repair 进度。

## 核心对象

| 对象 | 职责 |
| --- | --- |
| `DataPartition` | 最小 decommission 执行单元，记录源副本、目标位置、disk path、重试状态和 repair 状态。 |
| `DecommissionDisk` | Disk 级任务，找到一块 Disk 上的所有 DP 副本并聚合状态。 |
| `DataNode` | Node 级编排对象，把节点下线拆成 Disk 任务并聚合 Disk 状态。 |
| `AdminTask` | Master 到 DataNode 的控制任务封装。 |
| `NodeSet` | 队列和并发控制边界。 |

## 端到端流程

```text
Admin 请求
  -> Master 标记 decommission 状态
  -> Master 通过 Raft 持久化元数据
  -> 后台调度器捞取任务
  -> NodeSet 队列获取并发 token
  -> DataPartition decommission 开始执行
  -> Master 向 DataNode 同步下发控制任务
  -> DataNode 创建/删除 replica 和 Raft member
  -> DataNode repair replica 数据
  -> heartbeat 上报 IsRepairing / repair 进度
  -> Master 标记 DP 成功或失败
  -> Disk 聚合 DP 结果
  -> DataNode 聚合 Disk 结果
```

## 建议阅读顺序

从架构视角了解时，建议按以下顺序阅读：

1. 本总览。
2. [任务下发与 DataNode 执行](./task-dispatch.md)
3. [DataPartition 下线迁移](./data-partition.md)
4. [Disk 下线迁移](./disk.md)
5. [DataNode 下线迁移](./datanode.md)
6. [状态模型与恢复](./state-model.md)

## 关键源码文件

| 模块 | 文件 |
| --- | --- |
| Master 调度 | `master/server.go`, `master/cluster.go` |
| DP decommission | `master/data_partition.go`, `master/data_partition_check.go`, `master/data_partition_map.go` |
| Disk decommission | `master/disk_manager.go`, `master/data_node.go` |
| DataNode decommission | `master/cluster.go`, `master/data_node.go` |
| NodeSet 队列 | `master/topology.go` |
| 任务下发 | `master/admin_task_manager.go`, `master/cluster_task.go`, `proto/admin_task.go`, `proto/packet.go` |
| DataNode 执行 | `datanode/wrap_operator.go`, `datanode/space_manager.go`, `datanode/disk.go` |

## 架构结论

CubeFS decommission 调度是一个可持久化、分层、后台持续推进的任务系统。DataPartition 是执行单元，Disk 和 DataNode 是聚合层，Master 持有控制面状态，DataNode 负责数据 repair。
