# 状态模型与恢复

本文说明 DataPartition、Disk、DataNode decommission 流程中的状态模型、进度聚合和恢复行为。

## 分层状态模型

CubeFS decommission 有三层主要状态：

```text
DataPartition 状态
  -> 聚合为 Disk 状态
    -> 聚合为 DataNode 状态
```

每一层承担不同职责：

| 层级 | 职责 |
| --- | --- |
| DataPartition | 执行单个 replica 迁移，并跟踪 repair 结果。 |
| Disk | 跟踪某个源 Disk 上所有 DP 的迁移。 |
| DataNode | 跟踪某个源 Node 上所有 Disk 的迁移。 |

## 通用 Decommission 状态

精确枚举值以代码定义为准，架构层面可以理解为：

| 状态 | 含义 |
| --- | --- |
| Initial | 没有活跃的 decommission 任务。 |
| Mark | 意图已记录，等待调度。 |
| Prepare | 任务已获取执行资源，正在校验前置条件。 |
| Running | 控制面变更已开始，或 repair 正在进行。 |
| Success | 任务成功完成。 |
| Fail | retry 或 rollback 后任务仍然失败。 |
| Pause | 任务已暂停，恢复前不应继续推进。 |
| Cancel | 任务已取消，需要进入清理逻辑。 |

## DataPartition 状态流转

```text
Initial
  -> Mark
  -> Prepare
  -> Running
  -> Success

Running
  -> Fail
  -> retry or rollback
  -> Mark / Prepare / Running
```

最重要的流转是从 `Running` 到 `Success`。这不只是表示 Master 成功发送了 create/delete 任务，而是表示新 replica 已可用，并且 repair 已完成。

## Disk 状态聚合

Disk 状态由属于该 Disk 任务且处于同一 decommission term 的 DataPartition 计算得出。

```text
所有 DP 任务成功
  -> Disk Success

部分 DP 任务仍在运行
  -> Disk Running

出现不可恢复的 DP 失败
  -> Disk Fail
```

Disk 层也可能分别维护手动队列和自动队列。这样调度器可以区分用户主动触发的 Disk decommission 和自动坏盘处理，同时仍然复用同一套 DP 迁移路径。

## DataNode 状态聚合

DataNode 状态由它的 Disk decommission list 计算得出。

```text
所有 Disk 任务成功
  -> DataNode Success

任意 Disk 任务失败且无法恢复
  -> DataNode Fail

收到 pause 或 cancel 请求
  -> DataNode Pause / Cancel
```

Node 级成功后，Master 会检查源 DataNode 上是否仍存在 DataPartition replica。只有节点已经清空后，Master 才能删除 DataNode 元数据。

## 持久化与恢复

Decommission 任务可能运行很久。Master 重启或 leader 切换不能让集群进入未知状态。

当前设计会持久化关键 decommission 字段，包括：

- Decommission 状态。
- Source address。
- Destination address。
- Source disk path。
- Decommission term。
- Retry count。
- Rollback need 和 rollback count。
- Error message。
- Decommission type。

这些字段通过 Master 元数据更新持久化后，新 leader 可以重新加载之前的状态，并继续推进或清理流程。

## Retry 与 Rollback

Decommission 流程涉及多个分布式组件，因此需要 retry 和 rollback。

典型可 retry 条件包括：

- 临时网络错误。
- 短暂缺失 leader。
- 临时 replica 不健康。
- DataNode 响应超时。

典型 rollback 条件包括：

- 无法在选定目标上创建新 replica。
- 目标 Disk 变为 unavailable。
- Repair 进度长时间不推进。
- 流程进入需要重新选择目标的状态。

Retry 和 rollback 次数都有上限。达到上限后，任务会被标记为 failed，需要运维处理或由更高层调度器再次处理。

## Repair 进度跟踪

Repair 进度通过 DataNode heartbeat 间接跟踪。

```text
DataNode repair replica
  -> heartbeat 携带 PartitionReports
  -> Master 更新 replica metrics
  -> scheduler 检查 repair 进度
  -> DP Success or Fail
```

重要信号包括：

- `IsRepairing`
- `DecommissionRepairProgress`
- replica 可用性
- repair 超时

这意味着控制面任务成功并不等于 decommission 成功。状态机需要 heartbeat 反馈来判断最终成功。

## Pause 与 Cancel

Pause 和 cancel 是长任务控制能力。

- Pause 停止继续调度，但保留足够状态用于后续恢复。
- Cancel 停止当前任务，并交给 cleanup 或 recommission 逻辑处理剩余状态。

由于 decommission 是分层的，pause 或 cancel 可能需要反映到不同层级：

- DataPartition queue。
- Disk queue。
- DataNode aggregate state。

## 设计影响

当前状态模型有几个重要架构影响：

- 支持长时间运行且重启安全的迁移。
- 以 DataPartition 作为基本单元，让执行粒度保持较小。
- Disk 和 DataNode 进度可以通过聚合关系解释。
- 任务状态分散在多个对象中，也会让全局可观测性变得更复杂。

未来改进可以聚焦在暴露统一任务视图，而不一定要替换底层分层执行模型。

## 重要源码文件

| 文件 | 用途 |
| --- | --- |
| `master/data_partition.go` | DP decommission 字段、状态、retry 和 rollback 状态。 |
| `master/disk_manager.go` | Disk 级状态聚合和 repair 进度检查。 |
| `master/data_node.go` | DataNode 级状态聚合和 cleanup。 |
| `master/topology.go` | 队列遍历、token 释放和任务 cleanup。 |
| `master/metadata_fsm_op.go` | 通过 Master FSM 操作持久化元数据。 |
| `datanode/space_manager.go` | repair 进度跟踪使用的 heartbeat 上报。 |

## 架构结论

Decommission 状态机不是一个扁平任务，而是一组分层持久化状态。DataPartition 负责执行，Disk 负责 Disk 范围进度，DataNode 负责 Node 范围完成度。
