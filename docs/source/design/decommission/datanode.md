# DataNode 下线迁移

本文说明 CubeFS 中 DataNode 级别的 decommission 和 migration 流程。

## 职责

DataNode decommission 是常规 replica 下线路径中最高层的编排流程。它不会直接迁移每一个 DataPartition，而是先把节点拆成 Disk 级 decommission 任务，并复用 Disk 和 DataPartition 流程。

DataNode 层负责：

- 将节点标记为 decommissioning。
- 阻止或限制新分配落到该节点。
- 枚举节点上的 Disk。
- 创建 Disk decommission 任务。
- 将 Disk 结果聚合为 Node 级状态。
- 所有 replica 迁走后删除 DataNode 元数据。

## 入口

| 入口 | 说明 |
| --- | --- |
| `/dataNode/decommission` | 下线一个 DataNode。 |
| `/dataNode/migrate` | 在参数支持时，把一个 DataNode 迁移到指定目标 DataNode。 |
| pause / cancel / reset / query APIs | 管理长时间运行的 DataNode decommission 状态。 |
| CLI 和 SDK 封装 | 调用同一组 Master API。 |

## 高层流程

```text
/dataNode/decommission
  -> Server.decommissionDataNode
  -> Cluster.migrateDataNode
  -> DataNode.markDecommission
  -> 持久化 DataNode 状态
  -> checkDecommissionDataNode 调度器
  -> TryDecommissionDataNode
  -> 枚举源 DataNode 上的 Disk
  -> 对每块 Disk 调用 migrateDisk
  -> Disk decommission 流程
  -> DP decommission 流程
  -> 聚合 Disk 状态
  -> DataNode DecommissionSuccess or DecommissionFail
  -> 如果没有剩余 DP，则删除 DataNode 元数据
```

API 只启动状态机，后台调度器负责持续推进。

## Node 状态

DataNode 对象同时记录调度状态和运行控制标记。

重要字段包括：

| 字段 | 含义 |
| --- | --- |
| `DecommissionStatus` | Node 级 decommission 状态。 |
| `ToBeOffline` | 表示该节点不应被正常分配选中。 |
| `RdOnly` | 全量 decommission 期间用于更强保护。 |
| `DecommissionDstAddr` | 可选的迁移目标。 |
| `DecommissionLimit` | 限制一轮 decommission 处理多少 DP。 |
| `DecommissionWeight` | 调度优先级。 |
| `DecommissionDiskList` | 为该节点创建的 Disk 任务。 |
| `DecommissionDpTotal` | 当前操作计划处理的 DP 总数。 |
| `DecommissionedDisks` | 已禁止分配的 Disk。 |
| `DecommissionSuccessDisks` | 已完成 decommission 的 Disk。 |

## Disk 枚举

`TryDecommissionDataNode` 通过扫描 DataPartition replica 元数据枚举 Disk。当前设计采用 Disk 级分组，而不是直接迁移节点上的所有 DataPartition。

概念上可以理解为：

```text
遍历每个 volume:
  遍历每个 DataPartition:
    遍历每个 replica:
      if replica.Addr == source DataNode:
        按 replica.DiskPath 分组
```

随后每个 Disk 分组都会变成一个 Disk decommission 任务。

## 目标选择

未指定固定目标时，DP decommission 流程会根据 NodeSet、zone、已有 replica 分布以及 decommission 约束选择可用目标。

指定固定目标时，调度器必须避免非法布局，例如把同一个 DataPartition 的两个 replica 放到同一个 DataNode 上。

## 部分 Decommission 与全量 Decommission

`DecommissionLimit` 会改变操作语义：

- 零值或全量 limit 通常表示整节点 decommission。
- 非零 limit 可以表示部分推进，即一轮只迁移部分 DP。

全量 decommission 时，节点会更严格地避免新分配。部分 decommission 时，受限任务完成后节点可能重新变为可用。

## 完成规则

DataNode 是否完成取决于 Disk 级状态聚合。

```text
所有 Disk 任务成功
  -> DataNode DecommissionSuccess

任意 Disk 任务失败且无法恢复
  -> DataNode DecommissionFail

任务被 pause 或 cancel
  -> DataNode DecommissionPause / DecommissionCancel
```

进入 `DecommissionSuccess` 后，Master 会检查是否仍有 DataPartition replica 位于源 DataNode。如果没有剩余 replica，并且没有关联的 failed decommission 任务，Master 可以删除该 DataNode 元数据。

## 与 Disk 和 DP 的关系

DataNode 层负责整体编排，不负责实际迁移执行：

```text
DataNode 层
  -> 创建 Disk 任务

Disk 层
  -> 将 Disk 任务展开为 DP 任务

DataPartition 层
  -> 执行 replica 迁移
```

这个分层模型对未来设计调整很重要。提升 DataNode decommission 的可观测性或用户体验，不一定需要重写底层 DP 迁移算法。

## 重要源码文件

| 文件 | 用途 |
| --- | --- |
| `master/api_service.go` | Admin API handler。 |
| `master/cluster.go` | `migrateDataNode`、`checkDecommissionDataNode`、`TryDecommissionDataNode`。 |
| `master/data_node.go` | DataNode 状态字段和 Node 级聚合。 |
| `master/disk_manager.go` | Node 聚合使用的 Disk 进度。 |
| `master/topology.go` | Disk 和 DP 任务使用的 NodeSet 队列。 |
| `proto/admin_proto.go` | Admin API path 和请求定义。 |

## 设计说明

- DataNode decommission 是建立在 Disk decommission 之上的组合流程。
- Node 级成功表示所有 Disk 级流程都已完成，并且节点上没有剩余 DP replica。
- 当前设计更强调流程复用和恢复能力，而不是单体式的 Node 迁移事务。
