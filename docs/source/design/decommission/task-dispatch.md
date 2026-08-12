# 任务下发与节点执行

本文说明 Master 如何向 DataNode / MetaNode 下发 decommission 相关控制任务，以及节点如何把进度反馈给 Master。

## 控制面任务模型

通用任务封装是 `AdminTask`。它包含操作码、请求、响应、状态以及 retry 相关元数据。

Master 会为 DataNode 和 MetaNode 分别创建 `AdminTask`。每个目标地址对应一个 `AdminTaskManager`，异步任务通过 `addDataNodeTask` / `addMetaNodeTasks` 入队。

## Master 任务管理器

每个 DataNode / MetaNode 在 Master 侧各有一个 `AdminTaskManager`（DataNode 上字段名为 `TaskManager`，MetaNode 上为 `Sender`），维护 `TaskMap` 并在后台 goroutine 中周期性调度。

| 组件 | 源码 | 说明 |
| --- | --- | --- |
| 入队 | `addDataNodeTask` / `addMetaNodeTasks` | 将 `AdminTask` 写入目标节点的 `TaskMap` |
| 后台调度 | `AdminTaskManager.process` | 每 2s 触发一次 `doSendTasks` 和超时清理 |
| 同步发送 | `syncSendAdminTask` | 调用方阻塞等待 TCP 完整响应 |
| 异步发送 | `sendAdminTask` | 由 `process` 调用，仅等待 TCP 收包确认 |

异步任务重试与超时（`proto/admin_task.go`）：

| 参数 | 值 | 含义 |
| --- | --- | --- |
| `ResponseInterval` | 5s | 距上次发送超过该间隔才重试 |
| `MaxSendCount` | 5 | 最大发送次数 |
| `ResponseTimeOut` | 100s | 单次发送后等待业务响应的超时 |
| `MaxTaskNum` | 30 | 每轮 `getToDoTasks` 最多取出的任务数 |

TCP 读超时：

| 场景 | 超时 | 说明 |
| --- | --- | --- |
| 异步 `sendAdminTask` | `ReadDeadlineTime` = 5s | 只等 DataNode/MetaNode 收包并回 `OpOk` ack |
| 同步 `syncSendAdminTask` | `SyncSendTaskDeadlineTime` = 30s | 等业务执行完毕，检查 `ResultCode == OpOk` |

## 两种下发模式

CubeFS 对 Master 到 DataNode / MetaNode 的控制操作使用两种下发模式。判定标准不是 opcode 本身，而是 **Master 调用路径**：走 `syncSendAdminTask` 为同步，走 `AddTask` + 后台 `process` 为异步。

### 同步下发

同步下发用于 **有顺序依赖的拓扑变更**。Master 在单步 RPC 返回后才能继续 decommission 状态机。

调用链：

```text
Cluster / DataPartition / MetaPartition 编排逻辑
  -> 构造 AdminTask
  -> node.TaskManager.syncSendAdminTask(task)   // DataNode
  -> node.Sender.syncSendAdminTask(task)        // MetaNode
  -> TCP 写入 packet，阻塞读取响应（最长 30s）
  -> ResultCode != OpOk 则返回 error
  -> 继续下一步（更新 RocksDB、切换 leader、等待 repair 等）
```

节点侧处理：在 TCP handler 内 **同步执行** 完毕，通过 packet 直接返回结果（如 `OpCreateDataPartition` 在 body 中返回磁盘路径）。

### 异步下发

异步下发用于 **周期性探测、批量校验、与 decommission 主路径解耦** 的操作。Master 不阻塞在单任务上，由 `AdminTaskManager` 统一排队发送。

调用链：

```text
Cluster 构造 AdminTask
  -> addDataNodeTask / addMetaNodeTasks
  -> AdminTaskManager.AddTask（写入 TaskMap）
  -> process 每 2s: getToDoTasks -> sendAdminTask
  -> TCP 收到 ack 后更新 SendTime / SendCount
  -> 节点在 goroutine 中执行业务
  -> 节点 HTTP POST Master /dataNode/response 或 /metaNode/response
  -> handleDataNodeTaskResponse / handleMetaNodeTaskResponse
  -> DelTask，更新分区或节点状态
```

Heartbeat 也属于异步路径：TCP 侧先 `PacketOkReply`，再在 goroutine 里构造完整响应并通过 HTTP callback 上报（与 `OpLoadDataPartition` 类似）。

### 调度优先级

`getToDoTasks` 按以下顺序取任务，且每轮不超过 `MaxTaskNum`（30）：

1. **Heartbeat**（`IsHeartbeatTask`）：`OpDataNodeHeartbeat` / `OpMetaNodeHeartbeat` 等
2. **Urgent**（`IsUrgentTask`）：`OpCreateDataPartition`、`OpCreateMetaPartition`、`OpLoadDataPartition`、`OpUpdateMetaPartition`
3. **普通任务**：其余已入队任务

超时未响应的任务由 `getToBeDeletedTasks` 清理并打 Warn。

## Decommission 相关操作分类

下表列出 decommission 编排中 Master 对各 opcode 的实际使用方式（以当前源码为准）。

### DataNode

| Opcode | 模式 | 典型入口 | 说明 |
| --- | --- | --- | --- |
| `OpAddDataPartitionRaftMember` | 同步 | `addDataPartitionRaftMember` | 向 leader 发 ConfChange，失败可触发 rollback |
| `OpCreateDataPartition` | 同步 | `syncCreateDataPartitionToDataNode` / `createDataReplica` | `addDataReplica` 第二步；body 返回新 replica 磁盘路径 |
| `OpRemoveDataPartitionRaftMember` | 同步 | `removeDataPartitionRaftMember` | 删副本前先摘 Raft member |
| `OpDeleteDataPartition` | 同步 | `deleteDataReplica` | 先 `dp.update` 写 RocksDB，再 sync 删本地 replica |
| `OpDataPartitionTryToLeader` | 同步 | `tryToChangeLeader` | 删 leader 副本前切主 |
| `OpSetRepairingStatus` | 同步 | decommission repair 控制 | 标记/清除 repairing |
| `OpStopDataPartitionRepair` | 同步 | decommission repair 控制 | 停止 peer repair |
| `OpLoadDataPartition` | 异步 | `doLoadDataPartition` → `addDataNodeTasks` | 校验 CRC / replica 一致性；Master 轮询 `checkLoadResponse` |
| `OpDataNodeHeartbeat` | 异步 | `checkDataNodeHeartbeat` | 上报容量、磁盘、replica、repair 进度 |
| `OpVersionOperation` | 异步 | 多版本快照 | 与 decommission 弱相关 |

`addDataReplica` 串行两步均为同步：

```text
addDataPartitionRaftMember (OpAddDataPartitionRaftMember)
  -> createDataReplica / syncCreateDataPartitionToDataNode (OpCreateDataPartition)
```

`deleteDataReplica` 顺序：更新 Master 元数据 → `OpDeleteDataPartition`（sync）。repair 是否完成 **不** 由同步任务判定，而依赖 heartbeat 中的 `DecommissionRepairProgress`。

### MetaNode

| Opcode | 模式 | 典型入口 | 说明 |
| --- | --- | --- | --- |
| `OpAddMetaPartitionRaftMember` | 同步 | `buildAddMetaPartitionRaftMemberTaskAndSyncSend` | `addMetaReplica` 第一步 |
| `OpCreateMetaPartition` | 同步 | `createMetaReplica` / `syncCreateMetaPartitionToMetaNode` | 在目标 MetaNode 创建本地 MP |
| `OpRemoveMetaPartitionRaftMember` | 同步 | `removeMetaPartitionRaftMember` | 删副本前摘 member |
| `OpDeleteMetaPartition` | 同步 | `deleteMetaPartition` | 更新 MP 元数据后 sync 删除 |
| `OpMetaPartitionTryToLeader` | 同步 | `tryToChangeLeader` | 删 leader 前切主 |
| `OpLoadMetaPartition` | 同步（直连） | `doLoadMetaPartition` | 对每个 host 起 goroutine 调 `syncSendAdminTask`，**不经** `AddTask` |
| `OpUpdateMetaPartition` | 异步 | `addUpdateMetaReplicaTask` → `addMetaNodeTasks` | 扩展 inode 范围等，标记为 urgent |
| `OpMetaNodeHeartbeat` | 异步 | `checkMetaNodeHeartbeat` | 上报 MP 列表与 replica 状态 |

`addMetaReplica` / `deleteMetaReplica`（含 `migrateMetaPartition`）与 DP 对称：Raft member 变更 → 创建/删除本地 replica，全程同步等待。

Learner 模式（`migrateMetaPartitionByLearner`）在 `addMetaReplicaLearner` 阶段同样使用同步 `OpAddMetaPartitionRaftMember` + `OpCreateMetaPartition`；恢复进度由 `putBadMetaPartitions` 与定时检查驱动，而非异步任务轮询。

## DataNode 执行

DataNode 通过 TCP 服务接收 packet，并根据 opcode 分发处理。

重要 handler 包括：

| Opcode | 用途 |
| --- | --- |
| `OpCreateDataPartition` | 在 Disk 上创建新的 DP replica。 |
| `OpDeleteDataPartition` | 删除已有 DP replica。 |
| `OpAddDataPartitionRaftMember` | 为 DP 新增 Raft member。 |
| `OpRemoveDataPartitionRaftMember` | 为 DP 删除 Raft member。 |
| `OpDataNodeHeartbeat` | 构造并返回 DataNode 状态。 |
| `OpLoadDataPartition` | 加载 DP 元数据并检查 replica 一致性。 |
| `OpSetRepairingStatus` | 标记 repair 状态。 |
| `OpStopDataPartitionRepair` | 停止或暂停 repair。 |

同步 handler 在 TCP 连接上直接返回 `ResultCode` 与 body；异步 handler 先 `PacketOkReply`，再在 goroutine 中通过 HTTP callback 上报业务结果。

## Heartbeat 作用

Heartbeat 用于：

- 节点容量和健康状态上报。
- Disk 状态与 bad disk 上报。
- DataPartition / MetaPartition replica 上报。
- Repair 状态与 `DecommissionRepairProgress` 上报。
- 接收 Master 下发的 QoS 等配置类指令。

重要的 DataPartition heartbeat 字段包括：

- replica 是否为 leader。
- Applied Raft index。
- Used size。
- Replica 状态。
- `IsRepairing`.
- `DecommissionRepairProgress`.

Master 使用这些上报更新内存中的 replica metrics，并判断新 replica 是否已经完成 repair。

## 数据 Repair 路径

Master 不复制 extent 数据，只调整控制面拓扑。

新 replica 创建后，DataNode 侧 repair 逻辑会同步 replica 数据。DataNode 可能通知 peer replica，并通过 DataNode-to-DataNode 通信执行 extent 级 repair。

简化模型如下：

```text
Master 创建新 replica
  -> DataNode 启动 repair
  -> peer replica 提供缺失数据
  -> DataNode 通过 heartbeat 上报 repair 进度
  -> repair 完成后 Master 标记 DP success
```

## 响应处理

Master 侧有三条响应路径，与同步/异步模式对应。

### 同步任务：TCP 内联响应

```text
syncSendAdminTask
  -> 节点 handler 同步执行业务
  -> packet.ResultCode == OpOk（失败则带错误 body）
  -> 调用方直接根据 error / response.Data 决策
```

适用：Raft member 变更、创建/删除 replica、切主、repair 控制等 decommission 关键步骤。

### 异步任务：TCP ack + HTTP callback

```text
sendAdminTask（仅收到 OpOk ack）
  -> 节点 goroutine 执行业务
  -> ResponseDataNodeTask / ResponseMetaNodeTask
  -> Master HTTP /dataNode/response 或 /metaNode/response
  -> handleDataNodeTaskResponse / handleMetaNodeTaskResponse
  -> TaskManager.DelTask
```

DataNode 异步响应处理的 opcode：

| Opcode | 处理函数 |
| --- | --- |
| `OpDeleteDataPartition` | `dealDeleteDataPartitionResponse`（非 decommission 主路径；decommission 走 sync） |
| `OpLoadDataPartition` | `handleResponseToLoadDataPartition` |
| `OpDataNodeHeartbeat` | `handleDataNodeHeartbeatResp` |
| `OpVersionOperation` | `dealOpDataNodeMultiVerResp` |

MetaNode 异步响应处理的 opcode：

| Opcode | 处理函数 |
| --- | --- |
| `OpMetaNodeHeartbeat` | `dealMetaNodeHeartbeatResp` |
| `OpDeleteMetaPartition` | `dealDeleteMetaPartitionResp` |
| `OpUpdateMetaPartition` | `dealUpdateMetaPartitionResp` |
| `OpVersionOperation` | `dealOpMetaNodeMultiVerResp` |

### Heartbeat：状态驱动 decommission 进度

```text
checkDataNodeHeartbeat / checkMetaNodeHeartbeat
  -> addDataNodeTasks / addMetaNodeTasks（heartbeat 任务）
  -> 节点先 TCP ack，再 HTTP 上报完整 heartbeat 响应
  -> 更新节点容量、磁盘、replica 列表
  -> 更新 DecommissionRepairProgress / MP recover 状态
  -> decommission scheduler 据此判断 repair 是否完成
```

Heartbeat **不是** 任务拉取机制：节点不会通过 heartbeat 向 Master 索取待执行的 decommission 任务。

## 重要源码文件

| 文件 | 用途 |
| --- | --- |
| `proto/admin_task.go` | Admin task 结构和任务状态。 |
| `proto/packet.go` | Master 和 DataNode 使用的 packet opcode。 |
| `proto/admin_proto.go` | Heartbeat 和 admin API 协议结构。 |
| `master/admin_task_manager.go` | Task 队列和同步/异步发送逻辑。 |
| `master/cluster_task.go` | Task 入队和响应处理。 |
| `master/cluster.go` | Heartbeat scheduler 和 decommission 编排。 |
| `master/data_partition.go` / `master/meta_partition.go` | DP/MP 任务构造与 sync 调用。 |
| `datanode/wrap_operator.go` | DataNode packet 分发和 task handler。 |
| `metanode/manager_op.go` | MetaNode 对应 admin task handler。 |
| `datanode/space_manager.go` | Heartbeat 响应构造。 |
| `datanode/data_partition_repair.go` | DataNode 侧 replica repair。 |

## 设计说明

- Decommission 控制面任务由 Master 驱动；拓扑变更步骤以 **同步任务** 保证顺序，**异步任务** 负责探测与校验。
- Heartbeat 上报状态与 repair 进度，不携带待执行任务列表。
- 数据 repair 由 DataNode 执行；Master 通过 heartbeat 判断 repair 是否完成，而非同步等待 repair 结束。
- MetaNode MP 迁移与 DataNode DP 迁移在任务模式上对称：Raft + 创建/删除 replica 同步，恢复进度异步观测。
