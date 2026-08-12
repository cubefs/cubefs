# DP Repair 与 Raft Snapshot 并发一致性

本文说明 DataPartition repair 过程中与 raft snapshot、raft random write 并发时可能产生的数据不一致问题，以及对应修复方案。

## 背景

DataPartition decommission 或 add-replica repair 时，新 replica 会先在 DataNode 本地执行数据修复。修复完成后，才启动该 replica 的 raft 实例并加入正常复制流程。

这个阶段同时存在两条写入路径：

- repair 写：新 replica 从现有 replica 拉取 extent 数据，通过 `streamRepairExtent` 写入本地 extent。
- raft random write：已有 raft group 继续处理客户端随机覆盖写，通过 raft log apply 到各 replica。

如果两条路径没有共享同一个一致性边界，就可能出现新 replica 的 raft 状态已经追上，但本地 extent 数据缺失或被旧 repair 数据覆盖的问题。

## 问题一：Raft Snapshot 跳过未回放日志

DataPartition 的 snapshot 与普通状态机不同。DataPartition 数据本身已经落在 extent 文件中，因此 `ApplySnapshot` 并不会恢复完整数据内容。

旧逻辑中，`Snapshot()` 使用当前 `AppliedIndex()` 构造 snapshot iterator。若 repair replica 在追赶过程中收到 snapshot，raft 可能认为该 replica 已经追到较新的 snapshot index。

风险时序如下：

```text
新 replica 开始 repair
  -> 从 leader 拉取 extent 数据
  -> 原 raft group 继续产生 random write log
  -> leader 向新 replica 发送 snapshot
  -> snapshot index 使用 AppliedIndex
  -> firstLogIndex 到 snapshotIndex 之间的日志被跳过
  -> ApplySnapshot 不恢复 DataPartition 数据内容
  -> 新 replica 启动 raft 后数据缺失
```

这里的问题不是 snapshot 机制本身，而是 DataPartition 的 snapshot 是轻量 no-op 语义，不能承载跳过日志后的数据恢复。

## 修复方案一：Snapshot 使用 FirstLogIndex

将 DataPartition snapshot index 从 `AppliedIndex()` 改为当前 raft 保留日志的 `FirstLogIndex()`。

这样 snapshot 不会把新 replica 的进度推进到已 apply 的最新位置，而是停在当前可回放日志的起点，保证 `firstLogIndex` 之后的 raft log 仍可继续 replay。

修复后的关键语义：

```text
snapshot index = raft first log index
  -> 不跳过 firstLogIndex 之后的可用日志
  -> 新 replica 仍需要回放后续 random write log
  -> 避免 repair 数据快照与 raft applied index 不一致
```

相关接口：

- `raft.RaftServer.FirstLogIndex`
- `raftstore.Partition.FirstLogIndex`
- `DataPartition.Snapshot`

## 问题二：RandomWrite 越过本地逻辑水位

`RandomWriteType` 是覆盖写语义，正常情况下只能覆盖已经存在的逻辑数据范围。

旧逻辑中，如果 `RandomWriteType` 的 `offset` 大于本地 extent 逻辑大小，底层 `WriteAt` 仍可能成功。Linux 会在文件中生成 sparse hole，但 `RandomWriteType` 不会更新 extent 的逻辑大小。

风险时序如下：

```text
repair replica 本地 extent size = 1 MiB
  -> raft random write 写入 offset = 1.5 MiB
  -> WriteAt 成功并生成 sparse hole
  -> e.Size 仍然是 1 MiB
  -> repair 继续从 1 MiB 开始补数据
  -> repair 写覆盖 1.5 MiB 处的 random write
  -> 数据静默不一致
```

这个问题的关键是：物理文件写入成功不代表逻辑水位一致。repair 依赖逻辑水位推进，如果 random write 越界造洞，repair 可能把新数据当成缺口覆盖掉。

## 修复方案二：RandomWrite 逻辑水位校验

为 `RandomWriteType` 增加逻辑水位校验：

- 普通数据区：`offset + size` 不能超过 `e.Size()`。
- snapshot append 区：`offset + size` 不能超过 `SnapshotDataOff()`。
- tiny extent：同样校验 `RandomWriteType`，但 tiny 的 `e.Size()` 是 page 对齐后的分配水位。

如果本地逻辑水位不足以安全 apply random write，则返回错误。该错误会沿 raft apply 路径上抛，最终停止 raft apply，避免继续产生静默数据不一致。

这个策略是有意选择 fail-stop，而不是兼容性兜底。因为一旦发现 committed raft log 无法安全 apply，说明本地副本状态已经不满足一致性要求，继续运行风险更高。

## 删除 Extent 的并发语义

extent 删除与 raft random write 并不完全共用同一条 raft apply 顺序。删除可能先在某个 replica 本地执行，后续该 replica 再 apply 较早提交的 random write log。

当前逻辑对 `ErrExtentNotFound` 保持原行为：忽略该 random write，不复活已删除 extent。

这个语义仍然合理：

- extent 已删除说明元数据层已经不再引用该 extent。
- 后续旧 random write 不应重新创建或恢复该 extent。
- 该场景与“extent 存在但逻辑水位不足”不同，后者才需要 fail-stop。

## 修复后的保护边界

修复后，一致性边界由两部分共同保证：

```text
Raft snapshot
  -> 不使用 AppliedIndex 跳过可回放日志
  -> 使用 FirstLogIndex 保留 replay 空间

RandomWriteType
  -> 不允许越过本地逻辑水位
  -> 本地状态不足时 fail-stop
```

repair 写仍然走 append repair 语义：

- 普通 repair 使用 `AppendWriteType`。
- snapshot repair 使用 `AppendRandomWriteType`。
- repair 写带 `IsRepair=true`，用于和普通客户端写区分。

因此，修复方案不会阻止正常 repair 补齐缺口，但会阻止 raft random write 在本地数据水位之外造洞。

## 验证建议

建议覆盖以下场景：

- `RandomWriteType` 覆盖已有普通 extent 范围，成功。
- `RandomWriteType` 写超过普通 extent `e.Size()`，失败。
- `RandomWriteType` 覆盖 snapshot append 已有范围，成功。
- `RandomWriteType` 写超过 `SnapshotDataOff()`，失败。
- tiny extent 在分配水位内 random write 成功。
- tiny extent 超过分配水位 random write 失败。
- DataPartition snapshot 使用 `FirstLogIndex`，不会返回 `AppliedIndex`。

当前相关验证命令：

```bash
go test ./datanode/storage ./datanode
go test ./raftstore ./remotecache/flashgroupmanager
go test -run '^$' ./master ./metanode
```

涉及 `master`、`metanode` 时需要先加载 CGO 环境：

```bash
source build/cgo_env.sh
```
