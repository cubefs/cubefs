# 数据多存储池

::: warning 注意
从 v3.6.0 开始，CubeFS 支持将数据 Zone 和卷配置到多个存储池。
:::

存储池是具有明确存储类型的一组数据资源。它在 Zone 之上提供放置边界，使同一集群可以隔离容量、组织不同硬件资源，并跨多个存储池管理 DataPartition。

存储池与存储类型相互补充。存储类型描述 ReplicaSSD、ReplicaHDD 等介质类型，存储池则用于确定 DataPartition 放置到哪一组资源。

## 默认存储池

CubeFS 内置以下三个保留存储池：

- 存储池 `1`：`defaultSSDPool`，存储类型为 ReplicaSSD。
- 存储池 `2`：`defaultHDDPool`，存储类型为 ReplicaHDD。
- 存储池 `3`：`defaultECPool`，存储类型为 BlobStore。

自定义存储池 ID 的范围为 4–255。可以通过 CLI 创建 ReplicaSSD 和 ReplicaHDD 类型的自定义存储池，暂不支持创建自定义 BlobStore 存储池。

查看可用存储池：

```bash
./cfs-cli pool list
./cfs-cli pool info 1
```

## 创建存储池

将 DataNode 加入自定义存储池前，先创建副本存储池：

```bash
./cfs-cli pool create \
    --id 20 \
    --name pool-hdd-2 \
    --storageClass 2
```

`storageClass` 取值 `1` 表示 ReplicaSSD，取值 `2` 表示 ReplicaHDD。存储池名称长度必须为 3–32 个字符，以字母开头，并且只能包含字母、数字、下划线和连字符。

在 DataNode 配置中设置 `poolId`：

```json
{
    "zoneName": "az-hdd-2",
    "mediaType": 2,
    "poolId": 20
}
```

第一个 DataNode 加入 Zone 时，该 Zone 会与存储池绑定；后续加入同一 Zone 的所有 DataNode 必须使用相同的存储池 ID。

## 配置卷

创建卷时指定默认存储池和允许使用的存储池列表：

```bash
./cfs-cli vol create test root \
    --poolId 1 \
    --pools "1,20"
```

指定多个存储池时会自动启用跨 Zone 放置。每个允许使用的存储池都必须存在于卷配置的 Zone 中。

也可以向已有卷添加存储池：

```bash
./cfs-cli vol addPool test 20
./cfs-cli vol updatePoolId test 20 --poolName pool-hdd-2
```

默认存储池必须包含在卷的允许存储池列表中。修改默认存储池前，应确认目标存储池具有足够的可写 DataPartition 和可用容量。

## 工作原理

- 每个 DataPartition 都记录其所在的存储池。
- Master 分别维护每个允许存储池中的可写 DataPartition 容量。
- 拓扑选点根据存储池 ID 过滤候选 Zone 和 DataNode。
- 容量使用量和配额可以按存储池统计。
- 请求未显式选择存储池时，使用集群和卷配置的默认存储池。

多存储池可用于在相同存储类型内隔离不同工作负载或硬件资源组，但不会自动迁移已有数据。需要移动数据时，应使用生命周期规则或运维迁移流程。

::: warning 限制
启用 FaultDomain 模式时，卷不能配置多个副本存储池。
:::
