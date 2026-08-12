# 元数据多 Region

::: warning 注意
从 v3.6.0 开始，CubeFS 支持按 Region 组织元数据资源，并根据卷级 Region 策略放置 MetaPartition。
:::

Region 是一组元数据 Zone 的逻辑集合。通过 Region，可以在同一集群中管理位于不同地域的元数据资源，并显式控制元数据放置策略。卷可以指定默认 Region、添加多个允许使用的 Region，并在其他 Region 中维护 learner 副本。

Region 仅作用于 MetaNode 和 MetaPartition。数据放置由存储池和 Zone 独立管理。

## 配置元数据 Region

在每个 MetaNode 配置中设置 `region` 和 `zoneName`。第一个 MetaNode 加入 Zone 时，该 Zone 会与其 Region 绑定；后续加入同一 Zone 的 MetaNode 必须使用相同的 Region。

```json
{
    "zoneName": "az-east-1",
    "region": "east"
}
```

未配置 `region` 时，CubeFS 使用 `default` Region。

Region 名称长度必须为 3–32 个字符，以字母开头、以字母或数字结尾，并且只能包含字母、数字、下划线和连字符。

通过 CLI 查看生成的拓扑：

```bash
./cfs-cli region list
./cfs-cli region info east
./cfs-cli zone list
```

Region 由已注册的元数据 Zone 隐式创建，不需要单独执行创建操作。

## 配置卷

按需设置集群默认元数据 Region：

```bash
./cfs-cli cluster setDefaultMetaRegion east
```

在指定的默认 Region 中创建卷：

```bash
./cfs-cli vol create test root --default-region east
```

默认 Region 会自动加入卷的允许 Region 列表。卷使用其他 Region 前，需要先添加该 Region：

```bash
./cfs-cli vol addRegion test west
./cfs-cli vol updateDefaultRegion test west
```

目标 Region 必须存在于卷允许使用的 Zone 中。添加第二个 Region 时，卷会同时启用跨 Zone 放置。

客户端可以通过 `metaRegion` 挂载参数选择卷允许使用的元数据 Region。未指定时使用卷的默认 Region。

## 配置跨 Region Learner

MetaPartition Region 策略可用于在其他 Region 中维护 learner 副本。配置策略前需要开启基于 learner 的 MetaPartition 下线能力：

```bash
./cfs-cli cluster set --enableMpDecommissionByLearner true
./cfs-cli vol updateMpRegionPolicy test \
    --region east \
    --policy "west:rocksdb"
```

策略以源 Region 为单位配置。每个目标项的格式为 `targetRegion:storeMode`，其中 `storeMode` 可取 `rocksdb` 或 `memory`；多个目标项使用分号分隔。

```bash
./cfs-cli vol updateMpRegionPolicy test \
    --region east \
    --policy "west:rocksdb; north:memory"

./cfs-cli vol mpRegionPolicy test
```

源 Region 与目标 Region 不能相同，并且所有目标 Region 都必须已加入卷的允许 Region 列表。使用 `--policy "empty"` 可清除指定源 Region 的策略。

## 工作原理

- 新 MetaPartition 在请求指定的 Region 或卷的默认 Region 中创建。
- CubeFS 独立检查每个允许 Region 的可写 MetaPartition 容量，并按需扩容。
- Master 周期性检查跨 Region learner 副本，使其与配置策略保持一致。
- 启用 NearRead 或 follower read 后，客户端在元数据更新完成后会临时从 leader 读取，以保证 read-your-writes 一致性。

Region 策略可以改善元数据访问的本地性和容灾能力，但不能替代各 Region 的网络时延规划和容量规划。
