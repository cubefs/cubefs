# 基于 Tag 迁移 MP 和 DP

::: warning 注意
从 v3.6.0 开始，CubeFS 支持根据节点 Tag 自动迁移 MetaPartition 和 DataPartition 副本。
:::

基于 Tag 的迁移将副本放置策略与物理拓扑解耦。管理员为 MetaNode 和 DataNode 设置 Tag，配置从旧 Tag 到目标 Tag 的映射规则，Master 会自动检查并迁移副本记录与目标策略不一致的分区。

该能力适用于硬件替换、资源组迁移和节点逐步退役。它与元数据 Region 策略、存储池放置策略相互独立。

## 设置节点 Tag

节点 Tag 长度为 1–49 个字符，只能包含字母和数字。

```bash
./cfs-cli datanode update 10.0.0.10:17310 oldData
./cfs-cli metanode update 10.0.0.20:17210 oldMeta
```

更新节点 Tag 不会立即移动副本。只有配置映射规则并开启 Tag 自动修复后，迁移任务才会启动。

## 配置映射规则

映射规则使用 `source->target` 格式。一条规则可以映射多个副本 Tag，多条规则使用分号分隔。

```text
oldData->newData
oldA,oldA->newA,newA;oldB->newB
```

每条规则的源 Tag 与目标 Tag 数量必须相同。最多可以配置三条规则，并且源 Tag 与目标 Tag 之间不能交叉重复。

配置集群级默认规则：

```bash
./cfs-cli cluster set \
    --autoMigrateByTag true \
    --dpTagMapRules "oldData->newData" \
    --mpTagMapRules "oldMeta->newMeta" \
    --maxDpTagDecommissionLimit 100 \
    --maxMpTagDecommissionLimit 5
```

卷级规则的优先级高于集群默认规则：

```bash
./cfs-cli vol update test \
    --dpTagMapRules "oldData->newData" \
    --mpTagMapRules "oldMeta->newMeta"
```

将规则值设置为 `"null"` 可清除映射规则。

## 迁移流程

开启 `autoMigrateByTag` 后，Master 会周期性检查 Tag 一致性。

DataPartition 迁移流程：

1. Master 比较每个副本记录的 Tag 与所在 DataNode 的 Tag。
2. Tag 不一致的副本会被标记为 Tag 下线任务。
3. 根据目标 Tag 选择替换副本。
4. 通过常规 DataPartition 修复和下线流程完成迁移。

MetaPartition 迁移流程：

1. Master 收集不符合有效 MetaPartition Tag 规则的副本。
2. 根据 Tag 和元数据存储模式选择目标 MetaNode。
3. 添加 learner 副本并追赶 Raft 组数据。
4. learner 就绪后移除旧副本。

因此，MetaPartition 迁移需要开启基于 learner 的下线能力：

```bash
./cfs-cli cluster set --enableMpDecommissionByLearner true
```

## 查看迁移进度

查看集群级或卷级迁移状态：

```bash
./cfs-cli tag summary --all
./cfs-cli tag summary --meta --all
./cfs-cli tag summary --data --all
./cfs-cli tag vol-summary test --all
```

汇总信息包含生效规则、Tag 不匹配的分区、正在执行的下线任务、失败记录和调度器状态。解决持续失败的原因后，可清理失败记录以触发重试：

```bash
./cfs-cli tag clear-failed-keys
```

## 运维注意事项

- 卷级规则优先于集群级默认规则。
- DataPartition Tag 迁移要求卷至少有三个副本。
- 默认并发上限为 100 个 DataPartition 任务和 5 个 MetaPartition 任务。
- 将并发上限设置为 `0` 会恢复默认值。
- 开启自动迁移前，应检查目标 Tag 对应节点的容量和健康状态。
- 大规模集群应分阶段修改规则，并在迁移期间持续监控修复流量。
