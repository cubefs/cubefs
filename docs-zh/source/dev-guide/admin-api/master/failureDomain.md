# 故障域配置及管理

## 升级及配置项

### Cluster 级别配置

启用故障域需要增加 cluster 级别的配置，否则无法区分，新增 zone 是故障域 zone 还是归属于原有 cross_zone

```bash
FaultDomain               bool  # 默认false
```

### Volume级别配置

保留：

```bash
crossZone         bool  # 跨zone
```

新增：

```bash
default_priority  bool  # true优先选择原有的zone，而不是从故障域里面分配
```

### 配置小结

1.  对于现有的 cluster，无论是自建的还是社区的，无论是单个 zone 还是跨 zone，如果需要故障域启用，需要 cluster 支持故障域，master 需要重启，配置需要更新，同时管控现有 volume 更新的策略。否则，请继续沿用原有策略。
2.  如果 cluster 支持，但 volume 不选择使用，则继续原有 volume 策略，并根据原有策略在原有 zone 中分配资源。原有资源耗尽再使用新的 zone 资源，
3.  如果 cluster 不支持，volume 无法启用自己的故障域策略

| Cluster:faultDomain | Vol:crossZone | Vol:normalZonesFirst | Rules for volume to use domain                                                |
|---------------------|---------------|----------------------|-------------------------------------------------------------------------------|
| N                   | N/A           | N/A                  | Do not support domain                                                         |
| Y                   | N             | N/A                  | Write origin resources first before fault domain until origin reach threshold |
| Y                   | Y             | N                    | Write fault domain only                                                       |
| Y                   | Y             | Y                    | Write origin resources first before fault domain until origin reach threshold |

## 注意事项

故障域解决了多 zone 场景下 copysets 分布没有规划的问题，保证了数据的耐久性，但原有数据不能自动迁移。

1. 启用故障域后，新区域中的所有设备都将加入故障域

2. 创建的 volume 会优先选择原 zone 的资源

3. 新建卷时需要根据上表添加配置项使用域资源。默认情况下，如果可用，则首先使用原始 zone 资源

## 管理命令

创建使用故障域的 volume

```bash
curl "http://192.168.0.11:17010/admin/createVol?name=volDomain&capacity=1000&owner=cfs&crossZone=true&normalZonesFirst=false"
```

参数列表

| 参数               | 类型     | 描述      |
|------------------|--------|---------|
| crossZone        | string | 是否跨 zone |
| normalZonesFirst | bool   | 非故障域优先  |

### 查看故障域是否启用

```bash
curl "http://192.168.0.11:17010/admin/getIsDomainOn"
```

### 查看故障域使用情况

```bash
curl -v  "http://192.168.0.11:17010/admin/getDomainInfo"
```

查看故障域copyset group的使用情况

```bash
curl "http://192.168.0.11:17010/admin/getDomainNodeSetGrpInfo?id=37"
```

更新非故障域数据使用上限

```bash
curl "http://192.168.0.11:17010/admin/updateZoneExcludeRatio?ratio=0.7"
```