# 故障域配置及管理命令

## 升级及配置项

### Cluster级别配置

启用故障域需要增加cluster级别的配置，否则无法区分，新增zone是故障域zone还是归属于原有cross_zone

```bash
FaultDomain               bool  // 默认false
```

### Volume级别配置

保留：

```bash
crossZone        bool  //跨zone
```

新增：

```bash
default_priority  bool  // true优先选择原有的zone，而不是从故障域里面分配
```

### 配置小结

1.  现有的cluster，无论是自建的，还是社区的，无论是单个zone，还是跨zone，如果需要故障域启用，需要cluster支持，master重启，配置更新，同时管控更新现有volume的策略。否则继续沿用原有策略。
2.  如果cluster支持，volome不选择使用，则继续原有volome策略，需要在原有zone中按原有策略分配。原有资源耗尽再使用新的zone资源，
3.  如果cluster不支持，volome无法自己启用的故障域策略

| Cluster:faultDomain | Vol:crossZone | Vol:normalZonesFirst | Rules for volume to use domain                                                |
|---------------------|---------------|----------------------|-------------------------------------------------------------------------------|
| N                   | N/A           | N/A                  | Do not support domain                                                         |
| Y                   | N             | N/A                  | Write origin resources first before fault domain until origin reach threshold |
| Y                   | Y             | N                    | Write fault domain only                                                       |
| Y                   | Y             | Y                    | Write origin resources first before fault domain until origin reach threshold |

## 注意事项

故障域解决多zone场景下copysets分布没有规划影响了数据的耐久性的问题，但原有数据不能自动迁移

1.启用故障域后，新区域中的所有设备都将加入故障域

2.创建的volume会优先选择原zone的资源

3.新建卷时需要根据上表添加配置项使用域资源。默认情况下，如果可用，则首先使用原始zone资源

## 管理命令

创建使用故障域的volume ---------

```bash
curl "http://192.168.0.11:17010/admin/createVol?name=volDomain&capacity=1000&owner=cfs&crossZone=true&normalZonesFirst=false"
```

参数列表

| 参数               | 类型     | 描述      |
|------------------|--------|---------|
| crossZone        | string | 是否跨zone |
| normalZonesFirst | bool   | 非故障域优先  |

### 查看故障域是否启用

```bash
curl "http://192.168.0.11:17010/admin/getIsDomainOn"
```

### 查看故障域使用情况

```bash
curl -v  "http://192.168.0.11:17010/admin/getDomainInfo"
```

查看故障域copyset group的使用情况 ---------.. code-block:: bash

```bash
curl "http://192.168.0.11:17010/admin/getDomainNodeSetGrpInfo?id=37"
```

更新非故障域数据使用上限 ---------.. code-block:: bash

```bash
curl "http://192.168.0.11:17010/admin/updateZoneExcludeRatio?ratio=0.7"
```