# 数据节点管理

## 列出所有数据节点

获取所有数据节点的信息，包括id、地址、读写状态及存活状态

```bash
cfs-cli datanode list
```

## 展示数据节点基本信息

展示数据节点基本信息，包括状态、使用量、承载的partition ID等，

```bash
cfs-cli datanode info [Address]
```

## 下线数据节点

将该数据节点下线，该节点上的data partition将自动转移至其他可用节点

```bash
cfs-cli datanode decommission [Address]
```

## 转移数据节点上的dp

将源数据节点上的data partition转移至目标数据节点

```bash
cfs-cli datanode migrate [srcAddress] [dstAddress]
```