# 元数据节点管理

## 列出所有元数据节点

获取所有元数据节点的信息，包括id、地址、读写状态及存活状态

```bash
cfs-cli metanode list
```

## 展示元数据节点基本信息

展示元数据节点基本信息，包括状态、使用量、承载的partition ID等，

```bash
cfs-cli metanode info [Address]
```

## 下线元数据节点

将该元数据节点下线，该节点上的partition将自动转移至其他可用节点

```bash
cfs-cli metanode decommission [Address]
```

## 转移源元数据节点上的mp

将源元数据节点上的meta partition转移至目标元数据节点

```bash
cfs-cli metanode migrate [srcAddress] [dstAddress] 
```
