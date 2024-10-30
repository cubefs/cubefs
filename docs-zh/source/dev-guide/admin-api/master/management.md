# 资源管理

## 增加节点

``` bash
curl -v "http://192.168.0.1:17010/raftNode/add?addr=10.196.59.197:17010&id=3"
```

增加新的master节点到raft复制组。

参数列表

| 参数   | 类型     | 描述                      |
|------|--------|-------------------------|
| addr | string | master的ip地址, 格式为ip:port |
| id   | uint64 | master的节点标识             |

## 删除节点

``` bash
curl -v "http://192.168.0.1:17010/raftNode/remove?addr=10.196.59.197:17010&id=3"
```

从raft复制组中移除某个节点。

参数列表

| 参数   | 类型     | 描述                      |
|------|--------|-------------------------|
| addr | string | master的ip地址, 格式为ip:port |
| id   | uint64 | master的节点标识             |

## 获取nodeset列表

``` bash
curl -v "http://192.168.0.1:17010/nodeSet/list"
```
获取集群的nodeset列表。

## 获取nodeset详细信息

``` bash
curl -v "http://192.168.0.1:17010//nodeSet/get?nodesetId=1"
```

获取集群中nodeset的详细信息

参数列表

| 参数   | 类型     | 描述                      |
|------|--------|-------------------------|
| nodesetId | string | nodeset编号 |
