# 元数据分片管理

## 获取当前设备上所有分片信息

``` bash
curl -v  "http://10.196.59.202:17220/getPartitions"
```

## 获取指定分片 ID 的当前状态信息

``` bash
curl -v "http://10.196.59.202:17220/getPartitionById?pid=100"
```

获取指定分片 id 的当前状态信息，包含当前分片组的 raft
leader 地址，raft 组成员，inode 分配游标等信息

请求参数：

| 参数  | 类型  | 描述       |
|-----|-----|----------|
| pid | 整型  | 元数据分片的 ID |
