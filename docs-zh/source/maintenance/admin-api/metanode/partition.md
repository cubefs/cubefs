# 元数据分片管理

## 获取当前设备上所有分片信息

``` bash
curl -v  http://10.196.59.202:17210/getPartitions
```

## 获取指定分片ID的当前状态信息

``` bash
curl -v http://10.196.59.202:17210/getPartitionById?pid=100
```

获取指定分片id的当前状态信息，包含当前分片组的raft
leader地址，raft组成员，inode分配游标等信息

请求参数：

| 参数  | 类型  | 描述       |
|-----|-----|----------|
| pid | 整型  | 元数据分片的ID |
