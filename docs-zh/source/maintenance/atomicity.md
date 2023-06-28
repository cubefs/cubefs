# 原子性管理

CubeFS支持原子性操作，在开启原子性功能后，保证文件操作的元数据原子性得到满足，元数据采用最终一致性，即要么文件操作的元数据修改全部成功，要么全部失败回滚到之前状态。

## 原子性支持的FUSE接口

- Create
- Mkdir
- Remove
- Rename
- Mknod
- Symlink
- Link

::: tip 提示
原子性暂时支持FUSE接口，S3接口暂不支持
:::

## 原子性开关
CubeFS实现了事务的部分特性，但是和传统严格意义上的事务有区别，为方便描述，对文件的操作，统称为事务。

事务以卷（Volume）为单位生效，一个集群可以有多个卷，每个卷可以支持单独开关选择是否生效事务。客户端启动从master拉取关于事务相关配置，开启事务，则所有该客户端挂载的目录全部走事务逻辑，未开启则走原流程。

开关可以控制所有支持的接口，或者只开启某些接口，事务超时时间不设置默认是1分钟。

```
curl "192.168.0.11:17010/vol/update?name=ltptest&enableTxMask=all&txForceReset=true&txConflictRetryNum=13&txConflictRetryInterval=30&txTimeout=1&authKey=0e20229116d5a9a4a9e876806b514a85"
```

::: tip 提示
修改事务相关参数值后，最长两分钟从master同步到client端生效。
:::

| 参数      | 类型     | 描述                                                                        |
|---------|--------|---------------------------------------------------------------------------|
| enableTxMask    | string | 值可以为：create,mkdir,remove,rename,mknod,symlink,link,off,all。off和all以及其他值互斥 |
| txTimeout  | uint32 | 事务超时时间不设置默认是1分钟，单位分钟，最长60分钟                                               |
| txForceReset | uint32 | 设置值同enableTxMask，不同的是，enableTxMask会做合并，而txForceReset强制重置为指定值              |
| txConflictRetryNum | uint32 | 取值范围[1-100]，默认为10                                                         |
| txConflictRetryInterval | uint32 | 单位毫秒，取值范围[10-1000]，默认为20ms                                                |

可以通过获取卷信息查看是否开启事务
```
curl "192.168.0.11:17010/admin/getVol?name=ltptest"
```

也可以在创建卷时指定是否开启事务

```
curl "192.168.0.11:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3&enableTxMask=all&txTimeout=5"
```