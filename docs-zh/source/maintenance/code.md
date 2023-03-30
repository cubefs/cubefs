# 特殊状态码说明

## 纠删码子系统

纠删码子系统各个模块均支持审计日志，开启审计日志及监控指标（可以[参考文档](../maintenance/configs/blobstore/base.md)），则可在审计日志文件或者监控指标`service_response_code`
中查看到服务请求状态码信息，下面介绍各模块的特殊状态码。

### Access

::: tip 提示 
Access服务的错误状态码范围为[550,599]
:::

| 状态码 | 错误信息                                       | 说明                                   |
|-----|--------------------------------------------|--------------------------------------|
| 551 | access client service discovery disconnect | access client 无法从consul发现可用的access节点 |
| 552 | access limited                             | 服务接口达到链接数限制                          |
| 553 | access exceed object size                  | 上传文件超过最大大小限制                         |

### Proxy

::: tip 提示 
Proxy服务的错误状态码范围为[800,899]
:::

| 状态码 | 错误信息                                  | 说明                                  |
|-----|---------------------------------------|-------------------------------------|
| 801 | this codemode has no avaliable volume | 对应编码模式没有可用卷                         |
| 802 | alloc bid from clustermgr error       | 从cm获取bid失败                          |
| 803 | clusterId not match                   | 请求的clusterID和proxy服务所在的clusterID不一致 |

### Clustermgr

::: tip 提示 
Clustermgr服务的错误状态码范围为[900,999]
:::

| 状态码 | 错误信息                                                            | 说明                                |
|-----|-----------------------------------------------------------------|-----------------------------------|
| 900 | cm: unexpected error                                            | 出现内部错误                            |
| 902 | lock volume not allow                                           | 加锁失败，例如处于active状态                 |
| 903 | unlock volume not allow                                         | 解锁失败                              |
| 904 | volume not exist                                                | 卷不存在                              |
| 906 | raft propose error                                              | raft 提议信息错误                       |
| 907 | no leader                                                       | 暂无主节点                             |
| 908 | raft read index error                                           | 线性一致性读超时                          |
| 910 | duplicated member info                                          | 重复的成员信息                           |
| 911 | disk not found                                                  | 磁盘找不到                             |
| 912 | invalid status                                                  | 设置磁盘状态出现非法状态                      |
| 913 | not allow to change status back                                 | 不允许将磁盘状态回退，例如将坏盘回退到正常状态           |
| 914 | alloc volume unit concurrently                                  | 并发申请卷单元，重试可用解决                    |
| 916 | alloc volume request params is invalid                          | 分配卷请求参数非法                         |
| 917 | no available volume                                             | 分配卷时候，暂无可用的卷                      |
| 918 | update volume unit, old vuid not match                          | 更新卷单元时候新的旧的vuid不匹配                |
| 919 | update volume unit, new vuid not match                          | 更新卷单元时候新的vuid不匹配                  |
| 920 | update volume unit, new diskID not match                        | 更新卷单元时候新的磁盘id不匹配                  |
| 921 | config argument marshal error                                   | 配置序列化错误                           |
| 922 | request params error, invalid clusterID                         | 请求参数错误，clusterID非法                |
| 923 | request params error,invalid idc                                | 请求参数错误，idc非法                      |
| 924 | volume unit not exist                                           | 卷单元不存在                            |
| 925 | register service params is invalid                              | 注册服务参数非法                          |
| 926 | disk is abnormal or not readonly, can't add into dropping list  | 磁盘处于非正常或者不是只读状态，不能设置为下线           |
| 927 | stat blob node chunk failed                                     | 获取blobNode 的chunk信息失败             |
| 928 | request alloc volume codeMode not invalid                       | 请求分配的卷模式不存在                       |
| 929 | retain volume is not alloc                                      | 续租的卷没有分配                          |
| 930 | dropped disk still has volume unit remain, migrate them firstly | 不能提前标记磁盘下线完成，改磁盘还有剩余chunk，需要先迁移完成 |
| 931 | list volume v2 not support idle status                          | v2版本列举卷不支持idle状态                  |
| 932 | dropping disk not allow change state or set readonly            | 下线中磁盘不允许修改状态和设置只读                 |
| 933 | reject delete system config                                     | 系统配置不允许删除                         |

### BlobNode

::: tip 提示 
BlobNode服务的错误状态码为[600,699]
:::

| 状态码 | 错误信息                               | 说明                                               |
|-----|------------------------------------|--------------------------------------------------|
| 600 | blobnode: invalid params           | 参数错误                                             |
| 601 | blobnode: entry already exist      | 已经存在（vuid）                                       |
| 602 | blobnode: out of limit             | 请求超过并发限制                                         |
| 603 | blobnode: internal error           | 内部错误                                             |
| 604 | blobnode: service is overload      | 请求过载                                             |
| 605 | blobnode: path is not exist        | 注册的磁盘目录不存在                                       |
| 606 | blobnode: path is not empty        | 注册的磁盘目录非空                                        |
| 607 | blobnode: path find online disk    | 注册的路径还处于活动状态，需要下线后再重新注册                          |
| 611 | disk not found                     | 磁盘不存在                                            |
| 613 | disk is broken                     | 坏盘                                               |
| 614 | disk id is invalid                 | 磁盘id无效                                           |
| 615 | disk no space                      | 磁盘无可用空间                                          |
| 621 | vuid not found                     | vuid不存在                                          |
| 622 | vuid readonly                      | vuid只读状态                                         |
| 623 | vuid released                      | vuid处于released状态                                 |
| 624 | vuid not match                     | vuid不匹配                                          |
| 625 | chunk must readonly                | chunk需要是只读状态                                     |
| 626 | chunk must normal                  | chunk需要是normal状态                                 |
| 627 | chunk no space                     | chunk无可写空间                                       |
| 628 | chunk is compacting                | chunk处于压缩中                                       |
| 630 | chunk id is invalid                | 无效的chunk id                                      |
| 632 | too many chunks                    | chunk数超过阈值                                       |
| 633 | chunk in use                       | chunk有请求在处理                                      |
| 651 | bid not found                      | bid不存在                                           |
| 652 | shard size too large               | shard的size超过阈值(1<<32 - 1)                        |
| 653 | shard must mark delete             | shard需要先标记删除                                     |
| 654 | shard already mark delete          | shard已经标记删除                                      |
| 655 | shard offset is invalid            | shard的offset无效                                   |
| 656 | shard list exceed the limit        | shard list的个数超过阈值(65536)                         |
| 657 | shard key bid is invalid           | 无效的bid                                           |
| 670 | dest replica is bad can not repair | 待修补的目标节点状态异常，可能是请求超时或者服务未启动等                     |
| 671 | shard is an orphan                 | shard为孤本无法执行数据修补，Scheduler发起修补时如果遇到此错误则会记录相关孤本信息 |
| 672 | illegal task                       | 后台任务不合法                                          |
| 673 | request limited                    | 修补接口请求过载                                         |

### Scheduler

::: tip 提示 
Scheduler服务的状态码为[700,799]
:::

| 状态码 | 错误信息          | 说明                                                             |
|-----|---------------|----------------------------------------------------------------|
| 700 | nothing to do | BlobNode服务请求Scheduler拉取后台任务时，如果没有相关任务可做，Scheduler则会返回此状态码，可以忽略 |

