# 预读

在文件顺序读场景下，可以通过客户端aheadReadEnable的参数开启预读功能，提前将文件的后续数据内容缓存到客户端的内存中。

![aheadread](./pic/aheadread.png)


配置文件参数含义如下表示：

| 参数 | 类型 | 含义 | 必需 |
| ---- | ---- | ---- | ---- |
| aheadReadEnable        | bool  | 是否开启预读                              | 是  |
| aheadReadTotalMemGB      | int64 | 预读占用内存(单位GB,默认10)，若不够10G则占用当前可用内存的1/3 | 否  |
| aheadReadBlockTimeOut      | int64 | 缓存块未命中的回收时间(单位秒,默认3)                   | 否  |
| aheadReadWindowCnt          | int64 | 缓存滑动窗口的大小(默认：8)                     | 否  |
| minReadAheadSize          | int64 | 触发预读功能所需文件大小的最小值(单位字节，默认10)                     | 否  |

开启预读功能后，对于文件大小超过minReadAheadSize的文件，预读功能会将当前读取请求偏移offset之后，aheadReadWindowCnt个缓存数据块的内容通过异步的方式，加载到客户端的内存中，从而提升顺序读的性能。目前每个缓存数据块的大小为4MB。

所有文件的预读缓存数块总量共享一个缓存资源池，资源池大小为aheadReadTotalMemGB。如果缓存资源池满了，则无法继续缓存新的数据内容，需要等之前的缓存数据块淘汰。

::: tip 提示：
1、预读功能目前仅支持副本模式，不支持EC。
2、开启预读会占用一定的客户端内存，对于客户端内存限制高的场景，可以调节aheadReadTotalMemGB的大小，但是性能会有一定的衰减。
3、预读功能具有最高读取优先级，如果和其他读取功能一并开启，比如分布式缓存，会优先从预读中获取数据。
:::