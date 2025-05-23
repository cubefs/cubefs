# 分布式缓存

在如今AI大模型训练场景下，模型的数据集以及参数规模呈现爆发性增长。仅靠单个GPU计算节点的磁盘已经无法缓存TB或者PB级模型训练所需的数据。因此需要一种容量更大，吞吐能力更强，能被多个GPU计算节点共享的缓存策略来提升训练数据的访问效率。

## 系统拓扑

![image](./pic/flash_topo.png)

**数据读取流程**

CubeFS的分布式缓存由多个FlashGroup组成，每个FlashGroup负责管理一致性哈希环上的一组slot值。

客户端根据待缓存数据块所属的卷、inode、以及数据块的偏移信息，计算出一个唯一的对应到一致性哈希环上的一个值。分布式缓存的路由算法会在这个哈希环上找到第一个大于等于这个值的slot值，那么这个slot值所属的FlashGroup负责该数据块持久化并提供缓存读取服务。

FlashGroup由缓存节点FlashNode组成，可以分布在不同的zone中。客户端读取缓存数据时，则会通过对FlashNode进行延时分析，选择访问延时最低的FlashNode进行读取。

**FlashNode**

下面为 FlashNode 进程启动所需的配置文件示例：

```text
{
    "role": "flashnode",
    "listen": "18510",
    "prof": "18511",
    "logDir": "./logs",
    "masterAddr": [
        "xxx",
        "xxx",
        "xxx"
    ],
    "memTotal": 0,
    "cachePercent": 0.8,
    "readRps": 100000,
    "disableTmpfs": true,
    "diskDataPath": [
      "/path/data1:0",
      "/path/data2:0"
      ],
    "zoneName":"default"
}
```

**Master**

Master 负责对整个集群中所有分布式缓存的拓扑信息进行持久化存储与统一管理。它接收 FlashNode 的注册信息和心跳消息，以此判断各个 FlashNode 节点的存活状态。master接受cli命令对FlashGroup的slot分配，客户端在读取数据时，会向 Master 请求获取最新的分布式缓存拓扑结构，用于实现数据读取的正确路由。

**Client**

为支持文件缓存读取，客户端将结合卷级别的缓存开关和集群缓存功能状态，决定是否从分布式缓存中获取数据。
