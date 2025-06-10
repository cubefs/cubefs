# 使用分布式缓存系统


## 编译构建

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make server
```

## 启动flashnode

``` bash
./cfs-server -c flashnode.json
```

示例 `flashnode.json`:

``` json
{
    "role": "flashnode",
    "listen": "18510",
    "prof": "18511",
    "logDir": "./logs",
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ],
    "readRps": 100000,
    "disableTmpfs": true,
    "diskDataPath": [
      "/path/data1:0"
      ],
    "zoneName":"default"
}
```

更多详细配置请参考 [FlashNode Detailed Configuration](../ops/configs/flashnode.md).

## 分布式缓存配置

### 创建并启用flashGroup
通过cli工具的flashgroup create命令创建缓存组flashgroup，并分配到唯一识别该flashgroup的ID。

```bash
// fg的slot数量默认为32
./cfs-cli flashgroup create 
```

通过cli工具的flashgroup set命令启用flashGroup，参数为上一步被分配的ID。

```bash
// 创建后的flashGroup状态默认是inactive状态，需要设置成active状态
./cfs-cli flashgroup set 13 true
```

### flashGroup添加flashNode

通过cli工具的flashGroup nodeAdd命令往刚创建的flashGroup中添加缓存节点flashNode

```bash
// flashGroup添加flashNode，指定flashGroup的ID以及要添加的flashNode
./cfs-cli flashGroup nodeAdd 13 --zone-name=default --addr="*.*.*.*:18510"
```

通过cli工具的flashNode list命令，查看刚添加的flashNode是否正确。默认新添加的flashNode的active和enable状态都为true。

```bash
./cfs-cli flashnode list
```

### 开启卷的分布式缓存能力

通过cli工具的vol update命令开启目标卷的分布式缓存能力，默认新建卷以及集群升级前已存在的卷都没有不是缓存能力。

```bash
// 打开目标卷test（已创建）的remoteCacheEnable开关。
./cfs-cli vol update test --remoteCacheEnable true
```

通过cli工具的vol info命令查看目标卷是否打开分布式缓存能力

```bash
./cfs-cli vol info test
```
remoteCacheEnable为false表示未开启分布式缓存能力，true则表示已开启。


### 确认分布式缓存生效

client挂载已经开启分布式缓存能力的卷。

对卷根目录下的文件进行读请求测试。默认情况下，分布式缓存会缓存根目录下所有大小小于128GB的文件。

通过client端的mem_stat.log查看是否有flashNode的条目，该条目记录了客户端从flashNode读取的次数与平均延时。所以如果有对应条目，说明client端尝试flashNode读取对应的缓存数据。

