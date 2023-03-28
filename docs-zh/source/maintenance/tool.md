# CLI工具使用

使用命令行界面工具（CLI）可以实现方便快捷的集群管理。利用此工具，可以查看集群及各节点的状态，并进行各节点、卷及用户的管理。

::: tip 提示
随着CLI的不断完善，最终将会实现对于集群各节点接口功能的100%覆盖。
:::

## 编译及配置

下载CubeFS源码后，在 `cubefs/cli` 目录下，运行 `build.sh` 文件
，即可生成 `cfs-cli` 可执行程序。

同时，在 `root` 目录下会生成名为 `.cfs-cli.json`
的配置文件，修改master地址为当前集群的master地址即可。也可使用命令
`./cfs-cli config info` 和 `./cfs-cli config set` 来查看和设置配置文件。

## 使用方法

在 `cubefs/cli` 目录下，执行命令 `./cfs-cli --help` 或 `./cfs-cli -h`
，可获取CLI的帮助文档。

CLI主要分为六类管理命令：

| 命令                    | 描述         |
|-----------------------|------------|
| cfs-cli cluster       | 集群管理       |
| cfs-cli metanode      | 元数据节点管理    |
| cfs-cli datanode      | 数据节点管理     |
| cfs-cli datapartition | 数据分片管理     |
| cfs-cli metapartition | 元数据分片管理    |
| cfs-cli config        | 配置管理       |
| cfs-cli completion    | 生成自动补全命令脚本 |
| cfs-cli volume, vol   | 卷管理        |
| cfs-cli user          | 用户管理       |

### 集群管理

``` bash
./cfs-cli cluster info          #获取集群信息，包括集群名称、地址、卷数量、节点数量及使用率等
./cfs-cli cluster stat          #按区域获取元数据和数据节点的使用量、状态等
./cfs-cli cluster freeze [true/false]        #是否冻结集群，设置为 `true` 冻结后，当partition写满，集群不会自动分配新的partition
./cfs-cli cluster threshold [float]     #设置集群中每个MetaNode的内存阈值
./cli cluster cluster set [flags]    #设置集群的参数.
```

### 元数据节点管理

``` bash
./cfs-cli metanode list         #获取所有元数据节点的信息，包括id、地址、读写状态及存活状态
./cfs-cli metanode info [Address]     #展示元数据节点基本信息，包括状态、使用量、承载的partition ID等，
./cfs-cli metanode decommission [Address] #将该元数据节点下线，该节点上的partition将自动转移至其他可用节点
./cfs-cli metanode migrate [srcAddress] [dstAddress] #将源元数据节点上的meta partition转移至目标元数据节点
```

### 数据节点管理

``` bash
./cfs-cli datanode list         #获取所有数据节点的信息，包括id、地址、读写状态及存活状态
./cfs-cli datanode info [Address]     #展示数据节点基本信息，包括状态、使用量、承载的partition ID等，
./cfs-cli datanode decommission [Address] #将该数据节点下线，该节点上的data partition将自动转移至其他可用节点
./cfs-cli datanode migrate [srcAddress] [dstAddress] #将源数据节点上的data partition转移至目标数据节点
```

### 数据分片管理

``` bash
./cfs-cli datapartition info [Partition ID]        #获取指定data partition的信息
./cli datapartition decommission [Address] [Partition ID]   #将目标节点上的指定data partition分片下线，并自动转移至其他可用节点
./cfs-cli datapartition add-replica [Address] [Partition ID]    #在目标节点新增一个data partition分片
./cfs-cli datapartition del-replica [Address] [Partition ID]    #删除目标节点上的data partition分片
./cfs-cli datapartition check    #故障诊断，查找多半分片不可用和分片缺失的data partition
```

### 元数据分片管理

``` bash
./cfs-cli metapartition info [Partition ID]        #获取指定meta partition的信息
./cli metapartition decommission [Address] [Partition ID]   #将目标节点上的指定meta partition分片下线，并自动转移至其他可用节点
./cfs-cli metapartition add-replica [Address] [Partition ID]    #在目标节点新增一个meta partition分片
./cfs-cli metapartition del-replica [Address] [Partition ID]    #删除目标节点上的meta partition分片
./cfs-cli metapartition check    #故障诊断，查找多半分片不可用和分片缺失的meta partition
```

### 配置管理

``` bash
./cfs-cli config info     #展示配置信息
./cfs-cli config set [flags] #设置配置信息

Flags:
    --addr string      Specify master address [{HOST}:{PORT}]
-h, --help             help for set
    --timeout uint16   Specify timeout for requests [Unit: s]
```

### 自动补全管理

``` bash
./cfs-cli completion      #生成命令自动补全脚本
```

### 卷管理

``` bash
./cfs-cli volume create [VOLUME NAME] [USER ID] [flags]

Flags:
     --cache-action int          Specify low volume cacheAction (default 0)
     --cache-capacity int        Specify low volume capacity[Unit: GB]
     --cache-high-water int       (default 80)
     --cache-low-water int        (default 60)
     --cache-lru-interval int    Specify interval expiration time[Unit: min] (default 5)
     --cache-rule-key string     Anything that match this field will be written to the cache
     --cache-threshold int       Specify cache threshold[Unit: byte] (default 10485760)
     --cache-ttl int             Specify cache expiration time[Unit: day] (default 30)
     --capacity uint             Specify volume capacity (default 10)
     --crossZone string          Disable cross zone (default "false")
     --description string        Description
     --ebs-blk-size int          Specify ebsBlk Size[Unit: byte] (default 8388608)
     --follower-read string      Enable read form replica follower (default "true")
 -h, --help                      help for create
     --mp-count int              Specify init meta partition count (default 3)
     --normalZonesFirst string   Write to normal zone first (default "false")
     --replica-num string        Specify data partition replicas number(default 3 for normal volume,1 for low volume)
     --size int                  Specify data partition size[Unit: GB] (default 120)
     --vol-type int              Type of volume (default 0)
 -y, --yes                       Answer yes for all questions
     --zone-name string          Specify volume zone name
```

``` bash
./cfs-cli volume delete [VOLUME NAME] [flags]               #删除指定卷[VOLUME NAME], ec卷大小为0才能删除
Flags:
    -y, --yes                                           #跳过所有问题并设置回答为"yes"
```

``` bash
./cfs-cli volume info [VOLUME NAME] [flags]                 #获取卷[VOLUME NAME]的信息
Flags:
    -d, --data-partition                                #显示数据分片的详细信息
    -m, --meta-partition                                #显示元数据分片的详细信息
```

``` bash
./cfs-cli volume add-dp [VOLUME] [NUMBER]                   #创建并添加个数为[NUMBER]的数据分片至卷[VOLUME]
```

``` bash
./cfs-cli volume list                                       #获取包含当前所有卷信息的列表
```

``` bash
./cfs-cli volume transfer [VOLUME NAME] [USER ID] [flags]   #将卷[VOLUME NAME]转交给其他用户[USER ID]
Flags：
    -f, --force                                         #强制转交
    -y, --yes                                           #跳过所有问题并设置回答为"yes"
```

``` bash
./cli volume update                                     #更新集群的参数
Flags:
    --cache-action string      Specify low volume cacheAction (default 0)
    --cache-capacity string    Specify low volume capacity[Unit: GB]
    --cache-high-water int      (default 80)
    --cache-low-water int       (default 60)
    --cache-lru-interval int   Specify interval expiration time[Unit: min] (default 5)
    --cache-rule string        Specify cache rule
    --cache-threshold int      Specify cache threshold[Unit: byte] (default 10M)
    --cache-ttl int            Specify cache expiration time[Unit: day] (default 30)
    --capacity uint            Specify volume datanode capacity [Unit: GB]
    --description string       The description of volume
    --ebs-blk-size int         Specify ebsBlk Size[Unit: byte]
    --follower-read string     Enable read form replica follower (default false)
    -y, --yes               Answer yes for all questions
    --zonename string   Specify volume zone name
```

### 用户管理

``` bash
./cfs-cli user create [USER ID] [flags]         #创建用户[USER ID]
Flags：
    --access-key string                     #指定用户用于对象存储功能的access key
    --secret-key string                     #指定用户用于对象存储功能的secret key
    --password string                       #指定用户密码
    --user-type string                      #指定用户类型，可选项为normal或admin（默认为normal）
    -y, --yes                               #跳过所有问题并设置回答为"yes"
```

``` bash
./cfs-cli user delete [USER ID] [flags]         #删除用户[USER ID]
Flags：
    -y, --yes                               #跳过所有问题并设置回答为"yes"
```

``` bash
./cfs-cli user info [USER ID]                   #获取用户[USER ID]的信息
./cfs-cli user list                             #获取包含当前所有用户信息的列表
./cfs-cli user perm [USER ID] [VOLUME] [PERM]   #更新用户[USER ID]对于卷[VOLUME]的权限[PERM]
                                            #[PERM]可选项为"只读"（READONLY/RO）、"读写"（READWRITE/RW）、"删除授权"（NONE）
./cfs-cli user update [USER ID] [flags]         #更新用户[USER ID]的信息
Flags：
    --access-key string                     #更新后的access key取值
    --secret-key string                     #更新后的secret key取值
    --user-type string                      #更新后的用户类型，可选项为normal或admin
    -y, --yes                               #跳过所有问题并设置回答为"yes"
```

### 纠删码子系统管理

::: tip 提示
目前纠删码子系统的blobstore-cli还不够完善，功能覆盖不到70%；后续会不断完善，最终将会实现对于集群各模块接口功能的100%覆盖。
:::

- 命令行自动补全功能
- 显示为可读数据类型

#### 编译及配置

通过 `make cli` 编译得到纠删码子系统的CLI工具。

`./bin/blobstore-cli -c cli/cli/cli.conf` 启动命令行工具；其中 `-c cli/cli/cli.conf`
是可选配置项，主要配置一些常用变量，
比如access接入层服务发现地址，clustermgr服务地址等。

```json
{
    "access": {
        "conn_mode": 4,
        "priority_addrs": [
            "http://localhost:9500"
        ]
    },
    "default_cluster_id": 1,
    "cm_cluster": {
        "1": "http://127.0.0.1:9998 http://127.0.0.1:9999 http://127.0.0.1:10000"
    }
}
```

#### 使用方法

blobstore-cli 可以作为普通命令，比如：

```bash
blobstore-cli MainCmd SubCmd [Cmd ...] [--flag Val ...] -- [-arg ...]

1 #$> ./blobstore-cli config set conf-key conf-val
To set Key: conf-key Value: conf-val

2 #$> ./blobstore-cli util time
timestamp = 1640156245364981202 (seconds = 1640156245 nanosecs = 364981202)
        --> format: 2021-12-22T14:57:25.364981202+08:00 (now)
```

`./bin/blobstore-cli` 启动命令行。

```text
help 可以查看所有命令及简要说明
建议使用`cmd subCmd ... --flag -- -arg` 方式传递参数
```

目前实现了部分模块的主要功能，如下：

| 命令            | 描述                            |
|---------------|-------------------------------|
| blobstore-cli config    | 管理该blobstore-cli内存中的配置项                 |
| blobstore-cli util      | 小工具集合，如解析location、解析时间、生成特定数据 |
| blobstore-cli access    | 文件的上传、下载、删除等                  |
| blobstore-cli cm        | 集群信息查看和管理                     |
| blobstore-cli scheduler | 后台任务管理                        |
| blobstore-cli \...      | 补充完善中 \...\...                |

**Config**

```bash
manager memory cache of config

Usage:
  config [flags]

Sub Commands:
  del   del config of keys
  get   get config in cache
  set   set config to cache
  type  print type in cache
```

**Util**

```bash
util commands, parse everything

Usage:
  util [flags]

Sub Commands:
  location  parse location <[json | hex | base64]>
  redis     redis tools
  time      time format [unix] [format]
  token     parse token <token>
  vuid      parse vuid <vuid>
```

**Access**

```bash
blobstore access api tools

Usage:
  access [flags]

Sub Commands:
  cluster  show cluster
  del      del file
  ec       show ec buffer size
  get      get file
  put      put file
```

**Clustermgr**

```bash
cluster manager tools

Usage:
  cm [flags]

Sub Commands:
  cluster    cluster tools
  config     config tools
  disk       disk tools
  kv         kv tools
  listAllDB  list all db tools
  service    service tools
  stat       show stat of clustermgr
  volume     volume tools
  wal        wal tools
```

**Scheduler**

```bash
scheduler tools

Usage:
  scheduler [flags]

Flags:
  -h, --help     display help

Sub Commands:
  checkpoint  inspect checkpoint tools
  kafka       kafka consume tools
  migrate     migrate tools
  stat        show leader stat of scheduler
```
