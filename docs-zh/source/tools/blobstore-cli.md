# blobstore-cli工具使用

::: tip 提示
目前纠删码子系统的blobstore-cli还不够完善，功能覆盖不到70%；后续会不断完善，最终将会实现对于集群各模块接口功能的100%覆盖。
:::

- 命令行自动补全功能
- 显示为可读数据类型

## 编译及配置


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

## 使用方法

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

| 命令                      | 描述                            |
|-------------------------|-------------------------------|
| blobstore-cli config    | 管理该blobstore-cli内存中的配置项       |
| blobstore-cli util      | 小工具集合，如解析location、解析时间、生成特定数据 |
| blobstore-cli access    | 文件的上传、下载、删除等                  |
| blobstore-cli cm        | 集群信息查看和管理、后台任务开关管理            |
| blobstore-cli scheduler | 后台任务管理                        |
| blobstore-cli \...      | 补充完善中 \...\...                |

## Config

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

## Util

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

## Access

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

## Clustermgr

```bash
cluster manager tools

Usage:
  cm [flags]

Sub Commands:
  background  background task switch control tools
  cluster     cluster tools
  config      config tools
  disk        disk tools
  kv          kv tools
  listAllDB   list all db tools
  raft        raft db tools
  service     service tools
  snapshot    snapshot tools
  stat        show stat of clustermgr
  volume      volume tools
  wal         wal tools
```

如启用/停用后台任务：

```bash
blobstore-cli cm background
```

```text
Background task switch control for clustermgr, currently supported: [disk_repair, balance, disk_drop, manual_migrate, volume_inspect, shard_repair, blob_delete]

Usage:
  background [flags]

Flags:
  -h, --help     display help

Sub Commands:
  disable  disable background task
  enable   enable background task
  status   show status of a background task switch

```

```bash
blobstore-cli cm background status balance # 查看`balance`后台任务开关状态
blobstore-cli cm background enable balance # 打开`balance`后台任务开关
blobstore-cli cm background disable balance # 关闭`balance`后台任务开关
```

## Scheduler

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

**Scheduler Checkpoint**

```bash
inspect checkpoint tools for scheduler

Usage:
  checkpoint [flags]

Sub Commands:
  get  get inspect checkpoint
  set  set inspect checkpoint
```

**Scheduler Kafka**

```bash
kafka consume tools for scheduler

Usage:
  kafka [flags]

Sub Commands:
  get  get kafka consume offset
  set  set kafka consume offset
```

**Scheduler Migrate**

```bash
migrate tools for scheduler

Usage:
  migrate [flags]

Sub Commands:
  add       add manual migrate task
  disk      get migrating disk
  get       get migrate task
  list      list migrate tasks
  progress  show migrating progress
```
