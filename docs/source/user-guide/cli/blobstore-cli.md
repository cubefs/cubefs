# Using blobstore-cli Tool

::: tip Note
Currently, the blobstore-cli of the erasure coding subsystem is not perfect enough, and the function coverage is less than 70%. It will be continuously improved in the future, and eventually, 100% coverage of the interface functions of each module in the cluster will be achieved.
:::

- Command-line automatic completion function
- Display as a readable data type

## Compilation and Configuration

Use `./bin/blobstore-cli -c cli/cli/cli.conf` to start the command-line tool. `-c cli/cli/cli.conf` is an optional configuration item, which mainly configures some commonly used variables, such as the access discovery address of the access layer service and the clustermgr service address.

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

## Usage

The blobstore-cli can be used as a regular command, such as:

```bash
blobstore-cli MainCmd SubCmd [Cmd ...] [--flag Val ...] -- [-arg ...]

1 #$> ./blobstore-cli config set conf-key conf-val
To set Key: conf-key Value: conf-val

2 #$> ./blobstore-cli util time
timestamp = 1640156245364981202 (seconds = 1640156245 nanosecs = 364981202)
        --> format: 2021-12-22T14:57:25.364981202+08:00 (now)
```

Use `./bin/blobstore-cli` to start the command line.

```text
help can be used to view all commands and brief descriptions.
It is recommended to use the `cmd subCmd ... --flag -- -arg` method to pass parameters.
```

Currently, the main functions of some modules have been implemented, as follows:

| Command                  | Description                                                                                        |
|--------------------------|----------------------------------------------------------------------------------------------------|
| blobstore-cli config     | Manage the configuration items in the memory of the blobstore-cli.                                 |
| blobstore-cli util       | A collection of small tools, such as parsing location, parsing time, and generating specific data. |
| blobstore-cli access     | Upload, download, delete files, etc.                                                               |
| blobstore-cli cm         | View and manage cluster information, and background task switch control.                           |
| blobstore-cli scheduler  | Manage background tasks.                                                                           |
| blobstore-cli \...       | In progress...                                                                                     |

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

Enable or disable `balance` background task as following:

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
blobstore-cli cm background status balance # check `balance` background task switch status
blobstore-cli cm background enable balance # enable `balance` background task
blobstore-cli cm background disable balance # disable `balance` background task
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
