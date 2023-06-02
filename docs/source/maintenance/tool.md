# Using CLI Tools

Using the command-line interface tool (CLI) can achieve convenient and fast cluster management. With this tool, you can view the status of the cluster and each node, and manage each node, volume, and user.

::: tip Note
With the continuous improvement of the CLI, 100% coverage of the interface functions of each node in the cluster will eventually be achieved.
:::

## Compilation and Configuration

After downloading the CubeFS source code, run the `build.sh` file in the `cubefs/cli` directory to generate the `cfs-cli` executable.

At the same time, a configuration file named `.cfs-cli.json` will be generated in the `root` directory. Modify the master address to the master address of the current cluster. You can also use the `./cfs-cli config info` and `./cfs-cli config set` commands to view and set the configuration file.

## Usage

In the `cubefs/cli` directory, run the command `./cfs-cli --help` or `./cfs-cli -h` to get the CLI help document.

The CLI is mainly divided into six types of management commands:

| Command               | Description                                  |
|-----------------------|----------------------------------------------|
| cfs-cli cluster       | Cluster management                           |
| cfs-cli metanode      | Metadata node management                     |
| cfs-cli datanode      | Data node management                         |
| cfs-cli datapartition | Data partition management                    |
| cfs-cli metapartition | Metadata partition management                |
| cfs-cli config        | Configuration management                     |
| cfs-cli completion    | Generate automatic completion command script |
| cfs-cli volume, vol   | Volume management                            |
| cfs-cli user          | User management                              |

### Cluster Management

``` bash
./cfs-cli cluster info          # Get cluster information, including cluster name, address, number of volumes, number of nodes, and usage rate, etc.
./cfs-cli cluster stat          # Get usage, status, etc. of metadata and data nodes by region.
./cfs-cli cluster freeze [true/false]        # Freeze the cluster. After setting it to `true`, when the partition is full, the cluster will not automatically allocate new partitions.
./cfs-cli cluster threshold [float]     # Set the memory threshold for each MetaNode in the cluster. If the memory usage reaches this threshold, all the meta partition will be readOnly. [float] should be a float number between 0 and 1.
./cfs-cli cluster set [flags]    # Set the parameters of the cluster.
```

### MetaData Management

``` bash
./cfs-cli metanode list         # Get information of all metadata nodes, including ID, address, read/write status, and survival status.
./cfs-cli metanode info [Address]     # Show basic information of the metadata node, including status, usage, and the ID of the partition it carries.
./cfs-cli metanode decommission [Address] # Decommission the metadata node. The partitions on this node will be automatically transferred to other available nodes.
./cfs-cli metanode migrate [srcAddress] [dstAddress] # Transfer the meta partition on the source metadata node to the target metadata node.
```

### DataNode Management

``` bash
./cfs-cli datanode list         # Get information of all data nodes, including ID, address, read/write status, and survival status.
./cfs-cli datanode info [Address]     # Show basic information of the data node, including status, usage, and the ID of the partition it carries.
./cfs-cli datanode decommission [Address] # Decommission the data node. The data partitions on this node will be automatically transferred to other available nodes.
./cfs-cli datanode migrate [srcAddress] [dstAddress] # Transfer the data partition on the source data node to the target data node.
```

### Data Partition Management

``` bash
./cfs-cli datapartition info [Partition ID]        # Get information of the specified data partition.
./cfs-cli datapartition decommission [Address] [Partition ID]   # Decommission the specified data partition on the target node and automatically transfer it to other available nodes.
./cfs-cli datapartition add-replica [Address] [Partition ID]    # Add a new data partition on the target node.
./cfs-cli datapartition del-replica [Address] [Partition ID]    # Delete the data partition on the target node.
./cfs-cli datapartition check    # Fault diagnosis, find data partitions that are mostly unavailable and missing.
```

### Metadata Partition Management

``` bash
./cfs-cli metapartition info [Partition ID]        # Get information of the specified meta partition.
./cfs-cli metapartition decommission [Address] [Partition ID]   # Decommission the specified meta partition on the target node and automatically transfer it to other available nodes.
./cfs-cli metapartition add-replica [Address] [Partition ID]    # Add a new meta partition on the target node.
./cfs-cli metapartition del-replica [Address] [Partition ID]    # Delete the meta partition on the target node.
./cfs-cli metapartition check    # Fault diagnosis, find meta partitions that are mostly unavailable and missing.
```

### Configuration Management

``` bash
./cfs-cli config info     # Show configuration information.
./cfs-cli config set [flags] # Set configuration information.

Flags:
    --addr string      Specify master address [{HOST}:{PORT}]
-h, --help             help for set
    --timeout uint16   Specify timeout for requests [Unit: s]
```

### Automatic Completion Management

``` bash
./cfs-cli completion      # Generate command automatic completion script.
```

### Volume Management

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
./cfs-cli volume delete [VOLUME NAME] [flags]               # Delete the specified volume [VOLUME NAME]. The size of the ec volume must be 0 to be deleted.
Flags:
    -y, --yes                                           # Skip all questions and set the answer to "yes".
```

``` bash
./cfs-cli volume info [VOLUME NAME] [flags]                 # Get information of the volume [VOLUME NAME].
Flags:
    -d, --data-partition                                # Show detailed information of the data partition.
    -m, --meta-partition                                # Show detailed information of the metadata partition.
```

``` bash
./cfs-cli volume add-dp [VOLUME] [NUMBER]                   # Create and add [NUMBER] data partitions to the volume [VOLUME].
```

``` bash
./cfs-cli volume list                                       # Get a list of all current volumes.
```

``` bash
./cfs-cli volume transfer [VOLUME NAME] [USER ID] [flags]   # Transfer the volume [VOLUME NAME] to another user [USER ID].
Flags：
    -f, --force                                         # Force transfer.
    -y, --yes                                           # Skip all questions and set the answer to "yes".
```

``` bash
./cfs-cli volume update                                     # Update the parameters of the cluster.
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

### User Management

``` bash
./cfs-cli user create [USER ID] [flags]         # Create user [USER ID].
Flags：
    --access-key string                     # Specify the access key for the user to use the object storage function.
    --secret-key string                     # Specify the secret key for the user to use the object storage function.
    --password string                       # Specify the user password.
    --user-type string                      # Specify the user type, optional values are normal or admin (default is normal).
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

``` bash
./cfs-cli user delete [USER ID] [flags]         # Delete user [USER ID].
Flags：
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

``` bash
./cfs-cli user info [USER ID]                   # Get information of user [USER ID].
./cfs-cli user list                             # Get a list of all current users.
./cfs-cli user perm [USER ID] [VOLUME] [PERM]   # Update the permission [PERM] of user [USER ID] for volume [VOLUME].
                                            # [PERM] can be "READONLY/RO", "READWRITE/RW", or "NONE".
./cfs-cli user update [USER ID] [flags]         # Update the information of user [USER ID].
Flags：
    --access-key string                     # The updated access key value.
    --secret-key string                     # The updated secret key value.
    --user-type string                      # The updated user type, optional values are normal or admin.
    -y, --yes                               # Skip all questions and set the answer to "yes".
```

### Erasure Coding Subsystem Management

::: tip Note
Currently, the blobstore-cli of the erasure coding subsystem is not perfect enough, and the function coverage is less than 70%. It will be continuously improved in the future, and eventually, 100% coverage of the interface functions of each module in the cluster will be achieved.
:::

- Command-line automatic completion function
- Display as a readable data type

#### Compilation and Configuration

Compile the CLI tool of the erasure coding subsystem with `make cli`.

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

#### Usage

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
