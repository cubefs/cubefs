# Using Flash Cluster System

## Compilation and Building

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make server
```

### Launch FlashNode

``` bash
./cfs-server -c flashnode.json
```

Example `flashnode.json`, as follows:

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

For detailed configuration parameters, please refer to [FlashNode Detailed Configuration](../ops/configs/flashnode.md).

## Activate distributed caching through CLI commands

### Create and activate the FlashGroup

Use the following command to create a new FlashGroup. The system automatically generates a unique ID for the group.

```bash
// The default number of slots for a FlashGroup (FG) is 32.
./cfs-cli flashGroup create 
```

Use the flashgroup set command in the CLI tool to enable the FlashGroup, specifying the unique ID that was assigned during its creation.

```bash
// After creation, the FlashGroup status is inactive. You must activate it explicitly.
./cfs-cli flashGroup set 13 true
```

### flashGroup add flashNode

To associate cache nodes with a newly created FlashGroup, use the flashgroup nodeAdd command provided by the CLI tool.

```bash
// To add a FlashNode to a FlashGroup, specify the unique ID of the FlashGroup and the identifier of the FlashNode that you want to associate with the group.
./cfs-cli flashGroup nodeAdd 13 --zone-name=default --addr="*.*.*.*:18510"
```

After adding a FlashNode to the FlashGroup, use the flashnode list command to confirm that the node appears in the list. By default, a newly added FlashNode is enabled and active, with both the active and enable flags set to true.

```bash
./cfs-cli flashnode list
```

### Activate distributed caching on the volume

To enable distributed caching for a specific volume, use the vol update command in the CLI tool. By default, both newly created volumes and existing volumes present before a cluster upgrade are not configured with caching capabilities and must be explicitly enabled.

```bash
// To enable distributed caching for the existing volume named test, set the remoteCacheEnable parameter to true.
./cfs-cli vol update test --remoteCacheEnable true
```

To verify if distributed caching is enabled for a specific volume, use the vol info command in the CLI tool. The output will indicate the status of the remoteCacheEnable flag.

```bash
./cfs-cli vol info test
```
The remoteCacheEnable parameter controls whether distributed caching is active for the volume. When set to false, distributed caching is disabled; when set to true, it is enabled.


### Verify that distributed caching is effective

Mount a volume with distributed caching enabled.

Perform read operations on files under the volume’s root directory. Files smaller than 128 GB are automatically cached by default.

Check mem_stat.log on the client for FlashNode-related entries. The presence of these entries indicates that the client has accessed cached data from FlashNode, confirming that distributed caching is working.

# Using Distributed Cache as an Independent Service

::: tip 提示
Supported since v3.5.3.
:::

## Build

```bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make server
```

## Start flashGroupManager

```bash
./cfs-server -c flashGroupManager.json
```

Example `flashGroupManager.json`:

```json
{
  "clusterName": "cfs_ocs_accesstest",
  "id": "1",
  "role": "flashgroupmanager",
  "ip": "127.0.0.1",
  "peers": "1:127.0.0.1:21010,4:127.0.0.2:21010,5:127.0.0.3:21010",
  "listen": "21010",
  "prof": "21020",
  "retainLogs": "20000",
  "walDir": "/var/logs/cfs/master/wal",
  "storeDir": "/var/cfs/master/store",
  "logLevel": "debug",
  "logDir": "./logs",
  "heartbeatPort": 5991,
  "replicaPort": 5992
}
```

## Distributed Cache Configuration

For flashGroupManager, the configuration tool for distributed cache resources is `cfs-remotecache-config`. Its usage is identical to `cfs-cli` (used with Master).

For example, to create and enable a FlashGroup, use the same commands as with `cfs-cli`.

### Build the configuration tool

```bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make rcconfig
```

```bash
# The default number of FG slots is 32
./cfs-remotecache-config flashgroup create
```

## Using the RemoteCacheClient SDK

The RemoteCacheClient SDK provides read/write services for both the file storage engine and the object storage engine.

- For the file storage engine, the RemoteCacheClient SDK is integrated into `cfs-client`. Mount a volume with distributed cache enabled to use cached reads automatically.
- For the object storage engine, refer to `tool/remotecache-benchmark` in the CubeFS repository. Initialize RemoteCacheClient and invoke Put/Get to upload/download object blocks.

### RemoteCacheClient Configuration

ClientConfig supports the following parameters:

| Parameter              | Type   | Description                                                                                      | Required |
|------------------------|--------|--------------------------------------------------------------------------------------------------|----------|
| Masters                | string | Addresses of Master or FlashGroupManager (host:port)                                             | Yes      |
| BlockSize              | uint64 | Chunk size for cached data; files typically use 1MB; objects follow object block size            | Yes      |
| NeedInitLog            | bool   | Enable logging                                                                                   | No       |
| NeedInitStat           | bool   | Enable performance statistics                                                                     | No       |
| LogLevelStr            | string | Log level                                                                                        | No       |
| LogDir                 | string | Log directory                                                                                    | No       |
| ConnectTimeout         | int64  | Timeout to establish connection to FlashNode (ms)                                                | No       |
| FirstPacketTimeout     | int64  | First packet/first response timeout (ms)                                                         | No       |
| FromFuse               | bool   | Whether called by the file storage client (compatibility mode)                                   | Yes      |
| InitClientTime         | int    | Maximum SDK initialization timeout (s)                                                            | No       |
| DisableBatch           | bool   | Disable small-file batching (default batching threshold ≤ 16KB)                                   | No       |
| ActivateTime           | int64  | Maximum time interval for batching small-file uploads                                            | No       |
| ConnWorkers            | int    | Maximum number of concurrent connections for small-file batching                                 | No       |
| FlowLimit              | int64  | Client Get/Put bandwidth limit (bytes/s, default 5GB/s)                                         | No       |
| DisableFlowLimitUpdate | bool   | Disable Master-delivered bandwidth limits for client Get/Put                                     | No       |
| WriteChunkSize         | int64  | Chunk size per Put upload (bytes)                                                                | No       |