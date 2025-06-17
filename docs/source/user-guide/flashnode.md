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
./cfs-cli flashgroup create 
```

Use the flashgroup set command in the CLI tool to enable the FlashGroup, specifying the unique ID that was assigned during its creation.

```bash
// After creation, the FlashGroup status is inactive. You must activate it explicitly.
./cfs-cli flashgroup set 13 true
```

### flashGroup add flashNode

To associate cache nodes with a newly created FlashGroup, use the flashgroup nodeAdd command provided by the CLI tool.

```bash
// To add a FlashNode to a FlashGroup, specify the unique ID of the FlashGroup and the identifier of the FlashNode that you want to associate with the group.
./cfs-cli flashgroup nodeAdd 13 --zone-name=default --addr="*.*.*.*:18510"
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