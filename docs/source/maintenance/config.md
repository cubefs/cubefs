# Cluster Configuration Modification

## Online Configuration Modification
Through the CLI tool, the following cluster configuration items can be modified online.

### Modify Volume Configuration
The help option of the cfs-cli subcommand volume lists the currently supported online configuration modification options.

```bash
cfs-cli volume -h

Usage:
  cfs-cli volume [command]

Aliases:
  volume, vol

Available Commands:
  add-dp      Create and add more data partition to a volume
  create      Create a new volume
  delete      Delete a volume from cluster
  expand      Expand capacity of a volume
  info        Show volume information
  list        List cluster volumes
  shrink      Shrink capacity of a volume
  transfer    Transfer volume to another user. (Change owner of volume)
  update      Update configuration of the volume
```

For example, the owner of a volume can be modified using the `cfs-cli volume transfer` command. For specific usage methods, you can use `cfs-cli volume transfer -h` to query.

### Adjusting Subsystem Log Levels
If the profPort port is enabled in the subsystem's configuration file, the log level can be modified through this port.
```bash
curl -v "http://127.0.0.1:{profPort}/loglevel/set?level={log-level}"
```
The currently supported log levels for `log-level` are `debug, info, warn, error, critical, read, write,` and `fatal`.

### Adjusting Erasure Coding Log Levels

This method is supported by all modules of the erasure coding system. [Refer to the details](./admin-api/blobstore/base.md).

| Level | Value |
|-------|-------|
| Debug | 0     |
| Info  | 1     |
| Warn  | 2     |
| Error | 3     |
| Panic | 4     |
| Fatal | 5     |

```bash
# The following sets the log level to warn
curl -XPOST -d 'level=2' http://127.0.0.1:9500/log/level
```

## Offline Configuration Modification
Other configuration items of subsystems in the cluster need to be modified by modifying the startup configuration file of the subsystem and then restarting it to take effect.

### Modify DataNode Reserved Space
In the DataNode configuration file, the number after the disk parameter is the reserved space, **in bytes**. After modification, **restart is required**.
```bash
{ ...
  "disks": [
   "/cfs/disk:10737418240" //10737418240 is the size of the reserved space
  ],
  ...
}
```

### Modify the Maximum Available Memory of MetaNode
The `totalMem` parameter in the MetaNode configuration file indicates the total available memory size of the metadata node. When the memory usage of the MetaNode exceeds this value, the MetaNode becomes read-only. Usually, this value should be less than the node memory. **If the metadata subsystem and the replica subsystem are deployed together, memory space needs to be reserved for the replica subsystem.**

### Modify DataNode/MetaNode Ports

::: danger Warning
It is not recommended to modify the ports of DataNode/MetaNode. Because DataNode/MetaNode is registered in the master through ip:port. If the port is modified, the master will consider it as a new node, and the old node will be in the inactive state.
:::

### Other Erasure Coding Configuration Modifications

Please refer to the [Service Configuration Introduction](./configs/blobstore/base.md) section.