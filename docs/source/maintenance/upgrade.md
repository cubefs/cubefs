# System Upgrade

::: warning Note
Please refer to the release notes for the upgrade steps of each version.
:::

## Get Binary
+ Get the binary of the specified version. Obtain the code of the specified version from https://github.com/cubeFS/cubefs/releases, and compile to generate the binary.

## Freeze the Cluster

```
$ cfs-cli cluster freeze true
```
+ During the freeze period, new data partitions cannot be created for the volume, but it does not affect ongoing business processes.
## Notes
- Confirm the startup configuration file and do not change important information such as the data directory and port in the configuration file.
    - For other parameter modifications in the configuration file, refer to the configuration instructions, release notes, etc.
- Upgrade order of each component, refer to the corresponding version [release notes](https://github.com/cubefs/cubefs/releases)
    - If there are no special requirements, the components can generally be upgraded in the order of datanode->metanode->master->client.

## Upgrade DataNode
The following describes the upgrade process for datanode.
1. Stop the old datanode process.
2. Start the new datanode process.
3. After starting, check the node status. Upgrade the next machine after it is displayed as active.
```
$ cfs-cli datanode info 192.168.0.33:17310
[Data node info]
 ID                  : 9
 Address             : 192.168.0.33:17310
 Carry               : 0.06612836801123345
 Used ratio          : 0.0034684352702178426
 Used                : 96 GB
 Available           : 27 TB
 Total               : 27 TB
 Zone                : default
 IsActive            : Active
 Report time         : 2020-07-27 10:23:20
 Partition count     : 16
 Bad disks           : []
 Persist partitions  : [2 3 5 7 8 10 11 12 13 14 15 16 17 18 19 20]
```

## Upgrade MetaNode

Similar to DataNode.

## Upgrade Master

1. Stop the old master process.
2. Start the new master process.
3. Observe whether the monitoring is normal.
4. Check whether the corresponding raft status of the master is normal.
    - As follows, check whether the commit corresponding to the restarted master ID is consistent with other replicas, and whether the raft has a leader.
```shell
curl 192.168.0.1:17010/get/raftStatus | python -m json.tool
{
    "code": 0,
    "data": {
        "AppQueue": 0,
        "Applied": 25168073,
        "Commit": 25168074,
        "ID": 1,
        "Index": 25168074,
        "Leader": 2,
        "NodeID": 2,
        "Replicas": {
            "1": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
            },
            "2": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
                "Paused": false,
            },
            "3": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
            }
        },
        "RestoringSnapshot": false,
        "State": "StateLeader",
        "Stopped": false,
        "Term": 292,
        "Vote": 2
    },
    "msg": "success"
}
```
## Upgrade Client

1. Stop business read and write operations.
2. Unmount the mount point.
    - If the following error occurs, execute `umount -l mount point`.
```
umount: /xxx/mnt: target is busy.
        (In some cases useful info about processes that use
         the device is found by lsof(8) or fuser(1)
```
3. If the client process exists, stop it.
4. Start the new client process. Use `df -h` to check whether the upgrade is successful.
