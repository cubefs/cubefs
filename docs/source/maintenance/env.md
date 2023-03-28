# Environment and Capacity Planning

## Environment Requirements

The following table lists the system and hardware requirements for performance testing environments and production environments. You can also refer to the Capacity Planning section to customize deployment plans based on your actual cluster capacity planning.

::: warning Note
Because DataNode uses the Linux kernel's Punch Hole function, the corresponding kernel version needs to be selected according to the DataNode file system.
:::

| File System | Kernel Version Requirement |
|-------------|----------------------------|
| XFS         | \>=2.6.38                  |
| ext4        | \>=3.0                     |
| Btrfs       | \>=3.7                     |
| tmpfs(5)    | \>=3.5                     |
| gfs2(5)     | \>=4.16                    |

To speed up metadata reading, metadata is stored in memory, while DataNode data mainly occupies disk resources. If you want to maximize the use of node resources, you can use the method of mixed deployment of DataNode and MetaNode on the same node.

## Capacity Planning

First, you need to estimate the highest expected number of files and storage capacity for the cluster for a considerable period in the future.

Second, you need to have a clear understanding of the machine resources currently available. Know the memory size, CPU core count, and disk capacity of each machine.

If you have a clear understanding of the above data, you can use the reference values provided in the second section to see which scale your current environment belongs to, what volume of files it can support, or how much resources you need to prepare for the current file experience requirements to prevent frequent expansion of machine resources.

| Total Number of Files | Total Storage Capacity | Total Cluster Memory | Total Cluster Disk Space |
|-----------------------|------------------------|----------------------|--------------------------|
| 1 billion             | 10 PB                  | 2048 GB              | 10 PB                    |

::: tip Note
The higher the proportion of large files, the greater the pressure on MetaNode.
:::

Of course, if you think that the current resources are sufficient and there is no need to meet the capacity growth requirements at once, you can pay attention to the capacity warning information of MetaNode/DataNode in a timely manner.

::: tip Recommendation
When the memory or disk is about to be used up, dynamically increase MetaNode/DataNode to adjust the capacity. That is, if the disk space is insufficient, you can increase the disk or DataNode. If all MetaNode memory is too full, you can increase MetaNode to relieve memory pressure.
:::

## Multi-Data Center Deployment

If you want the cluster to support data center fault tolerance, you can deploy a CubeFS cluster across data centers.

At the same time, pay attention to the following points:
- Because the communication latency between data centers is higher than that within a data center, if the requirement for high availability is greater than that for low latency, you can choose a cross-data center deployment plan.
- If higher performance is required, it is recommended to deploy the cluster within a data center.

Configuration plan: Modify the `zoneName` parameter in the DataNode/MetaNode configuration file to specify the name of the data center, and then start the DataNode/MetaNode process. The data center will be stored and recorded by the master as DataNode/MetaNode registers.

Create a single-data center volume:

``` bash
$ cfs-cli volume create {name} --zone-name={zone}
```

::: tip Note
To prevent the initialization of a single-data center volume from failing, ensure that there are at least three DataNode/MetaNode nodes in a single data center.
:::

Create a cross-data center volume:

``` bash
$ cfs-cli volume create {name} --cross-zone=true
```