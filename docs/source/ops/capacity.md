# User Capacity Management

## Expand Volume Space

```bash
$ cfs-cli volume expand {volume name} {capacity / GB}
```

This interface is used to increase the volume capacity space.

::: tip Note
DpReadOnlyWhenVolFull can strictly limit volume capacity.

How to configure the configuration to stop writing when the volume is full:
(1)master
When creating the volume, set the DpReadOnlyWhenVolFull parameter to true;
If the value of the created volume is false, use the update interface to change it to true.
After this value is set to true, when the volume is full, the master will change the status of all DPs of the volume to readonly.
(2)client
Upgrade the client and set the "minWriteAbleDataPartitionCnt" parameter in the client's configuration to 0.
:::

## Volume Read and Write Performance Optimization

The more readable and writable data partitions (DPs), the more dispersed the data, and the better the read and write performance of the volume.

CubeFS adopts a dynamic space allocation mechanism. After creating a volume, a certain number of data partition DPs will be pre-allocated for the volume. When the number of readable and writable DPs is less than 10, the number of DPs will be automatically increased. If you want to manually increase the number of readable and writable DPs, you can use the following command:

```bash
$ cfs-cli volume create-dp {volume name} {number}
```

::: tip Note
The default size of a DP is 120GB. Please create DPs based on the actual usage of the volume to avoid overdraw of all DPs.
:::

## Recycle Excess Volume Space

```bash
$ cfs-cli volume shrink {volume name} {capacity in GB}
```

This interface is used to reduce the volume capacity space. It will be calculated based on the actual usage. If the set value is less than 120% of the used capacity, the operation will fail.

## Cluster Space Expansion

Prepare new data nodes (DNs) and metadata nodes (MNs), and configure the existing master address in the configuration file to automatically add the new nodes to the cluster.