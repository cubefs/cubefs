# DataNode Management

## List DataNodes

Get information of all dataNodes, including ID, address, read/write status, and survival status.

```bash
cfs-cli datanode list
```

## Show DataNode Information

Show basic information of the dataNode, including status, usage, and the ID of the partition it carries.

```bash
cfs-cli datanode info [Address]
```

## Decommission DataNode

Decommission the dataNode. The data partitions on this node will be automatically transferred to other available nodes.

```bash
cfs-cli datanode decommission [Address]
```

## Transfer Data Partitions

Transfer the data partition on the source dataNode to the target dataNode.

```bash
cfs-cli datanode migrate [srcAddress] [dstAddress]
```