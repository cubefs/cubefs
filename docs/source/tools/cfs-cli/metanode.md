# MetaNode Management

## List MetaNodes

Get information of all metaNodes, including ID, address, read/write status, and survival status.

```bash
cfs-cli metanode list
```

## Show MetaNode Information

Show basic information of the metaNode, including status, usage, and the ID of the partition it carries.

```bash
cfs-cli metanode info [Address]     
```

## Decommission MetaNode

Decommission the metaNode. The partitions on this node will be automatically transferred to other available nodes.

```bash
cfs-cli metanode decommission [Address] 
```

## Transfer Meta Partitions

Transfer the meta partition on the source metaNode to the target metaNode.

```bash
cfs-cli metanode migrate [srcAddress] [dstAddress] 
```
