# Meta Partition Management

## Show Meta Partition Information

Get information of the specified meta Partition.

```bash
cfs-cli metapartition info [Partition ID]
```

## Decommission Meta Partition

Decommission the specified meta partition on the target node and automatically transfer it to other available nodes.

```bash
cfs-cli metapartition decommission [Address] [Partition ID]
```

## Add Meta Partition

Add a new meta partition on the target node.

```bash
cfs-cli metapartition add-replica [Address] [Partition ID]
```

## Delete Meta Partition

Delete the meta partition on the target node.

```bash
cfs-cli metapartition del-replica [Address] [Partition ID]
```

## Fault Diagnosis

Fault diagnosis, find meta partitions that are mostly unavailable and missing.

```bash
cfs-cli metapartition check
```
