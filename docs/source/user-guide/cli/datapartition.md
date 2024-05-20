# Data Partition Management

## Show Data Partition Information

Get information of the specified data partition.

```bash
cfs-cli datapartition info [Partition ID]
```

## Decommission Data Partition

Decommission the specified data partition on the target node and automatically transfer it to other available nodes.

```bash
cfs-cli datapartition decommission [Address] [Partition ID]
```

## Add Data Partition

Add a new data partition on the target node.

```bash
cfs-cli datapartition add-replica [Address] [Partition ID]
```

## Delete Data Partition

Delete the data partition on the target node.

```bash
cfs-cli datapartition del-replica [Address] [Partition ID]
```

## Fault Diagnosis

Fault diagnosis, find data partitions that are mostly unavailable and missing.

```bash
cfs-cli datapartition check
```

## Mark data partition discard

```bash
cfs-cli datapartition set-discard [DATA PARTITION ID] [DISCARD]
```