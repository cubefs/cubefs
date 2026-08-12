# Tag-Based MP and DP Migration

::: warning Note
From v3.6.0, CubeFS supports automatically migrating MetaPartition and DataPartition replicas according to node tags.
:::

Tag-based migration separates replica placement from physical topology. Administrators assign tags to MetaNodes and DataNodes, define mappings from old tags to target tags, and let the Master reconcile replicas whose recorded tags no longer match the configured policy.

This feature is useful for hardware replacement, resource-group migration, or gradual node retirement. It is independent of metadata Region policies and storage-pool placement.

## Assign Tags to Nodes

A node tag must contain 1–49 letters or digits.

```bash
./cfs-cli datanode update 10.0.0.10:17310 oldData
./cfs-cli metanode update 10.0.0.20:17210 oldMeta
```

Updating a node tag does not immediately move replicas. Migration starts only after a mapping rule is configured and automatic tag repair is enabled.

## Configure Mapping Rules

A mapping rule uses the format `source->target`. Multiple replica tags can be mapped in one rule, and multiple rules are separated by semicolons.

```text
oldData->newData
oldA,oldA->newA,newA;oldB->newB
```

Each rule must contain the same number of source and target tags. A configuration may contain at most three rules, and source and target tags must not overlap.

Configure cluster-wide defaults:

```bash
./cfs-cli cluster set \
    --autoMigrateByTag true \
    --dpTagMapRules "oldData->newData" \
    --mpTagMapRules "oldMeta->newMeta" \
    --maxDpTagDecommissionLimit 100 \
    --maxMpTagDecommissionLimit 5
```

Volume-level rules override cluster defaults:

```bash
./cfs-cli vol update test \
    --dpTagMapRules "oldData->newData" \
    --mpTagMapRules "oldMeta->newMeta"
```

Use `"null"` as the rule value to clear a mapping.

## Migration Workflow

The Master periodically checks tag consistency after `autoMigrateByTag` is enabled.

For DataPartitions:

1. The Master compares each replica's recorded tag with its DataNode tag.
2. A mismatched replica is marked for tag decommissioning.
3. A replacement replica is selected according to the target tag.
4. Normal DataPartition repair and decommissioning complete the migration.

For MetaPartitions:

1. The Master collects replicas that do not match the effective MetaPartition tag rule.
2. A target MetaNode is selected by tag and metadata storage mode.
3. A learner replica is added and catches up with the Raft group.
4. The old replica is removed after the learner is ready.

MetaPartition migration therefore requires learner-based decommissioning:

```bash
./cfs-cli cluster set --enableMpDecommissionByLearner true
```

## Monitor Migration

Inspect cluster-wide or volume-level progress:

```bash
./cfs-cli tag summary --all
./cfs-cli tag summary --meta --all
./cfs-cli tag summary --data --all
./cfs-cli tag vol-summary test --all
```

The summary includes effective rules, mismatched partitions, active decommission tasks, failed keys, and scheduler status. After resolving a persistent failure, clear recorded failed keys so they can be retried:

```bash
./cfs-cli tag clear-failed-keys
```

## Operational Considerations

- Volume-level rules take precedence over cluster-level defaults.
- DataPartition tag migration requires volumes with at least three replicas.
- The default concurrency limits are 100 DataPartition tasks and 5 MetaPartition tasks.
- Setting a concurrency limit to `0` restores its default value.
- Check target-tag capacity and node health before enabling automatic migration.
- Change rules in stages for large clusters and monitor recovery traffic throughout the migration.
