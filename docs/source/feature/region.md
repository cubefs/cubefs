# Multi-Region Metadata

::: warning Note
From v3.6.0, CubeFS supports organizing metadata resources by Region and placing MetaPartitions according to volume-level Region policies.
:::

A Region is a logical group of metadata zones. It allows one cluster to manage metadata resources in multiple locations while keeping the placement policy explicit. A volume can select a default Region, allow additional Regions, and maintain learner replicas in other Regions.

Region applies to MetaNodes and MetaPartitions. Data placement is managed independently through storage pools and zones.

## Configure Metadata Regions

Set `region` and `zoneName` in each MetaNode configuration. When the first MetaNode joins a zone, the zone is associated with that Region. MetaNodes subsequently added to the same zone must use the same Region.

```json
{
    "zoneName": "az-east-1",
    "region": "east"
}
```

If `region` is omitted, CubeFS uses the `default` Region.

Region names must be 3–32 characters long, start with a letter, end with a letter or digit, and contain only letters, digits, underscores, or hyphens.

Use the CLI to inspect the resulting topology:

```bash
./cfs-cli region list
./cfs-cli region info east
./cfs-cli zone list
```

Regions are created implicitly from registered metadata zones; no separate Region creation operation is required.

## Configure a Volume

Set the cluster default metadata Region when required:

```bash
./cfs-cli cluster setDefaultMetaRegion east
```

Create a volume in a specified default Region:

```bash
./cfs-cli vol create test root --default-region east
```

The default Region is automatically included in the volume's allowed Region list. Add another Region before using it for the volume:

```bash
./cfs-cli vol addRegion test west
./cfs-cli vol updateDefaultRegion test west
```

The target Region must exist in one of the zones allowed by the volume. Adding a second Region also enables cross-zone placement for the volume.

Clients may select an allowed metadata Region through the `metaRegion` mount option. If it is not specified, the volume's default Region is used.

## Configure Cross-Region Learners

MetaPartition Region policies can maintain learner replicas in other Regions. Enable learner-based MetaPartition decommissioning before configuring a policy:

```bash
./cfs-cli cluster set --enableMpDecommissionByLearner true
./cfs-cli vol updateMpRegionPolicy test \
    --region east \
    --policy "west:rocksdb"
```

The policy is defined for a source Region. Each target uses the format `targetRegion:storeMode`, where `storeMode` is `rocksdb` or `memory`. Multiple targets are separated by semicolons.

```bash
./cfs-cli vol updateMpRegionPolicy test \
    --region east \
    --policy "west:rocksdb; north:memory"

./cfs-cli vol mpRegionPolicy test
```

The source and target Regions must differ, and every target must already be in the volume's allowed Region list. Use `--policy "empty"` to clear the policy for a source Region.

## How It Works

- New MetaPartitions are created in the requested or default Region.
- CubeFS independently checks writable MetaPartition capacity in every allowed Region and expands partitions when needed.
- The Master periodically reconciles cross-Region learner replicas with the configured policy.
- After a metadata update, clients temporarily read from the leader to preserve read-your-writes consistency when NearRead or follower reads are enabled.

Region policies improve metadata locality and resilience, but they do not replace network-latency planning or capacity provisioning for each Region.
