# Multiple Data Storage Pools

::: warning Note
From v3.6.0, CubeFS supports assigning data zones and volumes to multiple storage pools.
:::

A storage pool is a logical group of data resources with a defined storage class. It provides a placement boundary above zones, allowing one cluster to isolate capacity, organize hardware, and manage DataPartitions across multiple pools.

Storage pools complement storage classes. A storage class describes the media type, such as ReplicaSSD or ReplicaHDD, while a pool identifies the resource group in which DataPartitions are placed.

## Default Storage Pools

CubeFS provides three reserved pools:

- Pool `1`: `defaultSSDPool`, using ReplicaSSD.
- Pool `2`: `defaultHDDPool`, using ReplicaHDD.
- Pool `3`: `defaultECPool`, using BlobStore.

Custom pool IDs range from 4 to 255. Custom ReplicaSSD and ReplicaHDD pools can be created through the CLI. Creating custom BlobStore pools is not currently supported.

List the available pools:

```bash
./cfs-cli pool list
./cfs-cli pool info 1
```

## Create a Storage Pool

Create a custom replica pool before assigning DataNodes to it:

```bash
./cfs-cli pool create \
    --id 20 \
    --name pool-hdd-2 \
    --storageClass 2
```

`storageClass` accepts `1` for ReplicaSSD and `2` for ReplicaHDD. Pool names must be 3–32 characters long, start with a letter, and contain only letters, digits, underscores, or hyphens.

Set `poolId` in the DataNode configuration:

```json
{
    "zoneName": "az-hdd-2",
    "mediaType": 2,
    "poolId": 20
}
```

When the first DataNode joins a zone, the zone is associated with that pool. All DataNodes subsequently added to the same zone must use the same pool ID.

## Configure a Volume

Create a volume with a default pool and a list of allowed pools:

```bash
./cfs-cli vol create test root \
    --poolId 1 \
    --pools "1,20"
```

When multiple pools are specified, cross-zone placement is enabled automatically. Every allowed pool must be available through one of the volume's configured zones.

Pools can also be added to an existing volume:

```bash
./cfs-cli vol addPool test 20
./cfs-cli vol updatePoolId test 20 --poolName pool-hdd-2
```

The default pool must be included in the volume's allowed pool list. Before changing the default pool, ensure that it has enough writable DataPartitions and available capacity.

## How It Works

- Each DataPartition records the pool in which it is placed.
- The Master maintains writable DataPartition capacity independently for every allowed pool.
- Topology selection filters candidate zones and DataNodes by pool ID.
- Capacity usage and quotas can be accounted for by pool.
- The cluster and volume default pools determine placement when a request does not explicitly select a pool.

Multiple storage pools can isolate workloads or hardware groups within the same storage class. They do not migrate existing data automatically; use lifecycle rules or operational migration workflows when data movement is required.

::: warning Limitation
Multiple replica pools cannot be configured for a volume when FaultDomain mode is enabled.
:::
