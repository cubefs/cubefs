# Create Volume

A **volume** is a logical concept composed of multiple metadata and data shards.

- From the client's perspective, a volume can be seen as a file system instance that can be accessed by containers. A volume can be mounted in multiple containers, allowing files to be accessed by different clients simultaneously.
- From the perspective of object storage, a volume corresponds to a bucket.

The following will introduce how to create volumes in different modes. For more volume operations, please refer to [Volume Management Commands](../maintenance/admin-api/master/volume.md).

## Create Replicated Volume

Request the master service interface to create a volume.

- name: volume name
- capacity: volume quota. If the quota is used up, it needs to be expanded.
- owner: the owner of the volume. If there is no user with the same name as the owner in the cluster, a user with the user ID of Owner will be created automatically.

``` bash
curl -v "http://127.0.0.1:17010/admin/createVol?name=test&capacity=100&owner=cfs"
```

::: tip Tip
Example of expanding the volume (where authKey is the MD5 value of the volume owner string).
```shell
curl -v "http://127.0.0.1:17010/vol/expand?name=test&authKey=57f0162b2303be3449c7008484b0d306&capacity=200"
```
:::

## Create Erasure-Coded Volume

If the Blobstore erasure-coded subsystem is deployed, you can create an erasure-coded volume to store cold data.

```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1'
```

- name: volume name
- capacity: volume quota. If the quota is used up, it needs to be expanded.
- owner: the owner of the volume.
- volType: volume type. 0 for replicated volume, 1 for erasure-coded volume. The default is 0.

## Enable Multi-Level Cache

You can create an erasure-coded volume and set multiple replicas as read-write cache.

**Cache read data**
```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1&cacheCap=10&cacheAction=1'
```

- name: volume name
- capacity: volume quota. If the quota is used up, it needs to be expanded.
- owner: the owner of the volume.
- volType: volume type. 0 for replicated volume, 1 for erasure-coded volume. The default is 0.
- cacheCap: cache size in GB.
- cacheAction: cache type. 0 for no cache, 1 for cache read, 2 for cache read-write. The default is 0.

**Cache read and write data**

```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1&cacheCap=10&cacheAction=2'
```

- name: volume name
- capacity: volume quota. If the quota is used up, it needs to be expanded.
- owner: the owner of the volume.
- volType: volume type. 0 for replicated volume, 1 for erasure-coded volume. The default is 0.
- cacheCap: cache size in GB.
- cacheAction: cache type. 0 for no cache, 1 for cache read, 2 for cache read-write. The default is 0.