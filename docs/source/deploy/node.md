# Standalone Mode

## Get Software
``` bash
# Skip this step if it has been completed
$ git clone https://github.com/cubefs/cubefs.git
```

## Install Basic Cluster

CubeFS supports one-click deployment of the basic cluster using scripts, including components such as `Master`, `MetaNode`, and `DataNode`. The steps are as follows:

```bash
cd ./cubefs
# Compile
make
# Start the script
sh ./shell/depoly.sh /home/data bond0
```
+ `bond0`: The name of the local network card, fill in according to the actual situation
+ `/home/data`: A local directory used to store cluster running logs, data, and configuration files
+ Machine requirements
  + Need root permission
  + Able to use `ifconfig`
  + Memory of 4G or more
  + Remaining disk space corresponding to `/home/data` is more than 20G

## Check the cluster status

```bash
./build/bin/cfs-cli cluster info
[Cluster]
  Cluster name       : cfs_dev
  Master leader      : 172.16.1.101:17010
  Auto allocate      : Enabled
  MetaNode count     : 4
  MetaNode used      : 0 GB
  MetaNode total     : 21 GB
  DataNode count     : 4
  DataNode used      : 191 GB
  DataNode total     : 369 GB
  Volume count       : 2
...
```

::: tip Note
Basic clusters provide the basic functions of distributed storage, but usually do not directly provide external services, and need to be accessed through **clients** or **object nodes**. Here we use client as an example.:::
:::

## Create a Volume

```bash
./build/bin/cfs-cli volume create ltptest ltp
# View volume information
./build/bin/cfs-cli volume info ltptest
```

## Start the Client and Mount the Volume

```bash
./build/bin/cfs-client -c /home/data/conf/client.conf
```

## Verify File System Mounting

`/home/cfs/client/mnt` is the mount point. If the output of the command `df -h` is similar to the following, the mount is successful.
```bash
df -h
Filesystem      Size  Used Avail Use% Mounted on
udev            3.9G     0  3.9G   0% /dev
tmpfs           796M   82M  714M  11% /run
/dev/sda1        98G   48G   45G  52% /
tmpfs           3.9G   11M  3.9G   1% /dev/shm
cubefs-ltptest   10G     0   10G   0% /home/cfs/client/mnt
...
```

## Install Object Notes (Optional)

::: tip Note
Optional section. If you need to use the object storage service, you need to deploy the object gateway (ObjectNode).
:::

Refer to [Object Storage Section](../user-guide/objectnode.md)

## Stop the Cluster
+ The script will stop the server and the mount point
```bash
sh ./shell/stop.sh
```

## Install Erasure Code Subsystem (Optional)

::: tip Note
The following are optional sections. If you need to use the erasure-coded volume, you need to deploy it.
:::

``` bash
$> cd cubefs/blobstore
$> ./run.sh --consul
...
start blobstore service successfully, wait minutes for internal state preparation
$>
```

After the erasure code subsystem is deployed successfully, modify the `ebsAddr` configuration item in the Master configuration file ([more configuration reference](../maintenance/configs/master.md)) to the Consul address registered by the Access node, which is `http://localhost:8500` by default.

## Start the blobstore-cli

::: tip Note
The erasure coding subsystem (Blobstore) provides a separate interactive command-line management tool. Currently, this tool is not integrated into cfs-cli, and it will be integrated later.
:::

The Blobstore CLI can easily manage the erasure coding subsystem. Use `help` to view the help information. Here, we only introduce how to verify the correctness of the erasure coding system itself.

Based on the default configuration, start the command-line tool `cli`. For detailed usage, please refer to [CLI Tool User Guide](../maintenance/tool.md).

``` bash
$> cd ./cubefs
$>./build/bin/blobstore/blobstore-cli -c blobstore/cli/cli/cli.conf # Start the command line with the default configuration
```

## Verify Erasure Code Subsystem

``` bash
# Upload a file, and a location will be returned after success (-d parameter is the actual content of the file)
$> access put -v -d "test -data-"
# Return result
#"code_mode":11 is the encoding mode specified in the clustermgr configuration file, and 11 is the EC3P3 encoding mode.
{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

# Download the file, use the location obtained above as the parameter (-l), and you can download the file content
$> access get -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

# Delete the file, use the location obtained above as the parameter (-l); deleting the file requires manual confirmation
$> access del -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'
```