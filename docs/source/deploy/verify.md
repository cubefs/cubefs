# CLI Verification

## Verify File System Mounting

### Create a Volume

::: tip Note
If you have deployed the erasure coding module to create an erasure coding volume, please refer to the [Volume Creation](../user-guide/volume.md) section.
:::

```bash
./build/bin/cfs-cli volume create ltptest ltp
# View volume information
./build/bin/cfs-cli volume info ltptest
```

### Start the Client

- Start the client
```bash
./build/bin/cfs-client -c /home/data/conf/client.conf
```

- Check if the mount is successful
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

## Verify the Erasure Coding Subsystem

::: tip Note
The erasure coding subsystem (Blobstore) provides a separate interactive command-line management tool. Currently, this tool is not integrated into cfs-cli, and it will be integrated later.
:::

The Blobstore CLI can easily manage the erasure coding subsystem. Use `help` to view the help information. Here, we only introduce how to verify the correctness of the erasure coding system itself.

### Start the CLI

Based on the default configuration, start the command-line tool `cli`. For detailed usage, please refer to [CLI Tool User Guide](../maintenance/tool.md).
1. Physical machine environment
``` bash
$> cd ./cubefs
$>./build/bin/blobstore/blobstore-cli -c blobstore/cli/cli/cli.conf # Start the command line with the default configuration
```
2. Docker environment
``` bash
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```

### Verification

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