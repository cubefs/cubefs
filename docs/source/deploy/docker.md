# Docker Installation

## Get Software
```bash
# Skip this step if it has been completed
$ git clone https://github.com/cubefs/cubefs.git
```

## Install Basic Cluster

In the docker directory, the run_docker.sh tool is used to facilitate running the CubeFS docker-compose trial cluster, including the `Master`, `MetaNode`, `DataNode`, and `ObjectNode` components.

::: tip Note
Please make sure that docker and docker-compose are installed, and before executing the docker deployment, make sure that the firewall is turned off to avoid permission issues causing container startup failures.
:::

Execute the following command to create a minimal CubeFS cluster.

::: warning Note
`/data/disk` is the data root directory and requires at least 10GB of free space.
:::

```bash
$ docker/run_docker.sh -r -d /data/disk
```

After the client is started, use the `mount` command in the client docker container to check the directory mounting status:

```bash
$ mount | grep cubefs
```

Open `http://127.0.0.1:3000` in a browser, log in with `admin/123456`, and you can view the Grafana monitoring indicator interface of CubeFS.

Or use the following command to run step by step:

```bash
$ docker/run_docker.sh -b
$ docker/run_docker.sh -s -d /data/disk
$ docker/run_docker.sh -c
$ docker/run_docker.sh -m
```

For more commands, please refer to the help:

```bash
$ docker/run_docker.sh -h
```
The Prometheus and Grafana related configurations for monitoring are located in the `docker/monitor` directory.

## Verify File System Mounting

`/cfs/mnt` is the mount point. If the output of the command `df -h` is similar to the following, the mount is successful.
```bash
df -h
Filesystem      Size  Used Avail Use% Mounted on
overlay          40G   11G   27G  28% /
tmpfs            64M     0   64M   0% /dev
/dev/vda1        40G   11G   27G  28% /cfs/bin
shm              64M     0   64M   0% /dev/shm
cubefs-ltptest   30G     0   30G   0% /cfs/mnt
```

## Install Erasure Code Subsystem (Optional)

::: warning Note
The erasure code docker deployment method has not been unified with other modules (such as Master) for the time being. This section is currently only used to experience the function of the erasure code subsystem itself, and will be improved later.
:::

Support the following docker image deployment methods:

- Remote pull build [`recommended`]

```bash
$> docker pull cubefs/cubefs:blobstore-v3.2.0 # Pull the image
$> docker run cubefs/cubefs:blobstore-v3.2.0 # Run the image
$> docker container ls # View running containers
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # Enter the container
```

- Local script compilation and build

```bash
$> cd blobstore
$> ./run_docker.sh -b # Compile and build
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.2.0
$> ./run_docker.sh -r # Run the image
$> ... # The subsequent steps are the same as those for remote pull build
```

### Start the blobstore-cli

::: tip Note
The erasure coding subsystem (Blobstore) provides a separate interactive command-line management tool. Currently, this tool is not integrated into cfs-cli, and it will be integrated later.
:::

The Blobstore CLI can easily manage the erasure coding subsystem. Use `help` to view the help information. Here, we only introduce how to verify the correctness of the erasure coding system itself.

Based on the default configuration, start the command-line tool `cli`. For detailed usage, please refer to [CLI Tool User Guide](../maintenance/tool.md).

```bash
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```

### Verification

```bash
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