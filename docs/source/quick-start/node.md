# Standalone Mode

## Get Software
``` bash
# Skip this step if it has been completed
$ git clone https://github.com/cubefs/cubefs.git
```
## Install Software

### Script Installation

#### Install Basic Cluster
CubeFS supports deploying a basic cluster with a single script. The basic cluster includes components such as `Master`, `MetaNode`, and `DataNode`, with the option to additionally start a `client` and `ObjectNode`. The steps are as follows:
```bash
cd ./cubefs
# Compile
make
# Generate configuration files and start the basic cluster
sh ./shell/deploy.sh /home/data bond0

# Wait a minute for cluster to prepare state, and optionally execute the following command

# Mount the file system (optional, execute this command if you want to experience file storage. By default, it is mounted at /home/data/client/mnt)
sh ./shell/deploy_client.sh /home/data

# Start object storage (optional, execute this command if you want to experience object storage. The default listening port is 17410)
sh ./shell/deploy_object.sh /home/data
```
+ `bond0`: The name of the local network card, fill in according to the actual situation
+ `/home/data`: A local directory used to store cluster running logs, data, and configuration files. The directory should be the same for all three `sh` commands.
+ Machine requirements
  + Need root permission
  + Able to use `ifconfig`
  + Memory of 4G or more
  + Remaining disk space corresponding to `/home/data` is more than 20G
+ Check the cluster status
```bash
./build/bin/cfs-cli cluster info
[Cluster]
  Cluster name       : cfs_dev
  Master leader      : 172.16.1.101:17010
  Master-1           : 172.16.1.101:17010
  Master-2           : 172.16.1.102:17010
  Master-3           : 172.16.1.103:17010
  Auto allocate      : Enabled
  MetaNode count (active/total)    : 4/4
  MetaNode used                    : 0 GB
  MetaNode available               : 21 GB
  MetaNode total                   : 21 GB
  DataNode count (active/total)    : 4/4
  DataNode used                    : 44 GB
  DataNode available               : 191 GB
  DataNode total                   : 235 GB
  Volume count       : 2
...
```

For using the file system, refer to the [File System Usage section](../user-guide/file.md).

For using object storage, refer to the [Object Storage Usage section](../user-guide/objectnode.md).

#### Install Erasure Code Subsystem

::: tip Note
Optional section. If you need to use the erasure-coded volume, you need to deploy it.
:::

``` bash
$> cd cubefs/blobstore
$> ./run.sh --consul
...
start blobstore service successfully, wait minutes for internal state preparation
$>
```

After the erasure code subsystem is deployed successfully, modify the `ebsAddr` configuration item in the Master configuration file ([more configuration reference](../maintenance/configs/master.md)) to the Consul address registered by the Access node, which is `http://localhost:8500` by default.

#### Stop the Cluster
+ The script will stop the server and the mount point
```bash
sh ./shell/stop.sh
```

### Docker Installation

#### Install Basic Cluster
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


#### Install Erasure Code Subsystem

::: warning Note
The erasure code docker deployment method has not been unified with other modules (such as Master) for the time being. This section is currently only used to experience the function of the erasure code subsystem itself, and will be improved later.
:::

Support the following docker image deployment methods:

- Remote pull build [`recommended`]

``` bash
$> docker pull cubefs/cubefs:blobstore-v3.3.0 # Pull the image
$> docker run cubefs/cubefs:blobstore-v3.3.0 # Run the image
$> docker container ls # View running containers
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.3.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # Enter the container
```

- Local script compilation and build

``` bash
$> cd blobstore
$> ./run_docker.sh -b # Compile and build
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.3.0
$> ./run_docker.sh -r # Run the image
$> ... # The subsequent steps are the same as those for remote pull build
```
