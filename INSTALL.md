## Build CubeFS

Prerequisite:
* Go version `>=1.16`
* `export GO111MODULE=off`


### Build for x86
```
$ git clone http://github.com/cubefs/cubefs.git
$ cd cubefs
$ make
```
### Build for arm64 

For example,the current cubefs directory is /root/arm64/cubefs,build.sh will auto download  follow source codes to vendor/dep directory :
bzip2-1.0.6  lz4-1.9.2  zlib-1.2.11  zstd-1.4.5
  gcc version as  v4 or v5:
    ```
    cd /root/arm64/cubefs
    export CPUTYPE=arm64_gcc4 && bash ./build.sh
    ```

  gcc version as  v9 :
      ```
    export CPUTYPE=arm64_gcc9 && bash ./build.sh
      ```
#### Also support cross compiler with docker:

gcc version as  v4, support Ububtu 14.04 and up version,CentOS7.6 and up version. Check libstdc++.so.6 version must more than `GLIBCXX_3.4.19',if fail please update libstdc++. 

```
cd /root/arm64/cubefs
docker build --rm --tag arm64_gcc4_golang1_13_ubuntu_14_04_cubefs ./build/compile/arm64/gcc4

make dist-clean
docker run  -v /root/arm64/cubefs:/root/cubefs arm64_gcc4_golang1_13_ubuntu_14_04_cubefs /root/buildcfs.sh

```
 
Remove image:
```
docker image remove -f  arm64_gcc4_golang1_13_ubuntu_14_04_cubefs
```


## Yum Tools to Run a CubeFS Cluster for CentOS 7+

The list of RPM packages dependencies can be installed with:

```
$ yum install https://ocs-cn-north1.heytapcs.com/cubefs/rpm/3.0.0/cfs-install-3.0.0-el7.x86_64.rpm
$ cd /cfs/install
$ tree -L 2
.
├── install_cfs.yml
├── install.sh
├── iplist
├── src
└── template
    ├── client.json.j2
    ├── create_vol.sh.j2
    ├── datanode.json.j2
    ├── grafana
    ├── master.json.j2
    └── metanode.json.j2
```

Set parameters of the CubeFS cluster in `iplist`. 

1. `[master]`, `[datanode]`, `[metanode]`, `[monitor]`, `[client]` modules define IP addresses of each role. 

2. `#datanode config` module defines parameters of DataNodes. `datanode_disks` defines `path` and `reserved space` separated by ":". The `path` is where the data store in, so make sure it exists and has at least 30GB of space; `reserved space` is the minimum free space(Bytes) reserved for the path.

3. `[cfs:vars]` module defines parameters for SSH connection. So make sure the port, username and password for SSH connection is unified before start.

4. `#metanode config` module defines parameters of MetaNodes. `metanode_totalMem` defines the maximum memory(Bytes) can be use by MetaNode process.

```yaml
[master]
10.196.0.1
10.196.0.2
10.196.0.3
[datanode]
...
[cfs:vars]
ansible_ssh_port=22
ansible_ssh_user=root
ansible_ssh_pass="password"
...
#datanode config
...
datanode_disks =  '"/data0:10737418240","/data1:10737418240"'
...
#metanode config
...
metanode_totalMem = "28589934592"
...
```

For more configurations please refer to [documentation](https://cubefs.readthedocs.io/en/latest/user-guide/master.html).

Start the resources of CubeFS cluster with script `install.sh`. (make sure the Master is started first)

```
$ bash install.sh -h
Usage: install.sh -r | --role [datanode | metanode | master | objectnode | console | monitor | client | all | createvol ] [2.1.0 or latest]
$ bash install.sh -r master
$ bash install.sh -r metanode
$ bash install.sh -r datanode
$ bash install.sh -r monitor
$ bash install.sh -r client
$ bash install.sh -r console
```

Check mount point at `/cfs/mountpoint` on `client` node defined in `iplist`. 

Open [http://[the IP of console system]](https:/github.com/cubefs/cubefs) through a browser for web console system(the IP of console system is defined in `iplist`).  In console default user is `root`, password is `CubeFSRoot`. In  monitor default user is `admin`,password is `123456`.

## Run a CubeFS Cluster within Docker

A helper tool called `run_docker.sh` (under the `docker` directory) has been provided to run CubeFS with [docker-compose](https://docs.docker.com/compose/).

```
$ docker/run_docker.sh -r -d /data/disk
```

Note that **/data/disk** can be any directory but please make sure it has at least 10G available space. 

To check the mount status, use the `mount` command in the client docker shell:

```
$ mount | grep cubefs
```

To view grafana monitor metrics, open http://127.0.0.1:3000 in browser and login with `admin/123456`.
 
To run server and client separately, use the following commands:

```
$ docker/run_docker.sh -b
$ docker/run_docker.sh -s -d /data/disk
$ docker/run_docker.sh -c
$ docker/run_docker.sh -m
```

For more usage:

```
$ docker/run_docker.sh -h
```

