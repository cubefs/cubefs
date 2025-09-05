# 单机模式

## 拉取代码
``` bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```
## 部署

### 脚本部署

#### 部署基础集群
CubeFS 支持使用脚本一键部署基础集群，基础集群包含组件 `Master`、`MetaNode`、`DataNode`，同时可额外选择启动 `client` 和 `ObjectNode`。 步骤如下：
```bash
cd ./cubefs
# 编译
make
# 生成配置文件并启动基础集群
sh ./shell/deploy.sh /home/data bond0

# 等待1分钟待集群就绪后，可选执行以下命令

# 挂载文件系统（可选，如果想体验文件存储，则执行该条命令。默认挂载在/home/data/client/mnt，默认创建卷名为 ltptest ）
sh ./shell/deploy_client.sh /home/data

# 启动对象存储（可选，如果想体验对象存储，则执行该条命令。默认监听端口为17410）
sh ./shell/deploy_object.sh /home/data
```

+ 机器要求
  + 需 root 权限
  + 能使用 `ifconfig`
  + 内存 4G 以上
  + `/home/data` 对应磁盘剩余空间20G以上
+ `./shell/deploy.sh` 脚本用于启动一个集群服务，包括主节点（master）、元数据节点（metanode）和数据节点（datanode）。脚本首先检查是否传递了两个参数（本地目录 `baseDir` 和网卡名称 `bond0` ），并根据这些参数生成必要的配置文件，依次启动各个服务:
  + 生成子网 IP：通过 `genIp.sh` 脚本生成四个子网 IP 地址。
  + 生成配置文件：通过 `genConf.sh` 脚本生成配置文件。
  + 启动节点：启动三个主节点服务。
  + 启动元数据节点服务：启动四个元数据节点服务。
  + 启动数据节点服务：启动四个数据节点服务。
  + 设置集群配置：使用 `cfs-cli` 设置集群配置。
  + 等待准备状态：等待一段时间让集群进入准备状态。
  + 如果你在容器里执行，请确认以特权模式运行容器：docker run --privileged 或 Kubernetes 的 securityContext.privileged: true并自行调整所有.sh文件中的网络设置为容器的子网，否则可能由于无法创建子网产生错误

::: tip 提示
`bond0` 为本机网卡的名字, 根据实际填写，可使用ifconfig查看得到形如"ens192"，"eth0"的网卡名称。
`baseDir` 为本地的一个目录，用于保存集群运行日志、数据及配置文件。示例中配置文件位于 `/home/data/conf` 目录下。三个 `sh` 命令的目录应当相同。
最好测试一下挂载文件系统，看客户端能否启动，在一些ubuntu22.04(内核5.15.0-139-generic以上)的机器上可能会出现因为fusermount3的兼容性问题导致虽然集群状态正常，但客户端无法挂载的情况
:::

+ 查看集群状态
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

文件系统的使用可参考[使用文件系统章节](../user-guide/file.md)

对象存储的使用可参考[使用对象存储章节](../user-guide/objectnode.md)

#### 部署纠删码子系统

::: tip 提示
可选章节，如果需要使用纠删码卷则需要部署
:::

``` bash
$> cd cubefs/blobstore
$> ./run.sh --consul
...
start blobstore service successfully, wait minutes for internal state preparation
$>
```

纠删码子系统部署成功之后，修改 Master 配置文件中的 `ebsAddr` 配置项（[更多配置参考](../ops/configs/master.md)），配置为 Access 节点注册的 Consul 地址，默认为`http://localhost:8500`

#### 停止集群
+ 使用脚本将会 stop server 和挂载点
```bash
sh ./shell/stop.sh
```

### docker 部署

#### 部署基础集群
在 docker 目录下，run_docker.sh 工具用来方便运行 CubeFS docker-compose 试用集群，包含 `Master`、`MetaNode`、`DataNode` 与 `ObjectNode` 组件。

::: tip 提示
请确保已经安装 docker 和 docker-compose，并在执行 docker 部署前确保防火墙关闭，避免权限问题导致容器启动失败。
:::

执行下面的命令，可创建一个最小的 CubeFS 集群。

::: warning 注意
`/data/disk` 是数据根目录，至少需要 10GB 大小空闲空间。
:::

```bash
$ docker/run_docker.sh -r -d /data/disk
```

出现下面报错解决方法：删除 `/data/disk` 目录重新执行命令，数据根目录可自己指定，请确认没有重要文件

```bash
/data/disk: avaible size 0 GB < Min Disk avaible size 10 GB
```

如果目录下还有挂载点会导致目录删除不掉，请先 `umount` 挂载点

```bash
# rm: 无法删除'/data/disk/client/mnt'：设备或资源忙
umount -l /data/disk/client/mnt
```


客户端启动成功后，在客户端 docker 容器中使用 `mount` 命令检查目录挂载状态：

```bash
$ mount | grep cubefs
cubefs-ltptest on /cfs/mnt type fuse.cubefs (rw,nosuid,nodev.relatime,user_id=0,group_id=0,allow_other)
```

在浏览器中打开 `http://127.0.0.1:3000`，使用 `admin/123456` 登录，可查看 CubeFS 的 grafana 监控指标界面。

![arc](./pic/grafana.png)


或者使用下面的命令分步运行:

```bash
$ docker/run_docker.sh -b
$ docker/run_docker.sh -s -d /data/disk
$ docker/run_docker.sh -c
$ docker/run_docker.sh -m
```

更多命令请参考帮助:

```bash
$ docker/run_docker.sh -h
```
监控的 Prometheus 和 Grafana 相关配置位于 `docker/monitor` 目录下。


#### 部署纠删码子系统

::: warning 注意
纠删码 docker 方式部署暂未与其他模块（如 Master）统一，该章节目前仅用于体验纠删码子系统本身功能，后续完善
:::

支持以下 docker 镜像部署方式：

- 远端拉取构建【`推荐`】

``` bash
$> docker pull cubefs/cubefs:blobstore-v3.3.0 # 拉取镜像
$> docker run cubefs/cubefs:blobstore-v3.3.0 # 运行镜像
$> docker container ls # 查看运行中的容器
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.3.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # 进入容器
```

- 本地脚本编译构建

``` bash
$> cd blobstore
$> ./run_docker.sh -b # 编译构建
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.3.0
$> ./run_docker.sh -r # 运行镜像
$> ... # 后续步骤同拉取构建
```

# 实战训练

上面的文章已经提供两种单机版部署方式，下面是另外一种独立的部署方式。

本文是一篇介绍cubefs初级使用方式的文章。通过搭建一个单节点的cubefs存储，让用户体验cubefs的魅力。

我们搭建的单节点cubefs只使用副本模式，不支持EC模式。无论是数据还是元数据都是一份存储。

**备注：因为单节点的cubefs只有1份副本，所以没有高可靠性的保证。所以这种环境只适合初学者学习和验证，不适合用在生产环境上面。**

# 修改点

目前的cubefs并不能搭建单节点的存储，我们需要对master/cluster.go里面的代码进行一点小小的修改。（以后这个可能会在某个版本更新吧）

默认创建的卷的mp副本都是3个。因为我们只有1个节点，所以需要修改参数ReplicaNum为1。这样一个mp副本就可以跑起来。添加vv.ReplicaNum = 1

```less
func (c *Cluster) doCreateVol(req *createVolReq) (vol *Vol, err error) {
    vv := volValue{
        ReplicaNum:              defaultReplicaNum,
    }

    vv.ReplicaNum = 1

    if _, err = c.getVol(req.name); err == nil {
```

# 环境

我们选择操作系统ubuntu20的虚拟机，需要安装go1.17.13，cmake的依赖包。

```less
sudo apt install cmake
sudo apt install -y build-essential libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
sudo apt install git
```
下载go1.17.13.linux-amd64.tar.gz这个版本，然后安装它：
```less
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.17.13.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```
因为cubefs的源码包含了一个vendor的第三方库，如果使用别的golang版本，可能导致编译失败。这时就需要大量的时间和精力去处理这些第三方库的依赖和golang语言版本升级的关系。
简单而言，我们搭建环境，目的就是能够成功编译cubefs的源码。

从[https://github.com/cubefs/cubefs](https://github.com/cubefs/cubefs)下载最新master的代码，然后进行编译。

```less
git config --global http.sslVerify false
git clone https://github.com/cubefs/cubefs.git
cd cubefs/
git checkout release-3.5.0
vi master/cluster.go
# 记得先完成上面的修改点，或者确定这个singleNodeMode的配置已经在代码里面。
# 在doCreateVol函数里面添加一行代码：vv.ReplicaNum = 1
make server client cli
```
如果编译成功，就可以在build/bin目录下看到这3个文件：
**cfs-server，cfs-client，cfs-cli**

```less
test@test-VirtualBox:~/cubefs$ ls build/bin
blobstore  cfs-cli  cfs-client  cfs-server
```
其中**cfs-server是master，metanode，datanode，objectnode的综合程序。**
**cfs-client是挂载cubefs卷的客户端，是文件存储的入口。**

**cfs-cli是管理工具**。

# 启动

有了上面的可执行程序，我们就可以启动cubefs的服务。下面所有的服务都需要相应的配置文件和启动命令。我们操作系统的网络地址是192.168.12.60（请修改为你自己的ip地址）。

```less
cd build/bin
mkdir master metanode datanode objectnode client
```

## master

配置文件：

```less
cubefs@cubefs-VirtualBox:~/config$ cat master.conf 
{
  "clusterName": "cfs_single_test1",
  "id": "1",
  "role": "master",
  "ip": "192.168.12.60",
  "listen": "17010",
  "prof": "17020",
  "peers": "1:192.168.12.60:17010",
  "retainLogs": "200",
  "logLevel": "warn",
  "logDir": "./master/logs",
  "warnLogDir": "./master/logs",
  "walDir": "./master/wal",
  "storeDir": "./master/store",
  "disableAutoCreate":"true",
  "legacyDataMediaType": 1,
  "metaNodeReservedMem": "102400000",
  "singleNodeMode":"true"
}
```
启动命令：
```less
sudo ./cfs-server -f -c ./master.conf &
```
## metanode

配置文件：

```less
cubefs@cubefs-VirtualBox:~/config$ cat meta.conf 
{
  "role": "metanode",
  "listen": "17210",
  "prof": "17220",
  "raftHeartbeatPort": "17230",
  "raftReplicaPort": "17240",
  "exporterPort": 17250,
  "logLevel": "warn",
  "logDir": "./metanode/logs",
  "warnLogDir": "./metanode/logs",
  "memRatio": "70",
  "totalMem": "1024000000",
  "metadataDir": "./metanode/meta",
  "raftDir": "./metanode/raft",
  "masterAddr": ["192.168.12.60:17010"]
}
```
启动命令：
```less
sudo ./cfs-server -f -c ./meta.conf &
```
## datanode

我们配置的**存储目录是/data1，设置的大小是1G**。**这个目录一定要先创建好**。

配置文件：

```less
cubefs@cubefs-VirtualBox:~/config$ cat data.conf 
{
  "role": "datanode",
  "listen": "17310",
  "prof": "17320",
  "raftHeartbeat": "17330",
  "raftReplica": "17340",
  "exporterPort": 17350,
  "diskRdonlySpace": 1024000000,
  "raftDir": "./datanode",
  "enableSmuxConnPool": false,
  "cell": "cell-01",
  "logDir": "./datanode/logs",
  "warnLogDir": "./datanode/logs",
  "logLevel": "warn",
  "mediaType": 1,
  "smuxMaxConn": 10,
  "smuxStreamPerConn": 2,
  "disks":["/data1:1024000000"],
  "masterAddr": ["192.168.12.60:17010"]
}
```
启动命令：
```less
sudo ./cfs-server -f -c ./data.conf &
```
## objectnode

对象存储对外的endpoint就是ip地址加上下面的17410端口。也就是192.168.12.60:17410。

配置文件：

```less
cubefs@cubefs-VirtualBox:~/config$ cat object.conf 
{
     "role": "objectnode",
     "listen": "17410",
     "prof": "7420",
     "logDir": "./objectnode/logs",
     "logLevel": "warn",
     "masterAddr": ["192.168.12.60:17010"],
     "exporterPort": "17420"
}
```
启动命令：
```less
sudo ./cfs-server -f -c ./object.conf &
```
## cli

我们需要配置一下cli工具的配置文件，这个在当前用户的主目录下面.cfs-cli.json

```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ cat ~/.cfs-cli.json
{
  "masterAddr": [
        "192.168.12.60:17010"
  ],
  "timeout": 60
}
```
配置以后，我们就可以使用cfs-cli这个命令
我们在混合云的基础上面部署的，所以需要配置一下类型：

```less
./cfs-cli cluster set --clusterDataMediaType 1
```
查询集群环境如下：
```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ ./cfs-cli cluster info
[Cluster]
  Cluster name       : cfs_single_test1
  Master leader      : 192.168.12.60:17010
  Master-1           : 192.168.12.60:17010
  Auto allocate      : Enabled
  MetaNode count (active/total)    : 1/1
  MetaNode used                    : 0 GB
  MetaNode available               : 7 GB
  MetaNode total                   : 7 GB
  DataNode count (active/total)    : 1/1
  DataNode used                    : 28 GB
  DataNode available               : 58 GB
  DataNode total                   : 92 GB
  Volume count                     : 1
  Allow Mp Decomm                  : Enabled
  EbsAddr                          : 
  LoadFactor                       : 0
  DpRepairTimeout                  : 2h0m0s
  DataPartitionTimeout             : 20m0s
  volDeletionDelayTime             : 48 h
  EnableAutoDecommission           : false
  AutoDecommissionDiskInterval     : 10s
  EnableAutoDpMetaRepair           : false
  AutoDpMetaRepairParallelCnt      : 100
  MarkDiskBrokenThreshold          : 0%
  DecommissionDpLimit              : 10
  DecommissionDiskLimit            : 1
  DpBackupTimeout                  : 168h0m0s
  ForbidWriteOpOfProtoVersion0     : false
  LegacyDataMediaType              : 0
  BatchCount         : 0
  MarkDeleteRate     : 0
  DeleteWorkerSleepMs: 0
  AutoRepairRate     : 0
  MaxDpCntLimit      : 3000
  MaxMpCntLimit      : 300
```
## 创建卷

上面的服务启动完毕，我们就可以查看服务：

```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ ps -ef | grep "cfs"
root        8109    2818  0 2月25 pts/0   00:00:00 sudo ./cfs-server -f -c ./master.conf
root        8110    8109  0 2月25 pts/0   00:01:48 ./cfs-server -f -c ./master.conf
root        8147    2818  0 2月25 pts/0   00:00:00 sudo ./cfs-server -f -c ./meta.conf
root        8148    8147  0 2月25 pts/0   00:01:35 ./cfs-server -f -c ./meta.conf
root        8164    2818  0 2月25 pts/0   00:00:00 sudo ./cfs-server -f -c ./data.conf
root        8165    8164  0 2月25 pts/0   00:02:30 ./cfs-server -f -c ./data.conf
root        8182    2818  0 2月25 pts/0   00:00:00 sudo ./cfs-server -f -c ./object.conf
root        8183    8182  0 2月25 pts/0   00:00:53 ./cfs-server -f -c ./object.conf
```
然后检查状态
```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ ./cfs-cli metanode list
[Meta nodes]
ID        ADDRESS                                                              WRITABLE    ACTIVE      MEDIA        ForbidWriteOpOfProtoVer0
2         192.168.12.60:17210(cubefs-VirtualBox:17210)                         Yes         Active      N/A          notForbid   
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ ./cfs-cli datanode list
[Data nodes]
ID        ADDRESS                                                              WRITABLE    ACTIVE      MEDIA        ForbidWriteOpOfProtoVer0
3         192.168.12.60:17310(cubefs-VirtualBox:17310)                         Yes         Active      SSD          notForbid  
```
创建卷，只能创建一个副本的卷，必须添加上参数：--replica-num 1
```less
./cfs-cli volume create test test --capacity 1 --replica-num 1
```
## client

我们挂载的目录是/mnt/cubefs，使用的用户名和卷都是test。

配置文件：

```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ cat client.conf 
{
    "masterAddr": "192.168.12.60:17010",
    "mountPoint": "/mnt/cubefs",
    "volName": "test",
    "owner": "test",
    "logDir": "client/logs",
    "logLevel": "warn"
}
```
启动命令：
```less
sudo ./cfs-client -c ./client.conf
```
# 验证

## 文件存储

在挂载cubefs的卷到/mnt/cubefs目录以后，可以在上面进行文件读写操作。

## 对象存储

cubefs的卷对应s3的桶，用户信息则包含了ak/sk。

```less
cubefs@cubefs-VirtualBox:~/cubefs/build/bin$ ./cfs-cli user info test
[Summary]
  User ID    : test
  Access Key : lkrfkighKZdJrSEo
  Secret Key : sDDbLl8hBgeh1p0lyhlCNZlML4wtksIi
  Type       : normal
  Create Time: 2025-02-25 15:54:00
[Volumes]
VOLUME                  PERMISSION  
test                    Owner 
```
安装aws的客户端工具，然后就可以进行验证：
```less
sudo apt-get install awscli
```
配置好AK/SK，然后就可以使用s3存储：
```less
aws configure
```
对象存储接口：
```less
aws s3api --endpoint-url http://192.168.12.60:17410 put-object --bucket test --key cfs-server --body ./cfs-server
aws s3api --endpoint-url http://192.168.12.60:17410 get-object --bucket test --key cfs-server ./tst2
aws s3api --endpoint-url http://192.168.12.60:17410 head-object --bucket test --key cfs-server
```

