# 单机部署

## 拉取代码
``` bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```

## CubeFS部署


### CubeFS 脚本部署
#### 部署
cubefs 支持使用脚本进行进行单节点部署master&meta&data模块，步骤如下：
```bash
cd ./cubefs
#编译
make
# 启动脚本
sh ./shell/depoly.sh /home/data bond0
```
+ bond0: 为本机网卡的名字, 根据实际填写
+ /home/data: 为本地的一个目录,用于保存集群运行日志和数据，积极配置文件
+ 机器要求
  + 需root权限
  + 能使用ifconfig
  + 内存4G以上
  + /home/data对应磁盘剩余空间20G以上
+ 查看集群状态
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
#### 挂载
+ 创建卷
```bash
./build/bin/cfs-cli volume create ltptest ltp
# 查看卷信息
./build/bin/cfs-cli volume info ltptest
```
+ 启动客户端
  + /home/cfs/client/mnt即为挂载点，代表挂载成功
```bash
./build/bin/cfs-client -c /home/data/conf/client.conf
df -h
Filesystem      Size  Used Avail Use% Mounted on
udev            3.9G     0  3.9G   0% /dev
tmpfs           796M   82M  714M  11% /run
/dev/sda1        98G   48G   45G  52% /
tmpfs           3.9G   11M  3.9G   1% /dev/shm
cubefs-ltptest   10G     0   10G   0% /home/cfs/client/mnt
...
```
#### 停止集群
+ 使用脚本将会stop server和挂载点
```bash
sh ./shell/stop.sh
```

### CubeFS docker 部署

在docker目录下，run_docker.sh工具用来方便运行CubeFS docker-compose试用集群。请确保已经安装docker和docker-compose，并在执行docker部署前确保防火墙关闭，避免权限问题导致容器启动失败。

执行下面的命令，可完全重新创建一个最小的CubeFS集群。注意的是`/data/disk`是数据根目录，至少需要10GB大小空闲空间。

`$ docker/run_docker.sh -r -d /data/disk`

客户端启动成功后，在客户端docker容器中使用\`mount\`命令检查目录挂载状态：

`$ mount | grep cubefs`

在浏览器中打开http://127.0.0.1:3000，使用\`admin/123456\`登录，可查看cubefs的grafana监控指标界面。

或者使用下面的命令分步运行:

```
$ docker/run_docker.sh -b

$ docker/run_docker.sh -s -d /data/disk

$ docker/run_docker.sh -c

$ docker/run_docker.sh -m
```

更多命令:

$ docker/run_docker.sh -h

监控的Prometheus和Grafana相关配置位于\`docker/monitor\`目录下。
## Blobstore 部署 

### Blobstore 脚本部署
blobstore支持单机部署，运行一键启动命令即可，当显示有start blobstore
service successfully便表示部署成功，具体操作如下：

``` bash
$> cd cubefs/blobstore
$> ./run.sh
...
start blobstore service successfully, wait minutes for internal state preparation
$>
```

### Blobstore 容器部署

blobstore支持以下docker镜像部署方式：

1.  远端拉取构建【`推荐`】

``` bash
$> docker pull cubefs/cubefs:blobstore-v3.2.0 # 拉取镜像
$> docker run cubefs/cubefs:blobstore-v3.2.0 # 运行镜像
$> docker container ls # 查看运行中的容器
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # 进入容器
```

2.  本地脚本编译构建

> 小提示：整个初始编译过程可能需要些时间

``` bash
$> cd blobstore
$> ./run_docker.sh -b # 编译构建
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.2.0
$> ./run_docker.sh -r # 运行镜像
$> ... # 后续步骤同1
```
