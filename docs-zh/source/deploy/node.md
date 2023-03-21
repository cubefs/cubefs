# 单机部署

## 拉取代码
``` bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```
## 部署

### 脚本部署

#### 部署基础集群
CubeFS 支持使用脚本一键部署基础集群，包含组件`Master`、`MetaNode`、`DataNode`，步骤如下：
```bash
cd ./cubefs
# 编译
make
# 启动脚本
sh ./shell/depoly.sh /home/data bond0
```
+ bond0: 为本机网卡的名字, 根据实际填写
+ `/home/data`: 为本地的一个目录,用于保存集群运行日志、数据及配置文件
+ 机器要求
  + 需root权限
  + 能使用`ifconfig`
  + 内存4G以上
  + `/home/data`对应磁盘剩余空间20G以上
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

#### 部署对象网关

::: tip 提示
可选章节，如果需要使用对象存储服务，则需要部署对象网关（ObjectNode）
:::

参考[使用对象存储章节](../user-guide/objectnode.md)

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

纠删码子系统部署成功之后，修改Master配置文件中的`ebsAddr`配置项（[更多配置参考](../maintenance/configs/master.md)），配置为Access节点注册的Consul地址，默认为`http://localhost:8500`

#### 停止集群
+ 使用脚本将会stop server和挂载点
```bash
sh ./shell/stop.sh
```

### docker 部署

#### 部署基础集群
在docker目录下，run_docker.sh工具用来方便运行CubeFS docker-compose试用集群，包含`Master`、`MetaNode`、`DataNode`与`ObjectNode`组件。

::: tip 提示
请确保已经安装docker和docker-compose，并在执行docker部署前确保防火墙关闭，避免权限问题导致容器启动失败。
:::

执行下面的命令，可创建一个最小的CubeFS集群。

::: warning 注意
`/data/disk`是数据根目录，至少需要10GB大小空闲空间。
:::

```bash
$ docker/run_docker.sh -r -d /data/disk
```

客户端启动成功后，在客户端docker容器中使用`mount`命令检查目录挂载状态：

```bash
$ mount | grep cubefs
```

在浏览器中打开`http://127.0.0.1:3000`，使用`admin/123456`登录，可查看CubeFS的grafana监控指标界面。

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
监控的Prometheus和Grafana相关配置位于`docker/monitor`目录下。


#### 部署纠删码子系统

::: warning 注意
纠删码docker方式部署暂未与其他模块（如Master）统一，该章节目前仅用于体验纠删码子系统本身功能，后续完善
:::

支持以下docker镜像部署方式：

- 远端拉取构建【`推荐`】

``` bash
$> docker pull cubefs/cubefs:blobstore-v3.2.0 # 拉取镜像
$> docker run cubefs/cubefs:blobstore-v3.2.0 # 运行镜像
$> docker container ls # 查看运行中的容器
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # 进入容器
```

- 本地脚本编译构建

``` bash
$> cd blobstore
$> ./run_docker.sh -b # 编译构建
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.2.0
$> ./run_docker.sh -r # 运行镜像
$> ... # 后续步骤同拉取构建
```
