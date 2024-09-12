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

::: tip 提示
`bond0` 为本机网卡的名字, 根据实际填写。
`baseDir` 为本地的一个目录，用于保存集群运行日志、数据及配置文件。示例中配置文件位于 `/home/data/conf` 目录下。三个 `sh` 命令的目录应当相同。
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
