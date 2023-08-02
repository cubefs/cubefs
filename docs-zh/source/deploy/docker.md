# docker部署

## 拉取代码
```bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```

## 部署基础集群

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

## 验证文件系统挂载

在client容器中，`/cfs/mnt`即为挂载点，执行命令`df -h`如果有如下类似输出则代表挂载成功
```bash
df -h
Filesystem      Size  Used Avail Use% Mounted on
overlay          40G   11G   27G  28% /
tmpfs            64M     0   64M   0% /dev
/dev/vda1        40G   11G   27G  28% /cfs/bin
shm              64M     0   64M   0% /dev/shm
cubefs-ltptest   30G     0   30G   0% /cfs/mnt
```

## 部署纠删码子系统（可选）

::: warning 注意
纠删码docker方式部署暂未与其他模块（如Master）统一，该章节目前仅用于体验纠删码子系统本身功能，后续完善
:::

支持以下docker镜像部署方式：

- 远端拉取构建【`推荐`】

```bash
$> docker pull cubefs/cubefs:blobstore-v3.2.0 # 拉取镜像
$> docker run cubefs/cubefs:blobstore-v3.2.0 # 运行镜像
$> docker container ls # 查看运行中的容器
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # 进入容器
```

- 本地脚本编译构建

```bash
$> cd blobstore
$> ./run_docker.sh -b # 编译构建
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.2.0
$> ./run_docker.sh -r # 运行镜像
$> ... # 后续步骤同拉取构建
```

## 启动blobstore-cli

::: tip 提示
纠删码子系统（Blobstore）提供了单独交互式命令行管理工具，当前该工具暂时未集成至cfs-cli，后续会集成。
:::

blobstore-cli 可以方便的管理纠删码子系统, 用 help 可以查看帮助信息。这里仅介绍验证纠删码系统本身的正确性。

基于默认配置，启动命令行工具 `blobstore-cli` ，详细使用参考[BLOBSTORE-CLI工具使用指南](../maintenance/tool.md)

```bash
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```

## 验证纠删码子系统

```bash
# 上传文件，成功后会返回一个location，（-d 参数为文件实际内容）
$> access put -v -d "test -data-"
# 返回结果
#"code_mode":11是clustermgr配置文件中制定的编码模式，11就是EC3P3编码模式
{"cluster_id":1,"code_mode":11,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

# 下载文件，用上述得到的location作为参数（-l），即可下载文件内容
$> access get -v -l '{"cluster_id":1,"code_mode":11,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

# 删除文件，用上述location作为参数（-l）；删除文件需要手动确认
$> access del -v -l '{"cluster_id":1,"code_mode":11,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'
```