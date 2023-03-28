# 容器部署

BlobStore支持以下docker镜像部署方式：

1. 远端拉取构建【`推荐`】

```
$> docker pull cubefs/cubefs:blobstore-v3.2.0 # 拉取镜像
$> docker run cubefs/cubefs:blobstore-v3.2.0 # 运行镜像
$> docker container ls # 查看运行中的容器
   CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
   76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
$> docker exec -it thirsty_kare /bin/bash # 进入容器
```

2. 本地脚本编译构建

> 小提示：整个初始编译过程可能需要些时间

```
$> cd blobstore
$> ./run_docker.sh -b # 编译构建
&> Successfully built 0b29fda1cd22
   Successfully tagged blobstore:v3.2.0
$> ./run_docker.sh -r # 运行镜像
$> ... # 后续步骤同1
```

blobstore-cli工具使用

> 小提示: blobstore-cli 是为 blobstore 提供的交互式命令行管理工具, 配置 blobstore-cli 后能够更方便地使用, 用 help 可以查看帮助信息

```
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```
