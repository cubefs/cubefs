# 单机部署

## 拉取代码
``` bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```

## CubeFS部署
...

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
