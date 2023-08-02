# 单机部署

## 拉取代码
```bash
# 如该步骤已完成可跳过
$ git clone https://github.com/cubefs/cubefs.git
```

## 部署基础集群

CubeFS 支持使用脚本一键部署基础集群，包含组件`Master`、`MetaNode`、`DataNode`，步骤如下：

```bash
cd ./cubefs
# 编译
make
# 启动脚本
sh ./shell/depoly.sh /home/data bond0
```
+ `bond0`: 为本机网卡的名字, 根据实际填写
+ `/home/data`: 为本地的一个目录,用于保存集群运行日志、数据及配置文件
+ 机器要求
  + 需root权限
  + 能使用`ifconfig`
  + 内存4G以上
  + `/home/data`对应磁盘剩余空间20G以上

## 查看集群是否正常启动

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

::: tip 提示
基础集群提供分布式存储的基础功能，但通常不直接对外提供服务，需要通过**客户端（Client）**或者**对象网关（ObjectNode）**等方式访问。这里以客户端方式访问为例。
:::

## 创建卷

```bash
./build/bin/cfs-cli volume create ltptest ltp
# 查看卷信息
./build/bin/cfs-cli volume info ltptest
```

## 启动客户端并挂载卷

```bash
./build/bin/cfs-client -c /home/data/conf/client.conf
```

## 验证文件系统挂载

`/home/cfs/client/mnt`即为挂载点，执行命令`df -h`如果有如下类似输出则代表挂载成功
```bash
df -h
Filesystem      Size  Used Avail Use% Mounted on
udev            3.9G     0  3.9G   0% /dev
tmpfs           796M   82M  714M  11% /run
/dev/sda1        98G   48G   45G  52% /
tmpfs           3.9G   11M  3.9G   1% /dev/shm
cubefs-ltptest   10G     0   10G   0% /home/cfs/client/mnt
...
```

## 部署对象网关（可选）

::: tip 提示
可选章节，如果需要使用对象存储服务，则需要部署对象网关（ObjectNode）
:::

参考[使用对象存储章节](../user-guide/objectnode.md)

## 停止集群
+ 使用脚本将会stop server和挂载点
```bash
sh ./shell/stop.sh
```

## 部署纠删码子系统（可选）

::: tip 提示
以下为可选章节，如果需要使用纠删码卷则需要部署
:::

```bash
$> cd cubefs/blobstore
$> ./run.sh --consul
...
start blobstore service successfully, wait minutes for internal state preparation
$>
```

纠删码子系统部署成功之后，修改Master配置文件中的`ebsAddr`配置项（[更多配置参考](../maintenance/configs/master.md)），配置为Access节点注册的Consul地址，默认为`http://localhost:8500`

## 启动blobstore-cli

::: tip 提示
纠删码子系统（Blobstore）提供了单独交互式命令行管理工具，当前该工具暂时未集成至cfs-cli，后续会集成。
:::

blobstore-cli 可以方便的管理纠删码子系统, 用 help 可以查看帮助信息。这里仅介绍验证纠删码系统本身的正确性。


基于默认配置，启动命令行工具 `blobstore-cli` ，详细使用参考[BLOBSTORE-CLI工具使用指南](../maintenance/tool.md)
```bash
$> cd ./cubefs
$>./build/bin/blobstore/blobstore-cli -c blobstore/cli/cli/cli.conf # 采用默认配置启动cli 工具进入命令行
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