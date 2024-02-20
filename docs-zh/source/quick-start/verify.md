# 快速使用

## 验证文件系统挂载

### 创建卷

::: tip 提示
如果有部署纠删码模块像创建纠删码卷，请参考[创建卷章节](../user-guide/volume.md)
:::

`cfs-cli` 读配置文件`~/.cfs-cli.json`. 示例`cfs-cli.json`内容如下

```json
{
  "masterAddr": [
    "127.0.0.1:17010",
    "127.0.0.2:17010",
    "127.0.0.3:17010"
  ],
  "timeout": 60
}
```

```bash
./build/bin/cfs-cli volume create ltptest ltptest
# 查看卷信息
./build/bin/cfs-cli volume info ltptest
```
### 启动客户端

- 准备客户端配置文件. 示例 `client.conf`如下

```json
{
  "mountPoint": "/home/cfs/client/mnt",
  "volName": "ltptest",
  "owner": "ltptest",
  "masterAddr": "127.0.0.1:17010,127.0.0.2:17010,127.0.0.3:17010",
  "logDir": "/cfs/client/log",
  "logLevel": "info",
  "profPort": "27510"
}
```

注意`volName`和`owner`必须与刚才用`cfs-cli`创建卷时指定值相同。

- 启动客户端
```bash
./build/bin/cfs-client -c /home/data/conf/client.conf
```

- 查看挂载是否成功
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
## 使用GUI
使用[GUI](../tools/gui.md)快速体验CubeFS

## 验证纠删码子系统
::: tip 提示
注意 纠删码子系统 (Blobstore) 提供了单独的交互式命令行管理工具。目前该工具尚未集成到cfs-cli中，稍后会集成。
:::

Blobstore CLI 可以轻松管理纠删码子系统。使用help可以查看帮助信息。这里我们只介绍如何验证纠删码系统本身的正确性。

### 启动 CLI
在默认配置的基础上，启动命令行工具cli。详细使用方法请参见CLI工具用户指南。

1. 物理机部署
```bash
$> cd ./cubefs
$>./build/bin/blobstore/blobstore-cli -c blobstore/cli/cli/cli.conf # 在access服务的机器上使用默认配置即可
```

2. Docker环境
```bash
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```

2. 验证
```bash
# 上传一个文件，成功后会返回存储位置 (-d 参数后跟实际数据)
$> access put -v -d "test -data-"
#"code_mode":11 编码模式，对应EC3P3
{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

# 下载文件
$> access get -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

# 删除文件
$> access del -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'
```
