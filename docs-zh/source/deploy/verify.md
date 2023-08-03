# CLI验证

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

## 验证纠删码子系统

::: tip 提示
纠删码子系统（Blobstore）提供了单独交互式命令行管理工具，当前该工具暂时未集成至cfs-cli，后续会集成。
:::

Blobstore cli 可以方便的管理纠删码子系统, 用 help 可以查看帮助信息。这里仅介绍验证纠删码系统本身的正确性。

### 启动CLI

基于默认配置，启动命令行工具 `blobstore-cli` ，详细使用参考[BLOBSTORE-CLI工具使用指南](../maintenance/tool.md)
1. 物理机环境
``` bash
$> cd ./cubefs
$>./build/bin/blobstore/blobstore-cli -c blobstore/cli/cli/cli.conf # 采用默认配置启动cli 工具进入命令行
```
2. docker环境
``` bash
$> ./bin/blobstore-cli -c conf/blobstore-cli.conf
```

### 验证

``` bash
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
