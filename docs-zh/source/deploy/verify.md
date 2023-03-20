# CLI验证

## 验证文件系统挂载

### 创建卷

创建纠删码卷请参考[创建卷章节](../user-guide/volume.md)

```bash
./build/bin/cfs-cli volume create ltptest ltp
# 查看卷信息
./build/bin/cfs-cli volume info ltptest
```
### 启动客户端

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
cli 是为纠删码子系统（blobstore） 提供的交互式命令行管理工具, 配置 cli 后能够更方便地使用, 用 help 可以查看帮助信息。
:::

### 启动CLI

基于默认配置，启动命令行工具 `cli` ，详细使用参考[CLI工具使用](../maintenance/tool.md)
1. 物理机环境
``` bash
$> cd ./cubefs/blobstore
$>./bin/cli -c cli/cli/cli.conf # 采用默认配置启动cli 工具进入命令行
```
2. docker环境
``` bash
$> ./bin/cli -c conf/cli.conf
```

### 验证

``` bash
# 上传文件，成功后会返回一个location，（-d 参数为文件实际内容）
$> access put -v -d "test -data-"
# 返回结果
#"code_mode":11是clustermgr配置文件中制定的编码模式，11就是EC3P3编码模式
{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

# 下载文件，用上述得到的location作为参数（-l），即可下载文件内容
$> access get -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

# 删除文件，用上述location作为参数（-l）；删除文件需要手动确认
$> access del -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'
```