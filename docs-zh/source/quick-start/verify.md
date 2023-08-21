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