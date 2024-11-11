# 目录文件数与配额管理
::: warning 注意
配额管理为 v3.2.1 版本新增 feature
:::

## 单目录文件数目限制
限制单个目录下子文件数或者目录数，避免出现超大目录，导致 MP 节点资源耗尽。

- 每个目录的默认子文件数或者目录数为 2 千万，可以配置，最小值为 1 百万，无上限。当单个目录下创建的子文件数或者目录数超过 limit，则创建失败。 
- 配置的 limit 值对整个集群生效，持久化在 master。 
- 完整支持需要 Client，Metanode，Master 均升级到3.2.1版本。

### 设置目录文件数目
```bash
curl -v "http://192.168.0.11:17010/admin/setClusterInfo?dirQuota=20000000"
```

::: tip 提示
`192.168.0.11`为 Master 的 ip 地址，下同
:::

| 参数   | 类型     | 描述  |
|------|--------|-----|
| dirQuota | uint32 | 配额值 |

### 获取目录文件数目

```bash
curl -v "http://192.168.0.11:17010/admin/getIp"
```
响应如下：

```json
{
    "code": 0,
    "data": {
        "Cluster": "test",
        "DataNodeAutoRepairLimitRate": 0,
        "DataNodeDeleteLimitRate": 0,
        "DirChildrenNumLimit": 20000000,
        "EbsAddr": "",
        "Ip": "192.168.0.1",
        "MetaNodeDeleteBatchCount": 0,
        "MetaNodeDeleteWorkerSleepMs": 0,
        "ServicePath": ""
    },
    "msg": "success"
}
```

`DirChildrenNumLimit` 字段为当前集群的目录配额值

## 配额管理
::: warning 注意
目录配额管理为 v3.3.0 版本新增 feature
:::
### 创建配额
``` bash
create paths quota

Usage:
  cfs-cli quota create [volname] [fullpath1,fullpath2] [flags]

Flags:
  -h, --help            help for create
      --maxBytes uint   Specify quota max bytes (default 18446744073709551615)
      --maxFiles uint   Specify quota max files (default 18446744073709551615)
```
创建 quota 需要指定卷名、一个或多个 path 目录。
注意：path 之间不能重复，以及嵌套。

### 应用配额
``` bash
apply quota

Usage:
  cfs-cli quota apply [volname] [quotaId] [flags]

Flags:
  -h, --help                       help for apply
      --maxConcurrencyInode uint   max concurrency set Inodes (default 1000)
```
apply quota 需要指定卷名以及 quotaId，这个接口在创建 quota 后执行，目的是让 quota 目录下（包括 quota 目录自身）的已有文件和目录该 quotaId 生效。整个创建 quota 的流程先执行 quota create，然后执行 quota apply 命令。
注意：如果 quota 目录下的文件数量很多，则该接口返回时间会比较长
### 撤销配额
``` bash
revoke quota

Usage:
  cfs-cli quota revoke [volname] [quotaId] [flags]

Flags:
      --forceInode uint            force revoke quota inode
  -h, --help                       help for revoke
      --maxConcurrencyInode uint   max concurrency delete Inodes (default 1000)
```
revoke quota 需要指定卷名以及quotaId，这个接口在准备删除 quota 的时候执行，目的是让quota 目录下的（包括quota目录自身）的已有文件和目录该 quotaId 失效。整个删除 quota 的流程先执行 quota revoke，然后通过 quota list 查询确认 USEDFILES 和 USEDBYTES 的值为 0，再进行 quota delete 操作。
### 删除配额
``` bash
delete path quota

Usage:
  cfs-cli quota delete [volname] [quotaId] [flags]

Flags:
  -h, --help   help for delete
  -y, --yes    Do not prompt to clear the quota of inodes
```
delete quota 需要指定卷名以及 quotaId
### 更新配额
``` bash
update path quota

Usage:
  cfs-cli quota update [volname] [quotaId] [flags]

Flags:
  -h, --help            help for update
      --maxBytes uint   Specify quota max bytes
      --maxFiles uint   Specify quota max files

```
update quota 需要指定卷名以及 quotaId，目前可以更新的值只有 maxBytes 和 maxFiles
### 列举配额
``` bash
list volname all quota

Usage:
  cfs-cli quota list [volname] [flags]

Flags:
  -h, --help   help for list

```
list quota 需要指定卷名，遍历出所有该卷的 quota 信息
### 列举所有带配额的卷
``` bash
list all volname has quota

Usage:
  cfs-cli quota listAll [flags]

Flags:
  -h, --help   help for listAll
```
不带任何参数，遍历出所有带 quota 的卷信息
### 获取 inode 上的配额信息
``` bash
get inode quotaInfo

Usage:
  cfs-cli quota getInode [volname] [inode] [flags]

Flags:
  -h, --help   help for getInode

```
查看具体的某个 inode 是否带有 quota 信息
