# 目录文件数与配额管理
::: warning 注意
配额管理为v3.2.1版本新增feature
:::

## 单目录文件数目限制
限制单个目录下子文件数或者目录数，避免出现超大目录，导致MP节点资源耗尽。

- 每个目录的默认子文件数或者目录数为2千万，可以配置，最小值为1百万，无上限。当单个目录下创建的子文件数或者目录数超过limit，则创建失败。 
- 配置的limit值对整个集群生效，持久化在master。 
- 完整支持需要Client，Metanode，Master均升级到3.2.1版本。

### 设置目录文件数目
```bash
curl -v "http://192.168.0.11:17010/setClusterInfo?dirQuota=20000000"
```

::: tip 提示
`192.168.0.11`为Master的ip地址，下同
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

`DirChildrenNumLimit`字段为当前集群的目录配额值

## 配额管理
::: warning 注意
目录配额管理为v3.3.0版本新增feature
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
创建quota需要指定卷名、一个或多个path目录。
注意：path之间不能重复，以及嵌套。

### 应用配额
``` bash
apply quota

Usage:
  cfs-cli quota apply [volname] [quotaId] [flags]

Flags:
  -h, --help                       help for apply
      --maxConcurrencyInode uint   max concurrency set Inodes (default 1000)
```
apply quota需要指定卷名以及quotaId，这个接口在创建quota后执行，目的是让quota目录下（包括quota目录自身）的已有文件和目录该quotaId生效。整个创建quota的流程先执行quota create，然后执行quota apply命令。
注意：如果quota目录下的文件数量很多，则该接口返回时间会比较长
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
revoke quota需要指定卷名以及quotaId，这个接口在准备删除quota的时候执行，目的是让quota目录下的（包括quota目录自身）的已有文件和目录该quotaId失效。整个删除quota的流程先执行quota revoke，然后通过quota list查询确认USEDFILES和USEDBYTES的值为0，再进行quota delete操作。
### 删除配额
``` bash
delete path quota

Usage:
  cfs-cli quota delete [volname] [quotaId] [flags]

Flags:
  -h, --help   help for delete
  -y, --yes    Do not prompt to clear the quota of inodes
```
delete quota需要指定卷名以及quotaId
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
update quota需要指定卷名以及quotaId，目前可以更新的值只有maxBytes和maxFiles
### 列举配额
``` bash
list volname all quota

Usage:
  cfs-cli quota list [volname] [flags]

Flags:
  -h, --help   help for list

```
list quota需要指定卷名，遍历出所有该卷的quota信息
### 列举所有带配额的卷
``` bash
list all volname has quota

Usage:
  cfs-cli quota listAll [flags]

Flags:
  -h, --help   help for listAll
```
不带任何参数，遍历出所有带quota的卷信息
### 获取inode上的配额信息
``` bash
get inode quotaInfo

Usage:
  cfs-cli quota getInode [volname] [inode] [flags]

Flags:
  -h, --help   help for getInode

```
查看具体的某个inode是否带有quota信息
