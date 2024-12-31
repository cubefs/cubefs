# 配额管理

::: warning 注意
目录配额管理为 v3.3.0 版本新增 feature
:::

## 创建配额

创建 quota 需要指定卷名、一个或多个 path 目录。

注意：path 之间不能重复，以及嵌套。

```bash
cfs-cli quota create [volname] [fullpath1,fullpath2] [flags]
```

```bash
Flags:
  -h, --help            help for create
      --maxBytes uint   Specify quota max bytes (default 18446744073709551615)
      --maxFiles uint   Specify quota max files (default 18446744073709551615)
```

## 应用配额

apply quota 需要指定卷名以及 quotaId，这个接口在创建 quota 后执行，目的是让 quota 目录下（包括 quota 目录自身）的已有文件和目录该 quotaId 生效。整个创建 quota 的流程先执行 quota create，然后执行 quota apply 命令。

注意：如果 quota 目录下的文件数量很多，则该接口返回时间会比较长

```bash
cfs-cli quota apply [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help                       help for apply
      --maxConcurrencyInode uint   max concurrency set Inodes (default 1000)
```

## 取消应用配额

revoke quota 需要指定卷名以及 quotaId，这个接口在准备删除 quota 的时候执行，目的是让 quota 目录下的（包括 quota 目录自身）的已有文件和目录该 quotaId 失效。整个删除 quota 的流程先执行 quota revoke，然后通过 quota list 查询确认 USEDFILES 和 USEDBYTES 的值为0，再进行 quota delete 操作。

```bash
cfs-cli quota revoke [volname] [quotaId] [flags]
```

```bash
Flags:
      --forceInode uint            force revoke quota inode
  -h, --help                       help for revoke
      --maxConcurrencyInode uint   max concurrency delete Inodes (default 1000)
```

## 删除配额

delete quota 需要指定卷名以及 quotaId

```bash
cfs-cli quota delete [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help   help for delete
  -y, --yes    Do not prompt to clear the quota of inodes
```

## 更新配额

update quota 需要指定卷名以及 quotaId，目前可以更新的值只有 maxBytes 和 maxFiles

```bash
cfs-cli quota update [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help            help for update
      --maxBytes uint   Specify quota max bytes
      --maxFiles uint   Specify quota max files
```

## 列出卷配额信息

list quota 需要指定卷名，遍历出所有该卷的 quota 信息

``` bash
cfs-cli quota list [volname] [flags]
```

```bash
Flags:
  -h, --help   help for list
```

## 列出所有卷的配额信息

不带任何参数，遍历出所有带 quota 的卷信息

```bash
cfs-cli quota listAll [flags]
```

```bash
Flags:
  -h, --help   help for listAll
```

## 查看某个inode的配额信息

查看具体的某个 inode 是否带有 quota 信息

``` bash
cfs-cli quota getInode [volname] [inode] [flags]
```

```bash
Flags:
  -h, --help   help for getInode
```
