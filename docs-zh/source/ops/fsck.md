# 一致性检测工具

fsck，全称 File System Consistency Check，主要用于检查和修复 Cubefs 文件系统的不一致和错误。该工具用于解决潜在的文件系统问题，如 mp 在多副本上的不一致问题等。使用 fsck 可以快速定位问题所在，从而降低集群日常运维的人力成本。

## fsck 工具编译
在 cubefs 目录下编译 fsck 工具，cfs-fsck 可执行程序会生成在 ./build/bin/ 目录下

```bash
$ make fsck
```

## fsck 命令

### 检测指定卷 inode 和 dentry 命令
检测出过时的需要被清理的 inode 和 dentry

``` bash
$ ./cfs-fsck check inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check dentry --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check both --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check both --vol "<volName>" --inode-list "inodes.txt" --dentry-list "dens.txt"
``` 

使用命令会在执行目录根据卷名生成 `_export_volname` 文件夹，目录结构如下。其中，`inode.dump` 和 `dentry.dump` 保存了卷中所有 inode 和 dentry 信息，`.obsolete` 后缀文件保存了过时 inode 和 dentry 的信息。可以通过向master发送请求获取 inode 和 dentry 信息，或者根据现有的文件进行分析。 

``` bash
.
├── dentry.dump
├── dentry.dump.obsolete
├── inode.dump
└── inode.dump.obsolete
``` 

### 检测 mp 在多副本中的一致性
v3.5.0 混合云版本新加入的功能，用于检测 mp 中的 inode 和 dentry 在不同副本之间是否一致
可选参数：vol 和 mp 至少要指定一个
* vol：指定卷名，会比较卷下所有 mp 是否一致
* mp：检测指定 mp 是否一致
* check-apply-id：选择是否跳过 applyid 不同的副本，默认为 false

``` bash
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --vol "<volName>" --mport "17220" --mp 1
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1
$ ./cfs-fsck check mp --master "127.0.0.1:17010" --mport "17220" --mp 1 --check-apply-id true
```

执行命令在 `_export_volname` 或 `_export_mpID` 文件夹下生成 `mpCheck.log` 文件，检测的信息会输入到该文件中。

### 清理 inode 命令
``` bash
$ ./cfs-fsck clean inode --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
```
清理要求:
1. inode.Nlink=0  
2. 从修改时间已过去24小时
3. 类型为普通文件而不是目录

删除 Nlink!=0 的垃圾数据，可以加上 -f 参数强制删除

### 获取信息命令
``` bash
$ ./cfs-fsck get locations --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
Inode: 1
Generation: 1
Size: 0

MetaPartition:
 Id:1
 Leader:172.16.1.103:17210
 Hosts:[172.16.1.102:17210 172.16.1.103:17210 172.16.1.101:17210]
```

``` bash
$./cfs-fsck get path --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
# 运行示例
Inode: 8388610, Valid: true
Path: /111.test
```

``` bash
$ ./cfs-fsck get summary --inode <inodeID> --master "127.0.0.1:17010" --vol "<volName>" --mport "17220"
# 运行示例
Inode: 8388610, Valid: true
Path(is file): /111.test, Bytes: 0
```