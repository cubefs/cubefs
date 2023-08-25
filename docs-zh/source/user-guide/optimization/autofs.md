# Cfsauto

Autofs[1] 是一个可根据需要自动挂载指定目录的程序。它基于一个内核模块运行以实现高效率，并且可以同时管理本地目录和网络共享。这些自动挂载点仅会在被访问时挂载，一定时间内不活动后即会被卸载。这种按需行为可节省带宽，并实现比 `/etc/fstab` 管理的静态挂载更高的性能。虽然 autofs 是控制脚本，但 automount 才是实际执行自动挂载的命令（守护程序）。

这里介绍 CubeFS 对 mount、Autofs 特性的支持，以及结合 SSSD[2]、LDAP[3] 等周边生态的应用实践。

Cfsauto[4] 程序通过将 mount 选项转换为 CubeFS 参数，实现了 CubeFS 客户端侧文件系统挂载、Fuse 文件系统挂载列表展示等功能。

CubeFS Autofs LDAP 场景示意图如下：

![示意图](./pic/autofs-1.png)


## 部署

### 安装

>基于Master分支部署举例
```bash
wget https://github.com/cubefs/cubefs/archive/refs/heads/master.zip -O cubefs.zip
unzip -o cubefs.zip -d cubefs
cd cubefs/autofs
go build -v -ldflags="-X main.buildVersion=1.0.0" -o /usr/local/bin/cfsauto
```

### 环境变量

* `CFS_CLIENT_PATH`：cfs-client 程序路径，默认为 `/etc/cfs/cfs-client`。
* `CFSAUTO_LOG_FILE`：cfsauto 日志文件路径，默认为 `/var/log/cfsauto.log`。
## Mount 挂载示例

### CubeFS 挂载

方式一：使用 `mount` 命令挂载

mount -t fuse :cfsauto {挂载点} -o {挂载选项}

```bash
mount -t fuse :cfsauto  /home/cubetest3 -o subdir=subd,volName=project1,owner=123,accessKey=abc,secretKey=xyz,masterAddr=10.0.0.12:17010,logDir=/var/logs/cfs/log,enablePosixACL,logLevel=debug
```

方式二：使用 `cfsauto` 命令挂载

cfsauto {挂载点} -o {挂载选项}

```bash
cfsauto  /home/cubetest3  -o subdir=subd,volName=project1,owner=123,accessKey=abc,secretKey=xyz,masterAddr=10.0.0.12:17010,logDir=/var/logs/cfs/log,enablePosixACL,logLevel=debug
```

### CubeFS 挂载列表展示

```bash
# cfsauto 
cubefs-vol3 on /home/cubetest3 type fuse.cubefs (rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other)
```

## Autofs 配置挂载示例

autofs 的配置文件是 `/etc/auto.master`，这个文件指定了自动挂载的根目录和配置文件所在位置。当我们访问这个根目录下的子目录时，autofs会根据配置文件自动地挂载相应的文件系统。

`/etc/auto.master` 示例：

```bash
/- /etc/auto.direct -ro,hard,intr,nolock
# 增加
/tmp/cfstest /etc/auto.cfs

+auto_master
```

`/etc/auto.cfs` 示例：

```plain
autodir -fstype=fuse,subdir=subdir,volName=vol3,owner=cfs,masterAddr=10.0.0.1:17010,logDir=/home/service/logauto,enablePosixACL,logLever=debug :cfsauto
```

autofs 调试：`automount -f --debug`

挂载示例：

![挂载示例](./pic/autofs-2.png)


## 集成 SSSD、LDAP 自动挂载

### LDAP 配置

LDAP 自动挂载模块配置挂载点及 CubeFS 挂载选项。

automountkey 配置挂载点，如：fusetest

automountInformation 配置 CubeFS 挂载选项，如下：

```plain
-fstype=fuse,subdir=subd,volName=project1,owner=123,accessKey=abc,secretKey=xyz,masterAddr=10.0.0.12:17010,logDir=/var/logs/cfs/log,enablePosixACL,logLevel=debug :cfsauto
```
LDAP automount 配置示例：

![配置示例](./pic/autofs-3.png)


### SSSD 配置

autofs 模块关键配置示例如下：

```bash
autofs_provider			= ldap
ldap_autofs_search_base		= ou=mounts,dc=example,dc=com
ldap_autofs_map_master_name	= auto_master
ldap_autofs_map_object_class	= automountMap
ldap_autofs_entry_object_class	= automount
ldap_autofs_map_name		= automountMapName
ldap_autofs_entry_key		= automountKey
ldap_autofs_entry_value		= automountInformation
```

### 挂载示例

![挂载示例](./pic/autofs-4.png)


## 参考

[1] AutoFS: [https://documentation.suse.com/zh-cn/sles/15-SP3/html/SLES-all/cha-autofs.html](https://documentation.suse.com/zh-cn/sles/15-SP3/html/SLES-all/cha-autofs.html)

[2] SSSD: [https://sssd.io/docs/introduction.html](https://sssd.io/docs/introduction.html)

[3] LDAP: [https://www.ibm.com/docs/en/zos/2.1.0?topic=SSLTBW_2.1.0/com.ibm.zos.v2r1.cbdu100/cbd2ug00152.html](https://www.ibm.com/docs/en/zos/2.1.0?topic=SSLTBW_2.1.0/com.ibm.zos.v2r1.cbdu100/cbd2ug00152.html)

[4] CubeFS AutoFS: [https://github.com/cubefs/cubefs/tree/master/autofs](https://github.com/cubefs/cubefs/tree/master/autofs)

 

