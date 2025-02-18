# 保活脚本
保活脚本将在3.5.1版本提供

## 运行指南
进入/tool/keepalive目录，把三个脚本都取出放在指定位置，执行命令
```
./cbfs_auto_install.sh --mountPoint arg1 -- volName arg2 --owner arg3 -- accessKey arg4 --secretKey arg5 --masterAddr arg6 --logDir arg7 ---logLevel arg8 --rdOnly arg9 --subdir arg10 --clientPath arg11
```

* 执行命令前需要先创建用户和卷
* 如果在非root权限下执行，执行命令前需要在 `/etc/fuse.conf` 中添加 `user_allow_other`

## 参数说明
必要参数
* mountPoint: 需要挂载的本地目录（如果需要启动多个实例，需要区分）
* volName: 申请的卷名, 如 ltptest
* owner: 申请的账号，如 prod_xxx
* accessKey
* secretKey
* master 地址
* logDir: 客户端日志目录（如果需要启动多个实例，需要区分）
* clientPath: 客户端路径

可选参数
* logLevel: 日志级别，默认为warn
* rdOnly: 只读挂载，默认为false
* subdir: 以提供的子目录形式挂载，默认为 /

## 运行说明
`cbfs_auto_install.sh` 会根据提供的参数在 `./volName` 路径下创建 `client.conf`，并挂载卷，最后会将重启规则添加到`crontab`中。cbfs_restart 脚本默认每分钟执行一次。

cbfs_restart.sh：用于重启崩溃的客户端，定时自动挂载
cbfs_clean.sh：用于清理挂载点和 crontab 规则，需要手动执行
