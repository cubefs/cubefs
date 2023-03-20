# Fuse客户端问题

## 內存及性能优化相关

- Fuse客户端占用内存过高，超过了2GB，对其他业务影响过大
  - 离线修改：在配置文件中设置readRate和writeRate参数，重启客户端，[详情请参考](../maintenance/config.md)
  - 在线修改：`http://{clientIP}:{profPort} /rate/set?write=800&read=800`
- Fuse客户端性能优化，[请参考Fuse优化](../user-guide/fuse.md)

## 挂载问题

1. 支持子目录挂载吗?

支持。配置文件中设置subdir即可

2. 挂载失败有哪些原因？

挂载失败后，输出以下信息

```bash
$ ... err(readFromProcess: sub-process: fusermount: exec: "fusermount": executable file not found in $PATH)
```

- 查看是否已安装fuse，如果没有则安装

```bash
$ rpm –qa|grep fuse
$ yum install fuse
```
- 检查挂载目录是否存在
- 检查挂载点目录下是否为空
- 检查挂载点是否已经umount
- 检查挂载点状态是否正常，若挂载点 mnt 出现以下信息,需要先umount，再启动client

```bash
$ ls -lih
ls: cannot access 'mnt': Transport endpoint is not connected
total 0
6443448706 drwxr-xr-x 2 root root 73 Jul 29 06:19 bin
 811671493 drwxr-xr-x 2 root root 43 Jul 29 06:19 conf
6444590114 drwxr-xr-x 3 root root 28 Jul 29 06:20 log
         ? d????????? ? ?    ?     ?            ? mnt
 540443904 drwxr-xr-x 2 root root 45 Jul 29 06:19 script
```

- 检查配置文件是否正确，master地址 、volume name等信息
- 如果以上问题都不存在，通过client error日志定位错误，看是否是metanode或者master服务导致的挂载失败

## IO问题

1. IOPS过高导致客户端占用内存超过3GB甚至更高，有没有办法限制IOPS?

通过修改客户端rate limit来限制客户端响应io请求频率。

```bash
#查看当前iops：
$ http://[ClientIP]:[profPort]/rate/get
#设置iops，默认值-1代表不限制iops
$ http://[ClientIP]:[profPort]/rate/set?write=800&read=800
```

2. ls等操作io延迟过高?

- 因为客户端读写文件都是通过http协议，请检查网络状况是否健康
- 检查是否存在过载的mn，mn进程是否hang住，可以重启mn，或者扩充新的mn到集群中并且将过载mn上的部分mp下线以缓解mn压力

## 多客户端并发读写强一致

不是。CubeFS放宽了POSIX一致性语义，它只能确保文件/目录操作的顺序一致性，并没有任何阻止多个客户写入相同的文件/目录的leasing机制。这是因为在容器化环境中，许多情况下不需要严格的POSIX语义，即应用程序很少依赖文件系统来提供强一致性保障。并且在多租户系统中也很少会有两个互相独立的任务同时写入一个共享文件因此需要上层应用程序自行提供更严格的一致性保障。

## 能否接杀死client进程

不建议，最好走umount流程，umount后，client进程会自动停止。