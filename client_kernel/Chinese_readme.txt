1. 可以编译通过的操作系统有: centos 8.5, rocky 9.2, ubuntu 20, centos 7.6, centos 7.9
别的内核版本可能需要对代码进行一定的适配修改。
其中centos 8.5的内核: 4.18.0-348.7.1.el8_5.x86_64
Rocky9.2的内核: 5.14.0-284.11.1.el9_2.x86_64
centos 7.6的内核: 3.10.0-1160.114.2.el7.x86_64
centos 7.9的内核: 3.10.0-1160.99.1.el7.x86_64
ubuntu 20的内核: 5.15.0-91-generic

2. 在下载的cubefs目录下，运行编译命令:
cd client_kernel
./configure
make
编译的结果是模块cubefs.ko。
如果只使用TCP网络链接，不使用RDMA，就可以编译：
make no_rdma
这样的cubefs.ko不包含rdma的接口，从而避免加载时的错误。
目前rdma的接口只支持对接kernel原生的代码实现。

3. 使用的例子:
加载依赖的RDMA模块
modprobe rdma_cm
加载客户端模块：
insmod cubefs.ko
挂载tcp写链路的卷:
mount -t cubefs -o owner=whc,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 //10.177.182.171:17010,10.177.80.10:17010,10.177.80.11:17010/whc /mnt/cubefs
如果网卡支持RDMA，也可以挂载RDMA写链路的卷:
mount -t cubefs -o owner=whc,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000,enable_rdma=true,rdma_port=17360 //10.177.182.171:17010,10.177.80.10:17010,10.177.80.11:17010/whc /mnt/cubefs

注意:
1. 客户端依赖的是内核自带的RDMA接口和模块，请不要安装软件MLNX_OFED_LINUX的内核部分。
2. 在加载客户端模块cubefs.ko之前，需要加载依赖模块。可以使用命令进行加载：modprobe rdma_cm


问答:
1. 如果在编译或者加载模块时，找不到RDMA的链接，可以如下进行设置，然后重新编译:
export KBUILD_EXTRA_SYMBOLS=/usr/src/kernels/4.18.0-348.7.1.el8_5.x86_64/Module.symvers
其中4.18.0-348.7.1.el8_5.x86_64是内核版本，需要根据实际情况修改。

2. 为什么不能安装MLNX_OFED_LINUX的内核部分?
一旦安装MLNX_OFED_LINUX的内核软件，就会覆盖内核模块的接口。编译时，生成的接口版本和加载时就可能出现不匹配。

3. 为什么centos 7.6不能工作？
这个可能是因为没有加载rdma的设备。请尝试如下方式加载：
yum install -y rdma-core librdmacm libibverbs
modprobe rdma_cm
modprobe mlx5_ib
如果加载成功，应该就可以在/dev/infiniband/目录下看到几个rdma设备。

4. 内核态客户端是否可以和fuse客户端共同使用？
目前在centos7.6上面测试表明，如果insmod cubefs.ko以后，在启动fuse客户端就会失败。命令./cfs-client -c ./conf.cfs报错。
如果先rmmod cubefs，上面的命令就可以成功运行。这个可能和两个软件启动时，都加载了rdma模块有关系。
别的操作系统有待验证。
