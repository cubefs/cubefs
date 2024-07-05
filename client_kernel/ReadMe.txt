1. The compiled suceessed system platform includes: centos 8.5, rocky 9.2, ubuntu 20

2. Compile commands:
cd client_kernel
./configure
make

3. Usage example:
modprobe rdma_cm
insmod cubefs.ko
The write path by tcp:
mount -t cubefs -o owner=whc,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 //10.177.182.171:17010,10.177.80.10:17010,10.177.80.11:17010/whc /mnt/cubefs
The write path with rdma:
mount -t cubefs -o owner=whc,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000,enable_rdma=true,rdma_port=17360 //10.177.182.171:17010,10.177.80.10:17010,10.177.80.11:17010/whc /mnt/cubefs

Notify:
The rdma module is linux kernel's embed one. Please don't install MLNX_OFED_LINUX kernel module.
Before inserting cubefs.ko, it requires to run command: modprobe rdma_cm.

FQA:
1. If can't find the kernel depended module, it can be added by such command:
export KBUILD_EXTRA_SYMBOLS=/usr/src/kernels/4.18.0-348.7.1.el8_5.x86_64/Module.symvers
The 4.18.0-348.7.1.el8_5.x86_64 is kernel version.

2. Why not install MLNX_OFED_LINUX kernel module?
The rdma api is linked to kernel's. If install others, it will over write the kernel's.
Then rdam api version will be mismatch.

3. CentOS 7.6 can't works. Why?
Please check whether the rdma device under /dev/infiniband/.
If not, please try the commands: 
yum install -y rdma-core librdmacm libibverbs
modprobe rdma_cm
modprobe mlx5_ib
