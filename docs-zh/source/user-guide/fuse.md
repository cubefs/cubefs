# 优化配置FUSE参数

适当调整内核FUSE参数，能够在顺序写及高并发情况下获得更好的性能。具体可参考如下步骤：

1.  获取Linux内核源码

    下载对应的Linux内核源码包并且安装源码，源码安装目录为
    `~/rpmbuild/BUILD/`

    ``` bash
    rpm -i kernel-3.10.0-327.28.3.el7.src.rpm 2>&1 | grep -v exist
    cd ~/rpmbuild/SPECS
    rpmbuild -bp --target=$(uname -m) kernel.spec
    ```

2.  优化Linux FUSE内核模块参数

    为了达到最优的性能，可以修改内核FUSE的参数 `FUSE_MAX_PAGES_PER_REQ`
    和 `FUSE_DEFAULT_MAX_BACKGROUND` ，优化后的参考值如下：

    ``` C
    /* fs/fuse/fuse_i.h */
    #define FUSE_MAX_PAGES_PER_REQ 256
    /* fs/fuse/inode.c */
    #define FUSE_DEFAULT_MAX_BACKGROUND 32
    ```

3.  编译对应版本Linux内核模块

    ``` bash
    yum install kernel-devel-3.10.0-327.28.3.el7.x86_64
    cd ~/rpmbuild/BUILD/kernel-3.10.0-327.28.3.el7/linux-3.10.0-327.28.3.el7.x86_64/fs/fuse
    make -C /lib/modules/`uname -r`/build M=$PWD
    ```

4.  插入内核模块

    ``` bash
    cp fuse.ko /lib/modules/`uname -r`/kernel/fs/fuse
    rmmod fuse
    depmod -a
    modprobe fuse
    ```