# Optimizing FUSE Parameters

Properly adjusting the kernel FUSE parameters can achieve better performance in sequential writing and high-concurrency scenarios. You can refer to the following steps:

1. Obtain the Linux kernel source code.

   Download the corresponding Linux kernel source code package and install the source code. The source code installation directory is `~/rpmbuild/BUILD/`.

   ``` bash
   rpm -i kernel-3.10.0-327.28.3.el7.src.rpm 2>&1 | grep -v exist
   cd ~/rpmbuild/SPECS
   rpmbuild -bp --target=$(uname -m) kernel.spec
   ```

2. Optimize the Linux FUSE kernel module parameters.

   To achieve optimal performance, you can modify the FUSE parameters `FUSE_MAX_PAGES_PER_REQ` and `FUSE_DEFAULT_MAX_BACKGROUND`. The optimized reference values are as follows:

   ``` C
   /* fs/fuse/fuse_i.h */
   #define FUSE_MAX_PAGES_PER_REQ 256
   /* fs/fuse/inode.c */
   #define FUSE_DEFAULT_MAX_BACKGROUND 32
   ```

3. Compile the corresponding version of the Linux kernel module.

   ``` bash
   yum install kernel-devel-3.10.0-327.28.3.el7.x86_64
   cd ~/rpmbuild/BUILD/kernel-3.10.0-327.28.3.el7/linux-3.10.0-327.28.3.el7.x86_64/fs/fuse
   make -C /lib/modules/`uname -r`/build M=$PWD
   ```

4. Insert the kernel module.

   ``` bash
   cp fuse.ko /lib/modules/`uname -r`/kernel/fs/fuse
   rmmod fuse
   depmod -a
   modprobe fuse
   ```