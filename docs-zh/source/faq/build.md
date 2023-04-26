# 编译问题

## 本机编译，其他机器无法启动

- 首先请确认使用 PORTABLE=1 make static_lib 命令编译rocksdb
- 然后使用ldd命令查看依赖的库，在机器上是否安装，安装缺少的库后，执行 ldconfig 命令

## ZSTD_versionNumber未定义

可以使用下面两种方式解决
- CGO_LDFLAGS添加指定库即可编译，例如：`CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lzstd"` 这种方式，要求其他部署机器上也要安装 zstd 库
- 删除自动探测是否安装zstd库的脚本，文件位置示例: `rockdb-5.9.2/build_tools/build_detect_platform`
   删除如下内容
   ```bash
   # Test whether zstd library is installed
       $CXX $CFLAGS $COMMON_FLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
         #include <zstd.h>
         int main() {}
   EOF
       if [ "$?" = 0 ]; then
           COMMON_FLAGS="$COMMON_FLAGS -DZSTD"
           PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lzstd"
           JAVA_LDFLAGS="$JAVA_LDFLAGS -lzstd"
       fi
   ```
  

## rocksdb编译问题

编译纠删码子系统显示报错 `fatal error: rocksdb/c.h: no such file or directory...`
- 首先确认`.deps/include/rocksdb`目录下是否存在报错所指向的文件， 
- 如果存在 可`source env.sh`后再次尝试，如果没有该文件或者仍然报错，可将`.deps`目录下rocksdb相关的文件全部清理，然后重新编译。
   
## cannot find -lbz2

编译时候如果报错 `/usr/bin/ld: cannot find -lbz2`，确认是否安装`bzip2-devel`（版本1.0.6及以上）

## cannot find -lz

编译时候如果报错 `/usr/bin/ld: cannot find -lz`，确认是否安装`zlib-devel`（版本1.2.7及以上）