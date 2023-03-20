# 编译问题

## 纠删码子系统

1. 编译构建显示报错 `fatal error: rocksdb/c.h: no such file or directory...`
   - 首先确认`.deps/include/rocksdb`目录下是否存在报错所指向的文件， 
   - 如果存在 可`source env.sh`后再次尝试，如果没有该文件或者仍然报错，可将`.deps`目录下rocksdb相关的文件全部清理，然后重新编译。
   
