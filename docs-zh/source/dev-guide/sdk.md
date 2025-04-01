# SDK 使用手册

## libsdk 简介
libsdk 是 CubeFS 文件存储的用户态客户端库，用于应用程序与 CubeFS 分布式文件系统进行交互。提供了对文件系统的操作接口，包括文件和目录的创建、读取、写入和删除等。允许应用程序直接连接到 CubeFS 存储集群，不经过内核态挂载和转发。
应用程序使用libsdk接口，需要引用 libsdk 提供的 libcfs.h(在项目目录client/libsdk中) 和 libcfs.so(make libsdk编译得到)

## libsdk 访问流程
1. 应用程序创建 client，对应 cfs_new_client
2. 应用程序设置 client 配置参数，对应 cfs_set_client
3. 应用程序启动 client，对应 cfs_start_client
4. 应用程序调用对应的文件处理接口，对文件执行 open，read，write，close 等操作
5. 应用程序关闭client，对应 cfs_close_client

## libsdk 优点
灵活性：可以使用各种编程语言和框架来开发，从而提供更大的灵活性和自由度。
独立性：在用户空间中运行，相对独立于操作系统内核。可以更容易地升级和调试客户端程序，而无需关注操作系统的限制和依赖。
易扩展：用户态客户端可以实现更高级的功能和协议，以满足特定的应用需求。
高性能：相比 fuse 挂载，libsdk 减少了内核态的转发，有助于性能提高。

## libsdk 接口使用说明
### cfs_new_client
```
extern int64_t cfs_new_client();
```
创建一个新的客户端。返回值是一个 int64 类型的值，这个值是新创建的客户端的 ID。

### cfs_set_client
```
extern int cfs_set_client(int64_t id, char* key, char* val);
参数配置示例：
cfs_set_client(client_id, "volName", "test");
cfs_set_client(client_id, "accessKey", "WgkHUb03XViuRkHX");
cfs_set_client(client_id, "secretKey", "oMQsYfGNcEdaUbbrw5D2GEp092RgNvQb");
cfs_set_client(client_id, "masterAddr", "172.16.1.101:17010");
cfs_set_client(client_id, "logDir", "/home/test_sdk/log");
cfs_set_client(client_id, "logLevel", "debug");
cfs_set_client(client_id, "enableSummary", "true");
```
设置客户端的配置项。

参数：

id: 客户端的 ID

key: 配置项的键

val: 配置项的值

返回值：

statusOK：成功

statusEINVAL：非法参数

### cfs_start_client
```
extern int cfs_start_client(int64_t id);
```
启动客户端。

参数：

id：客户的 ID

返回值：

statusOK: 成功

statusEINVAL：非法参数

statusEIO:  io 错误

### cfs_close_client
```
extern void cfs_close_client(int64_t id);
```

关闭客户端。

参数：

id：客户的 ID

### cfs_open
```
extern int cfs_open(int64_t id, char* path, int flags, mode_t mode);
```
打开/创建文件。

参数:

id：表示 client 的 ID

path：表示文件的路径

flags：表示打开文件的标志

mode：表示文件的权限模式

返回值：

返回值大于 0，表示成功时返回的文件描述符；失败时返回小于 0 的值。

statusEINVAL：非法参数

statusEACCES： 权限错误

statusEMFILE：文件描述符已达上限

statusEIO: io 错误

### cfs_close
```
extern void cfs_close(int64_t id, int fd);
```
关闭文件。

参数：

id: 表示 client 的 ID

fd: 表示文件描述符

### cfs_read
```
extern ssize_t cfs_read(int64_t id, int fd, void* buf, size_t size, off_t off);
```
从指定的文件描述符中读取数据。

参数：

id: 表示 client 的 ID

fd: 表示文件描述符

buf: 指向内存区域的指针，用于存储读取的数据

size: 表示要读取的数据的大小

off: 表示文件的偏移量

返回值：

返回值大于 0，表示实际读取的数据的大小。失败时返回小于 0 的值。

statusEINVAL：非法参数

statusEACCES： 权限错误

statusEBADFD：错误文件描述符

statusEIO: io 错误

### cfs_write
```
extern ssize_t cfs_write(int64_t id, int fd, void* buf, size_t size, off_t off);
```
向指定的文件描述符中写数据。

参数：

id: 表示 client 的 ID

fd: 表示文件描述符

buf: 指向要写的数据缓存区

size: 表示缓冲区的大小

off: 表示偏移量

返回值:

返回值大于 0，表示写入的字节数。失败时返回小于 0 的值。

statusEINVAL：非法参数

statusEACCES： 权限错误

statusEBADFD：错误文件描述符

statusENOSPC：空间不足

statusEIO: io 错误

### cfs_flush
```
extern int cfs_flush(int64_t id, int fd);
```
flush 文件

参数：

id: 表示 client 的 ID

fd: 表示文件描述符

返回值：

成功时返回 0，失败时返回小于 0 的值。

statusOK：成功

statusEINVAL：非法参数

statusEBADFD：错误文件描述符

statusEIO: io 错误

### cfs_truncate
```
extern int cfs_truncate(int64_t id, int fd, size_t size);
```
truncate 文件。

参数：

id: 表示 client 的 ID

fd: 表示文件描述符

size: 表示 truncate 文件后的大小

返回值：

成功时返回 0，失败时返回小于 0 的值。

statusOK：成功

statusEINVAL：非法参数

statusEBADFD：错误文件描述符

statusEIO: io 错误

### cfs_unlink
```
extern int cfs_unlink(int64_t id, char* path);
```
删除文件。

参数：

id: 表示 client 的 ID

path：文件路径

返回值：

成功时返回 0，失败时返回小于 0 的值。

statusOK：成功

statusEINVAL：非法参数

statusEISDIR：是目录

### cfs_mkdirs
```
extern int cfs_mkdirs(int64_t id, char* path, mode_t mode);
```
创建多级目录。

参数：

id：表示 client 的 ID

path：表示要创建的目录路径

mode：表示权限模式

返回值：

成功时返回 0，失败时返回小于 0 的值。

statusOK：成功

statusEINVAL：非法参数

statusEEXIST：已存在

### cfs_readdir
```
extern int cfs_readdir(int64_t id, int fd, GoSlice dirents, int count);
```
读取目录的文件和子目录列表。

参数:

id: 表示 client 的 ID

fd: 文件描述符

dirents: 存储文件和子目录信息的数组

count: 要读取的文件和子目录数量

返回值：

返回值大于 0，表示读取的目录项数。失败时返回小于 0 的值。

statusEINVAL：非法参数

statusEBADFD：错误文件描述符

### cfs_lsdir
```
extern int cfs_lsdir(int64_t id, int fd, GoSlice direntsInfo, int count);
```
列出指定目录下的文件和子目录信息，包括元数据信息。

参数:

id: 表示 client 的 ID

fd: 文件描述符

direntsInfo: 存储文件和子目录信息的数组

count: 要读取的文件和子目录数量

返回值:

返回值大于 0，表示读取的目录项数。失败时返回小于 0 的值。

statusEINVAL：非法参数

statusEBADFD：错误文件描述符

### cfs_rmdir
```
extern int cfs_rmdir(int64_t id, char* path);
```
删除指定路径的目录。

参数：

id: 表示 client 的 ID

path：目录路径

返回值：

成功时返回 0，失败时返回小于 0 的值

### cfs_rename
```
extern int cfs_rename(int64_t id, char* from, char* to, GoUint8 overwritten);
```
重命名一个文件或目录。

参数：

id：表示 client 的 ID

from：原始文件或目录的路径

to：新的文件或目录的路径

overwritten：是否允许覆盖已存在的文件

返回值：

成功时返回 0，失败时返回小于 0 的值

### cfs_getattr
```
extern int cfs_getattr(int64_t id, char* path, struct cfs_stat_info* stat);
```
用于获取文件的属性。

参数：

id: 表示 client 的 ID

path: 表示文件的路径。

stat: C 语言结构体 cfs_stat_info 的指针，用于存储文件的属性信息。定义在libcfs.h中。

返回值：

成功时返回 0，失败时返回小于 0 的值

### cfs_setattr
```
extern int cfs_setattr(int64_t id, char* path, struct cfs_stat_info* stat, int valid);
```
用于设置文件的属性。

参数：

id: 表示 client 的 ID

path: 表示文件的路径

stat：指向结构体 cfs_stat_info 的指针，包含了文件或目录的属性信息。定义在libcfs.h中。

valid：表示哪些属性信息是有效的

返回值：

成功时返回 0，失败时返回小于 0 的值