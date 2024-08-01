# libsdk

libsdk 是 CubeFS 文件存储的用户态客户端库，用于应用程序与 CubeFS 分布式文件系统进行交互。它提供了对文件系统的操作接口，包括文件和目录的创建、读取、写入和删除等。允许应用程序直接连接到 CubeFS 存储集群，不经过内核态。

## **libsdk 优点**

- 灵活性：可以使用各种编程语言和框架来开发，从而提供更大的灵活性和自由度。
- 独立性：在用户空间中运行，相对独立于操作系统内核。可以更容易地升级和调试客户端程序，而无需关注操作系统的限制和依赖。
- 易扩展：用户态客户端可以实现更高级的功能和协议，以满足特定的应用需求。
- 高性能：相比 FUSE 挂载，libsdk 减少了内核态的转发，有助于性能提高。

## **libsdk 访问流程**

1. 应用程序创建 client，对应接口 cfs_new_client
2. 应用程序设置 client 配置参数，对应接口 cfs_set_client
3. 应用程序启动 client，对应接口 cfs_start_client
4. 应用程序调用对应的文件处理接口，对文件执行 open，read，write，close 等，详情请查看如下接口描述。
## libsdk 接口

### **状态码**

libsdk 的状态码与内核返回的状态码一致，只是再包装了一层而已。

|**状态码**|**数值**|**描述**|
|:----|:----|:----|
|statusOK|0|正常|
|statusEIO|-5|io 错误|
|statusEINVAL|-16|参数错误|
|statusEEXIST|-17|已存在|
|statusEBADFD|-77|错误文件描述符|
|statusEACCES|-13|无法访问|
|statusEMFILE|-18|文件描述符已达上限|
|statusENOTDIR|-14|非目录错误|
|statusEISDIR|-15|目录错误|
|statusENOSPC|-28|空间不足|
|statusEPERM|-1|权限错误|

### **接口**

#### **cfs_new_client**

```c++
extern int64_t cfs_new_client();
```
创建一个新的客户端。返回值是一个 int64 类型的值，这个值是新创建的客户端的 ID。

---
#### **cfs_set_client**

```c++
extern int cfs_set_client(int64_t id, char* key, char* val);
cfs_set_client(client_id, "volName", "test");
cfs_set_client(client_id, "accessKey", "WgkHUb03XViuRkHX");
cfs_set_client(client_id, "secretKey", "oMQsYfGNcEdaUbbrw5D2GEp092RgNvQb");
cfs_set_client(client_id, "masterAddr", "192.168.1.123:17010");
cfs_set_client(client_id, "logDir", "/home/test_sdk/log");
cfs_set_client(client_id, "logLevel", "debug");
```
设置客户端的配置项。
cfs_set_client 有如下几个参数是必须要设置的：

* **volName**：卷名称。
* **accessKey**：卷所属用户的 accessKey。可用 cli 工具命令 user list 查看。
* **secretKey**：卷所属用户的 secretKey。可用 cli 工具命令 user list 查看。
* **masterAddr**：CubeFS 的 master 地址，多个 master 地址用英文逗号隔开。
* **logDir**：日志目录
* **logLevel**：日志级别。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|key|配置项的键|
|val|配置项的值|

**返回值**

参考状态码列表


---
#### **cfs_start_client**

```c++
extern int cfs_start_client(int64_t id);
```
启动客户端。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|

**返回值**

参考状态码列表


---
#### **cfs_close_client**

```c++
extern void cfs_close_client(int64_t id);
```
关闭客户端。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|


---
#### **cfs_open**

```c++
extern int cfs_open(int64_t id, char* path, int flags, mode_t mode);
```
打开/创建文件。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|文件的路径|
|flags|打开文件的标志|
|mode|文件的权限模式|

**返回值：**

返回值大于 0，表示成功时返回的文件描述符；失败时参考状态码列表。


---
#### **cfs_close**

```c++
extern void cfs_close(int64_t id, int fd);
```
关闭文件。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|


---
#### **cfs_read**

```c++
extern ssize_t cfs_read(int64_t id, int fd, void* buf, size_t size, off_t off);
```
从指定的文件描述符中读取数据。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|
|buf|指向内存区域的指针，用于存储读取的数据|
|size|要读取的数据的大小|
|off|文件的偏移量|

**返回值：**

返回值大于 0，表示实际读取的数据的大小。失败时参考状态码列表。


---
#### **cfs_write**

```c++
extern ssize_t cfs_write(int64_t id, int fd, void* buf, size_t size, off_t off);
```
向指定的文件描述符中写数据。
**参数**：

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|
|buf|指向要写的数据缓存区|
|size|缓冲区的大小|
|off|文件的偏移量|

**返回值:**

返回值大于 0，表示写入的字节数。失败时参考状态码列表。


---
#### **cfs_flush**

```c++
extern int cfs_flush(int64_t id, int fd);
```
下刷文件缓存的数据到存储集群。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|

**返回值：**

成功时返回 0，失败时参考状态码列表。


---
#### **cfs_truncate**

```c++
extern int cfs_truncate(int64_t id, int fd, size_t size);
```
截断文件到指定大小。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|
|size| truncate 文件后的大小|

**返回值：**

成功时返回 0，失败时参考状态码列表。


---
#### **cfs_unlink**

```c++
extern int cfs_unlink(int64_t id, char* path);
```
删除文件。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|文件路径|

**返回值：**

成功时返回 0，失败时参考状态码列表。


---
#### **cfs_mkdirs**

```c++
extern int cfs_mkdirs(int64_t id, char* path, mode_t mode);
```
创建多级目录。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|要创建的目录路径|
|mode|权限模式|

**返回值：**

成功时返回 0，失败时参考状态码列表。


---
#### **cfs_readdir**

```c++
extern int cfs_readdir(int64_t id, int fd, GoSlice dirents, int count);
```
读取目录的文件和子目录列表。
**参数:**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|
|dirents|存储文件和子目录信息的数组|
|count|要读取的文件和子目录数量|

**返回值：**

返回值大于 0，表示读取的目录项数。失败时参考状态码列表。


---
#### **cfs_lsdir**

```c++
extern int cfs_lsdir(int64_t id, int fd, GoSlice direntsInfo, int count);
```
列出指定目录下的文件和子目录信息，包括元数据信息。
**参数:**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|fd|文件描述符|
|direntsInfo|存储文件和子目录信息的数组|
|count|要读取的文件和子目录数量|

**返回值:**

返回值大于 0，表示读取的目录项数。失败时参考状态码列表。


---
#### **cfs_rmdir**

```c++
extern int cfs_rmdir(int64_t id, char* path);
```
删除指定路径的目录。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|目录路径|

**返回值：**

成功时返回 0，失败时参考状态码列表


---
#### **cfs_rename**

```c++
extern int cfs_rename(int64_t id, char* from, char* to, GoUint8 overwritten);
```
重命名一个文件或目录。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|from|原始文件或目录的路径|
|to|新的文件或目录的路径|
|overwritten|是否允许覆盖已存在的文件|

**返回值：**

成功时返回 0，失败时参考状态码列表


---
#### **cfs_getattr**

```c++
extern int cfs_getattr(int64_t id, char* path, struct cfs_stat_info* stat);
```
用于获取文件的属性。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|文件的路径|
|stat|C 语言结构体 cfs_stat_info 的指针，用于存储文件的属性信息|

**返回值：**

成功时返回 0，失败时返回小于 0 的值


---
#### **cfs_setattr**

```c++
extern int cfs_setattr(int64_t id, char* path, struct cfs_stat_info* stat, int valid);
```
用于设置文件的属性。
**参数：**

|参数项|描述|
|:----|:----|
|id|客户端的 ID|
|path|文件的路径|
|stat|C 语言结构体 cfs_stat_info 的指针，用于存储文件的属性信息|
|valid|哪些属性信息是有效的|

**返回值：**

成功时返回 0，失败时参考状态码列表


## 示例代码

下面是一个使用 libsdk 的示例代码。示例程序目录结构如下所示：

```
libcfs-test
--libcfs.h
--libcfs.so
--sdk-example.c
```

libcfs.h 头文件和 libcfs.so 动态库可以从 CubeFS github 代码库的 Releases 获取，或者自行编译代码后在 build/bin 下获取。sdk-example.c 代码文件内容如下：

```c++
//gcc sdk-example.c -o sdk-example ./libcfs.so
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include "libcfs.h"

int64_t client_id = 0;

char *dir_path="/lib-sdk";
void* test_mkdir() {
    int ret = 0;
    ret = cfs_mkdirs(client_id, dir_path, 0x777);
    if (ret) {
        printf("mkdirs %s failed, ret=%d\n", dir_path, ret);
    }
    return NULL;
}



char *read_write_path="/lib-sdk/test_file";
void* test_write() {
    char *buf="123456";
    int fd = cfs_open(client_id, read_write_path, O_CREAT|O_RDWR, 0644);
    if (fd < 0) {
        printf("open file failed, fd=%d, err=%d\n",fd, errno);
        return NULL;
    }
    int i = 0;
    for (i = 0; i < 100; i++) {
        int ret = cfs_write(client_id, fd, buf, strlen(buf), 0);
        if (ret < 0) {
            printf("file%d write failed\n");
            cfs_close(client_id, fd);
            return NULL;
        }
    }
    cfs_flush(client_id, fd);
    cfs_close(client_id, fd);

    return NULL;
}

void* test_read() {
    sleep(3);
    char buf[1024];
    int test_read_fd = cfs_open(client_id, read_write_path, O_RDWR, 0);
    if (test_read_fd < 0) {
        printf("open file failed, fd=%d, err=%d\n",test_read_fd, errno);
        return NULL;
    }
    memset(buf, 0, sizeof(1024));
    int i = 0;
    for (i=0; i < 100; i++) {
        int ret = cfs_read(client_id, test_read_fd, buf, 1024, 0);
        if (ret<0) {
            break;
        }
    }
    cfs_close(client_id, test_read_fd);
    return NULL;
}

int main() {
    client_id = cfs_new_client();
    printf("client_id=%ld\n", client_id);
    cfs_set_client(client_id, "volName", "xxx");
    cfs_set_client(client_id, "accessKey", "xxx");
    cfs_set_client(client_id, "secretKey", "xxx");
    cfs_set_client(client_id, "masterAddr", "192.168.1.123:17010");
    cfs_set_client(client_id, "logDir", "/home/sdk-log");
    cfs_set_client(client_id, "logLevel", "debug");
    int ret = cfs_start_client(client_id);
    if (ret) {
	    printf("start client failed\n");
	    return -1;
    }
    printf("start client %ld success\n", client_id);

    // create dir
    test_mkdir();
    // open and write
    test_write();
    // read
    test_read();
    // close client
    cfs_close_client(client_id);

    return 0;
}
```

可以执行如下命令编译：

```c++
gcc sdk-example.c -o sdk-example ./libcfs.so
```
编译后二进制程序会出现在当前目录下。
二进制程序 sdk-example 的运行需要 libcfs.so 动态库，所以需要将这两者放在同一个目录。

