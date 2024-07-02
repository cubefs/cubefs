# libsdk

libsdk is a user-space client library for CubeFS file storage, used for applications to interact with the CubeFS distributed file system. It provides an operating interface for the file system, including the creation, reading, writing, and deletion of files and directories. It allows applications to connect directly to the CubeFS storage cluster without going through the kernel state.

## **libsdk advantages**

- Flexibility: It can be developed using a variety of programming languages ​​and frameworks, providing greater flexibility and freedom.

- Independence: It runs in user space and is relatively independent of the operating system kernel. It is easier to upgrade and debug client programs without paying attention to the limitations and dependencies of the operating system.

- Easy to expand: User-space clients can implement more advanced functions and protocols to meet specific application requirements.

- High performance: Compared with FUSE mounting, libsdk reduces kernel-state forwarding, which helps improve performance.

## **libsdk access process**

1. The application creates a client, corresponding to the interface cfs_new_client

2. The application sets the client configuration parameters, corresponding to the interface cfs_set_client

3. The application starts the client, corresponding to the interface cfs_start_client

4. The application calls the corresponding file processing interface to perform open, read, write, close, etc. on the file. For details, please refer to the following interface description.
## libsdk interface

### **Status code**

The status code of libsdk is consistent with the status code returned by the kernel, but it is just wrapped in another layer.

|**Status code**|**Value**|**Description**|
|:----|:----|:----|
|statusOK|0|Normal|
|statusEIO|-5|IO error|
|statusEINVAL|-16|Parameter error|
|statusEEXIST|-17|Already exists|
|statusEBADFD|-77|Wrong file descriptor|
|statusEACCES|-13|Unable to access|
|statusEMFILE|-18|File descriptor has reached the upper limit|
|statusENOTDIR|-14|Non-directory error|
|statusEISDIR|-15|Directory error|
|statusENOSPC|-28|Insufficient space|
|statusEPERM|-1|Permission error|

### **Interface**

#### **cfs_new_client**

```c++
extern int64_t cfs_new_client();
```
Create a new client. The return value is an int64 type value, which is the ID of the newly created client.

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
Set the client configuration items.
cfs_set_client has the following parameters that must be set:

* **volName**: volume name.
* **accessKey**: accessKey of the user to whom the volume belongs. You can use the cli tool command user list to view it.
* **secretKey**: secretKey of the user to whom the volume belongs. You can use the cli tool command user list to view it.
* **masterAddr**: master address of CubeFS. Multiple master addresses are separated by English commas.
* **logDir**: log directory
* **logLevel**: log level.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|key|Configuration item key|
|val|Configuration item value|

**Return value**

Reference status code list

---
#### **cfs_start_client**

```c++
extern int cfs_start_client(int64_t id);
```
Start the client.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|

**Return value**

Reference status code list

---
#### **cfs_close_client**

```c++
extern void cfs_close_client(int64_t id);
```
Close the client.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|

---
#### **cfs_open**

```c++
extern int cfs_open(int64_t id, char* path, int flags, mode_t mode);
```
Open/create a file.
**Parameter:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|File path|
|flags|Flags for opening a file|
|mode|File permission mode|

**Return value:**

A return value greater than 0 indicates a file descriptor returned on success; refer to the status code list for failure.

---
#### **cfs_close**

```c++
extern void cfs_close(int64_t id, int fd);
```
Close a file.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|

---
#### **cfs_read**

```c++
extern ssize_t cfs_read(int64_t id, int fd, void* buf, size_t size, off_t off);
```
Read data from the specified file descriptor.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|
|buf|Pointer to the memory area used to store the read data|
|size|Size of the data to be read|
|off|Offset of the file|

**Return value:**

The return value is greater than 0, indicating the size of the data actually read. Refer to the status code list in case of failure.

---
#### **cfs_write**

```c++
extern ssize_t cfs_write(int64_t id, int fd, void* buf, size_t size, off_t off);
```
Write data to the specified file descriptor.
**Parameter**:

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|
|buf|Points to the data buffer to be written|
|size|Buffer size|
|off|File offset|

**Return value:**

The return value is greater than 0, indicating the number of bytes written. Refer to the status code list for failure.

---
#### **cfs_flush**

```c++
extern int cfs_flush(int64_t id, int fd);
```
Flush the file cache data to the storage cluster.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|

**Return value:**

Return 0 if successful, refer to the status code list if failed.

---
#### **cfs_truncate**

```c++
extern int cfs_truncate(int64_t id, int fd, size_t size);
```
Truncate the file to the specified size.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|
|size|Size after truncate file|

**Return value:**

Return 0 if successful, refer to the status code list if failed.

---
#### **cfs_unlink**

```c++
extern int cfs_unlink(int64_t id, char* path);
```
Delete files.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|File path|

**Return value:**

Return 0 if successful, refer to the status code list if failed.

---
#### **cfs_mkdirs**

```c++
extern int cfs_mkdirs(int64_t id, char* path, mode_t mode);
```
Create multi-level directories.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|Directory path to be created|
|mode|Permission mode|

**Return value:**

Return 0 on success, refer to the status code list on failure.

---
#### **cfs_readdir**

```c++
extern int cfs_readdir(int64_t id, int fd, GoSlice dirents, int count);
```
Read the list of files and subdirectories in a directory.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|
|dirents|Array to store file and subdirectory information|
|count|Number of files and subdirectories to be read|

**Return value:**

The return value is greater than 0, indicating the number of directory entries read. Refer to the status code list on failure.

---
#### **cfs_lsdir**

```c++
extern int cfs_lsdir(int64_t id, int fd, GoSlice direntsInfo, int count);
```
List the files and subdirectories in the specified directory, including metadata information.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|fd|File descriptor|
|direntsInfo|Array to store file and subdirectory information|
|count|Number of files and subdirectories to read|

**Return value:**

The return value is greater than 0, indicating the number of directory entries read. Refer to the status code list for failure.

---
#### **cfs_rmdir**

```c++
extern int cfs_rmdir(int64_t id, char* path);
```
Delete the directory of the specified path.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|Directory path|

**Return value:**

Return 0 if successful, refer to the status code list if failed

---
#### **cfs_rename**

```c++
extern int cfs_rename(int64_t id, char* from, char* to, GoUint8 overwritten);
```
Rename a file or directory.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|from|Original file or directory path|
|to|New file or directory path|
|overwritten|Whether to allow overwriting of existing files|

**Return value:**

Return 0 if successful, refer to the status code list if failed

---
#### **cfs_getattr**

```c++
extern int cfs_getattr(int64_t id, char* path, struct cfs_stat_info* stat);
```
Used to obtain file attributes.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|File path|
|stat|Pointer to the C language structure cfs_stat_info, used to store file attribute information|

**Return value:**

Returns 0 on success and a value less than 0 on failure

---
#### **cfs_setattr**

```c++
extern int cfs_setattr(int64_t id, char* path, struct cfs_stat_info* stat, int valid);
```
Used to set file attributes.
**Parameters:**

|Parameter item|Description|
|:----|:----|
|id|Client ID|
|path|File path|
|stat|C language structure cfs_stat_info pointer, used to store file attribute information|
|valid|Which attribute information is valid|

**Return value:**

Return 0 on success, refer to the status code list on failure

## Sample code

The following is a sample code using libsdk. The sample program directory structure is as follows:

```
libcfs-test
--libcfs.h
--libcfs.so
--sdk-example.c
```

The libcfs.h header file and libcfs.so dynamic library can be obtained from Releases of the CubeFS github code library, or compiled by yourself and obtained in build/bin. The contents of the sdk-example.c code file are as follows:

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

You can compile by executing the following command:

```c++
gcc sdk-example.c -o sdk-example ./libcfs.so
```
After compilation, the binary program will appear in the current directory.
The binary program sdk-example requires the libcfs.so dynamic library to run, so both need to be placed in the same directory.