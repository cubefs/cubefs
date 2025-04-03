# SDK User 

## Introduction
Libsdk is a user-mode client library for CubeFS, which is used for applications to interact with the CubeFS distributed file system. It provides the operation interface to the file system, including the creation, reading, writing and deleting of files and directories. Applications are allowed to connect directly to the CubeFS storage cluster without kernel-mode mounting and forwarding.
To use the libsdk interface, the application needs to refer to libcfs.h(in the project directory client/libsdk) and libcfs.so(make libsdk compilation) provided by libsdk.

## Critical Process
1. The application creates a client corresponding to cfs_new_client.
2. The application sets the client configuration parameter, corresponding to cfs_set_client.
3. The application starts client, corresponding to cfs_start_client.
4. The application program invokes the corresponding file processing interface to perform open, read, write, close and other operations on the file.
5. The application close client, corresponding to cfs close client.

## Advantages
Flexibility: It can be developed using a variety of programming languages and frameworks, providing greater flexibility and freedom.

Independence: Runs in userspace and is relatively independent of the operating system kernel. It makes it easier to upgrade and debug client programs without having to worry about operating system limitations and dependencies.

Expandability: The user mode client can implement more advanced functions and protocols to meet the specific application requirements.

High-performance: Libsdk reduces kernel-mode forwarding compared to fuse mounts, contributing to performance improvements.

### cfs_new_client
```
extern int64_t cfs_new_client();
```
Create a new client. The return value is an int64 value, which is the ID of the newly created client.

### cfs_set_client
```
extern int cfs_set_client(int64_t id, char* key, char* val);
Example parameter configuration:
cfs_set_client(client_id, "volName", "test");
cfs_set_client(client_id, "accessKey", "WgkHUb03XViuRkHX");
cfs_set_client(client_id, "secretKey", "oMQsYfGNcEdaUbbrw5D2GEp092RgNvQb");
cfs_set_client(client_id, "masterAddr", "172.16.1.101:17010");
cfs_set_client(client_id, "logDir", "/home/test_sdk/log");
cfs_set_client(client_id, "logLevel", "debug");
cfs_set_client(client_id, "enableSummary", "true");
```
Set the client configuration.

Parameters:

id: The ID of the client

key: The key for the configuration entry

val: The value of the configuration entry

Return value:

statusOK: Success

statusEINVAL: Invalid parameter

### cfs_start_client
```
extern int cfs_start_client(int64_t id);
```
Start the client.

Parameters:

id: The ID of the client

Return value:

statusOK: Success

statusEINVAL: Invalid parameter

statusEIO: io error

### cfs_close_client
```
extern void cfs_close_client(int64_t id);
```

Close the client.

Parameters:

id: The ID of the client

### cfs_open
```
extern int cfs_open(int64_t id, char* path, int flags, mode_t mode);
```
Open or create files.

Parameters:

id: The ID of the client

path: The path to the file

flags: Indicates a flag to open the file

mode: This indicates the permission mode of the file

Return value:

A value greater than 0 indicates the file descriptor returned on success. Returns a value less than 0 on failure.

statusEINVAL: Invalid parameter

statusEACCES: Permission error

statusEMFILE: The file descriptor limit has been reached

statusEIO: io error

### cfs_close
```
extern void cfs_close(int64_t id, int fd);
```
Close the file.

Parameters:

id: The ID of the client

fd: This represents the file descriptor

### cfs_read
```
extern ssize_t cfs_read(int64_t id, int fd, void* buf, size_t size, off_t off);
```
Reads data from the specified file descriptor.

Parameters:

id: The ID of the client

fd: The file descriptor

buf: Pointer to the memory area used to store the data read

size: This indicates the size of the data to be read

off: This indicates the offset of the file

Return value:

The return value is greater than 0, indicating the actual size of the data read. Returns a value less than 0 on failure.

statusEINVAL: Invalid parameter

statusEACCES: Permission error

statusEBADFD: Error file descriptor

statusEIO: io error

### cfs_write
```
extern ssize_t cfs_write(int64_t id, int fd, void* buf, size_t size, off_t off);
```
Writes data to the specified file descriptor.

Parameters:

id: The ID of the client

fd: The file descriptor

buf: Points to the data cache to be written

size: This indicates the size of the buffer

off: This indicates the offset

Return value:

A value greater than 0 indicates the number of bytes written. Returns a value less than 0 on failure.

statusEINVAL: Invalid parameter

statusEACCES: Permission error

statusEBADFD: Error file descriptor

statusENOSPC: Not enough space

statusEIO: io error

### cfs_flush
```
extern int cfs_flush(int64_t id, int fd);
```
flush the file.

Parameters:

id: The ID of the client

fd: The file descriptor

Return value:

Returns 0 on success or a value less than 0 on failure.

statusOK: Success

statusEINVAL: Invalid parameter

statusEBADFD: Error file descriptor

statusEIO: io error

### cfs_truncate
```
extern int cfs_truncate(int64_t id, int fd, size_t size);
```
truncate the file.

Parameters:

id: The ID of the client

fd: The file descriptor

size: The truncate size of the file

Return value:

Returns 0 on success or a value less than 0 on failure.

statusOK: Success

statusEINVAL: Invalid parameter

statusEBADFD: Error file descriptor

statusEIO: io error

### cfs_unlink
```
extern int cfs_unlink(int64_t id, char* path);
```
Delete the file.

Parameters:

id: The ID of the client

path: The file path

Return value:

Returns 0 on success or a value less than 0 on failure.

statusOK: Success

statusEINVAL: Invalid parameter

statusEISDIR: This is the directory

### cfs_mkdirs
```
extern int cfs_mkdirs(int64_t id, char* path, mode_t mode);
```
Create multilevel directories.

Parameters:

id: The ID of the client

path: The path to the directory to create

mode: The permission mode

Return value:

Returns 0 on success or a value less than 0 on failure.

statusOK: Success

statusEINVAL: Invalid parameter

statusEEXIST: It exists

### cfs_readdir
```
extern int cfs_readdir(int64_t id, int fd, GoSlice dirents, int count);
```
Reads a list of files and subdirectories of a directory.

Parameters:

id: The ID of the client

fd: The file descriptor

dirents: An array to store information about files and subdirectories

count: The number of files and subdirectories to read

Return value:

A value greater than 0 indicates the number of directory entries read. Returns a value less than 0 on failure.

statusEINVAL: Invalid parameter

statusEBADFD: Error file descriptor

### cfs_lsdir
```
extern int cfs_lsdir(int64_t id, int fd, GoSlice direntsInfo, int count);
```
Lists information about files and subdirectories in the specified directory, including metadata information.

Parameters:

id: The ID of the client

fd: The file descriptor

direntsInfo: An array to store file and subdirectory information

count: The number of files and subdirectories to read

Return value:

A value greater than 0 indicates the number of directory entries read. Returns a value less than 0 on failure.

statusEINVAL: Invalid parameter

statusEBADFD: Error file descriptor

### cfs_rmdir
```
extern int cfs_rmdir(int64_t id, char* path);
```
Deletes the directory of the specified path.

Parameters:

id: The ID of the client

path: The directory path

Return value:

Returns 0 on success or a value less than 0 on failure

### cfs_rename
```
extern int cfs_rename(int64_t id, char* from, char* to, GoUint8 overwritten);
```
Renaming a file or directory

Parameters:

id: The ID of the client

from: The path to the original file or directory

to: The path to the new file or directory

overwritten: Whether existing files are allowed to be overwritten

Return value:

Returns 0 on success or a value less than 0 on failure

### cfs_getattr
```
extern int cfs_getattr(int64_t id, char* path, struct cfs_stat_info* stat);
```
Used to get attributes of a file.

Parameters:

id: The ID of the client

path: The path to the file

stat: Pointer to the cfs_stat_info struct used to store file attribute information. It is defined in libcfs.h.

Return value:

Returns 0 on success or a value less than 0 on failure

### cfs_setattr
```
extern int cfs_setattr(int64_t id, char* path, struct cfs_stat_info* stat, int valid);
```
Used to set file properties.

Parameters:

id: The ID of the client

path: The path to the file

stat: Pointer to the cfs_stat_info structure that contains information about file or directory attributes It is defined in libcfs.h.

valid: This indicates which attribute information is valid

Return value:

Returns 0 on success or a value less than 0 on failure


### cfs_lock_dir
```
extern int64_t cfs_lock_dir(int64_t id, char *path, int64_t lease, int64_t lock_id);
```
It is used to lock directories. It is suitable for applications that require mutual exclusion of directories. Locking the directory after it has been locked will return failure if it is within the validity period, and locking success will return the unique lockId.

Parameters:

id: The ID of the client

path: The path to the directory

lease: The duration of the lock

lock_id: If lock_id is specified, indicates the lease modification of the lock_id corresponding to it

Return value:

Returns a unique value lock_id on success or a value less than 0 on failure.

### cfs_unlock_dir
```
extern int cfs_unlock_dir(int64_t id, char *path);
```
Used to unlock directories.

Parameters:

id: The ID of the client

path: The path to the directory

Return value:

Returns 0 on success or a value less than 0 on failure.

### cfs_get_dir_lock
```
extern int cfs_get_dir_lock(int64_t id, char *path, int64_t *lock_id, char **valid_time);
```
To acquire the valid time of directory lock.

Parameters:

id: The ID of the client

path: The path to the directory

valid_time: This returns the validity period for the corresponding lock_id

Return value:

Returns 0 on success or a value less than 0 on failure.

### cfs_symlink
```
extern int cfs_symlink(int64_t id, char *src_path, char *dst_path);
```
Create symlink.

Parameters:

id: The ID of the client

src_path: The source path

dst_path: The destination path

Return value:

Returns 0 on success or a value less than 0 on failure.

### cfs_link
```
extern int cfs_link(int64_t id, char *src_path, char *dst_path);
```
Create link.

Parameters:

id: The ID of the client

src_path: The source path

dst_path: The destination path

Return value:

Returns 0 on success or a value less than 0 on failure.

### cfs_IsDir
```
extern int cfs_IsDir(mode_t mode);
```
Check mode to see if it is a directory.

Parameters:

mode:The mode value of the file

Return value:

1: Indicates it is a directory

0: Indicates it is not a directory

### cfs_IsRegular
```
extern int cfs_IsRegular(mode_t mode);
```
Check mode to see if it is a file.

Parameters:

mode:The mode value of the file

Return value:

1: Indicates it is a file

0: Indicates is it not a file