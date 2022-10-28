#ifndef CLIENT_H
#define CLIENT_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <search.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <map>
#include "client_type.h"
#include "ini.h"
#include "libc_operation.h"
#include "packet.h"
#include "sdk.h"

using namespace std;

/*
 * The implementation of opendir depend on struct __dirstream
 */
#define __libc_lock_define(CLASS,NAME)
struct __dirstream
{
    int fd;			/* File descriptor.  */

    __libc_lock_define (, lock) /* Mutex lock for this structure.  */

    size_t allocation;		/* Space allocated for the block.  */
    size_t size;		/* Total valid data in the block.  */
    size_t offset;		/* Current offset into the block.  */

    off_t filepos;		/* Position of next entry to read.  */

    int errcode;		/* Delayed error code.  */

    /* Directory block.  We must make sure that this block starts
       at an address that is aligned adequately enough to store
       dirent entries.  Using the alignment of "void *" is not
       sufficient because dirents on 32-bit platforms can require
       64-bit alignment.  We use "long double" here to be consistent
       with what malloc uses.  */
    char data[0] __attribute__ ((aligned (__alignof__ (long double))));
};

#ifdef __cplusplus
extern "C"
{
#endif

int real_close(int fd);
int real_openat(int dirfd, const char *pathname, int flags, ...);
int real_renameat(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname);
int real_renameat2(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname, unsigned int flags);
int real_truncate(const char *pathname, off_t length);
int real_ftruncate(int fd, off_t length);
int real_fallocate(int fd, int mode, off_t offset, off_t len);
int real_posix_fallocate(int fd, off_t offset, off_t len);
int real_mkdirat(int dirfd, const char *pathname, mode_t mode);
int real_rmdir(const char *pathname);
char *real_getcwd(char *buf, size_t size);
int real_chdir(const char *pathname);
int real_fchdir(int fd);
DIR *real_opendir(const char *pathname);
DIR *real_fdopendir(int fd);
struct dirent *real_readdir(DIR *dirp);
int real_readdir_r(DIR *dirp, struct dirent *entry, struct dirent **result);
int real_closedir(DIR *dirp);
char *real_realpath(const char *path, char *resolved_path);
char *real_realpath_chk(const char *path, char *resolved_path, size_t resolvedlen);
int real_linkat(int olddirfd, const char *old_pathname,
           int newdirfd, const char *new_pathname, int flags);
int real_symlinkat(const char *target, int dirfd, const char *linkpath);
int real_unlinkat(int dirfd, const char *pathname, int flags);
ssize_t real_readlinkat(int dirfd, const char *pathname, char *buf, size_t size);
int real_stat(int ver, const char *pathname, struct stat *statbuf);
int real_stat64(int ver, const char *pathname, struct stat64 *statbuf);
int real_lstat(int ver, const char *pathname, struct stat *statbuf);
int real_lstat64(int ver, const char *pathname, struct stat64 *statbuf);
int real_fstat(int ver, int fd, struct stat *statbuf);
int real_fstat64(int ver, int fd, struct stat64 *statbuf);
int real_fstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags);
int real_fstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags);
int real_fchmod(int fd, mode_t mode);
int real_fchmodat(int dirfd, const char *pathname, mode_t mode, int flags);
int real_lchown(const char *pathname, uid_t owner, gid_t group);
int real_fchown(int fd, uid_t owner, gid_t group);
int real_fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags);
int real_utime(const char *pathname, const struct utimbuf *times);
int real_utimes(const char *pathname, const struct timeval *times);
int real_futimesat(int dirfd, const char *pathname, const struct timeval times[2]);
int real_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags);
int real_futimens(int fd, const struct timespec times[2]);
int real_faccessat(int dirfd, const char *pathname, int mode, int flags);
int real_setxattr(const char *pathname, const char *name,
        const void *value, size_t size, int flags);
int real_lsetxattr(const char *pathname, const char *name,
        const void *value, size_t size, int flags);
int real_fsetxattr(int fd, const char *name, const void *value, size_t size, int flags);
ssize_t real_getxattr(const char *pathname, const char *name, void *value, size_t size);
ssize_t real_lgetxattr(const char *pathname, const char *name, void *value, size_t size);
ssize_t real_fgetxattr(int fd, const char *name, void *value, size_t size);
ssize_t real_listxattr(const char *pathname, char *list, size_t size);
ssize_t real_llistxattr(const char *pathname, char *list, size_t size);
ssize_t real_flistxattr(int fd, char *list, size_t size);
int real_removexattr(const char *pathname, const char *name);
int real_lremovexattr(const char *pathname, const char *name);
int real_fremovexattr(int fd, const char *name);
int real_fcntl(int fd, int cmd, ...);
int real_dup(int oldfd);
int real_dup2(int oldfd, int newfd);
int real_dup3(int oldfd, int newfd, int flags);
ssize_t real_read(int fd, void *buf, size_t count);
ssize_t real_readv(int fd, const struct iovec *iov, int iovcnt);
ssize_t real_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t real_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
ssize_t real_write(int fd, const void *buf, size_t count);
ssize_t real_writev(int fd, const struct iovec *iov, int iovcnt);
ssize_t real_pwrite(int fd, const void *buf, size_t count, off_t offset);
ssize_t real_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
off_t real_lseek(int fd, off_t offset, int whence);
ssize_t real_sendfile(int out_fd, int in_fd, off_t *offset, size_t count);
int real_fdatasync(int fd);
int real_fsync(int fd);

int start_libs(void*);
void* stop_libs();
void flush_logs();
#ifdef __cplusplus
}
#endif

const uint8_t FILE_TYPE_BIN_LOG = 1;
const uint8_t FILE_TYPE_REDO_LOG = 2;
const uint8_t FILE_TYPE_RELAY_LOG = 3;
const int UMP_CFS_READ = 25;
// hook or not, currently for test
const bool g_hook = true;
client_info_t g_client_info;

int config_handler(void* user, const char* section, const char* name, const char* value);
char *get_clean_path(const char *path);
char *cat_path(const char *cwd, const char *pathname);
char *get_cfs_path(const char *pathname);
int cfs_errno(int re);
ssize_t cfs_errno_ssize_t(ssize_t re);
bool has_renameat2();
bool fd_in_cfs(int fd);
int get_cfs_fd(int fd);
int dup_fd(int oldfd, int newfd);
int gen_fd(int start);
file_t *get_open_file(int fd);
inode_info_t *get_open_inode(ino_t ino);
const char *get_fd_path(int fd);
void find_diff_data(void *buf, void *buf_local, off_t offset, ssize_t size);
void log_debug(const char* message, ...);

#endif
