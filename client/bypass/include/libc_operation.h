#ifndef LIBC_OPERATION_H
#define LIBC_OPERATION_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>


// Define ALIASNAME as a weak alias for NAME.
# define weak_alias(name, aliasname) extern __typeof (name) aliasname __attribute__ ((weak, alias (#name)));

// compatible for glibc before 2.18
#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1 << 0)
#endif


typedef int (*openat_t)(int dirfd, const char *pathname, int flags, mode_t mode);
typedef int (*close_t)(int fd);
typedef int (*renameat_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath);
typedef int (*renameat2_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags);
typedef int (*truncate_t)(const char *path, off_t length);
typedef int (*ftruncate_t)(int fd, off_t length);
typedef int (*fallocate_t)(int fd, int mode, off_t offset, off_t len);
typedef int (*posix_fallocate_t)(int fd, off_t offset, off_t len);

typedef int (*chdir_t)(const char *path);
typedef int (*fchdir_t)(int fd);
typedef char *(*getcwd_t)(char *buf, size_t size);
typedef int (*mkdirat_t)(int dirfd, const char *pathname, mode_t mode);
typedef int (*rmdir_t)(const char *pathname);
typedef DIR *(*opendir_t)(const char *name);
typedef DIR *(*fdopendir_t)(int fd);
typedef struct dirent *(*readdir_t)(DIR *dirp);
typedef int (*closedir_t)(DIR *dirp);
typedef char *(*realpath_t)(const char *path, char *resolved_path);

typedef int (*linkat_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
typedef int (*symlinkat_t)(const char *target, int newdirfd, const char *linkpath);
typedef int (*unlinkat_t)(int dirfd, const char *pathname, int flags);
typedef ssize_t (*readlinkat_t)(int dirfd, const char *pathname, char *buf, size_t size);

typedef int (*stat_t)(int ver, const char *pathname, struct stat *statbuf);
typedef int (*stat64_t)(int ver, const char *pathname, struct stat64 *statbuf);
typedef int (*lstat_t)(int ver, const char *pathname, struct stat *statbuf);
typedef int (*lstat64_t)(int ver, const char *pathname, struct stat64 *statbuf);
typedef int (*fstat_t)(int ver, int fd, struct stat *statbuf);
typedef int (*fstat64_t)(int ver, int fd, struct stat64 *statbuf);
typedef int (*fstatat_t)(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags);
typedef int (*fstatat64_t)(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags);
typedef int (*fchmod_t)(int fd, mode_t mode);
typedef int (*fchmodat_t)(int dirfd, const char *pathname, mode_t mode, int flags);
typedef int (*lchown_t)(const char *pathname, uid_t owner, gid_t group);
typedef int (*fchown_t)(int fd, uid_t owner, gid_t group);
typedef int (*fchownat_t)(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags);
typedef int (*utime_t)(const char *filename, const struct utimbuf *times);
typedef int (*utimes_t)(const char *filename, const struct timeval times[2]);
typedef int (*futimesat_t)(int dirfd, const char *pathname, const struct timeval times[2]);
typedef int (*utimensat_t)(int dirfd, const char *pathname, const struct timespec times[2], int flags);
typedef int (*futimens_t)(int fd, const struct timespec times[2]);
typedef int (*access_t)(const char *pathname, int mode);
typedef int (*faccessat_t)(int dirfd, const char *pathname, int mode, int flags);

typedef int (*setxattr_t)(const char *path, const char *name, const void *value, size_t size, int flags);
typedef int (*lsetxattr_t)(const char *path, const char *name, const void *value, size_t size, int flags);
typedef int (*fsetxattr_t)(int fd, const char *name, const void *value, size_t size, int flags);
typedef ssize_t (*getxattr_t)(const char *path, const char *name, void *value, size_t size);
typedef ssize_t (*lgetxattr_t)(const char *path, const char *name, void *value, size_t size);
typedef ssize_t (*fgetxattr_t)(int fd, const char *name, void *value, size_t size);
typedef ssize_t (*listxattr_t)(const char *path, char *list, size_t size);
typedef ssize_t (*llistxattr_t)(const char *path, char *list, size_t size);
typedef ssize_t (*flistxattr_t)(int fd, char *list, size_t size);
typedef int (*removexattr_t)(const char *path, const char *name);
typedef int (*lremovexattr_t)(const char *path, const char *name);
typedef int (*fremovexattr_t)(int fd, const char *name);

typedef int (*fcntl_t)(int fd, int cmd, ...);
typedef int (*dup2_t)(int oldfd, int newfd);
typedef int (*dup3_t)(int oldfd, int newfd, int flags);

typedef ssize_t (*read_t)(int fd, void *buf, size_t count);
typedef ssize_t (*readv_t)(int fd, const struct iovec *iov, int iovcnt);
typedef ssize_t (*pread_t)(int fd, void *buf, size_t count, off_t offset);
typedef ssize_t (*preadv_t)(int fd, const struct iovec *iov, int iovcnt, off_t offset);
typedef ssize_t (*write_t)(int fd, const void *buf, size_t count);
typedef ssize_t (*writev_t)(int fd, const struct iovec *iov, int iovcnt);
typedef ssize_t (*pwrite_t)(int fd, const void *buf, size_t count, off_t offset);
typedef ssize_t (*pwritev_t)(int fd, const struct iovec *iov, int iovcnt, off_t offset);
typedef off_t (*lseek_t)(int fd, off_t offset, int whence);

typedef int (*fdatasync_t)(int fd);
typedef int (*fsync_t)(int fd);

typedef void (*abort_t)();
typedef void (*_exit_t)(int);
typedef void (*exit_t)(int);

//typedef int (*sigaction_t)(int signum, const struct sigaction *act, struct sigaction *oldact);


int libc_openat(int dirfd, const char *pathname, int flags, ...);
int libc_close(int fd);
int libc_renameat(int olddirfd, const char *oldpath,
                    int newdirfd, const char *newpath);
int libc_renameat2(int olddirfd, const char *old_pathname,
       int newdirfd, const char *new_pathname, unsigned int flags);
int libc_truncate(const char *path, off_t length);
int libc_ftruncate(int fd, off_t length);
int libc_fallocate(int fd, int mode, off_t offset, off_t len);
int libc_posix_fallocate(int fd, off_t offset, off_t len);
int libc_chdir(const char *path);
int libc_fchdir(int fd);
char *libc_getcwd(char *buf, size_t size);
int libc_mkdirat(int dirfd, const char *pathname, mode_t mode);
int libc_rmdir(const char *pathname);
DIR *libc_opendir(const char *name);
DIR *libc_fdopendir(int fd);
struct dirent *libc_readdir(DIR *dirp);
int libc_closedir(DIR *dirp);
char *libc_realpath(const char *path, char *resolved_path);
int libc_linkat(int olddirfd, const char *oldpath,
                  int newdirfd, const char *newpath, int flags);
int libc_symlinkat(const char *oldpath, int newdirfd, const char *newpath);
int libc_unlinkat(int dirfd, const char *pathname, int flags);
int libc_readlinkat(int dirfd, const char *pathname, char *buf, size_t bufsiz);
int libc_stat(int ver, const char *pathname, struct stat *statbuf);
int libc_stat64(int ver, const char *pathname, struct stat64 *statbuf);
int libc_lstat(int ver, const char *pathname, struct stat *statbuf);
int libc_lstat64(int ver, const char *pathname, struct stat64 *statbuf);
int libc_fstat(int ver, int fd, struct stat *statbuf);
int libc_fstat64(int ver, int fd, struct stat64 *statbuf);
int libc_fstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags);
int libc_fstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags);
int libc_fchmod(int fd, mode_t mode);
int libc_fchmodat(int dirfd, const char *pathname, mode_t mode, int flags);
int libc_lchown(const char *path, uid_t owner, gid_t group);
int libc_fchown(int fd, uid_t owner, gid_t group);
int libc_fchownat(int dirfd, const char *pathname,
                    uid_t owner, gid_t group, int flags);
int libc_utime(const char *filename, const struct utimbuf *times);
int libc_utimes(const char *filename, const struct timeval times[2]);
int libc_futimesat(int dirfd, const char *pathname, const struct timeval times[2]);
int libc_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags);
int libc_futimens(int fd, const struct timespec times[2]);
int libc_access(const char *pathname, int mode);
int libc_faccessat(int dirfd, const char *pathname, int mode, int flags);
int libc_setxattr(const char *pathname, const char *name, const void *value, size_t size, int flags);
int libc_lsetxattr(const char *pathname, const char *name, const void *value, size_t size, int flags);
int libc_fsetxattr(int fd, const char *name, const void *value, size_t size, int flags);
ssize_t libc_getxattr(const char *pathname, const char *name, void *value, size_t size);
ssize_t libc_lgetxattr(const char *pathname, const char *name, void *value, size_t size);
ssize_t libc_fgetxattr(int fd, const char *name, void *value, size_t size);
ssize_t libc_listxattr(const char *pathname, char *list, size_t size);
ssize_t libc_llistxattr(const char *pathname, char *list, size_t size);
ssize_t libc_flistxattr(int fd, char *list, size_t size);
int libc_removexattr(const char *pathname, const char *name);
int libc_lremovexattr(const char *pathname, const char *name);
int libc_fremovexattr(int fd, const char *name);
int libc_fcntl(int fd, int cmd, ...);
int libc_dup2(int oldfd, int newfd);
int libc_dup3(int oldfd, int newfd, int flags);
ssize_t libc_read(int fd, void *buf, size_t count);
ssize_t libc_readv(int fd, const struct iovec *iov, int iovcnt);
ssize_t libc_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t libc_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
ssize_t libc_write(int fd, const void *buf, size_t count);
ssize_t libc_writev(int fd, const struct iovec *iov, int iovcnt);
ssize_t libc_pwrite(int fd, const void *buf, size_t count, off_t offset);
ssize_t libc_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
off_t libc_lseek(int fd, off_t offset, int whence);
int libc_fdatasync(int fd);
int libc_fsync(int fd);
void libc_abort();
void libc__exit(int status);
void libc_exit(int status);

#endif
