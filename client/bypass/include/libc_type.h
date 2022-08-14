#ifndef LIBC_TYPE_H
#define LIBC_TYPE_H

#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
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

//typedef int (*sigaction_t)(int signum, const struct sigaction *act, struct sigaction *oldact);

#endif
