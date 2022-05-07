// This is a C SDK for ChubaoFS implemented by Golang, all the
// functions are in accordance with libc, except for cfs_ prefix
// in function name, a heading client_id parameter, and a negative
// ERRNO returned when fail(e.g. cfs_open will return -EACCES if
// it fails with error EACCES).
//
// Notice:
//   1. CFS DOESN'T support scatter-gather IO, these functions
// (cfs_readv,cfs_preadv,cfs_writev,cfs_pwritev) are implemented by
// copying buffers.
//   2. Some sophisticated functions are partly supported, e.g.
// cfs_fcntl, cfs_fcntl_lock. DON'T rely on them.
//   3. CFS DOESN'T check file permissions, cfs_access only check file
// existence.
//
// Usage:
//   cfs_config_t conf = (cfs_config_t) { ... };
//   int64_t client_id = cfs_new_client(&conf);
//   if(client_id < 0) { // error occurs }
//   cfs_close_client(client_id);

#ifndef CFS_SDK_H
#define CFS_SDK_H

#include <fcntl.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

typedef struct {
    const char* master_addr;
    const char* vol_name;
    const char* owner;
    // whether to read from follower nodes or not, set "false" if want to read the newest data
    const char* follower_read;
    const char* log_dir;
    // debug|info|warn|error
    const char* log_level;
    const char* app;

    const char* prof_port;
    const char* auto_flush;
    const char* master_client;

    // following are optional parameters for profiling
    const char* tracing_sampler_type;
    const char* tracing_sampler_param;
    const char* tracing_report_addr;
} cfs_config_t;

typedef struct {
	uint64_t total;
	uint64_t used;
} cfs_statfs_t;

// return client_id, should be positive if no error occurs
int64_t cfs_new_client(const cfs_config_t *conf);
void cfs_close_client(int64_t id);
int cfs_statfs(int64_t id, cfs_statfs_t* stat);
/*
 * Log is cached by default, will only be flushed when client close.
 * Call this function manually when necessary.
 */
void cfs_flush_log();

/*
 * File operations
 */
int cfs_close(int64_t id, int fd);
int cfs_open(int64_t id, const char *path, int flags, mode_t mode);
int cfs_openat(int64_t id, int dirfd, const char *path, int flags, mode_t mode);
int cfs_openat_fd(int64_t id, int dirfd, const char *path, int flags, mode_t mode, int fd);
int cfs_rename(int64_t id, const char *from, const char *to);
int cfs_renameat(int64_t id, int fromdirfd, const char *from, int todirfd, const char *to);
int cfs_truncate(int64_t id, const char *path, off_t len);
int cfs_ftruncate(int64_t id, int fd, off_t len);
int cfs_fallocate(int64_t id, int fd, int mode, off_t offset, off_t len);
int cfs_posix_fallocate(int64_t id, int fd, off_t offset, off_t len);
int cfs_flush(int64_t id, int fd);

/*
 * Directory operations
 */
int cfs_chdir(int64_t id, const char *path);
/*
 * The dir corresponding to fd is written to buf if buf is not NULL and size is enough.
 * This feature is for CFS kernel bypass client.
 */
int cfs_fchdir(int64_t id, int fd, char *buf, int size);
char* cfs_getcwd(int64_t id);
int cfs_mkdirs(int64_t id, const char *path, mode_t mode);
int cfs_mkdirsat(int64_t id, int dirfd, const char *path, mode_t mode);
int cfs_rmdir(int64_t id, const char *path);
int cfs_getdents(int64_t id, int fd, char *buf, int count);

/*
 * Link operations
 */
int cfs_link(int64_t id, const char *oldpath, const char *newpath);
int cfs_linkat(int64_t id, int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
int cfs_symlink(int64_t id, const char *target, const char *linkpath);
int cfs_symlinkat(int64_t id, const char *target, int newdirfd, const char *linkpath);
int cfs_unlink(int64_t id, const char *path);
int cfs_unlinkat(int64_t id, int dirfd, const char *pathname, int flags);
ssize_t cfs_readlink(int64_t id, const char *pathname, char *buf, size_t size);
ssize_t cfs_readlinkat(int64_t id, int dirfd, const char *pathname, char *buf, size_t size);

/*
 * Basic file attributes
 */
int cfs_stat(int64_t id, const char *path, struct stat *stat);
int cfs_stat64(int64_t id, const char *path, struct stat64 *stat);
int cfs_lstat(int64_t id, const char *path, struct stat *stat);
int cfs_lstat64(int64_t id, const char *path, struct stat64 *stat);
int cfs_fstat(int64_t id, int fd, struct stat *stat);
int cfs_fstat64(int64_t id, int fd, struct stat64 *stat);
int cfs_fstatat(int64_t id, int dirfd, const char *path, struct stat *stat, int flags);
int cfs_fstatat64(int64_t id, int dirfd, const char *path, struct stat64 *stat, int flags);
int cfs_chmod(int64_t id, const char *path, mode_t mode);
int cfs_fchmod(int64_t id, int fd, mode_t mode);
int cfs_fchmodat(int64_t id, int dirfd, const char *path, mode_t mode, int flags);
int cfs_chown(int64_t id, const char *path, uid_t uid, gid_t gid);
int cfs_lchown(int64_t id, const char *path, uid_t uid, gid_t gid);
int cfs_fchown(int64_t id, int fd, uid_t uid, gid_t gid);
int cfs_fchownat(int64_t id, int dirfd, const char *path, uid_t uid, gid_t gid, int flags);
int cfs_futimens(int64_t id, int fd, const struct timespec *times);
int cfs_utimens(int64_t id, const char *path, const struct timespec *times, int flags);
int cfs_utimensat(int64_t id, int dirfd, const char *path, const struct timespec *times, int flags);
int cfs_access(int64_t id, const char *pathname, int mode);
int cfs_faccessat(int64_t id, int dirfd, const char *pathname, int mode, int flags);

/*
 * Extended file attributes
 */
int cfs_setxattr(int64_t id, const char *path, const char *name, const void *value, size_t size, int flags);
int cfs_lsetxattr(int64_t id, const char *path, const char *name, const void *value, size_t size, int flags);
int cfs_fsetxattr(int64_t id, int fd, const char *name, const void *value, size_t size, int flags);
ssize_t cfs_getxattr(int64_t id, const char *path, const char *name, void *value, size_t size);
ssize_t cfs_lgetxattr(int64_t id, const char *path, const char *name, void *value, size_t size);
ssize_t cfs_fgetxattr(int64_t id, int fd, const char *name, void *value, size_t size);
ssize_t cfs_listxattr(int64_t id, const char *path, char *list, size_t size);
ssize_t cfs_llistxattr(int64_t id, const char *path, char *list, size_t size);
ssize_t cfs_flistxattr(int64_t id, int fd, char *list, size_t size);
int cfs_removexattr(int64_t id, const char *path, const char *name);
int cfs_lremovexattr(int64_t id, const char *path, const char *name);
int cfs_fremovexattr(int64_t id, int fd, const char *name);

/*
 * File descriptor manipulations
 */
// Only support some cmds, F_DUPFD, F_SETFL
int cfs_fcntl(int64_t id, int fd, int cmd, int arg);
// For fcntl cmd F_SETLK, F_SETLKW
int cfs_fcntl_lock(int64_t id, int fd, int cmd, struct flock *lk);

/*
 * Read & Write
 */
ssize_t cfs_read(int64_t id, int fd, char *buf, size_t size);
ssize_t cfs_pread(int64_t id, int fd, char *buf, size_t size, off_t off);
ssize_t cfs_readv(int64_t id, int fd, const struct iovec *iov, int iovcnt);
ssize_t cfs_preadv(int64_t id, int fd, const struct iovec *iov, int iovcnt, off_t off);
ssize_t cfs_write(int64_t id, int fd, const char *buf, size_t size);
ssize_t cfs_pwrite(int64_t id, int fd, const char *buf, size_t size, off_t off);
ssize_t cfs_writev(int64_t id, int fd, const struct iovec *iov, int iovcnt);
ssize_t cfs_pwritev(int64_t id, int fd, const struct iovec *iov, int iovcnt, off_t off);
off64_t cfs_lseek(int64_t id, int fd, off64_t offset, int whence);

#endif
