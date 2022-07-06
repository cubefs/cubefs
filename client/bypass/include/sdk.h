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

#ifndef CFS_SDK_DL_H
#define CFS_SDK_DL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <fcntl.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

typedef struct {
        int ignore_sighup;
        int ignore_sigterm;
        const char* log_dir;
        const char* log_level;
        const char* prof_port;
} cfs_sdk_init_t;

typedef struct {
    const char* master_addr;
    const char* vol_name;
    const char* owner;
    // whether to read from follower nodes or not, set "false" if want to read the newest data
    const char* follower_read;
    const char* app;
    const char* auto_flush;
    const char* master_client;
} cfs_config_t;

typedef struct {
	int fd;
	int flags;
    int file_type;
    int dup_ref;
    ino_t inode;
	size_t size;
	off_t pos;
} cfs_file_t;

typedef struct {
	off_t 		file_offset;
	size_t 		size;
	uint64_t 	partition_id;
	uint64_t 	extent_id;
	uint64_t 	extent_offset;
	char 	 	dp_host[32];
	int 	 	dp_port;
} cfs_read_req_t;

/*
 * Library / framework initialization
 * This method will initialize logging and HTTP APIs.
 */
typedef int (*cfs_sdk_init_func)(cfs_sdk_init_t* t);
typedef void (*cfs_sdk_close_t)();


// return client_id, should be positive if no error occurs
typedef int64_t (*cfs_new_client_t)(const cfs_config_t *conf, const char *config_path, char *str);
typedef void (*cfs_close_client_t)(int64_t id);
typedef size_t (*cfs_sdk_state_t)(int64_t id, char *buf, int size);
/*
 * Log is cached by default, will only be flushed when client close.
 * Call this function manually when necessary.
 */
typedef void (*cfs_flush_log_t)();
typedef void (*cfs_ump_t)(int64_t id, int umpType, int sec, int nsec);

/*
 * File operations
 */
typedef int (*cfs_close_t)(int64_t id, int fd);
typedef int (*cfs_open_t)(int64_t id, const char *path, int flags, mode_t mode);
typedef int (*cfs_openat_t)(int64_t id, int dirfd, const char *path, int flags, mode_t mode);
typedef int (*cfs_openat_fd_t)(int64_t id, int dirfd, const char *path, int flags, mode_t mode, int fd);
typedef int (*cfs_rename_t)(int64_t id, const char *from, const char *to);
typedef int (*cfs_renameat_t)(int64_t id, int fromdirfd, const char *from, int todirfd, const char *to);
typedef int (*cfs_truncate_t)(int64_t id, const char *path, off_t len);
typedef int (*cfs_ftruncate_t)(int64_t id, int fd, off_t len);
typedef int (*cfs_fallocate_t)(int64_t id, int fd, int mode, off_t offset, off_t len);
typedef int (*cfs_posix_fallocate_t)(int64_t id, int fd, off_t offset, off_t len);
typedef int (*cfs_flush_t)(int64_t id, int fd);
typedef int (*cfs_get_file_t)(int64_t id, int fd, cfs_file_t *file);

/*
 * Directory operations
 */
typedef int (*cfs_chdir_t)(int64_t id, const char *path);
/*
 * The dir corresponding to fd is written to buf if buf is not NULL and size is enough.
 * This feature is for CFS kernel bypass client.
 */
typedef int (*cfs_fchdir_t)(int64_t id, int fd, char *buf, int size);
typedef char* (*cfs_getcwd_t)(int64_t id);
typedef int (*cfs_mkdirs_t)(int64_t id, const char *path, mode_t mode);
typedef int (*cfs_mkdirsat_t)(int64_t id, int dirfd, const char *path, mode_t mode);
typedef int (*cfs_rmdir_t)(int64_t id, const char *path);
typedef int (*cfs_getdents_t)(int64_t id, int fd, char *buf, int count);

/*
 * Link operations
 */
typedef int (*cfs_link_t)(int64_t id, const char *oldpath, const char *newpath);
typedef int (*cfs_linkat_t)(int64_t id, int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
typedef int (*cfs_symlink_t)(int64_t id, const char *target, const char *linkpath);
typedef int (*cfs_symlinkat_t)(int64_t id, const char *target, int newdirfd, const char *linkpath);
typedef int (*cfs_unlink_t)(int64_t id, const char *path);
typedef int (*cfs_unlinkat_t)(int64_t id, int dirfd, const char *pathname, int flags);
typedef ssize_t (*cfs_readlink_t)(int64_t id, const char *pathname, char *buf, size_t size);
typedef ssize_t (*cfs_readlinkat_t)(int64_t id, int dirfd, const char *pathname, char *buf, size_t size);

/*
 * Basic file attributes
 */
typedef int (*cfs_stat_t)(int64_t id, const char *path, struct stat *stat);
typedef int (*cfs_stat64_t)(int64_t id, const char *path, struct stat64 *stat);
typedef int (*cfs_lstat_t)(int64_t id, const char *path, struct stat *stat);
typedef int (*cfs_lstat64_t)(int64_t id, const char *path, struct stat64 *stat);
typedef int (*cfs_fstat_t)(int64_t id, int fd, struct stat *stat);
typedef int (*cfs_fstat64_t)(int64_t id, int fd, struct stat64 *stat);
typedef int (*cfs_fstatat_t)(int64_t id, int dirfd, const char *path, struct stat *stat, int flags);
typedef int (*cfs_fstatat64_t)(int64_t id, int dirfd, const char *path, struct stat64 *stat, int flags);
typedef int (*cfs_chmod_t)(int64_t id, const char *path, mode_t mode);
typedef int (*cfs_fchmod_t)(int64_t id, int fd, mode_t mode);
typedef int (*cfs_fchmodat_t)(int64_t id, int dirfd, const char *path, mode_t mode, int flags);
typedef int (*cfs_chown_t)(int64_t id, const char *path, uid_t uid, gid_t gid);
typedef int (*cfs_lchown_t)(int64_t id, const char *path, uid_t uid, gid_t gid);
typedef int (*cfs_fchown_t)(int64_t id, int fd, uid_t uid, gid_t gid);
typedef int (*cfs_fchownat_t)(int64_t id, int dirfd, const char *path, uid_t uid, gid_t gid, int flags);
typedef int (*cfs_futimens_t)(int64_t id, int fd, const struct timespec *times);
typedef int (*cfs_utimens_t)(int64_t id, const char *path, const struct timespec *times, int flags);
typedef int (*cfs_utimensat_t)(int64_t id, int dirfd, const char *path, const struct timespec *times, int flags);
typedef int (*cfs_access_t)(int64_t id, const char *pathname, int mode);
typedef int (*cfs_faccessat_t)(int64_t id, int dirfd, const char *pathname, int mode, int flags);

/*
 * Extended file attributes
 */
typedef int (*cfs_setxattr_t)(int64_t id, const char *path, const char *name, const void *value, size_t size, int flags);
typedef int (*cfs_lsetxattr_t)(int64_t id, const char *path, const char *name, const void *value, size_t size, int flags);
typedef int (*cfs_fsetxattr_t)(int64_t id, int fd, const char *name, const void *value, size_t size, int flags);
typedef ssize_t (*cfs_getxattr_t)(int64_t id, const char *path, const char *name, void *value, size_t size);
typedef ssize_t (*cfs_lgetxattr_t)(int64_t id, const char *path, const char *name, void *value, size_t size);
typedef ssize_t (*cfs_fgetxattr_t)(int64_t id, int fd, const char *name, void *value, size_t size);
typedef ssize_t (*cfs_listxattr_t)(int64_t id, const char *path, char *list, size_t size);
typedef ssize_t (*cfs_llistxattr_t)(int64_t id, const char *path, char *list, size_t size);
typedef ssize_t (*cfs_flistxattr_t)(int64_t id, int fd, char *list, size_t size);
typedef int (*cfs_removexattr_t)(int64_t id, const char *path, const char *name);
typedef int (*cfs_lremovexattr_t)(int64_t id, const char *path, const char *name);
typedef int (*cfs_fremovexattr_t)(int64_t id, int fd, const char *name);

/*
 * File descriptor manipulations
 */
// Only support some cmds, F_DUPFD, F_SETFL
typedef int (*cfs_fcntl_t)(int64_t id, int fd, int cmd, int arg);
// For fcntl cmd F_SETLK, F_SETLKW
typedef int (*cfs_fcntl_lock_t)(int64_t id, int fd, int cmd, struct flock *lk);

/*
 * Read & Write
 */
typedef ssize_t (*cfs_read_t)(int64_t id, int fd, void *buf, size_t size);
typedef ssize_t (*cfs_pread_t)(int64_t id, int fd, void *buf, size_t size, off_t off);
typedef ssize_t (*cfs_readv_t)(int64_t id, int fd, const struct iovec *iov, int iovcnt);
typedef ssize_t (*cfs_preadv_t)(int64_t id, int fd, const struct iovec *iov, int iovcnt, off_t off);
typedef ssize_t (*cfs_write_t)(int64_t id, int fd, const void *buf, size_t size);
typedef ssize_t (*cfs_pwrite_t)(int64_t id, int fd, const void *buf, size_t size, off_t off);
typedef ssize_t (*cfs_pwrite_inode_t)(int64_t id, ino_t ino, const void *buf, size_t size, off_t off);
typedef ssize_t (*cfs_writev_t)(int64_t id, int fd, const struct iovec *iov, int iovcnt);
typedef ssize_t (*cfs_pwritev_t)(int64_t id, int fd, const struct iovec *iov, int iovcnt, off_t off);
typedef off64_t (*cfs_lseek_t)(int64_t id, int fd, off64_t offset, int whence);


typedef void (*InitModule_t)(void*);
typedef void (*FinishModule_t)(void*);

static cfs_sdk_init_func cfs_sdk_init;
static cfs_sdk_close_t cfs_sdk_close;
static cfs_new_client_t cfs_new_client;
static cfs_close_client_t cfs_close_client;
static cfs_sdk_state_t cfs_sdk_state;
static cfs_flush_log_t cfs_flush_log;
static cfs_ump_t cfs_ump;

static cfs_close_t cfs_close;
static cfs_open_t cfs_open;
static cfs_openat_t cfs_openat;
static cfs_openat_fd_t cfs_openat_fd;
static cfs_rename_t cfs_rename;
static cfs_renameat_t cfs_renameat;
static cfs_truncate_t cfs_truncate;
static cfs_ftruncate_t cfs_ftruncate;
static cfs_fallocate_t cfs_fallocate;
static cfs_posix_fallocate_t cfs_posix_fallocate;
static cfs_flush_t cfs_flush;
static cfs_get_file_t cfs_get_file;

static cfs_chdir_t cfs_chdir;
static cfs_fchdir_t cfs_fchdir;
static cfs_getcwd_t cfs_getcwd;
static cfs_mkdirs_t cfs_mkdirs;
static cfs_mkdirsat_t cfs_mkdirsat;
static cfs_rmdir_t cfs_rmdir;
static cfs_getdents_t cfs_getdents;

static cfs_link_t cfs_link;
static cfs_linkat_t cfs_linkat;
static cfs_symlink_t cfs_symlink;
static cfs_symlinkat_t cfs_symlinkat;
static cfs_unlink_t cfs_unlink;
static cfs_unlinkat_t cfs_unlinkat;
static cfs_readlink_t cfs_readlink;
static cfs_readlinkat_t cfs_readlinkat;

static cfs_stat_t cfs_stat;
static cfs_stat64_t cfs_stat64;
static cfs_lstat_t cfs_lstat;
static cfs_lstat64_t cfs_lstat64;
static cfs_fstat_t cfs_fstat;
static cfs_fstat64_t cfs_fstat64;
static cfs_fstatat_t cfs_fstatat;
static cfs_fstatat64_t cfs_fstatat64;
static cfs_chmod_t cfs_chmod;
static cfs_fchmod_t cfs_fchmod;
static cfs_fchmodat_t cfs_fchmodat;
static cfs_chown_t cfs_chown;
static cfs_lchown_t cfs_lchown;
static cfs_fchown_t cfs_fchown;
static cfs_fchownat_t cfs_fchownat;
static cfs_futimens_t cfs_futimens;
static cfs_utimens_t cfs_utimens;
static cfs_utimensat_t cfs_utimensat;
static cfs_access_t cfs_access;
static cfs_faccessat_t cfs_faccessat;

static cfs_setxattr_t cfs_setxattr;
static cfs_lsetxattr_t cfs_lsetxattr;
static cfs_fsetxattr_t cfs_fsetxattr;
static cfs_getxattr_t cfs_getxattr;
static cfs_lgetxattr_t cfs_lgetxattr;
static cfs_fgetxattr_t cfs_fgetxattr;
static cfs_listxattr_t cfs_listxattr;
static cfs_llistxattr_t cfs_llistxattr;
static cfs_flistxattr_t cfs_flistxattr;
static cfs_removexattr_t cfs_removexattr;
static cfs_lremovexattr_t cfs_lremovexattr;
static cfs_fremovexattr_t cfs_fremovexattr;

static cfs_fcntl_t cfs_fcntl;
static cfs_fcntl_lock_t cfs_fcntl_lock;

static cfs_read_t cfs_read;
static cfs_pread_t cfs_pread;
static cfs_readv_t cfs_readv;
static cfs_preadv_t cfs_preadv;
static cfs_write_t cfs_write;
static cfs_pwrite_t cfs_pwrite;
static cfs_pwrite_inode_t cfs_pwrite_inode;
static cfs_writev_t cfs_writev;
static cfs_pwritev_t cfs_pwritev;
static cfs_lseek_t cfs_lseek;
#ifdef __cplusplus
}
#endif

#endif
