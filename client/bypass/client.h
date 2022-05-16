#ifndef CLIENT_H
#define CLIENT_H

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <gnu/libc-version.h>
#include <limits.h>
#include <search.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>
#include "ini.h"
#include "sdk.h"

// Define ALIASNAME as a weak alias for NAME.
# define weak_alias(name, aliasname) extern __typeof (name) aliasname __attribute__ ((weak, alias (#name)));

// compatible for glibc before 2.18
#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1 << 0)
#endif

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

typedef int (*open_t)(const char *pathname, int flags, mode_t mode);
typedef int (*openat_t)(int dirfd, const char *pathname, int flags, mode_t mode);
typedef int (*close_t)(int fd);
typedef int (*rename_t)(const char *oldpath, const char *newpath);
typedef int (*renameat_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath);
typedef int (*renameat2_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags);
typedef int (*truncate_t)(const char *path, off_t length);
typedef int (*ftruncate_t)(int fd, off_t length);
typedef int (*fallocate_t)(int fd, int mode, off_t offset, off_t len);
typedef int (*posix_fallocate_t)(int fd, off_t offset, off_t len);

typedef int (*chdir_t)(const char *path);
typedef int (*fchdir_t)(int fd);
typedef char *(*getcwd_t)(char *buf, size_t size);
typedef int (*mkdir_t)(const char *pathname, mode_t mode);
typedef int (*mkdirat_t)(int dirfd, const char *pathname, mode_t mode);
typedef int (*rmdir_t)(const char *pathname);
typedef DIR *(*opendir_t)(const char *name);
typedef DIR *(*fdopendir_t)(int fd);
typedef struct dirent *(*readdir_t)(DIR *dirp);
typedef int (*closedir_t)(DIR *dirp);
typedef char *(*realpath_t)(const char *path, char *resolved_path);

typedef int (*link_t)(const char *oldpath, const char *newpath);
typedef int (*linkat_t)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
typedef int (*symlink_t)(const char *target, const char *linkpath);
typedef int (*symlinkat_t)(const char *target, int newdirfd, const char *linkpath);
typedef int (*unlink_t)(const char *pathname);
typedef int (*unlinkat_t)(int dirfd, const char *pathname, int flags);
typedef ssize_t (*readlink_t)(const char *pathname, char *buf, size_t size);
typedef ssize_t (*readlinkat_t)(int dirfd, const char *pathname, char *buf, size_t size);

typedef int (*stat_t)(int ver, const char *pathname, struct stat *statbuf);
typedef int (*stat64_t)(int ver, const char *pathname, struct stat64 *statbuf);
typedef int (*lstat_t)(int ver, const char *pathname, struct stat *statbuf);
typedef int (*lstat64_t)(int ver, const char *pathname, struct stat64 *statbuf);
typedef int (*fstat_t)(int ver, int fd, struct stat *statbuf);
typedef int (*fstat64_t)(int ver, int fd, struct stat64 *statbuf);
typedef int (*fstatat_t)(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags);
typedef int (*fstatat64_t)(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags);
typedef int (*chmod_t)(const char *pathname, mode_t mode);
typedef int (*fchmod_t)(int fd, mode_t mode);
typedef int (*fchmodat_t)(int dirfd, const char *pathname, mode_t mode, int flags);
typedef int (*chown_t)(const char *pathname, uid_t owner, gid_t group);
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
typedef off64_t (*lseek64_t)(int fd, off64_t offset, int whence);

typedef int (*fdatasync_t)(int fd);
typedef int (*fsync_t)(int fd);

typedef void (*abort_t)();
typedef void (*_exit_t)();
typedef void (*exit_t)();
//typedef int (*sigaction_t)(int signum, const struct sigaction *act, struct sigaction *oldact);

static open_t real_open;
static openat_t real_openat;
static close_t real_close;
static rename_t real_rename;
static renameat_t real_renameat;
static renameat2_t real_renameat2;
static truncate_t real_truncate;
static ftruncate_t real_ftruncate;
static fallocate_t real_fallocate;
static posix_fallocate_t real_posix_fallocate;

static chdir_t real_chdir;
static fchdir_t real_fchdir;
static getcwd_t real_getcwd;
static mkdir_t real_mkdir;
static mkdirat_t real_mkdirat;
static rmdir_t real_rmdir;
static opendir_t real_opendir;
static fdopendir_t real_fdopendir;
static readdir_t real_readdir;
static closedir_t real_closedir;
static realpath_t real_realpath;

static link_t real_link;
static linkat_t real_linkat;
static symlink_t real_symlink;
static symlinkat_t real_symlinkat;
static unlink_t real_unlink;
static unlinkat_t real_unlinkat;
static readlink_t real_readlink;
static readlinkat_t real_readlinkat;

static stat_t real_stat;
static stat64_t real_stat64;
static lstat_t real_lstat;
static lstat64_t real_lstat64;
static fstat_t real_fstat;
static fstat64_t real_fstat64;
static fstatat_t real_fstatat;
static fstatat64_t real_fstatat64;
static chmod_t real_chmod;
static fchmod_t real_fchmod;
static fchmodat_t real_fchmodat;
static chown_t real_chown;
static lchown_t real_lchown;
static fchown_t real_fchown;
static fchownat_t real_fchownat;
static utime_t real_utime;
static utimes_t real_utimes;
static futimesat_t real_futimesat;
static utimensat_t real_utimensat;
static futimens_t real_futimens;
static access_t real_access;
static faccessat_t real_faccessat;

static setxattr_t real_setxattr;
static lsetxattr_t real_lsetxattr;
static fsetxattr_t real_fsetxattr;
static getxattr_t real_getxattr;
static lgetxattr_t real_lgetxattr;
static fgetxattr_t real_fgetxattr;
static listxattr_t real_listxattr;
static llistxattr_t real_llistxattr;
static flistxattr_t real_flistxattr;
static removexattr_t real_removexattr;
static lremovexattr_t real_lremovexattr;
static fremovexattr_t real_fremovexattr;

static fcntl_t real_fcntl;
static dup2_t real_dup2;
static dup3_t real_dup3;

static read_t real_read;
static readv_t real_readv;
static pread_t real_pread;
static preadv_t real_preadv;
static write_t real_write;
static writev_t real_writev;
static pwrite_t real_pwrite;
static pwritev_t real_pwritev;
static lseek_t real_lseek;
static lseek64_t real_lseek64;

static fdatasync_t real_fdatasync;
static fsync_t real_fsync;

static abort_t real_abort;
static _exit_t real__exit;
static exit_t real_exit;
//static sigaction_t real_sigaction;

/*
 * In many bash commands, e.g. touch, cat, etc, dup2 is used to redirect IO.
 * Thus, maintaining a map between system fd and CFS fd is necessary.
 */
#define _CFS_BASH
#ifdef _CFS_BASH
#define CFS_FD_MAP_SIZE 16
static int g_cfs_fd_map[CFS_FD_MAP_SIZE];
#endif

// whether the initialization has been done or not
static bool g_cfs_inited;
static void cfs_init();

#define CFS_FD_MASK (1 << (sizeof(int)*8 - 2))
// the current working directory, doesn't include the mount point part if in cfs
static char *g_cwd;
// whether the _cwd is in CFS or not
static bool g_in_cfs;
static int64_t g_cfs_client_id;
// hook or not, currently for test
static const bool g_hook = true;

static char *g_mount_point;
static char *g_ignore_path;
static cfs_config_t g_cfs_config;
static const char *CFS_CFG_PATH = "cfs_client.ini";
static const char *CFS_CFG_PATH_JED = "/export/servers/cfs/cfs_client.ini";

static bool g_has_renameat2 = false;

#if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
// map for each open fd to its pathname, to print pathname in debug log
static struct hsearch_data g_fdmap = {0};
#define FD_MAP_SIZE 100

static char *str_int(int i) {
    int len = snprintf(NULL, 0, "%d", i);
    char *key = malloc(len + 1);
    if(key == NULL) {
        return NULL;
    }
    snprintf(key, len + 1, "%d", i);
    return key;
}

static ENTRY *getFdEntry(int fd) {
    char *key = str_int(fd);
    if(key == NULL) {
        return NULL;
    }
    ENTRY *entry = malloc(sizeof(ENTRY));
    if(entry == NULL) {
        free(key);
        return NULL;
    }
    memset(entry, 0, sizeof(ENTRY));
    entry->key = key;
    if(!hsearch_r(*entry, FIND, &entry, &g_fdmap)) {
        free(entry);
        entry = NULL;
    }
    free(key);
    return entry;
}

static int addFdEntry(int fd, char *path) {
    ENTRY *entry = getFdEntry(fd);
    if(entry != NULL) {
        free(entry->data);
        entry->data = path;
        return 1;
    }
    char *key = str_int(fd);
    if(key == NULL) {
        return 0;
    }
    entry = malloc(sizeof(ENTRY));
    if(entry == NULL) {
        return 0;
    }
    memset(entry, 0, sizeof(ENTRY));
    entry->key = key;
    entry->data = path;
    int re = hsearch_r(*entry, ENTER, &entry, &g_fdmap);
    if(!re) {
        free(key);
        free(entry);
    }
    return re;
}
#endif

typedef struct {
     char* mount_point;
     char* ignore_path;
     char* log_dir;
     char* log_level;
     char* prof_port;
     char* master_addr;
     char* vol_name;
     char* owner;
     // whether to read from follower nodes or not, set "false" if want to read the newest data
     char* follower_read;
     char* app;
     char* auto_flush;
     char* master_client;
} client_config_t;

//static void (*g_sa_handler[30])(int);

static int config_handler(void* user, const char* section,
        const char* name, const char* value) {
    client_config_t *pconfig = (client_config_t*)user;
    #define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0

    if (MATCH("", "mountPoint")) {
        pconfig->mount_point = strdup(value);
    } else if (MATCH("", "ignorePath")) {
        pconfig->ignore_path = strdup(value);
    } else if (MATCH("", "volName")) {
        pconfig->vol_name = strdup(value);
    } else if (MATCH("", "owner")) {
        pconfig->owner = strdup(value);
    } else if (MATCH("", "masterAddr")) {
        pconfig->master_addr = strdup(value);
    } else if (MATCH("", "followerRead")) {
        pconfig->follower_read = strdup(value);
    } else if (MATCH("", "logDir")) {
        pconfig->log_dir = strdup(value);
    } else if (MATCH("", "logLevel")) {
        pconfig->log_level = strdup(value);
    } else if (MATCH("", "app")) {
        pconfig->app = strdup(value);
    } else if (MATCH("", "profPort")) {
        pconfig->prof_port = strdup(value);
    } else if (MATCH("", "autoFlush")) {
        pconfig->auto_flush = strdup(value);
    } else if (MATCH("", "masterClient")) {
        pconfig->master_client = strdup(value);
    } else {
        return 0;  /* unknown section/name, error */
    }
    return 1;
}

/*
 * get_clean_path is a c implementation of golang path.Clean().
 * The caller should free the returned buffer.
 *
 * Function returns the shortest path name equivalent to path
 * by purely lexical processing. It applies the following rules
 * iteratively until no further processing can be done:
 *
 *	1. Replace multiple slashes with a single slash.
 *	2. Eliminate each . path name element (the current directory).
 *	3. Eliminate each inner .. path name element (the parent directory)
 *	   along with the non-.. element that precedes it.
 *	4. Eliminate .. elements that begin a rooted path:
 *	   that is, replace "/.." by "/" at the beginning of a path.
 *
 * The returned path ends in a slash only if it is the root "/".
 *
 * If the result of this process is an empty string, function returns the string ".".
 */
static char *get_clean_path(const char *path) {
    if(path == NULL) {
        return NULL;
    }

    int rooted = path[0] == '/';
    int n = strlen(path);

    // Invariants:
    //	reading from path; r is index of next byte to process.
    //	writing to buf; w is index of next byte to write.
    //	dotdot is index in buf where .. must stop, either because
    //		it is the leading slash or it is a leading ../../.. prefix.
    char *out = (char *) malloc(n + 1);
    if(out == NULL) {
        return NULL;
    }
    int r = 0, w = 0, dotdot = 0;
    if(rooted) {
        out[w++] = '/';
        r = 1, dotdot = 1;
    }

    while(r < n) {
        if(path[r] == '/') {
            // empty path element
            r++;
        } else if(path[r] == '.' && (r + 1 == n || path[r + 1] == '/')) {
            // . element
            r++;
        } else if(path[r] == '.' && path[r + 1] == '.' && (r + 2 == n || path[r + 2] == '/')) {
            // .. element: remove to last /
            r += 2;
            if(w > dotdot) {
                // can backtrack
                w--;
                while(w > dotdot && out[w] != '/') {
                    w--;
                }
            } else if(!rooted) {
                // cannot backtrack, but not rooted, so append .. element.
                if(w > 0) {
                    out[w++] = '/';
                }

                out[w++] = '.';
                out[w++] = '.';
                dotdot = w;
            }
        } else {
            // real path element.
            // add slash if needed
            if(rooted && w != 1 || !rooted && w != 0) {
                out[w++] = '/';
            }
            // copy element
            for(; r < n && path[r] != '/'; r++) {
                out[w++] = path[r];
            }
        }
    }

    // Turn empty string into "."
    if(w == 0) {
        out[w++] = '.';
    }
    out[w] = '\0';
    return out;
}

/*
 * cat_path concatenate the cwd and the relative path.
 * The caller should free the returned buffer.
 */
static char *cat_path(const char *cwd, const char *pathname) {
    if(cwd == NULL || pathname == NULL) {
        return NULL;
    }

    int len = strlen(cwd) + strlen(pathname) + 2;
    char *path = (char *)malloc(len);
    if(path == NULL) {
        return NULL;
    }

    memset(path, '\0', len);
    strcat(path, cwd);
    strcat(path, "/");
    strcat(path, pathname);
    return path;
}

/*
 * Return the remainder part if input path is in CFS, stripping the mount point part.
 * The mount point part MUST be stripped before passing to CFS.
 * Return NULL if input path is not in CFS or an error occured.
 * The caller should free the returned buffer.
 */
static char *get_cfs_path(const char *pathname) {
    if(pathname == NULL || (pathname[0] != '/' && !g_in_cfs)) {
        return NULL;
    }

    // realpath() in glibc cannot be used here.
    // There are two reasons:
    // 1. realpath() depends on _lxstat64(), which in turn depends on get_cfs_path().
    //    This causes circular dependencies.
    // 2. realpath() uses _lxstat64() many times to validate directory,
    //    which is needless and harm the performance.
    char *real_path = get_clean_path(pathname);
    if(real_path == NULL) {
        return NULL;
    }

    char *result;
    if(pathname[0] != '/' && g_in_cfs) {
        result = cat_path(g_cwd, real_path);
        free(real_path);
        return result;
    }

    // check if real_path contains mount_point, and doesn't contain ignore_path
    // the mount_point has been strip off the last '/' in cfs_init()
    size_t len = strlen(g_mount_point);
    size_t len_real = strlen(real_path);
    bool is_cfs = false;
    char *ignore_path = strdup(g_ignore_path);
    if(ignore_path == NULL) {
        free(real_path);
        return NULL;
    }
    if(strncmp(real_path, g_mount_point, len) == 0) {
        if(strlen(g_ignore_path) > 0) {
            char *token = strtok(ignore_path, ",");
            size_t len_token;
            while(token != NULL) {
                len_token = strlen(token);
                if(real_path[len] == '/' && strncmp(real_path+len+1, token, len_token) == 0 && 
                (real_path[len+1+len_token] == '\0' || real_path[len+1+len_token] == '/')) {
                    is_cfs = false;
                    break;
                }
                is_cfs = true;
                token = strtok(NULL, ",");
            }
        } else if(real_path[len] == '\0' || real_path[len] == '/') {
            is_cfs = true;
        }
    }
    free(ignore_path);

    if (!is_cfs) {
        free(real_path);
        return NULL;
    }

    // strip the mount point part for path in CFS
    int len_result = len_real - len;
    result = (char *) malloc((len_result == 0 ? 1 : len_result) + 1);
    if (result == NULL) {
        free(real_path);
        return NULL;
    }
    if (len_result > 0) {
        memcpy(result, real_path + len, len_result);
    } else {
        result[0] = '/';
    }
    result[len_result == 0 ? 1 : len_result] = '\0';
    free(real_path);
    return result;
}

static void log_debug(const char* message, ...) {
    va_list args;
    va_start(args, message);
    /*
    char *func = va_arg(args, char *);
    va_end(args);
    if(!strstr(func, "write")) return;
    va_start(args, message);
    */
    va_end(args);
    struct timeval now;
    gettimeofday(&now, NULL);
    struct tm *ptm = localtime(&now.tv_sec);
    char buf[27];
    strftime(buf, 20, "%F %H:%M:%S", ptm);
    sprintf(buf + 19, ".%.6d", now.tv_usec);
    buf[26] = '\0';
    printf("%s [debug] ", buf);
    vprintf(message, args);
}

// process returned int from cfs functions
static int cfs_re(int re) {
    if(re < 0) {
        errno = -re;
        re = -1;
    } else {
        errno = 0;
    }
    return re;
}

// process returned ssize_t from cfs functions
static ssize_t cfs_sre(ssize_t re) {
    if(re < 0) {
        errno = -re;
        re = -1;
    } else {
        errno = 0;
    }
    return re;
}

/*
static void signal_handler(int signum) {
    cfs_flush_log();
    if(g_sa_handler[signum] && g_sa_handler[signum] != SIG_IGN && g_sa_handler[signum] != SIG_DFL) {
        g_sa_handler[signum](signum);
    }
    #ifdef _CFS_DEBUG
    printf("%s, signum:%d\n", __func__, signum);
    #endif
}
*/

bool has_renameat2() {
    const char *ver = gnu_get_libc_version();
    char *ver1 = strdup(ver);
    if(ver1 == NULL) {
        return false;
    }
    char *delimiter = strstr(ver1, ".");
    int len = 0;
    if(delimiter != NULL) {
        len = strlen(delimiter);
        delimiter[0] = '\0';
    }
    int major = atoi(ver1);
    int minor = 0;
    if(len > 1) {
        minor = atoi(delimiter + 1);
    }
    free(ver1);
    return major > 2 || (major == 2 && minor >= 28);
}

#endif
