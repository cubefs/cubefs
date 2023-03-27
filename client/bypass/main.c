// This is a kernel bypass client for ChubaoFS(CFS), using LD_PRELOAD to hook
// libc wrapper functions of file system calls.
//
// Design:
// CFS is virtually mounted at some mount point. All functons with paths or
// file descriptors belong to the mount point are distributed to CFS. Pathname
// prefix check is utilized if a function calls with pathname. Otherwise, the
// CFS bit is checked if a function calls with fd, which has been set when
// openning the file according to the fd.
//
// 1. The fd produced by CFS and system MUST be distinguished, to determine to
// distribute the system call to CFS or not. We use the second highest bit
// (named CFS bit) to indicate that the fd is from CFS. So, the original fd
// produced by CFS and system should be less than 1 << (sizeof(int)*8 - 2).
// Notice that the highest bit of fd is the sign bit(fd is of type int).
// The CFS bit of a fd is cleared before passing to CFS.
//
// 2. The current working directory is maintained to convert relative paths to
// absolute paths. If a path belongs to CFS, the mount point prefix is truncated
// before passing to CFS.
//
// 3. In order to support large file in 32-bit systems, some functions in glibc
// have 64-bit versions. These functions are defined as macros of 64-bit versions
// according to feature test macros. Simply treating 64-bit versions as weak
// aliases is practicable, except for stat functions.
//
// 4. The buffered functions like fopen, fread, fwrite, fclose CANNOT be hooked
// by hooking open, read, write, close, respectively. Because the internal
// functions of libc is called through symbol table, instead of PLT table.
// Rewriting all these functions in this lib is inadvisable. Instead, we rewrite
// the glibc buffered functions, replace internal calls on double-underscore
// names with normal ones, e.g. replace __open with open, etc.
// Refer to https://sourceware.org/glibc/wiki/Style_and_Conventions
//

#include "hook.h"

#define CHECK_UPDATE_INTERVAL 5
#define CFS_CFG_PATH "cfs_client.ini"
#define CFS_CFG_PATH_JED "/export/servers/cfs/cfs_client.ini"
#ifdef DYNAMIC_UPDATE
#define LIB_EMPTY "/usr/lib64/libempty.so"
#endif
#define SDK_SO "libcfssdk.so"
#define C_SO "libcfsc.so"

static bool g_inited;
pthread_rwlock_t update_rwlock;
char *lib_cfssdk;
char *lib_cfsc;
char *config_path;

#define LOCK(cmd, res)                                                                     \
    do {                                                                                   \
        if(!g_inited) {                                                                    \
            res = libc_##cmd;                                                              \
            break;                                                                         \
        }                                                                                  \
        errno = pthread_rwlock_rdlock(&update_rwlock);                                     \
        if(errno == 0)                                                                     \
        {                                                                                  \
            res = real_##cmd;                                                              \
            pthread_rwlock_unlock(&update_rwlock);                                         \
        }                                                                                  \
        else if(errno == EDEADLK)                                                          \
        {                                                                                  \
            res = libc_##cmd;                                                              \
            fprintf(stderr, "Warning: Updating. Operation %s call libc function.\n", __func__);   \
        }                                                                                  \
    } while(0)

#define LOCK_RETURN_INT(cmd)      \
    int res = -1;                 \
    LOCK(cmd, res);               \
    return res

#define LOCK_RETURN_LONG(cmd)     \
    long res = -1;                \
    LOCK(cmd, res);               \
    return res

#define LOCK_RETURN_PTR(cmd, res) \
    res = NULL;                   \
    LOCK(cmd, res);               \
    return res


/*
 * File operations
 */

int close(int fd) {
    LOCK_RETURN_INT(close(fd));
}

int creat(const char *pathname, mode_t mode) {
    return openat(AT_FDCWD, pathname, O_CREAT|O_WRONLY|O_TRUNC, mode);
}
weak_alias (creat, creat64)

int open(const char *pathname, int flags, ...) {
    mode_t mode = 0;
    if(flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    return openat(AT_FDCWD, pathname, flags, mode);
}
weak_alias (open, open64)

int openat(int dirfd, const char *pathname, int flags, ...) {
    mode_t mode = 0;
    if(flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    LOCK_RETURN_INT(openat(dirfd, pathname, flags, mode));
}
weak_alias (openat, openat64)

// rename between cfs and ordinary file is not allowed
int renameat2(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname, unsigned int flags) {
    LOCK_RETURN_INT(renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags));
}

int rename(const char *old_pathname, const char *new_pathname) {
    return renameat(AT_FDCWD, old_pathname, AT_FDCWD, new_pathname);
}

int renameat(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname) {
    LOCK_RETURN_INT(renameat(olddirfd, old_pathname, newdirfd, new_pathname));
}

int truncate(const char *pathname, off_t length) {
    LOCK_RETURN_INT(truncate(pathname, length));
}

int ftruncate(int fd, off_t length) {
    LOCK_RETURN_INT(ftruncate(fd, length));
}
weak_alias (ftruncate, ftruncate64)

int fallocate(int fd, int mode, off_t offset, off_t len) {
    LOCK_RETURN_INT(fallocate(fd, mode, offset, len));
}
weak_alias (fallocate, fallocate64)

int posix_fallocate(int fd, off_t offset, off_t len) {
    LOCK_RETURN_INT(posix_fallocate(fd, offset, len));
}
weak_alias (posix_fallocate, posix_fallocate64)

/*
 * Directory operations
 */

int mkdir(const char *pathname, mode_t mode) {
    return mkdirat(AT_FDCWD, pathname, mode);
}

int mkdirat(int dirfd, const char *pathname, mode_t mode) {
    LOCK_RETURN_INT(mkdirat(dirfd, pathname, mode));
}

int rmdir(const char *pathname) {
    LOCK_RETURN_INT(rmdir(pathname));
}

char *getcwd(char *buf, size_t size) {
    char *res;
    LOCK_RETURN_PTR(getcwd(buf, size), res);
}

int chdir(const char *pathname) {
    LOCK_RETURN_INT(chdir(pathname));
}

int fchdir(int fd) {
    LOCK_RETURN_INT(fchdir(fd));
}

DIR *opendir(const char *pathname) {
    DIR *res;
    LOCK_RETURN_PTR(opendir(pathname), res);
}

DIR *fdopendir(int fd) {
    DIR *res;
    LOCK_RETURN_PTR(fdopendir(fd), res);
}

struct dirent *readdir(DIR *dirp) {
    struct dirent *res;
    LOCK_RETURN_PTR(readdir(dirp), res);
}

struct dirent64 *readdir64(DIR *dirp) {
    return (struct dirent64 *)readdir(dirp);
}

int closedir(DIR *dirp) {
    LOCK_RETURN_INT(closedir(dirp));
}

char *realpath(const char *path, char *resolved_path) {
    char *res;
    LOCK_RETURN_PTR(realpath(path, resolved_path), res);
}

char *__realpath_chk (const char *buf, char *resolved, size_t resolvedlen) {
    char *res;
    LOCK_RETURN_PTR(realpath_chk(buf, resolved, resolvedlen), res);
}

/*
 * Link operations
 */

// link between CFS and ordinary file is not allowed
int link(const char *old_pathname, const char *new_pathname) {
    return linkat(AT_FDCWD, old_pathname, AT_FDCWD, new_pathname, 0);
}

// link between CFS and ordinary file is not allowed
int linkat(int olddirfd, const char *old_pathname,
           int newdirfd, const char *new_pathname, int flags) {
    LOCK_RETURN_INT(linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags));
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlink(const char *target, const char *linkpath) {
    return symlinkat(target, AT_FDCWD, linkpath);
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlinkat(const char *target, int dirfd, const char *linkpath) {
    LOCK_RETURN_INT(symlinkat(target, dirfd, linkpath));
}

int unlink(const char *pathname) {
    return unlinkat(AT_FDCWD, pathname, 0);
}

int unlinkat(int dirfd, const char *pathname, int flags) {
    LOCK_RETURN_INT(unlinkat(dirfd, pathname, flags));
}

ssize_t readlink(const char *pathname, char *buf, size_t size) {
    return readlinkat(AT_FDCWD, pathname, buf, size);
}

ssize_t readlinkat(int dirfd, const char *pathname, char *buf, size_t size) {
    LOCK_RETURN_LONG(readlinkat(dirfd, pathname, buf, size));
}


/*
 * Basic file attributes
 *
 * According to sys/stat.h, stat, fstat, lstat, fstatat are macros in glibc 2.17,
 * the actually called functions are __xstat, __fxstat, __lxstat, __fxstatat,
 * respectively. And because they are handled in header file, the original
 * functions cannot be intercepted.
 *
 * The 64-bit versions cannot be ignored, or realized as weak symbols, because
 * in glibc the original versions and the 64-bit versions have different signatures,
 * and struct stat and struct stat64 are defined independently.
 */

int __xstat(int ver, const char *pathname, struct stat *statbuf) {
    LOCK_RETURN_INT(stat(ver, pathname, statbuf));
}

int __xstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    LOCK_RETURN_INT(stat64(ver, pathname, statbuf));
}

int __lxstat(int ver, const char *pathname, struct stat *statbuf) {
    LOCK_RETURN_INT(lstat(ver, pathname, statbuf));
}

int __lxstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    LOCK_RETURN_INT(lstat64(ver, pathname, statbuf));
}

int __fxstat(int ver, int fd, struct stat *statbuf) {
    LOCK_RETURN_INT(fstat(ver, fd, statbuf));
}

int __fxstat64(int ver, int fd, struct stat64 *statbuf) {
    LOCK_RETURN_INT(fstat64(ver, fd, statbuf));
}

int __fxstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    LOCK_RETURN_INT(fstatat(ver, dirfd, pathname, statbuf, flags));
}

int __fxstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    LOCK_RETURN_INT(fstatat64(ver, dirfd, pathname, statbuf, flags));
}

int chmod(const char *pathname, mode_t mode) {
    return fchmodat(AT_FDCWD, pathname, mode, 0);
}

int fchmod(int fd, mode_t mode) {
    LOCK_RETURN_INT(fchmod(fd, mode));
}

int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    LOCK_RETURN_INT(fchmodat(dirfd, pathname, mode, flags));
}

int chown(const char *pathname, uid_t owner, gid_t group) {
    return fchownat(AT_FDCWD, pathname, owner, group, 0);
}

int lchown(const char *pathname, uid_t owner, gid_t group) {
    LOCK_RETURN_INT(lchown(pathname, owner, group));
}

int fchown(int fd, uid_t owner, gid_t group) {
    LOCK_RETURN_INT(fchown(fd, owner, group));
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) {
    LOCK_RETURN_INT(fchownat(dirfd, pathname, owner, group, flags));
}

int utime(const char *pathname, const struct utimbuf *times) {
    LOCK_RETURN_INT(utime(pathname, times));
}

int utimes(const char *pathname, const struct timeval *times) {
    LOCK_RETURN_INT(utimes(pathname, times));
}

int futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    LOCK_RETURN_INT(futimesat(dirfd, pathname, times));
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    LOCK_RETURN_INT(utimensat(dirfd, pathname, times, flags));
}

int futimens(int fd, const struct timespec times[2]) {
    LOCK_RETURN_INT(futimens(fd, times));
}

int access(const char *pathname, int mode) {
    return faccessat(AT_FDCWD, pathname, mode, 0);
}

int faccessat(int dirfd, const char *pathname, int mode, int flags) {
    LOCK_RETURN_INT(faccessat(dirfd, pathname, mode, flags));
}


/*
 * Extended file attributes
 */

int setxattr(const char *pathname, const char *name,
        const void *value, size_t size, int flags) {
    LOCK_RETURN_INT(setxattr(pathname, name, value, size, flags));
}

int lsetxattr(const char *pathname, const char *name,
             const void *value, size_t size, int flags) {
    LOCK_RETURN_INT(lsetxattr(pathname, name, value, size, flags));
}

int fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    LOCK_RETURN_INT(fsetxattr(fd, name, value, size, flags));
}

ssize_t getxattr(const char *pathname, const char *name, void *value, size_t size) {
    LOCK_RETURN_LONG(getxattr(pathname, name, value, size));
}

ssize_t lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    LOCK_RETURN_LONG(lgetxattr(pathname, name, value, size));
}

ssize_t fgetxattr(int fd, const char *name, void *value, size_t size) {
    LOCK_RETURN_LONG(fgetxattr(fd, name, value, size));
}

ssize_t listxattr(const char *pathname, char *list, size_t size) {
    LOCK_RETURN_LONG(listxattr(pathname, list, size));
}

ssize_t llistxattr(const char *pathname, char *list, size_t size) {
    LOCK_RETURN_LONG(llistxattr(pathname, list, size));
}

ssize_t flistxattr(int fd, char *list, size_t size) {
    LOCK_RETURN_LONG(flistxattr(fd, list, size));
}

int removexattr(const char *pathname, const char *name) {
    LOCK_RETURN_INT(removexattr(pathname, name));
}

int lremovexattr(const char *pathname, const char *name) {
    LOCK_RETURN_INT(lremovexattr(pathname, name));
}

int fremovexattr(int fd, const char *name) {
    LOCK_RETURN_INT(fremovexattr(fd, name));
}


/*
 * File descriptor manipulations
 */

int fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    void *arg = va_arg(args, void *);
    va_end(args);
    LOCK_RETURN_INT(fcntl(fd, cmd, arg));
}
weak_alias (fcntl, fcntl64)

int dup2(int oldfd, int newfd) {
    LOCK_RETURN_INT(dup2(oldfd, newfd));
}

int dup3(int oldfd, int newfd, int flags) {
    LOCK_RETURN_INT(dup3(oldfd, newfd, flags));
}


/*
 * Read & Write
 */

ssize_t read(int fd, void *buf, size_t count) {
    LOCK_RETURN_LONG(read(fd, buf, count));
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    LOCK_RETURN_LONG(readv(fd, iov, iovcnt));
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    LOCK_RETURN_LONG(pread(fd, buf, count, offset));
}
weak_alias (pread, pread64)

ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    LOCK_RETURN_LONG(preadv(fd, iov, iovcnt, offset));
}

ssize_t write(int fd, const void *buf, size_t count) {
    LOCK_RETURN_LONG(write(fd, buf, count));
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    LOCK_RETURN_LONG(writev(fd, iov, iovcnt));
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    LOCK_RETURN_LONG(pwrite(fd, buf, count, offset));
}
weak_alias (pwrite, pwrite64)

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    LOCK_RETURN_LONG(pwritev(fd, iov, iovcnt, offset));
}

off_t lseek(int fd, off_t offset, int whence) {
    LOCK_RETURN_LONG(lseek(fd, offset, whence));
}
weak_alias (lseek, lseek64)


/*
 * Synchronized I/O
 */

int fdatasync(int fd) {
    LOCK_RETURN_INT(fdatasync(fd));
}

int fsync(int fd) {
    LOCK_RETURN_INT(fsync(fd));
}


/*
 * Others
 */

void abort() {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        if(flush_logs != NULL) {
            flush_logs();
        }
        pthread_rwlock_unlock(&update_rwlock);
    }
    libc_abort();

    // abort is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {}
}

void _exit(int status) {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        if(flush_logs != NULL) {
            flush_logs();
        }
        pthread_rwlock_unlock(&update_rwlock);
    }
    libc__exit(status);

    // _exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {}
}

void exit(int status) {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        if(flush_logs != NULL) {
            flush_logs();
        }
        pthread_rwlock_unlock(&update_rwlock);
    }
    libc_exit(status);

    // exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {}
}

/*
 * The setup may have not been called when the hook function is called.
 * Call cfs_init() when necessary, especially in bash environments.
 *
 * Refer to https://tbrindus.ca/correct-ld-preload-hooking-libc/
 */
__attribute__((constructor)) static void setup(void) {
    init();
}

__attribute__((destructor)) static void destroy(void) {
    pthread_rwlock_destroy(&update_rwlock);
    if(flush_logs != NULL) {
        flush_logs();
    }
    if(lib_cfssdk != NULL) {
        free(lib_cfssdk);
    }
    if(lib_cfsc != NULL) {
        free(lib_cfsc);
    }
    if(config_path != NULL) {
        free(config_path);
    }
}

static void init() {
    config_path = getenv("CFS_CONFIG_PATH");
    if(config_path == NULL) {
        config_path = CFS_CFG_PATH;
        if(libc_access(config_path, F_OK)) {
            config_path = CFS_CFG_PATH_JED;
        }
    }
    config_path = strdup(config_path);

    char *p = strrchr(config_path, '/');
    if(p == NULL) {
        lib_cfssdk = (char *)malloc(strlen(SDK_SO) + 3);
        memset(lib_cfssdk, 0, strlen(SDK_SO) + 3);
        sprintf(lib_cfssdk, "./%s", SDK_SO);
        lib_cfsc = (char *)malloc(strlen(C_SO) + 3);
        memset(lib_cfsc, 0, strlen(C_SO) + 3);
        sprintf(lib_cfsc, "./%s", C_SO);
    } else {
        char tmp = *(p+1);
        *(p+1) = 0;
        lib_cfssdk = (char *)malloc(strlen(config_path) + strlen(SDK_SO) + 1);
        memset(lib_cfssdk, 0, strlen(config_path) + strlen(SDK_SO) + 1);
        sprintf(lib_cfssdk, "%s%s", config_path, SDK_SO);
        lib_cfsc = (char *)malloc(strlen(config_path) + strlen(C_SO) + 1);
        memset(lib_cfsc, 0, strlen(config_path) + strlen(C_SO) + 1);
        sprintf(lib_cfsc, "%s%s", config_path, C_SO);
        *(p+1) = tmp;

    }
    setenv("CFS_CONFIG_PATH", config_path, 1);
    setenv("CFS_CFSSDK_PATH", lib_cfssdk, 1);
    setenv("CFS_CFSC_PATH", lib_cfsc, 1);

    void* handle;
    #ifdef DYNAMIC_UPDATE
    handle = base_open(LIB_EMPTY);
    if(handle == NULL) {
        fprintf(stderr, "dlopen /usr/lib64/libempty.so error: %s.\n", dlerror());
        libc_exit(1);
    }
    #endif
    handle = dlopen(lib_cfsc, RTLD_NOW|RTLD_GLOBAL);
    if(handle == NULL) {
        fprintf(stderr, "dlopen %s error: %s\n", lib_cfsc, dlerror());
        libc_exit(1);
    }
    init_cfsc_func(handle);
    int res = start_libs(NULL);
    if(res != 0) {
        fprintf(stderr, "start libs error.");
        libc_exit(1);
    }

    pthread_rwlock_init(&update_rwlock, NULL);
    pthread_t thread;
    if(pthread_create(&thread, NULL, &update_dynamic_libs, handle)) {
        fprintf(stderr, "pthread_create update_dynamic_libs error.\n");
        libc_exit(1);
    }
    g_inited = true;
}

static void init_cfsc_func(void *handle) {
    start_libs = (start_libs_t)dlsym(handle, "start_libs");
    stop_libs = (stop_libs_t)dlsym(handle, "stop_libs");
    flush_logs = (flush_logs_t)dlsym(handle, "flush_logs");

    real_openat = (openat_t)dlsym(handle, "real_openat");
    real_close = (close_t)dlsym(handle, "real_close");
    real_renameat = (renameat_t)dlsym(handle, "real_renameat");
    real_renameat2 = (renameat2_t)dlsym(handle, "real_renameat2");
    real_truncate = (truncate_t)dlsym(handle, "real_truncate");
    real_ftruncate = (ftruncate_t)dlsym(handle, "real_ftruncate");
    real_fallocate = (fallocate_t)dlsym(handle, "real_fallocate");
    real_posix_fallocate = (posix_fallocate_t)dlsym(handle, "real_posix_fallocate");

    real_chdir = (chdir_t)dlsym(handle, "real_chdir");
    real_fchdir = (fchdir_t)dlsym(handle, "real_fchdir");
    real_getcwd = (getcwd_t)dlsym(handle, "real_getcwd");
    real_mkdirat = (mkdirat_t)dlsym(handle, "real_mkdirat");
    real_rmdir = (rmdir_t)dlsym(handle, "real_rmdir");
    real_opendir = (opendir_t)dlsym(handle, "real_opendir");
    real_fdopendir = (fdopendir_t)dlsym(handle, "real_fdopendir");
    real_readdir = (readdir_t)dlsym(handle, "real_readdir");
    real_closedir = (closedir_t)dlsym(handle, "real_closedir");
    real_realpath = (realpath_t)dlsym(handle, "real_realpath");
    real_realpath_chk = (realpath_chk_t)dlsym(handle, "real_realpath_chk");

    real_linkat = (linkat_t)dlsym(handle, "real_linkat");
    real_symlinkat = (symlinkat_t)dlsym(handle, "real_symlinkat");
    real_unlinkat = (unlinkat_t)dlsym(handle, "real_unlinkat");
    real_readlinkat = (readlinkat_t)dlsym(handle, "real_readlinkat");

    real_stat = (stat_t)dlsym(handle, "real_stat");
    real_stat64 = (stat64_t)dlsym(handle, "real_stat64");
    real_lstat = (lstat_t)dlsym(handle, "real_lstat");
    real_lstat64 = (lstat64_t)dlsym(handle, "real_lstat64");
    real_fstat = (fstat_t)dlsym(handle, "real_fstat");
    real_fstat64 = (fstat64_t)dlsym(handle, "real_fstat64");
    real_fstatat = (fstatat_t)dlsym(handle, "real_fstatat");
    real_fstatat64 = (fstatat64_t)dlsym(handle, "real_fstatat64");
    real_fchmod = (fchmod_t)dlsym(handle, "real_fchmod");
    real_fchmodat = (fchmodat_t)dlsym(handle, "real_fchmodat");
    real_lchown = (lchown_t)dlsym(handle, "real_lchown");
    real_fchown = (fchown_t)dlsym(handle, "real_fchown");
    real_fchownat = (fchownat_t)dlsym(handle, "real_fchownat");
    real_utime = (utime_t)dlsym(handle, "real_utime");
    real_utimes = (utimes_t)dlsym(handle, "real_utimes");
    real_futimesat = (futimesat_t)dlsym(handle, "real_futimesat");
    real_utimensat = (utimensat_t)dlsym(handle, "real_utimensat");
    real_futimens = (futimens_t)dlsym(handle, "real_futimens");
    real_faccessat = (faccessat_t)dlsym(handle, "real_faccessat");

    real_setxattr = (setxattr_t)dlsym(handle, "real_setxattr");
    real_lsetxattr = (lsetxattr_t)dlsym(handle, "real_lsetxattr");
    real_fsetxattr = (fsetxattr_t)dlsym(handle, "real_fsetxattr");
    real_getxattr = (getxattr_t)dlsym(handle, "real_getxattr");
    real_lgetxattr = (lgetxattr_t)dlsym(handle, "real_lgetxattr");
    real_fgetxattr = (fgetxattr_t)dlsym(handle, "real_fgetxattr");
    real_listxattr = (listxattr_t)dlsym(handle, "real_listxattr");
    real_llistxattr = (llistxattr_t)dlsym(handle, "real_llistxattr");
    real_flistxattr = (flistxattr_t)dlsym(handle, "real_flistxattr");
    real_removexattr = (removexattr_t)dlsym(handle, "real_removexattr");
    real_lremovexattr = (lremovexattr_t)dlsym(handle, "real_lremovexattr");
    real_fremovexattr = (fremovexattr_t)dlsym(handle, "real_fremovexattr");

    real_fcntl = (fcntl_t)dlsym(handle, "real_fcntl");
    real_dup2 = (dup2_t)dlsym(handle, "real_dup2");
    real_dup3 = (dup3_t)dlsym(handle, "real_dup3");

    real_read = (read_t)dlsym(handle, "real_read");
    real_readv = (readv_t)dlsym(handle, "real_readv");
    real_pread = (pread_t)dlsym(handle, "real_pread");
    real_preadv = (preadv_t)dlsym(handle, "real_preadv");
    real_write = (write_t)dlsym(handle, "real_write");
    real_writev = (writev_t)dlsym(handle, "real_writev");
    real_pwrite = (pwrite_t)dlsym(handle, "real_pwrite");
    real_pwritev = (pwritev_t)dlsym(handle, "real_pwritev");
    real_lseek = (lseek_t)dlsym(handle, "real_lseek");

    real_fdatasync = (fdatasync_t)dlsym(handle, "real_fdatasync");
    real_fsync = (fsync_t)dlsym(handle, "real_fsync");
}


static void *update_dynamic_libs(void* handle) {
    char* reload;
    char* ld_preload = getenv("LD_PRELOAD");
    char* mount_point = getenv("CFS_MOUNT_POINT");
    char* config_path = getenv("CFS_CONFIG_PATH");

    while(1) {
        sleep(CHECK_UPDATE_INTERVAL);
        reload = getenv("RELOAD_CLIENT");
        if (reload == NULL)
            continue;
        if (strcmp(reload, "1") != 0 && strcmp(reload, "test") != 0)
            continue;

        pthread_rwlock_wrlock(&update_rwlock);
        reload = getenv("RELOAD_CLIENT");
        if (reload == NULL) {
            pthread_rwlock_unlock(&update_rwlock);
            continue;
        }

        fprintf(stderr, "Begin to update client.\n");
        flush_logs = NULL;
        void *client_state = stop_libs();
        if(client_state == NULL) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "stop libs error.");
            libc_exit(1);
        }

        int res = dlclose(handle);
        if(res != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "dlclose error: %s\n", dlerror());
            libc_exit(1);
        }
        fprintf(stderr, "finish dlclose client.\n");

        if (strcmp(reload, "test") == 0)
            sleep(CHECK_UPDATE_INTERVAL);

        setenv("LD_PRELOAD", ld_preload, 1);
        setenv("CFS_CONFIG_PATH", config_path, 1);
        setenv("CFS_CFSSDK_PATH", lib_cfssdk, 1);
        setenv("CFS_CFSC_PATH", lib_cfsc, 1);
        if(mount_point != NULL) setenv("CFS_MOUNT_POINT", mount_point, 1);

        unsetenv("RELOAD_CLIENT");

        handle = dlopen(lib_cfsc, RTLD_NOW|RTLD_GLOBAL);
        if(handle == NULL) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "dlopen %s error: %s\n", lib_cfsc, dlerror());
            libc_exit(1);
        }
        fprintf(stderr, "finish dlopen client.\n");

        init_cfsc_func(handle);
        res = start_libs(client_state);
        if(res != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "start libs error.");
            libc_exit(1);
        }
        pthread_rwlock_unlock(&update_rwlock);
        fprintf(stderr, "finish update client.\n");
    }
}
