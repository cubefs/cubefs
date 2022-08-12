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

#define LOCK(cmd)                          \
    int res;                               \
    LOCK_WITH_RES(cmd, res)

#define LOCK_WITH_RES(cmd, res)            \
    pthread_rwlock_rdlock(&update_rwlock); \
    res = cmd;                             \
    pthread_rwlock_unlock(&update_rwlock); \
    return res

/*
 * File operations
 */

int close(int fd) {
    LOCK(real_close(fd));
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
    LOCK(real_openat(dirfd, pathname, flags, mode));
}
weak_alias (openat, openat64)

// rename between cfs and ordinary file is not allowed
int renameat2(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname, unsigned int flags) {
    LOCK(real_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags));
}

int rename(const char *old_pathname, const char *new_pathname) {
    return renameat2(AT_FDCWD, old_pathname, AT_FDCWD, new_pathname, 0);
}

int renameat(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname) {
    LOCK(real_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, 0));
}

int truncate(const char *pathname, off_t length) {
    LOCK(real_truncate(pathname, length));
}

int ftruncate(int fd, off_t length) {
    LOCK(real_ftruncate(fd, length));
}
weak_alias (ftruncate, ftruncate64)

int fallocate(int fd, int mode, off_t offset, off_t len) {
    LOCK(real_fallocate(fd, mode, offset, len));
}
weak_alias (fallocate, fallocate64)

int posix_fallocate(int fd, off_t offset, off_t len) {
    LOCK(real_posix_fallocate(fd, offset, len));
}
weak_alias (posix_fallocate, posix_fallocate64)

/*
 * Directory operations
 */

int mkdir(const char *pathname, mode_t mode) {
    return mkdirat(AT_FDCWD, pathname, mode);
}

int mkdirat(int dirfd, const char *pathname, mode_t mode) {
    LOCK(real_mkdirat(dirfd, pathname, mode));
}

int rmdir(const char *pathname) {
    LOCK(real_rmdir(pathname));
}

char *getcwd(char *buf, size_t size) {
    char *res;
    LOCK_WITH_RES(real_getcwd(buf, size), res);
}

int chdir(const char *pathname) {
    LOCK(real_chdir(pathname));
}

int fchdir(int fd) {
    LOCK(real_fchdir(fd));
}

DIR *opendir(const char *pathname) {
    DIR *res;
    LOCK_WITH_RES(real_opendir(pathname), res);
}

DIR *fdopendir(int fd) {
    DIR *res;
    LOCK_WITH_RES(real_fdopendir(fd), res);
}

struct dirent *readdir(DIR *dirp) {
    struct dirent *res;
    LOCK_WITH_RES(real_readdir(dirp), res);
}

struct dirent64 *readdir64(DIR *dirp) {
    return (struct dirent64 *)readdir(dirp);
}

int closedir(DIR *dirp) {
    LOCK(real_closedir(dirp));
}

char *realpath(const char *path, char *resolved_path) {
    char *res;
    LOCK_WITH_RES(real_realpath(path, resolved_path), res);
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
    LOCK(real_linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags));
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlink(const char *target, const char *linkpath) {
    return symlinkat(target, AT_FDCWD, linkpath);
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlinkat(const char *target, int dirfd, const char *linkpath) {
    LOCK(real_symlinkat(target, dirfd, linkpath));
}

int unlink(const char *pathname) {
    return unlinkat(AT_FDCWD, pathname, 0);
}

int unlinkat(int dirfd, const char *pathname, int flags) {
    LOCK(real_unlinkat(dirfd, pathname, flags));
}

ssize_t readlink(const char *pathname, char *buf, size_t size) {
    return readlinkat(AT_FDCWD, pathname, buf, size);
}

ssize_t readlinkat(int dirfd, const char *pathname, char *buf, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_readlinkat(dirfd, pathname, buf, size), res);
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
    LOCK(real_stat(ver, pathname, statbuf));
}

int __xstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    LOCK(real_stat64(ver, pathname, statbuf));
}

int __lxstat(int ver, const char *pathname, struct stat *statbuf) {
    LOCK(real_lstat(ver, pathname, statbuf));
}

int __lxstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    LOCK(real_lstat64(ver, pathname, statbuf));
}

int __fxstat(int ver, int fd, struct stat *statbuf) {
    LOCK(real_fstat(ver, fd, statbuf));
}

int __fxstat64(int ver, int fd, struct stat64 *statbuf) {
    LOCK(real_fstat64(ver, fd, statbuf));
}

int __fxstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    LOCK(real_fstatat(ver, dirfd, pathname, statbuf, flags));
}

int __fxstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    LOCK(real_fstatat64(ver, dirfd, pathname, statbuf, flags));
}

int chmod(const char *pathname, mode_t mode) {
    return fchmodat(AT_FDCWD, pathname, mode, 0);
}

int fchmod(int fd, mode_t mode) {
    LOCK(real_fchmod(fd, mode));
}

int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    LOCK(real_fchmodat(dirfd, pathname, mode, flags));
}

int chown(const char *pathname, uid_t owner, gid_t group) {
    return fchownat(AT_FDCWD, pathname, owner, group, 0);
}

int lchown(const char *pathname, uid_t owner, gid_t group) {
    LOCK(real_lchown(pathname, owner, group));
}

int fchown(int fd, uid_t owner, gid_t group) {
    LOCK(real_fchown(fd, owner, group));
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) {
    LOCK(real_fchownat(dirfd, pathname, owner, group, flags));
}

int utime(const char *pathname, const struct utimbuf *times) {
    LOCK(real_utime(pathname, times));
}

int utimes(const char *pathname, const struct timeval *times) {
    LOCK(real_utimes(pathname, times));
}

int futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    LOCK(real_futimesat(dirfd, pathname, times));
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    LOCK(real_utimensat(dirfd, pathname, times, flags));
}

int futimens(int fd, const struct timespec times[2]) {
    LOCK(real_futimens(fd, times));
}

int access(const char *pathname, int mode) {
    return faccessat(AT_FDCWD, pathname, mode, 0);
}

int faccessat(int dirfd, const char *pathname, int mode, int flags) {
     // libdl.so call access before libcfsclient.so inited
    if(!g_inited) {
        faccessat_t libc_faccessat = (faccessat_t)dlsym(RTLD_NEXT, "faccessat");
        return libc_faccessat(dirfd, pathname, mode, flags);
    }
    LOCK(real_faccessat(dirfd, pathname, mode, flags));
}


/*
 * Extended file attributes
 */

int setxattr(const char *pathname, const char *name,
        const void *value, size_t size, int flags) {
    LOCK(real_setxattr(pathname, name, value, size, flags));
}

int lsetxattr(const char *pathname, const char *name,
             const void *value, size_t size, int flags) {
    LOCK(real_lsetxattr(pathname, name, value, size, flags));
}

int fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    LOCK(real_fsetxattr(fd, name, value, size, flags));
}

ssize_t getxattr(const char *pathname, const char *name, void *value, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_getxattr(pathname, name, value, size), res);
}

ssize_t lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_lgetxattr(pathname, name, value, size), res);
}

ssize_t fgetxattr(int fd, const char *name, void *value, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_fgetxattr(fd, name, value, size), res);
}

ssize_t listxattr(const char *pathname, char *list, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_listxattr(pathname, list, size), res);
}

ssize_t llistxattr(const char *pathname, char *list, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_llistxattr(pathname, list, size), res);
}

ssize_t flistxattr(int fd, char *list, size_t size) {
    ssize_t res;
    LOCK_WITH_RES(real_flistxattr(fd, list, size), res);
}

int removexattr(const char *pathname, const char *name) {
    LOCK(real_removexattr(pathname, name));
}

int lremovexattr(const char *pathname, const char *name) {
    LOCK(real_lremovexattr(pathname, name));
}

int fremovexattr(int fd, const char *name) {
    LOCK(real_fremovexattr(fd, name));
}


/*
 * File descriptor manipulations
 */

int fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    void *arg = va_arg(args, void *);
    va_end(args);
    LOCK(real_fcntl(fd, cmd, arg));
}
weak_alias (fcntl, fcntl64)

int dup2(int oldfd, int newfd) {
    LOCK(real_dup2(oldfd, newfd));
}

int dup3(int oldfd, int newfd, int flags) {
    LOCK(real_dup3(oldfd, newfd, flags));
}


/*
 * Read & Write
 */

ssize_t read(int fd, void *buf, size_t count) {
    ssize_t res;
    LOCK_WITH_RES(real_read(fd, buf, count), res);
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    ssize_t res;
    LOCK_WITH_RES(real_readv(fd, iov, iovcnt), res);
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    ssize_t res;
    LOCK_WITH_RES(real_pread(fd, buf, count, offset), res);
}
weak_alias (pread, pread64)

ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    ssize_t res;
    LOCK_WITH_RES(real_preadv(fd, iov, iovcnt, offset), res);
}

ssize_t write(int fd, const void *buf, size_t count) {
    ssize_t res;
    LOCK_WITH_RES(real_write(fd, buf, count), res);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    ssize_t res;
    LOCK_WITH_RES(real_writev(fd, iov, iovcnt), res);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    ssize_t res;
    LOCK_WITH_RES(real_pwrite(fd, buf, count, offset), res);
}
weak_alias (pwrite, pwrite64)

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    ssize_t res;
    LOCK_WITH_RES(real_pwritev(fd, iov, iovcnt, offset), res);
}

off_t lseek(int fd, off_t offset, int whence) {
    off_t res;
    LOCK_WITH_RES(real_lseek(fd, offset, whence), res);
}
weak_alias (lseek, lseek64)


/*
 * Synchronized I/O
 */

int fdatasync(int fd) {
    LOCK(real_fdatasync(fd));
}

int fsync(int fd) {
    LOCK(real_fsync(fd));
}


/*
 * Others
 */

void abort() {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        flush_logs();
        pthread_rwlock_unlock(&update_rwlock);
    }
    void (*libc_abort)() = dlsym(RTLD_NEXT, "abort");
    libc_abort();

    // abort is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {}
}

void _exit(int status) {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        flush_logs();
        pthread_rwlock_unlock(&update_rwlock);
    }
    void (*libc__exit)(int) = dlsym(RTLD_NEXT, "_exit");
    libc__exit(status);
    // _exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {}
}

void exit(int status) {
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        flush_logs();
        pthread_rwlock_unlock(&update_rwlock);
    }
    void (*libc_exit)(int) = dlsym(RTLD_NEXT, "exit");
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
    flush_logs();
    pthread_rwlock_destroy(&update_rwlock);
}

static void init() {
    void* handle;
    handle = base_open("/usr/lib64/libempty.so");
    if(handle == NULL) {
        fprintf(stderr, "dlopen /usr/lib64/libempty.so error: %s.\n", dlerror());
        exit(1);
    }
    handle = dlopen("/usr/lib64/libcfsc.so", RTLD_NOW|RTLD_GLOBAL);
    if(handle == NULL) {
        fprintf(stderr, "dlopen /usr/lib64/libcfsc.so error: %s\n", dlerror());
        exit(1);
    }
    init_cfsc_func(handle);
    int res = start_libs(NULL);
    if(res != 0) {
        fprintf(stderr, "start libs error.");
        exit(1);
    }

    pthread_rwlock_init(&update_rwlock, NULL);
    pthread_t thread;
    if(pthread_create(&thread, NULL, &update_dynamic_libs, handle)) {
        fprintf(stderr, "pthread_create update_dynamic_libs error.\n");
        exit(1);
    }
    g_inited = true;
}

static void init_cfsc_func(void *handle) {
    start_libs = (start_libs_t)dlsym(handle, "start_libs");
    stop_libs = (stop_libs_t)dlsym(handle, "stop_libs");
    flush_logs = (flush_logs_t)dlsym(handle, "flush_logs");

    real_openat = (openat_t)dlsym(handle, "real_openat");
    real_close = (close_t)dlsym(handle, "real_close");
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
    char* config_path = getenv("CFS_CONFIG_PATH");
    char *mount_point = getenv("CFS_MOUNT_POINT");

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
        void *client_state = stop_libs();
        if(client_state == NULL) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "stop libs error.");
            exit(1);
        }

        int res = dlclose(handle);
        if(res != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "dlclose error: %s\n", dlerror());
            exit(1);
        }
        fprintf(stderr, "finish dlclose client.\n");

        if (strcmp(reload, "test") == 0)
            sleep(CHECK_UPDATE_INTERVAL);

        setenv("LD_PRELOAD", ld_preload, 1);
        if(config_path != NULL) setenv("CFS_CONFIG_PATH", config_path, 1);
        if(mount_point != NULL) setenv("CFS_MOUNT_POINT", mount_point, 1);
        unsetenv("RELOAD_CLIENT");

        handle = dlopen("/usr/lib64/libcfsc.so", RTLD_NOW|RTLD_GLOBAL);
        if(res != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "dlopen /usr/lib64/libcfsc.so error: %s\n", dlerror());
            exit(1);
        }
        fprintf(stderr, "finish dlopen client.\n");

        init_cfsc_func(handle);
        res = start_libs(client_state);
        if(res != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            fprintf(stderr, "start libs error.");
            exit(1);
        }
        pthread_rwlock_unlock(&update_rwlock);
        fprintf(stderr, "finish update client.\n");
    }
}
