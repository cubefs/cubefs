#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <utime.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include "libc_operation.h"

static openat_t func_openat;
static close_t func_close;
static renameat_t func_renameat;
static renameat2_t func_renameat2;
static truncate_t func_truncate;
static ftruncate_t func_ftruncate;
static fallocate_t func_fallocate;
static posix_fallocate_t func_posix_fallocate;

static chdir_t func_chdir;
static fchdir_t func_fchdir;
static getcwd_t func_getcwd;
static mkdirat_t func_mkdirat;
static rmdir_t func_rmdir;
static opendir_t func_opendir;
static fdopendir_t func_fdopendir;
static readdir_t func_readdir;
static closedir_t func_closedir;
static realpath_t func_realpath;

static linkat_t func_linkat;
static symlinkat_t func_symlinkat;
static unlinkat_t func_unlinkat;
static readlinkat_t func_readlinkat;

static stat_t func_stat;
static stat64_t func_stat64;
static lstat_t func_lstat;
static lstat64_t func_lstat64;
static fstat_t func_fstat;
static fstat64_t func_fstat64;
static fstatat_t func_fstatat;
static fstatat64_t func_fstatat64;
static fchmod_t func_fchmod;
static fchmodat_t func_fchmodat;
static lchown_t func_lchown;
static fchown_t func_fchown;
static fchownat_t func_fchownat;
static utime_t func_utime;
static utimes_t func_utimes;
static futimesat_t func_futimesat;
static utimensat_t func_utimensat;
static futimens_t func_futimens;
static access_t func_access;
static faccessat_t func_faccessat;

static setxattr_t func_setxattr;
static lsetxattr_t func_lsetxattr;
static fsetxattr_t func_fsetxattr;
static getxattr_t func_getxattr;
static lgetxattr_t func_lgetxattr;
static fgetxattr_t func_fgetxattr;
static listxattr_t func_listxattr;
static llistxattr_t func_llistxattr;
static flistxattr_t func_flistxattr;
static removexattr_t func_removexattr;
static lremovexattr_t func_lremovexattr;
static fremovexattr_t func_fremovexattr;

static fcntl_t func_fcntl;
static dup2_t func_dup2;
static dup3_t func_dup3;

static read_t func_read;
static readv_t func_readv;
static pread_t func_pread;
static preadv_t func_preadv;
static write_t func_write;
static writev_t func_writev;
static pwrite_t func_pwrite;
static pwritev_t func_pwritev;
static lseek_t func_lseek;

static fdatasync_t func_fdatasync;
static fsync_t func_fsync;

static abort_t func_abort;
static _exit_t func__exit;
static exit_t func_exit;

int libc_openat(int dirfd, const char *pathname, int flags, ...) {
    mode_t mode = 0;
    if(flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    if(func_openat == NULL) {
        func_openat = (openat_t)dlsym(RTLD_NEXT, "openat");
    }
    return func_openat(dirfd, pathname, flags, mode);
}

int libc_close(int fd) {
    if(func_close == NULL) {
        func_close = (close_t)dlsym(RTLD_NEXT, "close");
    }
    return func_close(fd);
}

int libc_renameat(int olddirfd, const char *oldpath,
                    int newdirfd, const char *newpath) {
    if(func_renameat == NULL) {
        func_renameat = (renameat_t)dlsym(RTLD_NEXT, "renameat");
    }
    return func_renameat(olddirfd, oldpath, newdirfd, newpath);
}

int libc_renameat2(int olddirfd, const char *old_pathname,
       int newdirfd, const char *new_pathname, unsigned int flags) {
    if(func_renameat2 == NULL) {
        func_renameat2 = (renameat2_t)dlsym(RTLD_NEXT, "renameat2");
    }
    return func_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
}

int libc_truncate(const char *path, off_t length) {
    if(func_truncate == NULL) {
        func_truncate = (truncate_t)dlsym(RTLD_NEXT, "truncate");
    }
    return func_truncate(path, length);
}

int libc_ftruncate(int fd, off_t length) {
    if(func_ftruncate == NULL) {
        func_ftruncate = (ftruncate_t)dlsym(RTLD_NEXT, "ftruncate");
    }
    return func_ftruncate(fd, length);
}

int libc_fallocate(int fd, int mode, off_t offset, off_t len) {
    if(func_fallocate == NULL) {
        func_fallocate = (fallocate_t)dlsym(RTLD_NEXT, "fallocate");
    }
    return func_fallocate(fd, mode, offset, len);
}

int libc_posix_fallocate(int fd, off_t offset, off_t len) {
    if(func_posix_fallocate == NULL) {
        func_posix_fallocate = (posix_fallocate_t)dlsym(RTLD_NEXT, "posix_fallocate");
    }
    return func_posix_fallocate(fd, offset, len);
}

int libc_chdir(const char *path) {
    if(func_chdir == NULL) {
        func_chdir = (chdir_t)dlsym(RTLD_NEXT, "chdir");
    }
    return func_chdir(path);
}

int libc_fchdir(int fd) {
    if(func_fchdir == NULL) {
        func_fchdir = (fchdir_t)dlsym(RTLD_NEXT, "fchdir");
    }
    return func_fchdir(fd);
}

char *libc_getcwd(char *buf, size_t size) {
    if(func_getcwd == NULL) {
        func_getcwd = (getcwd_t)dlsym(RTLD_NEXT, "getcwd");
    }
    return func_getcwd(buf, size);
}

int libc_mkdirat(int dirfd, const char *pathname, mode_t mode) {
    if(func_mkdirat == NULL) {
        func_mkdirat = (mkdirat_t)dlsym(RTLD_NEXT, "mkdirat");
    }
    return func_mkdirat(dirfd, pathname, mode);
}

int libc_rmdir(const char *pathname) {
    if(func_rmdir == NULL) {
        func_rmdir = (rmdir_t)dlsym(RTLD_NEXT, "rmdir");
    }
    return func_rmdir(pathname);
}

DIR *libc_opendir(const char *name) {
    if(func_opendir == NULL) {
        func_opendir = (opendir_t)dlsym(RTLD_NEXT, "opendir");
    }
    return func_opendir(name);
}

DIR *libc_fdopendir(int fd) {
    if(func_fdopendir == NULL) {
        func_fdopendir = (fdopendir_t)dlsym(RTLD_NEXT, "fdopendir");
    }
    return func_fdopendir(fd);
}


struct dirent *libc_readdir(DIR *dirp) {
    if(func_readdir == NULL) {
        func_readdir = (readdir_t)dlsym(RTLD_NEXT, "readdir");
    }
    return func_readdir(dirp);
}

int libc_closedir(DIR *dirp) {
    if(func_closedir == NULL) {
        func_closedir = (closedir_t)dlsym(RTLD_NEXT, "closedir");
    }
    return func_closedir(dirp);
}

char *libc_realpath(const char *path, char *resolved_path) {
    if(func_realpath == NULL) {
        func_realpath = (realpath_t)dlsym(RTLD_NEXT, "realpath");
    }
    return func_realpath(path, resolved_path);
}

int libc_linkat(int olddirfd, const char *oldpath,
                  int newdirfd, const char *newpath, int flags) {
    if(func_linkat == NULL) {
        func_linkat = (linkat_t)dlsym(RTLD_NEXT, "linkat");
    }
    return func_linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}

int libc_symlinkat(const char *oldpath, int newdirfd, const char *newpath) {
    if(func_symlinkat == NULL) {
        func_symlinkat = (symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");
    }
    return func_symlinkat(oldpath, newdirfd, newpath);
}

int libc_unlinkat(int dirfd, const char *pathname, int flags) {
    if(func_unlinkat == NULL) {
        func_unlinkat = (unlinkat_t)dlsym(RTLD_NEXT, "unlinkat");
    }
    return func_unlinkat(dirfd, pathname, flags);
}

int libc_readlinkat(int dirfd, const char *pathname, char *buf, size_t bufsiz) {
    if(func_readlinkat == NULL) {
        func_readlinkat = (readlinkat_t)dlsym(RTLD_NEXT, "readlinkat");
    }
    return func_readlinkat(dirfd, pathname, buf, bufsiz);
}

int libc_stat(int ver, const char *pathname, struct stat *statbuf) {
    if(func_stat == NULL) {
        func_stat = (stat_t)dlsym(RTLD_NEXT, "__xstat");
    }
    return func_stat(ver, pathname, statbuf);
}

int libc_stat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(func_stat64 == NULL) {
        func_stat64 = (stat64_t)dlsym(RTLD_NEXT, "__xstat64");
    }
    return func_stat64(ver, pathname, statbuf);
}

int libc_lstat(int ver, const char *pathname, struct stat *statbuf) {
    if(func_lstat == NULL) {
        func_lstat = (lstat_t)dlsym(RTLD_NEXT, "__lxstat");
    }
    return func_lstat(ver, pathname, statbuf);
}

int libc_lstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(func_lstat64 == NULL) {
        func_lstat64 = (lstat64_t)dlsym(RTLD_NEXT, "__lxstat64");
    }
    return func_lstat64(ver, pathname, statbuf);
}

int libc_fstat(int ver, int fd, struct stat *statbuf) {
    if(func_fstat == NULL) {
        func_fstat = (fstat_t)dlsym(RTLD_NEXT, "__fxstat");
    }
    return func_fstat(ver, fd, statbuf);
}

int libc_fstat64(int ver, int fd, struct stat64 *statbuf) {
    if(func_fstat64 == NULL) {
        func_fstat64 = (fstat64_t)dlsym(RTLD_NEXT, "__fxstat64");
    }
    return func_fstat64(ver, fd, statbuf);
}

int libc_fstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    if(func_fstatat == NULL) {
        func_fstatat = (fstatat_t)dlsym(RTLD_NEXT, "__fxstatat");
    }
    return func_fstatat(ver, dirfd, pathname, statbuf, flags);
}

int libc_fstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    if(func_fstatat64 == NULL) {
        func_fstatat64 = (fstatat64_t)dlsym(RTLD_NEXT, "__fxstatat64");
    }
    return func_fstatat64(ver, dirfd, pathname, statbuf, flags);
}

int libc_fchmod(int fd, mode_t mode) {
    if(func_fchmod == NULL) {
        func_fchmod = (fchmod_t)dlsym(RTLD_NEXT, "fchmod");
    }
    return func_fchmod(fd, mode);
}

int libc_fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    if(func_fchmodat == NULL) {
        func_fchmodat = (fchmodat_t)dlsym(RTLD_NEXT, "fchmodat");
    }
    return func_fchmodat(dirfd, pathname, mode, flags);
}

int libc_lchown(const char *path, uid_t owner, gid_t group) {
    if(func_lchown == NULL) {
        func_lchown = (lchown_t)dlsym(RTLD_NEXT, "lchown");
    }
    return func_lchown(path, owner, group);
}

int libc_fchown(int fd, uid_t owner, gid_t group) {
    if(func_fchown == NULL) {
        func_fchown = (fchown_t)dlsym(RTLD_NEXT, "fchown");
    }
    return func_fchown(fd, owner, group);
}

int libc_fchownat(int dirfd, const char *pathname,
                    uid_t owner, gid_t group, int flags) {
    if(func_fchownat == NULL) {
        func_fchownat = (fchownat_t)dlsym(RTLD_NEXT, "fchownat");
    }
    return func_fchownat(dirfd, pathname, owner, group, flags);
}

int libc_utime(const char *filename, const struct utimbuf *times) {
    if(func_utime == NULL) {
        func_utime = (utime_t)dlsym(RTLD_NEXT, "utime");
    }
    return func_utime(filename, times);
}

int libc_utimes(const char *filename, const struct timeval times[2]) {
    if(func_utimes == NULL) {
        func_utimes = (utimes_t)dlsym(RTLD_NEXT, "utimes");
    }
    return func_utimes(filename, times);
}

int libc_futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    if(func_futimesat == NULL) {
        func_futimesat = (futimesat_t)dlsym(RTLD_NEXT, "futimesat");
    }
    return func_futimesat(dirfd, pathname, times);
}

int libc_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    if(func_utimensat == NULL) {
        func_utimensat = (utimensat_t)dlsym(RTLD_NEXT, "utimensat");
    }
    return func_utimensat(dirfd, pathname, times, flags);
}

int libc_futimens(int fd, const struct timespec times[2]) {
    if(func_futimens == NULL) {
        func_futimens = (futimens_t)dlsym(RTLD_NEXT, "futimens");
    }
    return func_futimens(fd, times);
}

int libc_access(const char *pathname, int mode) {
    if(func_access == NULL) {
        func_access = (access_t)dlsym(RTLD_NEXT, "access");
    }
    return func_access(pathname, mode);
}

int libc_faccessat(int dirfd, const char *pathname, int mode, int flags) {
    if(func_faccessat == NULL) {
        func_faccessat = (faccessat_t)dlsym(RTLD_NEXT, "faccessat");
    }
    return func_faccessat(dirfd, pathname, mode, flags);
}

int libc_setxattr(const char *pathname, const char *name, const void *value, size_t size, int flags) {
    if(func_setxattr == NULL) {
        func_setxattr = (setxattr_t)dlsym(RTLD_NEXT, "setxattr");
    }
    return func_setxattr(pathname, name, value, size, flags);
}

int libc_lsetxattr(const char *pathname, const char *name, const void *value, size_t size, int flags) {
    if(func_lsetxattr) {
        func_lsetxattr = (lsetxattr_t)dlsym(RTLD_NEXT, "lsetxattr");
    }
    return func_lsetxattr(pathname, name, value, size, flags);
}

int libc_fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    if(func_fsetxattr == NULL) {
        func_fsetxattr = (fsetxattr_t)dlsym(RTLD_NEXT, "fsetxattr");
    }
    return func_fsetxattr(fd, name, value, size, flags);
}

ssize_t libc_getxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(func_getxattr == NULL) {
        func_getxattr = (getxattr_t)dlsym(RTLD_NEXT, "getxattr");
    }
    return func_getxattr(pathname, name, value, size);
}

ssize_t libc_lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(func_lgetxattr == NULL) {
        func_lgetxattr = (lgetxattr_t)dlsym(RTLD_NEXT, "lgetxattr");
    }
    return func_lgetxattr(pathname, name, value, size);
}

ssize_t libc_fgetxattr(int fd, const char *name, void *value, size_t size) {
    if(func_fgetxattr == NULL) {
        func_fgetxattr = (fgetxattr_t)dlsym(RTLD_NEXT, "fgetxattr");
    }
    return func_fgetxattr(fd, name, value, size);
}

ssize_t libc_listxattr(const char *pathname, char *list, size_t size) {
    if(func_listxattr == NULL) {
        func_listxattr = (listxattr_t)dlsym(RTLD_NEXT, "listxattr");
    }
    return func_listxattr(pathname, list, size);
}

ssize_t libc_llistxattr(const char *pathname, char *list, size_t size) {
    if(func_llistxattr == NULL) {
        func_llistxattr = (llistxattr_t)dlsym(RTLD_NEXT, "llistxattr");
    }
    return func_llistxattr(pathname, list, size);
}

ssize_t libc_flistxattr(int fd, char *list, size_t size) {
    if(func_flistxattr == NULL) {
        func_flistxattr = (flistxattr_t)dlsym(RTLD_NEXT, "flistxattr");
    }
    return func_flistxattr(fd, list, size);
}

int libc_removexattr(const char *pathname, const char *name) {
    if(func_removexattr == NULL) {
        func_removexattr = (removexattr_t)dlsym(RTLD_NEXT, "removexattr");
    }
    return func_removexattr(pathname, name);
}

int libc_lremovexattr(const char *pathname, const char *name) {
    if(func_lremovexattr == NULL) {
        func_lremovexattr = (lremovexattr_t)dlsym(RTLD_NEXT, "lremovexattr");
    }
    return func_lremovexattr(pathname, name);
}

int libc_fremovexattr(int fd, const char *name) {
    if(func_fremovexattr == NULL) {
        func_fremovexattr = (fremovexattr_t)dlsym(RTLD_NEXT, "fremovexattr");
    }
    return func_fremovexattr(fd, name);
}

int libc_fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    void *arg = va_arg(args, void *);
    va_end(args);

    if(func_fcntl == NULL) {
        func_fcntl = (fcntl_t)dlsym(RTLD_NEXT, "fcntl");
    }
    return func_fcntl(fd, cmd, arg);
}

int libc_dup2(int oldfd, int newfd) {
    if(func_dup2 == NULL) {
        func_dup2 = (dup2_t)dlsym(RTLD_NEXT, "dup2");
    }
    return func_dup2(oldfd, newfd);
}

int libc_dup3(int oldfd, int newfd, int flags) {
    if(func_dup3 == NULL) {
        func_dup3 = (dup3_t)dlsym(RTLD_NEXT, "dup3");
    }
    return func_dup3(oldfd, newfd, flags);
}

ssize_t libc_read(int fd, void *buf, size_t count) {
    if(func_read == NULL) {
        func_read = (read_t)dlsym(RTLD_NEXT, "read");
    }
    return func_read(fd, buf, count);
}

ssize_t libc_readv(int fd, const struct iovec *iov, int iovcnt) {
    if(func_readv == NULL) {
        func_readv = (readv_t)dlsym(RTLD_NEXT, "readv");
    }
    return func_readv(fd, iov, iovcnt);
}

ssize_t libc_pread(int fd, void *buf, size_t count, off_t offset) {
    if(func_pread == NULL) {
        func_pread = (pread_t)dlsym(RTLD_NEXT, "pread");
    }
    return func_pread(fd, buf, count, offset);
}

ssize_t libc_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(func_preadv == NULL) {
        func_preadv = (preadv_t)dlsym(RTLD_NEXT, "preadv");
    }
    return func_preadv(fd, iov, iovcnt, offset);
}

ssize_t libc_write(int fd, const void *buf, size_t count) {
    if(func_write == NULL) {
        func_write = (write_t)dlsym(RTLD_NEXT, "write");
    }
    return func_write(fd, buf, count);
}

ssize_t libc_writev(int fd, const struct iovec *iov, int iovcnt) {
    if(func_writev == NULL) {
        func_writev = (writev_t)dlsym(RTLD_NEXT, "writev");
    }
    return func_writev(fd, iov, iovcnt);
}

ssize_t libc_pwrite(int fd, const void *buf, size_t count, off_t offset) {
    if(func_pwrite == NULL) {
        func_pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    }
    return func_pwrite(fd, buf, count, offset);
}

ssize_t libc_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(func_pwritev == NULL) {
        func_pwritev = (pwritev_t)dlsym(RTLD_NEXT, "pwritev");
    }
    return func_pwritev(fd, iov, iovcnt, offset);
}

off_t libc_lseek(int fd, off_t offset, int whence) {
    if(func_lseek == NULL) {
        func_lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
    }
    return func_lseek(fd, offset, whence);
}

int libc_fdatasync(int fd) {
    if(func_fdatasync == NULL) {
        func_fdatasync = (fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
    }
    return func_fdatasync(fd);
}

int libc_fsync(int fd) {
    if(func_fsync == NULL) {
        func_fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
    }
    return func_fsync(fd);
}

void libc_abort() {
    if(func_abort == NULL) {
        func_abort = (abort_t)dlsym(RTLD_NEXT, "abort");
    }
    func_abort();
}

void libc__exit(int status) {
    if(func__exit == NULL) {
        func__exit = (_exit_t)dlsym(RTLD_NEXT, "_exit");
    }
    func__exit(status);
}

void libc_exit(int status) {
    if(func_exit == NULL) {
        func_exit = (exit_t)dlsym(RTLD_NEXT, "exit");
    }
    func_exit(status);
}
