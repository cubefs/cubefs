#ifndef LIBC_OPERATION_H
#define LIBC_OPERATION_H

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
#include "libc_type.h"

static int libc_openat(int dirfd, const char *pathname, int flags, ...) {
    mode_t mode = 0;
    if(flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    openat_t func_openat = (openat_t)dlsym(RTLD_NEXT, "openat");
    return func_openat(dirfd, pathname, flags, mode);
}

static int libc_close(int fd) {
    close_t func_close = (close_t)dlsym(RTLD_NEXT, "close");
    return func_close(fd);
}

static int libc_renameat(int olddirfd, const char *oldpath,
                    int newdirfd, const char *newpath) {
    renameat_t func_renameat = (renameat_t)dlsym(RTLD_NEXT, "renameat");
    return func_renameat(olddirfd, oldpath, newdirfd, newpath);
}

static int libc_renameat2(int olddirfd, const char *old_pathname,
       int newdirfd, const char *new_pathname, unsigned int flags) {
    renameat2_t func_renameat2 = (renameat2_t)dlsym(RTLD_NEXT, "renameat2");
    return func_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
}

static int libc_truncate(const char *path, off_t length) {
    truncate_t func_truncate = (truncate_t)dlsym(RTLD_NEXT, "truncate");
    return func_truncate(path, length);
}

static int libc_ftruncate(int fd, off_t length) {
    ftruncate_t func_ftruncate = (ftruncate_t)dlsym(RTLD_NEXT, "ftruncate");
    return func_ftruncate(fd, length);
}

static int libc_fallocate(int fd, int mode, off_t offset, off_t len) {
    fallocate_t func_fallocate = (fallocate_t)dlsym(RTLD_NEXT, "fallocate");
    return func_fallocate(fd, mode, offset, len);
}

static int libc_posix_fallocate(int fd, off_t offset, off_t len) {
    posix_fallocate_t func_posix_fallocate = (posix_fallocate_t)dlsym(RTLD_NEXT, "posix_fallocate");
    return func_posix_fallocate(fd, offset, len);
}

static int libc_chdir(const char *path) {
    chdir_t func_chdir = (chdir_t)dlsym(RTLD_NEXT, "chdir");
    return func_chdir(path);
}

static int libc_fchdir(int fd) {
    fchdir_t func_fchdir = (fchdir_t)dlsym(RTLD_NEXT, "fchdir");
    return func_fchdir(fd);
}

static char *libc_getcwd(char *buf, size_t size) {
    getcwd_t func_getcwd = (getcwd_t)dlsym(RTLD_NEXT, "getcwd");
    return func_getcwd(buf, size);
}

static int libc_mkdirat(int dirfd, const char *pathname, mode_t mode) {
    mkdirat_t func_mkdirat = (mkdirat_t)dlsym(RTLD_NEXT, "mkdirat");
    return func_mkdirat(dirfd, pathname, mode);
}

static int libc_rmdir(const char *pathname) {
    rmdir_t func_rmdir = (rmdir_t)dlsym(RTLD_NEXT, "rmdir");
    return func_rmdir(pathname);
}

static DIR *libc_opendir(const char *name) {
    opendir_t func_opendir = (opendir_t)dlsym(RTLD_NEXT, "opendir");
    return func_opendir(name);
}

static DIR *libc_fdopendir(int fd) {
    fdopendir_t func_fdopendir = (fdopendir_t)dlsym(RTLD_NEXT, "fdopendir");
    return func_fdopendir(fd);
}


static struct dirent *libc_readdir(DIR *dirp) {
   readdir_t func_readdir = (readdir_t)dlsym(RTLD_NEXT, "readdir");
   return func_readdir(dirp);
}

static int libc_closedir(DIR *dirp) {
    closedir_t func_closedir = (closedir_t)dlsym(RTLD_NEXT, "closedir");
    return func_closedir(dirp);
}

static char *libc_realpath(const char *path, char *resolved_path) {
    realpath_t func_realpath = (realpath_t)dlsym(RTLD_NEXT, "realpath");
    return func_realpath(path, resolved_path);
}

static int libc_linkat(int olddirfd, const char *oldpath,
                  int newdirfd, const char *newpath, int flags) {
    linkat_t func_linkat = (linkat_t)dlsym(RTLD_NEXT, "linkat");
    return func_linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}

static int libc_symlinkat(const char *oldpath, int newdirfd, const char *newpath) {
    symlinkat_t func_symlinkat = (symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");
    return func_symlinkat(oldpath, newdirfd, newpath);
}

static int libc_unlinkat(int dirfd, const char *pathname, int flags) {
    unlinkat_t func_unlinkat = (unlinkat_t)dlsym(RTLD_NEXT, "unlinkat");
    return func_unlinkat(dirfd, pathname, flags);
}

static int libc_readlinkat(int dirfd, const char *pathname, char *buf, size_t bufsiz) {
    readlinkat_t func_readlinkat = (readlinkat_t)dlsym(RTLD_NEXT, "readlinkat");
    return func_readlinkat(dirfd, pathname, buf, bufsiz);
}

static int libc_stat(int ver, const char *pathname, struct stat *statbuf) {
    stat_t func_stat = (stat_t)dlsym(RTLD_NEXT, "__xstat");
    return func_stat(ver, pathname, statbuf);
}

static int libc_stat64(int ver, const char *pathname, struct stat64 *statbuf) {
    stat64_t func_stat64 = (stat64_t)dlsym(RTLD_NEXT, "__xstat64");
    return func_stat64(ver, pathname, statbuf);
}

static int libc_lstat(int ver, const char *pathname, struct stat *statbuf) {
    lstat_t func_lstat = (lstat_t)dlsym(RTLD_NEXT, "__lxstat");
    return func_lstat(ver, pathname, statbuf);
}

static int libc_lstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    lstat64_t func_lstat64 = (lstat64_t)dlsym(RTLD_NEXT, "__lxstat64");
    return func_lstat64(ver, pathname, statbuf);
}

static int libc_fstat(int ver, int fd, struct stat *statbuf) {
    fstat_t func_fstat = (fstat_t)dlsym(RTLD_NEXT, "__fxstat");
    return func_fstat(ver, fd, statbuf);
}

static int libc_fstat64(int ver, int fd, struct stat64 *statbuf) {
    fstat64_t func_fstat64 = (fstat64_t)dlsym(RTLD_NEXT, "__fxstat64");
    return func_fstat64(ver, fd, statbuf);
}

static int libc_fstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    fstatat_t func_fstatat = (fstatat_t)dlsym(RTLD_NEXT, "__fxstatat");
    return func_fstatat(ver, dirfd, pathname, statbuf, flags);
}

static int libc_fstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    fstatat64_t func_fstatat64 = (fstatat64_t)dlsym(RTLD_NEXT, "__fxstatat64");
    return func_fstatat64(ver, dirfd, pathname, statbuf, flags);
}

static int libc_fchmod(int fd, mode_t mode) {
    fchmod_t func_fchmod = (fchmod_t)dlsym(RTLD_NEXT, "fchmod");
    return func_fchmod(fd, mode);
}

static int libc_fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    fchmodat_t func_fchmodat = (fchmodat_t)dlsym(RTLD_NEXT, "fchmodat");
    return func_fchmodat(dirfd, pathname, mode, flags);
}

static int libc_lchown(const char *path, uid_t owner, gid_t group) {
    lchown_t func_lchown = (lchown_t)dlsym(RTLD_NEXT, "lchown");
    return func_lchown(path, owner, group);
}

static int libc_fchown(int fd, uid_t owner, gid_t group) {
    fchown_t func_fchown = (fchown_t)dlsym(RTLD_NEXT, "fchown");
    return func_fchown(fd, owner, group);
}

static int libc_fchownat(int dirfd, const char *pathname,
                    uid_t owner, gid_t group, int flags) {
    fchownat_t func_fchownat = (fchownat_t)dlsym(RTLD_NEXT, "fchownat");
    return func_fchownat(dirfd, pathname, owner, group, flags);
}

static int libc_utime(const char *filename, const struct utimbuf *times) {
    utime_t func_utime = (utime_t)dlsym(RTLD_NEXT, "utime");
    return func_utime(filename, times);
}

static int libc_utimes(const char *filename, const struct timeval times[2]) {
    utimes_t func_utimes = (utimes_t)dlsym(RTLD_NEXT, "utimes");
    return func_utimes(filename, times);
}

static int libc_futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    futimesat_t func_futimesat = (futimesat_t)dlsym(RTLD_NEXT, "futimesat");
    return func_futimesat(dirfd, pathname, times);
}

static int libc_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    utimensat_t func_utimensat = (utimensat_t)dlsym(RTLD_NEXT, "utimensat");
    return func_utimensat(dirfd, pathname, times, flags);
}

static int libc_futimens(int fd, const struct timespec times[2]) {
    futimens_t func_futimens = (futimens_t)dlsym(RTLD_NEXT, "futimens");
    return func_futimens(fd, times);
}

static int libc_access(const char *pathname, int mode) {
    access_t func_access = (access_t)dlsym(RTLD_NEXT, "access");
    return func_access(pathname, mode);
}

static int libc_faccessat(int dirfd, const char *pathname, int mode, int flags) {
    faccessat_t func_faccessat = (faccessat_t)dlsym(RTLD_NEXT, "faccessat");
    return func_faccessat(dirfd, pathname, mode, flags);
}

static int libc_setxattr(const char *pathname, const char *name, const void *value, size_t size, int flags) {
    setxattr_t func_setxattr = (setxattr_t)dlsym(RTLD_NEXT, "setxattr");
    return func_setxattr(pathname, name, value, size, flags);
}

static int libc_lsetxattr(const char *pathname, const char *name, const void *value, size_t size, int flags) {
    lsetxattr_t func_lsetxattr = (lsetxattr_t)dlsym(RTLD_NEXT, "lsetxattr");
    return func_lsetxattr(pathname, name, value, size, flags);
}

static int libc_fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    fsetxattr_t func_fsetxattr = (fsetxattr_t)dlsym(RTLD_NEXT, "fsetxattr");
    return func_fsetxattr(fd, name, value, size, flags);
}

static ssize_t libc_getxattr(const char *pathname, const char *name, void *value, size_t size) {
    getxattr_t func_getxattr = (getxattr_t)dlsym(RTLD_NEXT, "getxattr");
    return func_getxattr(pathname, name, value, size);
}

static ssize_t libc_lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    lgetxattr_t func_lgetxattr = (lgetxattr_t)dlsym(RTLD_NEXT, "lgetxattr");
    return func_lgetxattr(pathname, name, value, size);
}

static ssize_t libc_fgetxattr(int fd, const char *name, void *value, size_t size) {
    fgetxattr_t func_fgetxattr = (fgetxattr_t)dlsym(RTLD_NEXT, "fgetxattr");
    return func_fgetxattr(fd, name, value, size);
}

static ssize_t libc_listxattr(const char *pathname, char *list, size_t size) {
    listxattr_t func_listxattr = (listxattr_t)dlsym(RTLD_NEXT, "listxattr");
    return func_listxattr(pathname, list, size);
}

static ssize_t libc_llistxattr(const char *pathname, char *list, size_t size) {
    llistxattr_t func_llistxattr = (llistxattr_t)dlsym(RTLD_NEXT, "llistxattr");
    return func_llistxattr(pathname, list, size);
}

static ssize_t libc_flistxattr(int fd, char *list, size_t size) {
    flistxattr_t func_flistxattr = (flistxattr_t)dlsym(RTLD_NEXT, "flistxattr");
    return func_flistxattr(fd, list, size);
}

static int libc_removexattr(const char *pathname, const char *name) {
    removexattr_t func_removexattr = (removexattr_t)dlsym(RTLD_NEXT, "removexattr");
    return func_removexattr(pathname, name);
}

static int libc_lremovexattr(const char *pathname, const char *name) {
    lremovexattr_t func_lremovexattr = (lremovexattr_t)dlsym(RTLD_NEXT, "lremovexattr");
    return func_lremovexattr(pathname, name);
}

static int libc_fremovexattr(int fd, const char *name) {
    fremovexattr_t func_fremovexattr = (fremovexattr_t)dlsym(RTLD_NEXT, "fremovexattr");
    return func_fremovexattr(fd, name);
}

static int libc_fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    void *arg = va_arg(args, void *);
    va_end(args);
    fcntl_t func_fcntl = (fcntl_t)dlsym(RTLD_NEXT, "fcntl");
    return func_fcntl(fd, cmd, arg);
}

static int libc_dup2(int oldfd, int newfd) {
    dup2_t func_dup2 = (dup2_t)dlsym(RTLD_NEXT, "dup2");
    return func_dup2(oldfd, newfd);
}

static int libc_dup3(int oldfd, int newfd, int flags) {
    dup3_t func_dup3 = (dup3_t)dlsym(RTLD_NEXT, "dup3");
    return func_dup3(oldfd, newfd, flags);
}

static ssize_t libc_read(int fd, void *buf, size_t count) {
    read_t func_read = (read_t)dlsym(RTLD_NEXT, "read");
    return func_read(fd, buf, count);
}

static ssize_t libc_readv(int fd, const struct iovec *iov, int iovcnt) {
    readv_t func_readv = (readv_t)dlsym(RTLD_NEXT, "readv");
    return func_readv(fd, iov, iovcnt);
}

static ssize_t libc_pread(int fd, void *buf, size_t count, off_t offset) {
    pread_t func_pread = (pread_t)dlsym(RTLD_NEXT, "pread");
    return func_pread(fd, buf, count, offset);
}

static ssize_t libc_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    preadv_t func_preadv = (preadv_t)dlsym(RTLD_NEXT, "preadv");
    return func_preadv(fd, iov, iovcnt, offset);
}

static ssize_t libc_write(int fd, const void *buf, size_t count) {
    write_t func_write = (write_t)dlsym(RTLD_NEXT, "write");
    return func_write(fd, buf, count);
}

static ssize_t libc_writev(int fd, const struct iovec *iov, int iovcnt) {
    writev_t func_writev = (writev_t)dlsym(RTLD_NEXT, "writev");
    return func_writev(fd, iov, iovcnt);
}

static ssize_t libc_pwrite(int fd, const void *buf, size_t count, off_t offset) {
    pwrite_t func_pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    return func_pwrite(fd, buf, count, offset);
}

static ssize_t libc_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    pwritev_t func_pwritev = (pwritev_t)dlsym(RTLD_NEXT, "pwritev");
    return func_pwritev(fd, iov, iovcnt, offset);
}

static off_t libc_lseek(int fd, off_t offset, int whence) {
    lseek_t func_lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
    return func_lseek(fd, offset, whence);
}

static int libc_fdatasync(int fd) {
    fdatasync_t func_fdatasync = (fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
    return func_fdatasync(fd);
}

static int libc_fsync(int fd) {
    fsync_t func_fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
    return func_fsync(fd);
}

#endif
