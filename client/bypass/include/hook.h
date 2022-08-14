#ifndef HOOK_H
#define HOOK_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <errno.h>
#include "libc_type.h"

static openat_t real_openat;
static close_t real_close;
static renameat2_t real_renameat2;
static truncate_t real_truncate;
static ftruncate_t real_ftruncate;
static fallocate_t real_fallocate;
static posix_fallocate_t real_posix_fallocate;

static chdir_t real_chdir;
static fchdir_t real_fchdir;
static getcwd_t real_getcwd;
static mkdirat_t real_mkdirat;
static rmdir_t real_rmdir;
static opendir_t real_opendir;
static fdopendir_t real_fdopendir;
static readdir_t real_readdir;
static closedir_t real_closedir;
static realpath_t real_realpath;

static linkat_t real_linkat;
static symlinkat_t real_symlinkat;
static unlinkat_t real_unlinkat;
static readlinkat_t real_readlinkat;

static stat_t real_stat;
static stat64_t real_stat64;
static lstat_t real_lstat;
static lstat64_t real_lstat64;
static fstat_t real_fstat;
static fstat64_t real_fstat64;
static fstatat_t real_fstatat;
static fstatat64_t real_fstatat64;
static fchmod_t real_fchmod;
static fchmodat_t real_fchmodat;
static lchown_t real_lchown;
static fchown_t real_fchown;
static fchownat_t real_fchownat;
static utime_t real_utime;
static utimes_t real_utimes;
static futimesat_t real_futimesat;
static utimensat_t real_utimensat;
static futimens_t real_futimens;
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

static fdatasync_t real_fdatasync;
static fsync_t real_fsync;
//static sigaction_t real_sigaction;

typedef int (*start_libs_t)(void *client_state);
typedef void* (*stop_libs_t)();
typedef void (*flush_logs_t)();

static start_libs_t start_libs;
static stop_libs_t stop_libs;
static flush_logs_t flush_logs;

const int CHECK_UPDATE_INTERVAL = 10;
pthread_rwlock_t update_rwlock;
static bool g_inited;
static void init();
static void init_cfsc_func(void *);
static void *update_dynamic_libs(void *);
static void *base_open(const char* name) {return dlopen(name, RTLD_NOW|RTLD_GLOBAL);}

#endif
