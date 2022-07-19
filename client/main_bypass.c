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

#include "client.h"

#define WRAPPER(cmd, res)                          \
    g_need_rwlock? (                               \
            pthread_rwlock_rdlock(&update_rwlock), \
            res = cmd,                             \
            pthread_rwlock_unlock(&update_rwlock), \
            res): (res = cmd)

#define WRAPPER_IGNORE_RES(cmd)                    \
    g_need_rwlock? (                               \
            pthread_rwlock_rdlock(&update_rwlock), \
            cmd,                                   \
            pthread_rwlock_unlock(&update_rwlock)): cmd

/*
 * File operations
 */

ssize_t cfs_pwrite_with_lock(int64_t id, int fd, const void *buf, size_t count, off_t offset) {
    ssize_t res;
    if (g_need_rwlock) {
        pthread_rwlock_rdlock(&update_rwlock);
        res = cfs_pwrite(id, fd, buf, count, offset);
        pthread_rwlock_unlock(&update_rwlock);
    } else
        res = cfs_pwrite(id, fd, buf, count, offset);
    return res;
}

int close(int fd) {
    if(!g_cfs_inited) {
        libc_close = (close_t)dlsym(RTLD_NEXT, "close");
        return libc_close(fd);
    }

    if(fd < 0) {
        return -1;
    }
    int re = -1;
    int is_cfs = fd & CFS_FD_MASK;
    if(g_hook && is_cfs) {
        int in_use = 0;
        for(const auto &item : g_dup_origin) {
            if(item.second == fd) {
                in_use = 1;
            }
        }
        if(in_use) {
            goto log;
        }
        fd = fd & ~CFS_FD_MASK;
        #ifdef DUP_TO_LOCAL
        re = libc_close(fd);
        if(re < 0) {
            goto log;
        }
        #endif
        pthread_rwlock_wrlock(&g_open_file_lock);
        auto g_open_file_it = g_open_file.find(fd);
        if(g_open_file_it == g_open_file.end()) {
            pthread_rwlock_unlock(&g_open_file_lock);
            WRAPPER(cfs_errno(cfs_close(g_cfs_client_id, fd)), re);
            goto log;
        }
        cfs_file_t *f = g_open_file_it->second;
        g_open_file.erase(g_open_file_it);
        pthread_rwlock_unlock(&g_open_file_lock);
        flush_file(f);
        f->status = FILE_STATUS_CLOSED;

        pthread_rwlock_rdlock(&g_inode_open_file_lock);
        auto it = g_inode_open_file.find(f->inode);
        bool evict = true;
        for(const auto &tmpf : it->second) {
            if(tmpf->status == FILE_STATUS_OPEN) {
                evict = false;
            }
        }
        pthread_rwlock_unlock(&g_inode_open_file_lock);
        // if all files of the inode are closed, do resource cleaning
        if(evict) {
            // clear all pages of this file, to prevent possible wild pointer dereference
            clear_file(f);
            for(int i = 0; i < BLOCKS_PER_FILE; i++) {
                free(f->pages[i]);
            }
            free(f->pages);
            pthread_mutex_destroy(&f->lock);
            // defer closed file freeing to evict time, to prevent possible wild pointer dereference
            pthread_rwlock_wrlock(&g_inode_open_file_lock);
            for(const auto &tmpf : it->second) {
                free(tmpf);
            }
            WRAPPER(cfs_errno(cfs_close(g_cfs_client_id, fd & ~CFS_FD_MASK)), re);
            g_inode_open_file.erase(it);
            pthread_rwlock_unlock(&g_inode_open_file_lock);
        }
        WRAPPER(cfs_errno(cfs_close(g_cfs_client_id, fd)), re);
    } else {
        re = libc_close(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", re);
    if(it != g_fd_path.end()) {
        free(it->second);
        g_fd_path.erase(it);
    }
    #endif
    return re;
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

    if(!g_cfs_inited) {
        libc_openat = (openat_t)dlsym(RTLD_NEXT, "openat");
        return libc_openat(dirfd, pathname, flags, mode);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int fd;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        fd = libc_openat(dirfd, pathname, flags, mode);
        if(fd < 0) {
            goto log;
        }
        WRAPPER(cfs_re(cfs_openat_fd(g_cfs_client_id, dirfd, cfs_path, flags, mode, fd)), fd);
        #else
        /*
        if(strstr(cfs_path, ".ibd") || strstr(cfs_path, ".frm") || strstr(cfs_path, "ibdata1")) {
            is_cfs = 0;
            char fuse_path[256];
            sprintf(fuse_path, "/mnt/cfs%s", cfs_path);
            fd = libc_openat(dirfd, fuse_path, flags, mode);
            goto log;
        }
        */
        WRAPPER(cfs_errno(cfs_openat(g_cfs_client_id, dirfd, cfs_path, flags, mode)), fd);
        #endif
        if(fd < 0) {
            goto log;
        }
        pthread_rwlock_wrlock(&g_open_file_lock);
        auto g_open_file_it = g_open_file.find(fd);
        if(g_open_file_it != g_open_file.end()) {
            pthread_rwlock_unlock(&g_open_file_lock);
            goto log;
        }
        cfs_file_t *f = new_file(&cfs_pwrite_with_lock);
        WRAPPER_IGNORE_RES(cfs_get_file(g_cfs_client_id, fd, f));
        if(f->file_type != FILE_TYPE_RELAY_LOG && f->file_type != FILE_TYPE_BIN_LOG) {
            pthread_rwlock_unlock(&g_open_file_lock);
            free(f);
            goto log;
        }
        g_open_file[fd] = f;
        pthread_rwlock_unlock(&g_open_file_lock);

        if(f->file_type == FILE_TYPE_BIN_LOG || f->file_type == FILE_TYPE_RELAY_LOG) {
            f->c = g_big_page_cache;
        } else {
            f->c = g_small_page_cache;
        }
        f->cache_flag |= FILE_CACHE_WRITE_BACK;
        if(f->file_type == FILE_TYPE_RELAY_LOG) {
            f->cache_flag |= FILE_CACHE_PRIORITY_HIGH;
        }
        pthread_rwlock_wrlock(&g_inode_open_file_lock);
        auto it = g_inode_open_file.find(f->inode);
        if(it != g_inode_open_file.end() && !it->second.empty()) {
            // every open file of the same inode shares the same pages
            cfs_file_t *file = *it->second.begin();
            f->pages = file->pages;
            f->lock = file->lock;
        } else {
            init_file(f);
        }
        g_inode_open_file[f->inode].insert(f);
        pthread_rwlock_unlock(&g_inode_open_file_lock);
    } else {
        fd = libc_openat(dirfd, pathname, flags, mode);
    }

log:
    if(fd > 0 && fd & CFS_FD_MASK) {
        if(g_hook && is_cfs) {
            WRAPPER_IGNORE_RES(cfs_close(g_cfs_client_id, fd));
        } else {
            libc_close(fd);
        }
        fd = -1;
    }
    free(path);
    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    g_fd_path[fd] = strdup(pathname);
    #endif
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, dirfd:%d, pathname:%s, flags:%#x(%s%s%s%s%s%s%s), re:%d\n", 
    __func__, is_cfs > 0, dirfd, pathname, flags, flags&O_RDONLY?"O_RDONLY|":"", 
    flags&O_WRONLY?"O_WRONLY|":"", flags&O_RDWR?"O_RDWR|":"", flags&O_CREAT?"O_CREAT|":"", 
    flags&O_DIRECT?"O_DIRECT|":"", flags&O_SYNC?"O_SYNC|":"", flags&O_DSYNC?"O_DSYNC":"", fd);
    #endif
    if(g_hook && is_cfs && fd > 0) {
        fd |= CFS_FD_MASK;
    }
    return fd;
}
weak_alias (openat, openat64)

// rename between cfs and ordinary file is not allowed
int renameat2(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname, unsigned int flags) {
    if(!g_cfs_inited) {
        if(g_has_renameat2) {
            libc_renameat2 = (renameat2_t)dlsym(RTLD_NEXT, "renameat2");
            return libc_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            libc_renameat = (renameat_t)dlsym(RTLD_NEXT, "renameat");
            return libc_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
        }
    }

    int is_cfs_old = 0;
    char *old_path = NULL;
    if((old_pathname != NULL && old_pathname[0] == '/') || olddirfd == AT_FDCWD) {
        old_path = get_cfs_path(old_pathname);
        is_cfs_old = (old_path != NULL);
    } else {
        is_cfs_old = olddirfd & CFS_FD_MASK;
        olddirfd = olddirfd & ~CFS_FD_MASK;
    }

    int is_cfs_new = 0;
    char *new_path = NULL;
    if((new_pathname != NULL && new_pathname[0] == '/') || newdirfd == AT_FDCWD) {
        new_path = get_cfs_path(new_pathname);
        is_cfs_new = (new_path != NULL);
    } else {
        is_cfs_new = newdirfd & CFS_FD_MASK;
        newdirfd = newdirfd & ~CFS_FD_MASK;
    }

    const char *cfs_old_path = (old_path == NULL) ? old_pathname : old_path;
    const char *cfs_new_path = (new_path == NULL) ? new_pathname : new_path;
    int re = -1;
    if(g_hook && is_cfs_old && is_cfs_new) {
        if(flags & RENAME_NOREPLACE) {
            if(!WRAPPER(cfs_faccessat(g_cfs_client_id, newdirfd, cfs_new_path, F_OK, 0), re)) {
                errno = ENOTEMPTY;
                goto log;
            }
        } else if(flags) {
            // other flags unimplemented
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        if(g_has_renameat2) {
            re = libc_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            re = libc_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
        }
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_renameat(g_cfs_client_id, olddirfd, cfs_old_path, newdirfd, cfs_new_path)), re);
    } else if(!g_hook || (!is_cfs_old && !is_cfs_new)) {
        if(g_has_renameat2) {
            re = libc_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            re = libc_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
        }
    }

log:
    free(old_path);
    free(new_path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, olddirfd:%d, old_pathname:%s, is_cfs_old:%d, newdirfd:%d, new_pathname:%s, is_cfs_new:%d, flags:%#x, re:%d\n", __func__, olddirfd, old_pathname, is_cfs_old, newdirfd, new_pathname, is_cfs_new, flags, re);
    #endif
    return re;
}

int rename(const char *old_pathname, const char *new_pathname) {
    return renameat2(AT_FDCWD, old_pathname, AT_FDCWD, new_pathname, 0);
}

int renameat(int olddirfd, const char *old_pathname,
        int newdirfd, const char *new_pathname) {
    return renameat2(olddirfd, old_pathname, newdirfd, new_pathname, 0);
}

int truncate(const char *pathname, off_t length) {
    if(!g_cfs_inited) {
        libc_truncate = (truncate_t)dlsym(RTLD_NEXT, "truncate");
        return libc_truncate(pathname, length);
    }

    char *path = get_cfs_path(pathname);
    int re;
    if(g_hook && path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = libc_truncate(pathname, length);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_truncate(g_cfs_client_id, path, length)), re);
    } else {
        re = libc_truncate(pathname, length);
    }

log:
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, length:%d, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int ftruncate(int fd, off_t length) {
    if(!g_cfs_inited) {
        libc_ftruncate = (ftruncate_t)dlsym(RTLD_NEXT, "ftruncate");
        return libc_ftruncate(fd, length);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_ftruncate(fd, length);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f != NULL) {
            update_inode_size(f->inode, length);
        }
        WRAPPER(cfs_errno(cfs_ftruncate(g_cfs_client_id, fd, length)), re);
    } else {
        re = libc_ftruncate(fd, length);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, length:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", length, re);
    #endif
    return re;
}
weak_alias (ftruncate, ftruncate64)

int fallocate(int fd, int mode, off_t offset, off_t len) {
    if(!g_cfs_inited) {
        libc_fallocate = (fallocate_t)dlsym(RTLD_NEXT, "fallocate");
        return libc_fallocate(fd, mode, offset, len);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_fallocate(fd, mode, offset, len);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_fallocate(g_cfs_client_id, fd, mode, offset, len)), re);
    } else {
        re = libc_fallocate(fd, mode, offset, len);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, mode:%#X, offset:%ld, len:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", mode, offset, len, re);
    #endif
    return re;
}
weak_alias (fallocate, fallocate64)

int posix_fallocate(int fd, off_t offset, off_t len) {
    if(!g_cfs_inited) {
        libc_posix_fallocate = (posix_fallocate_t)dlsym(RTLD_NEXT, "posix_fallocate");
        return libc_posix_fallocate(fd, offset, len);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_posix_fallocate(fd, offset, len);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_posix_fallocate(g_cfs_client_id, fd, offset, len)), re);
    } else {
        re = libc_posix_fallocate(fd, offset, len);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, offset:%ld, len:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", offset, len, re);
    #endif
    return re;
}
weak_alias (posix_fallocate, posix_fallocate64)

/*
 * Directory operations
 */

int mkdir(const char *pathname, mode_t mode) {
    return mkdirat(AT_FDCWD, pathname, mode);
}

int mkdirat(int dirfd, const char *pathname, mode_t mode) {
    if(!g_cfs_inited) {
        libc_mkdirat = (mkdirat_t)dlsym(RTLD_NEXT, "mkdirat");
        return libc_mkdirat(dirfd, pathname, mode);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_mkdirat(dirfd, pathname, mode);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_mkdirsat(g_cfs_client_id, dirfd, cfs_path, mode)), re);
    } else {
        re = libc_mkdirat(dirfd, pathname, mode);
    }

log:
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, dirfd: %d, pathname:%s, mode:%d, re:%d\n", __func__, is_cfs > 0, dirfd, pathname == NULL ? "" : pathname, mode, re);
    #endif
    return re;
}

int rmdir(const char *pathname) {
    if(!g_cfs_inited) {
        libc_rmdir = (rmdir_t)dlsym(RTLD_NEXT, "rmdir");
        return libc_rmdir(pathname);
    }

    char *path = get_cfs_path(pathname);
    int re;
    if(g_hook && path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = libc_rmdir(pathname);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_rmdir(g_cfs_client_id, path)), re);
    } else {
        re = libc_rmdir(pathname);
    }

log:
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname == NULL ? "" : pathname, re);
    #endif
    return re;
}

char *getcwd(char *buf, size_t size) {
    if(!g_cfs_inited) {
        libc_getcwd = (getcwd_t)dlsym(RTLD_NEXT, "getcwd");
        return libc_getcwd(buf, size);
    }

    char *re = NULL;
    int alloc_size;
    int len_mount;
    int len_cwd;
    int len;
    if(buf != NULL && size == 0) {
        errno = EINVAL;
        goto log;
    }

    if(g_cwd == NULL) {
        char *cwd = libc_getcwd(buf, size);
        if(cwd == NULL) {
            goto log;
        }
        // Always duplicate cwd enven if cwd is malloc 'ed by libc_getcwd, 
        // because caller of getcwd may free the returned cwd afterwards.
        char *dupcwd = strdup(cwd);
        if(dupcwd == NULL) {
            if(buf == NULL) {
                free(cwd);
            }
            goto log;
        }
        g_cwd = dupcwd;
        g_in_cfs = false;
        re = cwd;
        goto log;
    }

    len_mount = 0;
    // If g_cwd="/" ignore the backslash
    len_cwd = strcmp(g_cwd, "/") ? strlen(g_cwd) : 0;
    len = len_cwd;
    if(g_in_cfs) {
        len_mount = strlen(g_mount_point);
        len += len_mount;
    }
    if(size > 0 && size < len+1) {
        errno = ENAMETOOLONG;
        goto log;
    }

    alloc_size = size;
    if(size == 0) {
        alloc_size = len + 1;
    }
    if(buf == NULL) {
        buf = (char *)malloc(alloc_size);
        if(buf == NULL) {
            goto log;
        }
        memset(buf, '\0', alloc_size);
    }

    if(g_in_cfs) {
        strcat(buf, g_mount_point);
    }
    if(len_cwd > 0) {
        strcat(buf, g_cwd);
    }
    re = buf;

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, re: %s\n", __func__, re == NULL ? "" : re);
    #endif
    return re;
}

int chdir(const char *pathname) {
    if(!g_cfs_inited) {
        libc_chdir = (chdir_t)dlsym(RTLD_NEXT, "chdir");
        return libc_chdir(pathname);
    }

    int re = -1;
    char *clean_path = get_clean_path(pathname);
    char *abs_path;
    char *cfs_path;
    if(clean_path == NULL) {
        goto log;
    }

    abs_path = clean_path;
    if(pathname[0] != '/') {
        char *cwd = getcwd(NULL, 0);
        if(cwd == NULL) {
            free(clean_path);
            goto log;
        }
        abs_path = cat_path(cwd, clean_path);
        free(cwd);
        free(clean_path);
        if(abs_path == NULL) {
            goto log;
        }
    }

    cfs_path = get_cfs_path(abs_path);
    if(g_hook && cfs_path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = libc_chdir(abs_path);
        if(re < 0) {
            free(abs_path);
            goto log;
        }
        #endif
        free(abs_path);
        WRAPPER(cfs_errno(cfs_chdir(g_cfs_client_id, cfs_path)), re);
        if(re == 0) {
            g_in_cfs = true;
            free(g_cwd);
            g_cwd = cfs_path;
        } else {
            free(cfs_path);
        }
    } else {
        free(cfs_path);
        re = libc_chdir(abs_path);
        if(re == 0) {
            g_in_cfs = false;
            free(g_cwd);
            g_cwd = abs_path;
        } else {
            free(abs_path);
        }
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, pathname:%s, re:%d\n", __func__, pathname == NULL ? "" : pathname, re);
    #endif
    return re;
}

int fchdir(int fd) {
    if(!g_cfs_inited) {
        libc_fchdir = (fchdir_t)dlsym(RTLD_NEXT, "fchdir");
        return libc_fchdir(fd);
    }

    int re = -1;
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    char *buf;
    if(!g_hook || !is_cfs) {
        re = libc_fchdir(fd);
        g_in_cfs = false;
        free(g_cwd);
        g_cwd = NULL;
        goto log;
    }

    #ifdef DUP_TO_LOCAL
    re = libc_fchdir(fd);
    if(re < 0) {
        goto log;
    }
    #endif
    buf = (char *) malloc(PATH_MAX);
    WRAPPER(cfs_errno(cfs_fchdir(g_cfs_client_id, fd, buf, PATH_MAX)), re);
    if (re == 0) {
        g_in_cfs = true;
        free(g_cwd);
        g_cwd = buf;
    } else {
        free(buf);
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, fd:%d, re:%d\n", __func__, is_cfs>0, fd, re);
    #endif
    return re;
}

DIR *opendir(const char *pathname) {
    if(!g_cfs_inited) {
        libc_opendir = (opendir_t)dlsym(RTLD_NEXT, "opendir");
        return libc_opendir(pathname);
    }

    char *path = get_cfs_path(pathname);
    if(!g_hook || path == NULL) {
        free(path);
        return libc_opendir(pathname);
    }
    int fd;
    WRAPPER(cfs_openat(g_cfs_client_id, AT_FDCWD, path, O_RDONLY | O_DIRECTORY, 0), fd);
    free(path);

    if(fd < 0) {
        return NULL;
    }
    if(fd & CFS_FD_MASK) {
        WRAPPER_IGNORE_RES(cfs_close(g_cfs_client_id, fd));
        return NULL;
    }

    fd |= CFS_FD_MASK;
    size_t allocation = sizeof(struct dirent);
    DIR *dirp = (DIR *)malloc(sizeof(DIR) + allocation);
    if(dirp == NULL) {
        return NULL;
    }
    dirp->fd = fd;
    dirp->allocation = allocation;
    dirp->size = 0;
    dirp->offset = 0;
    dirp->filepos = 0;
    return dirp;
}

DIR *fdopendir(int fd) {
    if(!g_cfs_inited) {
        libc_fdopendir = (fdopendir_t)dlsym(RTLD_NEXT, "fdopendir");
        return libc_fdopendir(fd);
    }

    int is_cfs = fd & CFS_FD_MASK;
    if(!g_hook || !is_cfs) {
        return libc_fdopendir(fd);
    }

    size_t allocation = sizeof(struct dirent);
    DIR *dirp = (DIR *)malloc(sizeof(DIR) + allocation);
    if(dirp == NULL) {
        return NULL;
    }
    dirp->fd = fd;
    dirp->allocation = allocation;
    dirp->size = 0;
    dirp->offset = 0;
    dirp->filepos = 0;
    return dirp;
}

struct dirent *readdir(DIR *dirp) {
    if(!g_cfs_inited) {
        libc_readdir = (readdir_t)dlsym(RTLD_NEXT, "readdir");
        return libc_readdir(dirp);
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    if(dirp == NULL) {
        errno = EBADF;
        return NULL;
    }
    int is_cfs = dirp->fd & CFS_FD_MASK;
    if(!g_hook || !is_cfs) {
        return libc_readdir(dirp);
    }

    struct dirent *dp;
    if(dirp->offset >= dirp->size) {
        int fd = dirp->fd & ~CFS_FD_MASK;
        int count;
        WRAPPER(cfs_getdents(g_cfs_client_id, fd, dirp->data, dirp->allocation), count);
        if(count <= 0) {
            if(count < 0) {
                errno = EBADF;
            }
            return NULL;
        }
        dirp->size = count;
        dirp->offset = 0;
    }

    dp = (struct dirent *) &dirp->data[dirp->offset];
    dirp->offset += dp->d_reclen;
    dirp->filepos = dp->d_off;
    return dp;
}

struct dirent64 *readdir64(DIR *dirp) {
    return (struct dirent64 *)readdir(dirp);
}

int closedir(DIR *dirp) {
    if(!g_cfs_inited) {
        libc_closedir = (closedir_t)dlsym(RTLD_NEXT, "closedir");
        return libc_closedir(dirp);
    }

    if(dirp == NULL) {
        errno = EBADF;
        return -1;
    }

    int is_cfs = dirp->fd & CFS_FD_MASK;
    dirp->fd &= ~CFS_FD_MASK;
    int re;
    if(!g_hook || !is_cfs) {
        re = libc_closedir(dirp);
    } else {
        WRAPPER(cfs_errno(cfs_close(g_cfs_client_id, dirp->fd)), re);
        free(dirp);
    }
    return re;
}

char *realpath(const char *path, char *resolved_path) {
    if(!g_cfs_inited) {
        libc_realpath = (realpath_t)dlsym(RTLD_NEXT, "realpath");
        return libc_realpath(path, resolved_path);
    }

    char *re = NULL;
    char *clean_path = get_clean_path(path);
    char *abs_path;
    if(clean_path == NULL) {
        goto log;
    }

    abs_path = clean_path;
    if(path[0] != '/') {
        char *cwd = getcwd(NULL, 0);
        if(cwd == NULL) {
            free(clean_path);
            goto log;
        }
        abs_path = cat_path(cwd, clean_path);
        free(cwd);
        free(clean_path);
        if(abs_path == NULL) {
            goto log;
        }
    }
    if(strlen(abs_path) >= PATH_MAX) {
        free(abs_path);
        errno = ENAMETOOLONG;
        goto log;
    }
    if(resolved_path != NULL) {
        memcpy(resolved_path, abs_path, strlen(abs_path)+1);
        free(abs_path);
        re = resolved_path;
    } else {
        re = abs_path;
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, path:%s, resolved_path:%s, re:%s\n", __func__, path == NULL ? "" : path, resolved_path == NULL ? "" : resolved_path, re == NULL ? "" : re);
    #endif
    return re;
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
    if(!g_cfs_inited) {
        libc_linkat = (linkat_t)dlsym(RTLD_NEXT, "linkat");
        return libc_linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags);
    }

    int is_cfs_old = 0;
    char *old_path = NULL;
    if((old_pathname != NULL && old_pathname[0] == '/') || olddirfd == AT_FDCWD) {
        old_path = get_cfs_path(old_pathname);
        is_cfs_old = (old_path != NULL);
    } else {
        is_cfs_old = olddirfd & CFS_FD_MASK;
        olddirfd = olddirfd & ~CFS_FD_MASK;
    }

    int is_cfs_new = 0;
    char *new_path = NULL;
    if((new_pathname != NULL && new_pathname[0] == '/') || newdirfd == AT_FDCWD) {
        new_path = get_cfs_path(new_pathname);
        is_cfs_new = (new_path != NULL);
    } else {
        is_cfs_new = newdirfd & CFS_FD_MASK;
        newdirfd = newdirfd & ~CFS_FD_MASK;
    }

    const char *cfs_old_path = (old_path == NULL) ? old_pathname : old_path;
    const char *cfs_new_path = (new_path == NULL) ? new_pathname : new_path;
    int re = -1;
    if(g_hook && is_cfs_old && is_cfs_new) {
        WRAPPER(cfs_errno(cfs_linkat(g_cfs_client_id, olddirfd, cfs_old_path, newdirfd, cfs_new_path, flags)), re);
    } else if(!g_hook || (!is_cfs_old && !is_cfs_new)) {
        re = libc_linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags);
    }

    free(old_path);
    free(new_path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, olddirfd:%d, old_pathname:%s, is_cfs_old:%d, newdirfd:%d, new_pathname:%s, is_cfs_new:%d, flags:%#x, re:%d\n", __func__, olddirfd, old_pathname, is_cfs_old, newdirfd, new_pathname, is_cfs_new, flags, re);
    #endif
    return re;
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlink(const char *target, const char *linkpath) {
    return symlinkat(target, AT_FDCWD, linkpath);
}

// symlink a CFS linkpath to ordinary file target is not allowed
int symlinkat(const char *target, int dirfd, const char *linkpath) {
    if(!g_cfs_inited) {
        libc_symlinkat = (symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");
        return libc_symlinkat(target, dirfd, linkpath);
    }

    char *t = get_cfs_path(target);
    int is_cfs = 0;
    char *path = NULL;
    if((linkpath != NULL && linkpath[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(linkpath);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    int re = -1;
    if(g_hook && is_cfs && t != NULL) {
        WRAPPER(cfs_errno(cfs_symlinkat(g_cfs_client_id, t, dirfd, (path == NULL) ? linkpath : path)), re);
    } else if(!g_hook || !is_cfs) {
        re = libc_symlinkat(target, dirfd, linkpath);
    }

    free(t);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, target:%s, dirfd:%d, linkpath:%s, re:%d\n", __func__, target, dirfd, linkpath, re);
    #endif
    return re;
}

int unlink(const char *pathname) {
    return unlinkat(AT_FDCWD, pathname, 0);
}

int unlinkat(int dirfd, const char *pathname, int flags) {
    if(!g_cfs_inited) {
        libc_unlinkat = (unlinkat_t)dlsym(RTLD_NEXT, "unlinkat");
        return libc_unlinkat(dirfd, pathname, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_unlinkat(dirfd, pathname, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_unlinkat(g_cfs_client_id, dirfd, cfs_path, flags)), re);
    } else {
        re = libc_unlinkat(dirfd, pathname, flags);
    }

log:
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, dirfd:%d, pathname:%s, flags:%#x, re:%d\n", __func__, is_cfs > 0, dirfd, pathname, flags, re);
    #endif
    return re;
}

ssize_t readlink(const char *pathname, char *buf, size_t size) {
    return readlinkat(AT_FDCWD, pathname, buf, size);
}

ssize_t readlinkat(int dirfd, const char *pathname, char *buf, size_t size) {
    if(!g_cfs_inited) {
        libc_readlinkat = (readlinkat_t)dlsym(RTLD_NEXT, "readlinkat");
        return libc_readlinkat(dirfd, pathname, buf, size);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    ssize_t re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno_ssize_t(cfs_readlinkat(g_cfs_client_id, dirfd, cfs_path, buf, size)), re) :
           libc_readlinkat(dirfd, pathname, buf, size);
    free(path);
    return re;
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
    if(!g_cfs_inited) {
        libc_stat = (stat_t)dlsym(RTLD_NEXT, "__xstat");
        return libc_stat(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_stat(g_cfs_client_id, path, statbuf)), re) : libc_stat(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __xstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        libc_stat64 = (stat64_t)dlsym(RTLD_NEXT, "__xstat64");
        return libc_stat64(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re;
    if(g_hook && path != NULL) {
        WRAPPER(cfs_errno(cfs_stat64(g_cfs_client_id, path, statbuf)), re);
        if(re > 0) {
            pthread_rwlock_rdlock(&g_inode_open_file_lock);
            auto it = g_inode_open_file.find(statbuf->st_ino);
            if(it != g_inode_open_file.end() && !it->second.empty()) {
                statbuf->st_size = (*it->second.begin())->size;
            }
            pthread_rwlock_unlock(&g_inode_open_file_lock);
        }
    } else {
        re = libc_stat64(ver, pathname, statbuf);
    }
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __lxstat(int ver, const char *pathname, struct stat *statbuf) {
    if(!g_cfs_inited) {
        libc_lstat = (lstat_t)dlsym(RTLD_NEXT, "__lxstat");
        return libc_lstat(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_lstat(g_cfs_client_id, path, statbuf)), re) : libc_lstat(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __lxstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        libc_lstat64 = (lstat64_t)dlsym(RTLD_NEXT, "__lxstat64");
        return libc_lstat64(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_lstat64(g_cfs_client_id, path, statbuf)), re) :
             libc_lstat64(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __fxstat(int ver, int fd, struct stat *statbuf) {
    if(!g_cfs_inited) {
        libc_fstat = (fstat_t)dlsym(RTLD_NEXT, "__fxstat");
        return libc_fstat(ver, fd, statbuf);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fstat(g_cfs_client_id, fd, statbuf)), re) : libc_fstat(ver, fd, statbuf);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, fd:%d, re:%d\n", __func__, is_cfs > 0, fd, re);
    #endif
    return re;
}

int __fxstat64(int ver, int fd, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        libc_fstat64 = (fstat64_t)dlsym(RTLD_NEXT, "__fxstat64");
        return libc_fstat64(ver, fd, statbuf);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fstat64(g_cfs_client_id, fd, statbuf)), re) :
        libc_fstat64(ver, fd, statbuf);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, fd:%d, re:%d\n", __func__, is_cfs > 0, fd, re);
    #endif
    return re;
}

int __fxstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    if(!g_cfs_inited) {
        libc_fstatat = (fstatat_t)dlsym(RTLD_NEXT, "__fxstatat");
        return libc_fstatat(ver, dirfd, pathname, statbuf, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fstatat(g_cfs_client_id, dirfd, cfs_path, statbuf, flags)), re) :
             libc_fstatat(ver, dirfd, pathname, statbuf, flags);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, dirfd:%d, pathname:%s, re:%d\n", __func__, dirfd, pathname, re);
    #endif
    return re;
}

int __fxstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    if(!g_cfs_inited) {
        libc_fstatat64 = (fstatat64_t)dlsym(RTLD_NEXT, "__fxstatat64");
        return libc_fstatat64(ver, dirfd, pathname, statbuf, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fstatat64(g_cfs_client_id, dirfd, cfs_path, statbuf, flags)), re) :
        libc_fstatat64(ver, dirfd, pathname, statbuf, flags);
    free(path);
    return re;
}

int chmod(const char *pathname, mode_t mode) {
    return fchmodat(AT_FDCWD, pathname, mode, 0);
}

int fchmod(int fd, mode_t mode) {
    if(!g_cfs_inited) {
        libc_fchmod = (fchmod_t)dlsym(RTLD_NEXT, "fchmod");
        return libc_fchmod(fd, mode);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return is_cfs ? WRAPPER(cfs_errno(cfs_fchmod(g_cfs_client_id, fd, mode)), re) : libc_fchmod(fd, mode);
}

int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    if(!g_cfs_inited) {
        libc_fchmodat = (fchmodat_t)dlsym(RTLD_NEXT, "fchmodat");
        return libc_fchmodat(dirfd, pathname, mode, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fchmodat(g_cfs_client_id, dirfd, cfs_path, mode, flags)), re) :
        libc_fchmodat(dirfd, pathname, mode, flags);
    free(path);
    return re;
}

int chown(const char *pathname, uid_t owner, gid_t group) {
    return fchownat(AT_FDCWD, pathname, owner, group, 0);
}

int lchown(const char *pathname, uid_t owner, gid_t group) {
    if(!g_cfs_inited) {
        libc_lchown = (lchown_t)dlsym(RTLD_NEXT, "lchown");
        return libc_lchown(pathname, owner, group);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_lchown(g_cfs_client_id, path, owner, group)), re) :
             libc_lchown(pathname, owner, group);
    free(path);
    return re;
}

int fchown(int fd, uid_t owner, gid_t group) {
    if(!g_cfs_inited) {
        libc_fchown = (fchown_t)dlsym(RTLD_NEXT, "fchown");
        return libc_fchown(fd, owner, group);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fchown(g_cfs_client_id, fd, owner, group)), re) :
        libc_fchown(fd, owner, group);
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) {
    if(!g_cfs_inited) {
        libc_fchownat = (fchownat_t)dlsym(RTLD_NEXT, "fchownat");
        return libc_fchownat(dirfd, pathname, owner, group, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fchownat(g_cfs_client_id, dirfd, cfs_path, owner, group, flags)), re):
        libc_fchownat(dirfd, pathname, owner, group, flags);
    free(path);
    return re;
}

int utime(const char *pathname, const struct utimbuf *times) {
    if(!g_cfs_inited) {
        libc_utime = (utime_t)dlsym(RTLD_NEXT, "utime");
        return libc_utime(pathname, times);
    }

    struct timespec *pts;
    if(times != NULL) {
        struct timespec ts[2] = {times->actime, 0, times->modtime, 0};
        pts = & ts[0];
    }
    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_utimens(g_cfs_client_id, path, pts, 0)), re) :
            libc_utime(pathname, times);
    free(path);
    return re;
}

int utimes(const char *pathname, const struct timeval *times) {
    if(!g_cfs_inited) {
        libc_utimes = (utimes_t)dlsym(RTLD_NEXT, "utimes");
        return libc_utimes(pathname, times);
    }

    struct timespec *pts;
    if(times != NULL) {
        struct timespec ts[2] = {times[0].tv_sec, times[0].tv_usec*1000, times[1].tv_sec, times[1].tv_usec*1000};
        pts = & ts[0];
    }
    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_utimens(g_cfs_client_id, path, pts, 0)), re) :
            libc_utimes(pathname, times);
    free(path);
    return re;
}

int futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    if(!g_cfs_inited) {
        libc_futimesat = (futimesat_t)dlsym(RTLD_NEXT, "futimesat");
        return libc_futimesat(dirfd, pathname, times);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    struct timespec *pts;
    if(times != NULL) {
        struct timespec ts[2] = {times[0].tv_sec, times[0].tv_usec*1000, times[1].tv_sec, times[1].tv_usec*1000};
        pts = & ts[0];
    }
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_utimensat(g_cfs_client_id, dirfd, cfs_path, pts, 0)), re) :
        libc_futimesat(dirfd, pathname, times);
    free(path);
    return re;
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    if(!g_cfs_inited) {
        libc_utimensat = (utimensat_t)dlsym(RTLD_NEXT, "utimensat");
        return libc_utimensat(dirfd, pathname, times, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    re = g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_utimensat(g_cfs_client_id, dirfd, cfs_path, times, flags)), re) :
        libc_utimensat(dirfd, pathname, times, flags);
    free(path);
    return re;
}

int futimens(int fd, const struct timespec times[2]) {
    if(!g_cfs_inited) {
        libc_futimens = (futimens_t)dlsym(RTLD_NEXT, "futimens");
        return libc_futimens(fd, times);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_futimens(g_cfs_client_id, fd, times)), re) : libc_futimens(fd, times);
}

int access(const char *pathname, int mode) {
    return faccessat(AT_FDCWD, pathname, mode, 0);
}

int faccessat(int dirfd, const char *pathname, int mode, int flags) {
    if(!g_cfs_inited) {
        libc_faccessat = (faccessat_t)dlsym(RTLD_NEXT, "faccessat");
        return libc_faccessat(dirfd, pathname, mode, flags);
    }

    int is_cfs = 0;
    char *path = NULL;
    if((pathname != NULL && pathname[0] == '/') || dirfd == AT_FDCWD) {
        path = get_cfs_path(pathname);
        is_cfs = (path != NULL);
    } else {
        is_cfs = dirfd & CFS_FD_MASK;
        dirfd = dirfd & ~CFS_FD_MASK;
    }

    const char *cfs_path = (path == NULL) ? pathname : path;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_faccessat(dirfd, pathname, mode, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno(cfs_faccessat(g_cfs_client_id, dirfd, cfs_path, mode, flags)), re);
    } else {
        re = libc_faccessat(dirfd, pathname, mode, flags);
    }

log:
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, dirfd:%d, pathname:%s, mode:%d, flags:%#x, re:%d\n", 
    __func__, is_cfs > 0, dirfd, pathname, mode, flags, re);
    #endif
    return re;
}


/*
 * Extended file attributes
 */

int setxattr(const char *pathname, const char *name,
        const void *value, size_t size, int flags) {
    if(!g_cfs_inited) {
        libc_setxattr = (setxattr_t)dlsym(RTLD_NEXT, "setxattr");
        return libc_setxattr(pathname, name, value, size, flags);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_setxattr(g_cfs_client_id, path, name, value, size, flags)), re) :
             libc_setxattr(pathname, name, value, size, flags);
    free(path);
    return re;
}

int lsetxattr(const char *pathname, const char *name,
             const void *value, size_t size, int flags) {
    if(!g_cfs_inited) {
        libc_lsetxattr = (lsetxattr_t)dlsym(RTLD_NEXT, "lsetxattr");
        return libc_lsetxattr(pathname, name, value, size, flags);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_lsetxattr(g_cfs_client_id, path, name, value, size, flags)), re) :
             libc_lsetxattr(pathname, name, value, size, flags);
    free(path);
    return re;
}

int fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    if(!g_cfs_inited) {
        libc_fsetxattr = (fsetxattr_t)dlsym(RTLD_NEXT, "fsetxattr");
        return libc_fsetxattr(fd, name, value, size, flags);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fsetxattr(g_cfs_client_id, fd, name, value, size, flags)), re) :
           libc_fsetxattr(fd, name, value, size, flags);
}

ssize_t getxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        libc_getxattr = (getxattr_t)dlsym(RTLD_NEXT, "getxattr");
        return libc_getxattr(pathname, name, value, size);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno_ssize_t(cfs_getxattr(g_cfs_client_id, path, name, value, size)), re) :
             libc_getxattr(pathname, name, value, size);
    free(path);
    return re;
}

ssize_t lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        libc_lgetxattr = (lgetxattr_t)dlsym(RTLD_NEXT, "lgetxattr");
        return libc_lgetxattr(pathname, name, value, size);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno_ssize_t(cfs_lgetxattr(g_cfs_client_id, path, name, value, size)), re) :
             libc_lgetxattr(pathname, name, value, size);
    free(path);
    return re;
}

ssize_t fgetxattr(int fd, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        libc_fgetxattr = (fgetxattr_t)dlsym(RTLD_NEXT, "fgetxattr");
        return libc_fgetxattr(fd, name, value, size);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno_ssize_t(cfs_fgetxattr(g_cfs_client_id, fd, name, value, size)), re) :
           libc_fgetxattr(fd, name, value, size);
}

ssize_t listxattr(const char *pathname, char *list, size_t size) {
    if(!g_cfs_inited) {
        libc_listxattr = (listxattr_t)dlsym(RTLD_NEXT, "listxattr");
        return libc_listxattr(pathname, list, size);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno_ssize_t(cfs_listxattr(g_cfs_client_id, path, list, size)), re) :
             libc_listxattr(pathname, list, size);
    free(path);
    return re;
}

ssize_t llistxattr(const char *pathname, char *list, size_t size) {
    if(!g_cfs_inited) {
        libc_llistxattr = (llistxattr_t)dlsym(RTLD_NEXT, "llistxattr");
        return libc_llistxattr(pathname, list, size);
    }

    char *path = get_cfs_path(pathname);
    ssize_t re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno_ssize_t(cfs_llistxattr(g_cfs_client_id, path, list, size)), re) :
             libc_llistxattr(pathname, list, size);
    free(path);
    return re;
}

ssize_t flistxattr(int fd, char *list, size_t size) {
    if(!g_cfs_inited) {
        libc_flistxattr = (flistxattr_t)dlsym(RTLD_NEXT, "flistxattr");
        return libc_flistxattr(fd, list, size);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno_ssize_t(cfs_flistxattr(g_cfs_client_id, fd, list, size)), re) :
           libc_flistxattr(fd, list, size);
}

int removexattr(const char *pathname, const char *name) {
    if(!g_cfs_inited) {
        libc_removexattr = (removexattr_t)dlsym(RTLD_NEXT, "removexattr");
        return libc_removexattr(pathname, name);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_removexattr(g_cfs_client_id, path, name)), re) :
             libc_removexattr(pathname, name);
    free(path);
    return re;
}

int lremovexattr(const char *pathname, const char *name) {
    if(!g_cfs_inited) {
        libc_lremovexattr = (lremovexattr_t)dlsym(RTLD_NEXT, "lremovexattr");
        return libc_lremovexattr(pathname, name);
    }

    char *path = get_cfs_path(pathname);
    int re;
    re = (g_hook && path != NULL) ? WRAPPER(cfs_errno(cfs_lremovexattr(g_cfs_client_id, path, name)), re) :
             libc_lremovexattr(pathname, name);
    free(path);
    return re;
}

int fremovexattr(int fd, const char *name) {
    if(!g_cfs_inited) {
        libc_fremovexattr = (fremovexattr_t)dlsym(RTLD_NEXT, "fremovexattr");
        return libc_fremovexattr(fd, name);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    return g_hook && is_cfs ? WRAPPER(cfs_errno(cfs_fremovexattr(g_cfs_client_id, fd, name)), re) :
           libc_fremovexattr(fd, name);
}


/*
 * File descriptor manipulations
 */

int fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    void *arg = va_arg(args, void *);
    va_end(args);

    if(!g_cfs_inited) {
        libc_fcntl = (fcntl_t)dlsym(RTLD_NEXT, "fcntl");
        return libc_fcntl(fd, cmd, arg);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re, re_old = 0;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_fcntl(fd, cmd, arg);
        if(re < 0) {
            goto log;
        }
        if(cmd == F_SETLK || cmd == F_SETLKW) {
            WRAPPER(cfs_fcntl_lock(g_cfs_client_id, fd, cmd, (struct flock *)arg), re);
        } else {
            re_old = re;
            WRAPPER(cfs_fcntl(g_cfs_client_id, fd, cmd,
            (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) ? re_old : (intptr_t)arg), re);
            if(re != re_old) {
                goto log;
            }
        }
        #else
        if(cmd == F_SETLK || cmd == F_SETLKW) {
            WRAPPER(cfs_fcntl_lock(g_cfs_client_id, fd, cmd, (struct flock *)arg), re);
        } else {
            WRAPPER(cfs_fcntl(g_cfs_client_id, fd, cmd, (intptr_t)arg), re);
        }
        #endif
        re = cfs_errno(re);
    } else {
        re = libc_fcntl(fd, cmd, arg);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    map<int, string> cmd_str = {{F_DUPFD, "F_DUPFD"}, {F_DUPFD_CLOEXEC, "F_DUPFD_CLOEXEC"}, {F_GETFD, "F_GETFD"}, {F_SETFD, "F_SETFD"}, {F_GETFL, "F_GETFL"}, {F_SETFL, "F_SETFL"}, {F_SETLK, "F_SETLK"}, {F_GETLK, "F_GETLK"}};
    auto it = cmd_str.find(cmd);
    log_debug("hook %s, is_cfs:%d, fd:%d, cmd:%d(%s), arg:%u(%s), re:%d, re_old:%d\n", __func__, is_cfs>0, fd, cmd, 
    it != cmd_str.end() ? it->second.c_str() : "", (intptr_t)arg, (cmd==F_SETFL&&(intptr_t)arg&O_DIRECT)?"O_DIRECT":"", re, re_old);
    #endif
    if(g_hook && is_cfs && (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC)) {
        re |= CFS_FD_MASK;
    }
    return re;
}
weak_alias (fcntl, fcntl64)

int dup2(int oldfd, int newfd) {
    if(!g_cfs_inited) {
        libc_dup2 = (dup2_t)dlsym(RTLD_NEXT, "dup2");
        return libc_dup2(oldfd, newfd);
    }

    // If newfd was open, close it before being reused
    auto it = g_dup_origin.find(newfd);
    if(it != g_dup_origin.end()) {
        WRAPPER_IGNORE_RES(cfs_close(g_cfs_client_id, it->second & ~CFS_FD_MASK));
        g_dup_origin.erase(it);
    }

    int is_cfs = oldfd & CFS_FD_MASK;
    int re = -1;
    if(g_hook && is_cfs) {
        if(newfd < 0) {
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        re = libc_dup2(oldfd & ~CFS_FD_MASK, newfd);
        if(re < 0) {
            goto log;
        }
        #endif
        g_dup_origin[newfd] = oldfd;
        re = newfd;
    } else {
        re = libc_dup2(oldfd, newfd);
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, oldfd:%d, newfd:%d, re:%d\n", __func__, is_cfs > 0, oldfd, newfd, re);
    #endif
    return re;
}

int dup3(int oldfd, int newfd, int flags) {
    if(!g_cfs_inited) {
        libc_dup3 = (dup3_t)dlsym(RTLD_NEXT, "dup3");
        return libc_dup3(oldfd, newfd, flags);
    }

    // If newfd was open, close it before being reused
    auto it = g_dup_origin.find(newfd);
    if(it != g_dup_origin.end()) {
        WRAPPER_IGNORE_RES(cfs_close(g_cfs_client_id, it->second & ~CFS_FD_MASK));
        g_dup_origin.erase(it);
    }

    int is_cfs = oldfd & CFS_FD_MASK;
    int re = -1;
    if(g_hook && is_cfs) {
        if(newfd < 0) {
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        re = libc_dup3(oldfd & ~CFS_FD_MASK, newfd, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        g_dup_origin[newfd] = oldfd;
        re = newfd;
    } else {
        re = libc_dup3(oldfd, newfd, flags);
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, oldfd:%d, newfd:%d, flags:%#x, re:%d\n", __func__, is_cfs > 0, oldfd, newfd, flags, re);
    #endif
    return re;
}


/*
 * Read & Write
 */

ssize_t read(int fd, void *buf, size_t count) {
    #ifdef _CFS_DEBUG
    struct timespec start, stop;
    clock_gettime(CLOCK_REALTIME, &start);
    #endif
    if(!g_cfs_inited) {
        libc_read = (read_t)dlsym(RTLD_NEXT, "read");
        return libc_read(fd, buf, count);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }

    off_t offset = 0;
    size_t size = 0;
    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    offset = lseek(fd, 0, SEEK_CUR);
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1, re_local = 0, re_cache = 0;

    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        char *buf_local = (char *)malloc(count);
        if(buf_local == NULL) {
            re = -1;
            goto log;
        }
        re_local = libc_read(fd, buf_local, count);
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f != NULL) {
            offset = f->pos;
            size = f->size;
            re_cache = read_cache(f, f->pos, count, buf);
            if(re_cache < count && f->pos + re_cache < f->size) {
                // data may reside both in cache and CFS, flush to prevent inconsistent read
                flush_file_range(f, f->pos, count);
                WRAPPER(cfs_errno_ssize_t(cfs_pread(g_cfs_client_id, fd, buf, count, f->pos)), re);
            } else {
                re = re_cache;
            }
            if(re > 0) {
                f->pos += re;
            }
        } else {
            WRAPPER(cfs_errno_ssize_t(cfs_read(g_cfs_client_id, fd, buf, count)), re);
        }
        #ifdef DUP_TO_LOCAL
        // Reading from local and CFS may be concurrent with writing to local and CFS.
        // There are two conditions in which data read from local and CFS may be different.
        // 1. read local -> write local -> write CFS -> read CFS
        // 2. write local -> read local -> read CFS -> write CFS
        // In contition 2, write CFS may be concurrent with read CFS, resulting in last bytes read being zero.
        if(re_local > 0 && re > 0 && memcmp(buf, buf_local, re)) {
            auto it = g_fd_path.find(fd);
            log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, count:%d, offset:%ld, re:%d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", count, offset, re);
            printf("CFS:\n");
            int total = 0;
            for(int i = 0; i < re; i++) {
                if(++total > 1024) {
                    break;
                }
                printf("%x ", ((unsigned char*)buf)[i]);
            }
            printf("\nlocal:\n");
            total = 0;
            for(int i = 0; i < re; i++) {
                if(++total > 1024) {
                    break;
                }
                printf("%x ", ((unsigned char*)buf_local)[i]);
            }
            printf("\n");
            WRAPPER_IGNORE_RES(cfs_flush_log());
        }
        free(buf_local);
        #endif
    } else {
        re = libc_read(fd, buf, count);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    clock_gettime(CLOCK_REALTIME, &stop);
    long time = (stop.tv_sec - start.tv_sec)*1000000000 + stop.tv_nsec - start.tv_nsec;
    log_debug("hook %s, is_cfs:%d, fd:%d, path: %s, count:%d, offset:%ld, size:%d, re:%d, re_cache:%d, time:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", count, offset, size, re, re_cache, time/1000);
    #endif
    return re;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    if(!g_cfs_inited) {
        libc_readv = (readv_t)dlsym(RTLD_NEXT, "readv");
        return libc_readv(fd, iov, iovcnt);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1;
    if(g_hook && is_cfs) {
        cfs_file_t *f = get_open_file(fd);
        if(f != NULL) {
            WRAPPER(cfs_errno_ssize_t(cfs_preadv(g_cfs_client_id, fd, iov, iovcnt, f->pos)), re);
            if(re > 0) {
                f->pos += re;
            }
        } else {
            WRAPPER(cfs_errno_ssize_t(cfs_readv(g_cfs_client_id, fd, iov, iovcnt)), re);
        }
        #ifdef DUP_TO_LOCAL
        if(re <= 0) {
            goto log;
        }
        struct iovec iov_local[iovcnt];
        for(int i = 0; i < iovcnt; i++) {
            iov_local[i].iov_base = malloc(iov[i].iov_len);
            iov_local[i].iov_len = iov[i].iov_len;
        }
        off_t offset = lseek(fd, 0, SEEK_CUR);
        re = libc_readv(fd, iov_local, iovcnt);
        if(re <= 0) {
            goto log;
        }
        for(int i = 0; i < iovcnt; i++) {
            if(memcmp(iov[i].iov_base, iov_local[i].iov_base, iov[i].iov_len)) {
                re = -1;
                WRAPPER_IGNORE_RES(cfs_flush_log());
                auto it = g_fd_path.find(fd);
                log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, offset:%ld, iovcnt:%d, iov_idx:%d, iov_len:%d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", offset, iovcnt, i, iov[i].iov_len);
            }
            free(iov_local[i].iov_base);
        }
        #endif
    } else {
        re = libc_readv(fd, iov, iovcnt);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", iovcnt, re);
    #endif
    return re;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    if(!g_cfs_inited) {
        libc_pread = (pread_t)dlsym(RTLD_NEXT, "pread");
        return libc_pread(fd, buf, count, offset);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1, re_local = 0, re_cache = 0;

    if(g_hook && is_cfs) {
        re = count;
        #ifdef DUP_TO_LOCAL
        char *buf_local = (char *)malloc(count);
        if(buf_local == NULL) {
            re = -1;
            goto log;
        }
        re_local = libc_pread(fd, buf_local, count, offset);
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f != NULL) {
            re_cache = read_cache(f, offset, count, buf);
            if(re_cache < count && offset + re_cache < f->size) {
                // data may reside both in cache and CFS, flush to prevent inconsistent read
                flush_file_range(f, offset, count);
                WRAPPER(cfs_errno_ssize_t(cfs_pread(g_cfs_client_id, fd, buf, count, offset)), re);
            } else {
                re = re_cache;
            }
        } else {
            WRAPPER(cfs_errno_ssize_t(cfs_pread(g_cfs_client_id, fd, buf, count, offset)), re);
        }
        #ifdef DUP_TO_LOCAL
        if(re_local > 0 && re > 0 && memcmp(buf, buf_local, re)) {
            auto it = g_fd_path.find(fd);
            log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, count:%d, offset:%ld, re:%d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", count, offset, re);
            printf("CFS:\n");
            int total = 0;
            for(int i = 0; i < re; i++) {
                if(++total > 1024) {
                    break;
                }
                printf("%x ", ((unsigned char*)buf)[i]);
            }
            printf("\nlocal:\n");
            total = 0;
            for(int i = 0; i < re; i++) {
                if(++total > 1024) {
                    break;
                }
                printf("%x ", ((unsigned char*)buf_local)[i]);
            }
            printf("\n");
            WRAPPER_IGNORE_RES(cfs_flush_log());
        }
        free(buf_local);
        #endif
    } else {
        re = libc_pread(fd, buf, count, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%ld, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", count, offset, re);
    #endif
    return re;
}
weak_alias (pread, pread64)

ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(!g_cfs_inited) {
        libc_preadv = (preadv_t)dlsym(RTLD_NEXT, "preadv");
        return libc_preadv(fd, iov, iovcnt, offset);
    }

    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        WRAPPER(cfs_errno_ssize_t(cfs_preadv(g_cfs_client_id, fd, iov, iovcnt, offset)), re);
        #ifdef DUP_TO_LOCAL
        if(re <= 0) {
            goto log;
        }
        struct iovec iov_local[iovcnt];
        for(int i = 0; i < iovcnt; i++) {
            iov_local[i].iov_base = malloc(iov[i].iov_len);
            iov_local[i].iov_len = iov[i].iov_len;
        }
        re = libc_preadv(fd, iov_local, iovcnt, offset);
        if(re <= 0) {
            goto log;
        }
        for(int i = 0; i < iovcnt; i++) {
            if(memcmp(iov[i].iov_base, iov_local[i].iov_base, iov[i].iov_len)) {
                re = -1;
                WRAPPER_IGNORE_RES(cfs_flush_log());
                auto it = g_fd_path.find(fd);
                log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, iovcnt:%d, offset:%ld, iov_idx: %d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", iovcnt, offset, i);
            }
            free(iov_local[i].iov_base);
        }
        #endif
    } else {
        re = libc_preadv(fd, iov, iovcnt, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, iovcnt:%d, offset:%ld, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", iovcnt, offset, re);
    #endif
    return re;
}

ssize_t write(int fd, const void *buf, size_t count) {
    #ifdef _CFS_DEBUG
    struct timespec start, stop;
    clock_gettime(CLOCK_REALTIME, &start);
    #endif
    if(!g_cfs_inited) {
        libc_write = (write_t)dlsym(RTLD_NEXT, "write");
        return libc_write(fd, buf, count);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }

    off_t offset = 0;
    size_t size = 0;
    #ifdef _CFS_DEBUG
    offset = lseek(fd, 0, SEEK_CUR);
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1, re_cache = 0;

    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_write(fd, buf, count);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f == NULL) {
            WRAPPER(cfs_errno_ssize_t(cfs_write(g_cfs_client_id, fd, buf, count)), re);
            goto log;
        }

        if(f->flags&O_APPEND) {
            f->pos = f->size;
        }
        offset = f->pos;
        re_cache = write_cache(f, f->pos, count, buf);
        if(f->cache_flag&FILE_CACHE_WRITE_THROUGH || re_cache < count) {
            //if(re_cache < count) log_debug("write cache fail, fd:%d, inode:%d, file_type:%d, offset:%ld, count:%d, size:%d, re_cache:%d\n", fd, f->inode, f->file_type, offset, count, f->size, re_cache);
            if(re_cache < count) {
                clear_file_range(f, f->pos, count);
            }
            WRAPPER(cfs_errno_ssize_t(cfs_pwrite(g_cfs_client_id, fd, buf, count, f->pos)), re);
        } else {
            re = re_cache;
        }
        if(re > 0) {
            f->pos += re;
            if(f->size < f->pos) {
                update_inode_size(f->inode, f->pos);
            }
            size = f->size;
        }
    } else {
        re = libc_write(fd, buf, count);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    clock_gettime(CLOCK_REALTIME, &stop);
    long time = (stop.tv_sec - start.tv_sec)*1000000000 + stop.tv_nsec - start.tv_nsec;
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%ld, size:%d, re:%d, re_cache:%d, time:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", count, offset, size, re, re_cache, time/1000);
    #endif
    return re;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    if(!g_cfs_inited) {
        libc_writev = (writev_t)dlsym(RTLD_NEXT, "writev");
        return libc_writev(fd, iov, iovcnt);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_writev(fd, iov, iovcnt);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f == NULL) {
            WRAPPER(cfs_errno_ssize_t(cfs_writev(g_cfs_client_id, fd, iov, iovcnt)), re);
            goto log;
        }

        if(f->flags&O_APPEND) {
            f->pos = f->size;
        }
        WRAPPER(cfs_errno_ssize_t(cfs_pwritev(g_cfs_client_id, fd, iov, iovcnt, f->pos)), re);
        if(re > 0) {
            f->pos += re;
            if(f->size < f->pos) {
                update_inode_size(f->inode, f->pos);
            }
        }
    } else {
        re = libc_writev(fd, iov, iovcnt);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", iovcnt, re);
    #endif
    return re;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    #ifdef _CFS_DEBUG
    struct timespec start, stop;
    clock_gettime(CLOCK_REALTIME, &start);
    #endif
    if(!g_cfs_inited) {
        libc_pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
        return libc_pwrite(fd, buf, count, offset);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1, re_cache = 0;

    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_pwrite(fd, buf, count, offset);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f == NULL) {
            WRAPPER(cfs_errno_ssize_t(cfs_pwrite(g_cfs_client_id, fd, buf, count, offset)), re);
            goto log;
        }

        re_cache = write_cache(f, offset, count, buf);
        if(f->cache_flag&FILE_CACHE_WRITE_THROUGH || re_cache < count) {
            if(re_cache < count) {
                clear_file_range(f, offset, count);
            }
            WRAPPER(cfs_errno_ssize_t(cfs_pwrite(g_cfs_client_id, fd, buf, count, offset)), re);
        } else {
            re = re_cache;
        }
        if(re > 0 && f->size < offset + re) {
            update_inode_size(f->inode, offset + re);
        }
    } else {
        re = libc_pwrite(fd, buf, count, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    clock_gettime(CLOCK_REALTIME, &stop);
    long time = (stop.tv_sec - start.tv_sec)*1000000000 + stop.tv_nsec - start.tv_nsec;
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%ld, re:%d, re_cache:%d, time:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", count, offset, re, re_cache, time/1000);
    #endif
    return re;
}
weak_alias (pwrite, pwrite64)

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(!g_cfs_inited) {
        libc_pwritev = (pwritev_t)dlsym(RTLD_NEXT, "pwritev");
        return libc_pwritev(fd, iov, iovcnt, offset);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re = -1;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_pwritev(fd, iov, iovcnt, offset);
        if(re < 0) {
            goto log;
        }
        #endif
        WRAPPER(cfs_errno_ssize_t(cfs_pwritev(g_cfs_client_id, fd, iov, iovcnt, offset)), re);
        cfs_file_t *f = get_open_file(fd);
        if(re > 0 && f != NULL && f->size < offset + re) {
            update_inode_size(f->inode, offset + re);
        }
    } else {
        re = libc_pwritev(fd, iov, iovcnt, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, offset:%ld, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", iovcnt, offset, re);
    #endif
    return re;
}

off_t lseek(int fd, off_t offset, int whence) {
    if(!g_cfs_inited) {
        libc_lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
        return libc_lseek(fd, offset, whence);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    off_t re = -1, re_cfs = -1;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_lseek(fd, offset, whence);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        if(f != NULL) {
            if(whence == SEEK_SET) {
                f->pos = offset;
            } else if(whence == SEEK_CUR) {
                f->pos += offset;
            } else if(whence == SEEK_END) {
                f->pos = f->size + offset;
            }
            re_cfs = f->pos;
        } else {
            WRAPPER(cfs_lseek(g_cfs_client_id, fd, offset, whence), re_cfs);
        }
        #ifdef DUP_TO_LOCAL
        if(re_cfs != re) {
            auto it = g_fd_path.find(fd);
            log_debug("hook %s, re from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, offset:%ld, whence:%d, re:%d, re_cfs:%d\n", __func__, is_cfs > 0, fd, it != g_fd_path.end() ? it->second : "", offset, whence, re, re_cfs);
        }
        #else
        re = re_cfs;
        #endif
    } else {
        re = libc_lseek(fd, offset, whence);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, offset:%ld, whence:%d, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", offset, whence, re);
    #endif
    return re;
}
weak_alias (lseek, lseek64)


/*
 * Synchronized I/O
 */

int fdatasync(int fd) {
    if(!g_cfs_inited) {
        libc_fdatasync = (fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
        return libc_fdatasync(fd);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_fdatasync(fd);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        int re_flush = 0;
        if(f != NULL) {
            re_flush = flush_file(f);
        }
        WRAPPER(cfs_errno(cfs_flush(g_cfs_client_id, fd)), re);
        if(re == 0) {
            re = re_flush;
        }
    } else {
        re = libc_fdatasync(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", re);
    #endif
    return re;
}

int fsync(int fd) {
    if(!g_cfs_inited) {
        libc_fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
        return libc_fsync(fd);
    }

    if(fd < 0) {
        return -1;
    }
    auto it = g_dup_origin.find(fd);
    if(it != g_dup_origin.end()) {
        fd = it->second;
    }
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = libc_fsync(fd);
        if(re < 0) {
            goto log;
        }
        #endif
        cfs_file_t *f = get_open_file(fd);
        int re_flush = 0;
        if(f != NULL) {
            re_flush = flush_file(f);
        }
        WRAPPER(cfs_errno(cfs_flush(g_cfs_client_id, fd)), re);
        if(re == 0) {
            re = re_flush;
        }
    } else {
        re = libc_fsync(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    auto fd_path_it = g_fd_path.find(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd, fd_path_it != g_fd_path.end() ? fd_path_it->second : "", re);
    #endif
    return re;
}


/*
 * Others
 */

void abort() {
    if(!g_cfs_inited) {
        libc_abort = (abort_t)dlsym(RTLD_NEXT, "abort");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        cfs_flush_log();
        pthread_rwlock_unlock(&update_rwlock);
    }
    // abort is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        libc_abort();
    }
}

void _exit(int status) {
    if(!g_cfs_inited) {
        libc__exit = (_exit_t)dlsym(RTLD_NEXT, "_exit");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        cfs_flush_log();
        pthread_rwlock_unlock(&update_rwlock);
    }
    // _exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        libc__exit(status);
    }
}

void exit(int status) {
    if(!g_cfs_inited) {
        libc_exit = (exit_t)dlsym(RTLD_NEXT, "exit");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    if (pthread_rwlock_tryrdlock(&update_rwlock) == 0) {
        cfs_flush_log();
        pthread_rwlock_unlock(&update_rwlock);
    }
    // exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        libc_exit(status);
    }
}

/*
// DON'T hook signal register function, segfault would occur when calling go
// function in signal handlers. Golang runtime would panic at runtime.cgocallback_gofunc.
int sigaction(int signum, const struct sigaction *act, struct sigaction *oldact) {
    // can't call cfs_init to initialize libc_sigaction, otherwise will be blocked in cfs_new_client
    libc_sigaction = (sigaction_t)dlsym(RTLD_NEXT, "sigaction");
    int re;
    // only hook signals which may terminate process
    bool is_fatal = signum == SIGSEGV || signum == SIGABRT || signum == SIGBUS ||
        signum == SIGILL || signum == SIGFPE || signum == SIGTERM;
    bool hook_action = act != NULL && act->sa_handler != SIG_IGN && is_fatal;
    if(!hook_action) {
        re = libc_sigaction(signum, act, oldact);
        goto log;
    }

    g_sa_handler[signum] = act->sa_handler;
    struct sigaction new_act = {
        .sa_handler = &signal_handler,
        .sa_mask = act->sa_mask,
        .sa_flags = act->sa_flags,
        .sa_restorer = act->sa_restorer
    };
    re = libc_sigaction(signum, &new_act, oldact);

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, hook_action:%d, signum:%d, re:%d\n", __func__, hook_action, signum, re);
    #endif
    return re;
}
*/

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
    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    #endif
    #ifdef _CFS_DEBUG
    printf("destructor\n");
    #endif
    pthread_rwlock_destroy(&update_rwlock);
    //cfs_close_client(g_cfs_client_id); //consume too much time
}

void* baseOpen(const char* name) {
    void *handle = dlopen(name, RTLD_NOW|RTLD_GLOBAL);
    if(handle == NULL) {
        char msg[1024];
        sprintf(msg, "dlopen %s error: %s\n", name, dlerror());
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        libc_exit(1);
    }
    return handle;
}

void* plugOpen(const char* name) {
    void *handle = dlopen(name, RTLD_NOW|RTLD_GLOBAL);
    if(handle == NULL) {
        char msg[1024];
        sprintf(msg, "dlopen %s error: %s\n", name, dlerror());
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        libc_exit(1);
    }

    void* task = dlsym(handle, "main..inittask");
    InitModule_t initModule = (InitModule_t)dlsym(handle, "InitModule");
    initModule(task);

    return handle;
}

void plugClose(void* handle) {
    FinishModule_t finishModule = (FinishModule_t)dlsym(handle, "FinishModule");
    void* task = dlsym(handle, "main..finitask");
    finishModule(task);

    int res = dlclose(handle);
    if(res != 0)
        fprintf(stderr, "dlclose error: %s\n", dlerror());
}

static void init() {
    if(g_cfs_inited) {
        return;
    }

    #ifdef _CFS_DEBUG
    printf("constructor\n");
    #endif

    init_libc_func();
    baseOpen("/usr/lib64/libempty.so");
    void *handle = plugOpen("/usr/lib64/libcfssdk.so");
    init_cfs_func(handle);

    g_config_path = getenv("CFS_CONFIG_PATH");
    if(g_config_path == NULL) {
        g_config_path = CFS_CFG_PATH;
        if(libc_access(g_config_path, F_OK)) {
            g_config_path = CFS_CFG_PATH_JED;
        }
    }

    // parse client configurations from ini file.
    client_config_t client_config;
    memset(&client_config, 0, sizeof(client_config_t));
    // libc printf CANNOT be used in this init function, otherwise will cause circular dependencies.
    if(ini_parse(g_config_path, config_handler, &client_config) < 0) {
        const char *msg = "Can't load CFS config file, use CFS_CONFIG_PATH env variable.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    if(client_config.mount_point == NULL || client_config.master_addr == NULL ||
    client_config.vol_name == NULL || client_config.owner == NULL || client_config.log_dir == NULL) {
        const char *msg = "Check CFS config file for necessary parameters.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_mount_point = client_config.mount_point;
    g_ignore_path = client_config.ignore_path;

    g_init_config.ignore_sighup = 1;
    g_init_config.ignore_sigterm = 1;
    g_init_config.log_dir = client_config.log_dir;
    g_init_config.log_level = client_config.log_level;
    g_init_config.prof_port = client_config.prof_port;

    if(g_ignore_path == NULL) {
        g_ignore_path = "";
    }

    char *mount_point = getenv("CFS_MOUNT_POINT");
    if(mount_point != NULL) {
        free((char *)g_mount_point);
        g_mount_point = mount_point;
    }

    if(g_mount_point == NULL || g_mount_point[0] != '/') {
        const char *msg = "Mount point is null or not an absolute path.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }
    char *path = get_clean_path(g_mount_point);
    // should not free the returned string from getenv()
    if(mount_point == NULL) {
        free((char *)g_mount_point);
    }
    g_mount_point = path;

    if(cfs_sdk_init(&g_init_config) != 0) {
        const char *msg= "Can't initialize CFS SDK, check the config file.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_cfs_client_id = cfs_new_client(NULL, g_config_path, NULL);
    if(g_cfs_client_id < 0) {
        const char *msg = "Can't start CFS client, check the config file.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_has_renameat2 = has_renameat2();
    pthread_rwlock_init(&g_inode_open_file_lock, NULL);
    g_big_page_cache = new_lru_cache(BIG_PAGE_CACHE_SIZE, BIG_PAGE_SIZE);
    g_small_page_cache = new_lru_cache(SMALL_PAGE_CACHE_SIZE, SMALL_PAGE_SIZE);
    bg_flush(&g_open_file);
    pthread_rwlock_init(&update_rwlock, NULL);
    g_need_rwlock = true;
    g_cfs_inited = true;

    pthread_t thread;
    if(pthread_create(&thread, NULL, &update_cfs_func, handle)) {
        const char *msg = "pthread_create error.\n";
        libc_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }
}

static void init_libc_func() {
    libc_open = (open_t)dlsym(RTLD_NEXT, "open");
    libc_openat = (openat_t)dlsym(RTLD_NEXT, "openat");
    libc_close = (close_t)dlsym(RTLD_NEXT, "close");
    libc_rename = (rename_t)dlsym(RTLD_NEXT, "rename");
    libc_renameat = (renameat_t)dlsym(RTLD_NEXT, "renameat");
    libc_renameat2 = (renameat2_t)dlsym(RTLD_NEXT, "renameat2");
    libc_truncate = (truncate_t)dlsym(RTLD_NEXT, "truncate");
    libc_ftruncate = (ftruncate_t)dlsym(RTLD_NEXT, "ftruncate");
    libc_fallocate = (fallocate_t)dlsym(RTLD_NEXT, "fallocate");
    libc_posix_fallocate = (posix_fallocate_t)dlsym(RTLD_NEXT, "posix_fallocate");

    libc_chdir = (chdir_t)dlsym(RTLD_NEXT, "chdir");
    libc_fchdir = (fchdir_t)dlsym(RTLD_NEXT, "fchdir");
    libc_getcwd = (getcwd_t)dlsym(RTLD_NEXT, "getcwd");
    libc_mkdir = (mkdir_t)dlsym(RTLD_NEXT, "mkdir");
    libc_mkdirat = (mkdirat_t)dlsym(RTLD_NEXT, "mkdirat");
    libc_rmdir = (rmdir_t)dlsym(RTLD_NEXT, "rmdir");
    libc_opendir = (opendir_t)dlsym(RTLD_NEXT, "opendir");
    libc_fdopendir = (fdopendir_t)dlsym(RTLD_NEXT, "fdopendir");
    libc_readdir = (readdir_t)dlsym(RTLD_NEXT, "readdir");
    libc_closedir = (closedir_t)dlsym(RTLD_NEXT, "closedir");
    libc_realpath = (realpath_t)dlsym(RTLD_NEXT, "realpath");

    libc_link = (link_t)dlsym(RTLD_NEXT, "link");
    libc_linkat = (linkat_t)dlsym(RTLD_NEXT, "linkat");
    libc_symlink = (symlink_t)dlsym(RTLD_NEXT, "symlink");
    libc_symlinkat = (symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");
    libc_unlink = (unlink_t)dlsym(RTLD_NEXT, "unlink");
    libc_unlinkat = (unlinkat_t)dlsym(RTLD_NEXT, "unlinkat");
    libc_readlink = (readlink_t)dlsym(RTLD_NEXT, "readlink");
    libc_readlinkat = (readlinkat_t)dlsym(RTLD_NEXT, "readlinkat");

    libc_stat = (stat_t)dlsym(RTLD_NEXT, "__xstat");
    libc_stat64 = (stat64_t)dlsym(RTLD_NEXT, "__xstat64");
    libc_lstat = (lstat_t)dlsym(RTLD_NEXT, "__lxstat");
    libc_lstat64 = (lstat64_t)dlsym(RTLD_NEXT, "__lxstat64");
    libc_fstat = (fstat_t)dlsym(RTLD_NEXT, "__fxstat");
    libc_fstat64 = (fstat64_t)dlsym(RTLD_NEXT, "__fxstat64");
    libc_fstatat = (fstatat_t)dlsym(RTLD_NEXT, "__fxstatat");
    libc_fstatat64 = (fstatat64_t)dlsym(RTLD_NEXT, "__fxstatat64");
    libc_chmod = (chmod_t)dlsym(RTLD_NEXT, "chmod");
    libc_fchmod = (fchmod_t)dlsym(RTLD_NEXT, "fchmod");
    libc_fchmodat = (fchmodat_t)dlsym(RTLD_NEXT, "fchmodat");
    libc_chown = (chown_t)dlsym(RTLD_NEXT, "chown");
    libc_lchown = (lchown_t)dlsym(RTLD_NEXT, "lchown");
    libc_fchown = (fchown_t)dlsym(RTLD_NEXT, "fchown");
    libc_fchownat = (fchownat_t)dlsym(RTLD_NEXT, "fchownat");
    libc_utime = (utime_t)dlsym(RTLD_NEXT, "utime");
    libc_utimes = (utimes_t)dlsym(RTLD_NEXT, "utimes");
    libc_futimesat = (futimesat_t)dlsym(RTLD_NEXT, "futimesat");
    libc_utimensat = (utimensat_t)dlsym(RTLD_NEXT, "utimensat");
    libc_futimens = (futimens_t)dlsym(RTLD_NEXT, "futimens");
    libc_access = (access_t)dlsym(RTLD_NEXT, "access");
    libc_faccessat = (faccessat_t)dlsym(RTLD_NEXT, "faccessat");

    libc_setxattr = (setxattr_t)dlsym(RTLD_NEXT, "setxattr");
    libc_lsetxattr = (lsetxattr_t)dlsym(RTLD_NEXT, "lsetxattr");
    libc_fsetxattr = (fsetxattr_t)dlsym(RTLD_NEXT, "fsetxattr");
    libc_getxattr = (getxattr_t)dlsym(RTLD_NEXT, "getxattr");
    libc_lgetxattr = (lgetxattr_t)dlsym(RTLD_NEXT, "lgetxattr");
    libc_fgetxattr = (fgetxattr_t)dlsym(RTLD_NEXT, "fgetxattr");
    libc_listxattr = (listxattr_t)dlsym(RTLD_NEXT, "listxattr");
    libc_llistxattr = (llistxattr_t)dlsym(RTLD_NEXT, "llistxattr");
    libc_flistxattr = (flistxattr_t)dlsym(RTLD_NEXT, "flistxattr");
    libc_removexattr = (removexattr_t)dlsym(RTLD_NEXT, "removexattr");
    libc_lremovexattr = (lremovexattr_t)dlsym(RTLD_NEXT, "lremovexattr");
    libc_fremovexattr = (fremovexattr_t)dlsym(RTLD_NEXT, "fremovexattr");

    libc_fcntl = (fcntl_t)dlsym(RTLD_NEXT, "fcntl");
    libc_dup2 = (dup2_t)dlsym(RTLD_NEXT, "dup2");
    libc_dup3 = (dup3_t)dlsym(RTLD_NEXT, "dup3");

    libc_read = (read_t)dlsym(RTLD_NEXT, "read");
    libc_readv = (readv_t)dlsym(RTLD_NEXT, "readv");
    libc_pread = (pread_t)dlsym(RTLD_NEXT, "pread");
    libc_preadv = (preadv_t)dlsym(RTLD_NEXT, "preadv");
    libc_write = (write_t)dlsym(RTLD_NEXT, "write");
    libc_writev = (writev_t)dlsym(RTLD_NEXT, "writev");
    libc_pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    libc_pwritev = (pwritev_t)dlsym(RTLD_NEXT, "pwritev");
    libc_lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
    libc_lseek64 = (lseek64_t)dlsym(RTLD_NEXT, "lseek64");

    libc_fdatasync = (fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
    libc_fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");

    libc_abort = (abort_t)dlsym(RTLD_NEXT, "abort");
    libc__exit = (_exit_t)dlsym(RTLD_NEXT, "_exit");
    libc_exit = (exit_t)dlsym(RTLD_NEXT, "exit");
}

static void init_cfs_func(void *handle) {
    cfs_sdk_init = (cfs_sdk_init_func)dlsym(handle, "cfs_sdk_init");
    cfs_sdk_close = (cfs_sdk_close_t)dlsym(handle, "cfs_sdk_close");
    cfs_new_client = (cfs_new_client_t)dlsym(handle, "cfs_new_client");
    cfs_close_client = (cfs_close_client_t)dlsym(handle, "cfs_close_client");
    cfs_client_state = (cfs_client_state_t)dlsym(handle, "cfs_client_state");
    cfs_flush_log = (cfs_flush_log_t)dlsym(handle, "cfs_flush_log");
    cfs_ump = (cfs_ump_t)dlsym(handle, "cfs_ump");

    cfs_close = (cfs_close_t)dlsym(handle, "cfs_close");
    cfs_open = (cfs_open_t)dlsym(handle, "cfs_open");
    cfs_openat = (cfs_openat_t)dlsym(handle, "cfs_openat");
    cfs_openat_fd = (cfs_openat_fd_t)dlsym(handle, "cfs_openat_fd");
    cfs_rename = (cfs_rename_t)dlsym(handle, "cfs_rename");
    cfs_renameat = (cfs_renameat_t)dlsym(handle, "cfs_renameat");
    cfs_truncate = (cfs_truncate_t)dlsym(handle, "cfs_truncate");
    cfs_ftruncate = (cfs_ftruncate_t)dlsym(handle, "cfs_ftruncate");
    cfs_fallocate = (cfs_fallocate_t)dlsym(handle, "cfs_fallocate");
    cfs_posix_fallocate = (cfs_posix_fallocate_t)dlsym(handle, "cfs_posix_fallocate");
    cfs_flush = (cfs_flush_t)dlsym(handle, "cfs_flush");
    cfs_get_file = (cfs_get_file_t)dlsym(handle, "cfs_get_file");

    cfs_chdir = (cfs_chdir_t)dlsym(handle, "cfs_chdir");
    cfs_fchdir = (cfs_fchdir_t)dlsym(handle, "cfs_fchdir");
    cfs_getcwd = (cfs_getcwd_t)dlsym(handle, "cfs_getcwd");
    cfs_mkdirs = (cfs_mkdirs_t)dlsym(handle, "cfs_mkdirs");
    cfs_mkdirsat = (cfs_mkdirsat_t)dlsym(handle, "cfs_mkdirsat");
    cfs_rmdir = (cfs_rmdir_t)dlsym(handle, "cfs_rmdir");
    cfs_getdents = (cfs_getdents_t)dlsym(handle, "cfs_getdents");

    cfs_link = (cfs_link_t)dlsym(handle, "cfs_link");
    cfs_linkat = (cfs_linkat_t)dlsym(handle, "cfs_linkat");
    cfs_symlink = (cfs_symlink_t)dlsym(handle, "cfs_symlink");
    cfs_symlinkat = (cfs_symlinkat_t)dlsym(handle, "cfs_symlinkat");
    cfs_unlink = (cfs_unlink_t)dlsym(handle, "cfs_unlink");
    cfs_unlinkat = (cfs_unlinkat_t)dlsym(handle, "cfs_unlinkat");
    cfs_readlink = (cfs_readlink_t)dlsym(handle, "cfs_readlink");
    cfs_readlinkat = (cfs_readlinkat_t)dlsym(handle, "cfs_readlinkat");

    cfs_stat = (cfs_stat_t)dlsym(handle, "cfs_stat");
    cfs_stat64 = (cfs_stat64_t)dlsym(handle, "cfs_stat64");
    cfs_lstat = (cfs_lstat_t)dlsym(handle, "cfs_lstat");
    cfs_lstat64 = (cfs_lstat64_t)dlsym(handle, "cfs_lstat64");
    cfs_fstat = (cfs_fstat_t)dlsym(handle, "cfs_fstat");
    cfs_fstat64 = (cfs_fstat64_t)dlsym(handle, "cfs_fstat64");
    cfs_fstatat = (cfs_fstatat_t)dlsym(handle, "cfs_fstatat");
    cfs_fstatat64 = (cfs_fstatat64_t)dlsym(handle, "cfs_fstatat64");
    cfs_chmod = (cfs_chmod_t)dlsym(handle, "cfs_chmod");
    cfs_fchmod = (cfs_fchmod_t)dlsym(handle, "cfs_fchmod");
    cfs_fchmodat = (cfs_fchmodat_t)dlsym(handle, "cfs_fchmodat");
    cfs_chown = (cfs_chown_t)dlsym(handle, "cfs_chown");
    cfs_lchown = (cfs_lchown_t)dlsym(handle, "cfs_lchown");
    cfs_fchown = (cfs_fchown_t)dlsym(handle, "cfs_fchown");
    cfs_fchownat = (cfs_fchownat_t)dlsym(handle, "cfs_fchownat");
    cfs_futimens = (cfs_futimens_t)dlsym(handle, "cfs_futimens");
    cfs_utimens = (cfs_utimens_t)dlsym(handle, "cfs_utimens");
    cfs_utimensat = (cfs_utimensat_t)dlsym(handle, "cfs_utimensat");
    cfs_access = (cfs_access_t)dlsym(handle, "cfs_access");
    cfs_faccessat = (cfs_faccessat_t)dlsym(handle, "cfs_faccessat");

    cfs_setxattr = (cfs_setxattr_t)dlsym(handle, "cfs_setxattr");
    cfs_lsetxattr = (cfs_lsetxattr_t)dlsym(handle, "cfs_lsetxattr");
    cfs_fsetxattr = (cfs_fsetxattr_t)dlsym(handle, "cfs_fsetxattr");
    cfs_getxattr = (cfs_getxattr_t)dlsym(handle, "cfs_getxattr");
    cfs_lgetxattr = (cfs_lgetxattr_t)dlsym(handle, "cfs_lgetxattr");
    cfs_fgetxattr = (cfs_fgetxattr_t)dlsym(handle, "cfs_fgetxattr");
    cfs_listxattr = (cfs_listxattr_t)dlsym(handle, "cfs_listxattr");
    cfs_llistxattr = (cfs_llistxattr_t)dlsym(handle, "cfs_llistxattr");
    cfs_flistxattr = (cfs_flistxattr_t)dlsym(handle, "cfs_flistxattr");
    cfs_removexattr = (cfs_removexattr_t)dlsym(handle, "cfs_removexattr");
    cfs_lremovexattr = (cfs_lremovexattr_t)dlsym(handle, "cfs_lremovexattr");
    cfs_fremovexattr = (cfs_fremovexattr_t)dlsym(handle, "cfs_fremovexattr");

    cfs_fcntl = (cfs_fcntl_t)dlsym(handle, "cfs_fcntl");
    cfs_fcntl_lock = (cfs_fcntl_lock_t)dlsym(handle, "cfs_fcntl_lock");

    cfs_read = (cfs_read_t)dlsym(handle, "cfs_read");
    cfs_pread = (cfs_pread_t)dlsym(handle, "cfs_pread");
    cfs_readv = (cfs_readv_t)dlsym(handle, "cfs_readv");
    cfs_preadv = (cfs_preadv_t)dlsym(handle, "cfs_preadv");
    cfs_write = (cfs_write_t)dlsym(handle, "cfs_write");
    cfs_pwrite = (cfs_pwrite_t)dlsym(handle, "cfs_pwrite");
    cfs_writev = (cfs_writev_t)dlsym(handle, "cfs_writev");
    cfs_pwritev = (cfs_pwritev_t)dlsym(handle, "cfs_pwritev");
    cfs_lseek = (cfs_lseek_t)dlsym(handle, "cfs_lseek");
}

static void *update_cfs_func(void* param) {
    #define INTERVAL 6
    #define VERSIONLEN 1
    char* reload;
    void* old_handle = param;
    char* envPtr = strdup(getenv("LD_PRELOAD"));

    while(1) {
        sleep(INTERVAL);
        reload = getenv("RELOAD_CLIENT");
        if (reload == NULL || strcmp(reload, "1") != 0)
            continue;

        g_need_rwlock = true;
        pthread_rwlock_wrlock(&update_rwlock);
        if (reload == NULL || strcmp(reload, "1") != 0) {
            pthread_rwlock_unlock(&update_rwlock);
            continue;
        }

        fprintf(stderr, "Begin to update client.\n");
        char *client_state = NULL;
        size_t size = cfs_client_state(g_cfs_client_id, NULL, 0);
        if (size > 0) {
            client_state = (char *)malloc(size);
            if(client_state == NULL) {
                fprintf(stderr, "fail to malloc size\n: %d", size);
                pthread_rwlock_unlock(&update_rwlock);
                exit(1);
            }
            memset(client_state, '\0', size);
            cfs_client_state(g_cfs_client_id, client_state, size);
        }

        cfs_sdk_close();
        plugClose(old_handle);
        setenv("LD_PRELOAD", envPtr, 1);

        void *handle = plugOpen("/usr/lib64/libcfssdk.so");
        old_handle = handle;
        init_cfs_func(handle);

        if(cfs_sdk_init(&g_init_config) != 0) {
            const char *msg= "Can't initialize CFS SDK, check the config file.\n";
            libc_write(STDOUT_FILENO, msg, strlen(msg));
            exit(1);
        }
        int64_t new_client = cfs_new_client(NULL, g_config_path, client_state);
        if(new_client < 0) {
            pthread_rwlock_unlock(&update_rwlock);
            exit(1);
        }
        free(client_state);
        g_cfs_client_id = new_client;
        fprintf(stderr, "Finish to update client.\n");
        pthread_rwlock_unlock(&update_rwlock);
    }
    free(envPtr);
}
