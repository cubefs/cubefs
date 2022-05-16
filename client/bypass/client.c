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

#define _GNU_SOURCE

#include <dlfcn.h>
#include "client.h"


/*
 * File operations
 */

int close(int fd) {
    if(!g_cfs_inited) {
        real_close = dlsym(RTLD_NEXT, "close");
        return real_close(fd);
    }

    int is_cfs = fd & CFS_FD_MASK;
    int re = 0;
    if(g_hook && is_cfs) {
        int in_use = 0;
        #ifdef _CFS_BASH
        for (int i = 0; i < CFS_FD_MAP_SIZE; i++) {
            if(g_cfs_fd_map[i] == fd) {
                in_use = 1;
            }
        }
        #endif
        if(!in_use) {
            #ifdef DUP_TO_LOCAL
            re = real_close(fd & ~CFS_FD_MASK);
            if(re < 0) {
                goto log;
            }
            #endif
            re = cfs_re(cfs_close(g_cfs_client_id, fd & ~CFS_FD_MASK));
        }
    } else {
        re = real_close(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd & ~CFS_FD_MASK, entry && entry->data ? (char *)entry->data : "", re);
    if(entry) {
        free(entry->data);
        entry->data = NULL;
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
        real_openat = dlsym(RTLD_NEXT, "openat");
        return real_openat(dirfd, pathname, flags, mode);
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
        fd = real_openat(dirfd, pathname, flags, mode);
        if(fd < 0) {
            goto log;
        }
        fd = cfs_re(cfs_openat_fd(g_cfs_client_id, dirfd, cfs_path, flags, mode, fd));
        #else
        fd = cfs_re(cfs_openat(g_cfs_client_id, dirfd, cfs_path, flags, mode));
        #endif
    } else {
        fd = real_openat(dirfd, pathname, flags, mode);
    }

    if(fd < 0) {
        goto log;
    }

    if(fd & CFS_FD_MASK) {
        if(g_hook && is_cfs) {
            cfs_close(g_cfs_client_id, fd);
        } else {
            real_close(fd);
        }
        fd = -1;
        goto log;
    }

log:
    free(path);
    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    addFdEntry(fd, strdup(pathname));
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
            real_renameat2 = dlsym(RTLD_NEXT, "renameat2");
            return real_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            real_renameat = dlsym(RTLD_NEXT, "renameat");
            return real_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
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
            if(!cfs_faccessat(g_cfs_client_id, newdirfd, cfs_new_path, F_OK, 0)) {
                errno = ENOTEMPTY;
                goto log;
            }
        } else if(flags) {
            // other flags unimplemented
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        if(g_has_renameat2) {
            re = real_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            re = real_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
        }
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_renameat(g_cfs_client_id, olddirfd, cfs_old_path, newdirfd, cfs_new_path));
    } else if(!g_hook || (!is_cfs_old && !is_cfs_new)) {
        if(g_has_renameat2) {
            re = real_renameat2(olddirfd, old_pathname, newdirfd, new_pathname, flags);
        } else {
            re = real_renameat(olddirfd, old_pathname, newdirfd, new_pathname);
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
        real_truncate = dlsym(RTLD_NEXT, "truncate");
        return real_truncate(pathname, length);
    }

    char *path = get_cfs_path(pathname);
    int re;
    if(g_hook && path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = real_truncate(pathname, length);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_truncate(g_cfs_client_id, path, length));
    } else {
        re = real_truncate(pathname, length);
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
        real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");
        return real_ftruncate(fd, length);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_ftruncate(fd, length);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_ftruncate(g_cfs_client_id, fd, length));
    } else {
        re = real_ftruncate(fd, length);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, length:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", length, re);
    #endif
    return re;
}
weak_alias (ftruncate, ftruncate64)

int fallocate(int fd, int mode, off_t offset, off_t len) {
    if(!g_cfs_inited) {
        real_fallocate = dlsym(RTLD_NEXT, "fallocate");
        return real_fallocate(fd, mode, offset, len);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_fallocate(fd, mode, offset, len);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_fallocate(g_cfs_client_id, fd, mode, offset, len));
    } else {
        re = real_fallocate(fd, mode, offset, len);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, mode:%#X, offset:%d, len:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", mode, offset, len, re);
    #endif
    return re;
}
weak_alias (fallocate, fallocate64)

int posix_fallocate(int fd, off_t offset, off_t len) {
    if(!g_cfs_inited) {
        real_posix_fallocate = dlsym(RTLD_NEXT, "posix_fallocate");
        return real_posix_fallocate(fd, offset, len);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_posix_fallocate(fd, offset, len);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_posix_fallocate(g_cfs_client_id, fd, offset, len));
    } else {
        re = real_posix_fallocate(fd, offset, len);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, offset:%d, len:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", offset, len, re);
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
        real_mkdirat = dlsym(RTLD_NEXT, "mkdirat");
        return real_mkdirat(dirfd, pathname, mode);
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
        re = real_mkdirat(dirfd, pathname, mode);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_mkdirsat(g_cfs_client_id, dirfd, cfs_path, mode));
    } else {
        re = real_mkdirat(dirfd, pathname, mode);
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
        real_rmdir = dlsym(RTLD_NEXT, "rmdir");
        return real_rmdir(pathname);
    }

    char *path = get_cfs_path(pathname);
    int re;
    if(g_hook && path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = real_rmdir(pathname);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_rmdir(g_cfs_client_id, path));
    } else {
        re = real_rmdir(pathname);
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
        real_getcwd = dlsym(RTLD_NEXT, "getcwd");
        return real_getcwd(buf, size);
    }

    char *re = NULL;
    if(buf != NULL && size == 0) {
        errno = EINVAL;
        goto log;
    }

    if(g_cwd == NULL) {
        char *cwd = real_getcwd(buf, size);
        if(cwd == NULL) {
            goto log;
        }
        // Always duplicate cwd enven if cwd is malloc 'ed by real_getcwd, 
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

    int len_mount = 0;
    // If g_cwd="/" ignore the backslash
    int len_cwd = strcmp(g_cwd, "/") ? strlen(g_cwd) : 0;
    int len = len_cwd;
    if(g_in_cfs) {
        len_mount = strlen(g_mount_point);
        len += len_mount;
    }
    if(size > 0 && size < len+1) {
        errno = ENAMETOOLONG;
        goto log;
    }

    int alloc_size = size;
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
        real_chdir = dlsym(RTLD_NEXT, "chdir");
        return real_chdir(pathname);
    }

    int re = -1;
    char *clean_path = get_clean_path(pathname);
    if(clean_path == NULL) {
        goto log;
    }

    char *abs_path = clean_path;
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

    char *cfs_path = get_cfs_path(abs_path);
    if(g_hook && cfs_path != NULL) {
        #ifdef DUP_TO_LOCAL
        re = real_chdir(abs_path);
        if(re < 0) {
            free(abs_path);
            goto log;
        }
        #endif
        free(abs_path);
        re = cfs_re(cfs_chdir(g_cfs_client_id, cfs_path));
        if(re == 0) {
            g_in_cfs = true;
            free(g_cwd);
            g_cwd = cfs_path;
        } else {
            free(cfs_path);
        }
    } else {
        free(cfs_path);
        re = real_chdir(abs_path);
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
        real_fchdir = dlsym(RTLD_NEXT, "fchdir");
        return real_fchdir(fd);
    }

    int re = -1;
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    if(!g_hook || !is_cfs) {
        re = real_fchdir(fd);
        g_in_cfs = false;
        free(g_cwd);
        g_cwd = NULL;
        goto log;
    }

    #ifdef DUP_TO_LOCAL
    re = real_fchdir(fd);
    if(re < 0) {
        goto log;
    }
    #endif
    char *buf = (char *) malloc(PATH_MAX);
    re = cfs_re(cfs_fchdir(g_cfs_client_id, fd, buf, PATH_MAX));
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
        real_opendir = dlsym(RTLD_NEXT, "opendir");
        return real_opendir(pathname);
    }

    char *path = get_cfs_path(pathname);
    if(!g_hook || path == NULL) {
        free(path);
        return real_opendir(pathname);
    }
    int fd = cfs_openat(g_cfs_client_id, AT_FDCWD, path, O_RDONLY | O_DIRECTORY, 0);
    free(path);

    if(fd < 0) {
        return NULL;
    }
    if(fd & CFS_FD_MASK) {
        cfs_close(g_cfs_client_id, fd);
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
        real_fdopendir = dlsym(RTLD_NEXT, "fdopendir");
        return real_fdopendir(fd);
    }

    int is_cfs = fd & CFS_FD_MASK;
    if(!g_hook || !is_cfs) {
        return real_fdopendir(fd);
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
        real_readdir = dlsym(RTLD_NEXT, "readdir");
        return real_readdir(dirp);
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
        return real_readdir(dirp);
    }

    struct dirent *dp;
    if(dirp->offset >= dirp->size) {
        int fd = dirp->fd & ~CFS_FD_MASK;
        int count = cfs_getdents(g_cfs_client_id, fd, dirp->data, dirp->allocation);
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
        real_closedir = dlsym(RTLD_NEXT, "closedir");
        return real_closedir(dirp);
    }

    if(dirp == NULL) {
        errno = EBADF;
        return -1;
    }

    int is_cfs = dirp->fd & CFS_FD_MASK;
    dirp->fd &= ~CFS_FD_MASK;
    int re;
    if(!g_hook || !is_cfs) {
        re = real_closedir(dirp);
    } else {
        re = cfs_re(cfs_close(g_cfs_client_id, dirp->fd));
        free(dirp);
    }
    return re;
}

char *realpath(const char *path, char *resolved_path) {
    if(!g_cfs_inited) {
        real_realpath = dlsym(RTLD_NEXT, "realpath");
        return real_realpath(path, resolved_path);
    }

    char *re = NULL;
    char *clean_path = get_clean_path(path);
    if(clean_path == NULL) {
        goto log;
    }

    char *abs_path = clean_path;
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
        real_linkat = dlsym(RTLD_NEXT, "linkat");
        return real_linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags);
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
        re = cfs_re(cfs_linkat(g_cfs_client_id, olddirfd, cfs_old_path, newdirfd, cfs_new_path, flags));
    } else if(!g_hook || (!is_cfs_old && !is_cfs_new)) {
        re = real_linkat(olddirfd, old_pathname, newdirfd, new_pathname, flags);
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
        real_symlinkat = dlsym(RTLD_NEXT, "symlinkat");
        return real_symlinkat(target, dirfd, linkpath);
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
        re = cfs_re(cfs_symlinkat(g_cfs_client_id, t, dirfd, (path == NULL) ? linkpath : path));
    } else if(!g_hook || !is_cfs) {
        re = real_symlinkat(target, dirfd, linkpath);
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
        real_unlinkat = dlsym(RTLD_NEXT, "unlinkat");
        return real_unlinkat(dirfd, pathname, flags);
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
        re = real_unlinkat(dirfd, pathname, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_unlinkat(g_cfs_client_id, dirfd, cfs_path, flags));
    } else {
        re = real_unlinkat(dirfd, pathname, flags);
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
        real_readlinkat = dlsym(RTLD_NEXT, "readlinkat");
        return real_readlinkat(dirfd, pathname, buf, size);
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
    ssize_t re = g_hook && is_cfs ? cfs_sre(cfs_readlinkat(g_cfs_client_id, dirfd, cfs_path, buf, size)) :
           real_readlinkat(dirfd, pathname, buf, size);
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
        real_stat = dlsym(RTLD_NEXT, "__xstat");
        return real_stat(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_stat(g_cfs_client_id, path, statbuf)) : real_stat(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __xstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        real_stat64 = dlsym(RTLD_NEXT, "__xstat64");
        return real_stat64(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_stat64(g_cfs_client_id, path, statbuf)) : real_stat64(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __lxstat(int ver, const char *pathname, struct stat *statbuf) {
    if(!g_cfs_inited) {
        real_lstat = dlsym(RTLD_NEXT, "__lxstat");
        return real_lstat(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_lstat(g_cfs_client_id, path, statbuf)) : real_lstat(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __lxstat64(int ver, const char *pathname, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        real_lstat64 = dlsym(RTLD_NEXT, "__lxstat64");
        return real_lstat64(ver, pathname, statbuf);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_lstat64(g_cfs_client_id, path, statbuf)) :
             real_lstat64(ver, pathname, statbuf);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, pathname:%s, re:%d\n", __func__, path != NULL, pathname, re);
    #endif
    return re;
}

int __fxstat(int ver, int fd, struct stat *statbuf) {
    if(!g_cfs_inited) {
        real_fstat = dlsym(RTLD_NEXT, "__fxstat");
        return real_fstat(ver, fd, statbuf);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re = g_hook && is_cfs ? cfs_re(cfs_fstat(g_cfs_client_id, fd, statbuf)) : real_fstat(ver, fd, statbuf);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, fd:%d, re:%d\n", __func__, is_cfs > 0, fd, re);
    #endif
    return re;
}

int __fxstat64(int ver, int fd, struct stat64 *statbuf) {
    if(!g_cfs_inited) {
        real_fstat64 = dlsym(RTLD_NEXT, "__fxstat64");
        return real_fstat64(ver, fd, statbuf);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re = g_hook && is_cfs ? cfs_re(cfs_fstat64(g_cfs_client_id, fd, statbuf)) :
        real_fstat64(ver, fd, statbuf);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, fd:%d, re:%d\n", __func__, is_cfs > 0, fd, re);
    #endif
    return re;
}

int __fxstatat(int ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) {
    if(!g_cfs_inited) {
        real_fstatat = dlsym(RTLD_NEXT, "__fxstatat");
        return real_fstatat(ver, dirfd, pathname, statbuf, flags);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_fstatat(g_cfs_client_id, dirfd, cfs_path, statbuf, flags)) :
             real_fstatat(ver, dirfd, pathname, statbuf, flags);
    free(path);
    #ifdef _CFS_DEBUG
    log_debug("hook %s, dirfd:%d, pathname:%s, re:%d\n", __func__, dirfd, pathname, re);
    #endif
    return re;
}

int __fxstatat64(int ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) {
    if(!g_cfs_inited) {
        real_fstatat64 = dlsym(RTLD_NEXT, "__fxstatat64");
        return real_fstatat64(ver, dirfd, pathname, statbuf, flags);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_fstatat64(g_cfs_client_id, dirfd, cfs_path, statbuf, flags)) :
        real_fstatat64(ver, dirfd, pathname, statbuf, flags);
    free(path);
    return re;
}

int chmod(const char *pathname, mode_t mode) {
    return fchmodat(AT_FDCWD, pathname, mode, 0);
}

int fchmod(int fd, mode_t mode) {
    if(!g_cfs_inited) {
        real_fchmod = dlsym(RTLD_NEXT, "fchmod");
        return real_fchmod(fd, mode);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return is_cfs ? cfs_re(cfs_fchmod(g_cfs_client_id, fd, mode)) : real_fchmod(fd, mode);
}

int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags) {
    if(!g_cfs_inited) {
        real_fchmodat = dlsym(RTLD_NEXT, "fchmodat");
        return real_fchmodat(dirfd, pathname, mode, flags);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_fchmodat(g_cfs_client_id, dirfd, cfs_path, mode, flags)) : 
        real_fchmodat(dirfd, pathname, mode, flags);
    free(path);
    return re;
}

int chown(const char *pathname, uid_t owner, gid_t group) {
    return fchownat(AT_FDCWD, pathname, owner, group, 0);
}

int lchown(const char *pathname, uid_t owner, gid_t group) {
    if(!g_cfs_inited) {
        real_lchown = dlsym(RTLD_NEXT, "lchown");
        return real_lchown(pathname, owner, group);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_lchown(g_cfs_client_id, path, owner, group)) :
             real_lchown(pathname, owner, group);
    free(path);
    return re;
}

int fchown(int fd, uid_t owner, gid_t group) {
    if(!g_cfs_inited) {
        real_fchown = dlsym(RTLD_NEXT, "fchown");
        return real_fchown(fd, owner, group);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_re(cfs_fchown(g_cfs_client_id, fd, owner, group)) :
        real_fchown(fd, owner, group);
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) {
    if(!g_cfs_inited) {
        real_fchownat = dlsym(RTLD_NEXT, "fchownat");
        return real_fchownat(dirfd, pathname, owner, group, flags);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_fchownat(g_cfs_client_id, dirfd, cfs_path, owner, group, flags)) :
        real_fchownat(dirfd, pathname, owner, group, flags);
    free(path);
    return re;
}

int utime(const char *pathname, const struct utimbuf *times) {
    if(!g_cfs_inited) {
        real_utime = dlsym(RTLD_NEXT, "utime");
        return real_utime(pathname, times);
    }

    struct timespec *pts;
    if(times != NULL) {
        struct timespec ts[2] = {times->actime, 0, times->modtime, 0};
        pts = & ts[0];
    }
    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_utimens(g_cfs_client_id, path, pts, 0)) :
            real_utime(pathname, times);
    free(path);
    return re;
}

int utimes(const char *pathname, const struct timeval *times) {
    if(!g_cfs_inited) {
        real_utimes = dlsym(RTLD_NEXT, "utimes");
        return real_utimes(pathname, times);
    }

    struct timespec *pts;
    if(times != NULL) {
        struct timespec ts[2] = {times[0].tv_sec, times[0].tv_usec*1000, times[1].tv_sec, times[1].tv_usec*1000};
        pts = & ts[0];
    }
    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_utimens(g_cfs_client_id, path, pts, 0)) :
            real_utimes(pathname, times);
    free(path);
    return re;
}

int futimesat(int dirfd, const char *pathname, const struct timeval times[2]) {
    if(!g_cfs_inited) {
        real_futimesat = dlsym(RTLD_NEXT, "futimesat");
        return real_futimesat(dirfd, pathname, times);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_utimensat(g_cfs_client_id, dirfd, cfs_path, pts, 0)) :
        real_futimesat(dirfd, pathname, times);
    free(path);
    return re;
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags) {
    if(!g_cfs_inited) {
        real_utimensat = dlsym(RTLD_NEXT, "utimensat");
        return real_utimensat(dirfd, pathname, times, flags);
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
    int re = g_hook && is_cfs ? cfs_re(cfs_utimensat(g_cfs_client_id, dirfd, cfs_path, times, flags)) :
        real_utimensat(dirfd, pathname, times, flags);
    free(path);
    return re;
}

int futimens(int fd, const struct timespec times[2]) {
    if(!g_cfs_inited) {
        real_futimens = dlsym(RTLD_NEXT, "futimens");
        return real_futimens(fd, times);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_re(cfs_futimens(g_cfs_client_id, fd, times)) : real_futimens(fd, times);
}

int access(const char *pathname, int mode) {
    return faccessat(AT_FDCWD, pathname, mode, 0);
}

int faccessat(int dirfd, const char *pathname, int mode, int flags) {
    if(!g_cfs_inited) {
        real_faccessat = dlsym(RTLD_NEXT, "faccessat");
        return real_faccessat(dirfd, pathname, mode, flags);
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
        re = real_faccessat(dirfd, pathname, mode, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_faccessat(g_cfs_client_id, dirfd, cfs_path, mode, flags));
    } else {
        re = real_faccessat(dirfd, pathname, mode, flags);
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
        real_setxattr = dlsym(RTLD_NEXT, "setxattr");
        return real_setxattr(pathname, name, value, size, flags);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_setxattr(g_cfs_client_id, path, name, value, size, flags)) :
             real_setxattr(pathname, name, value, size, flags);
    free(path);
    return re;
}

int lsetxattr(const char *pathname, const char *name,
             const void *value, size_t size, int flags) {
    if(!g_cfs_inited) {
        real_lsetxattr = dlsym(RTLD_NEXT, "lsetxattr");
        return real_lsetxattr(pathname, name, value, size, flags);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_lsetxattr(g_cfs_client_id, path, name, value, size, flags)) :
             real_lsetxattr(pathname, name, value, size, flags);
    free(path);
    return re;
}

int fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) {
    if(!g_cfs_inited) {
        real_fsetxattr = dlsym(RTLD_NEXT, "fsetxattr");
        return real_fsetxattr(fd, name, value, size, flags);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_re(cfs_fsetxattr(g_cfs_client_id, fd, name, value, size, flags)) :
           real_fsetxattr(fd, name, value, size, flags);
}

ssize_t getxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        real_getxattr = dlsym(RTLD_NEXT, "getxattr");
        return real_getxattr(pathname, name, value, size);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_sre(cfs_getxattr(g_cfs_client_id, path, name, value, size)) :
             real_getxattr(pathname, name, value, size);
    free(path);
    return re;
}

ssize_t lgetxattr(const char *pathname, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        real_lgetxattr = dlsym(RTLD_NEXT, "lgetxattr");
        return real_lgetxattr(pathname, name, value, size);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_sre(cfs_lgetxattr(g_cfs_client_id, path, name, value, size)) :
             real_lgetxattr(pathname, name, value, size);
    free(path);
    return re;
}

ssize_t fgetxattr(int fd, const char *name, void *value, size_t size) {
    if(!g_cfs_inited) {
        real_fgetxattr = dlsym(RTLD_NEXT, "fgetxattr");
        return real_fgetxattr(fd, name, value, size);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_sre(cfs_fgetxattr(g_cfs_client_id, fd, name, value, size)) :
           real_fgetxattr(fd, name, value, size);
}

ssize_t listxattr(const char *pathname, char *list, size_t size) {
    if(!g_cfs_inited) {
        real_listxattr = dlsym(RTLD_NEXT, "listxattr");
        return real_listxattr(pathname, list, size);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_sre(cfs_listxattr(g_cfs_client_id, path, list, size)) :
             real_listxattr(pathname, list, size);
    free(path);
    return re;
}

ssize_t llistxattr(const char *pathname, char *list, size_t size) {
    if(!g_cfs_inited) {
        real_llistxattr = dlsym(RTLD_NEXT, "llistxattr");
        return real_llistxattr(pathname, list, size);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_sre(cfs_llistxattr(g_cfs_client_id, path, list, size)) :
             real_llistxattr(pathname, list, size);
    free(path);
    return re;
}

ssize_t flistxattr(int fd, char *list, size_t size) {
    if(!g_cfs_inited) {
        real_flistxattr = dlsym(RTLD_NEXT, "flistxattr");
        return real_flistxattr(fd, list, size);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_sre(cfs_flistxattr(g_cfs_client_id, fd, list, size)) :
           real_flistxattr(fd, list, size);
}

int removexattr(const char *pathname, const char *name) {
    if(!g_cfs_inited) {
        real_removexattr = dlsym(RTLD_NEXT, "removexattr");
        return real_removexattr(pathname, name);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_removexattr(g_cfs_client_id, path, name)) :
             real_removexattr(pathname, name);
    free(path);
    return re;
}

int lremovexattr(const char *pathname, const char *name) {
    if(!g_cfs_inited) {
        real_lremovexattr = dlsym(RTLD_NEXT, "lremovexattr");
        return real_lremovexattr(pathname, name);
    }

    char *path = get_cfs_path(pathname);
    int re = (g_hook && path != NULL) ? cfs_re(cfs_lremovexattr(g_cfs_client_id, path, name)) :
             real_lremovexattr(pathname, name);
    free(path);
    return re;
}

int fremovexattr(int fd, const char *name) {
    if(!g_cfs_inited) {
        real_fremovexattr = dlsym(RTLD_NEXT, "fremovexattr");
        return real_fremovexattr(fd, name);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    return g_hook && is_cfs ? cfs_re(cfs_fremovexattr(g_cfs_client_id, fd, name)) :
           real_fremovexattr(fd, name);
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
        real_fcntl = dlsym(RTLD_NEXT, "fcntl");
        return real_fcntl(fd, cmd, arg);
    }

    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re, re_old;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_fcntl(fd, cmd, arg);
        if(re < 0) {
            goto log;
        }
        if(cmd == F_SETLK || cmd == F_SETLKW) {
            re = cfs_fcntl_lock(g_cfs_client_id, fd, cmd, (struct flock *)arg);
        } else {
            re_old = re;
            re = cfs_fcntl(g_cfs_client_id, fd, cmd, 
            (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) ? re : (intptr_t)arg);
            if(re != re_old) {
                goto log;
            }
        }
        #else
        if(cmd == F_SETLK || cmd == F_SETLKW) {
            re = cfs_fcntl_lock(g_cfs_client_id, fd, cmd, (struct flock *)arg);
        } else {
            re = cfs_fcntl(g_cfs_client_id, fd, cmd, (intptr_t)arg);
        }
        #endif
        re = cfs_re(re);
    } else {
        re = real_fcntl(fd, cmd, arg);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    char *cmdstr;
    switch(cmd) {
        case F_DUPFD:
        cmdstr = "F_DUPFD";
        break;
        case F_DUPFD_CLOEXEC:
        cmdstr = "F_DUPFD_CLOEXEC";
        break;
        case F_GETFD:
        cmdstr = "F_GETFD";
        break;
        case F_SETFD:
        cmdstr = "F_SETFD";
        break;
        case F_GETFL:
        cmdstr = "F_GETFL";
        break;
        case F_SETFL:
        cmdstr = "F_SETFL";
        break;
        case F_SETLK:
        cmdstr = "F_SETLK";
        break;
        case F_SETLKW:
        cmdstr = "F_SETLKW";
        break;
        case F_GETLK:
        cmdstr = "F_GETLK";
        break;
    }
    log_debug("hook %s, is_cfs:%d, fd:%d, cmd:%d(%s), arg:%u(%s), re:%d, re_old:%d\n", __func__, is_cfs>0, fd, cmd, 
    cmdstr, (intptr_t)arg, (cmd==F_SETFL&&(intptr_t)arg&O_DIRECT)?"O_DIRECT":"", re, re_old);
    #endif
    if(g_hook && is_cfs && (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC)) {
        re |= CFS_FD_MASK;
    }
    return re;
}
weak_alias (fcntl, fcntl64)

#ifdef _CFS_BASH
int dup2(int oldfd, int newfd) {
    if(!g_cfs_inited) {
        real_dup2 = dlsym(RTLD_NEXT, "dup2");
        return real_dup2(oldfd, newfd);
    }

    // If newfd was open, close it before being reused
    if(newfd >= 0 && newfd < CFS_FD_MAP_SIZE && g_cfs_fd_map[newfd] > 0) {
        cfs_close(g_cfs_client_id, g_cfs_fd_map[newfd] & ~CFS_FD_MASK);
        g_cfs_fd_map[newfd] = 0;
    }

    int is_cfs = oldfd & CFS_FD_MASK;
    int re = -1;
    if(g_hook && is_cfs) {
        if(newfd < 0 || newfd >= CFS_FD_MAP_SIZE) {
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        re = real_dup2(oldfd & ~CFS_FD_MASK, newfd);
        if(re < 0) {
            goto log;
        }
        #endif
        g_cfs_fd_map[newfd] = oldfd;
        re = newfd;
    } else {
        re = real_dup2(oldfd, newfd);
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, oldfd:%d, newfd:%d, re:%d\n", __func__, is_cfs > 0, oldfd, newfd, re);
    #endif
    return re;
}

int dup3(int oldfd, int newfd, int flags) {
    if(!g_cfs_inited) {
        real_dup3 = dlsym(RTLD_NEXT, "dup3");
        return real_dup3(oldfd, newfd, flags);
    }

    // If newfd was open, close it before being reused
    if(newfd >= 0 && newfd < CFS_FD_MAP_SIZE && g_cfs_fd_map[newfd] > 0) {
        cfs_close(g_cfs_client_id, g_cfs_fd_map[newfd] & ~CFS_FD_MASK);
        g_cfs_fd_map[newfd] = 0;
    }

    int is_cfs = oldfd & CFS_FD_MASK;
    int re = -1;
    if(g_hook && is_cfs) {
        if(newfd < 0 || newfd >= CFS_FD_MAP_SIZE) {
            goto log;
        }
        #ifdef DUP_TO_LOCAL
        re = real_dup3(oldfd & ~CFS_FD_MASK, newfd, flags);
        if(re < 0) {
            goto log;
        }
        #endif
        g_cfs_fd_map[newfd] = oldfd;
        re = newfd;
    } else {
        re = real_dup3(oldfd, newfd, flags);
    }

log:
    #ifdef _CFS_DEBUG
    log_debug("hook %s, is_cfs:%d, oldfd:%d, newfd:%d, flags:%#x, re:%d\n", __func__, is_cfs > 0, oldfd, newfd, flags, re);
    #endif
    return re;
}
#endif


/*
 * Read & Write
 */

ssize_t read(int fd, void *buf, size_t count) {
    if(!g_cfs_inited) {
        real_read = dlsym(RTLD_NEXT, "read");
        return real_read(fd, buf, count);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif

    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    off_t offset = lseek(fd, 0, SEEK_CUR);
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re, re_cfs;

    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_read(fd, buf, count);
        if(re <= 0) {
            goto log;
        }
        void *buf_copy = malloc(count);
        if(buf_copy == NULL) {
            re = -1;
            goto log;
        }
        // Reading from local and CFS may be concurrent with writing to local and CFS.
        // There are two conditions in which data read from local and CFS may be different.
        // 1. read local -> write local -> write CFS -> read CFS
        // 2. write local -> read local -> read CFS -> write CFS
        // In contition 2, write CFS may be concurrent with read CFS, resulting in last bytes read being zero.
        re_cfs = cfs_sre(cfs_read(g_cfs_client_id, fd, buf_copy, re));
        if(re_cfs <= 0) {
            goto log;
        }
        if(memcmp(buf, buf_copy, re_cfs)) {
            ENTRY *entry = getFdEntry(fd);
            log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, count:%d, offset:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset);
            int total = 0;
            for(int i = 0; i < re_cfs; i++) {
                if(((unsigned char*)buf)[i] != ((unsigned char*)buf_copy)[i] && ((unsigned char*)buf_copy)[i] > 0) {
                    if(++total > 64) {
                        break;
                    }
                    printf("i: %d, local: %x, CFS: %x, ", i, ((unsigned char*)buf)[i], ((unsigned char*)buf_copy)[i]);
                }
            }
            if(total > 0) {
                printf("\n");
                re = -1;
            }
            cfs_flush_log();
        }
        free(buf_copy);
        #else
        re = cfs_sre(cfs_read(g_cfs_client_id, fd, buf, count));
        #endif
    } else {
        re = real_read(fd, buf, count);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path: %s, count:%d, offset:%d, re:%d, re_cfs:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset, re, re_cfs);
    #endif
    return re;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    if(!g_cfs_inited) {
        real_readv = dlsym(RTLD_NEXT, "readv");
        return real_readv(fd, iov, iovcnt);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        re = cfs_sre(cfs_readv(g_cfs_client_id, fd, iov, iovcnt));
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
        re = real_readv(fd, iov_local, iovcnt);
        if(re <= 0) {
            goto log;
        }
        for(int i = 0; i < iovcnt; i++) {
            if(memcmp(iov[i].iov_base, iov_local[i].iov_base, iov[i].iov_len)) {
                re = -1;
                cfs_flush_log();
                ENTRY *entry = getFdEntry(fd);
                log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, offset:%d, iovcnt:%d, iov_idx:%d, iov_len:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", offset, iovcnt, i, iov[i].iov_len);
            }
            free(iov_local[i].iov_base);
        }
        #endif
    } else {
        re = real_readv(fd, iov, iovcnt);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", iovcnt, re);
    #endif
    return re;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    if(!g_cfs_inited) {
        real_pread = dlsym(RTLD_NEXT, "pread");
        return real_pread(fd, buf, count, offset);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_pread(fd, buf, count, offset);
        if(re <= 0) {
            goto log;
        }
        void *buf_copy = malloc(count);
        if(buf_copy == NULL) {
            re = -1;
            goto log;
        }
        re = cfs_sre(cfs_pread(g_cfs_client_id, fd, buf_copy, count, offset));
        if(re <= 0) {
            goto log;
        }
        if(memcmp(buf, buf_copy, re)) {
            ENTRY *entry = getFdEntry(fd);
            log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, count:%d, offset:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset);
            int total = 0;
            for(int i = 0; i < re; i++) {
                if(++total > 64) {
                    break;
                }
                if(((unsigned char*)buf)[i] != ((unsigned char*)buf_copy)[i]) {
                    printf("i: %x, local: %x, CFS: %x, ", i, ((unsigned char*)buf)[i], ((unsigned char*)buf_copy)[i]);
                }
            }
            printf("\n");
            re = -1;
            cfs_flush_log();
        }
        free(buf_copy);
        #else
        re = cfs_sre(cfs_pread(g_cfs_client_id, fd, buf, count, offset));
        #endif
    } else {
        re = real_pread(fd, buf, count, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset, re);
    #endif
    return re;
}
weak_alias (pread, pread64)

ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(!g_cfs_inited) {
        real_preadv = dlsym(RTLD_NEXT, "preadv");
        return real_preadv(fd, iov, iovcnt, offset);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        re = cfs_sre(cfs_preadv(g_cfs_client_id, fd, iov, iovcnt, offset));
        #ifdef DUP_TO_LOCAL
        if(re <= 0) {
            goto log;
        }
        struct iovec iov_local[iovcnt];
        for(int i = 0; i < iovcnt; i++) {
            iov_local[i].iov_base = malloc(iov[i].iov_len);
            iov_local[i].iov_len = iov[i].iov_len;
        }
        re = real_preadv(fd, iov_local, iovcnt, offset);
        if(re <= 0) {
            goto log;
        }
        for(int i = 0; i < iovcnt; i++) {
            if(memcmp(iov[i].iov_base, iov_local[i].iov_base, iov[i].iov_len)) {
                re = -1;
                cfs_flush_log();
                ENTRY *entry = getFdEntry(fd);
                log_debug("hook %s, data from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, iovcnt:%d, offset:%d, iov_idx: %d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", iovcnt, offset, i);
            }
            free(iov_local[i].iov_base);
        }
        #endif
    } else {
        re = real_preadv(fd, iov, iovcnt, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, iovcnt:%d, offset:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", iovcnt, offset, re);
    #endif
    return re;
}

ssize_t write(int fd, const void *buf, size_t count) {
    if(!g_cfs_inited) {
        real_write = dlsym(RTLD_NEXT, "write");
        return real_write(fd, buf, count);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif

    #ifdef _CFS_DEBUG
    off_t offset = lseek(fd, 0, SEEK_CUR);
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;

    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_write(fd, buf, count);
        if(re < 0) {
            goto log;
        }
        void *buf_copy = malloc(count);
        if(buf_copy == NULL) {
            re = -1;
            goto log;
        }
        memcpy(buf_copy, buf, count);
        re = cfs_sre(cfs_write(g_cfs_client_id, fd, buf_copy, count));
        free(buf_copy);
        #else
        re = cfs_sre(cfs_write(g_cfs_client_id, fd, buf, count));
        #endif
    } else {
        re = real_write(fd, buf, count);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset, re);
    #endif
    return re;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    if(!g_cfs_inited) {
        real_writev = dlsym(RTLD_NEXT, "writev");
        return real_writev(fd, iov, iovcnt);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_writev(fd, iov, iovcnt);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_sre(cfs_writev(g_cfs_client_id, fd, iov, iovcnt));
    } else {
        re = real_writev(fd, iov, iovcnt);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", iovcnt, re);
    #endif
    return re;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    if(!g_cfs_inited) {
        real_pwrite = dlsym(RTLD_NEXT, "pwrite");
        return real_pwrite(fd, buf, count, offset);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_pwrite(fd, buf, count, offset);
        if(re < 0) {
            goto log;
        }
        void *buf_copy = malloc(count);
        if(buf_copy == NULL) {
            re = -1;
            goto log;
        }
        memcpy(buf_copy, buf, count);
        re = cfs_sre(cfs_pwrite(g_cfs_client_id, fd, buf_copy, count, offset));
        free(buf_copy);
        #else
        re = cfs_sre(cfs_pwrite(g_cfs_client_id, fd, buf, count, offset));
        #endif
    } else {
        re = real_pwrite(fd, buf, count, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, count:%d, offset:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", count, offset, re);
    #endif
    return re;
}
weak_alias (pwrite, pwrite64)

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if(!g_cfs_inited) {
        real_pwritev = dlsym(RTLD_NEXT, "pwritev");
        return real_pwritev(fd, iov, iovcnt, offset);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    ssize_t re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_pwritev(fd, iov, iovcnt, offset);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_sre(cfs_pwritev(g_cfs_client_id, fd, iov, iovcnt, offset));
    } else {
        re = real_pwritev(fd, iov, iovcnt, offset);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, iovcnt:%d, offset:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", iovcnt, offset, re);
    #endif
    return re;
}

off_t lseek(int fd, off_t offset, int whence) {
    if(!g_cfs_inited) {
        real_lseek = dlsym(RTLD_NEXT, "lseek");
        return real_lseek(fd, offset, whence);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    off_t re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_lseek(fd, offset, whence);
        if(re < 0) {
            goto log;
        }
        off_t re_cfs = cfs_lseek(g_cfs_client_id, fd, offset, whence);
        if(re_cfs != re) {
            ENTRY *entry = getFdEntry(fd);
            log_debug("hook %s, re from CFS and local is not consistent. is_cfs:%d, fd:%d, path:%s, offset:%d, whence:%d, re:%d, re_cfs:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", offset, whence, re, re_cfs);
        }
        #else
        re = cfs_lseek(g_cfs_client_id, fd, offset, whence);
        if(re < 0) {
            errno = -re;
            re = -1;
        }
        #endif
    } else {
        re = real_lseek(fd, offset, whence);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, offset:%d, whence:%d, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", offset, whence, re);
    #endif
    return re;
}
weak_alias (lseek, lseek64)


/*
 * Synchronized I/O
 */

int fdatasync(int fd) {
    if(!g_cfs_inited) {
        real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
        return real_fdatasync(fd);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_fdatasync(fd);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_flush(g_cfs_client_id, fd));
    } else {
        re = real_fdatasync(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", re);
    #endif
    return re;
}

int fsync(int fd) {
    if(!g_cfs_inited) {
        real_fsync = dlsym(RTLD_NEXT, "fsync");
        return real_fsync(fd);
    }

    #ifdef _CFS_BASH
    if(fd >=0 && fd < CFS_FD_MAP_SIZE && g_cfs_fd_map[fd] > 0) {
        fd = g_cfs_fd_map[fd];
    }
    #endif
    int is_cfs = fd & CFS_FD_MASK;
    fd = fd & ~CFS_FD_MASK;
    int re;
    if(g_hook && is_cfs) {
        #ifdef DUP_TO_LOCAL
        re = real_fsync(fd);
        if(re < 0) {
            goto log;
        }
        #endif
        re = cfs_re(cfs_flush(g_cfs_client_id, fd));
    } else {
        re = real_fsync(fd);
    }

log:
    #ifdef _CFS_DEBUG
    ; // labels can only be followed by statements
    ENTRY *entry = getFdEntry(fd);
    log_debug("hook %s, is_cfs:%d, fd:%d, path:%s, re:%d\n", __func__, is_cfs > 0, fd, entry && entry->data ? (char *)entry->data : "", re);
    #endif
    return re;
}


/*
 * Others
 */

void abort() {
    if(!g_cfs_inited) {
        real_abort = dlsym(RTLD_NEXT, "abort");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    cfs_flush_log();
    // abort is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        real_abort();
    }
}

void _exit(int status) {
    if(!g_cfs_inited) {
        real__exit = dlsym(RTLD_NEXT, "_exit");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    cfs_flush_log();
    // _exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        real__exit(status);
    }
}

void exit(int status) {
    if(!g_cfs_inited) {
        real_exit = dlsym(RTLD_NEXT, "exit");
    }

    #ifdef _CFS_DEBUG
    log_debug("hook %s\n", __func__);
    #endif
    cfs_flush_log();
    // exit is marked with __attribute__((noreturn)) by GCC.
    // If not ends with an infinite loop, there will be a compile warning.
    while(1) {
        real_exit(status);
    }
}

/*
// DON'T hook signal register function, segfault would occur when calling go
// function in signal handlers. Golang runtime would panic at runtime.cgocallback_gofunc.
int sigaction(int signum, const struct sigaction *act, struct sigaction *oldact) {
    // can't call cfs_init to initialize real_sigaction, otherwise will be blocked in cfs_new_client
    real_sigaction = dlsym(RTLD_NEXT, "sigaction");
    int re;
    // only hook signals which may terminate process
    bool is_fatal = signum == SIGSEGV || signum == SIGABRT || signum == SIGBUS ||
        signum == SIGILL || signum == SIGFPE || signum == SIGTERM;
    bool hook_action = act != NULL && act->sa_handler != SIG_IGN && is_fatal;
    if(!hook_action) {
        re = real_sigaction(signum, act, oldact);
        goto log;
    }

    g_sa_handler[signum] = act->sa_handler;
    struct sigaction new_act = {
        .sa_handler = &signal_handler,
        .sa_mask = act->sa_mask,
        .sa_flags = act->sa_flags,
        .sa_restorer = act->sa_restorer
    };
    re = real_sigaction(signum, &new_act, oldact);

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
    cfs_init();
}

__attribute__((destructor)) static void destroy(void) {
    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    // DONOT call hdestroy_r here, otherwise subsequent getFdEntry will panic after destroy called.
    //hdestroy_r(&g_fdmap); // the elements are NOT freed here
    #endif
    #ifdef _CFS_DEBUG
    printf("destructor\n");
    #endif
    cfs_close_client(g_cfs_client_id);
}

static void cfs_init() {
    if(g_cfs_inited) {
        return;
    }

    #if defined(_CFS_DEBUG) || defined(DUP_TO_LOCAL)
    hcreate_r(FD_MAP_SIZE, &g_fdmap);
    #endif
    #ifdef _CFS_DEBUG
    printf("constructor\n");
    #endif

    real_open = dlsym(RTLD_NEXT, "open");
    real_openat = dlsym(RTLD_NEXT, "openat");
    real_close = dlsym(RTLD_NEXT, "close");
    real_rename = dlsym(RTLD_NEXT, "rename");
    real_renameat = dlsym(RTLD_NEXT, "renameat");
    real_renameat2 = dlsym(RTLD_NEXT, "renameat2");
    real_truncate = dlsym(RTLD_NEXT, "truncate");
    real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");
    real_fallocate = dlsym(RTLD_NEXT, "fallocate");
    real_posix_fallocate = dlsym(RTLD_NEXT, "posix_fallocate");

    real_chdir = dlsym(RTLD_NEXT, "chdir");
    real_fchdir = dlsym(RTLD_NEXT, "fchdir");
    real_getcwd = dlsym(RTLD_NEXT, "getcwd");
    real_mkdir = dlsym(RTLD_NEXT, "mkdir");
    real_mkdirat = dlsym(RTLD_NEXT, "mkdirat");
    real_rmdir = dlsym(RTLD_NEXT, "rmdir");
    real_opendir = dlsym(RTLD_NEXT, "opendir");
    real_fdopendir = dlsym(RTLD_NEXT, "fopendir");
    real_readdir = dlsym(RTLD_NEXT, "readdir");
    real_closedir = dlsym(RTLD_NEXT, "closedir");
    real_realpath = dlsym(RTLD_NEXT, "realpath");

    real_link = dlsym(RTLD_NEXT, "link");
    real_linkat = dlsym(RTLD_NEXT, "linkat");
    real_symlink = dlsym(RTLD_NEXT, "symlink");
    real_symlinkat = dlsym(RTLD_NEXT, "symlinkat");
    real_unlink = dlsym(RTLD_NEXT, "unlink");
    real_unlinkat = dlsym(RTLD_NEXT, "unlinkat");
    real_readlink = dlsym(RTLD_NEXT, "readlink");
    real_readlinkat = dlsym(RTLD_NEXT, "readlinkat");

    real_stat = dlsym(RTLD_NEXT, "__xstat");
    real_stat64 = dlsym(RTLD_NEXT, "__xstat64");
    real_lstat = dlsym(RTLD_NEXT, "__lxstat");
    real_lstat64 = dlsym(RTLD_NEXT, "__lxstat64");
    real_fstat = dlsym(RTLD_NEXT, "__fxstat");
    real_fstat64 = dlsym(RTLD_NEXT, "__fxstat64");
    real_fstatat = dlsym(RTLD_NEXT, "__fxstatat");
    real_fstatat64 = dlsym(RTLD_NEXT, "__fxstatat64");
    real_chmod = dlsym(RTLD_NEXT, "chmod");
    real_fchmod = dlsym(RTLD_NEXT, "fchmod");
    real_fchmodat = dlsym(RTLD_NEXT, "fchmodat");
    real_chown = dlsym(RTLD_NEXT, "chown");
    real_lchown = dlsym(RTLD_NEXT, "lchown");
    real_fchown = dlsym(RTLD_NEXT, "fchown");
    real_fchownat = dlsym(RTLD_NEXT, "fchownat");
    real_utime = dlsym(RTLD_NEXT, "utime");
    real_utimes = dlsym(RTLD_NEXT, "utimes");
    real_futimesat = dlsym(RTLD_NEXT, "futimesat");
    real_utimensat = dlsym(RTLD_NEXT, "utimensat");
    real_futimens = dlsym(RTLD_NEXT, "futimens");
    real_access = dlsym(RTLD_NEXT, "access");
    real_faccessat = dlsym(RTLD_NEXT, "faccessat");

    real_setxattr = dlsym(RTLD_NEXT, "setxattr");
    real_lsetxattr = dlsym(RTLD_NEXT, "lsetxattr");
    real_fsetxattr = dlsym(RTLD_NEXT, "fsetxattr");
    real_getxattr = dlsym(RTLD_NEXT, "getxattr");
    real_lgetxattr = dlsym(RTLD_NEXT, "lgetxattr");
    real_fgetxattr = dlsym(RTLD_NEXT, "fgetxattr");
    real_listxattr = dlsym(RTLD_NEXT, "listxattr");
    real_llistxattr = dlsym(RTLD_NEXT, "llistxattr");
    real_flistxattr = dlsym(RTLD_NEXT, "flistxattr");
    real_removexattr = dlsym(RTLD_NEXT, "removexattr");
    real_lremovexattr = dlsym(RTLD_NEXT, "lremovexattr");
    real_fremovexattr = dlsym(RTLD_NEXT, "fremovexattr");

    real_fcntl = dlsym(RTLD_NEXT, "fcntl");
    real_dup2 = dlsym(RTLD_NEXT, "dup2");
    real_dup3 = dlsym(RTLD_NEXT, "dup3");

    real_read = dlsym(RTLD_NEXT, "read");
    real_readv = dlsym(RTLD_NEXT, "readv");
    real_pread = dlsym(RTLD_NEXT, "pread");
    real_preadv = dlsym(RTLD_NEXT, "preadv");
    real_write = dlsym(RTLD_NEXT, "write");
    real_writev = dlsym(RTLD_NEXT, "writev");
    real_pwrite = dlsym(RTLD_NEXT, "pwrite");
    real_pwritev = dlsym(RTLD_NEXT, "pwritev");
    real_lseek = dlsym(RTLD_NEXT, "lseek");
    real_lseek64 = dlsym(RTLD_NEXT, "lseek64");

    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_fsync = dlsym(RTLD_NEXT, "fsync");

    real_abort = dlsym(RTLD_NEXT, "abort");
    real__exit = dlsym(RTLD_NEXT, "_exit");
    real_exit = dlsym(RTLD_NEXT, "exit");

    const char *cfs_config_path = getenv("CFS_CONFIG_PATH");
    if(cfs_config_path == NULL) {
        cfs_config_path = CFS_CFG_PATH;
        if(real_access(cfs_config_path, F_OK)) {
            cfs_config_path = CFS_CFG_PATH_JED;
        }
    }

    // parse client configurations from ini file.
    client_config_t client_config;
    memset(&client_config, 0, sizeof(client_config_t));
    // libc printf CANNOT be used in this init function, otherwise will cause circular dependencies.
    if(ini_parse(cfs_config_path, config_handler, &client_config) < 0) {
        char *msg = "Can't load CFS config file, use CFS_CONFIG_PATH env variable.\n";
        real_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    if(client_config.mount_point == NULL || client_config.master_addr == NULL ||
    client_config.vol_name == NULL || client_config.owner == NULL || client_config.log_dir == NULL) {
        char *msg = "Check CFS config file for necessary parameters.\n";
        real_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_mount_point = client_config.mount_point;
    g_ignore_path = client_config.ignore_path;
    g_cfs_config.master_addr = client_config.master_addr;
    g_cfs_config.vol_name = client_config.vol_name;
    g_cfs_config.owner = client_config.owner;
    g_cfs_config.follower_read = client_config.follower_read;
    g_cfs_config.app = client_config.app;
    g_cfs_config.auto_flush = client_config.auto_flush;
    g_cfs_config.master_client = client_config.master_client;

    cfs_sdk_init_t init_t;
    init_t.ignore_sighup = 1;
    init_t.ignore_sigterm = 1;
    init_t.log_dir = client_config.log_dir;
    init_t.log_level = client_config.log_level;
    init_t.prof_port = client_config.prof_port;

    if(g_ignore_path == NULL) {
        g_ignore_path = "";
    }

    char *mount_point = getenv("CFS_MOUNT_POINT");
    if(mount_point != NULL) {
        free((char *)g_mount_point);
        g_mount_point = mount_point;
    }
    char *vol_name = getenv("CFS_VOL_NAME");
    if(vol_name != NULL) {
        free((char *)g_cfs_config.vol_name);
        g_cfs_config.vol_name = vol_name;
    }
    char *prof_port = getenv("CFS_PROF_PORT");
    if(prof_port != NULL) {
        free((char *)init_t.prof_port);
        init_t.prof_port = prof_port;
    }
    char *master_client = getenv("CFS_MASTER_CLIENT");
    if(master_client != NULL) {
        free((char *)g_cfs_config.master_client);
        g_cfs_config.master_client = master_client;
    }

    if(g_mount_point == NULL || g_mount_point[0] != '/') {
        char *msg = "Mount point is null or not an absolute path.\n";
        real_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }
    char *path = get_clean_path(g_mount_point);
    // should not free the returned string from getenv()
    if(mount_point == NULL) {
        free((char *)g_mount_point);
    }
    g_mount_point = path;

    if(cfs_sdk_init(&init_t) != 0) {
        char *msg= "Can't initialize CFS SDK, check the config file.\n";
        real_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_cfs_client_id = cfs_new_client(&g_cfs_config);
    if(g_cfs_client_id < 0) {
        char *msg = "Can't start CFS client, check the config file.\n";
        real_write(STDOUT_FILENO, msg, strlen(msg));
        exit(1);
    }

    g_has_renameat2 = has_renameat2();
    g_cfs_inited = true;
}
