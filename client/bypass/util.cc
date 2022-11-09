#include <gnu/libc-version.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <map>
#include "client_type.h"

extern client_info_t g_client_info;

int config_handler(void* user, const char* section,
        const char* name, const char* value) {
    client_config_t *pconfig = (client_config_t*)user;
    #define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0

    if (MATCH("", "mountPoint")) {
        pconfig->mount_point = strdup(value);
    } else if (MATCH("", "ignorePath")) {
        pconfig->ignore_path = strdup(value);
    } else if (MATCH("", "logDir")) {
        pconfig->log_dir = strdup(value);
    } else if (MATCH("", "logLevel")) {
        pconfig->log_level = strdup(value);
    } else if (MATCH("", "profPort")) {
        pconfig->prof_port = strdup(value);
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
char *get_clean_path(const char *path) {
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
char *cat_path(const char *cwd, const char *pathname) {
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
char *get_cfs_path(const char *pathname) {
    if(pathname == NULL || (pathname[0] != '/' && !g_client_info.in_cfs)) {
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
    if(pathname[0] != '/' && g_client_info.in_cfs) {
        result = cat_path(g_client_info.cwd, real_path);
        free(real_path);
        return result;
    }

    // check if real_path contains mount_point, and doesn't contain ignore_path
    // the mount_point has been strip off the last '/' in cfs_init()
    size_t len = strlen(g_client_info.mount_point);
    size_t len_real = strlen(real_path);
    bool is_cfs = false;
    char *ignore_path = strdup(g_client_info.ignore_path);
    if(ignore_path == NULL) {
        free(real_path);
        return NULL;
    }
    if(strncmp(real_path, g_client_info.mount_point, len) == 0) {
        if(strlen(g_client_info.ignore_path) > 0) {
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

// process returned int from cfs functions
int cfs_errno(int re) {
    if(re < 0) {
        errno = -re;
        re = -1;
    } else {
        errno = 0;
    }
    return re;
}

// process returned ssize_t from cfs functions
ssize_t cfs_errno_ssize_t(ssize_t re) {
    if(re < 0) {
        errno = -re;
        re = -1;
    } else {
        errno = 0;
    }
    return re;
}

/*
void signal_handler(int signum) {
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

bool fd_in_cfs(int fd) {
    if(g_client_info.dup_fds.find(fd) != g_client_info.dup_fds.end()) {
        return true;
    }

    if (fd & CFS_FD_MASK) {
        return true;
    }
    return false;
}

int get_cfs_fd(int fd) {
    int cfs_fd = -1;
    auto it = g_client_info.dup_fds.find(fd);
    if(it != g_client_info.dup_fds.end()) {
        cfs_fd = it->second;
    } else if (fd & CFS_FD_MASK) {
        cfs_fd = fd & ~CFS_FD_MASK;
    }
    return cfs_fd;
}

int dup_fd(int oldfd, int newfd) {
    pthread_rwlock_rdlock(&g_client_info.open_files_lock);
    auto it = g_client_info.open_files.find(oldfd);
    if (it == g_client_info.open_files.end()) {
        pthread_rwlock_unlock(&g_client_info.open_files_lock);
        return -1;
    }

    file_t *f = it->second;
    pthread_mutex_lock(&f->file_lock);
    f->dup_ref++;
    pthread_mutex_unlock(&f->file_lock);
    pthread_rwlock_unlock(&g_client_info.open_files_lock);

    pthread_rwlock_wrlock(&g_client_info.dup_fds_lock);
    g_client_info.dup_fds[newfd] = oldfd;
    pthread_rwlock_unlock(&g_client_info.dup_fds_lock);
    return newfd;
}

int gen_fd(int start) {
    int fd = start;
    while(g_client_info.dup_fds.find(fd) != g_client_info.dup_fds.end()) {
        fd++;
    }
    return fd;
}

file_t *get_open_file(int fd) {
    pthread_rwlock_rdlock(&g_client_info.open_files_lock);
    auto it = g_client_info.open_files.find(fd);
    file_t *f = (it != g_client_info.open_files.end() ? it->second : NULL);
    pthread_rwlock_unlock(&g_client_info.open_files_lock);
    return f;
}

inode_info_t *get_open_inode(ino_t ino) {
    pthread_rwlock_rdlock(&g_client_info.open_inodes_lock);
    auto it = g_client_info.open_inodes.find(ino);
    inode_info_t *inode_info = (it != g_client_info.open_inodes.end() ? it->second : NULL);
    pthread_rwlock_unlock(&g_client_info.open_inodes_lock);
    return inode_info;
}

const char *get_fd_path(int fd) {
    pthread_rwlock_rdlock(&g_client_info.fd_path_lock);
    auto it = g_client_info.fd_path.find(fd);
    const char *path = it != g_client_info.fd_path.end() ? it->second : "";
    pthread_rwlock_unlock(&g_client_info.fd_path_lock);
    return path;
}

void log_debug(const char* message, ...) {
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
    fprintf(stderr, "%s [debug] ", buf);
    vprintf(message, args);
}
