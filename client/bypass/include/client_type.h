#ifndef CLIENT_TYPE_H
#define CLIENT_TYPE_H

#include <pthread.h>
#include "cache.h"
#include "conn_pool.h"

const int CFS_FD_MASK = 1 << (sizeof(int)*8 - 2);

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
    int fd;
    int flags;
    off_t pos;
    int dup_ref;
    int file_type;
    pthread_mutex_t file_lock;
    inode_info_t *inode_info;
} file_t;

typedef struct {
    char *sdk_state;
    cfs_file_t *files;
    int file_num;
    int* dup_fds;
    int fd_num;
    char *cwd;
    bool in_cfs;
} client_state_t;

typedef struct {
     char* mount_point;
     char* ignore_path;
     char* log_dir;
     char* log_level;
     char* prof_port;
} client_config_t;

typedef struct {
    pthread_rwlock_t dup_fds_lock;
    map<int, int> dup_fds;
    pthread_rwlock_t open_files_lock;
    map<int, file_t *> open_files;
    pthread_rwlock_t open_inodes_lock;
    map<ino_t, inode_info_t *> open_inodes;

    lru_cache_t *big_page_cache;
    lru_cache_t *small_page_cache;
    conn_pool_t *conn_pool;

    // map for each open fd to its pathname, to print pathname in debug log
    map<int, char *> fd_path;
    pthread_rwlock_t fd_path_lock;

    // the current working directory, doesn't include the mount point part if in cfs
    char *cwd;
    // whether the _cwd is in CFS or not
    bool in_cfs;
    int64_t cfs_client_id;
    bool has_renameat2;

    const char *mount_point;
    const char *ignore_path;
    pthread_t bg_pthread;
    void* sdk_handle;
    bool stop;
    inode_wrapper_t inode_wrapper;
} client_info_t;

#endif
