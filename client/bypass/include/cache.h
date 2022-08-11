#ifndef CACHE_H
#define CACHE_H

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <map>
#include <set>
#include "sdk.h"
#include "util.h"

#define BIG_PAGE_CACHE_SIZE 67108864
#define SMALL_PAGE_CACHE_SIZE 67108864
#define BIG_PAGE_SIZE 131072
#define SMALL_PAGE_SIZE 16384
#define PAGE_DIRTY 0x1
struct inode_shared;
typedef struct page {
    struct inode_shared *inode_info;
    int index;
    int offset;
    // flush from dirty_offset, to avoid overwriting to SDK
    int dirty_offset;
    int len;
    uint flag;
    time_t last_alloc;
    void *data;
    struct page *prev;
    struct page *next;
    pthread_rwlock_t lock;
} page_t;

page_t *new_page(int page_size);
void release_page(page_t* p);
int read_page(page_t *p, ino_t inode, int index, void *data, int offset, int count);
int write_page(page_t *p, ino_t inode, int index, const void *data, int offset, int count);
int flush_page(page_t *p, ino_t inode, int index);
void clear_page(page_t *p);
void occupy_page(page_t *p, struct inode_shared *inode_info, int index);
void clear_page_raw(page_t *p);

// sleep when dirty page ratio is below 1/threshold
#define BG_FLUSH_SLEEP_THRESHOLD 10
#define BG_FLUSH_SLEEP 1
#define CACHE_TTL 600
typedef struct lru_cache {
    int page_size;
    int max_count;
    int count;
    page_t *head;
    pthread_rwlock_t lock;
} lru_cache_t;

lru_cache_t *new_lru_cache(int cache_size, int page_size);
void release_lru_cache(lru_cache_t *c);
page_t *lru_cache_alloc(lru_cache_t *c);
void lru_cache_access(lru_cache_t *c, page_t *p);
void *do_flush_inode(void *arg);

typedef ssize_t (*cfs_pwrite_inode_t)(int64_t id, ino_t ino, const void *buf, size_t size, off_t off);

#define BLOCKS_PER_FILE 512
#define PAGES_PER_BLOCK 512
#define FILE_CACHE_WRITE_BACK 0x1
#define FILE_CACHE_WRITE_THROUGH 0x2
#define FILE_CACHE_PRIORITY_HIGH 0x4

typedef struct inode_info {
    int64_t client_id;
    ino_t inode;
    bool use_pagecache;
    uint8_t cache_flag;
    lru_cache_t *c;
    size_t size;
    int fd_ref;
    // The file contents are structured as a two dimentional array of pages for memory efficiency.
    page_t ***pages;
    pthread_mutex_t pages_lock;
    cfs_pwrite_inode_t write_func;
} inode_info_t;

typedef struct inode_wrapper {
    pthread_rwlock_t *open_inodes_lock;
    std::map<ino_t, inode_info_t *> *open_inodes;
    bool *stop;
} inode_wrapper_t;

inode_info_t *new_inode_info(ino_t inode, bool use_pagecache, cfs_pwrite_inode_t write_func);
void release_inode_info(inode_info_t *inode_info);
size_t read_cache(inode_info_t *inode_info, off_t offset, size_t count, void *data);
size_t write_cache(inode_info_t *inode_info, off_t offset, size_t count, const void *data);
int flush_inode(inode_info_t *inode_info);
void flush_inode_range(inode_info_t *inode_info, off_t offset, size_t count);
void clear_inode_range(inode_info_t *inode_info, off_t offset, size_t count);
void clear_inode(inode_info_t *inode_info);
void flush_and_release(std::map<ino_t, inode_info_t *> &arg);

#endif
