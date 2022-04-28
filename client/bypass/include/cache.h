#ifndef CACHE_H
#define CACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "util.h"

#define BIG_PAGE_SIZE 131072
#define SMALL_PAGE_SIZE 16384
#define PAGE_DIRTY 0x1
struct cfs_file;
typedef struct page {
    struct cfs_file *f;
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

page_t *new_page(struct lru_cache *c);
int read_page(page_t *p, ino_t inode, int index, void *data, int offset, int count);
int write_page(page_t *p, ino_t inode, int index, const void *data, int offset, int count);
int flush_page(page_t *p);
void clear_page(page_t *p);
void occupy_page(page_t *p, struct cfs_file *f, int index);
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
page_t *lru_cache_alloc(lru_cache_t *c);
void lru_cache_access(lru_cache_t *c, page_t *p);
void bg_flush(void *arg);

typedef ssize_t (*cfs_pwrite_t)(int64_t id, int fd, const void *buf, size_t count, off_t offset);

#define BLOCKS_PER_FILE 512
#define PAGES_PER_BLOCK 512
#define FILE_TYPE_BIN_LOG 1
#define FILE_TYPE_RELAY_LOG 3
#define FILE_CACHE_WRITE_BACK 0x1
#define FILE_CACHE_WRITE_THROUGH 0x2
#define FILE_CACHE_PRIORITY_HIGH 0x4
#define FILE_STATUS_OPEN 0
#define FILE_STATUS_CLOSED 1
typedef struct cfs_file {
    int64_t client_id;
    lru_cache_t *c;
    int fd;
    ino_t inode;
    int flags;
    int file_type;
    size_t size;
    off_t pos;
    uint8_t cache_flag;
    uint8_t status;
    // The file contents are structured as a two dimentional array of pages for memory efficiency.
    page_t ***pages;
    cfs_pwrite_t write_func;
    pthread_mutex_t lock;
} cfs_file_t;

cfs_file_t *new_file(cfs_pwrite_t write_func);
void init_file(cfs_file_t *f);
size_t read_cache(cfs_file_t *f, off_t offset, size_t count, void *data);
size_t write_cache(cfs_file_t *f, off_t offset, size_t count, const void *data);
int flush_file(cfs_file_t *f);
void flush_file_range(cfs_file_t *f, off_t offset, size_t count);
void clear_file_range(cfs_file_t *f, off_t offset, size_t count);
void clear_file(cfs_file_t *f);

#ifdef __cplusplus
}
#endif

#endif