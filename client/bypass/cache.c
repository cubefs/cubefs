#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <map>
#include <set>
#include "cache.h"

using namespace std;

#ifdef _CFS_DEBUG
char *page_str(page_t *p) {
    char *str = (char *)malloc(128);
    sprintf(str, "fd:%d, inode:%d, index:%d, offset:%d, dirty_offset:%d, len:%d, flag:%d", p->f == NULL ? 0 : p->f->fd, p->f == NULL ? 0 : p->f->inode, p->index, p->offset, p->dirty_offset, p->len, p->flag);
    return str;
}
#endif

page_t *new_page(lru_cache_t *c) {
    page_t *p = (page_t *)malloc(sizeof(page_t));
    if(p == NULL) {
        return NULL;
    }
    memset(p, 0, sizeof(page_t));
    p->data = malloc(c->page_size);
    if(p->data == NULL) {
        free(p);
        return NULL;
    }
    memset(p->data, 0, c->page_size);
    pthread_rwlock_init(&p->lock, NULL);
    return p;
}

int read_page(page_t *p, ino_t inode, int index, void *data, int offset, int count) {
    int read_len = 0;
    pthread_rwlock_rdlock(&p->lock);
    if(p->f == NULL || p->f->inode != inode || p->index != index || offset < p->offset || offset >= p->offset + p->len) {
        goto log;
    }
    read_len = p->len - (offset - p->offset);
    if(read_len > count) {
        read_len = count;
    }
    memcpy(data, (char *)p->data + offset, read_len);

log:
    #ifdef _CFS_DEBUG
    char *str = page_str(p);
    log_debug("read_page, inode:%d, index:%d, offset:%d, count:%d, p:(%s) re:%d\n", inode, index, offset, count, str, read_len);
    free(str);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return read_len;
}

int write_page(page_t *p, ino_t inode, int index, const void *data, int offset, int count) {
    int write = 0;
    pthread_rwlock_wrlock(&p->lock);
    if(p->f == NULL || p->f->inode != inode || p->index != index) {
        goto log;
    }

    if(p->len == 0) {
        p->offset = offset;
        p->len = count;
    } else {
        int end = offset + count;
        int page_end = p->offset + p->len;
        // don't allow hole in page
        if(end < p->offset || offset > page_end) {
            goto log;
        }
        if(p->offset > offset) {
            p->offset = offset;
        }
        if(page_end < end) {
            page_end = end;
        }
        p->len = page_end - p->offset;
    }

    memcpy((char *)p->data + offset, data, count);
    if(p->f->cache_flag&FILE_CACHE_WRITE_BACK) {
        if(!(p->flag&PAGE_DIRTY) || p->dirty_offset > offset) {
            p->dirty_offset = offset;
        }
        p->flag |= PAGE_DIRTY;
    }
    write = count;

log:
    #ifdef _CFS_DEBUG
    char *str = page_str(p);
    log_debug("write_page, inode:%d, index:%d, offset:%d, count:%d, p:(%s), re:%d\n", inode, index, offset, count, str, write);
    free(str);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return write;
}

int flush_page(page_t *p) {
    pthread_rwlock_wrlock(&p->lock);
    ssize_t re = 0;
    // the page may have been cleared by page allocation
    if(p->f == NULL) {
        goto log;
    }
    if(p->flag&PAGE_DIRTY) {
        int dirty_len = p->offset + p->len - p->dirty_offset;
        off_t off = (off_t)p->f->c->page_size*p->index + p->dirty_offset;
        re = p->f->write_func(p->f->client_id, p->f->fd, (char *)p->data + p->dirty_offset, dirty_len, off);
        p->dirty_offset = 0;
        p->flag &= ~PAGE_DIRTY;
    }

log:
    #ifdef _CFS_DEBUG
    char *str = page_str(p);
    log_debug("flush_page, p:(%s), re:%d\n", str, re);
    free(str);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return re;
}

void clear_page(page_t *p) {
    pthread_rwlock_wrlock(&p->lock);
    #ifdef _CFS_DEBUG
    char *str = page_str(p);
    log_debug("clear_page, p:(%s)\n", str);
    free(str);
    #endif
    clear_page_raw(p);
    pthread_rwlock_unlock(&p->lock);
}

void occupy_page(page_t *p, cfs_file_t *f, int index) {
    pthread_rwlock_wrlock(&p->lock);
    #ifdef _CFS_DEBUG
    char *str = page_str(p);
    log_debug("occupy_page, p:(%s), fd:%d, inode:%d, index:%d\n", str, f->fd, f->inode, index);
    free(str);
    #endif
    clear_page_raw(p);
    p->f = f;
    p->index = index;
    p->last_alloc = time(NULL);
    pthread_rwlock_unlock(&p->lock);
}

void clear_page_raw(page_t *p) {
    // both file eviction and page allocation will clear page
    if(p->f == NULL) {
        return;
    }
    if(p->flag&PAGE_DIRTY) {
        int dirty_len = p->offset + p->len - p->dirty_offset;
        off_t off = (off_t)p->f->c->page_size*p->index + p->dirty_offset;
        p->f->write_func(p->f->client_id, p->f->fd, (char *)p->data + p->dirty_offset, dirty_len, off);
    }
    p->f->pages[p->index/PAGES_PER_BLOCK][p->index%PAGES_PER_BLOCK] = NULL;
    p->index = 0;
    p->offset = 0;
    p->dirty_offset = 0;
    p->len = 0;
    p->flag = 0;
    p->last_alloc = 0;
    memset(p->data, 0, p->f->c->page_size);
    p->f = NULL;
}

lru_cache_t *new_lru_cache(int cache_size, int page_size) {
    lru_cache_t *c = (lru_cache_t *)malloc(sizeof(lru_cache_t));
    if(c == NULL) {
        return NULL;
    }
    c->page_size = page_size;
    c->max_count = cache_size/page_size;
    c->count = 0;
    c->head = NULL;
    pthread_rwlock_init(&c->lock, NULL);
    return c;
}

page_t *lru_cache_alloc(lru_cache_t *c) {
    pthread_rwlock_wrlock(&c->lock);
    page_t *p = NULL;
    if(c->max_count == 0) {
        return p;
    }
    time_t now = time(NULL);
    if(c->count < c->max_count) {
        p = new_page(c);
        if(p == NULL) {
            pthread_rwlock_unlock(&c->lock);
            return NULL;
        }
        if(c->count == 0) {
            p->prev = p;
            p->next = p;
            c->head = p;
        } else if(c->count == 1) {
            c->head->next = p;
            p->prev = c->head;
            p->next = c->head;
            c->head->prev = p;
        } else {
            page_t *tail = c->head->prev;
            tail->next = p;
            p->prev = tail;
            p->next = c->head;
            c->head->prev = p;
        }
        c->count++;
        pthread_rwlock_unlock(&c->lock);
        p->last_alloc = now;
        return p;
    }

    p = c->head;
    bool available;
    while(1) {
        // protect from clear_page
        pthread_rwlock_rdlock(&p->lock);
        available = (p->f == NULL || !(p->f->cache_flag&FILE_CACHE_PRIORITY_HIGH) || now - p->last_alloc > CACHE_TTL);
        pthread_rwlock_unlock(&p->lock);
        if(available) {
            break;
        }
        p = p->next;
        if(p == c->head) {
            break;
        }
    }
    lru_cache_access(c, p);
    pthread_rwlock_unlock(&c->lock);
    return p;
}

void lru_cache_access(lru_cache_t *c, page_t *p) {
    if(p->next == c->head) {
        return;
    }
    if(p == c->head) {
        c->head = c->head->next;
        return;
    }
    p->prev->next = p->next;
    p->next->prev = p->prev;
    page_t *tail = c->head->prev;
    tail->next = p;
    p->prev = tail;
    p->next = c->head;
    c->head->prev = p;
}

void *do_flush_lru(void *arg) {
    lru_cache_t *c = (lru_cache_t *)arg;
    page_t *head, *p;
    int dirty_num;
    while(1) {
        pthread_rwlock_rdlock(&c->lock);
        head = c->head;
        p = head;
        dirty_num = 0;
        while(p != NULL) {
            if(p->flag&PAGE_DIRTY && p->dirty_offset == 0 && p->len == c->page_size) {
                dirty_num++;
                flush_page(p);
            }
            p = p->next;
            if(p == head) {
                break;
            }
        }
        pthread_rwlock_unlock(&c->lock);
        if(c->count == 0 || dirty_num < c->count/BG_FLUSH_SLEEP_THRESHOLD) {
            sleep(BG_FLUSH_SLEEP);
        }
    }
}

void *do_flush_file(void *arg) {
    auto open_file = *(map<int, cfs_file_t *> *)arg;
    page_t *p;
    int dirty_num, page_num;
    while(1) {
        dirty_num = 0;
        page_num = 0;
        for(const auto &item : open_file) {
            cfs_file_t *f = item.second;
            for(int i = 0; i < BLOCKS_PER_FILE; i++) {
                if(f->pages[i] == NULL) {
                    continue;
                }
                for(int j = 0; j < PAGES_PER_BLOCK; j++) {
                    p = f->pages[i][j];
                    if(p == NULL) {
                        continue;
                    }
                    page_num++;
                    if(p->flag&PAGE_DIRTY && p->dirty_offset == 0 && p->len == f->c->page_size) {
                        dirty_num++;
                        flush_page(p);
                    }
                }
            }
        }
        if(page_num == 0 || dirty_num < page_num/BG_FLUSH_SLEEP_THRESHOLD) {
            sleep(BG_FLUSH_SLEEP);
        }
    }
}

void bg_flush(void *arg) {
    pthread_t thd;
    pthread_create(&thd, NULL, do_flush_file, arg);
}

cfs_file_t *new_file(cfs_pwrite_t write_func) {
    cfs_file_t *f = (cfs_file_t *)malloc(sizeof(cfs_file_t));
    if(f == NULL) {
        return NULL;
    }
    memset(f, 0, sizeof(cfs_file_t));
    f->write_func = write_func;
    return f;
}

void init_file(cfs_file_t *f) {
    f->pages = (page_t ***)calloc(BLOCKS_PER_FILE, sizeof(page_t **));
    pthread_mutex_init(&f->lock, NULL);
}

size_t read_cache(cfs_file_t *f, off_t offset, size_t count, void *data) {
    int index = offset/f->c->page_size;
    int block_index;
    int page_offset = offset%f->c->page_size;
    size_t page_len = count;
    size_t read = 0;
    page_t *p;

    while(read < count) {
        block_index = index/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE || f->pages[block_index] == NULL) {
            break;
        }
        p = f->pages[block_index][index%PAGES_PER_BLOCK];
        if(p == NULL) {
            break;
        }
        if(page_offset + page_len > f->c->page_size) {
            page_len = f->c->page_size - page_offset;
        }
        size_t read_len = read_page(p, f->inode, index, (char *)data + read, page_offset, page_len);
        read += read_len;
        if(read_len != page_len) {
            break;
        }
        page_len = count - read;
        page_offset = 0;
        index++;
    }

    #ifdef _CFS_DEBUG
    log_debug("read_cache, fd:%d, inode:%d, offset:%ld, count:%d, re:%d\n", f->fd, f->inode, offset, count, read);
    #endif
    return read;
}

size_t write_cache(cfs_file_t *f, off_t offset, size_t count, const void *data) {
    int index = offset/f->c->page_size;
    int block_index;
    int page_offset = offset%f->c->page_size;
    size_t page_len = count;
    size_t write = 0;
    page_t *p;

    while(write < count) {
        block_index = index/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }

        pthread_mutex_lock(&f->lock);
        if(f->pages[block_index] == NULL) {
            f->pages[block_index] = (page_t **)calloc(PAGES_PER_BLOCK, sizeof(page_t *));
        }
        p = f->pages[block_index][index%PAGES_PER_BLOCK];
        bool alloc = (p == NULL);
        if(alloc) {
            p = lru_cache_alloc(f->c);
            if(p == NULL) {
                pthread_mutex_unlock(&f->lock);
                break;
            }
            f->pages[block_index][index%PAGES_PER_BLOCK] = p;
        }
        pthread_mutex_unlock(&f->lock);
        if(alloc) {
            occupy_page(p, f, index);
        }

        if(page_offset + page_len > f->c->page_size) {
            page_len = f->c->page_size - page_offset;
        }
        size_t write_len = write_page(p, f->inode, index, (char *)data + write, page_offset, page_len);
        write += write_len;
        if(write_len != page_len) {
            break;
        }
        page_len = count - write;
        page_offset = 0;
        index++;
    }

    #ifdef _CFS_DEBUG
    log_debug("write_cache, fd:%d, inode:%d, offset:%ld, count:%d, re:%d\n", f->fd, f->inode, offset, count, write);
    #endif
    return write;
}

int flush_file(cfs_file_t *f) {
    page_t *p;
    int re = 0;
    int re_tmp;
    for(int i = 0; i < BLOCKS_PER_FILE; i++) {
        if(f->pages[i] == NULL) {
            continue;
        }
        for(int j = 0; j < PAGES_PER_BLOCK; j++) {
            p = f->pages[i][j];
            if(p == NULL || !(p->flag&PAGE_DIRTY)) {
                continue;
            }
            re_tmp = flush_page(p);
            if(re_tmp < 0) {
                re = re_tmp;
            }
        }
    }
    return re;
}

void flush_file_range(cfs_file_t *f, off_t offset, size_t count) {
    int begin_index = offset/f->c->page_size;
    int end_index = (offset + count - 1)/f->c->page_size;
    int block_index;
    page_t *p;
    for(int i = begin_index; i <= end_index; i++) {
        block_index = i/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }
        if(f->pages[block_index] == NULL) {
            continue;
        }
        p = f->pages[block_index][i%PAGES_PER_BLOCK];
        if(p != NULL && p->flag&PAGE_DIRTY) {
            flush_page(p);
        }
    }
}

void clear_file_range(cfs_file_t *f, off_t offset, size_t count) {
    int begin_index = offset/f->c->page_size;
    int end_index = (offset + count - 1)/f->c->page_size;
    int block_index;
    page_t *p;
    for(int i = begin_index; i <= end_index; i++) {
        block_index = i/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }
        if(f->pages[block_index] == NULL) {
            continue;
        }
        p = f->pages[block_index][i%PAGES_PER_BLOCK];
        if(p != NULL) {
            clear_page(p);
        }
    }
}

void clear_file(cfs_file_t *f) {
    page_t *p;
    for(int i = 0; i < BLOCKS_PER_FILE; i++) {
        if(f->pages[i] == NULL) {
            continue;
        }
        for(int j = 0; j < PAGES_PER_BLOCK; j++) {
            p = f->pages[i][j];
            if(p != NULL) {
                clear_page(p);
            }
        }
    }
}
