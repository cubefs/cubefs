#include "cache.h"

using namespace std;

#define page_str(p)      \
    char str[128] = {0}; \
    sprintf(str, "inode:%d, index:%d, offset:%d, dirty_offset:%d, len:%d, flag:%d", p->inode_info == NULL ? 0 : p->inode_info->inode, p->index, p->offset, p->dirty_offset, p->len, p->flag)

page_t *new_page(int page_size) {
    page_t *p = (page_t *)malloc(sizeof(page_t));
    if(p == NULL) {
        fprintf(stderr, "malloc page_t failed.\n");
        return NULL;
    }
    memset(p, 0, sizeof(page_t));
    p->data = malloc(page_size);
    if(p->data == NULL) {
        free(p);
        fprintf(stderr, "malloc page_size failed.\n");
        return NULL;
    }
    memset(p->data, 0, page_size);
    pthread_rwlock_init(&p->lock, NULL);
    return p;
}

void release_page(page_t* p) {
    if(p == NULL) {
        return;
    }
    free(p->data);
    pthread_rwlock_destroy(&p->lock);
    free(p);
}

int read_page(page_t *p, ino_t inode, int index, void *data, int offset, int count) {
    int read_len = 0;
    pthread_rwlock_rdlock(&p->lock);
    if(p->inode_info == NULL || p->inode_info->inode != inode || p->index != index || offset < p->offset || offset >= p->offset + p->len) {
        goto log;
    }
    read_len = p->len - (offset - p->offset);
    if(read_len > count) {
        read_len = count;
    }
    memcpy(data, (char *)p->data + offset, read_len);

log:
    #ifdef _CFS_DEBUG
    page_str(p);
    log_debug("read_page, inode:%d, index:%d, offset:%d, count:%d, p:(%s) re:%d\n", inode, index, offset, count, str, read_len);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return read_len;
}

int write_page(page_t *p, ino_t inode, int index, const void *data, int offset, int count) {
    int write = 0;
    pthread_rwlock_wrlock(&p->lock);
    if(p->inode_info == NULL || p->inode_info->inode != inode || p->index != index) {
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
    if(p->inode_info->cache_flag&FILE_CACHE_WRITE_BACK) {
        if(!(p->flag&PAGE_DIRTY) || p->dirty_offset > offset) {
            p->dirty_offset = offset;
        }
        p->flag |= PAGE_DIRTY;
    }
    write = count;

log:
    #ifdef _CFS_DEBUG
    page_str(p);
    log_debug("write_page, inode:%d, index:%d, offset:%d, count:%d, p:(%s), re:%d\n", inode, index, offset, count, str, write);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return write;
}

int flush_page(page_t *p, ino_t inode, int index) {
    ssize_t re = 0;
    pthread_rwlock_wrlock(&p->lock);
    // the page may have been cleared by page allocation
    if(p->inode_info == NULL || p->inode_info->inode != inode || p->index != index) {
        goto log;
    }
    if(p->flag&PAGE_DIRTY) {
        int dirty_len = p->offset + p->len - p->dirty_offset;
        off_t off = (off_t)p->inode_info->c->page_size*p->index + p->dirty_offset;
        re = p->inode_info->write_func(p->inode_info->client_id, p->inode_info->inode, (char *)p->data + p->dirty_offset, dirty_len, off);
        p->dirty_offset = 0;
        p->flag &= ~PAGE_DIRTY;
    }

log:
    #ifdef _CFS_DEBUG
    page_str(p);
    log_debug("flush_page, p:(%s), re:%d\n", str, re);
    #endif
    pthread_rwlock_unlock(&p->lock);
    return re;
}

void clear_page(page_t *p, ino_t inode, int index) {
    pthread_rwlock_wrlock(&p->lock);
    #ifdef _CFS_DEBUG
    page_str(p);
    log_debug("clear_page, p:(%s)\n", str);
    #endif
    clear_page_raw(p, inode, index);
    pthread_rwlock_unlock(&p->lock);
}

void occupy_page(page_t *p, inode_info_t *inode_info, int index) {
    pthread_rwlock_wrlock(&p->lock);
    #ifdef _CFS_DEBUG
    page_str(p);
    log_debug("occupy_page, p:(%s), inode:%d, index:%d\n", str, inode_info->inode, index);
    #endif
    // should not check inode and index here
    clear_page_raw(p, 0, 0);
    p->inode_info = inode_info;
    p->index = index;
    p->last_alloc = time(NULL);
    pthread_rwlock_unlock(&p->lock);
}

void clear_page_raw(page_t *p, ino_t inode, int index) {
    // both file eviction and page allocation will clear page
    // inode 0 means not check
    if(p->inode_info == NULL || (inode > 0 && (p->inode_info->inode != inode || p->index != index))) {
        return;
    }
    if(p->flag&PAGE_DIRTY) {
        int dirty_len = p->offset + p->len - p->dirty_offset;
        off_t off = (off_t)p->inode_info->c->page_size*p->index + p->dirty_offset;
        p->inode_info->write_func(p->inode_info->client_id, p->inode_info->inode, (char *)p->data + p->dirty_offset, dirty_len, off);
    }
    p->inode_info->pages[p->index/PAGES_PER_BLOCK][p->index%PAGES_PER_BLOCK] = NULL;
    p->index = 0;
    p->offset = 0;
    p->dirty_offset = 0;
    p->len = 0;
    p->flag = 0;
    p->last_alloc = 0;
    memset(p->data, 0, p->inode_info->c->page_size);
    p->inode_info = NULL;
}

lru_cache_t *new_lru_cache(int cache_size, int page_size) {
    lru_cache_t *c = (lru_cache_t *)malloc(sizeof(lru_cache_t));
    if(c == NULL) {
        fprintf(stderr, "malloc lru_cache_t failed.\n");
        return NULL;
    }
    c->page_size = page_size;
    c->max_count = cache_size/page_size;
    c->count = 0;
    c->head = NULL;
    pthread_rwlock_init(&c->lock, NULL);
    return c;
}

void release_lru_cache(lru_cache_t *c) {
    if(c == NULL) {
        return;
    }
    page_t *p, *next;
    p = c->head;
    if (p != NULL) {
        p->prev->next = NULL;
    }
    while(p != NULL) {
        next = p->next;
        release_page(p);
        p = next;
    }
    pthread_rwlock_destroy(&c->lock);
    free(c);
}

page_t *lru_cache_alloc(lru_cache_t *c) {
    pthread_rwlock_wrlock(&c->lock);
    page_t *p = NULL;
    if(c->max_count == 0) {
        return p;
    }
    time_t now = time(NULL);
    if(c->count < c->max_count) {
        p = new_page(c->page_size);
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
        available = (p->inode_info == NULL || !(p->inode_info->cache_flag&FILE_CACHE_PRIORITY_HIGH) || now - p->last_alloc > CACHE_TTL);
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

void *do_flush_inode(void *arg) {
    inode_wrapper_t *inode_wrapper = (inode_wrapper_t *)arg;
    pthread_rwlock_t *open_inodes_lock = inode_wrapper->open_inodes_lock;
    auto *open_inodes = inode_wrapper->open_inodes;
    bool *stop = inode_wrapper->stop;
    page_t *p;
    int dirty_num, page_num;
    while(1) {
        if (*stop) {
            break;
        }
        dirty_num = 0;
        page_num = 0;
        vector<page_meta_t> pages;
        pthread_rwlock_rdlock(open_inodes_lock);
        for(const auto &item : *open_inodes) {
            if (*stop) {
                break;
            }
            inode_info_t *inode_info = item.second;
            if (!inode_info->use_pagecache)
                continue;
            for(int i = 0; i < BLOCKS_PER_FILE; i++) {
                if (*stop) {
                    break;
                }
                if(inode_info->pages[i] == NULL) {
                    continue;
                }
                for(int j = 0; j < PAGES_PER_BLOCK; j++) {
                    p = inode_info->pages[i][j];
                    if(p == NULL) {
                        continue;
                    }
                    pages.push_back(page_meta_t{.p = p, .inode = inode_info->inode, .index = i*PAGES_PER_BLOCK + j});
                }
            }
        }
        pthread_rwlock_unlock(open_inodes_lock);

        if (*stop) {
            break;
        }
        for(int i = 0; i < pages.size(); i++) {
            p = pages[i].p;
            if(p->flag&PAGE_DIRTY && p->len == p->inode_info->c->page_size) {
                dirty_num++;
                flush_page(p, pages[i].inode, pages[i].index);
            }
        }

        if (*stop) {
            break;
        }
        if(pages.size() == 0 || dirty_num < pages.size()/BG_FLUSH_SLEEP_THRESHOLD) {
            sleep(BG_FLUSH_SLEEP);
        }
    }
    return NULL;
}

void flush_and_release(map<ino_t, inode_info_t *> &open_inodes) {
    page_t *p;
    for(const auto &item : open_inodes) {
        inode_info_t *inode_info = item.second;
        for(int i = 0; i < BLOCKS_PER_FILE; i++) {
            if (!inode_info->use_pagecache)
                continue;
            if(inode_info->pages[i] == NULL) {
                continue;
            }
            for(int j = 0; j < PAGES_PER_BLOCK; j++) {
                p = inode_info->pages[i][j];
                if(p == NULL) {
                    continue;
                }
                if(p->flag&PAGE_DIRTY) {
                    flush_page(p, inode_info->inode, i*PAGES_PER_BLOCK + j);
                }
            }
        }
        release_inode_info(inode_info);
    }
}

inode_info_t *new_inode_info(ino_t inode, bool use_pagecache, cfs_pwrite_inode_t write_func) {
    inode_info_t *inode_info = (inode_info_t *)malloc(sizeof(inode_info_t));
    if(inode_info == NULL) {
        fprintf(stderr, "malloc inode_info_t failed.\n");
        return NULL;
    }
    memset(inode_info, 0, sizeof(inode_info_t));
    pthread_mutex_init(&inode_info->pages_lock, NULL);
    inode_info->inode = inode;
    inode_info->fd_ref = 1;
    inode_info->write_func = write_func;
    inode_info->use_pagecache = use_pagecache;
    inode_info->c = NULL;
    if(use_pagecache) {
        inode_info->pages = (page_t ***)calloc(BLOCKS_PER_FILE, sizeof(page_t **));
        if(inode_info->pages == NULL) {
            fprintf(stderr, "calloc inode_info->pages failed.\n");
            free(inode_info);
            return NULL;
        }
    } else {
        inode_info->pages = NULL;
    }
    return inode_info;
}

void release_inode_info(inode_info_t *inode_info) {
    if(inode_info == NULL) {
        return;
    }

    pthread_mutex_destroy(&inode_info->pages_lock);
    if(!inode_info->use_pagecache) {
        free(inode_info);
        return;
    }

    page_t *p;
    for(int i = 0; i < BLOCKS_PER_FILE; i++) {
        if(inode_info->pages[i] == NULL) {
            continue;
        }
        for(int j = 0; j < PAGES_PER_BLOCK; j++) {
            p = inode_info->pages[i][j];
            if (p != NULL) {
                clear_page(p, inode_info->inode, i*PAGES_PER_BLOCK + j);
            }
        }
        free(inode_info->pages[i]);
    }
    free(inode_info->pages);
    free(inode_info);
}

size_t read_cache(inode_info_t *inode_info, off_t offset, size_t count, void *data) {
    if (!inode_info->use_pagecache) {
        return 0;
    }

    int page_size = inode_info->c->page_size;
    int index = offset/page_size;
    int block_index;
    int page_offset = offset%page_size;
    size_t page_len = count;
    size_t read = 0;
    page_t *p;

    while(read < count) {
        block_index = index/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE || inode_info->pages[block_index] == NULL) {
            break;
        }
        p = inode_info->pages[block_index][index%PAGES_PER_BLOCK];
        if(p == NULL) {
            break;
        }
        if(page_offset + page_len > page_size) {
            page_len = page_size - page_offset;
        }
        size_t read_len = read_page(p, inode_info->inode, index, (char *)data + read, page_offset, page_len);
        read += read_len;
        if(read_len != page_len) {
            break;
        }
        page_len = count - read;
        page_offset = 0;
        index++;
    }

    #ifdef _CFS_DEBUG
    log_debug("read_cache, inode:%d, offset:%ld, count:%d, re:%d\n", inode_info->inode, offset, count, read);
    #endif
    return read;
}

size_t write_cache(inode_info_t *inode_info, off_t offset, size_t count, const void *data) {
    if (!inode_info->use_pagecache) {
        return 0;
    }
    int page_size = inode_info->c->page_size;
    int index = offset/page_size;
    int block_index;
    int page_offset = offset%page_size;
    size_t page_len = count;
    size_t write = 0;
    page_t *p;

    while(write < count) {
        block_index = index/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }

        pthread_mutex_lock(&inode_info->pages_lock);
        if(inode_info->pages[block_index] == NULL) {
            inode_info->pages[block_index] = (page_t **)calloc(PAGES_PER_BLOCK, sizeof(page_t *));
            if(inode_info->pages[block_index] == NULL) {
                fprintf(stderr, "calloc inode_info->pages[] failed.\n");
                pthread_mutex_unlock(&inode_info->pages_lock);
                break;
            }
        }
        p = inode_info->pages[block_index][index%PAGES_PER_BLOCK];
        bool alloc = (p == NULL);
        if(alloc) {
            p = lru_cache_alloc(inode_info->c);
            if(p == NULL) {
                pthread_mutex_unlock(&inode_info->pages_lock);
                break;
            }
            inode_info->pages[block_index][index%PAGES_PER_BLOCK] = p;
        }
        pthread_mutex_unlock(&inode_info->pages_lock);
        if(alloc) {
            occupy_page(p, inode_info, index);
        }

        if(page_offset + page_len > page_size) {
            page_len = page_size - page_offset;
        }
        size_t write_len = write_page(p, inode_info->inode, index, (char *)data + write, page_offset, page_len);
        write += write_len;
        if(write_len != page_len) {
            break;
        }
        page_len = count - write;
        page_offset = 0;
        index++;
    }

    #ifdef _CFS_DEBUG
    log_debug("write_cache, inode:%d, offset:%ld, count:%d, re:%d\n", inode_info->inode, offset, count, write);
    #endif
    return write;
}

int flush_inode(inode_info_t *inode_info) {
    if (!inode_info->use_pagecache) {
        return 0;
    }
    page_t *p;
    int re = 0;
    int re_tmp;
    for(int i = 0; i < BLOCKS_PER_FILE; i++) {
        if(inode_info->pages[i] == NULL) {
            continue;
        }
        for(int j = 0; j < PAGES_PER_BLOCK; j++) {
            p = inode_info->pages[i][j];
            if(p == NULL || !(p->flag&PAGE_DIRTY)) {
                continue;
            }
            re_tmp = flush_page(p, inode_info->inode, i*PAGES_PER_BLOCK + j);
            if(re_tmp < 0) {
                re = re_tmp;
            }
        }
    }
    return re;
}

void flush_inode_range(inode_info_t *inode_info, off_t offset, size_t count) {
    if (!inode_info->use_pagecache) {
        return;
    }
    int begin_index = offset/inode_info->c->page_size;
    int end_index = (offset + count - 1)/inode_info->c->page_size;
    int block_index;
    page_t *p;
    for(int i = begin_index; i <= end_index; i++) {
        block_index = i/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }
        if(inode_info->pages[block_index] == NULL) {
            continue;
        }
        p = inode_info->pages[block_index][i%PAGES_PER_BLOCK];
        if(p != NULL && p->flag&PAGE_DIRTY) {
            flush_page(p, inode_info->inode, i);
        }
    }
}

void clear_inode_range(inode_info_t *inode_info, off_t offset, size_t count) {
    if (!inode_info->use_pagecache) {
        return;
    }
    int begin_index = offset/inode_info->c->page_size;
    int end_index = (offset + count - 1)/inode_info->c->page_size;
    int block_index;
    page_t *p;
    for(int i = begin_index; i <= end_index; i++) {
        block_index = i/PAGES_PER_BLOCK;
        if(block_index >= BLOCKS_PER_FILE) {
            break;
        }
        if(inode_info->pages[block_index] == NULL) {
            continue;
        }
        p = inode_info->pages[block_index][i%PAGES_PER_BLOCK];
        if(p != NULL) {
            clear_page(p, inode_info->inode, i);
        }
    }
}
