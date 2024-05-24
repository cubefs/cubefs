#include "memory_pool.h"

void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        log_error("posix_memalign failed");
        return tmp;
    }

    memset(tmp, 0x00, aligned_size);

    return tmp;
}

memory_pool* init_memory_pool(int block_num, int block_size, int level, struct ibv_pd* pd) {
    memory_pool * pool = (memory_pool*)malloc(sizeof(memory_pool));
    if (pool == NULL) {
        return NULL;
    }
    pool->size = (uint64_t)block_num * (uint64_t)block_size;
    pool->original_mem = page_aligned_zalloc(pool->size);
    if (pool->original_mem == NULL) {
        log_error("malloc pool original memory failed");
        goto err_free_pool;
    }
    pool->allocation = buddy_new(level);
    pool->pd = pd;
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    pool->mr = ibv_reg_mr(pool->pd, pool->original_mem, pool->size, access);
    if (pool->mr == NULL) {
        log_error("RDMA: reg mr for data buffer failed, errno:%d", errno);
        goto err_delete_buddy;
    }
    return pool;
err_delete_buddy:
    buddy_delete(pool->allocation);
    free(pool->original_mem);
err_free_pool:
    free(pool);
    return NULL;
}

void close_memory_pool(memory_pool* pool) {
    if (pool != NULL) {
        if (pool->mr != NULL) {
            ibv_dereg_mr(pool->mr);
        }
        pool->pd = NULL;
        if (pool->allocation != NULL) {
            buddy_delete(pool->allocation);
        }
        if (pool->original_mem != NULL) {
            free(pool->original_mem);
        }
        free(pool);
    }
}

