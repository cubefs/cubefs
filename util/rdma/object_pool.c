#include "object_pool.h"


object_pool* init_object_pool(int block_num, int block_size, int level) {
    size_t aligned_size,page_size = sysconf(_SC_PAGESIZE);
    object_pool * pool = (object_pool*)malloc(sizeof(object_pool));
    if (pool == NULL) {
        log_debug("malloc object pool failed");
        return NULL;
    }
    size_t size = block_num * block_size;
    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    posix_memalign((void**)&pool->original_mem, sysconf(_SC_PAGESIZE), aligned_size);
    if (pool->original_mem == NULL) {
        log_debug("malloc pool original memory failed");
        goto err_free_pool;
    }
    pool->original_mem_size = aligned_size;
    memset(pool->original_mem, 0x00, aligned_size);
    pool->block_num = block_num;
    pool->allocation = buddy_new(level);
    return pool;
err_free_pool:
    free(pool);
    return NULL;
}

void close_object_pool(object_pool* pool) {
    if (pool != NULL) {
            free(pool->original_mem);
            buddy_delete(pool->allocation);
            free(pool);
    }
}



