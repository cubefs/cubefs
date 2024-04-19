#include "memory_pool.h"

/*
struct ibv_pd* alloc_pd() {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if(dev_list == NULL) {
        return NULL;
    }
    struct ibv_device *selected_device = dev_list[0];
    ibv_free_device_list(dev_list);
    struct ibv_context *context =  ibv_open_device(selected_device);
    if(context == NULL) {
        return NULL;
    }
    struct ibv_pd *pd = ibv_alloc_pd(context);
    if (!pd) {
        //printf("RDMA: ibv alloc pd failed\n");
        return NULL;
    }
    return pd;
}

void dealloc_pd(struct ibv_pd* pd) {
    if(pd) {
        ibv_dealloc_pd(pd);
    }
    return;
}

struct ibv_mr* regist_mr(MemoryPool* pool, struct ibv_pd* pd) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    struct ibv_mr *mr = ibv_reg_mr(pd, pool->original_mem, pool->size, access);
    if (!mr) {
        //printf("RDMA: reg mr for recv data buffer failed");
        return NULL;
    }
    return mr;
}

void dereg_mr(struct ibv_mr* mr) {
    if (mr) {
        ibv_dereg_mr(mr);
    }
    return;
}
*/

memory_pool* init_memory_pool(int block_num, int block_size, int level, struct ibv_pd* pd) {
    memory_pool * pool = (memory_pool*)malloc(sizeof(memory_pool));
    if (pool == NULL) {
        return NULL;
    }
    pool->size = (int64_t)block_num * (int64_t)block_size;
    posix_memalign((void**)(&(pool->original_mem)), sysconf(_SC_PAGESIZE), pool->size);
    if (pool->original_mem == NULL) {
        log_debug("malloc pool original memory failed");
        goto err_free_pool;
    }
    memset(pool->original_mem, 0x00, pool->size);
    pool->allocation = buddy_new(level);
    pool->pd = pd;
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    pool->mr = ibv_reg_mr(pool->pd, pool->original_mem, pool->size, access);
    if (pool->mr == NULL) {
        log_debug("RDMA: reg mr for data buffer failed");
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

