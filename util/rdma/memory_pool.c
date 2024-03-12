#include "memory_pool.h"


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

MemoryPool* InitMemoryPool(int block_num, int block_size, int level) {
    struct ibv_pd* pd = NULL;
    struct ibv_mr* mr = NULL;

    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));
    pool->size = (int64_t)block_num * (int64_t)block_size;
    posix_memalign((void**)(&(pool->original_mem)), sysconf(_SC_PAGESIZE), pool->size);
    pool->allocation = buddy_new(level);
    pd = alloc_pd();
    if(!pd) {
        return NULL;
    }
    pool->pd = pd;
    mr = regist_mr(pool, pd);
    if(!mr) {
        return NULL;
    }
    pool->mr = mr;
    return pool;
}

void CloseMemoryPool(MemoryPool* pool) {
    dealloc_pd(pool->pd);
    dereg_mr(pool->mr);
    free(pool->original_mem);
    buddy_delete(pool->allocation);
    free(pool);
}

