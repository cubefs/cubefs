#include "memory_pool.h"


struct ibv_pd* alloc_pd() {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if(dev_list == NULL) {
        //TODO 获取设备列表失败的处理逻辑
        return NULL;
    }
    struct ibv_device *selected_device = dev_list[0];
    ibv_free_device_list(dev_list);
    struct ibv_context *context =  ibv_open_device(selected_device);
    if(context == NULL) {
        //TODO 打开设备失败的处理逻辑
        return NULL;
    }

    struct ibv_pd *pd = ibv_alloc_pd(context);

    if (!pd) {
        //serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        //TODO error handler
        printf("RDMA: ibv alloc pd failed\n");
        return NULL;
    }
    
    
    return pd;
}

void dealloc_pd(struct ibv_pd* pd) {
    if(pd) {//TODO if pd is NULL
        ibv_dealloc_pd(pd);
    }
    return;
}

struct ibv_mr* regist_mr(MemoryPool* pool, struct ibv_pd* pd) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    struct ibv_mr *mr = ibv_reg_mr(pd, pool->original_mem, pool->size, access);
    if (!mr) {//TODO error handler
        //serverLog(LL_WARNING, "RDMA: reg mr for recv buffer failed");
        //TODO error handler
        printf("RDMA: reg mr for recv data buffer failed");
        //goto destroy_iobuf;
        //TODO clear resources
        return NULL;
    }
    return mr;
}

void dereg_mr(struct ibv_mr* mr) {
    if (mr) {//TODO if mr = NULL
        ibv_dereg_mr(mr);
    }
    return;
}

MemoryPool* InitMemoryPool(int block_num, int block_size, int level) {
    struct ibv_pd* pd = NULL;
    struct ibv_mr* mr = NULL;

    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));//TODO 没有考虑返回值为NULL的情况
    pool->size = block_num * block_size;
    posix_memalign((void**)(&(pool->original_mem)), sysconf(_SC_PAGESIZE), pool->size);
    printf("memoryPool size: %ld\n", pool->size);
    pool->allocation = buddy_new(level);//TODO 没有考虑返回值为NULL的情况
    pd = alloc_pd();
    if(!pd) {
        //TODO error handler 
        //free memory
        return NULL;
    }
    pool->pd = pd;
    mr = regist_mr(pool, pd);
    if(!mr) {
        //TODO error handler
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

