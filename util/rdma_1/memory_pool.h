#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_
#include <stdlib.h>
#include "buddy.h"
#include "rdma.h"



static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;

typedef struct MemoryPool {
    void*  original_mem;
    struct buddy* allocation;
    int size;
    struct ibv_pd* pd;
    struct ibv_mr* mr;
} MemoryPool;

static struct ibv_pd* alloc_pd();
static struct ibv_mr* regist_mr(MemoryPool*, struct ibv_pd*);
static void dealloc_pd(struct ibv_pd*);
static void dereg_mr(struct ibv_mr*);

//static void initParam(int block_size, int block_num) {
//    MEMORY_BLOCK_SIZE  = block_size;
//    MEMORY_BLOCK_COUNT = block_num;
//}

//static int AllocAlignMemory(void** mem_addr) {
//    int len = MEMORY_BLOCK_SIZE * MEMORY_BLOCK_COUNT;
//    int ret = posix_memalign(mem_addr, sysconf(_SC_PAGESIZE), len);
//    return len;
//}

static MemoryPool* InitMemoryPool(int block_num, int block_size, int level) {
    struct ibv_pd* pd = NULL;
    struct ibv_mr* mr = NULL;

    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));//TODO 没有考虑返回值为NULL的情况
    //initParam(block_size, block_num);
    pool->size = block_num * block_size;
    posix_memalign((void**)(&(pool->original_mem)), sysconf(_SC_PAGESIZE), pool->size);
    //int size = AllocAlignMemory((void**)(&(pool->original_mem)));
    //pool->size = size;
    printf("memoryPool size: %d\n", pool->size);
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

static void CloseMemoryPool(MemoryPool* pool) {
    dealloc_pd(pool->pd);
    dereg_mr(pool->mr);

    free(pool->original_mem);
    buddy_delete(pool->allocation);

    free(pool);
}

#endif