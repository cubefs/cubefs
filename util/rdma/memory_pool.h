#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include "buddy.h"
#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>

static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;

typedef struct MemoryPool {
    void*  original_mem;
    struct buddy* allocation;
    int size;
    struct ibv_pd* pd;
    struct ibv_mr* mr;
} MemoryPool;

MemoryPool* InitMemoryPool(int block_num, int block_size, int level);

void CloseMemoryPool(MemoryPool* pool);

#endif