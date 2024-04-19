#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include "buddy.h"
#include "log.h"
#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>

static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;

typedef struct memory_pool {
    void*  original_mem;
    struct buddy* allocation;
    int64_t size;
    struct ibv_pd* pd;
    struct ibv_mr* mr;
} memory_pool;

memory_pool* init_memory_pool(int block_num, int block_size, int level, struct ibv_pd* pd);

void close_memory_pool(memory_pool* pool);

#endif