#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>

#include "buddy.h"
#include "log.h"

static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;

typedef struct memory_pool {
    char*  original_mem;
    struct buddy* allocation;
    uint64_t size;
    struct ibv_pd* pd;
    struct ibv_mr* mr;
} memory_pool;

void *page_aligned_zalloc(size_t size);

memory_pool* init_memory_pool(int block_num, int block_size, int level, struct ibv_pd* pd);

void close_memory_pool(memory_pool* pool);

#endif