#ifndef OBJECT_POOL_H
#define OBJECT_POOL_H

#include "buddy.h"
#include "log.h"
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>

//typedef void* Item;

typedef struct object_pool {
    void *original_mem;
    uint32_t original_mem_size;
    struct buddy *allocation;
    int block_num;
} object_pool;


object_pool* init_object_pool(int block_num, int block_size, int level);

void close_object_pool(object_pool* pool);

#endif