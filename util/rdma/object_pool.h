#ifndef OBJECT_POOL_H
#define OBJECT_POOL_H

#include "buddy.h"
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

typedef void* Item;

typedef struct ObjectPool {
    void *original_mem;
    uint32_t original_mem_size;
    struct buddy* allocation;
    int block_num;
} ObjectPool;


ObjectPool* InitObjectPool(int block_num, int block_size, int level);

void CloseObjectPool(ObjectPool* pool);

#endif