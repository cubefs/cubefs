#include "object_pool.h"


ObjectPool* InitObjectPool(int block_num, int block_size, int level) {//TODO int maybe too small
    size_t aligned_size,page_size = sysconf(_SC_PAGESIZE);
    ObjectPool * pool = (ObjectPool*)malloc(sizeof(ObjectPool));
    size_t size = block_num * block_size;
    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if(posix_memalign((void**)&pool->original_mem, sysconf(_SC_PAGESIZE), aligned_size)) {
        //printf("posix_memalign failed\n");
    }
    pool->original_mem_size = aligned_size;
    //printf("ObjectPool mem size: %d\n", pool->original_mem_size);

    memset(pool->original_mem, 0x00, aligned_size);
    pool->block_num = block_num;
    //printf("ObjectPool block num: %d\n", block_num);
    pool->allocation = buddy_new(level);

    return pool;
}

void CloseObjectPool(ObjectPool* pool) {
    free(pool->original_mem);
    buddy_delete(pool->allocation);
    free(pool);
}



