#ifndef OBJECT_POOL_H_
#define OBJECT_POOL_H_

#include <stdlib.h>

typedef void* Item;

/*typedef struct ObjectEntry {
    void *buf;
    uint32_t buf_len;

    //void *remote_buf;
    //uint32_t remote_buf_len;
    struct MemoryEntry* next;

    int index;

    int free;
} ObjectEntry;
*/

typedef struct ObjectPool {

    //ObjectEntry* freeHeadList;
    //ObjectEntry* freeTailList;

    void *original_mem;
    uint32_t original_mem_size;

    struct buddy* allocation;

    //int size_of_type;
    //int avaliable_object_num;
    int block_num;

    //ObjectEntry* allList;
} ObjectPool;

static ObjectPool* InitObjectPool(int block_num, int block_size, int level) {//TODO int maybe too small
    size_t aligned_size,page_size = sysconf(_SC_PAGESIZE);
    ObjectPool * pool = (ObjectPool*)malloc(sizeof(ObjectPool));
    size_t size = block_num * block_size;
    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if(posix_memalign((void**)&pool->original_mem, sysconf(_SC_PAGESIZE), aligned_size)) {
        printf("posix_memalign failed\n");
    }
    pool->original_mem_size = aligned_size;
    printf("ObjectPool mem size: %d\n", pool->original_mem_size);

    memset(pool->original_mem, 0x00, aligned_size);
    /*pool->freeHeadList = (ObjectEntry*)malloc(sizeof(ObjectEntry) * block_num);
    pool->allList = pool->freeHeadList;
    ObjectEntry* entry = pool->freeHeadList;
    for(int i = 0; i < block_num; i++) {
        entry->buf = pool->original_mem + block_size*i;
        entry->buf_len = block_size;
        entry->index = i;
        if (i == block_num -1) {
            entry->next = NULL;
            pool->freeTailList = entry;
            break;
        }

        entry->next = (ObjectEntry*)(entry+1);
        entry++;
    }
    */
    pool->block_num = block_num;
    //pool->avaliable_object_num = block_num;
    printf("ObjectPool block num: %d\n", block_num);
    pool->allocation = buddy_new(level);

    return pool;
}

static void CloseObjectPool(ObjectPool* pool) {
    free(pool->original_mem);
    buddy_delete(pool->allocation);
    free(pool);
}


#endif

