#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <map>
#include <vector>
#include "cache.h"

// g++ cache_test.c cache.c -g -I include -lpthread
using namespace std;

#define clean_errno() (errno == 0 ? "None" : strerror(errno))
#define log_error(M, ...) fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define assertf(A, M, ...) if(!(A)) {log_error(M, ##__VA_ARGS__); assert(A); }

void test_lru_cache() {
    int len = 3;
    lru_cache_t *c = new_lru_cache(BIG_PAGE_SIZE*len, BIG_PAGE_SIZE);
    page_t *p;
    for(int i = 1; i <= len + 1; i++) {
        p = lru_cache_alloc(c);
        p->index = i;
    }
    lru_cache_access(c, c->head->next);
    lru_cache_access(c, c->head);

    p = c->head;
    vector<int> re;
    vector<int> expect{ 4, 3, 2 };
    while(1) {
        re.push_back(p->index);
        p = p->next;
        if(p == c->head) {
            break;
        }
    }
    assert(re == expect);

    p = c->head->prev;
    re.clear();
    reverse(expect.begin(), expect.end());
    while(1) {
        re.push_back(p->index);
        p = p->prev;
        if(p == c->head->prev) {
            break;
        }
    }
    assert(re == expect);
}

#define PAGE_SIZE 1024
#define PAGE_COUNT 1024
#define TEST_ROUND 1000*1000*10
#define INODE_COUNT 6
char *data_origin[INODE_COUNT], *data[INODE_COUNT];
typedef struct {
    int offset;
    short len;
    unsigned char type;
    char *data;
} operation_t;
operation_t operation[TEST_ROUND];

ssize_t pwrite_func(int64_t id, ino_t ino, const void *buf, size_t count, off_t offset) {
    memcpy(data[ino-1] + offset, buf, count);
    return count;
}

void produce_operation() {
    int size = PAGE_SIZE*PAGE_COUNT;
    for(int i = 0; i < TEST_ROUND; i++) {
        operation[i].offset = rand() % size;
        operation[i].len = rand() % PAGE_SIZE;
        operation[i].type = i % 2;
        operation[i].data = (char *)malloc(operation[i].len);
        for(int j = 0; j < operation[i].len/sizeof(int); j++) {
            *(int *)(operation[i].data + j*sizeof(int)) = rand();
        }
    }
}

void *execute_operation(void *arg) {
    inode_shared_t *inode_info = (inode_shared_t *)arg;
    int offset, len;
    char *d;
    for(int i = 0; i < TEST_ROUND; i++) {
        offset = operation[i].offset;
        len = operation[i].len;
        d = operation[i].data;
        if(operation[i].type == 0) {
            memcpy(data_origin[inode_info->inode-1] + offset, d, len);
            int re = write_cache(inode_info, offset, len, d);
            if(inode_info->cache_flag&FILE_CACHE_WRITE_THROUGH || re < len) {
                if(re < len) {
                    clear_inode_range(inode_info, offset, len);
                }
                inode_info->write_func(inode_info->client_id, inode_info->inode, d, len, offset);
            }
        } else {
            char *buf = (char *)malloc(len);
            int re = read_cache(inode_info, offset, len, buf);
            char *buf_cmp = buf;
            if(re < len) {
                flush_inode_range(inode_info, offset, len);
                buf_cmp = data[inode_info->inode-1] + offset;
            }
            if(memcmp(data_origin[inode_info->inode-1]+offset, buf_cmp, re)) {
                printf("read inconsistency, inode:%d, offset:%d, len:%d\n", inode_info->inode, offset, len);
                exit(0);
            }
            free(buf);
        }
    }
    return NULL;
}

void test_inode_operation() {
    produce_operation();
    int size = PAGE_SIZE*PAGE_COUNT;
    for(int i = 0; i < INODE_COUNT; i++) {
        data_origin[i] = (char *)malloc(size);
    }
    for(int i = 0; i < size/sizeof(int); i++) {
        *(int *)(data_origin[0] + i*sizeof(int)) = rand();
    }
    for(int i = 1; i < INODE_COUNT; i++) {
        memcpy(data_origin[i], data_origin[0], size);
    }

    lru_cache_t *c = new_lru_cache(size, PAGE_SIZE);
    inode_shared_t *inodes[INODE_COUNT];
    map<ino_t, inode_shared_t *> open_inodes;
    pthread_rwlock_t open_inodes_lock;
    pthread_rwlock_init(&open_inodes_lock, NULL);
    bool stop;
    ino_t ino;
    for(int i = 0; i < INODE_COUNT; i++) {
        data[i] = (char *)malloc(size);
        memcpy(data[i], data_origin[0], size);
        inodes[i] = new_inode_info(i+1, true, &pwrite_func);
        inodes[i]->c = c;
        inodes[i]->cache_flag |= FILE_CACHE_WRITE_BACK;
        if(i == 0) {
            inodes[i]->cache_flag |= FILE_CACHE_PRIORITY_HIGH;
        }
        open_inodes[i+1] = inodes[i];
    }

    struct inode_wrapper_t wrapper = {&open_inodes_lock, &open_inodes, &stop};
    pthread_t pid;
    pthread_create(&pid, NULL, do_flush_inode, &wrapper);
    pthread_t thd[INODE_COUNT];
    for(int i = 0; i < INODE_COUNT; i++) {
        pthread_create(&thd[i], NULL, execute_operation, inodes[i]);
    }
    for(int i = 0; i < INODE_COUNT; i++) {
        pthread_join(thd[i], NULL);
    }
}

void test_read_cache() {
    const char *path = "/export/data/mysql/relay-bin.0";
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    int len = BIG_PAGE_SIZE;
    void *buf = malloc(len);
    int count = 1000;
    struct timespec start, stop;
    clock_gettime(CLOCK_REALTIME, &start);
    for(int i = 0; i < count; i++) {
        write(fd, buf, 100);
    }
    clock_gettime(CLOCK_REALTIME, &stop);
    long time1 = (stop.tv_sec - start.tv_sec)*1000000000 + stop.tv_nsec - start.tv_nsec;

    clock_gettime(CLOCK_REALTIME, &start);
    for(int i = 0; i < count; i++) {
        pread(fd, buf, len, 0);
    }
    clock_gettime(CLOCK_REALTIME, &stop);
    long time2 = (stop.tv_sec - start.tv_sec)*1000000000 + stop.tv_nsec - start.tv_nsec;
    printf("times: %d, write avg: %d ns, read avg: %d ns\n", count, time1/count, time2/count);
}

int main() {
    test_lru_cache();
    test_inode_operation();
    test_read_cache();
}
