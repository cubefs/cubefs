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
#define FILE_COUNT 6
char *data_origin[FILE_COUNT], *data[FILE_COUNT];
typedef struct {
    int offset;
    short len;
    unsigned char type;
    char *data;
} operation_t;
operation_t operation[TEST_ROUND];

ssize_t pwrite_func(int64_t id, int fd, const void *buf, size_t count, off_t offset) {
    memcpy(data[fd-1] + offset, buf, count);
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
    cfs_file_t *f = (cfs_file_t *)arg;
    int offset, len;
    char *d;
    for(int i = 0; i < TEST_ROUND; i++) {
        offset = operation[i].offset;
        len = operation[i].len;
        d = operation[i].data;
        if(operation[i].type == 0) {
            memcpy(data_origin[f->fd-1] + offset, d, len);
            int re = write_cache(f, offset, len, d);
            if(f->cache_flag&FILE_CACHE_WRITE_THROUGH || re < len) {
                if(re < len) {
                    clear_file_range(f, offset, len);
                }
                f->write_func(f->client_id, f->fd, d, len, offset);
            }
        } else {
            char *buf = (char *)malloc(len);
            int re = read_cache(f, offset, len, buf);
            char *buf_cmp = buf;
            if(re < len) {
                flush_file_range(f, offset, len);
                buf_cmp = data[f->fd-1] + offset;
            }
            if(memcmp(data_origin[f->fd-1]+offset, buf_cmp, re)) {
                printf("read inconsistency, fd:%d, offset:%d, len:%d\n", f->fd, offset, len);
                exit(0);
            }
            free(buf);
        }
    }
    return NULL;
}

void test_file_operation() {
    produce_operation();
    int size = PAGE_SIZE*PAGE_COUNT;
    for(int i = 0; i < FILE_COUNT; i++) {
        data_origin[i] = (char *)malloc(size);
    }
    for(int i = 0; i < size/sizeof(int); i++) {
        *(int *)(data_origin[0] + i*sizeof(int)) = rand();
    }
    for(int i = 1; i < FILE_COUNT; i++) {
        memcpy(data_origin[i], data_origin[0], size);
    }

    lru_cache_t *c = new_lru_cache(size, PAGE_SIZE);
    cfs_file_t *f[FILE_COUNT];
    map<int, cfs_file_t *> open_file;
    int fd;
    for(int i = 0; i < FILE_COUNT; i++) {
        data[i] = (char *)malloc(size);
        memcpy(data[i], data_origin[0], size);
        f[i] = new_file(&pwrite_func);
        init_file(f[i]);
        f[i]->c = c;
        fd = i + 1;
        f[i]->fd = fd;
        f[i]->inode = fd;
        f[i]->cache_flag |= FILE_CACHE_WRITE_BACK;
        if(i == 0) {
            f[i]->cache_flag |= FILE_CACHE_PRIORITY_HIGH;
        }
        open_file[fd] = f[i];
    }

    bg_flush(&open_file);
    pthread_t thd[FILE_COUNT];
    for(int i = 0; i < FILE_COUNT; i++) {
        pthread_create(&thd[i], NULL, execute_operation, f[i]);
    }
    for(int i = 0; i < FILE_COUNT; i++) {
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
    //test_lru_cache();
    test_file_operation();
    //test_read_cache();
}