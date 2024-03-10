#ifndef RDMA_POOL_H
#define RDMA_POOL_H

#include "rdma_proto.h"

#define C_OK 1
#define C_ERR 0

typedef struct RdmaPool {
    MemoryPool *memoryPool;
    ObjectPool *headerPool;
    ObjectPool *responsePool;
};

typedef struct RdmaPoolConfig {
    int memBlockNum;
    int memBlockSize;
    int memPoolLevel;
    
    int headerBlockNum;
    int headerPoolLevel;

    int responseBlockNum;
    int responsePoolLevel;

    int wqDepth;
    int minCqeNum;
};

extern struct RdmaPool *rdmaPool;
extern struct RdmaPoolConfig *rdmaPoolConfig;

struct RdmaPoolConfig* getRdmaPoolConfig();

void destroyRdmaPool();

int initRdmaPool(struct RdmaPoolConfig* config);

#endif