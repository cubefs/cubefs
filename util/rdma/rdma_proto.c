#include "rdma_proto.h"

int WQ_DEPTH = 32;
int WQ_SG_DEPTH = 2;
int MIN_CQE_NUM = 1024;

struct rdma_pool *rdma_pool = NULL;
struct rdma_pool_config *rdma_pool_config = NULL;
FILE *fp = NULL;
struct net_env_st *g_net_env = NULL;