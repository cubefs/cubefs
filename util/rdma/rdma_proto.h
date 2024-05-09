#ifndef RDMA_PROTO_H
#define RDMA_PROTO_H

#include "memory_pool.h"
#include "object_pool.h"

#include "hashmap.h"
#include "queue.h"
#include "log.h"
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
//#include "wait_group.h"
#include <stdbool.h>
#include <netdb.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <semaphore.h>

#define C_OK 0
#define C_ERR 1
#define RDMA_INVALID_OPCODE 0xffff

#define RDMA_MAX_WQE 1024
#define SERVER_MAX_CONN 32

#define CONN_ID_BIT_LEN         32
#define WORKER_ID_BIT_LEN       8
#define CONN_TYPE_BIT_LEN       8
#define CONN_ID_MASK_BIT_LEN    8

static const int TIMEOUT_IN_MS = 500;

typedef void *event_callback(void *ctx);

void *cm_thread(void *ctx);
void *cq_thread(void *ctx);

extern int WQ_DEPTH;
extern int WQ_SG_DEPTH;
extern int MIN_CQE_NUM;

extern struct rdma_pool *rdma_pool;
extern struct rdma_pool_config *rdma_pool_config;
extern FILE *fp;
extern struct net_env_st *g_net_env;

struct rdma_pool {
    memory_pool *memory_pool;
    object_pool *header_pool;
    object_pool *response_pool;
};

struct rdma_pool_config {
    int mem_block_num;
    int mem_block_size;
    int mem_pool_level;

    int header_block_num;
    int header_pool_level;

    int response_block_num;
    int response_pool_level;

    int wq_depth;
    int min_cqe_num;
    int enable_rdma_log;
    int worker_num;
};

typedef enum conn_type_bit_mask {
    CONN_SERVER_BIT = 1 << 7,
    CONN_ACTIVE_BIT = 1 << 6,
} conn_type_mask;

typedef enum id_gen_enum {
    ID_GEN_CTRL,
    ID_GEN_DATA,
    ID_GEN_MAX
} IdGenEnum;

struct conn_nd_t {
    uint64_t id:32;//CONN_ID_BIT_LEN;                //32
    uint64_t worker_id:8;//WORKER_ID_BIT_LEN;       //8
    uint64_t type:8;//CONN_TYPE_BIT_LEN;            //8 RDMA/TCP Server/Conn ACtive/Passive
    uint64_t m2:8;//CONN_ID_MASK_BIT_LEN;           //8 b
    uint64_t m1:8;//CONN_ID_MASK_BIT_LEN;           //8 c
};

union conn_nd_union {
    struct conn_nd_t nd_;
    uint64_t nd;
};

typedef struct worker {
    struct ibv_pd      *pd;
    struct ibv_cq      *cq;
    struct ibv_comp_channel   *comp_channel;
    pthread_t  cq_poller_thread;
    pthread_spinlock_t nd_map_lock;
    khash_t(map)       *nd_map;
    khash_t(map)       *closing_nd_map;
    pthread_spinlock_t lock;
    Queue              *conn_list;
    uint8_t          id;
    uint32_t         qp_cnt;
    pthread_t        w_pid;
    int              close;
} worker;

struct net_env_st {
    uint8_t             worker_num;
    int8_t              pad[6];

    struct ibv_context  **all_devs;
    struct ibv_context  *ctx;
    struct ibv_pd       *pd;

    struct rdma_event_channel *event_channel;
    pthread_t                 cm_event_loop_thread;

    pthread_spinlock_t  lock;

    uint32_t            server_cnt;
    int32_t             ib_dev_cnt;
    pthread_spinlock_t  server_lock;
    khash_t(map)        *server_map;
    uint32_t            id_gen[ID_GEN_MAX];
    int                 close;
    worker              worker[];
};

typedef struct request_header {//__attribute__((packed))
    uint8_t              magic;
    uint8_t              extent_type;// the highest bit be set while rsp to client if version not consistent then Verseq be valid
    uint8_t              opcode;
    uint8_t              result_code;
    uint8_t              remaining_followers;
    uint32_t             crc;
    uint32_t             size;
    uint32_t             arg_len;
    uint64_t             partition_id;
    uint64_t             extent_id;
    int64_t              extent_offset;
    int64_t              req_id;
    uint64_t             kernel_offset;
    uint64_t             ver_seq;// only used in mod request to datanode
    unsigned char        arg[40];// for create or append ops, the data contains the address
    unsigned char        list[40];
    uint8_t              rdma_version;//rdma协议版本
    uint64_t             rdma_addr;
    uint32_t             rdma_length;
    uint32_t             rdma_key;
}__attribute__((packed)) header;//

typedef struct request_response {
    uint8_t              magic;
    uint8_t              extent_type;// the highest bit be set while rsp to client if version not consistent then Verseq be valid
    uint8_t              opcode;
    uint8_t              result_code;
    uint8_t              remaining_followers;
    uint32_t             crc;
    uint32_t             size;
    uint32_t             arg_len;
    uint64_t             partition_id;
    uint64_t             extent_id;
    int64_t              extent_offset;
    int64_t              req_id;
    uint64_t             kernel_offset;
    uint64_t             ver_seq;// only used in mod request to datanode
    unsigned char        arg[40];// for create or append ops, the data contains the address
    unsigned char        data[40];
    unsigned char        list[40];
    uint8_t              rdma_version;//rdma协议版本
    uint64_t             rdma_addr;
    uint32_t             rdma_length;
    uint32_t             rdma_key;
} response;

typedef struct memory_entry {
    void* header_buff;
    void* data_buff;
    void* response_buff;
    uint32_t data_len;
    uint32_t header_len;
    uint32_t response_len;
    bool     is_response;
    uint64_t nd;
} memory_entry;

typedef enum connection_state {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_CONNECTED,
    CONN_STATE_ERROR,
    CONN_STATE_DISCONNECTING,
    CONN_STATE_DISCONNECTED
} connection_state;

typedef enum connection_type {
    CONN_TYPE_SERVER = 1,
    CONN_TYPE_CLIENT
} connection_type;

typedef struct connection {
    uint64_t nd;
    char   local_addr[INET_ADDRSTRLEN + NI_MAXSERV];
    char   remote_addr[INET_ADDRSTRLEN + NI_MAXSERV];
    int     conn_type;
    struct rdma_cm_id * cm_id;
    struct ibv_qp *qp;
    struct ibv_mr *mr;
    memory_pool *pool;
    object_pool* header_pool;
    object_pool* response_pool;
    header *header_buf;
    struct ibv_mr *header_mr;
    response *response_buf;
    struct ibv_mr *response_mr;
    Queue *free_list;
    Queue *msg_list;
    void* context;
    void* conn_context;
    connection_state state;
    int connect_fd;
    int msg_fd;
    int close_fd;
    pthread_spinlock_t spin_lock;
    int64_t send_timeout_ns;
    int64_t recv_timeout_ns;
    worker *worker;
} connection;

struct rdma_listener {
    uint64_t nd;
    struct rdma_cm_id *listen_id;
    char* ip;
    char* port;
    pthread_spinlock_t conn_lock;
    khash_t(map) *conn_map;
    Queue *wait_conns;
    int connect_fd;//sem_t*
};

static int get_header_size() {
    return sizeof(struct request_header);
}

static int get_response_size() {
    return sizeof(struct request_response);
}

static uint64_t allocate_nd(int type) {
    int id_index = ID_GEN_CTRL;
    union conn_nd_union id;

    id_index += 1;
    id.nd_.worker_id = __sync_fetch_and_add((g_net_env->id_gen + id_index), 1) & 0xFF;
    id.nd_.type = type & 0xFF;
    id.nd_.m1 = 'c';
    id.nd_.m2 = 'b';
    id.nd_.id = __sync_fetch_and_add((g_net_env->id_gen + ID_GEN_MAX -1), 1);
    return id.nd;
}

static void cbrdma_parse_nd(uint64_t nd, int *id, int * worker_id, int * is_server, int * is_active) {
    *id = (nd & 0xFFFFFFFF);
    *worker_id = ((nd >> 32) & 0xFF);
    uint8_t type  = (((nd >> 32) & 0xFF00) >> 8);
    *is_server = type & 0x80;
    *is_active = type & 0x40;
}

static struct rdma_pool_config* get_rdma_pool_config() {
    rdma_pool_config = (struct rdma_pool_config*)malloc(sizeof(struct rdma_pool_config));
    memset(rdma_pool_config, 0, sizeof(struct rdma_pool_config));
    rdma_pool_config->mem_block_num = 8 * 5 * 1024;//
    rdma_pool_config->mem_block_size = 65536 * 2;
    rdma_pool_config->mem_pool_level = 18;
    rdma_pool_config->header_block_num = 32 * 1024;
    rdma_pool_config->header_pool_level = 15;
    rdma_pool_config->response_block_num = 32 * 1024;
    rdma_pool_config->response_pool_level = 15;
    rdma_pool_config->wq_depth = 32;
    rdma_pool_config->min_cqe_num = 1024;
    rdma_pool_config->enable_rdma_log = 0;
    rdma_pool_config->worker_num = 32;
    return rdma_pool_config;
}

static int init_worker(worker *worker, event_callback cb) {
    int ret = 0;
    pthread_attr_t attr;
    struct sched_param param;
    int policy;

    worker->pd = g_net_env->pd;
    //log_debug("ibv_alloc_pd:%p", worker->pd);
    worker->comp_channel = ibv_create_comp_channel(g_net_env->ctx);
    if (worker->comp_channel == NULL) {
        log_debug("ibv create comp channel failed\n");
        return 0;
    }
    //log_debug("ibv_create_comp_channel:%p",worker->comp_channel);
    worker->cq = ibv_create_cq(g_net_env->ctx, MIN_CQE_NUM, NULL, worker->comp_channel, 0);
    if (worker->cq == NULL) {
        //return assert,ignore resource free
        log_debug("create cq failed, errno:%d", errno);
        goto err_destroy_compchannel;
    }
    //log_debug("ibv_create_cq:%p", worker->cq);
    ibv_req_notify_cq(worker->cq, 0);

    ret = pthread_spin_init(&(worker->lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_debug("init worker spin task_lock failed, err:%d", ret);
        goto err_destroy_cq;
    }
    ret = pthread_spin_init(&(worker->nd_map_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_debug("init worker spin lock failed, err:%d", ret);
        goto err_destroy_workerlock;
    }
    worker->nd_map = hashmap_create();
    worker->closing_nd_map = hashmap_create();
    //list_head_init(&worker->conn_list);
    worker->conn_list = InitQueue();
    if (worker->conn_list == NULL) {
        log_debug("init worker conn list failed");
        goto err_destroy_map;
    }
    worker->w_pid = 0;

    pthread_attr_init(&attr);
    pthread_attr_getschedpolicy(&attr, &policy);
    //pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    policy = SCHED_FIFO;
    pthread_attr_setschedpolicy(&attr, policy);
    param.sched_priority = sched_get_priority_max(policy);
    pthread_attr_setschedparam(&attr, &param);

    pthread_create(&worker->cq_poller_thread, &attr, cb, worker);
    return C_OK;
err_destroy_map:
    hashmap_destroy(worker->closing_nd_map);
    hashmap_destroy(worker->nd_map);
    pthread_spin_destroy(&worker->nd_map_lock);
err_destroy_workerlock:
    pthread_spin_destroy(&worker->lock);
err_destroy_cq:
    ibv_destroy_cq(worker->cq);
err_destroy_compchannel:
    ibv_destroy_comp_channel(worker->comp_channel);
    return C_ERR;
}

static void destroy_worker(worker *worker) {
    worker->close = 1;
    pthread_join(worker->cq_poller_thread, NULL);
    worker->w_pid = 0;

    if (worker->conn_list != NULL) {
        DestroyQueue(worker->conn_list);
        worker->conn_list = NULL;
    }
    if (worker->closing_nd_map != NULL) {
        hashmap_destroy(worker->closing_nd_map);
        worker->closing_nd_map = NULL;
    }
    if (worker->nd_map != NULL) {
        hashmap_destroy(worker->nd_map);
        worker->nd_map = NULL;
    }
    pthread_spin_destroy(&worker->nd_map_lock);
    pthread_spin_destroy(&worker->lock);

    if (worker->cq != NULL) {
        log_debug("ibv_destroy_cq:%p", worker->cq);
        ibv_destroy_cq(worker->cq);
        worker->cq = NULL;
    }
    if(worker->comp_channel != NULL) {
        log_debug("ibv_destroy_comp_channel:%p", worker->comp_channel);
        ibv_destroy_comp_channel(worker->comp_channel);
        worker->comp_channel = NULL;
    }

    worker->pd = NULL;
}

static void destroy_rdma_env() {
    if (g_net_env != NULL) {
        for (int i = 0; i < g_net_env->worker_num; i++) {
            destroy_worker(g_net_env->worker + i);
        }

        if (g_net_env->event_channel != NULL) {
            rdma_destroy_event_channel(g_net_env->event_channel);
            g_net_env->event_channel = NULL;
        }

        g_net_env->close = 1;
        pthread_join(g_net_env->cm_event_loop_thread, NULL);

        if (g_net_env->all_devs != NULL) {
            rdma_free_devices(g_net_env->all_devs);
            g_net_env->all_devs = NULL;
        }

        pthread_spin_destroy(&g_net_env->server_lock);

        hashmap_destroy(g_net_env->server_map);

        free(g_net_env);
        g_net_env = NULL;
    }

    if (rdma_pool != NULL) {
        if(rdma_pool->memory_pool != NULL) {
            close_memory_pool(rdma_pool->memory_pool);
        }
        if(rdma_pool->header_pool != NULL) {
            close_object_pool(rdma_pool->header_pool);
        }
        if(rdma_pool->response_pool != NULL) {
            close_object_pool(rdma_pool->response_pool);
        }
        free(rdma_pool);
    }
    if (rdma_pool_config != NULL) {
        free(rdma_pool_config);
    }

    if (fp != NULL) {
        fclose(fp);
    }
}

static int init_rdma_env(struct rdma_pool_config* config) {
    if(config == NULL) {
        return 0;
    }

    pthread_attr_t attr;
    struct sched_param param;
    int policy;

    rdma_pool_config = config;

    if (rdma_pool_config->enable_rdma_log == 1) {
        log_set_level(0);
        log_set_quiet(0);
        fp = fopen("/c_debug.log", "ab");
        if(fp == NULL) {
            goto err_free_config;
        }
        log_add_fp(fp, LOG_DEBUG);
    } else {
        log_set_quiet(1);
    }

    int len = sizeof(struct net_env_st) + config->worker_num * sizeof(worker);//32
    g_net_env = (struct net_env_st*)malloc(len);
    if (g_net_env == NULL) {
        log_debug("init env failed: no enouth memory");
        goto err_close_fp;
    }
    g_net_env->worker_num = config->worker_num;//32
    g_net_env->server_map = hashmap_create();
    log_debug("%p\n",g_net_env->server_map);

    if (pthread_spin_init(&(g_net_env->server_lock), PTHREAD_PROCESS_SHARED) != 0) {
        log_debug("init g_net_env->server_lock spin lock failed");
        goto err_free_gnetenv;
    }

    g_net_env->all_devs = rdma_get_devices(&g_net_env->ib_dev_cnt);
    if (g_net_env->all_devs == NULL) {
        log_debug("init env failed: get rdma devices failed");
        goto err_destroy_spinlock;
    }
    log_debug("rdma_get_devices find ib_dev_cnt:%d", g_net_env->ib_dev_cnt);

    if (g_net_env->ib_dev_cnt > 0) {
        g_net_env->ctx = g_net_env->all_devs[0];
    } else {
        log_debug("can not find rdma dev");
        goto err_free_devices;
    }

    g_net_env->event_channel = rdma_create_event_channel();
    g_net_env->pd = ibv_alloc_pd(g_net_env->ctx);
    if (g_net_env->pd == NULL) {
        log_debug("alloc pd failed, errno:%d", errno);
        goto err_destroy_eventchannel;
    }
    log_debug("g net env alloc pd:%p",g_net_env->pd);

    pthread_attr_init(&attr);
    pthread_attr_getschedpolicy(&attr, &policy);
    //pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    policy = SCHED_FIFO;
    pthread_attr_setschedpolicy(&attr, policy);
    param.sched_priority = sched_get_priority_max(policy);
    pthread_attr_setschedparam(&attr, &param);

    pthread_create(&g_net_env->cm_event_loop_thread, &attr, cm_thread, g_net_env);
    int index;
    for (index = 0; index < g_net_env->worker_num; index++) {
        log_debug("init worker(%d)", index);
        g_net_env->worker[index].id = index;
        if(init_worker(g_net_env->worker + index, cq_thread) == C_ERR) {
            log_debug("init env failed: init worker[%d] failed", index);
            goto err_destroy_worker;
        }
    }
    WQ_DEPTH = rdma_pool_config->wq_depth;
    MIN_CQE_NUM = rdma_pool_config->min_cqe_num;
    rdma_pool = (struct rdma_pool*)malloc(sizeof(struct rdma_pool));
    if (rdma_pool == NULL) {
        log_debug("malloc rdma pool failed");
        goto err_destroy_worker;
    }
    memset(rdma_pool, 0, sizeof(struct rdma_pool));
    rdma_pool->memory_pool = init_memory_pool(rdma_pool_config->mem_block_num, rdma_pool_config->mem_block_size, rdma_pool_config->mem_pool_level, g_net_env->pd);
    if(rdma_pool->memory_pool == NULL) {
        log_debug("init rdma memory pool failed");
        goto err_free_rdmapool;
    }
    rdma_pool->header_pool = init_object_pool(rdma_pool_config->header_block_num, get_header_size(), rdma_pool_config->header_pool_level);
    if(rdma_pool->header_pool == NULL) {
        log_debug("init rdma header pool failed");
        goto err_close_memorypool;
    }
    rdma_pool->response_pool = init_object_pool(rdma_pool_config->response_block_num, get_response_size(), rdma_pool_config->response_pool_level);
    if(rdma_pool->response_pool == NULL) {
        log_debug("init rdma response pool failed");
        goto err_close_headerpool;
    }
    return C_OK;
err_close_headerpool:
    close_object_pool(rdma_pool->header_pool);
err_close_memorypool:
    close_memory_pool(rdma_pool->memory_pool);
err_free_rdmapool:
    free(rdma_pool);
err_destroy_worker:
    ibv_dealloc_pd(g_net_env->pd);
    for (int i = 0; i < index; i++) {
        destroy_worker(g_net_env->worker + i);
    }
err_destroy_eventchannel:
    rdma_destroy_event_channel(g_net_env->event_channel);
err_free_devices:
    rdma_free_devices(g_net_env->all_devs);
err_destroy_spinlock:
    pthread_spin_destroy(&g_net_env->server_lock);
err_free_gnetenv:
    hashmap_destroy(g_net_env->server_map);
    free(g_net_env);
err_close_fp:
    fclose(fp);
err_free_config:
    free(rdma_pool_config);
    return C_ERR;
}

static void set_conn_state(connection* conn, int state) {
    int old_state = conn->state;
    conn->state = state;
    log_debug("conn(%lu-%p) state: %d-->%d", conn->nd, conn, old_state, state);
}

static worker* get_worker_by_nd(uint64_t nd) {
    int worker_id = ((nd) >>32) % g_net_env->worker_num;//CONN_ID_BIT_LEN
    log_debug("get worker by nd: worker_id:%d",worker_id);
    return g_net_env->worker + worker_id;
}

static int add_conn_to_worker(connection * conn, worker * worker, khash_t(map) *hmap) {
    int ret = 0;
    pthread_spin_lock(&worker->nd_map_lock);
    ret = hashmap_put(hmap, conn->nd, (uint64_t)conn);
    pthread_spin_unlock(&worker->nd_map_lock);
    log_debug("add conn(%p nd:%d) from worker(%p) nd_map(%p)",conn,conn->nd,worker,worker->nd_map);
    return ret >= 0;
}

static int del_conn_from_worker(uint64_t nd, worker * worker, khash_t(map) *hmap) {
    int ret = 0;
    pthread_spin_lock(&worker->nd_map_lock);
    ret = hashmap_del(hmap, nd);
    pthread_spin_unlock(&worker->nd_map_lock);
    log_debug("del conn(nd:%d) from worker(%p) nd_map(%p)",nd,worker,worker->nd_map);
    return ret >= 0;
}

static void get_worker_and_connect_by_nd(uint64_t nd, worker ** worker, connection** conn) {
    *worker = get_worker_by_nd(nd);
    pthread_spin_lock(&(*worker)->nd_map_lock);
    *conn = (connection*)hashmap_get((*worker)->nd_map, nd);
    pthread_spin_unlock(&(*worker)->nd_map_lock);
}

static int add_server_to_env(struct rdma_listener *server, khash_t(map) *hmap) {
    int ret = 0;
    pthread_spin_lock(&g_net_env->server_lock);
    ret = hashmap_put(hmap, server->nd, (uint64_t)server);
    pthread_spin_unlock(&g_net_env->server_lock);
    return ret >= 0;
}

static int del_server_from_env(struct rdma_listener *server) {
    int ret = 0;
    pthread_spin_lock(&g_net_env->server_lock);
    ret = hashmap_del(g_net_env->server_map, server->nd);
    pthread_spin_unlock(&g_net_env->server_lock);
    return ret >= 0;
}

static inline int open_event_fd() {
    return eventfd(0, EFD_SEMAPHORE);
}

static inline void wait_event(int fd) {
    uint64_t value = 0;
    return read(fd, &value, 8);
}

static inline void notify_event(int fd, int flag) {
	if (flag == 0) {
		uint64_t value = 1;
        write(fd, &value, 8);
	} else {
		close(fd);
	}
	return;
}

#endif