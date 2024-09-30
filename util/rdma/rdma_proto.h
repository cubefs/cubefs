#ifndef RDMA_PROTO_H
#define RDMA_PROTO_H

#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdbool.h>
#include <netdb.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <poll.h>

#include "memory_pool.h"
#include "hashmap.h"
#include "queue.h"
#include "log.h"

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
extern int CONN_DATA_SIZE;

extern struct rdma_pool *rdma_pool;
extern struct rdma_env_config *rdma_env_config;
extern FILE *fp;
extern struct net_env_st *g_net_env;

struct rdma_pool {
    memory_pool *memory_pool;
};

struct event_fd {
    int fd;
    struct pollfd poll_fd;
};

struct rdma_env_config {
    int mem_block_num;
    int mem_block_size;
    int mem_pool_level;
    int conn_data_size;
    int wq_depth;
    int min_cqe_num;
    int enable_rdma_log;
    char* rdma_log_dir;
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
    struct ibv_cq      *send_cq;
    struct ibv_cq      *recv_cq;
    struct ibv_comp_channel   *send_comp_channel;
    struct ibv_comp_channel   *recv_comp_channel;
    int send_wc_cnt;
    int recv_wc_cnt;
    pthread_t          cq_poller_thread;
    pthread_spinlock_t nd_map_lock;
    khash_t(map)       *nd_map;
    pthread_spinlock_t lock;
    uint8_t          id;
    pthread_t        w_pid;
    //int              close;
} worker;

struct net_env_st {
    uint8_t             worker_num;

    struct ibv_context  **all_devs;
    struct ibv_context  *ctx;
    struct ibv_pd       *pd;

    struct rdma_event_channel *event_channel;
    pthread_t                 cm_event_loop_thread;

    int32_t             ib_dev_cnt;
    pthread_spinlock_t  server_lock;
    khash_t(map)        *server_map;
    uint32_t            id_gen[ID_GEN_MAX];
    //int                 close;
    worker              worker[];
};

typedef struct rdma_memory {
    uint16_t opcode;
    uint8_t rsvd[14];
    uint64_t addr;
    uint32_t length;
    uint32_t key;
} rdma_memory;

typedef struct rdma_full_msg {
    uint16_t opcode;
    uint8_t rsvd[26];
    uint32_t tx_full_offset;
} rdma_full_msg;

typedef union rdma_ctl_cmd {
    rdma_memory memory;
    rdma_full_msg full_msg;
} rdma_ctl_cmd;

typedef enum rdma_opcode {
    EXCHANGE_MEMORY = 0,
    NOTIFY_FULLBUF = 1,
} rdma_opcode;

typedef struct data_buf {
    struct ibv_mr *mr;
    char *addr;
    uint32_t length;
    uint32_t offset;
    uint32_t pos;
} data_buf;

typedef struct cmd_entry {
    rdma_ctl_cmd *cmd;
    uint64_t nd;
} cmd_entry;

typedef struct data_entry {
    char *addr;
    char *remote_addr;
    uint32_t data_len;
    uint32_t mem_len;
    uint32_t lkey;
} data_entry;

typedef enum connection_state {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_CONNECT_FAIL,
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
    //TX
    data_buf *tx;
    char *remote_rx_addr;
    uint32_t remote_rx_key;
    uint32_t remote_rx_length;
    uint32_t remote_rx_offset;

    //RX
    data_buf *rx;

    //control_buff
    rdma_ctl_cmd *ctl_buf;
    struct ibv_mr *ctl_buf_mr;

    uint32_t tx_full_offset;
    uint32_t rx_full_offset;

    int tx_flag;//0: pos offset  1:offset pos

    Queue *free_list;
    Queue *msg_list;
    Queue *tx_buffer_list;
    Queue *rx_buffer_list;
    Queue *wr_list;
    pthread_spinlock_t free_list_lock;
    pthread_spinlock_t msg_list_lock;
    void* context;
    void* conn_context;
    connection_state state;
    struct event_fd connect_fd;
    struct event_fd read_fd;
    struct event_fd write_fd;
    struct event_fd close_fd;
    pthread_spinlock_t spin_lock;//state
    pthread_spinlock_t tx_lock;
    pthread_spinlock_t rx_lock;
    int64_t send_timeout_ns;
    int64_t recv_timeout_ns;
    worker *worker;
    int ref;
    int loop_exchange_flag;
    int send_wr_cnt;
    int recv_wr_cnt;
} connection;

struct rdma_listener {
    uint64_t nd;
    struct rdma_cm_id *listen_id;
    char* ip;
    char* port;
    pthread_spinlock_t conn_lock;
    khash_t(map) *conn_map;
    pthread_spinlock_t wait_conns_lock;
    Queue *wait_conns;
    struct event_fd connect_fd;
};



uint64_t allocate_nd(int type);

void cbrdma_parse_nd(uint64_t nd, int *id, int * worker_id, int * is_server, int * is_active);

struct rdma_env_config* get_rdma_env_config();

int init_worker(worker *worker, event_callback cb, int index);

void destroy_worker(worker *worker);

void destroy_rdma_env();

int init_rdma_env(struct rdma_env_config* config);

void set_conn_state(connection* conn, int state);

int get_conn_state(connection* conn);

void add_conn_ref(connection* conn, int value);

void sub_conn_ref(connection* conn, int value);

void get_conn_ref(connection* conn, int* ref);

void add_conn_send_cnt(connection* conn, int value);

void sub_conn_send_cnt(connection* conn, int value);

void get_conn_send_cnt(connection* conn, int* send_wr_cnt, int* send_wc_cnt);

void add_worker_recv_cnt(worker* worker, int value);

void sub_worker_recv_cnt(worker* worker, int value);

void get_worker_recv_cnt(worker* worker, int* recv_wc_cnt);

worker* get_worker_by_nd(uint64_t nd);

int add_conn_to_worker(connection * conn, worker * worker, khash_t(map) *hmap);

int del_conn_from_worker(uint64_t nd, worker * worker, khash_t(map) *hmap);

void get_worker_and_connect_by_nd(uint64_t nd, worker ** worker, connection** conn);

int add_server_to_env(struct rdma_listener *server, khash_t(map) *hmap);

int del_server_from_env(struct rdma_listener *server);

int open_event_fd(struct event_fd* event_fd);

int wait_event(struct event_fd fd, int64_t timeout_ns);

int notify_event(struct event_fd, int flag);

#endif