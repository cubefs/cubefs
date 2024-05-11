#ifndef CONNECTION_EVENT_H_
#define CONNECTION_EVENT_H_

//#define MAX_QPS 4
#define C_OK 1
#define C_ERR 0

#define SERVER_MAX_CONN 10

#include <pthread.h>
#include <error.h>
#include "rdma.h"
//#include "client.c"
#include "queue.h"
#include "hashmap.h"
#include "memory_pool.h"
#include "object_pool.h"

static char buffer[100];

typedef int (*PreConnCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*ConnectedCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*DisConnectedCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*RejectedCb)(struct rdma_cm_id *id, void* ctx);

#define RDMA_MAX_WQE 32
#define SERVER_MAX_CONN 32

static int rdma_comp_vector = -1; /* -1 means a random one */

//typedef void (*ConnectionReady)(int, void* conn);

/*
typedef struct RdmaResponse {
    uint16_t opcode;
    uint16_t statusCode;
    uint32_t addr;
    uint32_t length;
    uint32_t leftSpace;
    uint8_t rsvd[20];
} RdmaResponse;
*/
/*
typedef struct RdmaMemory {
    uint16_t opcode;
    //uint8_t rsvd[14];
    uint64_t addr;
    uint32_t length;
    uint32_t key;
} RdmaMemory;
*/
/*
typedef struct RdmaKeepalive {
    uint16_t opcode;
    uint8_t rsvd[30];
} RdmaKeepalive;
*/
/*
typedef union RdmaMessage {
    //uint16_t opcode;
    RdmaKeepalive keepalive;
    RdmaResponse response;
    RdmaMemory memory;
} RdmaMessage;
*/
/*
typedef enum RdmaOpcode {
    RegisterXferMemory = 0,
    ResponseRdmaMessage = 1,
} RdmaOpcode;
*/
/*
typedef enum ResponseOpcode {
    Failure = 0,
    Success = 1,
} ResponseOpcode;
*/

typedef struct RdmaMeta {
    uint8_t              RdmaVersion;//rdma协议版本
    uint64_t             RdmaAddr;
    uint32_t             RdmaLength;
    uint32_t             RdmaKey;
};
typedef struct RequestHeader {//__attribute__((packed))
    uint8_t              Magic;              
    uint8_t              ExtentType;// the highest bit be set while rsp to client if version not consistent then Verseq be valid
    uint8_t              Opcode;             
    uint8_t              ResultCode;         
    uint8_t              RemainingFollowers; 
    uint32_t             CRC;                
    uint32_t             Size;               
    uint32_t             ArgLen;             
    uint64_t             PartitionID;        
    uint64_t             ExtentID;           
    int64_t              ExtentOffset;       
    int64_t              ReqID;
    uint64_t             KernelOffset;
    uint64_t             VerSeq;// only used in mod request to datanode
    unsigned char        Arg[40];// for create or append ops, the data contains the address            
    unsigned char        list[40];
    uint8_t              RdmaVersion;//rdma协议版本
    uint64_t             RdmaAddr;
    uint32_t             RdmaLength;
    uint32_t             RdmaKey;
    //struct RdmaMeta      rdmaMeta;
}__attribute__((packed)) Header;//

typedef struct Response {

    uint8_t              Magic;              
    uint8_t              ExtentType;// the highest bit be set while rsp to client if version not consistent then Verseq be valid
    uint8_t              Opcode;             
    uint8_t              ResultCode;         
    uint8_t              RemainingFollowers; 
    uint32_t             CRC;                
    uint32_t             Size;               
    uint32_t             ArgLen;             
    uint64_t             PartitionID;        
    uint64_t             ExtentID;           
    int64_t              ExtentOffset;       
    int64_t              ReqID;
    uint64_t             KernelOffset;
    uint64_t             VerSeq;// only used in mod request to datanode
    unsigned char        Arg[40];// for create or append ops, the data contains the address            
    unsigned char        data[40];
    unsigned char        list[40];

    uint8_t              RdmaVersion;//rdma协议版本
    uint64_t             RdmaAddr;
    uint32_t             RdmaLength;
    uint32_t             RdmaKey;
} Response;

typedef enum ConnectionState {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_ACCEPTING,
    CONN_STATE_CONNECTED,
    CONN_STATE_CLOSED,
    CONN_STATE_CLOSING,
    CONN_STATE_ERROR
} ConnectionState;

//typedef struct RdmaXfer {
//    struct ibv_mr *mr; /* memory region of the transfer buffer */
//    char *addr;        /* address of transfer buffer in local memory */
//    uint32_t length;   /* bytes of transfer buffer */
//    uint32_t offset;   /* the offset of consumed transfer buffer */
//    uint32_t pos;      /* the position in use of the transfer buffer */
//} RdmaXfer;

typedef struct MemoryEntry {
    //char* addr;
    void* header_buff;
    void* data_buff;
    //uint32_t len;
    //uint32_t remote_len;
    void* response_buff;
    uint32_t data_len;
    uint32_t header_len;
    uint32_t response_len;
    bool     isResponse;
    //int64_t recv_time;
    //struct MemoryEntry* next;
    //int index;
    //int free;
} MemoryEntry;

typedef struct WaitGroup {
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int count;
    int wgInitialized;
};

typedef struct Connection {
    //int     connId;
    //int     agentId;
    //char*   ip;
    //char*   port;
    char*   localAddr;
    char*   remoteAddr;
    int     conntype;
    void    *buf;
    int     buf_len;

    struct rdma_cm_id * cm_id;

    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    //struct ibv_mr *msg_mr;

    /* DataBuff */
    struct ibv_mr *mr; /* memory region of the transfer buffer */
    MemoryPool *pool;

    //RdmaXfer dx;
    //char *dx_addr;      /* remote transfer buffer address */
    //uint32_t dx_key;    /* remote transfer buffer key */
    //uint32_t dx_length; /* remote transfer buffer length */

    //uint32_t dx_offset; /* remote transfer buffer offset */
    //uint32_t dx_ops;    /* operations on remote transfer */

    /* CMD 0 ~ RDMA_MAX_WQE for recv buffer
     * RDMA_MAX_WQE ~ 2 * RDMA_MAX_WQE -1 for send buffer */
    //RdmaMessage *ctl_buf;
    //struct ibv_mr *ctl_mr;

    ObjectPool* header_pool;
    ObjectPool* response_pool;

    Header *header_buf;
    struct ibv_mr *header_mr;
    
    Response *response_buf;
    struct ibv_mr *response_mr;

    Queue *freeList;

    //MemoryPool* memory_pool;
    //MemoryArea* memory;

    //ConnectionReady readyCallback;

    void* csContext;

    ConnectionState state;

    //Queue *waitRead;

    void* connContext;

    //int initFd;
    int cFd;

    //int eFd;
    struct WaitGroup wg;
    

    pthread_spinlock_t lock;
    int lockInitialized;

    int64_t send_timeout_ns;
    int64_t recv_timeout_ns;
    //int activeClosingFlag;
    //int sendLimitCount;

    //map_t waitResponse;

} Connection;

typedef struct RdmaContext {
    Connection *conn;
    //char *ip;
    //int port;
    //struct ibv_pd *pd;
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    //struct ibv_comp_channel *comp_channel;
    //struct ibv_cq *cq;
    char* ip;
    char* port;
    struct ConnectionEvent *conn_ev;
    int cFd;
    //int closeFd;
    int state;

    bool isReConnect;
};

typedef struct RdmaListener {
    //Connection *allConns[SERVER_MAX_CONN];
    //map_t allConns;
    struct hashmap *allConns;
    Queue *waitConns;
    pthread_mutex_t mutex;
    int count;
    //char *ip;
    //int port;
    //struct ibv_pd *pd;
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    //struct ibv_comp_channel *comp_channel;
    //struct ibv_cq *cq;
    char* ip;
    char* port;
    struct ConnectionEvent *conn_ev;
    int cFd;
    //int closeFd;
    struct WaitGroup closeWg;
    int state;
};

typedef struct ConnectionEvent {
    struct rdma_cm_id *cm_id;
    void* ctx;

    //MemoryPool *pool;
    //struct ibv_pd *pd;
    //struct ibv_mr *mr;
    //ObjectPool *header_pool;
    //ObjectPool *response_pool;

    PreConnCb preconnect_callback;
    ConnectedCb connected_callback;
    DisConnectedCb disconnected_callback;
    RejectedCb rejected_callback;
    //TimeWaitExitCb timewaitexit_callback;
} ConnectionEvent;

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
};


static const int TIMEOUT_IN_MS = 500;


static struct RdmaPool *rdmaPool = NULL;
static struct RdmaPoolConfig *rdmaPoolConfig = NULL;

static int wait_group_init(struct WaitGroup* wg) {
    if(pthread_mutex_init(&(wg->mutex), NULL)) {
        printf("Failed to initialize mutex\n");
        wg->wgInitialized = 0;
        return 1;
    }
    if(pthread_cond_init(&(wg->cond), NULL)) {
        printf("Failed to initialize cond\n");
        pthread_mutex_destroy(&wg->mutex);
        wg->wgInitialized = 0;
        return 1;
    }
    wg->count = 0;
    wg->wgInitialized = 1;
    return 0;
}

static void wait_group_add(struct WaitGroup* wg, int delta) {
    pthread_mutex_lock(&wg->mutex);
    wg->count += delta;
    pthread_mutex_unlock(&wg->mutex);
}

static void wait_group_done(struct WaitGroup* wg) {
    pthread_mutex_lock(&wg->mutex);
    wg->count--;
    if (wg->count == 0) {
        pthread_cond_broadcast(&wg->cond);
    }
    pthread_mutex_unlock(&wg->mutex);
}

static void wait_group_wait(struct WaitGroup* wg) {
    pthread_mutex_lock(&wg->mutex);
    while (wg->count != 0) {
        pthread_cond_wait(&wg->cond, &wg->mutex);
    }
    pthread_mutex_unlock(&wg->mutex);
}

static void wait_group_destroy(struct WaitGroup* wg) {
    pthread_mutex_destroy(&wg->mutex);
    pthread_cond_destroy(&wg->cond);
}

static int getHeaderSize() {
    return sizeof(Header);
}

static int getResponseSize() {
    return sizeof(Response);
}

static struct RdmaPoolConfig* getRdmaPoolConfig() {
    rdmaPoolConfig = (struct RdmaPoolConfig*)malloc(sizeof(struct RdmaPoolConfig));
    memset(rdmaPoolConfig, 0, sizeof(struct RdmaPoolConfig));

    rdmaPoolConfig->memBlockNum = 1024;


    rdmaPoolConfig->memBlockSize = 16;


    rdmaPoolConfig->memPoolLevel = 10;


    rdmaPoolConfig->headerBlockNum = 32 * 1024;


    rdmaPoolConfig->headerPoolLevel = 15;


    rdmaPoolConfig->responseBlockNum = 32 * 1024;


    rdmaPoolConfig->responsePoolLevel = 15;
    return rdmaPoolConfig;
}

static void destroyRdmaPool() {
    if(rdmaPool == NULL) {
        return;
    }
    if(rdmaPool->memoryPool != NULL) {
        CloseMemoryPool(rdmaPool->memoryPool);
    }
    if(rdmaPool->headerPool != NULL) {
        CloseObjectPool(rdmaPool->headerPool);
    }
    if(rdmaPool->responsePool != NULL) {
        CloseObjectPool(rdmaPool->responsePool);
    }
    free(rdmaPoolConfig);
    free(rdmaPool);
}

static int initRdmaPool(struct RdmaPoolConfig* config) {
    if(config == NULL) {
        return C_ERR;
    }
    rdmaPoolConfig = config;
    rdmaPool = (struct RdmaPool*)malloc(sizeof(struct RdmaPool));
    rdmaPool->memoryPool = InitMemoryPool(rdmaPoolConfig->memBlockNum, rdmaPoolConfig->memBlockSize, rdmaPoolConfig->memPoolLevel);
    if(rdmaPool->memoryPool == NULL) {
        goto error;
    }
    rdmaPool->headerPool = InitObjectPool(rdmaPoolConfig->headerBlockNum, getHeaderSize(), rdmaPoolConfig->headerPoolLevel);
    if(rdmaPool->headerPool == NULL) {
        goto error;
    }
    rdmaPool->responsePool = InitObjectPool(rdmaPoolConfig->responseBlockNum, getResponseSize(), rdmaPoolConfig->responsePoolLevel);
    if(rdmaPool->responsePool == NULL) {
        goto error;
    }
    return C_OK;
error:
    destroyRdmaPool();
    return C_ERR;
}

static struct ibv_pd* alloc_pd() {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if(dev_list == NULL) {
        //TODO 获取设备列表失败的处理逻辑
        return NULL;
    }
    struct ibv_device *selected_device = dev_list[0];
    ibv_free_device_list(dev_list);
    struct ibv_context *context =  ibv_open_device(selected_device);
    if(context == NULL) {
        //TODO 打开设备失败的处理逻辑
        return NULL;
    }

    struct ibv_pd *pd = ibv_alloc_pd(context);

    if (!pd) {
        //serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        //TODO error handler
        printf("RDMA: ibv alloc pd failed\n");
        return NULL;
    }
    
    
    return pd;
}

static void dealloc_pd(struct ibv_pd* pd) {
    if(pd) {//TODO if pd is NULL
        ibv_dealloc_pd(pd);
    }
    return;
}

static struct ibv_mr* regist_mr(MemoryPool* pool, struct ibv_pd* pd) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    struct ibv_mr *mr = ibv_reg_mr(pd, pool->original_mem, pool->size, access);
    if (!mr) {//TODO error handler
        //serverLog(LL_WARNING, "RDMA: reg mr for recv buffer failed");
        //TODO error handler
        printf("RDMA: reg mr for recv data buffer failed");
        //goto destroy_iobuf;
        //TODO clear resources
        return NULL;
    }
    return mr;
}

static void dereg_mr(struct ibv_mr* mr) {
    if (mr) {//TODO if mr = NULL
        ibv_dereg_mr(mr);
    }
    return;
}

static int connection_compare(const void *a, const void *b, void *udata) {
    Connection *ca = a;
    Connection *cb = b;
    if(ca->cm_id > cb->cm_id) {
        return 1;
    } else if(ca->cm_id < cb->cm_id) {
        return -1;
    } else {
        return 0;
    }
}

static bool connection_iter(const void *item, void *udata) {
    const Connection *conn = item;
    printf("connn=%d (cm_id=%d)\n", conn, conn->cm_id);
    return true;
}

static uint64_t connection_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const Connection *conn = item;
    return hashmap_sip(conn->cm_id, sizeof(conn->cm_id), seed0, seed1);
}

static void destroy_connection(Connection *conn) {
    if(conn->pd) {
        conn->pd = NULL;
    }

    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            printf("Failed to destroy qp: %s\n", strerror(errno));
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }

    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            printf("%d\n",ret);
            printf("Failed to destroy cq: %s\n", strerror(errno));
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->comp_channel) {
        int ret = ibv_destroy_comp_channel(conn->comp_channel);
        if(ret != 0) {
            printf("%d\n,ret");
            printf("Failed to destroy comp channel: %s\n", strerror(errno));
            //printf("Failed to destroy comp channel cleanly\n");
            // we continue anyways;
        }
        conn->comp_channel = NULL;
    }


}

static int build_connection(struct ConnectionEvent *conn_ev, Connection *conn) {
    int ret = C_OK;
    struct ibv_device_attr device_attr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_comp_channel *comp_channel = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_pd *pd = NULL;
    struct rdma_cm_id *cm_id = conn->cm_id;
    
    //pd = conn_ev->pool->pd;
    pd = rdmaPool->memoryPool->pd;
    cm_id->verbs = pd->context;

    if (ibv_query_device(cm_id->verbs, &device_attr)) {
        //serverLog(LL_WARNING, "RDMA: ibv ibv query device failed");
        printf("RDMA: ibv query device failed\n");
        goto error;
        //return C_ERR;
    }

    //printf("alloc pd %d\n",pd);

    //pd = conn_ev->pd;

    //printf("verbs %d\n",cm_id->verbs);
    //printf("pd %d\n",pd);

    /*
    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        //serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        //TODO error handler
        printf("RDMA: ibv alloc pd failed\n");
        return C_ERR;
    }
    */

    //((struct RdmaContext*)(conn_ev->ctx))->pd = pd; //TODO need to modify ()
    conn->pd = pd;

    comp_channel = ibv_create_comp_channel(cm_id->verbs);

    if (!comp_channel) {
        //serverLog(LL_WARNING, "RDMA: ibv create comp channel failed");
        printf("RDMA: ibv create comp channel failed\n");
        goto error;
        //return C_ERR;
    }

    //((struct RdmaContext*)(conn_ev->ctx))->comp_channel = comp_channel;
    conn->comp_channel = comp_channel;
   

    //rdma_comp_vector % cm_id->verbs->num_comp_vectors

    cq = ibv_create_cq(cm_id->verbs, RDMA_MAX_WQE * 2, NULL, comp_channel, 0);//when -1, cq is null?

    if (!cq) {
        //serverLog(LL_WARNING, "RDMA: ibv create cq failed");
        printf("RDMA: ibv create cq failed: cq:%d\n",cq);
        goto error;
        //return C_ERR;
    }
    //((struct RdmaContext*)(conn_ev->ctx))->cq = cq;
    conn->cq = cq;

    ibv_req_notify_cq(cq, 0);

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = RDMA_MAX_WQE;
    init_attr.cap.max_recv_wr = RDMA_MAX_WQE;
    init_attr.cap.max_send_sge = device_attr.max_sge;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cq;
    init_attr.recv_cq = cq;

    ret = rdma_create_qp(cm_id, pd, &init_attr);
    sprintf(buffer,"RDMA: create qp: qp:%d pd:%d\n",cm_id->qp,pd);
    PrintCallback(buffer);

    if (ret) {
        //serverLog(LL_WARNING, "RDMA: create qp failed");
        printf("RDMA: create qp failed: %d\n",ret);
        sprintf(buffer,"RDMA: create qp failed: %d\n",ret);
        PrintCallback(buffer);
        goto error;
        //return C_ERR;
    }

/*    if (rdmaSetupIoBuf(ctx, cm_id)) {
        return C_ERR;
    }*/

    return C_OK;
error:

    destroy_connection(conn);
    return C_ERR;
}

static void build_params(struct rdma_conn_param *params){
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1; //指定发送队列的深度
  params->rnr_retry_count = 7; /* infinite retry */
  params->retry_count = 7;
}


static int connection_event_cb(void *ctx) {//TODO error handler
  struct ConnectionEvent *conn_ev = (struct ConnectionEvent*)ctx;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn_id;
  int event_type;

  int ret = rdma_get_cm_event(conn_ev->cm_id->channel, &event);//TODO

  if (ret < 0) {
      //TODO error handler
      //printf("rdma_get_cm_event failed:（%d：%s）。\n",errno,strerror(errno));
      printf("rdma_get_cm_event failed: %d", ret);
      sprintf(buffer,"rdma_get_cm_event failed: %d", ret);
      PrintCallback(buffer);

      return C_ERR;
  }

  conn_id = event->id;

  event_type = event->event;

  if (rdma_ack_cm_event(event)) { //ack event //TODO hava problem
      printf("ack cm event failed\n");
      sprintf(buffer,"ack cm event failed\n");
      PrintCallback(buffer);
      return C_ERR;
  }

  //printf("RDMA: connection event handler success status: status: 0x%x\n", event->status);
  if (!ret) {
      //struct rdma_cm_event event_copy;
      //memcpy(&event_copy, event, sizeof(*event));
      //rdma_ack_cm_event(event);
      //event = &event_copy;
      if (event_type == RDMA_CM_EVENT_ADDR_RESOLVED) { //client

          int v = conn_ev->preconnect_callback(conn_id, conn_ev); //创建connection

          if(!v) {
            rdma_disconnect(conn_id);//need verify
            printf("client: rdma addr resolved failed, call rdma_disconnect\n");
            sprintf(buffer,"client: rdma addr resolved failed, call rdma_disconnect\n");
            PrintCallback(buffer);
            return;
          } else {
            ret = rdma_resolve_route(conn_id, TIMEOUT_IN_MS);
            printf("client: rdma addr resolved success\n");
            sprintf(buffer,"client: rdma addr resolved success %d\n",ret);
            PrintCallback(buffer);
            return;
          }
      } else if (event_type == RDMA_CM_EVENT_ROUTE_RESOLVED) { //client

          struct rdma_conn_param cm_params;
          build_params(&cm_params);
          ret = rdma_connect(conn_id, &cm_params);
          if(ret) {
              //TODO error handler & release resources ()
              const char *error_str = strerror(ret);
              printf("client: rdma_route resolved failed, call rdma_disconnect\n");
              sprintf(buffer,"client: rdma_route resolved failed %d %s, call rdma_disconnect\n",ret,error_str);
              PrintCallback(buffer);
              rdma_disconnect(conn_id);
              //return C_ERR;
              return;
          }
          printf("client: rdma route resolved success\n");
          sprintf(buffer,"client: rdma route resolved success\n");
          PrintCallback(buffer);
          return;
      } else if (event_type == RDMA_CM_EVENT_CONNECT_REQUEST) { //server
          //build_connection(event->id, conn_ev);
          int v = conn_ev->preconnect_callback(conn_id, conn_ev);
          if(!v) {
              printf("server: rdma_connect request failed, call rdma_reject\n");
              sprintf(buffer,"server: rdma_connect request failed, call rdma_reject\n");
              PrintCallback(buffer);

              rdma_reject(conn_id, NULL, 0);
              rdma_destroy_id(conn_id);

              return;
          } else {
              struct rdma_conn_param cm_params;
              build_params(&cm_params);
              ret = rdma_accept(conn_id, &cm_params);
              if (ret) {
                  //TODO error handler & release resources ()
                  printf("server: rdma_connect request failed, call rdma_reject\n");
                  sprintf(buffer,"server: rdma_connect request failed, call rdma_reject\n");
                  PrintCallback(buffer);
                  rdma_reject(conn_id, NULL, 0);
                  return;
              }
          }
          printf("server: rdma connect request success\n");
          sprintf(buffer,"server: rdma connect request success\n");
          PrintCallback(buffer);
          return;
      } else if (event_type == RDMA_CM_EVENT_ESTABLISHED) {

          conn_ev->connected_callback(conn_id, conn_ev->ctx);

          printf("server and client: rdma conn established\n");
          sprintf(buffer,"server and client: rdma conn established\n");
          PrintCallback(buffer);
      } else if (event_type == RDMA_CM_EVENT_DISCONNECTED) { //TODO before disconnect,need to poll cqe ()
          //conn_ev->disconnected_callback(event->id, conn_ev->ctx);
          
          Connection *conn = (Connection *)conn_id->context;

          if (conn->conntype == 1) {//server
            //struct rdma_cm_event *_event = event;

            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
            
            printf("server: rdma conn disconnected\n");
            sprintf(buffer,"server: rdma conn disconnected\n");
            PrintCallback(buffer);
          } else {
            //struct cm_id *id = event->id;

            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
            
            printf("client: rdma conn disconnected\n");
            sprintf(buffer,"client: rdma conn disconnected\n");
            PrintCallback(buffer);
          }
      } else if(event_type == RDMA_CM_EVENT_REJECTED) {//client
           conn_ev->rejected_callback(conn_id, conn_ev->ctx);
           printf("client: rdma conn rejected\n");
           sprintf(buffer,"client: rdma conn rejected\n");
           PrintCallback(buffer);
      } else if(event_type == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
          //conn_ev->timewaitexit_callback(event->id, conn_ev->ctx);
           Connection *conn = (Connection *)conn_id->context;
           if (conn->conntype == 2) {//client
             //conn_ev->disconnected_callback(event->id, conn_ev->ctx);
             printf("client: rdma conn timewait exit\n");
             sprintf(buffer,"client: rdma conn timewait exit\n");
             PrintCallback(buffer);
           } else {
             //conn_ev->disconnected_callback(event->id, conn_ev->ctx);
             printf("server: rdma conn timewait exit\n");
             sprintf(buffer,"server: rdma conn timewait exit\n");
             PrintCallback(buffer);
           }
          //printf("server and client: rdma conn timewait exit\n");
      } else {
          printf("unknown event %d \n", event_type);
          sprintf(buffer,"unknown event %d \n", event_type);
          PrintCallback(buffer);
      }

  }
}

#endif
