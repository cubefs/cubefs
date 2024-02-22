#ifndef RDMA_PROTO_H
#define RDMA_PROTO_H

#include "memory_pool.h"
#include "object_pool.h"
#include "queue.h"
#include "wait_group.h"
#include <stdbool.h>
#include <netdb.h>
#include <sys/eventfd.h>

#define C_OK 1
#define C_ERR 0
#define RDMA_INVALID_OPCODE 0xffff

//#define RDMA_MAX_WQE 1024
#define SERVER_MAX_CONN 10
#define SERVER_MAX_CONN 32

static const int TIMEOUT_IN_MS = 500;

typedef int (*PreConnCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*ConnectedCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*DisConnectedCb)(struct rdma_cm_id *id, void* ctx);
typedef int (*RejectedCb)(struct rdma_cm_id *id, void* ctx);

extern void PrintCallback(char*);
static char buffer[100];
extern int WQ_DEPTH;
extern int MIN_CQE_NUM;

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

typedef struct ConnectionEvent {
    struct rdma_cm_id *cm_id;
    void* ctx;

    PreConnCb preconnect_callback;
    ConnectedCb connected_callback;
    DisConnectedCb disconnected_callback;
    RejectedCb rejected_callback;
} ConnectionEvent;

typedef struct MemoryEntry {
    void* header_buff;
    void* data_buff;
    void* response_buff;
    uint32_t data_len;
    uint32_t header_len;
    uint32_t response_len;
    bool     isResponse;
} MemoryEntry;

typedef enum ConnectionState {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_ACCEPTING,
    CONN_STATE_CONNECTED,
    CONN_STATE_CLOSED,
    CONN_STATE_CLOSING,
    CONN_STATE_ERROR
} ConnectionState;

typedef struct Connection {
    char   local_addr[INET_ADDRSTRLEN + NI_MAXSERV];
    char   remote_addr[INET_ADDRSTRLEN + NI_MAXSERV];
    int     conntype;
    void    *buf;
    int     buf_len;
    struct rdma_cm_id * cm_id;
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    struct ibv_mr *mr;
    MemoryPool *pool;
    ObjectPool* header_pool;
    ObjectPool* response_pool;
    Header *header_buf;
    struct ibv_mr *header_mr;
    Response *response_buf;
    struct ibv_mr *response_mr;
    Queue *freeList;
    Queue *msgList;
    void* csContext;
    ConnectionState state;
    void* connContext;
    int cFd;
    int mFd;
    struct WaitGroup wg;
    pthread_spinlock_t lock;
    int lockInitialized;
    int64_t send_timeout_ns;
    int64_t recv_timeout_ns;
} Connection;

typedef struct RdmaContext {
    Connection *conn;
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    char* ip;
    char* port;
    struct ConnectionEvent *conn_ev;
    int cFd;
    int state;
    bool isReConnect;
};

typedef struct RdmaListener {
    struct hashmap *allConns;
    Queue *waitConns;
    pthread_mutex_t mutex;
    int count;
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    char* ip;
    char* port;
    struct ConnectionEvent *conn_ev;
    int cFd;
    struct WaitGroup closeWg;
    int state;
};

static inline int open_event_fd() {
   return eventfd(0, EFD_SEMAPHORE);
}

static inline int wait_event(int fd) {
	uint64_t value = 0;
	return read(fd, &value, 8);
}

static inline int notify_event(int fd, int flag) {
	if (flag == 0) {
		uint64_t value = 1;
		return write(fd, &value, 8);
	} else {
		close(fd);
	}
	return 0;
}

#endif