#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <stdio.h>
#include <string.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>

#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

#include "rdma.h"
#include "connection_event.h"
//#include "transfer_event.h"

//#include "memory_pool.h"

static inline int open_event_fd();
static inline int wait_event(int);
static inline int notify_event(int, int); 
static int wait_group_init(struct WaitGroup* wg);
static void wait_group_add(struct WaitGroup* wg, int delta);
static void wait_group_done(struct WaitGroup* wg);
static void wait_group_wait(struct WaitGroup* wg);


extern struct RdmaPool *rdmaPool; 

//static void* getHeaderBuffer(Connection *conn);
//static int transport_sendAndRecv_event_cb(void *ctx);//TODO why need declaration here

static const int trace = 0;
#define TRACE_PRINT(fn) if (trace) fn
//typedef void (*ConnectionReady)(int, void* conn);

#define RDMA_DEFAULT_DX_SIZE  (1024*1024)
static int rdma_dx_size = RDMA_DEFAULT_DX_SIZE;
//static int rdma_comp_vector = -1; /* -1 means a random one */
//#define RDMA_MAX_WQE 1024
#define C_OK 1
#define C_ERR 0
#define RDMA_INVALID_OPCODE 0xffff

#define ntohu64(v) (v)
#define htonu64(v) (v)

static int64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static int rdmaPostRecv(Connection *conn, void *block) {

    struct ibv_sge sge;
    //size_t length;

    struct rdma_cm_id *cm_id = conn->cm_id;

    struct ibv_recv_wr recv_wr, *bad_wr;
    int ret;

    sge.addr = (uint64_t)block;
    //sge.length = sizeof(RdmaMessage);
    //sge.lkey = conn->ctl_mr->lkey;
    //printf("conn type: %d\n",conn->conntype);

    if(conn->conntype == 1) {//server
        sge.length = sizeof(Header);
        sge.lkey = conn->header_mr->lkey;
    } else {//client
        sge.length = sizeof(Response);
        sge.lkey = conn->response_mr->lkey;
    }

    recv_wr.wr_id = (uint64_t)block;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;
    //printf("wr_id:%d\n",recv_wr.wr_id);
    ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post recv failed: %d", ret);
        //TODO error handler
        printf("RDMA: post recv failed: %d", ret);
        sprintf(buffer,"RDMA: post recv failed: %d", ret);
        PrintCallback(buffer);
        return C_ERR;
    }

    return C_OK;
}

static void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        //serverPanic("posix_memalign failed");
        printf("posix_memalign failed");
        sprintf(buffer,"posix_memalign failed");
        PrintCallback(buffer);
    }

    memset(tmp, 0x00, aligned_size);

    return tmp;
}

static void rdmaDestroyIoBuf(Connection *conn) {//TODO need to modify
    int index;
    /*
    if (conn->mr) {
    //    ibv_dereg_mr(conn->mr);
        conn->mr = NULL;
    }

    //free(conn->dx.addr);
    //conn->dx.addr = NULL;
    conn->pool = NULL;
    */

    if(conn->freeList) {
        ClearQueue(conn->freeList);
        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
    }
    if (conn->header_mr) {
        ibv_dereg_mr(conn->header_mr);
        conn->header_mr = NULL;
    }
    if (conn->header_buf) {
        //free(conn->header_buf);
        //TODO 需不需要先把这块内存置为零
        index = (int)(((char*)(conn->header_buf) - (char*)(conn->header_pool->original_mem)) / sizeof(Header));
        //printf("index: %d\n",index);
        buddy_free(conn->header_pool->allocation, index);//TODO 对于dataNode leader来说，收到响应不需要释放内存，对于客户端来说，收到响应需要释放内存
        buddy_dump(conn->header_pool->allocation);
        conn->header_buf = NULL;
    }

    if (conn->response_mr) {
        ibv_dereg_mr(conn->response_mr);
        conn->response_mr = NULL;
    }
    if (conn->response_buf) {
        //free(conn->response_buf);
        index = (int)(((char*)(conn->response_buf) - (char*)(conn->response_pool->original_mem)) / sizeof(Response));
        buddy_free(conn->response_pool->allocation, index);//TODO 对于dataNode leader来说，收到响应不需要释放内存，对于客户端来说，收到响应需要释放内存
        buddy_dump(conn->response_pool->allocation);
        conn->response_buf = NULL;
    }
    
}

static int rdmaSetupIoBuf(Connection *conn, struct ConnectionEvent *conn_ev, int conntype) {
    //struct rdma_cm_id *cm_id = conn->cm_id;

    //MemoryPool* pool = conn_ev->pool;
    MemoryPool* pool = rdmaPool->memoryPool;
    //struct ibv_mr* mr = conn_ev->pool->mr;
    struct ibv_mr* mr = rdmaPool->memoryPool->mr;
    //ObjectPool* headerPool = conn_ev->header_pool;
    ObjectPool* headerPool = rdmaPool->headerPool;
    //ObjectPool* responsePool = conn_ev->response_pool;
    ObjectPool* responsePool = rdmaPool->responsePool;

    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t headers_length = sizeof(Header) * RDMA_MAX_WQE;
    size_t responses_length = sizeof(Response) * RDMA_MAX_WQE;
    //size_t length = sizeof(RdmaMessage) * RDMA_MAX_WQE * 2;
    //RdmaMessage *msg;
    Header* header;
    Response* response;
    int i;
    
    printf("headers length: %d\n",headers_length);
    sprintf(buffer,"headers length: %d\n",headers_length);
    PrintCallback(buffer);
    printf("responses length: %d\n",responses_length);
    sprintf(buffer,"responses length: %d\n",responses_length);
    PrintCallback(buffer);
    int index = buddy_alloc(headerPool->allocation, RDMA_MAX_WQE);
    buddy_dump(headerPool->allocation);
    int s = buddy_size(headerPool->allocation,index);//when index == -1,assert is not pass
    printf("index %d (sz = %d)\n",index,s);
    sprintf(buffer,"index %d (sz = %d)\n",index,s);
    PrintCallback(buffer);
    if(index == -1) {
        printf("headerPool: there is no space to alloc\n");
        sprintf(buffer,"headerPool: there is no space to alloc\n");
        PrintCallback(buffer);
        goto destroy_iobuf; //TODO maybe return -1
    }
    void* addr = headerPool->original_mem + index * sizeof(Header);
    //conn->header_buf = page_aligned_zalloc(length);
    conn->header_buf = addr;//(RdmaMessage*)
    conn->header_mr = ibv_reg_mr(conn->pd, conn->header_buf, headers_length, access);
    sprintf(buffer,"RDMA: regist header mr: pd:%d\n",conn->pd);
    PrintCallback(buffer);

    if (!conn->header_mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for CMD failed");
        printf("RDMA: reg header mr failed\n");
        sprintf(buffer,"RDMA: reg header mr failed\n");
        PrintCallback(buffer);
        goto destroy_iobuf;
    }

    
    index = buddy_alloc(responsePool->allocation, RDMA_MAX_WQE);
    buddy_dump(responsePool->allocation);
    s = buddy_size(responsePool->allocation,index);
    printf("index %d (sz = %d)\n",index,s);
    sprintf(buffer,"index %d (sz = %d)\n",index,s);
    PrintCallback(buffer);
    if(index == -1) {
        printf("responsePool: there is no space to alloc\n");
        sprintf(buffer,"responsePool: there is no space to alloc\n");
        PrintCallback(buffer);
        goto destroy_iobuf; //TODO maybe return -1
    }
    addr = responsePool->original_mem + index * sizeof(Response);
    
    //conn->response_buf = page_aligned_zalloc(length);
    conn->response_buf = addr;//(RdmaMessage*)
    conn->response_mr = ibv_reg_mr(conn->pd, conn->response_buf, responses_length, access);

    if (!conn->response_mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for CMD failed");
        printf("RDMA: reg response mr failed\n");
        sprintf(buffer,"RDMA: reg response mr failed\n");
        PrintCallback(buffer);
        goto destroy_iobuf;
    }
    
    if (conntype == 1) {//server
        for (i = 0; i < RDMA_MAX_WQE; i++) {//
            //printf("i: %d\n",i);
            header = conn->header_buf + i;
            if (rdmaPostRecv(conn, header) == C_ERR) {
                //serverLog(LL_WARNING, "RDMA: post recv failed");
                printf("headers: RDMA: post recv failed\n");
                sprintf(buffer,"headers: RDMA: post recv failed\n");
                PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }

        for (i = 0; i < RDMA_MAX_WQE; i++) {
            response = conn->response_buf + i;
            //printf("msg: %d\n",msg);
            if(EnQueue(conn->freeList,response) == NULL) { //TODO error handler
                printf("conn freeList has no more memory can be malloced\n");
                sprintf(buffer,"conn freeList has no more memory can be malloced\n");
                PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }
        printf("response freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
    } else {//client

        for (i = 0; i < RDMA_MAX_WQE; i++) {
            response = conn->response_buf + i;
            //printf("msg: %d\n",msg);
            if (rdmaPostRecv(conn, response) == C_ERR) {
                //serverLog(LL_WARNING, "RDMA: post recv failed");
                printf("responses: RDMA: post recv failed\n");
                sprintf(buffer,"responses: RDMA: post recv failed\n");
                PrintCallback(buffer);
                goto destroy_iobuf;
            }

        }
        
        for (i = 0; i < RDMA_MAX_WQE; i++) {
            header = conn->header_buf + i;
            //printf("msg: %d\n",msg);
            if(EnQueue(conn->freeList,header) == NULL) { //TODO error handler
                printf("conn freeList has no more memory can be malloced\n");
                sprintf(buffer,"conn freeList has no more memory can be malloced\n");
                PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }
        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
        //printf("get size: %d\n",GetSize(conn->freeList));
    }
    
    conn->header_pool = headerPool;
    conn->response_pool = responsePool;

    conn->pool = pool;
    conn->mr = mr;
    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(conn);
    return C_ERR;
}

static Connection* AllocConnection(struct rdma_cm_id *cm_id, struct ConnectionEvent *conn_ev, int conntype) {

    Connection* conn = (Connection*)malloc(sizeof(Connection));

    memset(conn,0,sizeof(Connection));

    //conn->waitRead = InitQueue();

    conn->freeList = InitQueue();

    //conn->state = CONN_STATE_NONE;

    conn->conntype = conntype;
    
    conn->cm_id = cm_id;

    conn->connContext = NULL;

    conn->cFd = open_event_fd();

    /*
    conn->pd = NULL;
    conn->comp_channel = NULL;
    conn->cq = NULL;
    conn->header_buf = NULL;
    conn->header->mr = NULL;
    conn->response_buf = NULL;
    conn->response_mr = NULL;
    conn->header_pool = NULL;
    conn->response_pool = NULL;
    conn->pool = NULL;
    conn->mr = NULL;
    */

    //conn->initFd = open_event_fd();
    //conn->eFd = open_event_fd();

    int ret = wait_group_init(&conn->wg);

    if(ret) {
        printf("init conn wg failed, err:%d",ret);
        sprintf(buffer,"init conn wg failed, err:%d",ret);
        PrintCallback(buffer);
        goto error;
    }
    conn->lockInitialized = 0;

    ret = pthread_spin_init(&conn->lock,PTHREAD_PROCESS_SHARED);

    if(ret) {
        printf("init conn spin lock failed, err:%d",ret);
        sprintf(buffer,"init conn spin lock failed, err:%d",ret);
        PrintCallback(buffer);
        goto error;
    }
    conn->lockInitialized = 1;

    ret = build_connection(conn_ev, conn);

    if(ret == C_ERR) {
        printf("server build connection failed");
        sprintf(buffer,"server build connection failed");
        PrintCallback(buffer);
        goto error;
        //return NULL;
    }

    //conn->memory_pool = InitMemoryPool(MEMORY_BLOCK_COUNT, MEMORY_BLOCK_SIZE);
    //conn->memory = InitMemoryArea(memoryCapacity)

    if(!rdmaSetupIoBuf(conn, conn_ev, conntype)) {
        printf("set up io buf failed\n");
        sprintf(buffer,"set up io buf failed\n");
        PrintCallback(buffer);
        goto error;
        //return NULL;
    };

    //conn->msg_mr = ibv_reg_mr(cm_id->pd, conn->memory_pool->original_mem, conn->memory_pool->original_mem_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    
    //printf("AllocConnection %ld %ld %ld\n", (uint64_t)conn->memory->original_mem,  (uint64_t)conn->memory->original_mem + conn->memory->original_mem_size,conn->msg_mr->rkey);
    //conn->memory->localRegion->key = conn->msg_mr->rkey;
    //conn->connId = -1;
    
    return conn;
error:

    DestroyQueue(conn->freeList);
    conn->freeList = NULL;
    conn->state = CONN_STATE_ERROR;
    conn->cm_id = NULL;
    close(conn->cFd);
    conn->cFd = -1;
    if(conn->wg.wgInitialized == 1) {
        wait_group_destroy(&conn->wg);
    }
    if(conn->lockInitialized == 1) {
        pthread_spin_destroy(&conn->lock);
    }
    free(conn);
    return NULL;
    //return conn;
}

static int UpdateConnection(Connection* conn) {
    struct ibv_device_attr device_attr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_cq *cq = NULL;
    Response* response;
    Header* header;
    
    conn->cm_id->verbs = conn->pd->context;

    if (ibv_query_device(conn->cm_id->verbs, &device_attr)) {
        //serverLog(LL_WARNING, "RDMA: ibv ibv query device failed");
        //TODO error handler
        printf("RDMA: ibv query device failed\n");
        sprintf(buffer,"RDMA: ibv query device failed\n");
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }
    
    cq = ibv_create_cq(conn->cm_id->verbs, RDMA_MAX_WQE * 2, NULL, conn->comp_channel, 0);//when -1, cq is null?
    if (!cq) {
        //serverLog(LL_WARNING, "RDMA: ibv create cq failed");
        //TODO error handler
        printf("RDMA: ibv create cq failed: cq:%d\n",cq);
        sprintf(buffer,"RDMA: ibv create cq failed: cq:%d\n",cq);
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }

    
    conn->cq = cq;
    ibv_req_notify_cq(cq, 0);

    conn->cFd = open_event_fd();
    //conn->initFd = open_event_fd();

    
    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = RDMA_MAX_WQE;
    init_attr.cap.max_recv_wr = RDMA_MAX_WQE;
    init_attr.cap.max_send_sge = device_attr.max_sge;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = conn->cq;
    init_attr.recv_cq = conn->cq;
    int ret = rdma_create_qp(conn->cm_id, conn->pd, &init_attr);
    if (ret) {//TODO error handler
        //serverLog(LL_WARNING, "RDMA: create qp failed");
        printf("RDMA: create qp failed: %s\n",strerror(errno));
        sprintf(buffer,"RDMA: create qp failed: %s\n",strerror(errno));
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
        //return C_ERR;
    }


    for (int i = 0; i < RDMA_MAX_WQE; i++) {
        response = conn->response_buf + i;
        //printf("msg: %d\n",msg);
        if (rdmaPostRecv(conn, response) == C_ERR) {//TODO error handler
            //serverLog(LL_WARNING, "RDMA: post recv failed");
            printf("responses: RDMA: post recv failed\n");
            sprintf(buffer,"responses: RDMA: post recv failed\n");
            PrintCallback(buffer);
            //return C_ERR;
            goto error;
            //goto destroy_iobuf;
        }

    }
    
    for (int i = 0; i < RDMA_MAX_WQE; i++) {
        header = conn->header_buf + i;
        //printf("msg: %d\n",msg);
        if(EnQueue(conn->freeList,header) == NULL) { //TODO error handler
            printf("no more memory can be malloced\n");
            sprintf(buffer,"no more memory can be malloced\n");
            PrintCallback(buffer);
            //return C_ERR;
            goto error;
            //goto destroy_iobuf;
        }
    }
    printf("header freeList size: %d\n",GetSize(conn->freeList));
    sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
    PrintCallback(buffer);
    //printf("get size: %d\n",GetSize(conn->freeList));
    

    return C_OK;
error:
    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            printf("Failed to destroy qp: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            printf("%d\n",ret);
            sprintf(buffer,"%d\n",ret);
            PrintCallback(buffer);
            printf("Failed to destroy cq: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }        
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
    }
    close(conn->cFd);
    conn->cFd = -1;
    return C_ERR;
}

static int ReConnect(Connection* conn) {
    struct addrinfo *addr;
    struct rdma_cm_id *id;
    struct rdma_event_channel *ec = ((struct RdmaContext*)(conn->csContext))->ec;
    char *ip = ((struct RdmaContext*)(conn->csContext))->ip;
    char *port = ((struct RdmaContext*)(conn->csContext))->port;
    int ret;
    struct RdmaContext* client = ((struct RdmaContext*)(conn->csContext));
    struct ConnectionEvent* conn_ev = client->conn_ev;
    

    getaddrinfo(ip, port, NULL, &addr);
    //ec = rdma_create_event_channel();
    //conn->cm_id = NULL;

    rdma_create_id(ec, &id, NULL, RDMA_PS_TCP);
    conn->cm_id = id;
    client->listen_id = id;
    conn_ev->cm_id = id;
    EpollAddConnectEvent(client->listen_id->channel->fd,conn_ev);


    ((struct RdmaContext*)conn->csContext)->isReConnect = true;
    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    //ret = rdma_resolve_route(conn->cm_id, TIMEOUT_IN_MS);
    if(ret) {//TODO
        //TODO error handler & release resources ()
        printf("Failed to resolve addr: %s\n", strerror(errno));
        sprintf(buffer,"Failed to resolve addr: %s\n", strerror(errno));
        PrintCallback(buffer);
        return C_ERR;
    }
    

    if(wait_event(client->cFd) < 0) {//TODO error handler
        return C_ERR;
    }

    return C_OK;
}

static int DisConnect(Connection* conn, bool force) { //TODO ()
    //printf("conn cm_id %d\n",conn->cm_id);
    sprintf(buffer,"DisConnect: conn %d , force %d\n",conn,force);
    PrintCallback(buffer);

    if(force) {
        pthread_spin_lock(&conn->lock);
        if(conn->state == CONN_STATE_CLOSING || conn->state == CONN_STATE_CLOSED) {
            pthread_spin_unlock(&conn->lock);
            return C_OK;
        } else {
            conn->state = CONN_STATE_CLOSING;
            pthread_spin_unlock(&conn->lock);
            printf("force disconnect\n");
            sprintf(buffer,"force disconnect\n");
            PrintCallback(buffer);

            EpollDelConnEvent(conn->comp_channel->fd);
            int ret = rdma_disconnect(conn->cm_id);
            if(ret != 0) {
                return C_ERR;
            }
            return C_OK;
        }
    }

    if (conn->conntype == 1) {//server
        //for(;conn->state != CONN_STATE_CLOSED;) { //阻塞实现方式?
        //    usleep(10 * 1000);
        //} 

        //conn->state = CONN_STATE_CLOSING;
        if (wait_event(conn->cFd) <= 0) {//TODO error handler
		    return C_ERR;
	    }

        if(conn->cFd > 0) {
            notify_event(conn->cFd,0);
            close(conn->cFd);
            conn->cFd  = -1;
        }


        free(conn);

        printf("server connect closed success\n");
        sprintf(buffer,"server connect closed success\n");
        PrintCallback(buffer);
        return C_OK;
    } else {//client
        
        //TODO 如果另一端异常关闭，则此时client已被free掉，这里会报错
        pthread_spin_lock(&conn->lock);
        if(conn->state == CONN_STATE_CONNECTED) {//正常关闭
            pthread_spin_unlock(&conn->lock);
            //printf("client conn poll all cq event bufore disconnect\n");
            //transport_sendAndRecv_event_cb(conn);
            
            conn->state = CONN_STATE_CLOSING;
            
            //TODO 判断client是否已经被free了
            EpollDelConnEvent(conn->comp_channel->fd);// do not transfer data 

            //wait_event(&conn->eFd);

            //wait_group_wait(&(conn->wg));

            int ret= rdma_disconnect(conn->cm_id);
            
            if(ret != 0) {
                return C_ERR;
            }

            //for (;conn->state != CONN_STATE_CLOSED;) {//阻塞实现方式?
            //    usleep(10 * 1000);
            //}
            if(wait_event(conn->cFd) <= 0) {//TODO error handler
		        return C_ERR;
	        }

            //printf("client connect closed success\n");
        } else {//对端异常关闭 异常关闭
            pthread_spin_unlock(&conn->lock);

            EpollDelConnEvent(conn->comp_channel->fd);
            if(wait_event(conn->cFd) <= 0) {//TODO error handler
		        return C_ERR;
	        }
            printf("client connect has been closed\n");
            sprintf(buffer,"client connect has been closed\n");
            PrintCallback(buffer);
        }

        if(conn->cFd > 0) {
            notify_event(conn->cFd,0);
            close(conn->cFd);
            conn->cFd = -1;
        }
        //if(conn->initFd > 0) {
        //    notify_event(conn->initFd,0);
        //    close(conn->initFd);
        //    conn->initFd = -1;
        //}
        

        //conn->connContext = NULL;
        //((struct RdmaContext*)conn->csContext)->conn = NULL;
        //conn->csContext = NULL;

        //rdma_destroy_id(conn->cm_id);
        //free(conn);

        printf("client connect closed success\n");
        sprintf(buffer,"client connect closed success\n");
        PrintCallback(buffer);

        return C_OK;
    }
}

static int rdmaSendCommand(Connection *conn, void *block, int32_t len) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    struct rdma_cm_id *cm_id = conn->cm_id;
    if(conn->conntype == 1) {//server
        sge.addr = (uint64_t)block;
        sge.length = len;
        sge.lkey = conn->response_mr->lkey;
    } else {
        sge.addr = (uint64_t)block;
        sge.length = len;
        sge.lkey = conn->header_mr->lkey;
        sprintf(buffer,"conn header mr lkey (%d)\n",conn->header_mr->lkey);
        PrintCallback(buffer);
    }
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    //send_wr.wr_id = (uint64_t)buff;
    send_wr.wr_id = 0;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    int ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    sprintf(buffer,"ibv post send: qp:%d \n", cm_id->qp);
    PrintCallback(buffer);

    if (ret != 0) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        printf("RDMA: post send failed: %d", ret);
        sprintf(buffer,"RDMA: post send failed: %d", ret);
        PrintCallback(buffer);
        return C_ERR;
    }

    return C_OK;
}
/*
static int rdmaSendCommand(Connection *conn, struct rdma_cm_id *cm_id, void *msg) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    //RdmaMessage *_msg;
    void *_msg;
    int i, ret;

    if(conn->conntype == 1) {//server
        //RdmaMessage *_msg;
        DeQueue(conn->freeList,&(_msg));//freeList如果没有free的block的话，需要考虑怎么处理
        
        if(_msg == NULL) {//TODO
            printf("freeList has no more free msg\n");
            return C_ERR;
        }
        //_msg = conn->response_buf;
        memcpy(_msg, msg, sizeof(RdmaMessage));
        sge.addr = (uint64_t)_msg;
        sge.length = sizeof(RdmaMessage);
        sge.lkey = conn->response_mr->lkey;
    } else {//client
        //RdmaMessage *_msg;
        //printf("get size: %d\n",GetSize(conn->freeList));
        DeQueue(conn->freeList,&(_msg));//freeList如果没有free的block的话，需要考虑怎么处理
        //DeQueue(conn->freeList,&(_msg));
        //printf("message: %d\n",_msg);
        if(_msg == NULL) {//TODO
            printf("freeList has no more free msg\n");
            return C_ERR;
        }
        //_msg = conn->header_buf;
        memcpy(_msg, msg, sizeof(RdmaMessage));
        sge.addr = (uint64_t)_msg;
        sge.length = sizeof(RdmaMessage);
        sge.lkey = conn->header_mr->lkey;
    }
    

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = (uint64_t)_msg;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        printf("RDMA: post send failed: %d", ret);
        return C_ERR;
    }

    return C_OK;
}
*/

/*
static int connRdmaRegisterRx(Connection *conn, char *addr, uint32_t length, uint32_t rkey) {
    RdmaMessage msg;

    msg.memory.opcode = htons(RegisterXferMemory); // TODO maybe modify ()
    //printf("%d\n",RegisterXferMemory);
    //printf("msg.opcode:%d\n",htons(RegisterXferMemory));
    msg.memory.addr = htonu64((uint64_t)addr);
    //printf("%d\n",msg.memory.addr);
    msg.memory.length = htonl(length);
    //printf("%d\n",length);
    msg.memory.key = htonl(rkey);
    //printf("%d\n",rkey);

    //conn->dx.offset = 0;
    //conn->dx.pos = 0;

    return rdmaSendCommand(conn, conn->cm_id, &msg);
}
*/

static int connRdmaSendHeader(Connection *conn, void* header, int32_t len) {
    //void* block = getHeaderBuff(conn);
    //Header *header = (Header*)block;
    //header->memory.opcode = htons(RegisterXferMemory);
    //printf("lock3\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    int ret = rdmaSendCommand(conn,header,sizeof(Header));
    pthread_spin_unlock(&conn->lock);
    //printf("lock3\n");
    return ret;
    
}

static int connRdmaSendResponse(Connection *conn, Response *response, int32_t len) {
    //printf("lock4\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    
    int ret = rdmaSendCommand(conn,response,len);
    pthread_spin_unlock(&conn->lock);
    //printf("lock4\n");
    return ret;
}

static int rdmaPostRecvHeader(Connection *conn, void *headerCtx) {
    //printf("lock1\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {//test problem
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
        //goto error;
    }
    Header *header = (Header*)headerCtx;
    
    int ret = rdmaPostRecv(conn,header);
    pthread_spin_unlock(&conn->lock);
    //printf("lock1\n");
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}

static int rdmaPostRecvResponse(Connection *conn, void *responseCtx) {
    //printf("lock2\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {//test problem
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
        //goto error;
    }
    Response *response = (Response*)responseCtx;
    
    int ret = rdmaPostRecv(conn,response);
    pthread_spin_unlock(&conn->lock);
    //printf("lock2\n");
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}

/*
static int connRdmaSendResponse(Connection *conn, MemoryEntry *entry) {
    
    RdmaMessage msg;

    msg.response.opcode = htons(ResponseRdmaMessage);
    msg.response.statusCode = htons(Success);
    msg.response.addr = htonu64((uint64_t)(entry->remote_buff));

    free(entry);
    //msg.response.length = htonl(length);
    //msg.response.leftSpace = htonl(leftSpace);
    
    //printf("send response cm_id: %d\n",conn->cm_id);
    return rdmaSendCommand(conn, conn->cm_id, &msg);
}
*/

static void* getDataBuffer(Connection *conn, uint32_t size, int64_t timeout_us,int64_t *ret_size) {//TODO 线程安全
    //struct rdma_cm_id *cm_id = conn->cm_id;
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();
    //if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) { //在使用之前需要判断连接的状态
    //    printf("conn state error or conn closed: state(%d)\n",conn->state);//TODO change print msg
    //    return NULL;
    //}
    
    if(timeout_us <= 0) {
        if(conn->send_timeout_ns == -1 || conn->send_timeout_ns == 0) {
            dead_line = -1;
        } else {
           dead_line = now+conn->send_timeout_ns; 
        }
    } else {
        dead_line = now+timeout_us*1000;
    }


    while(1) {//TODO get timeout handler
        pthread_spin_lock(&conn->lock);
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            *ret_size = -1;
            printf("get data buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);//TODO change print msg
            sprintf(buffer,"get data buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        //pthread_spin_unlock(&conn->lock);

        now = get_time_ns();


        if(dead_line == -1) {
            printf("conn(%p) get data buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get data buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            //DisConnect(conn,true);
            return NULL;
        }
        if(now >= dead_line) {
            printf("conn(%p) get data buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get data buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            //DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        //printf("lock5\n");
        
        int index = buddy_alloc(conn->pool->allocation,size / 16);
        if(index == -1) {
            printf("conn(%p) get data buffer failed, no more data buffer can get\n",conn);
            sprintf(buffer,"conn(%p) get data buffer failed, no more data buffer can get\n",conn);
            PrintCallback(buffer);
            //return NULL;
            pthread_spin_unlock(&conn->lock);
            continue;
        }
        buddy_dump(conn->pool->allocation);
        int s = buddy_size(conn->pool->allocation,index);
        printf("index %d (sz = %d)\n",index,s);
        sprintf(buffer,"index %d (sz = %d)\n",index,s);
        PrintCallback(buffer);
        assert(s >= (size / 16));

        pthread_spin_unlock(&conn->lock);
        //printf("lock5\n");

        *ret_size = s * 16;
        void* send_buffer = conn->pool->original_mem + index * 16;  //TODO BLOCK_SIZE
        //break;
        return send_buffer;
    }
    
}

static void* getResponseBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size) {//TODO 线程安全
    Response* response;
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();

    if(timeout_us <= 0) {
        if(conn->send_timeout_ns == -1 || conn->send_timeout_ns == 0) {
            dead_line = -1;
        } else {
           dead_line = now+conn->send_timeout_ns; 
        }
    } else {
        dead_line = now+timeout_us*1000;
    }

    while(1) {//TODO get timeout handler
        pthread_spin_lock(&conn->lock);
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            *ret_size = -1;
            printf("get response buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);//TODO change print msg
            sprintf(buffer,"conn(%p) get data buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        //printf("lock6\n");
        
        now = get_time_ns();
        if(dead_line == -1) {
            printf("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        if(now >= dead_line) {
            printf("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        if(DeQueue(conn->freeList,&(response)) == NULL) {//TODO freeList如果没有free的block的话，需要考虑怎么处理,OK
            printf("conn(%d) get response buffer failed, no more response buffer can get\n", conn);
            sprintf(buffer,"conn(%d) get response buffer failed, no more response buffer can get\n", conn);
            PrintCallback(buffer);
            //return NULL;
            pthread_spin_unlock(&conn->lock);
            continue;
        }

        pthread_spin_unlock(&conn->lock);
        //printf("lock6\n");

        printf("response freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);

        *ret_size = sizeof(Response);
        return response;
    }

    //Response *resp = (Response*)block;
    //resp->response.opcode = htons(ResponseRdmaMessage);
    //resp->response.statusCode = htons(Success);
    //resp->response.rsvd = {'r','e','s','p'};
    //resp.response.addr = htonu64((uint64_t)(entry->remote_buff));	

    //return response;
}

static void* getHeaderBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size) {//TODO 线程安全
    Header *header;
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();

    if(timeout_us <= 0) {
        if(conn->send_timeout_ns == -1 || conn->send_timeout_ns == 0) {
            dead_line = -1;
        } else {
           dead_line = now+conn->send_timeout_ns; 
        }
    } else {
        dead_line = now+timeout_us*1000;
    }

    while(1) {//TODO get timeout handler
        pthread_spin_lock(&conn->lock);
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            *ret_size = -1;
            printf("get header buffer: conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
            sprintf(buffer,"get header buffer: conn state is not connected: state(%d)\n",conn->state);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        now = get_time_ns();
        if(dead_line == -1) {
            printf("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        if(now >= dead_line) {
            printf("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        //printf("lock7\n");
        if(DeQueue(conn->freeList,&(header)) == NULL) {//TODO freeList如果没有free的block的话，需要考虑怎么处理,OK
            printf("conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            sprintf(buffer,"conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            PrintCallback(buffer);
            //return NULL;
            pthread_spin_unlock(&conn->lock);
            continue;
        }

        pthread_spin_unlock(&conn->lock);
        //printf("lock7\n");

        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);

        *ret_size = sizeof(Header);
        return header;
    }

    //RdmaMessage *header = (RdmaMessage*)block;
    //resp->response.opcode = htons(ResponseRdmaMessage);
    //resp->response.statusCode = htons(Success);
    //resp->response.rsvd = {'r','e','s','p'};
    //resp.response.addr = htonu64((uint64_t)(entry->remote_buff));	

    //return header;
}

static void setConnContext(Connection* conn, void* connContext) {
    pthread_spin_lock(&conn->lock);
    conn->connContext = connContext;
    //notify_event(conn->initFd,0);
    conn->state = CONN_STATE_CONNECTED;
    EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
    pthread_spin_unlock(&conn->lock);
    return;
}

static void setSendTimeoutUs(Connection* conn, int64_t timeout_us) {
    pthread_spin_lock(&conn->lock);
    if(timeout_us > 0) {
        conn->send_timeout_ns = timeout_us * 1000;
    } else {
        conn->send_timeout_ns = -1;
    }
    pthread_spin_unlock(&conn->lock);
    return;
}

static void setRecvTimeoutUs(Connection* conn, int64_t timeout_us) {
    pthread_spin_lock(&conn->lock);
    if(timeout_us > 0) {
        conn->recv_timeout_ns = timeout_us * 1000;
    } else {
        conn->recv_timeout_ns = -1;
    }
    pthread_spin_unlock(&conn->lock);
    return;
}

/*
static void preSendPesp(Connection* conn, MemoryEntry* entry) {
    //TODO 需不需要先把这块内存置为零
    //printf("entry: %d\n",entry);
    int index = (int)((entry->buff - (conn->pool->original_mem)) / 16);  //(char*)
    //printf("index: %d\n",(entry->addr - (char*)(conn->pool->original_mem)));
    buddy_free(conn->pool->allocation, index);
    buddy_dump(conn->pool->allocation);
}
*/

static int releaseDataBuffer(Connection* conn, void* buff) {
    //printf("lock8\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    int index = (int)((buff - (conn->pool->original_mem)) / 16); 
    
    buddy_free(conn->pool->allocation, index);
    buddy_dump(conn->pool->allocation);
    pthread_spin_unlock(&conn->lock);
    //printf("lock8\n");
    return C_OK;
}

static int releaseResponseBuffer(Connection* conn, void* buff) {
    //printf("lock9\n");
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    
    if(EnQueue(conn->freeList,(Response*)buff) == NULL) { //TODO error handler
        pthread_spin_unlock(&conn->lock);
        printf("no more memory can be malloced\n");
        sprintf(buffer,"no more memory can be malloced\n");
        PrintCallback(buffer);
        return C_ERR;
    };
    printf("response freeList size: %d\n",GetSize(conn->freeList));
    sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
    PrintCallback(buffer);
    pthread_spin_unlock(&conn->lock);
    //printf("lock9\n");
    
    return C_OK;
}

static int releaseHeaderBuffer(Connection* conn, void* buff) {
    //printf("lock10\n");
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    if(EnQueue(conn->freeList,(Header*)buff) == NULL) { //TODO error handler
        pthread_spin_unlock(&conn->lock);
        //printf("lock10\n");
        printf("no more memory can be malloced\n");
        sprintf(buffer,"no more memory can be malloced\n");
        PrintCallback(buffer);
        return C_ERR;
    };
    printf("header freeList size: %d\n",GetSize(conn->freeList));
    sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
    PrintCallback(buffer);
    pthread_spin_unlock(&conn->lock);
    //printf("lock10\n");
    return C_OK;
}

static int connAppWrite(Connection *conn, void* buff, void *headerCtx, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    pthread_spin_unlock(&conn->lock);

    Header* header = (Header*)headerCtx;
    header->RdmaAddr = htonu64((uint64_t)buff);
    printf("addr:%d\n",header->RdmaAddr);
    header->RdmaLength = htonl(len);
    printf("length:%d\n",header->RdmaLength);
    header->RdmaKey = htonl(conn->mr->rkey);
    printf("key:%d\n",header->RdmaKey);
    //printf("header:%u\n",header->Opcode);
    //printf("header:%u\n",*((u_int32_t*)(headerCtx + 13)));
    //printf("header:%u\n",header->ArgLen);
    //printf("header:%u\n",header->Size);
    //printf("header:%s\n",header->Arg);

    //size_t offset = offsetof(Header, ArgLen);
    //printf("Offset of Arg: %zu\n", offset);

    //*((u_int64_t*)(header + 57 + 40)) = htonu64((uint64_t)buff);
    //*((u_int32_t*)(header + 57 + 40 + 8)) = htonl(len);
    //*((u_int32_t*)(header + 57 + 40 + 8 + 4)) = htonl(conn->mr->rkey);
    int ret = connRdmaSendHeader(conn, header, len);
    //int ret = connRdmaRegisterRx(conn, buff, len, conn->mr->rkey);

    if (ret==C_ERR) {//TODO
        printf("app write failed\n");
        sprintf(buffer,"app write failed\n");
        PrintCallback(buffer);
        goto failed;
    }
    printf("app write success\n");
    sprintf(buffer,"app write success\n");
    PrintCallback(buffer);
    return C_OK;

failed:
    //TODO close connection, OK
    DisConnect(conn,true);
    return C_ERR;
}

static int connAppSendResp(Connection *conn, void* responseCtx, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    pthread_spin_unlock(&conn->lock);

    Response* response = (Response*)responseCtx;
    //response->RdmaAddr = htonu64((uint64_t)buff);
    //response->RdmaLength = htonl(len);
    //response->RdmaKey = htonl(conn->mr->rkey);
    int ret = connRdmaSendResponse(conn, response, len);
    //int ret = connRdmaRegisterRx(conn, buff, len, conn->mr->rkey);

    if (ret==C_ERR) {//TODO
        printf("app send resp failed\n");
        sprintf(buffer,"app send resp failed\n",conn->state);
        PrintCallback(buffer);
        goto failed;
    }
    printf("app send resp success\n");
    sprintf(buffer,"app send resp success\n");
    PrintCallback(buffer);
    return C_OK;

failed:
    //TODO close connection, OK
    DisConnect(conn,true);
    return C_ERR;
}

static int RdmaRead(Connection *conn, Header *header, MemoryEntry* entry) {//, int64_t now
    //rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = conn->cm_id;
    //RdmaContext *ctx = cm_id->context;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;

    char* remote_addr = (char *)ntohu64(header->RdmaAddr);
    printf("addr:%d\n",header->RdmaAddr);
    uint32_t remote_length = ntohl(header->RdmaLength);
    printf("length:%d\n",header->RdmaLength);
    uint32_t remote_key = ntohl(header->RdmaKey);
    printf("addr:%d\n",header->RdmaKey);

    int64_t now = get_time_ns();
    int64_t dead_line = 0;
    int index;

    if(conn->recv_timeout_ns == -1 || conn->recv_timeout_ns == 0) {
        dead_line = -1;
    } else {
        dead_line = now+conn->recv_timeout_ns; 
    }

    while(1) {
        pthread_spin_lock(&conn->lock);
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            printf("conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);//TODO change print msg
            sprintf(buffer,"conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        now = get_time_ns();
        if(dead_line == -1) {
            printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        if(now >= dead_line) {
            printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            sprintf(buffer,"conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        index = buddy_alloc(conn->pool->allocation,remote_length / 16);
        if(index == -1) {
            printf("rdmaMeta length:%d\n",remote_length/16);
            printf("conn(%p) rdma read failed, there is no space to read\n", conn);
            sprintf(buffer,"conn(%p) rdma read failed, there is no space to read\n", conn);
            PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            //return C_ERR; //TODO maybe return -1
            continue;
        }
        buddy_dump(conn->pool->allocation);
        int s = buddy_size(conn->pool->allocation,index);
        printf("index %d (sz = %d)\n",index,s);
        sprintf(buffer,"index %d (sz = %d)\n",index,s);
        PrintCallback(buffer);
        assert(s >= (remote_length / 16));
        
        pthread_spin_unlock(&conn->lock);        
        break;
    }


    void* addr = conn->pool->original_mem + index * 16;//TODO BLOCK_SIZE

    
    //MemoryEntry* entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
    entry->data_buff = addr;
    //entry->header_buff = header;
    entry->data_len = remote_length;
    //entry->header_len = sizeof(Header);
    entry->isResponse = false;
    //entry->recv_time = now;
    
    int ret;

    sge.addr = (uint64_t)addr;
    sge.lkey = conn->mr->lkey;
    sge.length = remote_length;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = remote_key;
    //send_wr.wr_id = 0;
    send_wr.wr_id = (uint64_t)entry;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        printf("RDMA: rdma read failed: %d", ret);
        sprintf(buffer,"RDMA: rdma read failed: %d", ret);
        PrintCallback(buffer);
        //conn->state = CONN_STATE_ERROR;
        return C_ERR; //TODO
    }
    //conn->dx.offset += (conn->dx_length);

    return C_OK;
}

static int connRdmaRead(Connection *conn, void *block, MemoryEntry *entry) { //, int64_t now//非异步
    //rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = conn->cm_id;
    //RdmaContext *ctx = cm_id->context;
    uint32_t towrite;


    //if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
    //    printf("conn(%p) has been closed\n",conn);
    //    return C_ERR;
    //}
    
    return RdmaRead(conn, (Header*)(block), entry);//, now
}

#endif
