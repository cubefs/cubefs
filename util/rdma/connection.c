#include "connection.h"

int64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

int rdmaPostRecv(Connection *conn, void *block) {

    struct ibv_sge sge;

    struct rdma_cm_id *cm_id = conn->cm_id;

    struct ibv_recv_wr recv_wr, *bad_wr;
    int ret;

    sge.addr = (uint64_t)block;

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
    ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post recv failed: %d", ret);
        //TODO error handler
        //printf("RDMA: post recv failed: %d", ret);
        //sprintf(buffer,"RDMA: post recv failed: %d", ret);
        //PrintCallback(buffer);
        return C_ERR;
    }

    return C_OK;
}

void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        //serverPanic("posix_memalign failed");
        //printf("posix_memalign failed");
        //sprintf(buffer,"posix_memalign failed");
        //PrintCallback(buffer);
    }

    memset(tmp, 0x00, aligned_size);

    return tmp;
}

void rdmaDestroyIoBuf(Connection *conn) {//TODO need to modify
    int index;

    if(conn->freeList) {
        ClearQueue(conn->freeList);
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
    }
    if (conn->header_mr) {
        ibv_dereg_mr(conn->header_mr);
        conn->header_mr = NULL;
    }
    if (conn->header_buf) {
        //TODO 需不需要先把这块内存置为零
        index = (int)(((char*)(conn->header_buf) - (char*)(conn->header_pool->original_mem)) / sizeof(Header));
        buddy_free(conn->header_pool->allocation, index);//TODO 对于dataNode leader来说，收到响应不需要释放内存，对于客户端来说，收到响应需要释放内存
        buddy_dump(conn->header_pool->allocation);
        conn->header_buf = NULL;
    }

    if (conn->response_mr) {
        ibv_dereg_mr(conn->response_mr);
        conn->response_mr = NULL;
    }
    if (conn->response_buf) {
        index = (int)(((char*)(conn->response_buf) - (char*)(conn->response_pool->original_mem)) / sizeof(Response));
        buddy_free(conn->response_pool->allocation, index);//TODO 对于dataNode leader来说，收到响应不需要释放内存，对于客户端来说，收到响应需要释放内存
        buddy_dump(conn->response_pool->allocation);
        conn->response_buf = NULL;
    }
    
}

int rdmaSetupIoBuf(Connection *conn, struct ConnectionEvent *conn_ev, int conntype) {
    MemoryPool* pool = rdmaPool->memoryPool;
    struct ibv_mr* mr = rdmaPool->memoryPool->mr;
    ObjectPool* headerPool = rdmaPool->headerPool;
    ObjectPool* responsePool = rdmaPool->responsePool;

    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t headers_length = sizeof(Header) * WQ_DEPTH;
    size_t responses_length = sizeof(Response) * WQ_DEPTH;
    Header* header;
    Response* response;
    int i;

    //printf("wq_depth: %d\n",WQ_DEPTH);
    //sprintf(buffer,"wq_depth: %d\n",WQ_DEPTH);
    //PrintCallback(buffer);

    //printf("headers length: %d\n",headers_length);
    //sprintf(buffer,"headers length: %d\n",headers_length);
    //PrintCallback(buffer);
    //printf("responses length: %d\n",responses_length);
    //sprintf(buffer,"responses length: %d\n",responses_length);
    //PrintCallback(buffer);

    int index = buddy_alloc(headerPool->allocation, WQ_DEPTH);
    //sprintf(buffer,"%d\n",index);
    //PrintCallback(buffer);
    buddy_dump(headerPool->allocation);
    int s = buddy_size(headerPool->allocation,index);//when index == -1,assert is not pass
    //printf("index %d (sz = %d)\n",index,s);
    //sprintf(buffer,"index %d (sz = %d)\n",index,s);
    //PrintCallback(buffer);
    if(index == -1) {
        //printf("headerPool: there is no space to alloc\n");
        //sprintf(buffer,"headerPool: there is no space to alloc\n");
        //PrintCallback(buffer);
        goto destroy_iobuf; //TODO maybe return -1
    }
    void* addr = headerPool->original_mem + index * sizeof(Header);
    conn->header_buf = addr;//(RdmaMessage*)
    conn->header_mr = ibv_reg_mr(conn->pd, conn->header_buf, headers_length, access);

    if (!conn->header_mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for CMD failed");
        //printf("RDMA: reg header mr failed\n");
        //sprintf(buffer,"RDMA: reg header mr failed\n");
        //PrintCallback(buffer);
        goto destroy_iobuf;
    }

    index = buddy_alloc(responsePool->allocation, WQ_DEPTH);
    buddy_dump(responsePool->allocation);
    s = buddy_size(responsePool->allocation,index);
    //printf("index %d (sz = %d)\n",index,s);
    //sprintf(buffer,"index %d (sz = %d)\n",index,s);
    //PrintCallback(buffer);
    if(index == -1) {
        //printf("responsePool: there is no space to alloc\n");
        //sprintf(buffer,"responsePool: there is no space to alloc\n");
        //PrintCallback(buffer);
        goto destroy_iobuf; //TODO maybe return -1
    }
    addr = responsePool->original_mem + index * sizeof(Response);

    conn->response_buf = addr;//(RdmaMessage*)
    conn->response_mr = ibv_reg_mr(conn->pd, conn->response_buf, responses_length, access);
    if (!conn->response_mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for CMD failed");
        //printf("RDMA: reg response mr failed\n");
        //sprintf(buffer,"RDMA: reg response mr failed\n");
        //PrintCallback(buffer);
        goto destroy_iobuf;
    }
    if (conntype == 1) {//server
        for (i = 0; i < WQ_DEPTH; i++) {//
            header = conn->header_buf + i;
            if (rdmaPostRecv(conn, header) == C_ERR) {
                //serverLog(LL_WARNING, "RDMA: post recv failed");
                //printf("headers: RDMA: post recv failed\n");
                //sprintf(buffer,"headers: RDMA: post recv failed\n");
                //PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }
        //sprintf(buffer,"555");
        //PrintCallback(buffer);
        for (i = 0; i < WQ_DEPTH; i++) {
            response = conn->response_buf + i;
            if(EnQueue(conn->freeList,response) == NULL) { //TODO error handler
                //printf("conn freeList has no more memory can be malloced\n");
                //sprintf(buffer,"conn freeList has no more memory can be malloced\n");
                //PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }
        //printf("response freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
    } else {//client

        for (i = 0; i < WQ_DEPTH; i++) {
            response = conn->response_buf + i;
            if (rdmaPostRecv(conn, response) == C_ERR) {
                //printf("responses: RDMA: post recv failed\n");
                //sprintf(buffer,"responses: RDMA: post recv failed\n");
                //PrintCallback(buffer);
                goto destroy_iobuf;
            }

        }
        
        for (i = 0; i < WQ_DEPTH; i++) {
            header = conn->header_buf + i;
            if(EnQueue(conn->freeList,header) == NULL) { //TODO error handler
                //printf("conn freeList has no more memory can be malloced\n");
                //sprintf(buffer,"conn freeList has no more memory can be malloced\n");
                //PrintCallback(buffer);
                goto destroy_iobuf;
            }
        }
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
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

Connection* AllocConnection(struct rdma_cm_id *cm_id, struct ConnectionEvent *conn_ev, int conntype) {
    Connection* conn = (Connection*)malloc(sizeof(Connection));
    memset(conn,0,sizeof(Connection));

    conn->freeList = InitQueue();

    conn->conntype = conntype;
    
    conn->cm_id = cm_id;

    conn->connContext = NULL;

    conn->cFd = open_event_fd();



    int ret = wait_group_init(&conn->wg);
    if(ret) {
        //printf("init conn wg failed, err:%d",ret);
        //sprintf(buffer,"init conn wg failed, err:%d",ret);
        //PrintCallback(buffer);
        goto error;
    }
    conn->lockInitialized = 0;
    ret = pthread_spin_init(&conn->lock,PTHREAD_PROCESS_SHARED);
    if(ret) {
        //printf("init conn spin lock failed, err:%d",ret);
        //sprintf(buffer,"init conn spin lock failed, err:%d",ret);
        //PrintCallback(buffer);
        goto error;
    }
    conn->lockInitialized = 1;

    ret = build_connection(conn_ev, conn);
    if(ret == C_ERR) {
        //printf("server build connection failed");
        //sprintf(buffer,"server build connection failed");
        //PrintCallback(buffer);
        goto error;
        //return NULL;
    }

    if(!rdmaSetupIoBuf(conn, conn_ev, conntype)) {
        //printf("set up io buf failed\n");
        //sprintf(buffer,"set up io buf failed\n");
        //PrintCallback(buffer);
        goto error;
        //return NULL;
    };

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
}

int UpdateConnection(Connection* conn) {
    struct ibv_device_attr device_attr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_cq *cq = NULL;
    Response* response;
    Header* header;
    
    conn->cm_id->verbs = conn->pd->context;

    if (ibv_query_device(conn->cm_id->verbs, &device_attr)) {
        //serverLog(LL_WARNING, "RDMA: ibv ibv query device failed");
        //TODO error handler
        //printf("RDMA: ibv query device failed\n");
        //sprintf(buffer,"RDMA: ibv query device failed\n");
        //PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }
    
    cq = ibv_create_cq(conn->cm_id->verbs, MIN_CQE_NUM, NULL, conn->comp_channel, 0);//when -1, cq is null?     RDMA_MAX_WQE * 2
    if (!cq) {
        //serverLog(LL_WARNING, "RDMA: ibv create cq failed");
        //TODO error handler
        //printf("RDMA: ibv create cq failed: cq:%d\n",cq);
        //sprintf(buffer,"RDMA: ibv create cq failed: cq:%d\n",cq);
        //PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }

    
    conn->cq = cq;
    ibv_req_notify_cq(cq, 0);

    conn->cFd = open_event_fd();

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = WQ_DEPTH;
    init_attr.cap.max_recv_wr = WQ_DEPTH;
    init_attr.cap.max_send_sge = device_attr.max_sge;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = conn->cq;
    init_attr.recv_cq = conn->cq;
    int ret = rdma_create_qp(conn->cm_id, conn->pd, &init_attr);
    if (ret) {//TODO error handler
        //serverLog(LL_WARNING, "RDMA: create qp failed");
        //printf("RDMA: create qp failed: %s\n",strerror(errno));
        //sprintf(buffer,"RDMA: create qp failed: %s\n",strerror(errno));
        //PrintCallback(buffer);
        goto error;
    }


    for (int i = 0; i < WQ_DEPTH; i++) {
        response = conn->response_buf + i;
        if (rdmaPostRecv(conn, response) == C_ERR) {//TODO error handler
            //serverLog(LL_WARNING, "RDMA: post recv failed");
            //printf("responses: RDMA: post recv failed\n");
            //sprintf(buffer,"responses: RDMA: post recv failed\n");
            //PrintCallback(buffer);
            goto error;
        }

    }
    
    for (int i = 0; i < WQ_DEPTH; i++) {
        header = conn->header_buf + i;
        if(EnQueue(conn->freeList,header) == NULL) { //TODO error handler
            //printf("no more memory can be malloced\n");
            //sprintf(buffer,"no more memory can be malloced\n");
            //PrintCallback(buffer);
            goto error;
        }
    }
    //printf("header freeList size: %d\n",GetSize(conn->freeList));
    //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
    //PrintCallback(buffer);
    

    return C_OK;
error:
    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //printf("%d\n",ret);
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }        
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
    }
    close(conn->cFd);
    conn->cFd = -1;
    return C_ERR;
}

int ReConnect(Connection* conn) {
    struct addrinfo *addr;
    struct rdma_cm_id *id;
    struct rdma_event_channel *ec = ((struct RdmaContext*)(conn->csContext))->ec;
    char *ip = ((struct RdmaContext*)(conn->csContext))->ip;
    char *port = ((struct RdmaContext*)(conn->csContext))->port;
    int ret;
    struct RdmaContext* client = ((struct RdmaContext*)(conn->csContext));
    struct ConnectionEvent* conn_ev = client->conn_ev;
    

    getaddrinfo(ip, port, NULL, &addr);

    rdma_create_id(ec, &id, NULL, RDMA_PS_TCP);
    conn->cm_id = id;
    client->listen_id = id;
    conn_ev->cm_id = id;
    EpollAddConnectEvent(client->listen_id->channel->fd,conn_ev);


    ((struct RdmaContext*)conn->csContext)->isReConnect = true;
    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if(ret) {//TODO
        //TODO error handler & release resources ()
        //printf("Failed to resolve addr: %s\n", strerror(errno));
        //sprintf(buffer,"Failed to resolve addr: %s\n", strerror(errno));
        //PrintCallback(buffer);
        return C_ERR;
    }
    

    if(wait_event(client->cFd) < 0) {//TODO error handler
        return C_ERR;
    }

    return C_OK;
}

int DisConnect(Connection* conn, bool force) { //TODO ()

    if(force) {
        pthread_spin_lock(&conn->lock);
        if(conn->state == CONN_STATE_CLOSING || conn->state == CONN_STATE_CLOSED) {
            pthread_spin_unlock(&conn->lock);
            return C_OK;
        } else {
            conn->state = CONN_STATE_CLOSING;
            pthread_spin_unlock(&conn->lock);
            //printf("force disconnect\n");
            //sprintf(buffer,"force disconnect\n");
            //PrintCallback(buffer);
            EpollDelConnEvent(conn->comp_channel->fd);
            int ret = rdma_disconnect(conn->cm_id);
            if(ret != 0) {
                return C_ERR;
            }
            return C_OK;
        }
    }

    if (conn->conntype == 1) {//server
        if (wait_event(conn->cFd) <= 0) {//TODO error handler
		    return C_ERR;
	    }

        if(conn->cFd > 0) {
            notify_event(conn->cFd,0);
            close(conn->cFd);
            conn->cFd  = -1;
        }


        free(conn);

        //printf("server connect closed success\n");
        //sprintf(buffer,"server connect closed success\n");
        //PrintCallback(buffer);
        return C_OK;
    } else {//client
        
        //TODO 如果另一端异常关闭，则此时client已被free掉，这里会报错
        pthread_spin_lock(&conn->lock);
        if(conn->state == CONN_STATE_CONNECTED) {//正常关闭
            pthread_spin_unlock(&conn->lock);
            
            conn->state = CONN_STATE_CLOSING;
            
            //TODO 判断client是否已经被free了
            EpollDelConnEvent(conn->comp_channel->fd);// do not transfer data

            int ret= rdma_disconnect(conn->cm_id);
            
            if(ret != 0) {
                return C_ERR;
            }

            if(wait_event(conn->cFd) <= 0) {//TODO error handler
		        return C_ERR;
	        }
        } else {//对端异常关闭 异常关闭
            pthread_spin_unlock(&conn->lock);

            EpollDelConnEvent(conn->comp_channel->fd);
            if(wait_event(conn->cFd) <= 0) {//TODO error handler
		        return C_ERR;
	        }
            //printf("client connect has been closed\n");
            //sprintf(buffer,"client connect has been closed\n");
            //PrintCallback(buffer);
        }

        if(conn->cFd > 0) {
            notify_event(conn->cFd,0);
            close(conn->cFd);
            conn->cFd = -1;
        }

        //printf("client connect closed success\n");
        //sprintf(buffer,"client connect closed success\n");
        //PrintCallback(buffer);

        return C_OK;
    }
}

int rdmaSendCommand(Connection *conn, void *block, int32_t len) {
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
    }
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = 0;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    int ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        //printf("RDMA: post send failed: %d", ret);
        //sprintf(buffer,"RDMA: post send failed: %d", ret);
        //PrintCallback(buffer);
        return C_ERR;
    }

    return C_OK;
}

int connRdmaSendHeader(Connection *conn, void* header, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    int ret = rdmaSendCommand(conn,header,sizeof(Header));
    pthread_spin_unlock(&conn->lock);
    return ret;
    
}

int connRdmaSendResponse(Connection *conn, Response *response, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
       //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
       //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
       //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    
    int ret = rdmaSendCommand(conn,response,len);
    pthread_spin_unlock(&conn->lock);
    return ret;
}

int rdmaPostRecvHeader(Connection *conn, void *headerCtx) {
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {//test problem
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    Header *header = (Header*)headerCtx;
    
    int ret = rdmaPostRecv(conn,header);
    pthread_spin_unlock(&conn->lock);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}

int rdmaPostRecvResponse(Connection *conn, void *responseCtx) {;
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {//test problem
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    Response *response = (Response*)responseCtx;
    
    int ret = rdmaPostRecv(conn,response);
    pthread_spin_unlock(&conn->lock);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}

void* getDataBuffer(uint32_t size, int64_t timeout_us,int64_t *ret_size) {//buddy alloc add lock?
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();

    if(timeout_us <= 0) {
        dead_line = -1;
    } else {
        dead_line = now+timeout_us*1000;
    }

    while(1) {//TODO get timeout handler

        //TODO rdmaPool is closed?
        
        now = get_time_ns();
        if(dead_line == -1) {
            //printf("get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            //sprintf(buffer,"get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            //PrintCallback(buffer);
            return NULL;
        }
        if(now >= dead_line) {
            //printf("get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            //sprintf(buffer,"get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            //PrintCallback(buffer);
            return NULL;
        }

        int index = buddy_alloc(rdmaPool->memoryPool->allocation,size / rdmaPoolConfig->memBlockSize);
        if(index == -1) {
            //printf("get data buffer failed, no more data buffer can get\n");
            //sprintf(buffer,"get data buffer failed, no more data buffer can get\n");
            //PrintCallback(buffer);
            continue;
        }
        buddy_dump(rdmaPool->memoryPool->allocation);
        int s = buddy_size(rdmaPool->memoryPool->allocation,index);
        //printf("index %d (sz = %d)\n",index,s);
        assert(s >= (size / rdmaPoolConfig->memBlockSize));

        *ret_size = s * rdmaPoolConfig->memBlockSize;
        void* send_buffer = rdmaPool->memoryPool->original_mem + index * rdmaPoolConfig->memBlockSize;  //TODO BLOCK_SIZE
        //sprintf(buffer,"getDataBuffer: buff(%d)\n",send_buffer);
        //PrintCallback(buffer);
        return send_buffer;
    }
}

void* getResponseBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size) {//TODO 线程安全
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
            //printf("get response buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);//TODO change print msg
            //sprintf(buffer,"get response buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        
        now = get_time_ns();
        if(dead_line == -1) {
            //printf("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        if(now >= dead_line) {
            //printf("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        if(DeQueue(conn->freeList,&(response)) == NULL) {//TODO freeList如果没有free的block的话，需要考虑怎么处理,OK
            //printf("conn(%d) get response buffer failed, no more response buffer can get\n", conn);
            //sprintf(buffer,"conn(%d) get response buffer failed, no more response buffer can get\n", conn);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            continue;
        }

        pthread_spin_unlock(&conn->lock);

        //printf("response freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);

        *ret_size = sizeof(Response);
        return response;
    }
}

void* getHeaderBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size) {//TODO 线程安全
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
            //printf("get header buffer: conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
            //sprintf(buffer,"get header buffer: conn state is not connected: state(%d)\n",conn->state);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        now = get_time_ns();
        if(dead_line == -1) {
            //printf("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }
        if(now >= dead_line) {
            //printf("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            DisConnect(conn,true);
            pthread_spin_unlock(&conn->lock);
            return NULL;
        }

        if(DeQueue(conn->freeList,&(header)) == NULL) {//TODO freeList如果没有free的block的话，需要考虑怎么处理,OK
            //printf("conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            //sprintf(buffer,"conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            continue;
        }

        pthread_spin_unlock(&conn->lock);

        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);

        *ret_size = sizeof(Header);
        return header;
    }
}

void setConnContext(Connection* conn, void* connContext) {
    //sprintf(buffer,"setConnContext: conn %d\n",conn);
    //PrintCallback(buffer);
    //sprintf(buffer,"setConnContext: connContext %d\n",connContext);
    //PrintCallback(buffer);
    //sprintf(buffer,"setConnContext: connState %d\n",conn->state);
    //PrintCallback(buffer);
    pthread_spin_lock(&conn->lock);
    conn->connContext = connContext;
    conn->state = CONN_STATE_CONNECTED;
    //sprintf(buffer,"setConnContext: 111\n");
    //PrintCallback(buffer);
    //sprintf(buffer,"setConnContext: conn->comp_channel %d\n",conn->comp_channel);
    //PrintCallback(buffer);
    //sprintf(buffer,"setConnContext: conn->comp_channel->fd %d\n",conn->comp_channel->fd);
    //PrintCallback(buffer);
    EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
    pthread_spin_unlock(&conn->lock);
    return;
}

void setSendTimeoutUs(Connection* conn, int64_t timeout_us) {
    pthread_spin_lock(&conn->lock);
    if(timeout_us > 0) {
        conn->send_timeout_ns = timeout_us * 1000;
    } else {
        conn->send_timeout_ns = -1;
    }
    pthread_spin_unlock(&conn->lock);
    return;
}

void setRecvTimeoutUs(Connection* conn, int64_t timeout_us) {
    pthread_spin_lock(&conn->lock);
    if(timeout_us > 0) {
        conn->recv_timeout_ns = timeout_us * 1000;
    } else {
        conn->recv_timeout_ns = -1;
    }
    pthread_spin_unlock(&conn->lock);
    return;
}

int releaseDataBuffer(void* buff) {
    //TODO rdmaPool is closed?
    //sprintf(buffer,"releaseDataBuffer: buff(%d)\n",buff);
    //PrintCallback(buffer);
    int index = (int)((buff - (rdmaPool->memoryPool->original_mem)) / (rdmaPoolConfig->memBlockSize));
    buddy_free(rdmaPool->memoryPool->allocation, index);
    buddy_dump(rdmaPool->memoryPool->allocation);
    return C_OK;
}

int releaseResponseBuffer(Connection* conn, void* buff) {
    pthread_spin_lock(&conn->lock);
    if(conn->state != CONN_STATE_CONNECTED) {
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;
    }
    
    if(EnQueue(conn->freeList,(Response*)buff) == NULL) { //TODO error handler
        pthread_spin_unlock(&conn->lock);
        //printf("no more memory can be malloced\n");
        //sprintf(buffer,"no more memory can be malloced\n");
        //PrintCallback(buffer);
        return C_ERR;
    };
    //printf("response freeList size: %d\n",GetSize(conn->freeList));
    //sprintf(buffer,"response freeList size: %d\n",GetSize(conn->freeList));
    //PrintCallback(buffer);
    pthread_spin_unlock(&conn->lock);
    return C_OK;
}

int releaseHeaderBuffer(Connection* conn, void* buff) {
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    if(EnQueue(conn->freeList,(Header*)buff) == NULL) { //TODO error handler
        pthread_spin_unlock(&conn->lock);
        //printf("no more memory can be malloced\n");
        //sprintf(buffer,"no more memory can be malloced\n");
        //PrintCallback(buffer);
        return C_ERR;
    };
    //printf("header freeList size: %d\n",GetSize(conn->freeList));
    //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
    //PrintCallback(buffer);
    pthread_spin_unlock(&conn->lock);
    return C_OK;
}

int connAppWrite(Connection *conn, void* buff, void *headerCtx, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    pthread_spin_unlock(&conn->lock);

    Header* header = (Header*)headerCtx;
    header->RdmaAddr = htonu64((uint64_t)buff);
    //printf("addr:%d\n",header->RdmaAddr);
    header->RdmaLength = htonl(len);
    //printf("length:%d\n",header->RdmaLength);
    header->RdmaKey = htonl(conn->mr->rkey);
    //printf("key:%d\n",header->RdmaKey);
    int ret = connRdmaSendHeader(conn, header, len);

    if (ret==C_ERR) {//TODO
        //printf("app write failed\n");
        //sprintf(buffer,"app write failed\n");
        //PrintCallback(buffer);
        goto failed;
    }
    //printf("app write success\n");
    //sprintf(buffer,"app write success\n");
    //PrintCallback(buffer);
    return C_OK;

failed:
    //TODO close connection, OK
    DisConnect(conn,true);
    return C_ERR;
}

int connAppSendResp(Connection *conn, void* responseCtx, int32_t len) {
    pthread_spin_lock(&conn->lock);
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        //printf("conn state is not connected: state(%d)\n",conn->state);//TODO change print msg
        //sprintf(buffer,"conn state is not connected: state(%d)\n",conn->state);
        //PrintCallback(buffer);
        //TODO release buff
        pthread_spin_unlock(&conn->lock);
        return C_ERR;//TODO 错误码分情况
    }
    pthread_spin_unlock(&conn->lock);

    Response* response = (Response*)responseCtx;
    int ret = connRdmaSendResponse(conn, response, len);

    if (ret==C_ERR) {//TODO
        //printf("app send resp failed\n");
        //sprintf(buffer,"app send resp failed\n");
        //PrintCallback(buffer);
        goto failed;
    }
    //printf("app send resp success\n");
    //sprintf(buffer,"app send resp success\n");
    //PrintCallback(buffer);
    return C_OK;

failed:
    //TODO close connection, OK
    DisConnect(conn,true);
    return C_ERR;
}

int RdmaRead(Connection *conn, Header *header, MemoryEntry* entry) {//, int64_t now
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;

    char* remote_addr = (char *)ntohu64(header->RdmaAddr);
    //printf("addr:%d\n",header->RdmaAddr);
    uint32_t remote_length = ntohl(header->RdmaLength);
    //printf("length:%d\n",header->RdmaLength);
    uint32_t remote_key = ntohl(header->RdmaKey);
    //printf("addr:%d\n",header->RdmaKey);

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
            //printf("conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);//TODO change print msg
            //sprintf(buffer,"conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        now = get_time_ns();
        if(dead_line == -1) {
            //printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        if(now >= dead_line) {
            //printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //sprintf(buffer,"conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            return C_ERR;
        }
        index = buddy_alloc(conn->pool->allocation,remote_length / (rdmaPoolConfig->memBlockSize));
        if(index == -1) {
            //printf("rdmaMeta length:%d\n",remote_length/(rdmaPoolConfig->memBlockSize));
            //printf("conn(%p) rdma read failed, there is no space to read\n", conn);
            //sprintf(buffer,"conn(%p) rdma read failed, there is no space to read\n", conn);
            //PrintCallback(buffer);
            pthread_spin_unlock(&conn->lock);
            //return C_ERR; //TODO maybe return -1
            continue;
        }
        buddy_dump(conn->pool->allocation);
        int s = buddy_size(conn->pool->allocation,index);
        //printf("index %d (sz = %d)\n",index,s);
        assert(s >= (remote_length / (rdmaPoolConfig->memBlockSize)));
        
        pthread_spin_unlock(&conn->lock);        
        break;
    }


    void* addr = conn->pool->original_mem + index * rdmaPoolConfig->memBlockSize;//TODO BLOCK_SIZE

    entry->data_buff = addr;
    entry->data_len = remote_length;
    entry->isResponse = false;
    
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
    send_wr.wr_id = (uint64_t)entry;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        //printf("RDMA: rdma read failed: %d", ret);
        //sprintf(buffer,"RDMA: rdma read failed: %d", ret);
        //PrintCallback(buffer);
        //conn->state = CONN_STATE_ERROR;
        return C_ERR; //TODO
    }
    return C_OK;
}

int connRdmaRead(Connection *conn, void *block, MemoryEntry *entry) { //, int64_t now//非异步
    struct rdma_cm_id *cm_id = conn->cm_id;
    uint32_t towrite;
    
    return RdmaRead(conn, (Header*)(block), entry);//, now
}


