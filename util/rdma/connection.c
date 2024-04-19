#include "connection.h"

int64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

void build_qp_attr(struct ibv_cq *cq, struct ibv_qp_init_attr *qp_attr) {
    memset(qp_attr, 0, sizeof(*qp_attr));
    qp_attr->cap.max_send_wr = WQ_DEPTH;
    qp_attr->cap.max_recv_wr = WQ_DEPTH;
    qp_attr->cap.max_send_sge = WQ_SG_DEPTH;
    qp_attr->cap.max_recv_sge = WQ_SG_DEPTH;
    qp_attr->qp_type = IBV_QPT_RC;
    qp_attr->send_cq = cq;
    qp_attr->recv_cq = cq;
}

int conn_rdma_read(connection *conn, memory_entry* entry) {//, int64_t now
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;

    header* header = (struct request_header*)entry->header_buff;
    //log_debug("entry:%p",entry);
    //log_debug("entry->header_buff:%p",entry->header_buff);
    //log_debug("header:%p",header);

    char* remote_addr = (char *)ntohu64(header->rdma_addr);
    //log_debug("conn rdma read: header->rdma_addr:%p",header->rdma_addr);
    uint32_t remote_length = ntohl(header->rdma_length);
    //log_debug("conn rdma read: header->rdma_length:%d",header->rdma_length);
    uint32_t remote_key = ntohl(header->rdma_key);
    //log_debug("conn rdma read: header->rdma_key:%d",header->rdma_key);
    int64_t now = get_time_ns();
    int64_t dead_line = 0;
    int index;
    if(conn->recv_timeout_ns <= 0) {
        dead_line = now + 2000; //TODO
    } else {
        dead_line = now + conn->recv_timeout_ns;
    }
    while(1) {
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            log_debug("conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);
            return C_ERR;
        }
        now = get_time_ns();
        //if(dead_line == -1) {
        //    log_debug("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
        //    return C_ERR;
        //}
        if(now >= dead_line) {
            printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            return C_ERR;
        }
        index = buddy_alloc(conn->pool->allocation,remote_length / (rdma_pool_config->mem_block_size));
        if(index == -1) {
            log_debug("conn(%p) rdma read failed, there is no space to read\n", conn);
            continue;
        }
        //buddy_dump(conn->pool->allocation);
        int s = buddy_size(conn->pool->allocation,index);
        assert(s >= (remote_length / (rdma_pool_config->mem_block_size)));
        break;
    }
    void* addr = conn->pool->original_mem + index * rdma_pool_config->mem_block_size;
    entry->data_buff = addr;
    entry->data_len = remote_length;
    entry->is_response = false;
    int ret;
    sge.addr = (uintptr_t)addr;
    sge.lkey = conn->mr->lkey;
    sge.length = remote_length;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = remote_key;
    send_wr.wr_id = (uintptr_t)entry;
    send_wr.next = NULL;
    //log_debug("rdma read:%p",conn->qp);
    ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        log_debug("ibv post send: remote read failed: %d",ret);
        return C_ERR;
    }
    return C_OK;
}

int conn_rdma_post_recv(connection *conn, void *block) {
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;
    memory_entry *entry;
    int ret;

    entry = (memory_entry*)malloc(sizeof(memory_entry));
    if (entry == NULL) {
        log_debug("conn rdma post recv: malloc entry failed");
        return C_ERR;
    }
    //log_debug("conn rdma post recv: entry: %p",entry);
    sge.addr = (uintptr_t)block;
    if(conn->conn_type == CONN_TYPE_SERVER) {//server
        sge.length = sizeof(struct request_header);
        sge.lkey = conn->header_mr->lkey;
        entry->header_buff = block;
        entry->header_len = sizeof(struct request_header);
        entry->is_response = false;
        entry->nd = conn->nd;
        //log_debug("entry->header_buff:%p",entry->header_buff);
        //log_debug("entry->header_len:%d",entry->header_len);
        //log_debug("entry->is_response:%d",entry->is_response);
        //log_debug("entry->nd:%d",entry->nd);

    } else {//client
        sge.length = sizeof(struct request_response);
        sge.lkey = conn->response_mr->lkey;
        entry->response_buff = block;
        entry->response_len = sizeof(struct request_response);
        entry->is_response = true;
        entry->nd = conn->nd;
    }

    recv_wr.wr_id = (uintptr_t)entry;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;
    //log_debug("rdma recv:%p",conn->qp);
    ret = ibv_post_recv(conn->qp, &recv_wr, &bad_wr);
    if (ret != 0) {
        log_debug("ibv post recv failed: %d", ret);
        free(entry);
        return C_ERR;
    }
    return C_OK;
}

int conn_rdma_post_send(connection *conn, void *block, int32_t len) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    if(conn->conn_type == CONN_TYPE_SERVER) {
        sge.addr = (uintptr_t)block;
        sge.length = len;
        sge.lkey = conn->response_mr->lkey;
    } else {
        sge.addr = (uintptr_t)block;
        sge.length = len;
        sge.lkey = conn->header_mr->lkey;
    }
    //log_debug("conn rdma post send: sge.addr:%p",sge.addr);
    //log_debug("conn rdma post send: sge.length:%d",sge.length);
    //log_debug("conn rdma post send: sge.lkey:%d",sge.addr);
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = conn->nd;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    //log_debug("rdma send:%p",conn->qp);
    int ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        log_debug("ibv post send failed: %d", ret);
        return C_ERR;
    }
    return C_OK;
}

void rdma_destroy_ioBuf(connection *conn) {
    int index;
    if (conn->header_mr) {
        ibv_dereg_mr(conn->header_mr);
        conn->header_mr = NULL;
    }
    if (conn->header_buf) {
        index = (int)(((char*)(conn->header_buf) - (char*)(conn->header_pool->original_mem)) / sizeof(struct request_header));
        buddy_free(conn->header_pool->allocation, index);
        //buddy_dump(conn->header_pool->allocation);
        conn->header_buf = NULL;
    }
    if (conn->response_mr) {
        ibv_dereg_mr(conn->response_mr);
        conn->response_mr = NULL;
    }
    if (conn->response_buf) {
        index = (int)(((char*)(conn->response_buf) - (char*)(conn->response_pool->original_mem)) / sizeof(struct request_response));
        buddy_free(conn->response_pool->allocation, index);
        //buddy_dump(conn->response_pool->allocation);
        conn->response_buf = NULL;
    }
}

int rdma_setup_ioBuf(connection *conn, int conn_type) {
    memory_pool* pool = rdma_pool->memory_pool;
    struct ibv_mr* mr = rdma_pool->memory_pool->mr;
    object_pool* header_pool = rdma_pool->header_pool;
    object_pool* response_pool = rdma_pool->response_pool;
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t headers_length = sizeof(struct request_header) * WQ_DEPTH;
    size_t responses_length = sizeof(struct request_response) * WQ_DEPTH;
    header* header;
    response* response;
    int i;
    int index = buddy_alloc(header_pool->allocation, WQ_DEPTH);
    //buddy_dump(headerPool->allocation);
    int s = buddy_size(header_pool->allocation,index);//when index == -1,assert is not pass
    if(index == -1) {
        //printf("headerPool: there is no space to alloc\n");
        goto destroy_iobuf;
    }
    void* addr = header_pool->original_mem + index * sizeof(struct request_header);
    conn->header_buf = addr;//(RdmaMessage*)
    conn->header_mr = ibv_reg_mr(conn->worker->pd, conn->header_buf, headers_length, access);
    if (!conn->header_mr) {
        //printf("RDMA: reg header mr failed\n");
        goto destroy_iobuf;
    }
    index = buddy_alloc(response_pool->allocation, WQ_DEPTH);
    //buddy_dump(responsePool->allocation);
    s = buddy_size(response_pool->allocation,index);
    if(index == -1) {
        //printf("responsePool: there is no space to alloc\n");
        goto destroy_iobuf;
    }
    addr = response_pool->original_mem + index * sizeof(struct request_response);
    conn->response_buf = addr;//(RdmaMessage*)
    conn->response_mr = ibv_reg_mr(conn->worker->pd, conn->response_buf, responses_length, access);
    if (!conn->response_mr) {
        //printf("RDMA: reg response mr failed\n");
        goto destroy_iobuf;
    }
    if (conn_type == CONN_TYPE_SERVER) {//server
        for (i = 0; i < WQ_DEPTH; i++) {//
            header = conn->header_buf + i;
            if (conn_rdma_post_recv(conn, header) == C_ERR) {
                //printf("headers: RDMA: post recv failed\n");
                log_debug("headers: RDMA: post recv failed\n");
                goto destroy_iobuf;
            }
        }
        for (i = 0; i < WQ_DEPTH; i++) {
            response = conn->response_buf + i;
            if(EnQueue(conn->free_list,response) == NULL) {
                //printf("conn freeList has no more memory can be malloced\n");
                log_debug("conn freeList has no more memory can be malloced\n");
                goto destroy_iobuf;
            }
        }
    } else {//client
        for (i = 0; i < WQ_DEPTH; i++) {
            response = conn->response_buf + i;
            if (conn_rdma_post_recv(conn, response) == C_ERR) {
                //printf("responses: RDMA: post recv failed\n");
                log_debug("responses: RDMA: post recv failed\n");
                goto destroy_iobuf;
            }
        }
        for (i = 0; i < WQ_DEPTH; i++) {
            header = conn->header_buf + i;
            if(EnQueue(conn->free_list,header) == NULL) {
                //printf("conn freeList has no more memory can be malloced\n");
                log_debug("conn freeList has no more memory can be malloced\n");
                goto destroy_iobuf;
            }
        }
    }
    conn->header_pool = header_pool;
    conn->response_pool = response_pool;
    conn->pool = pool;
    conn->mr = mr;
    return C_OK;
destroy_iobuf:
    rdma_destroy_ioBuf(conn);
    return C_ERR;
}

void destroy_connection(connection *conn) {
    if (conn->free_list) {
        DestroyQueue(conn->free_list);
    }
    if (conn->msg_list) {
        DestroyQueue(conn->msg_list);
    }
    conn->conn_context = NULL;
    conn->context = NULL;
    if (conn->connect_fd) {
        notify_event(conn->connect_fd,1);
    }
    if (conn->msg_fd) {
        notify_event(conn->msg_fd,1);
    }
    if (conn->close_fd) {
       notify_event(conn->close_fd,1);
    }
    pthread_spin_destroy(&conn->spin_lock);
    memset(conn, 0, sizeof(connection));
    free(conn);
}

connection* init_connection(uint64_t nd, int conn_type) {
    int ret = 0;
    connection *conn = (connection*)malloc(sizeof(connection));
    if (conn == NULL) {
        log_debug("create conn mem obj failed");
        return NULL;
    }
    log_debug("malloc connect:%p", conn);

    conn->nd = nd;
    log_debug("conn nd:%d",nd);
    conn->worker = get_worker_by_nd(conn->nd);
    log_debug("conn worker:%p",conn->worker);
    conn->free_list = InitQueue();
    if (conn->free_list == NULL) {
        log_debug("init conn free list failed");
        goto err_free;
    }
    conn->msg_list = InitQueue();
    if (conn->msg_list == NULL) {
        log_debug("init conn msg list failed");
        goto err_destroy_freelist;
    }
    conn->conn_type = conn_type;
    conn->conn_context = NULL;
    conn->context = NULL;
    conn->send_timeout_ns = 0;
    conn->recv_timeout_ns = 0;

    conn->connect_fd = open_event_fd();
    if (conn->connect_fd == NULL) {
        log_debug("open conn connect fd failed");
        goto err_destroy_msglist;
    }
    conn->msg_fd = open_event_fd();
    if (conn->msg_fd == NULL) {
        log_debug("open conn msg fd failed");
        goto err_destroy_connectfd;
    }
    conn->close_fd = open_event_fd();
    if (conn->close_fd == NULL) {
        log_debug("open conn close fd failed");
        goto err_destroy_msgfd;
    }
    ret = pthread_spin_init(&(conn->spin_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_debug("init conn spin lock failed, err:%d", ret);
        goto err_destroy_closefd;
    }

    set_conn_state(conn, CONN_STATE_CONNECTING);
    return conn;
err_destroy_closefd:
    notify_event(conn->close_fd,1);
err_destroy_msgfd:
    notify_event(conn->msg_fd,1);
err_destroy_connectfd:
    notify_event(conn->connect_fd,1);
err_destroy_msglist:
    DestroyQueue(conn->msg_list);
err_destroy_freelist:
    DestroyQueue(conn->free_list);
err_free:
    free(conn);
    return NULL;
}

void destroy_conn_qp(connection *conn) {
    if (conn->qp != NULL && conn->cm_id != NULL) {
        rdma_destroy_qp(conn->cm_id);
        log_debug("conn(%lu-%p) destroy qp, cmid:%p", conn->nd, conn, conn->cm_id);
    }
    conn->qp = NULL;
}

int create_conn_qp(connection *conn, struct rdma_cm_id* id) {
    struct ibv_qp_init_attr qp_attr;
    build_qp_attr(conn->worker->cq, &qp_attr);
    //log_debug("create conn qp:qp_attr cq:%p",qp_attr.send_cq);
    //log_debug("create conn qp:qp_attr cq:%p",qp_attr.recv_cq);
    //log_debug("create conn qp:conn worker cq:%p",conn->worker->cq);
    //log_debug("create conn qp:conn worker pd:%p",conn->worker->pd);
    int ret = rdma_create_qp(id, conn->worker->pd, &qp_attr);
    if (ret != 0) {
        log_debug("conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
        return C_ERR;
    }
    conn->qp = id->qp;
    log_debug("conn(%lu-%p) rdma_create_qp:%p", conn->nd, conn, conn->qp);
    return C_OK;
}

int add_conn_to_server(connection *conn, struct rdma_listener *server) {
    int ret = 0;
    conn->context = server;
    pthread_spin_lock(&server->conn_lock);
    ret = hashmap_put(server->conn_map, conn->nd, (uint64_t)conn);
    pthread_spin_unlock(&server->conn_lock);
    log_debug("add conn(%p) from server(%p) conn_map(%p)",conn,server,server->conn_map);
    return ret >= 0;
}

int del_conn_from_server(connection *conn, struct rdma_listener *server) {
    int ret = 0;
    pthread_spin_lock(&server->conn_lock);
    ret = hashmap_del(server->conn_map, conn->nd);
    pthread_spin_unlock(&server->conn_lock);
    log_debug("del conn(%p) from server(%p) conn_map(%p)",conn,server,server->conn_map);
    return ret >= 0;
}

void conn_disconnect(connection *conn) {
    pthread_spin_lock(&conn->spin_lock);
    if (conn->state != CONN_STATE_DISCONNECTING && conn->cm_id != NULL) {
        rdma_disconnect(conn->cm_id);
    }
    set_conn_state(conn, CONN_STATE_DISCONNECTING);
    pthread_spin_unlock(&conn->spin_lock);
    return;
}

/*
int DisConnect(Connection* conn, bool force) {
    if(force) {
        if(conn->state == CONN_STATE_CLOSING || conn->state == CONN_STATE_CLOSED) {
            return C_OK;
        } else {
            conn->state = CONN_STATE_CLOSING;
            //printf("force disconnect\n");
            //EpollDelConnEvent(conn->comp_channel->fd);
            //DelEpollEvent(conn->comp_channel->fd);
            DelTransferEvent(conn);
            int ret = rdma_disconnect(conn->cm_id);
            if(ret != 0) {
                return C_ERR;
            }
            return C_OK;
        }
    }
    if (conn->conntype == 1) {//server
        wait_event(conn->cFd);
        notify_event(conn->cFd,1);
        free(conn);
        return C_OK;
    } else {//client
        if(conn->state == CONN_STATE_CONNECTED) {//正常关闭
            conn->state = CONN_STATE_CLOSING;
            //EpollDelConnEvent(conn->comp_channel->fd);
            //DelEpollEvent(conn->comp_channel->fd);
            DelTransferEvent(conn);
            int ret= rdma_disconnect(conn->cm_id);
            if(ret != 0) {
                return C_ERR;
            }
            wait_event(conn->cFd);
        } else {//对端异常关闭 异常关闭
            //EpollDelConnEvent(conn->comp_channel->fd);
            //DelEpollEvent(conn->comp_channel->fd);
            DelTransferEvent(conn);
            wait_event(conn->cFd);
        }
        notify_event(conn->cFd,1);
        return C_OK;
    }
}//TODO
*/

int rdma_post_send_header(connection *conn, void* header) {
    if(conn->state != CONN_STATE_CONNECTED) {
        log_debug("post send header failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    int ret = conn_rdma_post_send(conn, header, sizeof(struct request_header));
    return ret;
}

int rdma_post_send_response(connection *conn, response *response) {
    if(conn->state != CONN_STATE_CONNECTED) {
        log_debug("post send response failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    int ret = conn_rdma_post_send(conn, response, sizeof(struct request_response));
    return ret;
}

int rdma_post_recv_header(connection* conn, void *header_ctx) {
    if (conn->state != CONN_STATE_CONNECTED) {
        log_debug("post recv header failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    header *header = (struct request_header*) header_ctx;
    int ret = conn_rdma_post_recv(conn, header);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    conn_disconnect(conn);
    return C_ERR;
}

int rdma_post_recv_response(connection *conn, void *response_ctx) {
    if(conn->state != CONN_STATE_CONNECTED) {
        log_debug("post recv response failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    response *response = (struct response*)response_ctx;
    int ret = conn_rdma_post_recv(conn,response);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    conn_disconnect(conn);
    return C_ERR;
}

int conn_app_write(connection *conn, void* buff, void *header_ctx, int32_t len) {
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_debug("conn app write failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    header* header = (struct request_header*)header_ctx;
    header->rdma_addr = htonu64((uint64_t)buff);
    header->rdma_length = htonl(len);
    header->rdma_key = htonl(conn->mr->rkey);
    //log_debug("conn app write: header->rdma_addr:%p",header->rdma_addr);
    //log_debug("conn app write: header->rdma_length:%d",header->rdma_length);
    //log_debug("conn app write: header->rdma_key:%d",header->rdma_key);
    int ret = rdma_post_send_header(conn, header);
    if (ret == C_ERR) {
        log_debug("app write failed");
        goto error;
    }
    log_debug("app write success");
    return C_OK;
error:
    conn_disconnect(conn);
    return C_ERR;
}

int conn_app_send_resp(connection *conn, void* response_ctx) {
    if (conn->state != CONN_STATE_CONNECTED) {
        log_debug("conn app send response failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    response* response = (struct response*)response_ctx;
    int ret = rdma_post_send_response(conn, response);
    if (ret == C_ERR) {
        log_debug("app send resp failed");
        goto error;
    }
    return C_OK;
error:
    conn_disconnect(conn);
    return C_ERR;
}

void* get_data_buffer(uint32_t size, int64_t timeout_us,int64_t *ret_size) {//buddy alloc add lock?
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();
    if(timeout_us <= 0) {
        dead_line = -1;
    } else {
        dead_line = now+timeout_us*1000;
    }
    while(1) {
        now = get_time_ns();
        if(dead_line == -1) {
            log_debug("get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            return NULL;
        }
        if(now >= dead_line) {
            log_debug("get data buffer timeout, deadline:%ld, now:%ld\n", dead_line, now);
            return NULL;
        }

        int index = buddy_alloc(rdma_pool->memory_pool->allocation,size / rdma_pool_config->mem_block_size);
        if(index == -1) {
            log_debug("get data buffer failed, no more data buffer can get\n");
            continue;
        }
        //buddy_dump(rdmaPool->memoryPool->allocation);
        int s = buddy_size(rdma_pool->memory_pool->allocation,index);
        assert(s >= (size / rdma_pool_config->mem_block_size));
        *ret_size = s * rdma_pool_config->mem_block_size;
        void* send_buffer = rdma_pool->memory_pool->original_mem + index * rdma_pool_config->mem_block_size;
        return send_buffer;
    }
}

void* get_response_buffer(connection *conn, int64_t timeout_us, int32_t *ret_size) {
    response* response = NULL;
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();
    if (timeout_us <= 0) {
        if(conn->send_timeout_ns <= 0) {
            dead_line = now + 2000;//TODO
        } else {
           dead_line = now + conn->send_timeout_ns;
        }
    } else {
        dead_line = now+timeout_us*1000;
    }
    while(1) {
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            *ret_size = -1;
            log_debug("get response buffer: conn(%p) state is not connected: state(%d)\n",conn, conn->state);
            return NULL;
        }
        now = get_time_ns();
        //if (dead_line == -1) {
        //    log_debug("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
        //    //DisConnect(conn,true);
        //    conn_disconnect(conn);//todo
        //    return NULL;
        //}
        if (now >= dead_line) {
            log_debug("conn(%p) get response buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //DisConnect(conn,true);
            conn_disconnect(conn);//todo
            return NULL;
        }
        /*
        if (DeQueue(conn->free_list, &response) == NULL) {//(Item *)
            log_debug("conn(%p) get response buffer failed, no more response buffer can get\n", conn);
            continue;
        }
        */
        DeQueue(conn->free_list, &response);
        if (response == NULL) {//(Item *)
            log_debug("conn(%p) get response buffer failed, no more response buffer can get\n", conn);
            continue;
        }
        *ret_size = sizeof(struct request_response);
        return response;
    }
}

void* get_header_buffer(connection *conn, int64_t timeout_us, int32_t *ret_size) {
    header *header = NULL;
    *ret_size = 0;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();
    if (timeout_us <= 0) {
        if(conn->send_timeout_ns <= 0) {
            dead_line = now + 2000;//TODO
        } else {
           dead_line = now + conn->send_timeout_ns;
        }
    } else {
        dead_line = now+timeout_us*1000;
    }

    while(1) {
        if (conn->state != CONN_STATE_CONNECTED) {
            *ret_size = -1;
            log_debug("get header buffer: conn state is not connected: state(%d)\n",conn->state);
            return NULL;
        }
        now = get_time_ns();
        //if (dead_line == -1) {
        //    log_debug("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
        //    //DisConnect(conn,true);
        //    conn_disconnect(conn);//todo
        //    return NULL;
        //}
        if (now >= dead_line) {
            log_debug("conn(%p) get header buffer timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            //DisConnect(conn,true);
            conn_disconnect(conn);//todo
            return NULL;
        }

        /*
        if (DeQueue(conn->free_list, &header) == NULL) {//(Item *)
            log_debug("conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            continue;
        }
        */
        DeQueue(conn->free_list, &header);
        if (header == NULL) {//(Item *)
            log_debug("conn(%d) get header buffer failed, no more response buffer can get\n", conn);
            continue;
        }
        *ret_size = sizeof(struct request_header);
        return header;
    }
}

memory_entry* get_recv_msg_buffer(connection *conn) {
    wait_event(conn->msg_fd);
    log_debug("wait event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
    memory_entry *entry = NULL;
    /*
    if (DeQueue(conn->msg_list, &entry) == NULL) {//(Item *)
        log_debug("get recv msg buffer failed: dequeue entry is null");
        //TODO
    }
    */
    DeQueue(conn->msg_list, &entry);
    if (entry == NULL) {//(Item *)
        log_debug("conn(%p) get recv msg buffer failed: dequeue(%p) entry is null",conn,conn->msg_list);
        return NULL;
        //TODO
    }
    log_debug("conn(%p) get recv msg buffer success: dequeue(%p) entry is %p",conn,conn->msg_list,entry);
    return entry;
}

memory_entry* get_recv_response_buffer(connection *conn) {
    wait_event(conn->msg_fd);
    log_debug("wait event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
    memory_entry *entry = NULL;
    /*
    if (DeQueue(conn->msg_list, &entry) == NULL) {//(Item *)
        log_debug("get recv response buffer failed: dequeue entry is null");
        //TODO
    }
    */
    DeQueue(conn->msg_list, &entry);
    if (entry == NULL) {//(Item *)
        log_debug("conn(%p) get recv response buffer failed: dequeue(%p) entry is null",conn,conn->msg_list);
        return NULL;
        //TODO
    }
    log_debug("conn(%p) get recv response buffer success: dequeue(%p) entry is %p",conn,conn->msg_list,entry);
    return entry;
}

void set_conn_context(connection* conn, void* conn_context) {
    conn->conn_context = conn_context;
    //conn->state = CONN_STATE_CONNECTED;
    //epoll_rdma_transferEvent_add(conn->comp_channel->fd, conn, transport_sendAndRecv_event_cb);
    //rdma_transferEvent_thread(conn, cq_thread);
    return;
}

void set_send_timeout_us(connection* conn, int64_t timeout_us) {
    //log_debug("%p\n", conn);
    //log_debug("%d\n", conn->send_timeout_ns);
    if(timeout_us > 0) {
        conn->send_timeout_ns = timeout_us * 1000;
    } else {
        conn->send_timeout_ns = -1;
    }
    log_debug("set send timeout us:%ld",conn->send_timeout_ns);
    return;
}

void set_recv_timeout_us(connection* conn, int64_t timeout_us) {
    if(timeout_us > 0) {
        conn->recv_timeout_ns = timeout_us * 1000;
    } else {
        conn->recv_timeout_ns = -1;
    }
    log_debug("set recv timeout us:%ld",conn->recv_timeout_ns);
    return;
}

int release_data_buffer(void* buff) {
    int index = (int)((buff - (rdma_pool->memory_pool->original_mem)) / (rdma_pool_config->mem_block_size));
    buddy_free(rdma_pool->memory_pool->allocation, index);
    //buddy_dump(rdmaPool->memoryPool->allocation);
    return C_OK;
}

int release_response_buffer(connection* conn, void* buff) {
    if(conn->state != CONN_STATE_CONNECTED) {
        log_debug("release response buffer failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    if(EnQueue(conn->free_list,(response*)buff) == NULL) {
        log_debug("release response buffer failed: no more memory can be malloced");
        return C_ERR;
    };
    return C_OK;
}

int release_header_buffer(connection* conn, void* buff) {
    if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_debug("release header buffer failed: conn state is not connected: state(%d)",conn->state);
        return C_ERR;
    }
    if(EnQueue(conn->free_list,(header*)buff) == NULL) {
        log_debug("release header buffer failed: no more memory can be malloced");
        return C_ERR;
    };
    return C_OK;
}

/*
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
        //printf("RDMA: post recv failed: %d", ret);
        return C_ERR;
    }
    return C_OK;
}

void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);
    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        //printf("posix_memalign failed");
    }
    memset(tmp, 0x00, aligned_size);
    return tmp;
}

Connection* AllocConnection(struct rdma_cm_id *cm_id, struct ConnectionEvent *conn_ev, int conntype) {
    Connection* conn = (Connection*)malloc(sizeof(Connection));
    memset(conn,0,sizeof(Connection));
    conn->freeList = InitQueue();
    conn->msgList = InitQueue();
    conn->conntype = conntype;
    conn->cm_id = cm_id;
    conn->connContext = NULL;
    conn->cFd = open_event_fd();
    conn->mFd = open_event_fd();
    int ret = wait_group_init(&conn->wg);
    if(ret) {
        //printf("init conn wg failed, err:%d",ret);
        goto error;
    }
    conn->lockInitialized = 0;
    ret = pthread_spin_init(&conn->lock,PTHREAD_PROCESS_SHARED);
    if(ret) {
        //printf("init conn spin lock failed, err:%d",ret);
        goto error;
    }
    conn->lockInitialized = 1;

    ret = build_connection(conn_ev, conn);
    if(ret == C_ERR) {
        //printf("server build connection failed");
        goto error;
    }
    if(!rdmaSetupIoBuf(conn, conn_ev, conntype)) {
        //printf("set up io buf failed\n");
        goto error;
    };

    return conn;
error:
    DestroyQueue(conn->freeList);
    conn->freeList = NULL;
    conn->state = CONN_STATE_ERROR;
    conn->cm_id = NULL;
    notify_event(conn->cFd, 1);
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
        //printf("RDMA: ibv query device failed\n");
        goto error;
    }

    cq = ibv_create_cq(conn->cm_id->verbs, MIN_CQE_NUM, NULL, conn->comp_channel, 0);//when -1, cq is null?     RDMA_MAX_WQE * 2
    if (!cq) {
        //printf("RDMA: ibv create cq failed: cq:%d\n",cq);
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
    if (ret) {
        //printf("RDMA: create qp failed: %s\n",strerror(errno));
        goto error;
    }
    for (int i = 0; i < WQ_DEPTH; i++) {
        response = conn->response_buf + i;
        if (rdmaPostRecv(conn, response) == C_ERR) {
            //printf("responses: RDMA: post recv failed\n");
            goto error;
        }
    }
    for (int i = 0; i < WQ_DEPTH; i++) {
        header = conn->header_buf + i;
        if(EnQueue(conn->freeList,header) == NULL) {
            //printf("no more memory can be malloced\n");
            goto error;
        }
    }
    return C_OK;
error:
    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            // we continue anyways;
        }
    }
    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->freeList) {
        ClearQueue(conn->freeList);
    }
    notify_event(conn->cFd, 1);
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
    //EpollAddConnectEvent(client->listen_id->channel->fd,conn_ev);
    //epoll_rdma_connectEvent_add(client->listen_id->channel->fd, conn_ev, connection_event_cb);
    rdma_connectEvent_thread(2, client, cm_thread, conn_ev);

    ((struct RdmaContext*)conn->csContext)->isReConnect = true;
    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if(ret) {
        //printf("Failed to resolve addr: %s\n", strerror(errno));
        return C_ERR;
    }
    wait_event(client->cFd);
    return C_OK;
}
*/

/*
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
        //printf("RDMA: post send failed: %d", ret);
        return C_ERR;
    }
    return C_OK;
}

int connRdmaSendHeader(Connection *conn, void* header, int32_t len) {
    if(conn->state != CONN_STATE_CONNECTED) {
        //printf("conn state is not connected: state(%d)\n",conn->state);
        return C_ERR;
    }
    int ret = rdmaSendCommand(conn,header,sizeof(Header));
    return ret;

}

int connRdmaSendResponse(Connection *conn, Response *response, int32_t len) {
    if(conn->state != CONN_STATE_CONNECTED) {
       //printf("conn state is not connected: state(%d)\n",conn->state);
        return C_ERR;
    }
    int ret = rdmaSendCommand(conn,response,len);
    return ret;
}
*/

/*
int rdmaPostRecvHeader(Connection *conn, void *headerCtx) {
    if(conn->state != CONN_STATE_CONNECTED) {
        //printf("conn state is not connected: state(%d)\n",conn->state);
        return C_ERR;
    }
    Header *header = (Header*)headerCtx;
    int ret = rdmaPostRecv(conn, header);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}

int rdmaPostRecvResponse(Connection *conn, void *responseCtx) {
    if(conn->state != CONN_STATE_CONNECTED) {//test problem
        //printf("conn state is not connected: state(%d)\n",conn->state);
        return C_ERR;
    }
    Response *response = (Response*)responseCtx;
    int ret = rdmaPostRecv(conn,response);
    if(ret == C_ERR) {
        goto error;
    }
    return C_OK;
error:
    DisConnect(conn,true);
    return C_ERR;
}
*/

/*
int RdmaRead(Connection *conn, Header *header, MemoryEntry* entry) {//, int64_t now
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;

    char* remote_addr = (char *)ntohu64(header->RdmaAddr);
    uint32_t remote_length = ntohl(header->RdmaLength);
    uint32_t remote_key = ntohl(header->RdmaKey);
    int64_t now = get_time_ns();
    int64_t dead_line = 0;
    int index;
    if(conn->recv_timeout_ns == -1 || conn->recv_timeout_ns == 0) {
        dead_line = -1;
    } else {
        dead_line = now+conn->recv_timeout_ns;
    }
    while(1) {
        if (conn->state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
            //printf("conn(%p) state error or conn closed: state(%d)\n",conn, conn->state);
            return C_ERR;
        }
        now = get_time_ns();
        if(dead_line == -1) {
            //printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            return C_ERR;
        }
        if(now >= dead_line) {
            //printf("conn(%p) rdma read timeout, deadline:%ld, now:%ld\n", conn, dead_line, now);
            return C_ERR;
        }
        index = buddy_alloc(conn->pool->allocation,remote_length / (rdmaPoolConfig->memBlockSize));
        if(index == -1) {
            //printf("conn(%p) rdma read failed, there is no space to read\n", conn);
            continue;
        }
        //buddy_dump(conn->pool->allocation);
        int s = buddy_size(conn->pool->allocation,index);
        assert(s >= (remote_length / (rdmaPoolConfig->memBlockSize)));
        break;
    }
    void* addr = conn->pool->original_mem + index * rdmaPoolConfig->memBlockSize;
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
        //printf("RDMA: rdma read failed: %d", ret);
        return C_ERR;
    }
    return C_OK;
}

int connRdmaRead(Connection *conn, void *block, MemoryEntry *entry) { //, int64_t now//非异步
    struct rdma_cm_id *cm_id = conn->cm_id;
    uint32_t towrite;
    return RdmaRead(conn, (Header*)(block), entry);//, now
}
*/
