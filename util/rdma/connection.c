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

int conn_rdma_post_recv(connection *conn, rdma_ctl_cmd *cmd) {
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;
    cmd_entry *entry;
    int ret;

    entry = (cmd_entry*)malloc(sizeof(cmd_entry));
    if (entry == NULL) {
        log_error("conn(%lu-%p) rdma post recv: malloc entry failed", conn->nd, conn);
        return C_ERR;
    }
    sge.addr = (uint64_t)cmd;
    sge.length = sizeof(rdma_ctl_cmd);
    sge.lkey = conn->ctl_buf_mr->lkey;
    entry->cmd = cmd;
    entry->nd = conn->nd;

    recv_wr.wr_id = (uint64_t)entry;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;
    ret = ibv_post_recv(conn->qp, &recv_wr, &bad_wr);
    if (ret != 0) {
        log_error("conn(%lu-%p) ibv post recv failed: %d", conn->nd, conn, ret);
        //int state = get_conn_state(conn);
        //if (state == CONN_STATE_CONNECTED) {
        //    set_conn_state(conn, CONN_STATE_ERROR); //TODO
        //}
        free(entry);
        return C_ERR;
    }
    return C_OK;
}

int conn_rdma_post_send(connection *conn, rdma_ctl_cmd *cmd) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    cmd_entry *entry;
    int ret;

    entry = (cmd_entry*)malloc(sizeof(cmd_entry));
    if (entry == NULL) {
        log_error("conn(%lu-%p) rdma post recv: malloc entry failed", conn->nd, conn);
        return C_ERR;
    }
    sge.addr = (uint64_t)cmd;
    sge.length = sizeof(rdma_ctl_cmd);
    sge.lkey = conn->ctl_buf_mr->lkey;
    entry->cmd = cmd;
    entry->nd = conn->nd;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = (uint64_t)entry;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        log_error("conn(%lu-%p) ibv post send failed: %d", conn->nd,conn, ret);
        //int state = get_conn_state(conn);
        //if (state == CONN_STATE_CONNECTED) {
        //    set_conn_state(conn, CONN_STATE_ERROR); //TODO
        //}
        free(entry);
        return C_ERR;
    }
    return C_OK;
}

void rdma_destroy_ioBuf(connection *conn) {
    if (conn->ctl_buf_mr) {
        ibv_dereg_mr(conn->ctl_buf_mr);
        conn->ctl_buf_mr = NULL;
    }
    if (conn->ctl_buf) {
        free(conn->ctl_buf);
        conn->ctl_buf = NULL;
    }
    if (conn->rx) {
        if (conn->rx->mr) {
            ibv_dereg_mr(conn->rx->mr);
            conn->rx->mr = NULL;
        }
        if (conn->rx->addr) {
            //int index = (int)((conn->rx->addr - (rdma_pool->memory_pool->original_mem)) / (rdma_env_config->mem_block_size));
            //buddy_free(rdma_pool->memory_pool->allocation, index);
            //buddy_dump(rdmaPool->memoryPool->allocation);
            //log_debug("conn(%lu-%p) free rx: index(%d)", conn->nd, conn, index);
            free(conn->rx->addr);
            conn->rx->addr = NULL;
        }
    }
    if (conn->tx) {
        if (conn->tx->mr) {
            ibv_dereg_mr(conn->tx->mr);
            conn->tx->mr = NULL;
        }
        if (conn->tx->addr) {
            //int index = (int)((conn->tx->addr - (rdma_pool->memory_pool->original_mem)) / (rdma_env_config->mem_block_size));
            //buddy_free(rdma_pool->memory_pool->allocation, index);
            //buddy_dump(rdmaPool->memoryPool->allocation);
            //log_debug("conn(%lu-%p) free tx: index(%d)", conn->nd, conn, index);
            free(conn->tx->addr);
            conn->tx->addr = NULL;
        }
    }
}

int rdma_setup_ioBuf(connection *conn) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t ctl_buf_length = sizeof(rdma_ctl_cmd) * WQ_DEPTH * 2;
    rdma_ctl_cmd *cmd;
    int i;
    conn->ctl_buf = page_aligned_zalloc(ctl_buf_length);
    if (conn->ctl_buf == NULL) {
        log_error("conn(%lu-%p) ctl buf alloc failed", conn->nd, conn);
        goto destroy_iobuf;
    }
    conn->ctl_buf_mr = ibv_reg_mr(conn->worker->pd, conn->ctl_buf, ctl_buf_length, access);
    if (conn->ctl_buf_mr == NULL) {
        log_error("conn(%lu-%p) ctl buf register failed", conn->nd, conn);
        goto destroy_iobuf;
    }
    for (i = 0; i < WQ_DEPTH; i++) {
        cmd = conn->ctl_buf + i;
        if (conn_rdma_post_recv(conn, cmd) == C_ERR) {
            log_error("conn(%lu-%p) ctl buf post recv failed", conn->nd, conn);
            goto destroy_iobuf;
        }
    }
    for (i = WQ_DEPTH; i < WQ_DEPTH * 2; i++) {
        cmd = conn->ctl_buf + i;
        if(EnQueue(conn->free_list, cmd) == NULL) {
            log_error("conn(%lu-%p) freeList has no more memory can be malloced", conn->nd, conn);
            goto destroy_iobuf;
        }
    }

    access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;


    size_t rx_buf_length = (size_t) CONN_DATA_SIZE;
    conn->rx->addr = page_aligned_zalloc(rx_buf_length);
    if (conn->rx->addr == NULL) {
        log_error("conn(%lu-%p) rx buf alloc failed", conn->nd, conn);
        goto destroy_iobuf;
    }
    conn->rx->mr = ibv_reg_mr(conn->worker->pd, conn->rx->addr, rx_buf_length, access);
    if (conn->rx->mr == NULL) {
        log_error("conn(%lu-%p) rx buf register failed", conn->nd, conn);
        goto destroy_iobuf;
    }
    conn->rx->length = (uint32_t) rx_buf_length;
    return C_OK;

    /*
    int quotient = CONN_DATA_SIZE / rdma_env_config->mem_block_size;
    int remainder = CONN_DATA_SIZE % rdma_env_config->mem_block_size;
    if(remainder > 0) {
        quotient++;
    }
    int64_t dead_line = get_time_ns() + 10000 * 1000; //10ms
    while (1) {
        int64_t now = get_time_ns();
        if (now >= dead_line) {
            //time out;
            log_error("conn(%lu-%p) setup rx buffer timeout, deadline:%ld, now:%ld", conn->nd, conn, dead_line, now);
            goto destroy_iobuf;
        }

        int index = buddy_alloc(rdma_pool->memory_pool->allocation, quotient);
        //buddy_dump(rdma_pool->memory_pool->allocation);
        //int s = buddy_size(rdma_pool->memory_pool->allocation, index);//when index == -1,assert is not pass
        if(index == -1) {
            log_error("conn(%lu-%p) setup rx buffer: memory pool is no space to alloc", conn->nd, conn);
            usleep(5);
            continue;
        }
        int s = buddy_size(rdma_pool->memory_pool->allocation, index);
        assert(s * rdma_env_config->mem_block_size >= CONN_DATA_SIZE);
        uint32_t data_buf_length = (uint32_t) s * (uint32_t) rdma_env_config->mem_block_size;
        log_debug("conn(%lu-%p) setup rx buffer: index(%d) s(%d) quotient(%d) data_buf_length(%u)", conn->nd, conn, index, s, quotient, data_buf_length);
        conn->rx->addr = rdma_pool->memory_pool->original_mem + (uint32_t)index * (uint32_t)rdma_env_config->mem_block_size;
        conn->rx->length = data_buf_length;
        conn->rx->mr = rdma_pool->memory_pool->mr;
        return C_OK;
    }
    */
destroy_iobuf:
    rdma_destroy_ioBuf(conn);
    return C_ERR;
}

int rdma_adjust_txBuf(connection *conn, uint32_t length) {
    if (length == conn->remote_rx_length) {
        return C_OK;
    }

    if (conn->remote_rx_length) {
        ibv_dereg_mr(conn->tx->mr);
        conn->tx->mr = NULL;
        //int index = (int)((conn->tx->addr - (rdma_pool->memory_pool->original_mem)) / (rdma_env_config->mem_block_size));
        //buddy_free(rdma_pool->memory_pool->allocation, index);
        //buddy_dump(rdmaPool->memoryPool->allocation);
        //log_debug("conn(%lu-%p) free tx: index(%d)", conn->nd, conn, index);
        free(conn->tx->addr);
        conn->tx->addr = NULL;
        conn->remote_rx_length = 0;
    }


    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    size_t tx_buf_length = length;
    conn->tx->addr = page_aligned_zalloc(tx_buf_length);
    if (conn->tx->addr == NULL) {
        log_error("conn(%lu-%p) tx buf alloc failed", conn->nd, conn);
        return C_ERR;
    }
    conn->tx->mr = ibv_reg_mr(conn->worker->pd, conn->tx->addr, tx_buf_length, access);
    if (conn->tx->mr == NULL) {
        log_error("conn(%lu-%p) tx buf register failed", conn->nd, conn);
        return C_ERR;
    }
    conn->tx->length = (uint32_t) tx_buf_length;
    conn->remote_rx_length = length;
    return C_OK;

    /*
    int quotient = length / rdma_env_config->mem_block_size;
    int remainder = length % rdma_env_config->mem_block_size;
    if(remainder > 0) {
        quotient++;
    }
    int64_t dead_line = get_time_ns() + 10000 * 1000; //10ms
    while (1) {
        int64_t now = get_time_ns();
        if (now >= dead_line) {
            //time out;
            log_error("conn(%lu-%p) adjust tx buffer timeout, deadline:%ld, now:%ld", conn->nd, conn, dead_line, now);
            return C_ERR;
        }

        int index = buddy_alloc(rdma_pool->memory_pool->allocation, quotient);
        //buddy_dump(rdma_pool->memory_pool->allocation);
        //int s = buddy_size(rdma_pool->memory_pool->allocation, index);//when index == -1,assert is not pass
        if(index == -1) {
            log_error("conn(%lu-%p) adjust tx buffer: memory pool is no space to alloc", conn->nd, conn);
            usleep(5);
            continue;
        }
        int s = buddy_size(rdma_pool->memory_pool->allocation, index);
        assert(s *  rdma_env_config->mem_block_size >= length);
        uint32_t data_buf_length = (uint32_t) s * (uint32_t) rdma_env_config->mem_block_size;
        log_debug("conn(%lu-%p) adjust tx buffer: index(%d) s(%d) quotient(%d) data_buf_length(%u)", conn->nd, conn, index, s, quotient, data_buf_length);
        conn->tx->addr = rdma_pool->memory_pool->original_mem + (uint32_t)index * (uint32_t)rdma_env_config->mem_block_size;
        conn->remote_rx_length = length;
        conn->tx->mr = rdma_pool->memory_pool->mr;
        return C_OK;
    }
    */
}

void destroy_connection(connection *conn) {
    if (conn->free_list) {
        DestroyQueue(conn->free_list);
    }
    if (conn->msg_list) {
        DestroyQueue(conn->msg_list);
    }
    if (conn->tx_buffer_list) {
        DestroyQueue(conn->tx_buffer_list);
    }
    if (conn->rx_buffer_list) {
        DestroyQueue(conn->rx_buffer_list);
    }
    conn->conn_context = NULL;
    conn->context = NULL;
    conn->remote_rx_addr = NULL;
    if (conn->connect_fd >0) {
        notify_event(conn->connect_fd,1);
        conn->connect_fd = -1;
    }
    if (conn->msg_fd > 0) {
        notify_event(conn->msg_fd,1);
        conn->msg_fd = -1;
    }
    if (conn->close_fd > 0) {
       notify_event(conn->close_fd,1);
       conn->close_fd = -1;
    }
    pthread_spin_destroy(&conn->spin_lock);
    pthread_spin_destroy(&conn->rx_lock);
    pthread_spin_destroy(&conn->tx_lock);
    pthread_spin_destroy(&conn->free_list_lock);
    pthread_spin_destroy(&conn->msg_list_lock);
    if (conn->tx) {
        free(conn->tx);
    }
    if (conn->rx) {
        free(conn->rx);
    }
    memset(conn, 0, sizeof(connection));
    free(conn);
}

connection* init_connection(uint64_t nd, int conn_type) {
    int ret = 0;
    connection *conn = (connection*)malloc(sizeof(connection));
    if (conn == NULL) {
        log_error("create conn mem obj failed");
        return NULL;
    }
    memset(conn, 0, sizeof(connection));
    log_debug("malloc connect:%p", conn);

    conn->nd = nd;
    log_debug("conn nd:%lu",nd);
    conn->worker = get_worker_by_nd(conn->nd);
    log_debug("conn worker:%p",conn->worker);
    conn->free_list = InitQueue();
    if (conn->free_list == NULL) {
        log_error("conn(%lu-%p) init free list failed", conn->nd, conn);
        goto err_free;
    }
    conn->msg_list = InitQueue();
    if (conn->msg_list == NULL) {
        log_error("conn(%lu-%p) init msg list failed", conn->nd, conn);
        goto err_destroy_freelist;
    }
    conn->tx_buffer_list = InitQueue();
    if (conn->tx_buffer_list == NULL) {
        log_error("conn(%lu-%p) init tx buffer list failed", conn->nd, conn);
        goto err_destroy_msglist;
    }
    conn->rx_buffer_list = InitQueue();
    if (conn->rx_buffer_list == NULL) {
        log_error("conn(%lu-%p) init rx buffer list failed", conn->nd, conn);
        goto err_destroy_txbufferlist;
    }
    conn->conn_type = conn_type;
    conn->conn_context = NULL;
    conn->context = NULL;
    conn->send_timeout_ns = 0;
    conn->recv_timeout_ns = 0;
    conn->remote_rx_addr = NULL;
    conn->remote_rx_key = 0;
    conn->remote_rx_length = 0;
    conn->remote_rx_offset = 0;
    conn->tx_full_offset = 0;
    conn->rx_full_offset = 0;
    conn->tx_flag = 0;
    conn->connect_fd = -1;
    conn->msg_fd = -1;
    conn->close_fd = -1;
    conn->ref = 0;

    conn->connect_fd = open_event_fd();
    if (conn->connect_fd < 0) {
        log_error("conn(%lu-%p) open connect fd failed", conn->nd, conn);
        goto err_destroy_rxbufferlist;
    }
    conn->msg_fd = open_event_fd();
    if (conn->msg_fd < 0) {
        log_error("conn(%lu-%p) open msg fd failed", conn->nd, conn);
        goto err_destroy_connectfd;
    }
    conn->close_fd = open_event_fd();
    if (conn->close_fd < 0) {
        log_error("conn(%lu-%p) open close fd failed", conn->nd, conn);
        goto err_destroy_msgfd;
    }
    ret = pthread_spin_init(&(conn->spin_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_error("conn(%lu-%p) init spin lock failed, err:%d", conn->nd, conn, ret);
        goto err_destroy_closefd;
    }
    ret = pthread_spin_init(&(conn->tx_lock), PTHREAD_PROCESS_SHARED);
    if(ret != 0) {
        log_error("conn(%lu-%p) init tx lock failed, err:%d", conn->nd, conn, ret);
        goto err_destroy_spin_lock;
    }
    ret = pthread_spin_init(&(conn->rx_lock), PTHREAD_PROCESS_SHARED);
    if(ret != 0) {
        log_error("conn(%lu-%p) init rx lock failed, err:%d", conn->nd, conn, ret);
        goto err_destroy_tx_lock;
    }
    ret = pthread_spin_init(&(conn->free_list_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_error("init conn(%p) free list lock failed, err:%d", conn, ret);
        goto err_destroy_rx_lock;
    }
    ret = pthread_spin_init(&(conn->msg_list_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_error("init conn(%p) msg list lock failed, err:%d", conn, ret);
        goto err_destroy_free_list_lock;
    }

    conn->tx = (data_buf*)malloc(sizeof(data_buf));
    if (conn->tx == NULL) {
        log_error("conn(%lu-%p) malloc tx failed", conn->nd, conn);
        goto err_destroy_msg_list_lock;
    }
    memset(conn->tx,0,sizeof(data_buf));
    conn->rx = (data_buf*)malloc(sizeof(data_buf));
    if (conn->rx == NULL) {
        log_error("conn(%lu-%p) malloc rx failed", conn->nd, conn);
        goto err_free_tx;
    }
    memset(conn->rx,0,sizeof(data_buf));

    set_conn_state(conn, CONN_STATE_CONNECTING);
    return conn;
err_free_tx:
    free(conn->tx);
err_destroy_msg_list_lock:
    pthread_spin_destroy(&(conn->msg_list_lock));
err_destroy_free_list_lock:
    pthread_spin_destroy(&(conn->free_list_lock));
err_destroy_rx_lock:
    pthread_spin_destroy(&(conn->rx_lock));
err_destroy_tx_lock:
    pthread_spin_destroy(&(conn->tx_lock));
err_destroy_spin_lock:
    pthread_spin_destroy(&(conn->spin_lock));
err_destroy_closefd:
    notify_event(conn->close_fd,1);
    conn->close_fd = -1;
err_destroy_msgfd:
    notify_event(conn->msg_fd,1);
    conn->msg_fd = -1;
err_destroy_connectfd:
    notify_event(conn->connect_fd,1);
    conn->connect_fd = -1;
err_destroy_rxbufferlist:
    DestroyQueue(conn->rx_buffer_list);
err_destroy_txbufferlist:
    DestroyQueue(conn->tx_buffer_list);
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
    int ret = rdma_create_qp(id, conn->worker->pd, &qp_attr);
    if (ret != 0) {
        log_error("conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
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
    log_debug("add conn(%lu-%p) to server(%p) conn_map(%p)", conn->nd, conn, server, server->conn_map);
    return ret >= 0;
}

int del_conn_from_server(connection *conn, struct rdma_listener *server) {
    int ret = 0;
    pthread_spin_lock(&server->conn_lock);
    ret = hashmap_del(server->conn_map, conn->nd);
    pthread_spin_unlock(&server->conn_lock);
    log_debug("del conn(%lu-%p) from server(%p) conn_map(%p)", conn->nd, conn, server, server->conn_map);
    return ret >= 0;
}

void conn_disconnect(connection *conn) {
    worker  *worker = NULL;
    struct rdma_listener *server = NULL;
    worker = get_worker_by_nd((uintptr_t) conn->cm_id->context);
    int state = get_conn_state(conn);
    if (state != CONN_STATE_DISCONNECTING && state != CONN_STATE_DISCONNECTED && conn->cm_id != NULL) {
        while(conn->ref > 0) {
            usleep(10);
        }
        set_conn_state(conn, CONN_STATE_DISCONNECTING);
        rdma_disconnect(conn->cm_id);
        log_debug("conn(%lu-%p) exec rdma_disconnect", conn->nd, conn);
    }

    wait_event(conn->close_fd);

    log_debug("conn(%lu-%p) disconnect, resource release", conn->nd, conn);

    //release resource
    if (conn->conn_type == CONN_TYPE_SERVER) {//server
        server = (struct rdma_listener*)conn->context;
        del_conn_from_server(conn, server);
    } else {//client

    }
    del_conn_from_worker(conn->nd, worker, worker->nd_map);
    //del_conn_from_worker(conn->nd, conn->worker, conn->worker->closing_nd_map);

    destroy_conn_qp(conn);
    rdma_destroy_id(conn->cm_id);
    rdma_destroy_ioBuf(conn);
    destroy_connection(conn);

    return;
}

/*
int rdma_post_send_cmd(connection *conn, rdma_ctl_cmd *cmd) {
    int state = get_conn_state(conn);
    if(state != CONN_STATE_CONNECTED) {
        log_error("post send cmd failed: conn(%lu-%p) state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }
    int ret = conn_rdma_post_send(conn, cmd);
    return ret;
}
*/

/*
int rdma_post_recv_cmd(connection *conn, rdma_ctl_cmd *cmd) {
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) {
        log_error("post recv cmd failed: conn(%lu-%p) state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }
    int ret = conn_rdma_post_recv(conn, cmd);
    return ret;
}
*/

int rdma_exchange_rx(connection *conn) {
    rdma_ctl_cmd *cmd = get_cmd_buffer(conn);
    if (cmd == NULL) {
        return C_ERR;
    }

    cmd->memory.opcode = htons(EXCHANGE_MEMORY);
    cmd->memory.addr = htonu64((uint64_t)conn->rx->addr);
    cmd->memory.length = htonl(conn->rx->length);
    cmd->memory.key = htonl(conn->rx->mr->rkey);

    conn->rx->offset = 0;
    conn->rx->pos = 0;
    conn->rx_full_offset = 0;
    return conn_rdma_post_send(conn, cmd);
}

int rdma_notify_buf_full(connection *conn) {
    rdma_ctl_cmd *cmd = get_cmd_buffer(conn);
    if (cmd == NULL) {
        return C_ERR;
    }

    cmd->full_msg.opcode = htons(NOTIFY_FULLBUF);
    cmd->full_msg.tx_full_offset = htonl(conn->tx_full_offset);
    return conn_rdma_post_send(conn, cmd);
}

int conn_app_write_external_buffer(connection *conn, void *buffer, data_entry *entry, uint32_t lkey,uint32_t size) {
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_error("conn(%lu-%p) app write failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    char *addr = (char*)buffer;
    char *remote_addr = entry->remote_addr;
    //uint32_t mem_len = entry->mem_len;
    int ret;

    int index = (int)(((char*)buffer - (rdma_pool->memory_pool->original_mem)) / (rdma_env_config->mem_block_size));
    log_debug("conn(%lu-%p) write external data buffer index:%d", conn->nd, conn, index);

    /*
    while(1) {
        pthread_spin_lock(&(conn->tx_lock));
        int state = get_conn_state(conn);
        if (state != CONN_STATE_CONNECTED) {
            log_error("conn(%lu-%p) is not in connected state: state(%d)", conn->nd, conn, state);
            pthread_spin_unlock(&(conn->tx_lock));
            return C_ERR;
        }
        assert(conn->tx->offset <= conn->tx->length);
        if (conn->tx->pos < conn->tx->offset) {
            if (conn->tx->length - conn->tx->offset >= size) {
                conn->tx->offset += size;
            } else {
                conn->tx->offset = 0;
                log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                pthread_spin_unlock(&(conn->tx_lock));
                continue;
            }
            conn->tx_flag = 1;
        } else if(conn->tx->pos > conn->tx->offset) {
            if (conn->tx->pos - conn->tx->offset >= size) {
                conn->tx->offset += size;
            } else {
                log_debug("conn(%lu-%p) get data buffer failed, tx pos(%d) offset(%d) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                pthread_spin_unlock(&(conn->tx_lock));
                continue;
            }
            conn->tx_flag = -1;
        } else {
            if (conn->tx_flag == 0) {
                if (conn->tx->length - conn->tx->offset >= size) {
                    conn->tx->offset += size;
                } else {
                    conn->tx->offset = 0;
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    continue;
                }
                conn->tx_flag = 1;
            } else if (conn->tx_flag == 1) {
                if (conn->tx->pos == 0) {
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%d) offset(%d) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    continue;
                } else {
                    if (conn->tx->length - conn->tx->offset >= size) {
                        conn->tx->offset += size;
                    } else {
                        conn->tx->offset = 0;
                        log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                        pthread_spin_unlock(&(conn->tx_lock));
                        continue;
                    }
                    conn->tx_flag = 1;
                }
            } else {
                if (conn->tx->pos == 0) {
                    if (conn->tx->length - conn->tx->offset >= size) {
                        conn->tx->offset += size;
                    } else {
                        conn->tx->offset = 0;
                        log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                        pthread_spin_unlock(&(conn->tx_lock));
                        continue;
                    }
                    conn->tx_flag = 1;
                } else {
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    continue;
                }
            }
        }
        //conn->tx->offset += size;
        remote_addr =  conn->remote_rx_addr + conn->tx->offset - size;
        data_entry *entry = (data_entry*)malloc(sizeof(data_entry));
        if (entry == NULL) {
            log_error("conn(%lu-%p) malloc data entry failed", conn->nd, conn);
            pthread_spin_unlock(&(conn->tx_lock));
            return C_ERR;
        }
        entry->addr = conn->tx->addr + conn->tx->offset - size;
        entry->remote_addr = remote_addr;
        entry->mem_len = size;

        log_debug("conn(%lu-%p) tx start(%u) end(%u) len(%u)", conn->nd, conn, conn->tx->offset - size, conn->tx->offset, size);

        if(EnQueue(conn->buffer_list, entry) == NULL) {
            log_error("conn(%lu-%p) enQueue entry failed", conn->nd, conn);
            free(entry);
            pthread_spin_unlock(&(conn->tx_lock));
            return C_ERR;
        }
        pthread_spin_unlock(&(conn->tx_lock));
        break;
    }
    */

    sge.addr = (uint64_t)addr;
    //sge.lkey = conn->tx->mr->lkey;
    //sge.lkey = rdma_pool->memory_pool->mr->lkey;
    sge.lkey = lkey;
    sge.length = size;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(size);
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = conn->remote_rx_key;
    send_wr.wr_id = conn->nd;
    send_wr.next = NULL;
    ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        log_error("conn(%lu-%p) ibv post send: remote write failed: %d", conn->nd,conn, ret);
        set_conn_state(conn, CONN_STATE_ERROR);//TODO
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        return C_ERR;
    }
    return C_OK;
}

int conn_app_write(connection *conn, data_entry *entry, uint32_t size) {
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_error("conn(%lu-%p) app write failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    char *addr = entry->addr;
    char *remote_addr = entry->remote_addr;
    //uint32_t mem_len = entry->mem_len;
    int ret;

    sge.addr = (uint64_t)addr;
    sge.lkey = conn->tx->mr->lkey;
    sge.length = size;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(size);
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = conn->remote_rx_key;
    send_wr.wr_id = conn->nd;
    send_wr.next = NULL;
    ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
    if (ret != 0) {
        log_error("conn(%lu-%p) ibv post send: remote write failed: %d", conn->nd,conn, ret);
        set_conn_state(conn, CONN_STATE_ERROR);//TODO
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        return C_ERR;
    }

    return C_OK;
}

data_entry* get_pool_data_buffer(uint32_t size) {
    //*ret_size = 0;
    int quotient = size / rdma_env_config->mem_block_size;
    int remainder = size % rdma_env_config->mem_block_size;
    if(remainder > 0) {
        quotient++;
    }
    //int64_t dead_line = get_time_ns() + 10000 * 1000; //10ms
    while(1) {
        //int64_t now = get_time_ns();
        //if (now >= dead_line) {
        //    //time out;
        //    log_error("get pool data buffer timeout, deadline:%ld, now:%ld", dead_line, now);
        //    return NULL;
        //}
        int index = buddy_alloc(rdma_pool->memory_pool->allocation, quotient);
        if(index == -1) {
            log_debug("get pool data buffer failed, no more data buffer can get");
            usleep(10);
            continue;
        }
        //log_debug("get pool data buffer index:%d", index);
        //buddy_dump(rdmaPool->memoryPool->allocation);
        int s = buddy_size(rdma_pool->memory_pool->allocation,index);
        assert(s * rdma_env_config->mem_block_size >= size);
        uint32_t data_len = (uint32_t)s * (uint32_t)rdma_env_config->mem_block_size;
        char* data_buffer = rdma_pool->memory_pool->original_mem + (uint64_t)index * (uint64_t)rdma_env_config->mem_block_size;
        log_debug("get pool data buffer: index(%d) s(%d) quotient(%d) data_buf_length(%u)", index, s, quotient, data_len);

        data_entry *entry = (data_entry*)malloc(sizeof(data_entry));
        if (entry == NULL) {
            log_error("malloc data entry failed");
            return NULL;
        }
        entry->addr = data_buffer;
        entry->lkey = rdma_pool->memory_pool->mr->lkey;
        entry->mem_len = data_len;

        return entry;
    }
}

data_entry* get_conn_tx_data_buffer(connection *conn, uint32_t size) {
    int64_t dead_line = get_time_ns() + 2000000 * 1000; //2s
    while(1) {
        pthread_spin_lock(&(conn->tx_lock));
        int state = get_conn_state(conn);
        if (state != CONN_STATE_CONNECTED) {
            log_error("conn(%lu-%p) is not in connected state: state(%d)", conn->nd, conn, state);
            pthread_spin_unlock(&(conn->tx_lock));
            return NULL;
        }

        int64_t now = get_time_ns();
        if (now >= dead_line) {
            //time out;
            log_error("get conn(%lu-%p) tx data buffer timeout, deadline:%ld, now:%ld", conn->nd, conn, dead_line, now);
            pthread_spin_unlock(&(conn->tx_lock));
            return NULL;
        }

        //pthread_spin_lock(&conn->spin_lock);
        assert(conn->tx->offset <= conn->tx->length);
        if (conn->tx->pos < conn->tx->offset) {
            if (conn->tx->length - conn->tx->offset >= size) {
                conn->tx->offset += size;
            } else {
                conn->tx->offset = 0;
                log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                pthread_spin_unlock(&(conn->tx_lock));
                usleep(10);
                continue;
            }
            conn->tx_flag = 1;
        } else if(conn->tx->pos > conn->tx->offset) {
            if (conn->tx->pos - conn->tx->offset >= size) {
                conn->tx->offset += size;
            } else {
                log_debug("conn(%lu-%p) get data buffer failed, tx pos(%d) offset(%d) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                pthread_spin_unlock(&(conn->tx_lock));
                usleep(10);
                continue;
            }
            conn->tx_flag = -1;
        } else {
            if (conn->tx_flag == 0) {
                if (conn->tx->length - conn->tx->offset >= size) {
                    conn->tx->offset += size;
                } else {
                    conn->tx->offset = 0;
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    usleep(10);
                    continue;
                }
                conn->tx_flag = 1;
            } else if (conn->tx_flag == 1) {
                if (conn->tx->pos == 0) {
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%d) offset(%d) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    usleep(10);
                    continue;
                } else {
                    if (conn->tx->length - conn->tx->offset >= size) {
                        conn->tx->offset += size;
                    } else {
                        conn->tx->offset = 0;
                        log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                        pthread_spin_unlock(&(conn->tx_lock));
                        usleep(10);
                        continue;
                    }
                    conn->tx_flag = 1;
                }
            } else {
                if (conn->tx->pos == 0) {
                    if (conn->tx->length - conn->tx->offset >= size) {
                        conn->tx->offset += size;
                    } else {
                        conn->tx->offset = 0;
                        log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get, reset offset = 0", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                        pthread_spin_unlock(&(conn->tx_lock));
                        usleep(10);
                        continue;
                    }
                    conn->tx_flag = 1;
                } else {
                    log_debug("conn(%lu-%p) get data buffer failed, tx pos(%u) offset(%u) no more data buffer can get", conn->nd, conn, conn->tx->pos, conn->tx->offset);
                    pthread_spin_unlock(&(conn->tx_lock));
                    usleep(10);
                    continue;
                }
            }
        }
        /*
        if (conn->tx->length - conn->tx->offset < size) {
            if (conn->tx_full_offset == 0) {
                conn->tx_full_offset = conn->tx->offset;
                int ret = rdma_notify_buf_full(conn); //todo error handler
                if (ret == C_ERR) {
                    log_error("conn(%lu-%p) tx full, notify remote side failed");
                    set_conn_state(conn, CONN_STATE_ERROR);
                    rdma_disconnect(conn->cm_id);
                    //conn_disconnect(conn);
                    return NULL;
                }
            }
            //pthread_spin_unlock(&conn->spin_lock);
            log_error("conn(%lu-%p) get data buffer failed, no more data buffer can get", conn->nd, conn);
            //usleep(5);
            continue;
        }
        */
        data_entry *entry = (data_entry*)malloc(sizeof(data_entry));
        if (entry == NULL) {
            log_error("conn(%lu-%p) malloc data entry failed", conn->nd, conn);
            pthread_spin_unlock(&(conn->tx_lock));
            return NULL;
        }
        entry->addr = conn->tx->addr + conn->tx->offset - size;
        entry->remote_addr = conn->remote_rx_addr + conn->tx->offset - size;
        entry->mem_len = size;
        //conn->tx->offset += size;
        log_debug("conn(%lu-%p) tx start(%u) end(%u) len(%u) addr(%p)", conn->nd, conn, conn->tx->offset - size, conn->tx->offset, size, entry->addr);
        //pthread_spin_unlock(&conn->spin_lock);

        if(EnQueue(conn->tx_buffer_list, entry) == NULL) {
            log_error("conn(%lu-%p) enQueue entry failed", conn->nd, conn);
            free(entry);
            pthread_spin_unlock(&(conn->tx_lock));
            return NULL;
        }

        pthread_spin_unlock(&(conn->tx_lock));
        return entry;
    }
}

rdma_ctl_cmd* get_cmd_buffer(connection *conn) {
    rdma_ctl_cmd *cmd = NULL;
    int64_t dead_line = get_time_ns() + 10000 * 1000; //10ms
    while(1) {
        int state = get_conn_state(conn);
        if (state != CONN_STATE_CONNECTED && state != CONN_STATE_CONNECTING) {
            log_error("conn(%lu-%p) is not in connected state: state(%d)", conn->nd, conn, state);
            return NULL;
        }
        int64_t now = get_time_ns();
        if (now >= dead_line) {
            //time out;
            log_error("get cmd buffer timeout, deadline:%ld, now:%ld", dead_line, now);
            return NULL;
        }
        pthread_spin_lock(&(conn->free_list_lock));
        DeQueue(conn->free_list, (Item*)&cmd);
        pthread_spin_unlock(&(conn->free_list_lock));
        if (cmd == NULL) {//(Item *)
            log_error("conn(%lu-%p) get cmd buffer failed, no more cmd buffer can get", conn->nd, conn);
            usleep(10);
            continue;
        }
        return cmd;
    }
}

data_entry* get_recv_msg_buffer(connection *conn) {
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_error("conn(%lu-%p) get recv msg failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return NULL;
    }
    log_debug("wait event: conn(%lu-%p) msg_fd(%d) start", conn->nd, conn, conn->msg_fd);
    int val = wait_event(conn->msg_fd);
    log_debug("wait event: conn(%lu-%p) msg_fd(%d) read val(%d)", conn->nd, conn, conn->msg_fd, val);
    state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) { //在使用之前需要判断连接的状态
        log_error("conn(%lu-%p) get recv msg failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return NULL;
    }
    log_debug("wait event: conn(%lu-%p) msg_fd(%d)", conn->nd, conn, conn->msg_fd);
    data_entry *msg = NULL;
    pthread_spin_lock(&(conn->msg_list_lock));
    DeQueue(conn->msg_list, (Item*)&msg);
    pthread_spin_unlock(&(conn->msg_list_lock));
    if (msg == NULL) {//(Item *)
        log_error("conn(%lu-%p) get recv msg buffer failed: dequeue(%p) entry is null", conn->nd, conn, conn->msg_list);
        return NULL;
        //TODO
    }

    log_debug("conn(%lu-%p) get recv msg buffer success: dequeue(%p) size(%d) entry is %p", conn->nd, conn, conn->msg_list, GetSize(conn->msg_list), msg);
    return msg;
}

void set_conn_context(connection* conn, void* conn_context) {
    conn->conn_context = conn_context;
    return;
}

void set_send_timeout_us(connection* conn, int64_t timeout_us) {
    if(timeout_us > 0) {
        conn->send_timeout_ns = timeout_us * 1000;
    } else {
        conn->send_timeout_ns = -1;
    }
    log_debug("conn(%lu-%p) set send timeout us:%ld", conn->nd, conn, conn->send_timeout_ns);
    return;
} //todo lock

void set_recv_timeout_us(connection* conn, int64_t timeout_us) {
    if(timeout_us > 0) {
        conn->recv_timeout_ns = timeout_us * 1000;
    } else {
        conn->recv_timeout_ns = -1;
    }
    log_debug("conn(%lu-%p) set recv timeout us:%ld", conn->nd, conn, conn->recv_timeout_ns);
    return;
} //todo lock

int release_cmd_buffer(connection *conn, rdma_ctl_cmd *cmd) {
    int state = get_conn_state(conn);
    if(state != CONN_STATE_CONNECTED && state != CONN_STATE_CONNECTING) {
        log_error("conn(%lu-%p) release cmd buffer failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }
    pthread_spin_lock(&(conn->free_list_lock));
    if(EnQueue(conn->free_list, cmd) == NULL) {
        pthread_spin_unlock(&(conn->free_list_lock));
        log_error("conn(%lu-%p) release cmd buffer failed: no more memory can be malloced", conn->nd, conn);
        return C_ERR;
    };
    pthread_spin_unlock(&(conn->free_list_lock));
    return C_OK;
}

int release_pool_data_buffer(data_entry* entry) {
    if (entry != NULL) {
        void* buff = entry->addr;
        int index = (int)(((char*)buff - (rdma_pool->memory_pool->original_mem)) / (rdma_env_config->mem_block_size));
        log_debug("release pool data buffer index:%d", index);
        buddy_free(rdma_pool->memory_pool->allocation, index);
        //buddy_dump(rdmaPool->memoryPool->allocation);
        free(entry);
    }
    return C_OK;
}

/*
int release_conn_external_data_buffer(connection *conn, uint32_t size) {
    int state = get_conn_state(conn);
    if(state != CONN_STATE_CONNECTED) {
        log_error("conn(%lu-%p) release rx data buffer failed: conn state is not connected: state(%d)", conn->nd, conn, state);
        return C_ERR;
    }

    pthread_spin_lock(&(conn->tx_lock));
    if (conn->tx->pos + size > conn->tx->length) {
        conn->tx->pos = 0;
    }
    conn->tx->pos += size;
    if (conn->tx->pos < conn->tx->offset) {
        conn->tx_flag = 1;
    } else if (conn->tx->pos > conn->tx->offset) {
        conn->tx_flag = -1;
    } else {
        conn->tx_flag = 0;
    }
    log_debug("conn(%lu-%p) tx pos(%u) offset(%u)", conn->nd, conn, conn->tx->pos, conn->tx->offset);
    pthread_spin_unlock(&(conn->tx_lock));
    return C_OK;
}
*/

int release_conn_rx_data_buffer(connection *conn, data_entry *data) {
    data_entry *front_data;
    while(1) {
        int state = get_conn_state(conn);
        if(state != CONN_STATE_CONNECTED) {
            log_error("conn(%lu-%p) release rx data buffer failed: conn state is not connected: state(%d)", conn->nd, conn, state);
            free(data);
            return C_ERR;
        }
        pthread_spin_lock(&(conn->rx_lock));
        GetFront(conn->rx_buffer_list, (Item*)&front_data);
        if (front_data == NULL) {
            log_error("conn(%lu-%p) release rx data buffer failed: conn buffer list front item is NULL", conn->nd, conn);
            free(data);
            pthread_spin_unlock(&(conn->rx_lock));
            return C_ERR;
        }
        if (data->addr != front_data->addr) {
            log_error("conn(%lu-%p) release rx data buffer failed: data->addr(%p) != front_data->addr(%p)", conn->nd, conn, data->addr, front_data->addr);
            pthread_spin_unlock(&(conn->rx_lock));
            usleep(10);//todo
            continue;
        } else {
            break;
        }
    }

    //pthread_spin_lock(&conn->spin_lock);
    //assert(conn->rx->addr + conn->rx->pos == data->addr);
    if (conn->rx->pos + data->mem_len > conn->rx->length) {
        assert(data->addr == conn->rx->addr);
        conn->rx->pos = 0;
    } else {
        assert(conn->rx->addr + conn->rx->pos == data->addr);
    }
    conn->rx->pos += data->mem_len;
    log_debug("conn(%lu-%p) rx pos(%u) offset(%u)", conn->nd, conn, conn->rx->pos, conn->rx->offset);

    /*
    assert(conn->rx->addr + conn->rx->pos == data->addr);
    conn->rx->pos += data->mem_len;
    log_debug("conn(%lu-%p) rx pos:%d", conn->nd, conn, conn->rx->pos);
    if (conn->rx->pos == conn->rx->offset && conn->rx_full_offset == conn->rx->pos) {
        int ret = rdma_exchange_rx(conn); //TODO error handler
        if (ret == C_ERR) {
            log_error("conn(%lu-%p) release rx buffer failed: exchange rx return error");
            free(data);
            set_conn_state(conn, CONN_STATE_ERROR);
            rdma_disconnect(conn->cm_id);
            //conn_disconnect(conn);
            return C_ERR;
        }
    }
    */
    //pthread_spin_unlock(&conn->spin_lock);
    DeQueue(conn->rx_buffer_list, (Item*)&front_data);
    pthread_spin_unlock(&(conn->rx_lock));
    free(data);
    return C_OK;
}

int release_conn_tx_data_buffer(connection *conn, data_entry *data) {
    data_entry *front_data;
    while (1) {
        int state = get_conn_state(conn);
        if(state != CONN_STATE_CONNECTED) {
            log_error("conn(%lu-%p) release rx data buffer failed: conn state is not connected: state(%d)", conn->nd, conn, state);
            free(data);
            return C_ERR;
        }
        pthread_spin_lock(&(conn->tx_lock));
        GetFront(conn->tx_buffer_list, (Item*)&front_data);
        if (front_data == NULL) {
            log_error("conn(%lu-%p) release tx data buffer failed: conn buffer list front item is NULL", conn->nd, conn);
            free(data);
            pthread_spin_unlock(&(conn->tx_lock));
            return C_ERR;
        }
        if (data->addr != front_data->addr) {
            log_error("conn(%lu-%p) release tx data buffer failed: data->addr(%p) != front_data->addr(%p)", conn->nd, conn, data->addr, front_data->addr);
            pthread_spin_unlock(&(conn->tx_lock));
            usleep(10);//todo
            continue;
        } else {
            break;
        }
    }


    if (conn->tx->pos + data->mem_len > conn->tx->length) {
        assert(data->addr == conn->tx->addr);
        conn->tx->pos = 0;
    } else {
        log_debug("conn(%lu-%p) pos addr(%p) data addr(%p)", conn->nd, conn, conn->tx->addr+conn->tx->pos, data->addr);
        assert(conn->tx->addr + conn->tx->pos == data->addr);
    }
    conn->tx->pos += data->mem_len;
    if (conn->tx->pos < conn->tx->offset) {
        conn->tx_flag = 1;
    } else if (conn->tx->pos > conn->tx->offset) {
        conn->tx_flag = -1;
    } else {
        conn->tx_flag = 0;
    }
    log_debug("conn(%lu-%p) tx pos(%u) offset(%u) len(%u) addr(%p)", conn->nd, conn, conn->tx->pos, conn->tx->offset, data->mem_len, data->addr);
    DeQueue(conn->tx_buffer_list, (Item*)&front_data);
    pthread_spin_unlock(&(conn->tx_lock));
    free(data);
    return C_OK;
}