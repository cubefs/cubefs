#include <client.h>

/*
int OnClientConnPreConnect(struct rdma_cm_id *id, void* ctx) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaContext* client = (struct RdmaContext*)conn_ev->ctx;

    if(!client->isReConnect) {
        Connection* conn = AllocConnection(id, conn_ev, 2);
        if(conn == NULL) {
            //EpollDelConnEvent(client->listen_id->channel->fd);
            //DelEpollEvent(client->listen_id->channel->fd);
            DelConnectEvent(2, client);
            rdma_destroy_id(id);
            client->listen_id = NULL;
            return C_ERR;
        }
        conn->state = CONN_STATE_CONNECTING;
        conn->csContext = client;
        id->context = conn;
        return C_OK;
    } else {
        if(!UpdateConnection(client->conn)) {
            //EpollDelConnEvent(client->listen_id->channel->fd);
            //DelEpollEvent(client->listen_id->channel->fd);
            DelConnectEvent(2, client);
            rdma_destroy_id(id);
            client->conn->cm_id = NULL;
            client->listen_id = NULL;
            return C_ERR;
        }
        client->conn->state = CONN_STATE_CONNECTING;
        id->context = client->conn;
        return C_OK;
    }
}

int OnClientConnConnected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaContext* client = (struct RdmaContext*)ctx;
    Connection* conn = (Connection*)id->context;
    struct sockaddr *local_addr = rdma_get_local_addr(id);     // 获取本地地址
    struct sockaddr *remote_addr = rdma_get_peer_addr(id);     // 获取远程地址
    // 获取本地 IPv4 地址和端口号
    struct sockaddr_in *local_ipv4 = (struct sockaddr_in *)local_addr;
    inet_ntop(AF_INET, &(local_ipv4->sin_addr), conn->local_addr, INET_ADDRSTRLEN);
    snprintf(conn->local_addr + strlen(conn->local_addr), sizeof(conn->local_addr) - strlen(conn->local_addr),
            ":%d", ntohs(local_ipv4->sin_port));

    // 获取远程 IPv4 地址和端口号
    struct sockaddr_in *remote_ipv4 = (struct sockaddr_in *)remote_addr;
    inet_ntop(AF_INET, &(remote_ipv4->sin_addr), conn->remote_addr, INET_ADDRSTRLEN);
    snprintf(conn->remote_addr + strlen(conn->remote_addr), sizeof(conn->remote_addr) - strlen(conn->remote_addr),
            ":%d", ntohs(remote_ipv4->sin_port));
    if(!client->isReConnect) {
        client->conn = conn;
    }
    notify_event(client->cFd, 0);
    id->context = conn;
    return C_OK;
}

int OnClientConnRejected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaContext* client = (struct RdmaContext*)ctx;
    Connection* conn = (Connection*)id->context;
    //EpollDelConnEvent(client->listen_id->channel->fd);
    //DelEpollEvent(client->listen_id->channel->fd);
    DelConnectEvent(2, client);
    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }
    conn->cm_id = NULL;
    ((struct RdmaContext*)(conn->csContext))->listen_id = NULL;
    notify_event(conn->cFd,1);
    if(conn->freeList) {
        ClearQueue(conn->freeList);
    }
    client->conn = conn;
    return C_OK;
}

int OnClientConnDisconnected(struct rdma_cm_id *id, void* ctx) {

    struct RdmaContext* client = (struct RdmaContext*)ctx;
    Connection* conn = (Connection*)id->context;
    conn->state = CONN_STATE_CLOSED;
    DisConnectCallback(conn->connContext);
    wait_group_wait(&(conn->wg));
    //EpollDelConnEvent(client->listen_id->channel->fd);
    //DelEpollEvent(client->listen_id->channel->fd);
    DelConnectEvent(2, client);
    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        if(ibv_destroy_cq(conn->cq)) {
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }
    conn->cm_id = NULL;
    ((struct RdmaContext*)(conn->csContext))->listen_id = NULL;
    if(conn->freeList) {
        ClearQueue(conn->freeList);
    }
    notify_event(conn->cFd, 0);
    return C_OK;
}

struct Connection* getClientConn(struct RdmaContext *client) {
    wait_event(client->cFd);
    return client->conn;
}
*/

struct connection* rdma_connect_by_addr(const char* ip, const char* port) {//, MemoryPool* pool, ObjectPool* headerPool, ObjectPool* responsePool, struct ibv_pd* pd, struct ibv_mr* mr
    struct addrinfo *addr;
    struct rdma_conn_param cm_params;
    uint64_t nd;
    nd = allocate_nd(CONN_ACTIVE_BIT);
    connection* conn = init_connection(nd, CONN_TYPE_CLIENT);
    if (conn == NULL) {
        log_debug("init_connection return null");
        return NULL;
    }
    /*
    int ret = create_conn_qp(conn, id);
    if (ret != C_OK) {
        log_debug("conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
        conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }
    ret = rdma_setup_ioBuf(conn, CONN_TYPE_CLIENT);
    if (ret != C_OK) {
        log_debug("rdma reg mem failed, err:%d", errno);
        goto err_free;
    }
    */

    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);

    getaddrinfo(ip, port, NULL, &addr);
    int ret = rdma_create_id(g_net_env->event_channel, &conn->cm_id, (void*)(conn->nd), RDMA_PS_TCP);
    if (ret != 0) {
        log_debug("rdma create id failed, err:%d", errno);
        goto err_free;
    }
    log_debug("conn(%lu-%p) create cmid:%p", conn->nd, conn, conn->cm_id);

    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if (ret != 0) {
        log_debug("rdma solve addr failed, err:%d", errno);
        goto err_destroy_id;
    }
    freeaddrinfo(addr);

    wait_event(conn->connect_fd);
    return conn;
err_destroy_id:
    rdma_destroy_id(conn->cm_id);
//err_destroy_iobuf:
//    rdma_destroy_ioBuf(conn);
err_free:
    del_conn_from_worker(conn->nd,conn->worker,conn->worker->nd_map);
    destroy_connection(conn);
    return NULL;
}

/*
int CloseClient(struct RdmaContext* client) {
    if(client->conn != NULL) {
        Connection *conn = client->conn;

        if (conn->comp_channel) {
            if(ibv_destroy_comp_channel(conn->comp_channel) != 0) {
                //printf("Failed to destroy comp channel\n");
                // we continue anyways;
            }
            conn->comp_channel = NULL;
        }

        if (conn->pd) {
            conn->pd = NULL;
        }
        rdmaDestroyIoBuf(conn);
        if(conn->freeList) {
            DestroyQueue(conn->freeList);
        }
        conn->connContext = NULL;
        ((struct RdmaContext*)conn->csContext)->conn = NULL;
        conn->csContext = NULL;
        if(conn->wg.wgInitialized == 1) {
            wait_group_destroy(&conn->wg);
        }
        if(conn->lockInitialized == 1) {
            pthread_spin_destroy(&conn->lock);
        }
        free(conn);
    }
    notify_event(client->cFd,1);
    if(client->ec) {
        rdma_destroy_event_channel(client->ec);    
    }
    free(client->conn_ev);
    free(client);
    return C_OK;
}
*/