#include <client.h>

int OnClientConnPreConnect(struct rdma_cm_id *id, void* ctx) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaContext* client = (struct RdmaContext*)conn_ev->ctx;
    //printf("client=%p, %s \n", client, __FUNCTION__);
    //sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    //PrintCallback(buffer);

    if(!client->isReConnect) {

        Connection* conn = AllocConnection(id, conn_ev, 2);

        if(conn == NULL) {
            EpollDelConnEvent(client->listen_id->channel->fd);
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
            //printf("update connection failed\n");
            //sprintf(buffer,"update connection failed\n");
            //PrintCallback(buffer);

            EpollDelConnEvent(client->listen_id->channel->fd);
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
    //printf("client=%p, %s \n", client, __FUNCTION__);
    //sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    //PrintCallback(buffer);

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

    //printf("本地地址：%s\n", conn->local_addr);
    //printf("远程地址：%s\n", conn->remote_addr);
    
    if(!client->isReConnect) {
        client->conn = conn;
    }
    notify_event(client->cFd, 0);

    id->context = conn;
    return C_OK;
}

int OnClientConnRejected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaContext* client = (struct RdmaContext*)ctx;
    //printf("client=%p, %s \n", client, __FUNCTION__);
    //sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    //PrintCallback(buffer);

    Connection* conn = (Connection*)id->context;

    EpollDelConnEvent(client->listen_id->channel->fd);

    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            //PrintCallback(buffer);
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

    if(conn->cFd > 0) {
        notify_event(conn->cFd,0);
        close(conn->cFd);
        conn->cFd = -1;
    }


    //TODO before clear,need to determind all header or response is back to freeList
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
    }

    client->conn = conn;
    return C_OK;
}

int OnClientConnDisconnected(struct rdma_cm_id *id, void* ctx) {//TODO

    struct RdmaContext* client = (struct RdmaContext*)ctx;
    //printf("client=%p, %s \n", client, __FUNCTION__);
    //sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    //PrintCallback(buffer);

    Connection* conn = (Connection*)id->context;
    //pthread_spin_lock(&conn->lock);
    conn->state = CONN_STATE_CLOSED;
    //pthread_spin_unlock(&conn->lock);
    
    DisConnectCallback(conn->connContext);

    wait_group_wait(&(conn->wg));

    EpollDelConnEvent(client->listen_id->channel->fd);

    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        if(ibv_destroy_cq(conn->cq)) {
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            //PrintCallback(buffer);
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

    //TODO before clear,need to determind all header or response is back to freeList
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
        //sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        //PrintCallback(buffer);
    }

    notify_event(conn->cFd, 0);
    return C_OK;
}


struct Connection* getClientConn(struct RdmaContext *client) {

    if(wait_event(client->cFd) <= 0) {//TODO error handler
		return NULL;
	}

    return client->conn;
}

struct RdmaContext* Connect(const char* ip, const char* port, char* remoteAddr) {//, MemoryPool* pool, ObjectPool* headerPool, ObjectPool* responsePool, struct ibv_pd* pd, struct ibv_mr* mr

    struct addrinfo *addr;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_conn_param cm_params;

    //printf("ip=%s, port=%s\n", ip, port);
    //sprintf(buffer,"ip=%s, port=%s\n", ip, port);
    //PrintCallback(buffer);
    TEST_NZ_(getaddrinfo(ip, port, NULL, &addr));


    TEST_Z_(ec = rdma_create_event_channel());
    TEST_NZ_(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
    TEST_NZ_(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

    freeaddrinfo(addr);

    struct RdmaContext* client = (struct RdmaContext*)malloc(sizeof(struct RdmaContext));
    client->listen_id = conn;
    //printf("client listen id %d\n",client->listen_id);
    //sprintf(buffer,"client listen id %d\n",client->listen_id);
    //PrintCallback(buffer);
    client->ec = ec;
    client->ip = ip;
    client->port = port;
    client->state = 0;
    client->cFd = open_event_fd();
    client->conn = NULL;
    client->isReConnect = false;

    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = conn;
    conn_ev->ctx = client;

    conn_ev->preconnect_callback = OnClientConnPreConnect;
    conn_ev->connected_callback = OnClientConnConnected;
    conn_ev->disconnected_callback = OnClientConnDisconnected;
    conn_ev->rejected_callback = OnClientConnRejected;

    client->conn_ev = conn_ev;

    //EpollAddConnectEvent(client->listen_id->channel->fd, conn_ev);
    epoll_rdma_event_add(client->listen_id->channel->fd, conn_ev, connection_event_cb);

    //printf("start client %p \n", client);
    //sprintf(buffer,"start client %p \n", client);
    //PrintCallback(buffer);

    return client;
}


int CloseClient(struct RdmaContext* client) {
    if(client->conn != NULL) {
        Connection *conn = client->conn;

        if (conn->comp_channel) {
            if(ibv_destroy_comp_channel(conn->comp_channel) != 0) {
                //printf("Failed to destroy comp channel: %s\n", strerror(errno));
                //sprintf(buffer,"Failed to destroy comp channel: %s\n", strerror(errno));
                //PrintCallback(buffer);
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

    if(client->cFd > 0) {
        notify_event(client->cFd,0);
        close(client->cFd);
        client->cFd = -1;
    }

    if(client->ec) {
        rdma_destroy_event_channel(client->ec);    
    }

    free(client->conn_ev);
    free(client);
    return C_OK;
}
