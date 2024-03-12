#include "server.h"


int OnServerConnPreConnect(struct rdma_cm_id *id, void* ctx) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaListener* server = (struct RdmaListener*)conn_ev->ctx;
    Connection* conn = AllocConnection(id, conn_ev, 1);
    if(conn == NULL) {
        //PrintCallback(buffer);
        return C_ERR;
    }
    conn->state = CONN_STATE_CONNECTING;
    conn->csContext = server;
    id->context = conn;
    return C_OK;
}

int OnServerConnConnected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaListener* server = (struct RdmaListener*)ctx;
    Connection* conn = (Connection*)id->context;
    pthread_mutex_lock(&(server->mutex));
    if(EnQueue(server->waitConns,conn) == NULL) {
        //printf("no more memory can be malloced\n");
    };

    hashmap_set(server->allConns,conn);
    if(hashmap_oom(server->allConns)) {
        //printf("no more memory can be malloced\n");
    }
    server->count++;
    wait_group_add(&server->closeWg,1);
    pthread_mutex_unlock(&(server->mutex));
    notify_event(server->cFd, 0);
    id->context = conn;
    return C_OK;
}

int OnServerConnDisconnected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaListener* server = (struct RdmaListener*)ctx;
    Connection* conn = (Connection*)id->context;
    conn->state = CONN_STATE_CLOSED;
    if(conn->connContext != NULL) {
        DisConnectCallback(conn->connContext);
    }
    //EpollDelConnEvent(conn->comp_channel->fd);
    DelEpollEvent(conn->comp_channel->fd);
    wait_group_wait(&(conn->wg));
    destroy_connection(conn);
    pthread_mutex_lock(&(server->mutex));
    if(hashmap_delete(server->allConns,conn) == NULL) {
        //printf("server conn is not in map\n");
    } else {
        server->count--;
    }
    pthread_mutex_unlock(&(server->mutex));
    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }
    conn->cm_id = NULL;
    rdmaDestroyIoBuf(conn);
    if(conn->freeList) {
        //ClearQueue(conn->freeList);
        DestroyQueue(conn->freeList);
    }
    if(conn->wg.wgInitialized == 1) {
        wait_group_destroy(&conn->wg);
    }
    if(conn->lockInitialized == 1) {
        pthread_spin_destroy(&conn->lock);
    }
    conn->connContext = NULL;
    conn->csContext = NULL;
    notify_event(conn->cFd, 0);
    wait_group_done(&server->closeWg);
    return C_OK;
}

Connection* getServerConn(struct RdmaListener *server) {
    if(wait_event(server->cFd) <= 0) {
		return NULL;
	}
    Connection *conn;
    pthread_mutex_lock(&(server->mutex));
    DeQueue(server->waitConns, &conn);
    if(conn == NULL) {
    }
    pthread_mutex_unlock(&(server->mutex));
    return conn;
}

struct RdmaListener* StartServer(const char* ip, uint16_t port, char* serverAddr) {
    struct rdma_addrinfo hints, *res;
    struct ibv_qp_init_attr init_attr;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family  = AF_INET;
    addr.sin_port  = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip);
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec = NULL;
    TEST_Z_(ec = rdma_create_event_channel());
    TEST_NZ_(rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP));
    TEST_NZ_(rdma_bind_addr(listen_id, (struct sockaddr *)&addr));
    TEST_NZ_(rdma_listen(listen_id, 10));
    struct RdmaListener* server = (struct RdmaListener*)malloc(sizeof(struct RdmaListener));
    if(server == NULL) {
        return NULL;
    }
    pthread_mutex_init(&(server->mutex),NULL);
    server->waitConns = InitQueue();
    if(server->waitConns == NULL) {
        return NULL;
    }
    server->allConns = hashmap_new(sizeof(Connection),0,0,0,connection_hash,connection_compare,NULL,NULL);
    if(server->allConns == NULL) {
        return NULL;
    }
    server->count = 0;
    server->listen_id = listen_id;
    server->ec = ec;
    server->ip = ip;
    server->port = port;
    server->state = 0;
    server->cFd = open_event_fd();
    int ret = wait_group_init(&server->closeWg);
    if(ret) {
        //printf("init conn wg failed, err:%d",ret);
        return NULL;
    }
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = listen_id;
    conn_ev->ctx = server;
    conn_ev->preconnect_callback = OnServerConnPreConnect;
    conn_ev->connected_callback = OnServerConnConnected;
    conn_ev->disconnected_callback = OnServerConnDisconnected;
    server->conn_ev = conn_ev;
    //EpollAddConnectEvent(server->listen_id->channel->fd, conn_ev);
    epoll_rdma_connectEvent_add(server->listen_id->channel->fd, conn_ev, connection_event_cb);
    return server;
}

int CloseServer(struct RdmaListener* server) {
    wait_group_wait(&server->closeWg);
    wait_group_destroy(&server->closeWg);
    ClearQueue(server->waitConns);
    DestroyQueue(server->waitConns);
    hashmap_free(server->allConns);
    if(server->cFd > 0) {
        notify_event(server->cFd,0);
        close(server->cFd);
        server->cFd = -1;
    }
    //EpollDelConnEvent(server->listen_id->channel->fd);
    DelEpollEvent(server->listen_id->channel->fd);
    rdma_destroy_id(server->listen_id);
    if(server->ec) {
        rdma_destroy_event_channel(server->ec);
    }
    free(server->conn_ev);
    free(server);
    return C_OK;
}
