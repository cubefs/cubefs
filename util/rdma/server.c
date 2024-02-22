#include "server.h"



//在RDMA通信中，每个连接的建立过程都是独立的，连接之间不会干扰或交叉进行??
int OnServerConnPreConnect(struct rdma_cm_id *id, void* ctx) {//TODO maybe need to return C_OK or C_ERR
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaListener* server = (struct RdmaListener*)conn_ev->ctx;
    //printf("server=%p, on %s \n", server, __FUNCTION__);
    //sprintf(buffer,"server=%p, on %s \n", server, __FUNCTION__);
    //PrintCallback(buffer);

    Connection* conn = AllocConnection(id, conn_ev, 1);

    if(conn == NULL) {//TODO error handler
        //sprintf(buffer,"server=%p, there is no space to alloc conn\n", server);
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
    //printf("server=%p, on %s \n", server, __FUNCTION__);
    //sprintf(buffer,"server=%p, on %s \n", server, __FUNCTION__);
    //PrintCallback(buffer);

    Connection* conn = (Connection*)id->context;

    pthread_mutex_lock(&(server->mutex));

    if(EnQueue(server->waitConns,conn) == NULL) { //TODO error handler
        //printf("no more memory can be malloced\n");
        //sprintf(buffer,"no more memory can be malloced\n");
        //PrintCallback(buffer);
    };
    //printf("waitConns size: %d\n",GetSize(server->waitConns));
    //sprintf(buffer,"waitConns size: %d\n",GetSize(server->waitConns));
    //PrintCallback(buffer);

    hashmap_set(server->allConns,conn);
    if(hashmap_oom(server->allConns)) { //TODO error handler
        //printf("no more memory can be malloced\n");
        //sprintf(buffer,"no more memory can be malloced\n");
        //PrintCallback(buffer);
    }
    //printf("hashmap size :%d\n",hashmap_count(server->allConns));
    //sprintf(buffer,"hashmap size :%d\n",hashmap_count(server->allConns));
    //PrintCallback(buffer);
    server->count++;//TODO 需要判断是否达到最大连接数
    wait_group_add(&server->closeWg,1);

    pthread_mutex_unlock(&(server->mutex));

    notify_event(server->cFd, 0);

    id->context = conn;

    return C_OK;
}

int OnServerConnDisconnected(struct rdma_cm_id *id, void* ctx) { //TODO just conn disconnected

    struct RdmaListener* server = (struct RdmaListener*)ctx;
    //printf("server=%p, on %s \n", server, __FUNCTION__);
    //sprintf(buffer,"server=%p, on %s \n", server, __FUNCTION__);
    //PrintCallback(buffer);
    Connection* conn = (Connection*)id->context;
    //sprintf(buffer,"conn=%d disconnected\n", conn);
    //PrintCallback(buffer);
    pthread_spin_lock(&conn->lock);
    conn->state = CONN_STATE_CLOSED;
    pthread_spin_unlock(&conn->lock);
    
    if(conn->connContext != NULL) {
        DisConnectCallback(conn->connContext);
    }
    
    EpollDelConnEvent(conn->comp_channel->fd);

    wait_group_wait(&(conn->wg));

    destroy_connection(conn);

    pthread_mutex_lock(&(server->mutex));

    if(hashmap_delete(server->allConns,conn) == NULL) { //TODO error handler
        //printf("server conn is not in map\n");
        //sprintf(buffer,"server conn is not in map\n");
        //PrintCallback(buffer);
    } else {
        server->count--;
    }
    //printf("hashmap size :%d\n",hashmap_count(server->allConns));
    //sprintf(buffer,"hashmap size :%d\n",hashmap_count(server->allConns));
    //PrintCallback(buffer);

    pthread_mutex_unlock(&(server->mutex));

    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }
    conn->cm_id = NULL;



    rdmaDestroyIoBuf(conn);

    if(conn->freeList) {
        //ClearQueue(conn->freeList);
        //printf("header freeList size: %d\n",GetSize(conn->freeList));
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
    if(wait_event(server->cFd) <= 0) {//TODO error handler
		return NULL;
	}

    Connection *conn;
    //因为是串行获取连接的，所以不需要加锁
    pthread_mutex_lock(&(server->mutex));
    DeQueue(server->waitConns, &conn);
    if(conn == NULL) {//TODO
        
    }
    pthread_mutex_unlock(&(server->mutex));

    //printf("waitConns size: %d\n",GetSize(server->waitConns));
    //sprintf(buffer,"waitConns size: %d\n",GetSize(server->waitConns));
    //PrintCallback(buffer);
    return conn;
}

struct RdmaListener* StartServer(const char* ip, uint16_t port, char* serverAddr) {//, MemoryPool* pool, ObjectPool* headerPool, ObjectPool* responsePool, struct ibv_pd* pd, struct ibv_mr* mr
    //printf("StartServer ip=%s, port=%d\n", ip, port);
    //sprintf(buffer,"StartServer ip=%s, port=%d\n", ip, port);
    //PrintCallback(buffer);
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
    if(server == NULL) { //TODO
        return NULL;
    }

    pthread_mutex_init(&(server->mutex),NULL);

    server->waitConns = InitQueue();
    if(server->waitConns == NULL) { //TODO
        return NULL;
    }

    server->allConns = hashmap_new(sizeof(Connection),0,0,0,connection_hash,connection_compare,NULL,NULL);
    if(server->allConns == NULL) { //TODO
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
        //sprintf(buffer,"init conn wg failed, err:%d",ret);
        //PrintCallback(buffer);
        return NULL;
    }

    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = listen_id;
    conn_ev->ctx = server;

    conn_ev->preconnect_callback = OnServerConnPreConnect;
    conn_ev->connected_callback = OnServerConnConnected;
    conn_ev->disconnected_callback = OnServerConnDisconnected;

    server->conn_ev = conn_ev;

    EpollAddConnectEvent(server->listen_id->channel->fd, conn_ev);

    //printf("start server %p \n", server);
    //sprintf(buffer,"start server %p \n", server);
    //PrintCallback(buffer);

    return server;
}

int CloseServer(struct RdmaListener* server) { //TODO
    
    
    //TODO first close all id

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
    

    EpollDelConnEvent(server->listen_id->channel->fd);
    rdma_destroy_id(server->listen_id);
    
    if(server->ec) {
        rdma_destroy_event_channel(server->ec);
    }
    free(server->conn_ev);
    free(server);
    return C_OK;
}
