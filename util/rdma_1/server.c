#include <stdio.h>
#include "rdma.h"
//#include "connection_event.h"
//#include "connection.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdlib.h>

#include <unistd.h>

static inline int open_event_fd();
static inline int wait_event(int);
static inline int notify_event(int, int); 

//在RDMA通信中，每个连接的建立过程都是独立的，连接之间不会干扰或交叉进行??
static int OnServerConnPreConnect(struct rdma_cm_id *id, void* ctx) {//TODO maybe need to return C_OK or C_ERR
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaListener* server = (struct RdmaListener*)conn_ev->ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);

    Connection* conn = AllocConnection(id, conn_ev, 1);
    //int ret = build_connection(id, conn_ev, conn);
    //if(ret == C_ERR) {//TODO error handler
    //    printf("server build connection failed");
    //    return;
    //}
    //conn = NULL;

    if(conn == NULL) {//TODO error handler 
        
        //close connection
        //rdma_destroy_id(id);
        return C_ERR;
    }

    //char key_string[256];//TODO because key type is char*
    //snprintf(key_string, 256, "%d", conn->comp_channel->fd);
    //hashmap_put(server->conns, key_string, conn);

    //server->conns[server->count] = conn;
    //server->count++;//TODO 需要判断是否达到最大连接数

    //conn->remoteAddr = server->remoteAddr;
/*    if (ListenTransferRecvEvent(conn) == -1  || ListenTransferSendEvent(conn) == -1) {
         ServerConnectionCallback(0, conn); //connection close
         return;
    }*/

    
    //conn->readyCallback = OnServerConnReady;

    conn->state = CONN_STATE_CONNECTING;

    conn->csContext = server;
    id->context = conn;

    //EpollAddSendAndRecvEvent(server->comp_channel->fd, conn);
    //EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
    return C_OK;
}

static int OnServerConnConnected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaListener* server = (struct RdmaListener*)ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);

    Connection* conn = (Connection*)id->context;

    //connRdmaRegisterRx(conn, id);
    //SendMemory(conn, conn->memory_pool->localRegion);

    //conn->state = CONN_STATE_CONNECTED;
    
    //printf("poll queue %d\n",conn);

    pthread_mutex_lock(&(server->mutex));

    if(EnQueue(server->waitConns,conn) == NULL) { //TODO error handler
        printf("no more memory can be malloced\n");
    };
    printf("waitConns size: %d\n",GetSize(server->waitConns));

    //printf("%d\n",strlen((char*)(conn->cm_id)));
    /*int ret = hashmap_put(server->allConns,(char*)(conn->cm_id),conn);
    printf("char * conn->cm_id %d\n",(char*)(conn->cm_id));
    if (ret==-1) { //Out of Memory
        //TODO error handler
        printf("put conn to map failed\n");
        return;
    }*/

    //int ret = hashmap_put(&(server->allConns),(char*)(conn->cm_id),sizeof((char*)(conn->cm_id)),conn);
    hashmap_set(server->allConns,conn);
    if(hashmap_oom(server->allConns)) { //TODO error handler
        printf("no more memory can be malloced\n");
    }
    //printf("conn: %d\n",conn);
    printf("hashmap size :%d\n",hashmap_count(server->allConns));
    //server->allConns[server->count] = conn;
    server->count++;//TODO 需要判断是否达到最大连接数
    wait_group_add(&server->closeWg,1);
    //printf("server count %d\n",server->count);

    pthread_mutex_unlock(&(server->mutex));

    notify_event(server->cFd, 0);

    id->context = conn;

    //conn->readyCallback(1, conn);
 
    //EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
    return C_OK;
}

static int OnServerConnDisconnected(struct rdma_cm_id *id, void* ctx) { //TODO just conn disconnected
    struct RdmaListener* server = (struct RdmaListener*)ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);
    //struct rdma_cm_id *id = event->id;
    Connection* conn = (Connection*)id->context;
    pthread_spin_lock(&conn->lock);
    //conn->state = CONN_STATE_CLOSING;
    conn->state = CONN_STATE_CLOSED;
    pthread_spin_unlock(&conn->lock);
    //rdma_ack_cm_event(event);

    /*int ret = rdma_disconnect(id);
    if (ret!=0) {//TODO error handler
        printf("server disconnect failed\n");
        return;
    }*/

    //poll all CQ before close
    //printf("server conn poll all cq event bufore disconnect\n");
    //transport_sendAndRecv_event_cb(conn);
    
    if(conn->connContext != NULL) {
        DisConnectCallback(conn->connContext);
    }
    
    EpollDelConnEvent(conn->comp_channel->fd);

    wait_group_wait(&(conn->wg));

    /*
    if (conn->cq) {
        ibv_destroy_cq(conn->cq);
    }
    if (conn->comp_channel) {
        ibv_destroy_comp_channel(conn->comp_channel);
    }
    if (conn->pd) {
        //ibv_dealloc_pd(conn->pd);
        conn->pd = NULL;
    }
    if (id->qp) {
        ibv_destroy_qp(id->qp);
    }
    */

    destroy_connection(conn);

    //struct RdmaListener* server = (struct RdmaListener*)(conn->csContext);

    pthread_mutex_lock(&(server->mutex));

    if(hashmap_delete(server->allConns,conn) == NULL) { //TODO error handler
        printf("server conn is not in map\n");
    } else {
        server->count--;
    }
    printf("hashmap size :%d\n",hashmap_count(server->allConns));

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

    //ServerConnectionCallback(0, conn); //connection close
    

    //char key_string[256];//TODO because key type is char*
    //snprintf(key_string, 256, "%d", conn->comp_channel->fd);
    //hashmap_remove(server->conns, key_string);

    //printf("conn id %d\n",conn->cm_id);
    //printf("%d\n",server->listen_id);

    //printf("id %d\n",id);
    //printf("conn->cm_id %d\n",conn->cm_id);
    //printf("server->listen_id %d\n",server->listen_id);

    //free(conn); //TODO这里需要考虑conn复用的方式
    //conn = NULL;
    //rdma_disconnect(id);
    //printf("%d\n",id);
 
    //rdma_destroy_id(id); //TODO

    

    conn->connContext = NULL;
    conn->csContext = NULL;

    //pthread_spin_lock(&conn->lock);
    //conn->state = CONN_STATE_CLOSED;
    //pthread_spin_unlock(&conn->lock);



    notify_event(conn->cFd, 0);


    wait_group_done(&server->closeWg);
    return C_OK;
}

/*static void OnServerConnTimeWaitExit(struct rdma_cm_id *id, void* ctx) {
    struct RdmaListener* server = (struct RdmaListener*)ctx;
    printf("server=%p, %s \n", server, __FUNCTION__);
    rdma_destroy_id(id);
}*/

static Connection* getServerConn(struct RdmaListener *server) {
    //for(;GetSize(server->waitConns) == 0;) {//阻塞实现方式?
        //usleep(10 * 1000);
    //}
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

    printf("waitConns size: %d\n",GetSize(server->waitConns));
    return conn;
}

static struct RdmaListener* StartServer(const char* ip, uint16_t port, char* serverAddr) {//, MemoryPool* pool, ObjectPool* headerPool, ObjectPool* responsePool, struct ibv_pd* pd, struct ibv_mr* mr
    printf("StartServer ip=%s, port=%d\n", ip, port);
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

    //struct RdmaContext* server = (struct RdmaCOntext*)malloc(sizeof(struct RdmaContext));
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
    //printf("hashmap count %d\n",hashmap_count(server->allConns));
    server->count = 0;

    server->listen_id = listen_id;
    server->ec = ec;
    server->ip = ip;
    server->port = port;
    server->state = 0;
    server->cFd = open_event_fd();
    int ret = wait_group_init(&server->closeWg);
    if(ret) {
        printf("init conn wg failed, err:%d",ret);
        return NULL;
    }
    //server->closeFd = open_event_fd();
    //server->serverAddr = serverAddr;

    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = listen_id;
    conn_ev->ctx = server;

    //conn_ev->pool = pool;
    //conn_ev->header_pool = headerPool;
    //conn_ev->response_pool = responsePool;
    //conn_ev->pd = pd;
    //conn_ev->mr = mr;

    conn_ev->preconnect_callback = OnServerConnPreConnect;
    conn_ev->connected_callback = OnServerConnConnected;
    conn_ev->disconnected_callback = OnServerConnDisconnected;
    //conn_ev->timewaitexit_callback = OnServerConnTimeWaitExit;

    server->conn_ev = conn_ev;

    EpollAddConnectEvent(server->listen_id->channel->fd, conn_ev);

    printf("start server %p \n", server);

    return server;
}

static int CloseServer(struct RdmaListener* server) { //TODO
    
    
    //TODO first close all id
    /*for (int i=0;i<server->count;i++) {
        Connection* conn = server->allConns[i];
        for(conn->state != CONN_STATE_CLOSED) {
            Disconnect(conn);
            usleep(10 * 1000);
        }
        free(conn);
    }*/

    wait_group_wait(&server->closeWg);
    wait_group_destroy(&server->closeWg);
/*
    if(server->count != 0) {
        //TODO error handler
        printf("server count is not zero(%d)\n",server->count);
        return C_ERR;
    }
*/

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
