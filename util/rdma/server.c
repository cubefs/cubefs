#include "server.h"

/*
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
    //DelEpollEvent(conn->comp_channel->fd);
    DelTransferEvent(conn);
    wait_group_wait(&(conn->wg));
    destroy_connection(conn);
    pthread_mutex_lock(&(server->mutex));
    server->count--;
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
*/



connection* get_rdma_server_conn(struct rdma_listener *server) {
    wait_event(server->connect_fd);
    connection *conn;
    pthread_spin_lock(&server->conn_lock);
    DeQueue(server->wait_conns, &conn);//(Item *)
    if(conn == NULL) {
        log_debug("get server conn failed: conn is null");
        pthread_spin_unlock(&server->conn_lock);
        return NULL;
    }
    pthread_spin_unlock(&server->conn_lock);
    return conn;
}

struct rdma_listener* start_rdma_server_by_addr(char* ip, char* port) {
    struct rdma_listener* server = (struct rdma_listener*)malloc(sizeof(struct rdma_listener));
    if (server == NULL) {
        log_debug("create server failed: malloc failed");
        return NULL;
    }
    server->nd = allocate_nd(CONN_SERVER_BIT);
    server->ip = ip;
    server->port = port;
    int ret = pthread_spin_init(&(server->conn_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        log_debug("init server spin lock failed, err:%d", ret);
        goto err_free;
    }
    server->connect_fd = open_event_fd();
    if (server->connect_fd == NULL) {
        log_debug("open server event fd failed");
        goto err_destroy_spinlock;
    }
    server->conn_map = hashmap_create();
    if (server->conn_map == NULL) {
        log_debug("create server conn map failed");
        goto err_destroy_fd;
    }
    server->wait_conns = InitQueue();
    if (server->wait_conns == NULL) {
        log_debug("init server wait conns queue failed");
        goto err_destroy_map;
    }
    struct rdma_addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family  = AF_INET;
    addr.sin_port  = htons(atoi(port));
    addr.sin_addr.s_addr = inet_addr(ip);
    ret = rdma_create_id(g_net_env->event_channel, &server->listen_id, server, RDMA_PS_TCP);
    if (ret != 0) {
        log_debug("rdma create id failed, errno:%d", errno);
        goto err_destroy_queue;
    }
    log_debug("server: listen_id:%p",server->listen_id);
    ret = rdma_bind_addr(server->listen_id, (struct sockaddr *)&addr);
    if (ret != 0) {
        log_debug("rdma bind addr failed, errno:%d", errno);
        goto err_destroy_id;
    }
    ret = rdma_listen(server->listen_id, 10);
    if (ret != 0) {
        log_debug("rdma listen failed, errno:%d", errno);
        goto err_destroy_id;
    }
    add_server_to_env(server, g_net_env->server_map);

    return server;

err_destroy_id:
    rdma_destroy_id(server->listen_id);
err_destroy_queue:
    DestroyQueue(server->wait_conns);
err_destroy_map:
    hashmap_destroy(server->conn_map);
err_destroy_fd:
    notify_event(server->connect_fd,1);
err_destroy_spinlock:
    pthread_spin_destroy(&server->conn_lock);
err_free:
    free(server);
    return NULL;
}

void close_rdma_server(struct rdma_listener* server) {
    if (server != NULL) {
        del_server_from_env(server);
        notify_event(server->connect_fd,1);
        DestroyQueue(server->wait_conns);
        hashmap_destroy(server->conn_map);
        pthread_spin_destroy(&server->conn_lock);
        if (server->listen_id != 0) {
            rdma_destroy_id(server->listen_id);
        }
        free(server);
        return;
    }
}
