#include "server.h"

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
