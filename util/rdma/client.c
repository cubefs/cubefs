#include <client.h>

struct connection* rdma_connect_by_addr(const char* ip, const char* port, int use_external_tx_flag) {
    struct addrinfo *addr;
    struct rdma_conn_param cm_params;
    uint64_t nd;
    nd = allocate_nd(CONN_ACTIVE_BIT);
    connection* conn = init_connection(nd, CONN_TYPE_CLIENT, use_external_tx_flag);
    if (conn == NULL) {
        log_error("init_connection return null");
        return NULL;
    }

    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);

    getaddrinfo(ip, port, NULL, &addr);
    int ret = rdma_create_id(g_net_env->event_channel, &conn->cm_id, (void*)(conn->nd), RDMA_PS_TCP);
    if (ret != 0) {
        log_error("conn(%ld-%p) create cmid failed, err:%d", conn->nd, conn, errno);
        goto err_free;
    }
    log_debug("conn(%lu-%p) create cmid:%p", conn->nd, conn, conn->cm_id);

    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if (ret != 0) {
        log_error("conn(%lu-%p) solve addr failed, err:%d", conn->nd, conn, errno);
        goto err_destroy_id;
    }
    freeaddrinfo(addr);

    wait_event(conn->connect_fd, -1);
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) {//state is already equal to CONN_STATE_DISCONNECTED
        conn_disconnect(conn, 0);
        return NULL;
    }
    return conn;
err_destroy_id:
    rdma_destroy_id(conn->cm_id);
err_free:
    del_conn_from_worker(conn->nd,conn->worker,conn->worker->nd_map);
    destroy_connection(conn);
    return NULL;
}

struct connection* rdma_connect_by_addr_with_timeout(const char* ip, const char* port, int use_external_tx_flag, int64_t timeout_ns) {
    struct addrinfo *addr;
    struct rdma_conn_param cm_params;
    uint64_t nd;
    nd = allocate_nd(CONN_ACTIVE_BIT);
    connection* conn = init_connection(nd, CONN_TYPE_CLIENT, use_external_tx_flag);
    if (conn == NULL) {
        log_error("init_connection return null");
        return NULL;
    }

    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);

    getaddrinfo(ip, port, NULL, &addr);
    int ret = rdma_create_id(g_net_env->event_channel, &conn->cm_id, (void*)(conn->nd), RDMA_PS_TCP);
    if (ret != 0) {
        log_error("conn(%ld-%p) create cmid failed, err:%d", conn->nd, conn, errno);
        goto err_free;
    }
    log_debug("conn(%lu-%p) create cmid:%p", conn->nd, conn, conn->cm_id);

    ret = rdma_resolve_addr(conn->cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if (ret != 0) {
        log_error("conn(%lu-%p) solve addr failed, err:%d", conn->nd, conn, errno);
        goto err_destroy_id;
    }
    freeaddrinfo(addr);

    ret = wait_event(conn->connect_fd, timeout_ns);
    if (ret == -2) {
        log_error("conn(%lu-%p) wait connect fd timeout", conn->nd ,conn);
        conn_disconnect(conn, 0);
        return NULL;
    }
    int state = get_conn_state(conn);
    if (state != CONN_STATE_CONNECTED) {//state is already equal to CONN_STATE_DISCONNECTED
        conn_disconnect(conn, 0);
        return NULL;
    }
    return conn;
err_destroy_id:
    rdma_destroy_id(conn->cm_id);
err_free:
    del_conn_from_worker(conn->nd,conn->worker,conn->worker->nd_map);
    destroy_connection(conn);
    return NULL;
}