#include <client.h>

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
err_free:
    del_conn_from_worker(conn->nd,conn->worker,conn->worker->nd_map);
    destroy_connection(conn);
    return NULL;
}