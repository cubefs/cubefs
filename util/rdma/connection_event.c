#include "connection_event.h"

void build_params(struct rdma_conn_param *params) {
  memset(params, 0, sizeof(*params));
  params->initiator_depth = params->responder_resources = 1; //指定发送队列的深度
  params->rnr_retry_count = 7; /* infinite retry */
  params->retry_count = 7;
}

void on_addr_resolved(struct rdma_cm_id *id) {//client
    log_debug("on_addr_resolved:%p", id);
    connection *conn = NULL;
    worker  *worker = NULL;
    get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn);
    if (conn == NULL)  {
        log_debug("get worker and connect by nd: conn is null");
        //already closed
        return;
    }

    id->verbs = g_net_env->pd->context;
    int state = get_conn_state(conn);
    int ret = create_conn_qp(conn, id);
    if (ret != C_OK) {
        log_error("conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }

    /*
    ret = rdma_setup_ioBuf(conn);
    if (ret != C_OK) {
        log_error("conn(%lu-%p) reg mem failed, errno:%d", conn->nd, conn, errno);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);//rdma todo
        //conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }
    */

    ret = rdma_resolve_route(id, TIMEOUT_IN_MS);
    if (ret != 0) {
        log_error("conn(%lu-%p) resolve failed, errno:%d", conn->nd, conn, errno);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }

    log_debug("conn(%lu-%p) addr resolved", conn->nd, conn);
    return;
}

void on_route_resolved(struct rdma_cm_id *id) {//client
    log_debug("on_route_resolved:%p", id);
    connection *conn = NULL;
    worker  *worker = NULL;
    get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn);
    if (conn == NULL)  {
        //already closed
        log_error("get worker and connect by nd: conn is null");
        return;
    }
    struct rdma_conn_param cm_params;
    build_params(&cm_params);
    int ret = rdma_connect(id, &cm_params);
    if (ret) {
        log_error("conn(%lu-%p) failed to connect to remote host , errno: %d, call on_disconnected(%p)", conn->nd, conn, errno, id);
        int state = get_conn_state(conn);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }
    log_debug("conn(%lu-%p) rdma connect, cmid:%p", conn->nd, conn, id);
}

void on_accept(struct rdma_cm_id* listen_id, struct rdma_cm_id* id) {//server
    log_debug("on_accept:%p/%p", listen_id, id);
    int ret = 0;
    struct rdma_listener* server = (struct rdma_listener*)listen_id->context;

    uint64_t nd = allocate_nd(0);
    connection * conn = init_connection(nd, CONN_TYPE_SERVER);
    if (conn == NULL) {
        log_error("server(%lu-%p) init connection return null", server->nd, server);
        rdma_reject(id, NULL, 0);
        return;
    }

    id->verbs = g_net_env->pd->context;

    ret = create_conn_qp(conn, id);
    if (ret != C_OK) {
        log_error("conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
        rdma_reject(id, NULL, 0);
        goto err_free;
    }

    /*
    ret = rdma_setup_ioBuf(conn);
    if (ret != C_OK) {
        log_error("conn(%lu-%p) reg mem failed, err:%d", conn->nd, conn, errno);
        int state = get_conn_state(conn);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_reject(id, NULL, 0);//rdma todo
        goto err_destroy_qp;
    }
    */


    id->context = (void*)conn->nd;

    struct rdma_conn_param  cm_params;
    build_params(&cm_params);
    ret = rdma_accept(id, &cm_params);
    if (ret != 0) {
        log_error("server(%lu-%p) conn(%lu-%p) accept failed, errno:%d", server->nd, server, conn->nd ,conn, errno);
        rdma_reject(id, NULL, 0);
        //goto err_destroy_iobuf;
        goto err_destroy_qp;
    }
    log_debug("server(%lu-%p) conn(%lu-%p) accept cmid:%p", server->nd, server, conn->nd, conn, id);
    add_conn_to_server(conn, server);
    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);
    conn->cm_id = id;
    return;
//err_destroy_iobuf:
//    rdma_destroy_ioBuf(conn);
err_destroy_qp:
    destroy_conn_qp(conn);
err_free:
    destroy_connection(conn);
    return;
}

void on_connected(struct rdma_cm_id *id) {//server and client
    connection *conn = NULL;
    worker  *worker = NULL;
    struct rdma_listener *server = NULL;
    get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn);
    if (conn == NULL)  {
        //already closed
        log_error("get worker and connect by nd: conn is null");
        return;
    }

    struct sockaddr *local_addr = rdma_get_local_addr(id);// 获取本地地址
    struct sockaddr *remote_addr = rdma_get_peer_addr(id);// 获取远程地址
    // 获取本地 IPv4 地址和端口号
    struct sockaddr_in *local_ipv4 = (struct sockaddr_in *)local_addr;
    inet_ntop(AF_INET, &(local_ipv4->sin_addr), conn->local_addr, INET_ADDRSTRLEN);
    snprintf(conn->local_addr + strlen(conn->local_addr), sizeof(conn->local_addr) - strlen(conn->local_addr),
            ":%d", ntohs(local_ipv4->sin_port));
    log_debug("conn(%lu-%p) local addr(%s)", conn->nd, conn, conn->local_addr);

    // 获取远程 IPv4 地址和端口号
    struct sockaddr_in *remote_ipv4 = (struct sockaddr_in *)remote_addr;
    inet_ntop(AF_INET, &(remote_ipv4->sin_addr), conn->remote_addr, INET_ADDRSTRLEN);
    snprintf(conn->remote_addr + strlen(conn->remote_addr), sizeof(conn->remote_addr) - strlen(conn->remote_addr),
            ":%d", ntohs(remote_ipv4->sin_port));

    log_debug("conn(%lu-%p) remote addr(%s)", conn->nd, conn, conn->remote_addr);

    int state = get_conn_state(conn);
    int ret = rdma_setup_ioBuf(conn);
    if (ret == C_ERR) {
        log_error("conn(%lu-%p) on_connected failed, setup io buffer return error", conn->nd, conn);
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);//rdma todo
        //conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }

    ret = rdma_exchange_rx(conn); //TODO error handler
    if (ret == C_ERR) {
        log_error("conn(%lu-%p) on_connected failed: exchange rx return error");
        if (state == CONN_STATE_CONNECTING) {
            set_conn_state(conn, CONN_STATE_CONNECT_FAIL);
        }
        rdma_disconnect(conn->cm_id);
        //conn_disconnect(conn);
        return;
    }

    log_debug("conn(%lu-%p) on_connected; conn finished", conn->nd, conn);
    return;
}

void on_disconnected(struct rdma_cm_id* id) {//server and client
    connection *conn = NULL;
    worker  *worker = NULL;
    struct rdma_listener *server = NULL;
    get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn);
    if (conn == NULL)  {
        //already closed
        log_error("get worker and connect by nd: conn is null");
        //rdma_destroy_id(id);
        return;
    }

    log_debug("conn(%lu-%p) proccess disconnected event, close begin", conn->nd, conn);
    int state = get_conn_state(conn);
    set_conn_state(conn, CONN_STATE_DISCONNECTED);

    //notify all fds when disconnecting to avoid infinite waiting
    //if (conn->conn_type == CONN_TYPE_SERVER) {//server
    //} else {//client
    //    notify_event(conn->connect_fd, 0);
    //}
    notify_event(conn->msg_fd, 0);
    notify_event(conn->close_fd, 0);
    notify_event(conn->write_fd, 0);

    if (state == CONN_STATE_CONNECT_FAIL || state == CONN_STATE_CONNECTING) {//release resources directly when an error occurs during the connection build process
        //release resource
        if (conn->conn_type == CONN_TYPE_SERVER) {//server
            server = (struct rdma_listener*)conn->context;
            //del_conn_from_server(conn, server);
            notify_event(server->connect_fd, 0);
            conn_disconnect(conn);
        } else {//client
            notify_event(conn->connect_fd, 0);
        }
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //del_conn_from_worker(conn->nd, conn->worker, conn->worker->closing_nd_map);

        //destroy_conn_qp(conn);
        //rdma_destroy_id(id);
        //rdma_destroy_ioBuf(conn);
        //destroy_connection(conn);
    }

    return;
}

void process_cm_event(struct rdma_cm_id *conn_id, struct rdma_cm_id *listen_id, int event_type) {
    log_debug("process_net_event:%d->%s", event_type, rdma_event_str(event_type));
    switch(event_type) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            on_addr_resolved(conn_id);
            break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            on_route_resolved(conn_id);
            break;
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            on_accept(listen_id, conn_id);
            break;
        case RDMA_CM_EVENT_ESTABLISHED:
            on_connected(conn_id);
            break;
        case RDMA_CM_EVENT_DISCONNECTED:
            on_disconnected(conn_id);
            break;
        case RDMA_CM_EVENT_REJECTED:
            on_disconnected(conn_id);
            break;
        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            break;
        default :
            log_error("event channel received:unknown event:%d", event_type);
            assert(event_type == 0);
            break;

    }
}

void *cm_thread(void *ctx) {
    struct net_env_st *env = (struct net_env_st*)ctx;
    struct rdma_cm_event *event;
    struct rdma_cm_id *conn_id;
    struct rdma_cm_id *listen_id;
    int event_type;
    while(1) {
        if(env->close == 1) {
            goto exit;
        }
        int ret = rdma_get_cm_event(env->event_channel, &event);
        if (ret != 0) {
            log_error("rdma get cm event failed, ret:%d");
            goto error;
        }
        conn_id = event->id;
        listen_id = event->listen_id;
        event_type = event->event;
        rdma_ack_cm_event(event);
        process_cm_event(conn_id, listen_id, event_type);
    }
error:
    //TODO
exit:
    pthread_exit(NULL);
}


