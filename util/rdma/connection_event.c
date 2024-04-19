#include "connection_event.h"

/*
void destroy_connection(Connection *conn) {
    if(conn->pd) {
        conn->pd = NULL;
    }
    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            log_debug("Failed to destroy qp: %s\n", strerror(errno));
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            // we continue anyways;
        }
    }
    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            log_debug("Failed to destroy cq: %s\n", strerror(errno));
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->comp_channel) {
        int ret = ibv_destroy_comp_channel(conn->comp_channel);
        if(ret != 0) {
            log_debug("Failed to destroy comp channel: %s\n", strerror(errno));
            //printf("Failed to destroy comp channel: %s\n", strerror(errno));
            // we continue anyways;
        }
        conn->comp_channel = NULL;
    }
}

int build_connection(struct ConnectionEvent *conn_ev, Connection *conn) {
    int ret = C_OK;
    struct ibv_device_attr device_attr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_comp_channel *comp_channel = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_pd *pd = NULL;
    struct rdma_cm_id *cm_id = conn->cm_id;
    pd = rdmaPool->memoryPool->pd;
    cm_id->verbs = pd->context;
    if (ibv_query_device(cm_id->verbs, &device_attr)) {
        log_debug("RDMA: ibv query device failed\n");
        //printf("RDMA: ibv query device failed\n");
        goto error;
    }
    conn->pd = pd;
    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!comp_channel) {
        log_debug("RDMA: ibv create comp channel failed\n");
        //printf("RDMA: ibv create comp channel failed\n");
        goto error;
    }
    conn->comp_channel = comp_channel;
    cq = ibv_create_cq(cm_id->verbs, MIN_CQE_NUM, NULL, comp_channel, 0);//when -1, cq is null?     RDMA_MAX_WQE * 2
    if (!cq) {
        log_debug("RDMA: ibv create cq failed: cq:%d\n",cq);
        //printf("RDMA: ibv create cq failed: cq:%d\n",cq);
        goto error;
    }
    conn->cq = cq;
    ibv_req_notify_cq(cq, 0);
    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = WQ_DEPTH;
    init_attr.cap.max_recv_wr = WQ_DEPTH;
    init_attr.cap.max_send_sge = device_attr.max_sge;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cq;
    init_attr.recv_cq = cq;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret) {
        log_debug("RDMA: create qp failed: %d\n",ret);
        //printf("RDMA: create qp failed: %d\n",ret);
        goto error;
    }
    return C_OK;
error:
    destroy_connection(conn);
    return C_ERR;
}
*/

/*
void connection_event_cb(void *ctx) {
    return;
}

int connection_event_handler(struct rdma_cm_id *conn_id, struct rdma_cm_id *listen_id, int event_type) {
  int v;
  int ret;
  Connection *conn;
  struct rdma_conn_param cm_params;
  switch (event_type) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
          //printf("RDMA_CM_EVENT_ADDR_RESOLVED\n");
          v = conn_ev->preconnect_callback(conn_id, conn_ev); //创建connection
          if(!v) {
            rdma_disconnect(conn_id);
            log_debug("client: rdma addr resolved failed, call rdma_disconnect\n");
            //printf("client: rdma addr resolved failed, call rdma_disconnect\n");
            return C_ERR;
          } else {
            rdma_resolve_route(conn_id, TIMEOUT_IN_MS);
            log_debug("client: rdma addr resolved success\n");
            //printf("client: rdma addr resolved success\n");
            return C_OK;
          }
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
          log_debug("client: rdma_cn_event_route_resolved\n");
          //printf("RDMA_CM_EVENT_ROUTE_RESOLVED\n");
          build_params(&cm_params);
          if(rdma_connect(conn_id, &cm_params)) {
              log_debug("client: rdma_route resolved failed, call rdma_disconnect\n");
              //printf("client: rdma_route resolved failed, call rdma_disconnect\n");
              rdma_disconnect(conn_id);
              return C_ERR;
          }
          return C_OK;
    case RDMA_CM_EVENT_CONNECT_REQUEST:
          log_debug("server: rdma_cm_event_connect_request\n");
          //printf("RDMA_CM_EVENT_CONNECT_REQUEST\n");
          v = conn_ev->preconnect_callback(conn_id, conn_ev);
          if(!v) {
              log_debug("server: rdma_connect request failed, call rdma_reject\n");
              //printf("server: rdma_connect request failed, call rdma_reject\n");
              rdma_reject(conn_id, NULL, 0);
              rdma_destroy_id(conn_id);
              return C_ERR;
          } else {
              struct rdma_conn_param cm_params;
              build_params(&cm_params);
              ret = rdma_accept(conn_id, &cm_params);
              if (ret) {
                  log_debug("server: rdma_connect request failed, call rdma_reject\n");
                  //printf("server: rdma_connect request failed, call rdma_reject\n");
                  rdma_reject(conn_id, NULL, 0);
                  return C_ERR;
              }
          }
          return C_OK;
    case RDMA_CM_EVENT_ESTABLISHED:
          log_debug("rdma_cm_event_established\n");
          //printf("RDMA_CM_EVENT_ESTABLISHED\n");
          conn_ev->connected_callback(conn_id, conn_ev->ctx);
          return C_OK;
    case RDMA_CM_EVENT_DISCONNECTED:
          log_debug("rdma_cm_event_disconnected\n");
          //printf("RDMA_CM_EVENT_DISCONNECTED\n");
          conn = (Connection *)conn_id->context;
          if (conn->conntype == 1) {//server
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
          } else {
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
          }
          return C_OK;
    case RDMA_CM_EVENT_REJECTED:
          log_debug("rdma_cm_event_rejected\n");
          //printf("RDMA_CM_EVENT_REJECTED\n");
          conn_ev->rejected_callback(conn_id, conn_ev->ctx);
          return C_OK;
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
          log_debug("rdma_cm_event_timewait_exit\n");
          //printf("RDMA_CM_EVENT_TIMEWAIT_EXIT\n");
          conn = (Connection *)conn_id->context;
          if (conn->conntype == 2) {//client
          } else {
          }
          return C_OK;
    default:
          log_debug("unknown event %d \n", event_type);
          //printf("unknown event %d \n", event_type);
          return C_ERR;
  }
}
*/

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
        conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }


    ret = rdma_resolve_route(id, TIMEOUT_IN_MS);
    if (ret != 0) {
        log_debug("conn(%lu-%p) resolve failed, errno:%d", conn->nd, conn, errno);
        conn_disconnect(conn);
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
        log_debug("get worker and connect by nd: conn is null");
        return;
    }
    /*
    id->verbs = g_net_env->pd->context;
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
        conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }
    */
    struct rdma_conn_param cm_params;
    build_params(&cm_params);
    int ret = rdma_connect(id, &cm_params);
    if (ret) {
        log_debug("Failed to connect to remote host , errno: %d, call on_disconnected(%p)", errno, id);
        conn_disconnect(conn);
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
        log_debug("init_connection return null");
        rdma_reject(id, NULL, 0);
        return;
    }

    id->verbs = g_net_env->pd->context;

    ret = create_conn_qp(conn, id);
    if (ret != C_OK) {
        log_debug("rdma build connection failed");
        rdma_reject(id, NULL, 0);
        goto err_free;
    }

    ret = rdma_setup_ioBuf(conn, CONN_TYPE_SERVER);
    if (ret != C_OK) {
        log_debug("rdma reg mem failed, err:%d", errno);
        rdma_reject(id, NULL, 0);
        goto err_destroy_qp;
    }


    id->context = (void*)conn->nd;

    struct rdma_conn_param  cm_params;
    build_params(&cm_params);
    ret = rdma_accept(id, &cm_params);
    if (ret != 0) {
        log_debug("accept conn:%p, rdma accept failed, errno:%d", conn, errno);
        rdma_reject(id, NULL, 0);
        goto err_destroy_iobuf;
        //goto err_destroy_qp;
    }
    log_debug("conn(%lu-%p) rdma_accept cmid:%p", conn->nd, conn, id);
    add_conn_to_server(conn, server);
    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);
    conn->cm_id = id;
    return;
err_destroy_iobuf:
    rdma_destroy_ioBuf(conn);
err_destroy_qp:
    destroy_conn_qp(conn);
err_free:
    destroy_connection(conn);
    free(conn);
    return;
}

void on_connected(struct rdma_cm_id *id) {//server and client
    connection *conn = NULL;
    worker  *worker = NULL;
    struct rdma_listener *server = NULL;
    get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn);
    if (conn == NULL)  {
        //already closed
        log_debug("get worker and connect by nd: conn is null");
        return;
    }

    struct sockaddr *local_addr = rdma_get_local_addr(id);// 获取本地地址
    struct sockaddr *remote_addr = rdma_get_peer_addr(id);// 获取远程地址
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

    /*
    int ret = rdma_setup_ioBuf(conn, conn->conn_type);
    if (ret != C_OK) {
        log_debug("rdma set up mem failed, err:%d", errno);
        conn_disconnect(conn);
        //del_conn_from_worker(conn->nd, worker, worker->nd_map);
        //add_conn_to_worker(conn, worker, worker->closing_nd_map);
        return;
    }
    */

    if(conn->state == CONN_STATE_CONNECTING) {
        pthread_spin_lock(&conn->spin_lock);
        set_conn_state(conn, CONN_STATE_CONNECTED);
        pthread_spin_unlock(&conn->spin_lock);
    }

    if (conn->conn_type == CONN_TYPE_SERVER) {
        server = (struct rdma_listener*)conn->context;
        if(EnQueue(server->wait_conns,conn) == NULL) {
            log_debug("server wait conns has no more memory can be malloced\n");
            conn_disconnect(conn);
            //del_conn_from_worker(conn->nd, worker, worker->nd_map);
            //add_conn_to_worker(conn, worker, worker->closing_nd_map);
            return;
        }
        notify_event(server->connect_fd, 0);
    } else {
        notify_event(conn->connect_fd, 0);
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
        log_debug("get worker and connect by nd: conn is null");
        rdma_destroy_id(id);
        return;
    }

    log_debug("conn(%lu-%p) proccess disconnected event, close begin", conn->nd, conn);
    pthread_spin_lock(&conn->spin_lock);
    set_conn_state(conn, CONN_STATE_DISCONNECTED);
    pthread_spin_unlock(&conn->spin_lock);

    if (conn->conn_type == CONN_TYPE_SERVER) {//server
        server = (struct rdma_listener*)conn->context;
        del_conn_from_server(conn, server);
    } else {//client

    }
    del_conn_from_worker(conn->nd, worker, worker->nd_map);
    //del_conn_from_worker(conn->nd, conn->worker, conn->worker->closing_nd_map);
    //DisConnectCallback(conn->conn_context);//TODO
    //notify_event(conn->close_fd, 0);

    destroy_conn_qp(conn);
    rdma_destroy_id(id);
    rdma_destroy_ioBuf(conn);
    destroy_connection(conn);
    //free(conn);
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
            log_debug("event channel received:unknown event:%d", event_type);
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


