#include "connection_event.h"

int getHeaderSize() {
    return sizeof(Header);
}

int getResponseSize() {
    return sizeof(Response);
}

void destroy_connection(Connection *conn) {
    if(conn->pd) {
        conn->pd = NULL;
    }
    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            // we continue anyways;
        }
    }
    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->comp_channel) {
        int ret = ibv_destroy_comp_channel(conn->comp_channel);
        if(ret != 0) {
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
        //printf("RDMA: ibv query device failed\n");
        goto error;
    }
    conn->pd = pd;
    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!comp_channel) {
        //printf("RDMA: ibv create comp channel failed\n");
        goto error;
    }
    conn->comp_channel = comp_channel;
    cq = ibv_create_cq(cm_id->verbs, MIN_CQE_NUM, NULL, comp_channel, 0);//when -1, cq is null?     RDMA_MAX_WQE * 2
    if (!cq) {
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
        //printf("RDMA: create qp failed: %d\n",ret);
        goto error;
    }
    return C_OK;
error:
    destroy_connection(conn);
    return C_ERR;
}

void build_params(struct rdma_conn_param *params) {
  memset(params, 0, sizeof(*params));
  params->initiator_depth = params->responder_resources = 1; //指定发送队列的深度
  params->rnr_retry_count = 7; /* infinite retry */
  params->retry_count = 7;
}

int connection_event_cb(void *ctx) {
    return C_OK;
}

int connection_event_handler(struct rdma_cm_id *conn_id, int event_type, struct ConnectionEvent *conn_ev) {
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
            //printf("client: rdma addr resolved failed, call rdma_disconnect\n");
            return C_ERR;
          } else {
            rdma_resolve_route(conn_id, TIMEOUT_IN_MS);
            //printf("client: rdma addr resolved success\n");
            return C_OK;
          }
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
          //printf("RDMA_CM_EVENT_ROUTE_RESOLVED\n");
          build_params(&cm_params);
          if(rdma_connect(conn_id, &cm_params)) {
              //printf("client: rdma_route resolved failed, call rdma_disconnect\n");
              rdma_disconnect(conn_id);
              return C_ERR;
          }
          return C_OK;
    case RDMA_CM_EVENT_CONNECT_REQUEST:
          //printf("RDMA_CM_EVENT_CONNECT_REQUEST\n");
          v = conn_ev->preconnect_callback(conn_id, conn_ev);
          if(!v) {
              //printf("server: rdma_connect request failed, call rdma_reject\n");
              rdma_reject(conn_id, NULL, 0);
              rdma_destroy_id(conn_id);
              return C_ERR;
          } else {
              struct rdma_conn_param cm_params;
              build_params(&cm_params);
              ret = rdma_accept(conn_id, &cm_params);
              if (ret) {
                  //printf("server: rdma_connect request failed, call rdma_reject\n");
                  rdma_reject(conn_id, NULL, 0);
                  return C_ERR;
              }
          }
          return C_OK;
    case RDMA_CM_EVENT_ESTABLISHED:
          //printf("RDMA_CM_EVENT_ESTABLISHED\n");
          conn_ev->connected_callback(conn_id, conn_ev->ctx);
          return C_OK;
    case RDMA_CM_EVENT_DISCONNECTED:
          //printf("RDMA_CM_EVENT_DISCONNECTED\n");
          conn = (Connection *)conn_id->context;
          if (conn->conntype == 1) {//server
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
          } else {
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
          }
          return C_OK;
    case RDMA_CM_EVENT_REJECTED:
          //printf("RDMA_CM_EVENT_REJECTED\n");
          conn_ev->rejected_callback(conn_id, conn_ev->ctx);
          return C_OK;
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
          //printf("RDMA_CM_EVENT_TIMEWAIT_EXIT\n");
          conn = (Connection *)conn_id->context;
          if (conn->conntype == 2) {//client
          } else {
          }
          return C_OK;
    default:
          //printf("unknown event %d \n", event_type);
          return C_ERR;
  }
}

void *cm_thread(void *ctx) {
    struct ConnectionEvent *conn_ev = (struct ConnectionEvent*)ctx;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn_id;
    int event_type;
    int ret;

    while(1) {
        //pthread_testcancel();
        if(conn_ev->close == 1) {
            break;
        }
        ret = rdma_get_cm_event(conn_ev->cm_id->channel, &event);
        if (ret) {
            //printf("rdma_get_cm_event failed: %d", ret);
            goto error;
        }
        //printf("rdma_get_cm_event success, conn_ev->ctx(%d)",conn_ev);
        conn_id = event->id;
        event_type = event->event;

        rdma_ack_cm_event(event);
        ret = connection_event_handler(conn_id, event_type, conn_ev);
        //rdma_ack_cm_event(event);

        if(ret == C_ERR) {
            //printf("connection event handle err: %d", ret);
            goto error;
        }
    }
error:
    pthread_exit(NULL);
}


