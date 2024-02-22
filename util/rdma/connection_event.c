#include "connection_event.h"

int getHeaderSize() {
    return sizeof(Header);
}

int getResponseSize() {
    return sizeof(Response);
}

int connection_compare(const void *a, const void *b, void *udata) {
    Connection *ca = a;
    Connection *cb = b;
    if(ca->cm_id > cb->cm_id) {
        return 1;
    } else if(ca->cm_id < cb->cm_id) {
        return -1;
    } else {
        return 0;
    }
}

bool connection_iter(const void *item, void *udata) {
    const Connection *conn = item;
    //printf("connn=%d (cm_id=%d)\n", conn, conn->cm_id);
    return true;
}

uint64_t connection_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const Connection *conn = item;
    return hashmap_sip(conn->cm_id, sizeof(conn->cm_id), seed0, seed1);
}

void destroy_connection(Connection *conn) {
    if(conn->pd) {
        conn->pd = NULL;
    }

    if(conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            //printf("Failed to destroy qp: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }

    if(conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            //printf("%d\n",ret);
            //printf("Failed to destroy cq: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    if(conn->comp_channel) {
        int ret = ibv_destroy_comp_channel(conn->comp_channel);
        if(ret != 0) {
            //printf("%d\n,ret");
            //printf("Failed to destroy comp channel: %s\n", strerror(errno));
            //sprintf(buffer,"Failed to destroy comp channel: %s\n", strerror(errno));
            //PrintCallback(buffer);
            //printf("Failed to destroy comp channel cleanly\n");
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
        //serverLog(LL_WARNING, "RDMA: ibv ibv query device failed");
        //printf("RDMA: ibv query device failed\n");
        //sprintf(buffer,"RDMA: ibv query device failed\n");
        //PrintCallback(buffer);
        goto error;
    }

    //((struct RdmaContext*)(conn_ev->ctx))->pd = pd; //TODO need to modify ()
    conn->pd = pd;

    //printf("min_cqe_num: %d\n",MIN_CQE_NUM);
    //sprintf(buffer,"min_cqe_num: %d\n",MIN_CQE_NUM);
    //PrintCallback(buffer);
    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!comp_channel) {
        //serverLog(LL_WARNING, "RDMA: ibv create comp channel failed");
        //printf("RDMA: ibv create comp channel failed\n");
        //sprintf(buffer,"RDMA: ibv create comp channel failed\n");
        //PrintCallback(buffer);
        goto error;
        //return C_ERR;
    }
    conn->comp_channel = comp_channel;

    //rdma_comp_vector % cm_id->verbs->num_comp_vectors
    cq = ibv_create_cq(cm_id->verbs, MIN_CQE_NUM, NULL, comp_channel, 0);//when -1, cq is null?     RDMA_MAX_WQE * 2
    if (!cq) {
        //serverLog(LL_WARNING, "RDMA: ibv create cq failed");
        //printf("RDMA: ibv create cq failed: cq:%d\n",cq);
        //sprintf(buffer,"RDMA: ibv create cq failed: cq:%d\n",cq);
        //PrintCallback(buffer);
        goto error;
        //return C_ERR;
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
        //serverLog(LL_WARNING, "RDMA: create qp failed");
        //printf("RDMA: create qp failed: %d\n",ret);
        //sprintf(buffer,"RDMA: create qp failed: %d\n",ret);
        //PrintCallback(buffer);
        goto error;
        //return C_ERR;
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


int connection_event_cb(void *ctx) {//TODO error handler
  struct ConnectionEvent *conn_ev = (struct ConnectionEvent*)ctx;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn_id;
  int event_type;


  int ret = rdma_get_cm_event(conn_ev->cm_id->channel, &event);//TODO

  if (ret < 0) {
      //TODO error handler
      //printf("rdma_get_cm_event failed:（%d：%s）。\n",errno,strerror(errno));
      //printf("rdma_get_cm_event failed: %d", ret);
      //sprintf(buffer,"rdma_get_cm_event failed: %d", ret);
      //PrintCallback(buffer);
      return C_ERR;
  }

  conn_id = event->id;
  event_type = event->event;
  if (rdma_ack_cm_event(event)) { //ack event //TODO hava problem
      //printf("ack cm event failed\n");
      //sprintf(buffer,"ack cm event failed\n");
      //PrintCallback(buffer);
      return C_ERR;
  }

  //printf("RDMA: connection event handler success status: status: 0x%x\n", event->status);
  if (!ret) {
      if (event_type == RDMA_CM_EVENT_ADDR_RESOLVED) { //client
          int v = conn_ev->preconnect_callback(conn_id, conn_ev); //创建connection
          if(!v) {
            rdma_disconnect(conn_id);//todo need verify
            //printf("client: rdma addr resolved failed, call rdma_disconnect\n");
            //sprintf(buffer,"client: rdma addr resolved failed, call rdma_disconnect\n");
            //PrintCallback(buffer);
            return;
          } else {
            rdma_resolve_route(conn_id, TIMEOUT_IN_MS);
            //printf("client: rdma addr resolved success\n");
            //sprintf(buffer,"client: rdma addr resolved success\n");
            //PrintCallback(buffer);
            return;
          }
      } else if (event_type == RDMA_CM_EVENT_ROUTE_RESOLVED) { //client
          struct rdma_conn_param cm_params;
          build_params(&cm_params);
          if(rdma_connect(conn_id, &cm_params)) {
              //TODO error handler & release resources ()
              //printf("client: rdma_route resolved failed, call rdma_disconnect\n");
              //sprintf(buffer,"client: rdma_route resolved failed, call rdma_disconnect\n");
              //PrintCallback(buffer);
              rdma_disconnect(conn_id);
              //return C_ERR;
              return;
          }
          //printf("client: rdma route resolved success\n");
          //sprintf(buffer,"client: rdma route resolved success\n");
          //PrintCallback(buffer);
          return;
      } else if (event_type == RDMA_CM_EVENT_CONNECT_REQUEST) { //server
          //build_connection(event->id, conn_ev);
          int v = conn_ev->preconnect_callback(conn_id, conn_ev);
          if(!v) {
              //printf("server: rdma_connect request failed, call rdma_reject\n");
              //sprintf(buffer,"server: rdma_connect request failed, call rdma_reject\n");
              //PrintCallback(buffer);

              rdma_reject(conn_id, NULL, 0);
              rdma_destroy_id(conn_id);

              return;
          } else {
              struct rdma_conn_param cm_params;
              build_params(&cm_params);
              ret = rdma_accept(conn_id, &cm_params);
              if (ret) {
                  //TODO error handler & release resources ()
                  //printf("server: rdma_connect request failed, call rdma_reject\n");
                  //sprintf(buffer,"server: rdma_connect request failed, call rdma_reject\n");
                  //PrintCallback(buffer);
                  rdma_reject(conn_id, NULL, 0);
                  return;
              }
          }
          //printf("server: rdma connect request success\n");
          //sprintf(buffer,"server: rdma connect request success\n");
          //PrintCallback(buffer);
          return;
      } else if (event_type == RDMA_CM_EVENT_ESTABLISHED) {
          conn_ev->connected_callback(conn_id, conn_ev->ctx);

          //printf("server and client: rdma conn established\n");
          //sprintf(buffer,"server and client: rdma conn established\n");
          //PrintCallback(buffer);
      } else if (event_type == RDMA_CM_EVENT_DISCONNECTED) { //TODO before disconnect,need to poll cqe ()
          Connection *conn = (Connection *)conn_id->context;

          if (conn->conntype == 1) {//server
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
            //printf("server: rdma conn disconnected\n");
            //sprintf(buffer,"server: rdma conn disconnected\n");
            //PrintCallback(buffer);
          } else {
            conn_ev->disconnected_callback(conn_id, conn_ev->ctx);
            //printf("client: rdma conn disconnected\n");
            //sprintf(buffer,"client: rdma conn disconnected\n");
            //PrintCallback(buffer);
          }
      } else if(event_type == RDMA_CM_EVENT_REJECTED) {//client
           conn_ev->rejected_callback(conn_id, conn_ev->ctx);
           //printf("client: rdma conn rejected\n");
           //sprintf(buffer,"client: rdma conn rejected\n");
           //PrintCallback(buffer);
      } else if(event_type == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
           Connection *conn = (Connection *)conn_id->context;
           if (conn->conntype == 2) {//client
             //printf("client: rdma conn timewait exit\n");
             //sprintf(buffer,"client: rdma conn timewait exit\n");
             //PrintCallback(buffer);
           } else {
             //printf("server: rdma conn timewait exit\n");
             //sprintf(buffer,"server: rdma conn timewait exit\n");
             //PrintCallback(buffer);
           }
      } else {
          //printf("unknown event %d \n", event_type);
          //sprintf(buffer,"unknown event %d \n", event_type);
          //PrintCallback(buffer);
      }

  }
}


