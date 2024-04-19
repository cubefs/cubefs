#include "transfer_event.h"
#include "connection.h"

/*
int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len) {
    MemoryEntry* entry;
    switch (conn->conntype) {
    case 1:
        log_debug("conn rdma handle recv header\n");
        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->header_buff = block;
        entry->header_len = sizeof(Header);
        entry->isResponse = false;
        int ret = connRdmaRead(conn, block, entry);//rdmaMeta
        if(ret == C_ERR) {
            return C_ERR;
        }
        break;
    case 2:
        log_debug("conn rdma handle recv response\n");
        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->response_buff = block;
        entry->response_len = sizeof(Response);
        entry->isResponse = true;
        if(EnQueue(conn->msgList, entry) == NULL) {
            log_debug("conn msgList enQueue failed, no more memory can be malloced\n");
            //printf("conn msgList enQueue failed, no more memory can be malloced\n");
        };
        notify_event(conn->mFd, 0);
        break;
    default:
        log_debug("RDMA: FATAL error, unknown message\n");
        //printf("RDMA: FATAL error, unknown message\n");
        return C_ERR;
    }
    return C_OK;
}

int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len) {
    if(EnQueue(conn->msgList, entry) == NULL) {
        log_debug("conn msgList enQueue failed, no more memory can be malloced\n");
        //printf("conn msgList enQueue failed, no more memory can be malloced\n");
    };
    //printf("conn msgList enQueue success, waitMsg size: %d\n",GetSize(conn->msgList));
    log_debug("conn msgList enQueue success, waitMsg size: %d\n",GetSize(conn->msgList));
    notify_event(conn->mFd, 0);
    return C_OK;
}

int connRdmaHandleSend(Connection *conn) {
    //clear cmd and mark this cmd has already sent
    return C_OK;
}
int transport_sendAndRecv_event_cb(void *ctx) {
    return C_OK;
}

int transport_sendAndRecv_event_handler(Connection *conn) {

    wait_group_add(&(conn->wg),1);
    if(conn->state != CONN_STATE_CONNECTED) {
        goto error;
    }
    struct rdma_cm_id* cm_id = conn->cm_id;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    //struct ibv_wc wcs[32];
    struct ibv_wc wc;
    void *block;
    MemoryEntry *entry;
    int ret;

    while((ret = ibv_poll_cq(conn->cq, 1 ,&wc)) == 1) {
        ret = 0;
        if(wc.status != IBV_WC_SUCCESS) {
            goto error;
        }
        switch (wc.opcode) {
          case IBV_WC_RECV:
              log_debug("ibv_wc_recv\n");
              block = (void *)wc.wr_id;
              if (connRdmaHandleRecv(conn, block, wc.byte_len) == C_ERR) {
                  log_debug("rdma recv failed\n");
                  //printf("rdma recv failed");
                  goto error;
              }
              break;

          case IBV_WC_RECV_RDMA_WITH_IMM:
              log_debug("ibv_wc_recv_with_imm\n");
              //printf("ibv_wc_recv_with_imm\n");
              break;
          case IBV_WC_RDMA_READ:
              log_debug("ibv_wc_rdma_read\n");
              entry = (MemoryEntry *)wc.wr_id;
              if (connRdmaHandleRead(conn, entry, wc.byte_len) == C_ERR) {
                  log_debug("rdma read failed\n");
                  //printf("rdma read failed");
                  goto error;
              }
              break;
          case IBV_WC_RDMA_WRITE:
              log_debug("ibv_wc_rdma_write\n");
              //printf("ibv_wc_rdma_write\n");
              break;
          case IBV_WC_SEND:
              log_debug("ibv_wc_send");
              if (connRdmaHandleSend(conn) == C_ERR) {
                  log_debug("rdma send failed\n");
                  //printf("rdma send failed");
                  goto error;
              }
              break;
          default:
              log_debug("RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
              //printf("RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
              goto error;
          }
    }
    if(ret) {
        goto error;
    }
    goto ok;

error:
    wait_group_done(&(conn->wg));
    //DisConnect(conn,true);
    return C_ERR;
ok:
    wait_group_done(&(conn->wg));
    return C_OK;
}
*/

int process_recv_event(connection *conn, memory_entry *entry) {
    if (conn->conn_type == CONN_TYPE_SERVER) {
        log_debug("server conn process header recv event\n");
        if(conn->state == CONN_STATE_CONNECTING) {
            pthread_spin_lock(&conn->spin_lock);
            set_conn_state(conn, CONN_STATE_CONNECTED);
            pthread_spin_unlock(&conn->spin_lock);
        }
        return conn_rdma_read(conn, entry);
    } else {
        log_debug("client conn process response recv event\n");
        if(EnQueue(conn->msg_list, entry) == NULL) {
            log_debug("client conn msg list enQueue failed, no more memory can be malloced\n");
            return C_ERR;
        };
        log_debug("client conn(%p) msg list enQueue(%p) entry(%p) success, wait msg size: %d\n",conn,conn->msg_list,entry,GetSize(conn->msg_list));
        log_debug("notify event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
        notify_event(conn->msg_fd, 0);
        return C_OK;
    }
}

int process_send_event(connection *conn) {
    if(conn->conn_type == CONN_TYPE_CLIENT) {
        if(conn->state == CONN_STATE_CONNECTING) {
            pthread_spin_lock(&conn->spin_lock);
            set_conn_state(conn, CONN_STATE_CONNECTED);
            pthread_spin_unlock(&conn->spin_lock);
        }
    }
    return C_OK;
}

int process_read_event(connection *conn, memory_entry *entry) {
    if(EnQueue(conn->msg_list, entry) == NULL) {
        log_debug("server conn msg list enQueue failed, no more memory can be malloced\n");
        return C_ERR;
    };
    log_debug("server conn(%p) msg list enQueue(%p) entry(%p) success, wait msg size: %d\n",conn,conn->msg_list,entry,GetSize(conn->msg_list));
    log_debug("notify event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
    notify_event(conn->msg_fd, 0);
    return C_OK;
}

void process_cq_event(struct ibv_wc *wcs, int num, worker *worker) {
    struct ibv_wc *wc = NULL;
    connection *conn = NULL;
    memory_entry *entry = NULL;
    uint64_t nd;
    int ret;
    for (int i = 0; i < num; i++) {
        wc = wcs + i;
        if(wc->opcode == IBV_WC_SEND) {
            nd = wc->wr_id;
        } else { //read and recv
            entry = (memory_entry*)wc->wr_id;
            //log_debug("read or recv: entry: %p",entry);
            //log_debug("entry->header_buff:%p",entry->header_buff);
            //log_debug("entry->header_len:%d",entry->header_len);
            //log_debug("entry->is_response:%d",entry->is_response);
            //log_debug("entry->nd:%d",entry->nd);
            nd = entry->nd;
        }
        conn = (connection*)hashmap_get((worker)->nd_map, nd);
        if (conn == NULL) {
            continue;
        }
        if (wc->status != IBV_WC_SUCCESS) {
            log_debug("conn:(%lu-%p) failed:%d %s", conn->nd, conn, wc->status, ibv_wc_status_str(wc->status));
            conn_disconnect(conn);
            //del_conn_from_worker(conn->nd, worker, worker->nd_map);
            //add_conn_to_worker(conn, worker, worker->closing_nd_map);
            continue;
        }
        log_debug("op code:%d, status:%d, %d", wc->opcode, wc->status, wc->byte_len);
        switch (wc->opcode) {
            case IBV_WC_RECV: //128
                ret = process_recv_event(conn, entry);
                if (ret == C_ERR) {
                    free(entry);
                    conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                break;
            case IBV_WC_SEND:
                ret = process_send_event(conn);
                if (ret == C_ERR) {
                    conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                break;
            case IBV_WC_RDMA_READ:
                ret = process_read_event(conn, entry);
                if (ret == C_ERR) {
                    free(entry);
                    conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                break;
            case IBV_WC_RDMA_WRITE:
                /*
                ret = process_write_event(conn, entry, wc.byte_len);
                if (ret == 0) {
                    conn_disconnect(conn);
                    del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    add_conn_to_worker(conn->nd, worker, worker->closing_nd_map);
                }
                break;
                */
            case IBV_WC_RECV_RDMA_WITH_IMM:
                /*
                ret = process_write_with_imm_event(conn, entry, wc.byte_len);
                if (ret == 0) {
                    conn_disconnect(conn);
                    del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    add_conn_to_worker(conn->nd, worker, worker->closing_nd_map);
                }
                break;
                */
            default:
                log_debug("not support wc->opcode:%d", wc->opcode);
                break;
        }
    }
}

void *cq_thread(void *ctx) {
    struct worker *worker = (struct worker*)ctx;
    //struct ibv_cq *cq = worker->cq;
    int ret = 0;
    struct ibv_wc wcs[32];
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    if (worker->w_pid == 0) {
        worker->w_pid = pthread_self();
    }

    memset(wcs, 0 ,sizeof(wcs));
    while(1) {
        if (worker->close == 1) {
            goto exit;
        }
        //log_debug("cq_thread: work comp channel:%p", worker->comp_channel);
        ret = ibv_get_cq_event(worker->comp_channel, &ev_cq, &ev_ctx);
        if(ret != 0) {
            log_debug("ibv get cq event error\n");
            goto error;
        }
        ibv_ack_cq_events(worker->cq, 1);
        ret = ibv_req_notify_cq(worker->cq, 0);
        if (ret != 0) {
            log_debug("ibv req notify cq error\n");
            goto error;
        }

        ret = ibv_poll_cq(worker->cq, 32, wcs);
        if (ret < 0) {
            log_debug("ibv poll cq failed: %d", ret);
            goto error;
        }
        process_cq_event(wcs, ret, worker);
    }
error:
    //TODO
exit:
    pthread_exit(NULL);
}


