#include "transfer_event.h"
#include "connection.h"

int process_recv_event(connection *conn, memory_entry *entry) {
    log_debug("process recv event start");
    if (conn->conn_type == CONN_TYPE_SERVER) {
        log_debug("server conn process header recv event\n");
        if(conn->state == CONN_STATE_CONNECTING) {
            pthread_spin_lock(&conn->spin_lock);
            set_conn_state(conn, CONN_STATE_CONNECTED);
            pthread_spin_unlock(&conn->spin_lock);
        }
        int ret = conn_rdma_read(conn, entry);
        log_debug("process recv event end");
        return ret;
    } else {
        log_debug("client conn process response recv event\n");
        if(EnQueue(conn->msg_list, entry) == NULL) {
            log_debug("client conn msg list enQueue failed, no more memory can be malloced\n");
            return C_ERR;
        };
        log_debug("client conn(%p) msg list enQueue(%p) entry(%p) success, wait msg size: %d\n",conn,conn->msg_list,entry,GetSize(conn->msg_list));
        //log_debug("notify event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
        notify_event(conn->msg_fd, 0);
        log_debug("process recv event end");
        return C_OK;
    }
}

int process_send_event(connection *conn) {
    log_debug("process send event start");
    if(conn->conn_type == CONN_TYPE_CLIENT) {
        if(conn->state == CONN_STATE_CONNECTING) {
            pthread_spin_lock(&conn->spin_lock);
            set_conn_state(conn, CONN_STATE_CONNECTED);
            pthread_spin_unlock(&conn->spin_lock);
        }
    }
    log_debug("process send event end");
    return C_OK;
}

int process_read_event(connection *conn, memory_entry *entry) {
    log_debug("process read event start");
    if(EnQueue(conn->msg_list, entry) == NULL) {
        log_debug("server conn msg list enQueue failed, no more memory can be malloced\n");
        return C_ERR;
    };
    log_debug("server conn(%p) msg list enQueue(%p) entry(%p) success, wait msg size: %d\n",conn,conn->msg_list,entry,GetSize(conn->msg_list));
    //log_debug("notify event: conn(%p) msg_fd(%d)",conn,conn->msg_fd);
    notify_event(conn->msg_fd, 0);
    log_debug("process read event end");
    return C_OK;
}

void process_cq_event(struct ibv_wc *wcs, int num, worker *worker) {
    struct ibv_wc *wc = NULL;
    connection *conn = NULL;
    memory_entry *entry = NULL;
    uint64_t nd;
    int ret;
    log_debug("process cq event: num(%d)",num);
    for (int i = 0; i < num; i++) {
        wc = wcs + i;
        if(wc->opcode == IBV_WC_SEND) {
            nd = wc->wr_id;
        } else { //read and recv
            entry = (memory_entry*)wc->wr_id;
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

            case IBV_WC_RECV_RDMA_WITH_IMM:

            default:
                log_debug("not support wc->opcode:%d", wc->opcode);
                break;
        }
    }
}

void *cq_thread(void *ctx) {
    struct worker *worker = (struct worker*)ctx;
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
        log_debug("ibv_get_cq_event success");
        //ibv_ack_cq_events(worker->cq, 1);
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
        ibv_ack_cq_events(worker->cq, ret);
        log_debug("process cq event finish");
    }
error:
    //TODO
exit:
    pthread_exit(NULL);
}


