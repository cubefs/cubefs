#include "transfer_event.h"
#include "connection.h"

int process_recv_event(connection *conn, cmd_entry *entry) {
    rdma_ctl_cmd *cmd = entry->cmd;
    switch (ntohs(cmd->memory.opcode)) {
    case EXCHANGE_MEMORY:
        conn->remote_rx_addr = (char*)ntohu64(cmd->memory.addr);
        conn->tx->length = ntohl(cmd->memory.length);
        conn->remote_rx_key = ntohl(cmd->memory.key);
        conn->tx->offset = 0;
        conn->tx_full_offset = 0;
        rdma_adjust_txBuf(conn, conn->tx->length);//TODO error handle

        int state = get_conn_state(conn);
        if(state == CONN_STATE_CONNECTING) {//first time to exchange memory
            set_conn_state(conn, CONN_STATE_CONNECTED);
            if (conn->conn_type == CONN_TYPE_SERVER) {
                struct rdma_listener *server = (struct rdma_listener*)conn->context;
                pthread_spin_lock(&(server->wait_conns_lock));
                if(EnQueue(server->wait_conns,conn) == NULL) {
                    pthread_spin_unlock(&(server->wait_conns_lock));
                    log_error("server(%lu-%p) wait conns has no more memory can be malloced", server->nd, server);
                    return C_ERR;
                }
                pthread_spin_unlock(&(server->wait_conns_lock));
                notify_event(server->connect_fd, 0);
            } else {
                notify_event(conn->connect_fd, 0);
            }
        }

        break;
    case NOTIFY_FULLBUF:
        conn->rx_full_offset = ntohl(cmd->full_msg.tx_full_offset);
        if (conn->rx->pos == conn->rx->offset && conn->rx_full_offset == conn->rx->pos) {
            int ret = rdma_exchange_rx(conn);//TODO error handler
            if (ret == C_ERR) {
                log_error("conn(%lu-%p) process recv event failed: exchange rx return error");
                return C_ERR;
            }
        }
        break;
    default:
        log_error("unknown cmd");
        return C_ERR;
    }
    return conn_rdma_post_recv(conn, cmd);
}

int process_send_event(connection *conn, cmd_entry *entry) {
    rdma_ctl_cmd *cmd = entry->cmd;
    memset(cmd, 0x00, sizeof(rdma_ctl_cmd));
    int ret = release_cmd_buffer(conn, entry->cmd);
    return ret;
}

int process_write_event(connection *conn) {
    return C_OK;
}

int process_recv_imm_event(connection *conn, cmd_entry *entry, uint32_t offset_add, uint32_t byte_len) {
    rdma_ctl_cmd *cmd = entry->cmd;
    //pthread_spin_lock(&conn->spin_lock);

    if (conn->rx->offset + offset_add > conn->rx->length) {
        conn->rx->offset = 0;
    }
    conn->rx->offset += offset_add;

    //assert(offset_add + conn->rx->offset <= conn->rx->length);
    //conn->rx->offset += offset_add;
    log_debug("conn(%lu-%p) rx start(%u) end(%u)", conn->nd, conn, conn->rx->offset - offset_add, conn->rx->offset);
    data_entry *msg = (data_entry*)malloc(sizeof(data_entry));
    if (msg == NULL) {
        //pthread_spin_unlock(&conn->spin_lock);
        log_error("conn(%lu-%p) malloc data entry failed", conn->nd, conn);
        return C_ERR;
    }
    msg->addr = conn->rx->addr + conn->rx->offset - offset_add;
    msg->data_len = byte_len;
    msg->mem_len = offset_add;
    //pthread_spin_unlock(&conn->spin_lock);
    pthread_spin_lock(&(conn->msg_list_lock));
    if (EnQueue(conn->msg_list, msg) == NULL) {
        pthread_spin_unlock(&(conn->msg_list_lock));
        log_error("conn(%lu-%p) msg list enQueue failed, no more memory can be malloced", conn->nd, conn);
        return C_ERR;
    }
    pthread_spin_unlock(&(conn->msg_list_lock));
    log_debug("conn(%lu-%p) msg list enQueue(%p) msg(%p) success, wait msg size: %d", conn->nd, conn, conn->msg_list, msg, GetSize(conn->msg_list));
    notify_event(conn->msg_fd, 0);
    return conn_rdma_post_recv(conn, cmd);
}

void process_cq_event(struct ibv_wc *wcs, int num, worker *worker) {
    struct ibv_wc *wc = NULL;
    connection *conn = NULL;
    cmd_entry *entry = NULL;
    uint64_t nd;
    int ret;
    if(num > 0) {
        log_debug("process cq event: num(%d)",num);
    }
    for (int i = 0; i < num; i++) {
        wc = wcs + i;
        if(wc->opcode == IBV_WC_RDMA_WRITE) {
            nd = wc->wr_id;
        } else { //send and recv
            entry = (cmd_entry*)wc->wr_id;
            nd = entry->nd;
        }
        conn = (connection*)hashmap_get((worker)->nd_map, nd);
        if (conn == NULL) {
            continue;
        }
        int state = get_conn_state(conn);
        if (state != CONN_STATE_CONNECTED && state != CONN_STATE_CONNECTING) { //在使用之前需要判断连接的状态
            //log_error("conn(%lu-%p) process cq event failed: conn state is not connected: state(%d)", conn->nd, conn, state);
            //already closed, no need deal
            continue;
        }
        if (wc->status != IBV_WC_SUCCESS) {
            log_error("conn:(%lu-%p) failed: %d %d %d %s", conn->nd, conn, wc->opcode, wc->byte_len, wc->status, ibv_wc_status_str(wc->status));
            set_conn_state(conn, CONN_STATE_ERROR);
            rdma_disconnect(conn->cm_id);
            //conn_disconnect(conn);
            //del_conn_from_worker(conn->nd, worker, worker->nd_map);
            //add_conn_to_worker(conn, worker, worker->closing_nd_map);
            continue;
        }
        log_debug("op code:%d, status:%d, %d", wc->opcode, wc->status, wc->byte_len);
        switch (wc->opcode) {
            case IBV_WC_RECV: //128
                ret = process_recv_event(conn, entry);
                if (ret == C_ERR) {
                    log_error("process recv event failed");
                    if (state == CONN_STATE_CONNECTED) {
                        set_conn_state(conn, CONN_STATE_ERROR);
                    }
                    rdma_disconnect(conn->cm_id);
                    //conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                free(entry);
                break;
            case IBV_WC_SEND:
                ret = process_send_event(conn, entry);
                if (ret == C_ERR) {
                    log_error("process send event failed");
                    set_conn_state(conn, CONN_STATE_ERROR);
                    rdma_disconnect(conn->cm_id);
                    //conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                free(entry);
                break;
            case IBV_WC_RDMA_WRITE:
                ret = process_write_event(conn);
                if (ret == C_ERR) {
                    log_error("process write event failed");
                    set_conn_state(conn, CONN_STATE_ERROR);
                    rdma_disconnect(conn->cm_id);
                    //conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                break;
            case IBV_WC_RECV_RDMA_WITH_IMM:
                ret = process_recv_imm_event(conn, entry, ntohl(wc->imm_data), wc->byte_len);
                if (ret == C_ERR) {
                    log_error("process recv imm event failed");
                    set_conn_state(conn, CONN_STATE_ERROR);
                    rdma_disconnect(conn->cm_id);
                    //conn_disconnect(conn);
                    //del_conn_from_worker(conn->nd, worker, worker->nd_map);
                    //add_conn_to_worker(conn, worker, worker->closing_nd_map);
                }
                free(entry);
                break;
            case IBV_WC_RDMA_READ:

            default:
                log_error("not support wc->opcode:%d", wc->opcode);
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

    memset(wcs, 0 , 32 * sizeof(struct ibv_wc));
    while(1) {
        if (worker->close == 1) {
            goto exit;
        }
        /*
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
        */
        ret = ibv_poll_cq(worker->cq, 32, wcs);
        if (ret < 0) {
            log_error("ibv poll cq failed: %d", ret);
            goto error;
        }
        process_cq_event(wcs, ret, worker);
        //ibv_ack_cq_events(worker->cq, ret);
        //log_debug("process cq event finish");
    }
error:
    //TODO
exit:
    pthread_exit(NULL);
}


