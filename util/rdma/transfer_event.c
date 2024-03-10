#include "transfer_event.h"



int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len) {
    MemoryEntry* entry;
    switch (conn->conntype) {
    case 1:
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
        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->response_buff = block;
        entry->response_len = sizeof(Response);
        entry->isResponse = true;
        if(EnQueue(conn->msgList, entry) == NULL) {
            //printf("conn msgList enQueue failed, no more memory can be malloced\n");
        };
        notify_event(conn->mFd, 0);
        break;
    default:
        //printf("RDMA: FATAL error, unknown message\n");
        return C_ERR;
    }
    return C_OK;
}

int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len) {
    if(EnQueue(conn->msgList, entry) == NULL) {
        printf("conn msgList enQueue failed, no more memory can be malloced\n");
    };
    //printf("conn msgList enQueue success, waitMsg size: %d\n",GetSize(conn->msgList));
    notify_event(conn->mFd, 0);
    return C_OK;
}

int connRdmaHandleSend(Connection *conn) {
    /* clear cmd and mark this cmd has already sent */
    return C_OK;
}

int transport_sendAndRecv_event_cb(void *ctx) {
    Connection* conn = (Connection*)ctx;
    wait_group_add(&(conn->wg),1);
    if(conn->state != CONN_STATE_CONNECTED) {
        goto error;
    }
    struct rdma_cm_id* cm_id = conn->cm_id;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wcs[32];
    void *block;
    MemoryEntry *entry;
    int ret;
    if (ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx) < 0) {
        //printf("RDMA: get CQ event error");
        goto error;
    } 
    ibv_ack_cq_events(conn->cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0)) {
        //printf("RDMA: notify CQ error");
        goto error;
    }
    int ne = 0;
    do {
        ne = ibv_poll_cq(conn->cq, 32, wcs);
        if (ne < 0) {
            //printf("RDMA: poll recv CQ error");
            goto error;
        } else if (ne == 0) {
            goto ok;
        }
        for (int i = 0; i < ne; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
        	    //printf("RDMA: CQ handler error status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
                goto error;
            }
            //printf("RDMA: CQ handler success status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
            switch (wcs[i].opcode) {
            case IBV_WC_RECV:
                block = wcs[i].wr_id;
                if (connRdmaHandleRecv(conn, block, wcs[i].byte_len) == C_ERR) {
                    //printf("rdma recv failed");
                    goto error;
                }
                break;

            case IBV_WC_RECV_RDMA_WITH_IMM:
                //printf("ibv_wc_recv_with_imm\n");
                break;
            case IBV_WC_RDMA_READ:
                entry = (MemoryEntry *)wcs[i].wr_id;
                if (connRdmaHandleRead(conn, entry, wcs[i].byte_len) == C_ERR) {
                    //printf("rdma read failed");
                    goto error;
                }
                break;
            case IBV_WC_RDMA_WRITE:
                //printf("ibv_wc_rdma_write\n");
                break;
            case IBV_WC_SEND:
                if (connRdmaHandleSend(conn) == C_ERR) {
                    //printf("rdma send failed");
                    goto error;
                }
                break;
            default:
                //printf("RDMA: unexpected opcode 0x[%x]\n", wcs[i].opcode);
                goto error;
            }
        }
    } while (ne);
error:
    wait_group_done(&(conn->wg));
    DisConnect(conn,true);
    return C_ERR;
ok:
    wait_group_done(&(conn->wg));
    return C_OK;
}


