#include "transfer_event.h"



int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len) {//, int64_t now
    printf("handle recv\n");
    sprintf(buffer,"handle recv\n");
    PrintCallback(buffer);
   
    MemoryEntry* entry;

    switch (conn->conntype) {
    case 1: //要考虑并发的问题,现在只能是串行
        printf("handleRecvHeader\n");
        sprintf(buffer,"handleRecvHeader\n");
        PrintCallback(buffer);

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
        printf("handleRecvResponse\n");
        sprintf(buffer,"handleRecvResponse\n");
        PrintCallback(buffer);

        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->response_buff = block;
        entry->response_len = sizeof(Response);
        entry->isResponse = true;

        RecvMessageCallback(conn->connContext, entry);
        free(entry);

        break;

    default:
        //TODO error handler
        printf("RDMA: FATAL error, unknown message\n");
        sprintf(buffer,"RDMA: FATAL error, unknown message\n");
        PrintCallback(buffer);
        return C_ERR;
    }
    return C_OK;
}

int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len) {//, int64_t now

    RecvMessageCallback(conn->connContext, entry);
    free(entry);

    return C_OK;
}

int connRdmaHandleSend(Connection *conn) {  //, RdmaMessage *msg //TODO need to modify, Non-copies cannot be overwritten until a response is received
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
    //struct ibv_wc wc = {0};
    struct ibv_wc wcs[32];
    void *block;
    MemoryEntry *entry;
    int ret;
    if (ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx) < 0) {
        //serverLog(LL_WARNING, "RDMA: get CQ event error");
        //TODO error handler
        printf("RDMA: get CQ event error");
        sprintf(buffer,"RDMA: get CQ event error");
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
    } 
    ibv_ack_cq_events(conn->cq, 1);

    if (ibv_req_notify_cq(ev_cq, 0)) {
        //serverLog(LL_WARNING, "RDMA: notify CQ error");
        //TODO error handler
        printf("RDMA: notify CQ error");
        sprintf(buffer,"RDMA: notify CQ error");
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }

    //int i = 0;

    int ne = 0;
    do {
        ne = ibv_poll_cq(conn->cq, 32, wcs);
        if (ne < 0) {
            //TODO error handler
            printf("RDMA: poll recv CQ error");
            goto error;
        } else if (ne == 0) {
            printf("poll event ret:%d\n",ne);
            sprintf(buffer,"poll event ret:%d\n",ne);
            PrintCallback(buffer);
            goto ok;
            //continue;
        }
        for (int i = 0; i < ne; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
        	    //serverLog(LL_WARNING, "RDMA: CQ handle error status: %s[0x%x], opcode : 0x%x", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
        	    //TODO error handler
        	    printf("RDMA: CQ handler error status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
        	    sprintf(buffer,"RDMA: CQ handler error status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
                PrintCallback(buffer);
        	    //return C_ERR;
                goto error;
            }
            printf("RDMA: CQ handler success status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
            sprintf(buffer,"RDMA: CQ handler success status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
            PrintCallback(buffer);
            switch (wcs[i].opcode) {
            case IBV_WC_RECV:
                printf("ibv_wc_recv\n");
                sprintf(buffer,"ibv_wc_recv\n");
                PrintCallback(buffer);
                block = wcs[i].wr_id;
                if (connRdmaHandleRecv(conn, block, wcs[i].byte_len) == C_ERR) {//, now
                    printf("rdma recv failed");
                    sprintf(buffer,"rdma recv failed");
                    PrintCallback(buffer);
                    goto error;
                }
                break;

            case IBV_WC_RECV_RDMA_WITH_IMM:
                printf("ibv_wc_recv_with_imm\n");
                sprintf(buffer,"ibv_wc_recv_with_imm\n");
                PrintCallback(buffer);

                break;
            case IBV_WC_RDMA_READ:
                printf("ibv_wc_rdma_read\n");
                sprintf(buffer,"ibv_wc_rdma_read\n");
                PrintCallback(buffer);
                entry = (MemoryEntry *)wcs[i].wr_id;
                if (connRdmaHandleRead(conn, entry, wcs[i].byte_len) == C_ERR) {//, now
                    //TODO error handler
                    printf("rdma read failed");
                    sprintf(buffer,"rdma read failed");
                    PrintCallback(buffer);
                    goto error;
                }
                break;

            case IBV_WC_RDMA_WRITE:
                printf("ibv_wc_rdma_write\n");
                sprintf(buffer,"ibv_wc_rdma_write\n");
                PrintCallback(buffer);

                break;

            case IBV_WC_SEND:
                printf("ibv_wc_send\n");
                sprintf(buffer,"ibv_wc_send\n");
                PrintCallback(buffer);
                if (connRdmaHandleSend(conn) == C_ERR) {  //,msg
                    //TODO error handler
                    printf("rdma send failed");
                    sprintf(buffer,"rdma send failed");
                    PrintCallback(buffer);
                    goto error;
                }

                break;

            default:
                //serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
                //TODO error handler
                printf("RDMA: unexpected opcode 0x[%x]\n", wcs[i].opcode);
                sprintf(buffer,"RDMA: unexpected opcode 0x[%x]\n", wcs[i].opcode);
                PrintCallback(buffer);
                //return C_ERR;
                goto error;
            }
        }
    } while (ne);//ne
/*
pollcq:
    i++;
    printf("enter %d\n",i);
    sprintf(buffer,"enter %d\n",i);
    PrintCallback(buffer);
    
    ret = ibv_poll_cq(conn->cq, 1, &wc);
    
    if (ret < 0) {
	    //serverLog(LL_WARNING, "RDMA: poll recv CQ error");
	    //TODO error handler
	    printf("RDMA: poll recv CQ error");
        sprintf(buffer,"RDMA: poll recv CQ error");
        PrintCallback(buffer);
	    //return C_ERR;
        goto error;
    } else if (ret == 0) {
	    printf("poll event ret:%d\n",ret);
        sprintf(buffer,"poll event ret:%d\n",ret);
        PrintCallback(buffer);

        goto ok;
        //goto pollcq;
    }
    printf("cq num:%d\n",ret);
    sprintf(buffer,"cq num:%d\n",ret);
    PrintCallback(buffer);

    if (wc.status != IBV_WC_SUCCESS) {
	    //serverLog(LL_WARNING, "RDMA: CQ handle error status: %s[0x%x], opcode : 0x%x", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
	    //TODO error handler
	    printf("RDMA: CQ handler error status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
	    sprintf(buffer,"RDMA: CQ handler error status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
        PrintCallback(buffer);
	    //return C_ERR;
        goto error;
    }
    printf("RDMA: CQ handler success status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
    sprintf(buffer,"RDMA: CQ handler success status: %s[0x%x], opcpde: 0x%x\n", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
    PrintCallback(buffer);
    switch (wc.opcode) {
    case IBV_WC_RECV:
        printf("ibv_wc_recv\n");
        sprintf(buffer,"ibv_wc_recv\n");
        PrintCallback(buffer);
        block = wc.wr_id;
        if (connRdmaHandleRecv(conn, block, wc.byte_len) == C_ERR) {//, now
	        printf("rdma recv failed");
            sprintf(buffer,"rdma recv failed");
            PrintCallback(buffer);
            goto error;
        }
        break;

    case IBV_WC_RECV_RDMA_WITH_IMM:
        printf("ibv_wc_recv_with_imm\n");
        sprintf(buffer,"ibv_wc_recv_with_imm\n");
        PrintCallback(buffer);

        break;
    case IBV_WC_RDMA_READ:    
        printf("ibv_wc_rdma_read\n");
        sprintf(buffer,"ibv_wc_rdma_read\n");
        PrintCallback(buffer);
        entry = (MemoryEntry *)wc.wr_id;
        if (connRdmaHandleRead(conn, entry, wc.byte_len) == C_ERR) {//, now
            //TODO error handler
            printf("rdma read failed");
            sprintf(buffer,"rdma read failed");
            PrintCallback(buffer);
            goto error;
        }
        break;

    case IBV_WC_RDMA_WRITE:
        printf("ibv_wc_rdma_write\n");
        sprintf(buffer,"ibv_wc_rdma_write\n");
        PrintCallback(buffer);

        break;

    case IBV_WC_SEND:
        printf("ibv_wc_send\n");
        sprintf(buffer,"ibv_wc_send\n");
        PrintCallback(buffer);
        if (connRdmaHandleSend(conn) == C_ERR) {  //,msg
	        //TODO error handler
	        printf("rdma send failed");
            sprintf(buffer,"rdma send failed");
            PrintCallback(buffer);
            goto error;
        }

        break;

    default:
        //serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
        //TODO error handler
        printf("RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
        sprintf(buffer,"RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
        PrintCallback(buffer);
        //return C_ERR;
        goto error;
    }
    
    goto pollcq;
*/
error:
    wait_group_done(&(conn->wg));
    DisConnect(conn,true);
    return C_ERR;
ok:
    wait_group_done(&(conn->wg));
    return C_OK;
}


