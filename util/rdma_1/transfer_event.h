#include <stdio.h>
#include <string.h>
#include "rdma.h"
//#include "connection.h"
//#include "connection_event.h"
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>

//#include <infiniband/verbs.h>
#ifndef TRANSFER_EVENT_H_
#define TRANSFER_EVENT_H_


//#define ntohu64(v) (v)
#define UNUSED(x) (void)(x)

typedef void (*CompleteCb)(struct rdma_cm_id *id, void* ctx);
typedef void (*EPoolCb)(void* ctx);

extern int RecvHeaderCallback(void*, int, char*);

extern void RecvMessageCallback(void*, MemoryEntry*);

extern void EpollAddSendAndRecvEvent(int, void*);

extern void EpollDelConnEvent(int);

extern void DisConnectCallback(void*);


#define C_OK 1
#define C_ERR 0

static int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len) {//, int64_t now
    printf("handle recv\n");
    sprintf(buffer,"handle recv\n");
    PrintCallback(buffer);
    /*
    if(conn->conntype == 1) {//server
        if (byte_len != sizeof(Header)) {
            //serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
            //TODO error handler
            printf("RDMA: FATAL error, recv corrupted cmd");
            return C_ERR;
        }
    } else {
        if (byte_len != sizeof(Response)) {
            //serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
            //TODO error handler
            printf("RDMA: FATAL error, recv corrupted cmd");
            return C_ERR;
        }
    }
    */
   
    MemoryEntry* entry;

    //switch (ntohs(msg->keepalive.opcode)) {
    switch (conn->conntype) {
    case 1: //要考虑并发的问题,现在只能是串行
        printf("handleRecvHeader\n");
        sprintf(buffer,"handleRecvHeader\n");
        PrintCallback(buffer);

//readRetry: //TODO
        /*
        while(1) {
            if (conn->state == CONN_STATE_CONNECTED) {
                break;
            }
            if(conn->state == CONN_STATE_CONNECTING) {
                usleep(10);
                continue;
            }
        }
        */

        //int offset = RecvHeaderCallback(block, byte_len, "rdmaVersion");

        //printf("header:%d\n",((Header*)(block))->Opcode);
        //printf("header:%d\n",((Header*)(block))->ArgLen);
        //printf("header:%d\n",((Header*)(block))->Size);

        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->header_buff = block;
        entry->header_len = sizeof(Header);
        entry->isResponse = false;

        //struct RdmaMeta *rdmaMeta = (struct RdmaMeta*)(block + offset);
        
        int ret = connRdmaRead(conn, block, entry);//rdmaMeta
        //printf("connRdmaRead ret: %d\n",ret);
        if(ret == C_ERR) {
            //printf("connRdmaRead retry\n");
            //DisConnect(conn,true);
            return C_ERR;
            //goto readRetry;
        }
        break;

    case 2:
        printf("handleRecvResponse\n");
        sprintf(buffer,"handleRecvResponse\n");
        PrintCallback(buffer);
        //printf("%d\n",(uint64_t)(conn->dx.addr + conn->dx.pos));
        //printf("%d\n",msg->response.addr);
        //while(((uint64_t)(conn->dx.addr + conn->dx.pos)) != (msg->response.addr)) {
        //    //printf("111");
        //}
        
        //conn->dx.pos += msg->response.length;
        //TODO 需不需要先把这块内存置为零
        
        /*
        int index = (int)(((char*)ntohu64(msg->response.addr) - (char*)(conn->pool->original_mem)) / 16);

        buddy_free(conn->pool->allocation, index);//TODO 对于dataNode leader来说，收到响应不需要释放内存，对于客户端来说，收到响应需要释放内存
        buddy_dump(conn->pool->allocation);

        if(ntohs(msg->response.statusCode) == Success) {
            // success, callback
            printf("response status: success\n");
        } else {
            printf("response status: failed\n");
            // fail, callback
        }
        */

        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->response_buff = block;
        //entry->remote_buff = remote_addr;
        entry->response_len = sizeof(Response);
        entry->isResponse = true;
        //while(conn->connContext == NULL) {
        //}

        /*
        while(1) {//no need
            if(conn->state == CONN_STATE_CONNECTED) {
                break;
            }
            if(conn->state == CONN_STATE_CONNECTING) {
                usleep(10);
                continue;
            }
        }
        */

        //if(wait_event(conn->initFd) <= 0) {//TODO error handler
		//    return C_ERR;
	    //}
        //notify_event(conn->initFd,0);

        RecvMessageCallback(conn->connContext, entry);
        free(entry);

        break;

    default:
        //serverLog(LL_WARNING, "RDMA: FATAL error, unknown cmd");
        //TODO error handler
        printf("RDMA: FATAL error, unknown message\n");
        sprintf(buffer,"RDMA: FATAL error, unknown message\n");
        PrintCallback(buffer);
        return C_ERR;
    }

    //return rdmaPostRecv(conn, cm_id, block);
    return C_OK;
}

static int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len) {//, int64_t now
    //int recv_dead_line = -1;
    //if(EnQueue(conn->waitRead, entry) == NULL) {//TODO error handler
    //    printf("no more memory can be malloced\n");
    //};
   

    /*
    if(conn->recv_timeout_ns != -1 || conn->recv_timeout_ns != 0) {
        recv_dead_line = entry->recv_time + conn->recv_timeout_ns;
    }
    if(recv_dead_line != -1 && now > recv_dead_line) {
        printf("conn(%p) recv timeout, deadline:%ld, now:%ld", conn, recv_dead_line, now);
        return C_ERR;
    }
    */

    //conn->connContext == NULL

    //if(wait_event(conn->initFd) <= 0) {//TODO error handler
	//	return C_ERR;
	//}
    //notify_event(conn->initFd,0);

    RecvMessageCallback(conn->connContext, entry);
    free(entry);

//     int index = (int)((entry->addr - conn->pool->original_mem) / MEMORY_BLOCK_SIZE);
//     int ret = RecvMessageCallback(conn, entry->addr, byte_len);
//     if (!ret) {//C_ERR
//         //TODO error handler: No more data to read () maybe direct return C_ERR is correct
//         printf("RDMA: app read: conn error or closed");
//         return C_ERR;
//     }
//     assert(ret == byte_len);

//     //要考虑并发的问题,现在只能是串行
//     buddy_free(conn->pool->allocation, index);
//     buddy_dump(conn->pool->allocation);
//     //sleep(2);

// responseRetry://TODO in fact,app send response,need to modify, before send response, need to clear header and data
//     //printf("ready send response\n");
//     ret = connRdmaSendResponse(conn, entry->remote_addr);//TODO need to modify ()
//     if(!ret) {
//         printf("send response retry");
//         return C_ERR;
//         //goto responseRetry;
//     }
//     printf("send response\n");

    return C_OK;
}

/*static int connRdmaHandleRecvImm(Connection *conn, struct rdma_cm_id *cm_id, RdmaMessage *msg, uint32_t addrOffset) {

    uint32_t *base_addr = (uint32_t)conn->dx.addr + addrOffset;
    uint32_t byte_len = *(base_addr)

    assert(byte_len + addrOffset <= conn->dx.length);

    conn->dx.offset = addrOffset + byte_len;
    //TODO can read(addrOffset,conn->dx.offset)

    return rdmaPostRecv(conn, cm_id, msg);
}*/

/*static int connRdmaHandleWrite(Connection *conn, uint32_t byte_len) {
    //UNUSED(ctx);
    //UNUSED(byte_len);

    return C_OK;
}*/

static int connRdmaHandleSend(Connection *conn) {  //, RdmaMessage *msg //TODO need to modify, Non-copies cannot be overwritten until a response is received
    /* clear cmd and mark this cmd has already sent */
    
    /*
    memset(msg, 0x00, sizeof(*msg));
    if(EnQueue(conn->freeList,msg) == NULL) { //TODO error handler
        printf("no more memory can be malloced\n");
        //TODO
    }
    */

    return C_OK;
}

static int transport_sendAndRecv_event_cb(void *ctx) {
    
    //int64_t now = get_time_ns(); 
    //printf("call\n");
    Connection* conn = (Connection*)ctx;

    //conn.runningEventEpoll += 1;
    wait_group_add(&(conn->wg),1);
    
    if(conn->state != CONN_STATE_CONNECTED) {
        goto error;
    }
    struct rdma_cm_id* cm_id = conn->cm_id;
    //printf("transport data cm_id: %d\n",cm_id);
    //RdmaContext *ctx = conn->csContext;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    //struct ibv_wc wcs[RDMA_MAX_WQE];
    //RdmaMessage *msg;
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

   

    int i = 0;
    
pollcq:
    i++;
    printf("enter %d\n",i);
    sprintf(buffer,"enter %d\n",i);
    PrintCallback(buffer);
    //printf("ev_cq: %d\n",ev_cq);
    //printf("rdmaContext->cq: %d\n",((struct RdmaContext*)conn->csContext)->cq);
    
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

        //notify_event(conn->eFd,0);
        //continue;

	    //return C_OK;
        goto ok;
    }
    printf("cq num:%d\n",ret);
    sprintf(buffer,"cq num:%d\n",ret);
    PrintCallback(buffer);
    //ibv_ack_cq_events(conn->cq, 1);


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
        //printf("wc.wr_id:%d\n",wc.wr_id);
        //msg = (RdmaMessage *)wc.wr_id;
        block = wc.wr_id;
        if (connRdmaHandleRecv(conn, block, wc.byte_len) == C_ERR) {//, now
	        printf("rdma recv failed");
	        sprintf(buffer,"rdma recv failed");
            PrintCallback(buffer);
	        //return C_ERR;
            goto error;
        }
        break;

    case IBV_WC_RECV_RDMA_WITH_IMM:
        printf("ibv_wc_recv_with_imm\n");
        sprintf(buffer,"ibv_wc_recv_with_imm\n");
        PrintCallback(buffer);
        //msg = (RdmaMessage *)wc.wr_id;
        //if (connRdmaHandleRecvImm(conn, cm_id, msg, ntohl(wc.imm_data)) == C_ERR) {
        //    //rdma_conn->c.state = CONN_STATE_ERROR;
        //    //TODO error handler
        //    printf("rdma recv imm failed");
        //    return C_ERR;
        //}

        break;
    case IBV_WC_RDMA_READ:    
        printf("ibv_wc_rdma_read\n");
        sprintf(buffer,"ibv_wc_rdma_read\n");
        PrintCallback(buffer);
        entry = (MemoryEntry *)wc.wr_id;
        //char* addr = conn->pool->originMem + index * MEMORY_BLOCK_SIZE;
        if (connRdmaHandleRead(conn, entry, wc.byte_len) == C_ERR) {//, now
            //TODO error handler
            printf("rdma read failed");
            sprintf(buffer,"rdma read failed");
            PrintCallback(buffer);
            //return C_ERR;
            goto error;
        }
        break;

    case IBV_WC_RDMA_WRITE:
        printf("ibv_wc_rdma_write\n");
        sprintf(buffer,"ibv_wc_rdma_write\n");
        PrintCallback(buffer);
        //if (connRdmaHandleWrite(conn, wc.byte_len) == C_ERR) {
        //    //TODO error handler
        //    printf("rdma write failed");
        //    return C_ERR;
        //}

        break;

    case IBV_WC_SEND:
        printf("ibv_wc_send\n");
        sprintf(buffer,"ibv_wc_send\n");
        PrintCallback(buffer);
        //msg = (RdmaMessage *)wc.wr_id;
        if (connRdmaHandleSend(conn) == C_ERR) {  //,msg
	        //TODO error handler
	        printf("rdma send failed");
	        sprintf(buffer,"rdma send failed");
            PrintCallback(buffer);
	        //return C_ERR;
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
error:
    wait_group_done(&(conn->wg));
    DisConnect(conn,true);
    return C_ERR;
ok:
    wait_group_done(&(conn->wg));
    return C_OK;
}

#endif
