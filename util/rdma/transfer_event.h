#ifndef TRANSFER_EVENT_H
#define TRANSFER_EVENT_H

#include <stdio.h>
#include <string.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include "rdma_proto.h"

#define UNUSED(x) (void)(x)
#define C_OK 1
#define C_ERR 0


typedef void (*CompleteCb)(struct rdma_cm_id *id, void* ctx);
typedef void (*EPoolCb)(void* ctx);

extern int RecvHeaderCallback(void*, int, char*);

extern void RecvMessageCallback(void*, MemoryEntry*);

extern void EpollAddSendAndRecvEvent(int, void*);

extern void EpollDelConnEvent(int);

extern void DisConnectCallback(void*);

int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len);

int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len);

int connRdmaHandleSend(Connection *conn);

int transport_sendAndRecv_event_cb(void *ctx);

void *cq_thread(void *ctx);

#endif