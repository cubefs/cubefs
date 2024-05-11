#ifndef CONNECTION_H
#define CONNECTION_H

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
//#include "wait_group.h"
#include "rdma_pool.h"
//#include "rdma_proto.h"


static const int trace = 0;
#define TRACE_PRINT(fn) if (trace) fn

#define RDMA_DEFAULT_DX_SIZE  (1024*1024)
static int rdma_dx_size = RDMA_DEFAULT_DX_SIZE;


#define ntohu64(v) (v)
#define htonu64(v) (v)

int64_t get_time_ns();

int rdmaPostRecv(Connection *conn, void *block);

void *page_aligned_zalloc(size_t size);

void rdmaDestroyIoBuf(Connection *conn);

int rdmaSetupIoBuf(Connection *conn, struct ConnectionEvent *conn_ev, int conntype);

Connection* AllocConnection(struct rdma_cm_id *cm_id, struct ConnectionEvent *conn_ev, int conntype);

int UpdateConnection(Connection* conn);

int ReConnect(Connection* conn);

int DisConnect(Connection* conn, bool force);

int rdmaSendCommand(Connection *conn, void *block, int32_t len);

int connRdmaSendHeader(Connection *conn, void* header, int32_t len);

int connRdmaSendResponse(Connection *conn, Response *response, int32_t len);

int rdmaPostRecvHeader(Connection *conn, void *headerCtx);

int rdmaPostRecvResponse(Connection *conn, void *responseCtx);

void* getDataBuffer(uint32_t size, int64_t timeout_us,int64_t *ret_size);

void* getResponseBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size);

void* getHeaderBuffer(Connection *conn, int64_t timeout_us, int32_t *ret_size);

void setConnContext(Connection* conn, void* connContext);

void setSendTimeoutUs(Connection* conn, int64_t timeout_us);

void setRecvTimeoutUs(Connection* conn, int64_t timeout_us);

int releaseDataBuffer(void* buff);

int releaseResponseBuffer(Connection* conn, void* buff);

int releaseHeaderBuffer(Connection* conn, void* buff);

int connAppWrite(Connection *conn, void* buff, void *headerCtx, int32_t len);

int connAppSendResp(Connection *conn, void* responseCtx, int32_t len);

int RdmaRead(Connection *conn, Header *header, MemoryEntry* entry);

int connRdmaRead(Connection *conn, void *block, MemoryEntry *entry);

#endif