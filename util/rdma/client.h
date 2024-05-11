#ifndef CLIENT_H
#define CLIENT_H

#include <stdio.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <infiniband/verbs.h>
#include "rdma.h"
//#include "rdma_proto.h"



int OnClientConnPreConnect(struct rdma_cm_id *id, void* ctx);

int OnClientConnConnected(struct rdma_cm_id *id, void* ctx);

int OnClientConnRejected(struct rdma_cm_id *id, void* ctx);

int OnClientConnDisconnected(struct rdma_cm_id *id, void* ctx);

struct Connection* getClientConn(struct RdmaContext *client);

struct RdmaContext* Connect(const char* ip, const char* port, char* remoteAddr);

int CloseClient(struct RdmaContext* client);


#endif