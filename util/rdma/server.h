#ifndef SERVER_H
#define SERVER_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdlib.h>

#include <unistd.h>

#include "rdma.h"
//#include "rdma_proto.h"

int OnServerConnPreConnect(struct rdma_cm_id *id, void* ctx);

int OnServerConnConnected(struct rdma_cm_id *id, void* ctx);

int OnServerConnDisconnected(struct rdma_cm_id *id, void* ctx);

Connection* getServerConn(struct RdmaListener *server);

struct RdmaListener* StartServer(const char* ip, uint16_t port, char* serverAddr);

int CloseServer(struct RdmaListener* server);



#endif