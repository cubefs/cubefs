#ifndef SERVER_H
#define SERVER_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "rdma.h"

int OnServerConnPreConnect(struct rdma_cm_id *id, void* ctx);

int OnServerConnConnected(struct rdma_cm_id *id, void* ctx);

int OnServerConnDisconnected(struct rdma_cm_id *id, void* ctx);

Connection* getServerConn(struct RdmaListener *server);

struct RdmaListener* StartServer(const char* ip, char* port, char* serverAddr);

int CloseServer(struct RdmaListener* server);



#endif