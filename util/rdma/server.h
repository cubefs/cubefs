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

connection* get_rdma_server_conn(struct rdma_listener *server);

struct rdma_listener* start_rdma_server_by_addr(char* ip, char* port);

void close_rdma_server(struct rdma_listener* server);

#endif