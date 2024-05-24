#ifndef CLIENT_H
#define CLIENT_H

#include <stdio.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <infiniband/verbs.h>
#include "rdma.h"
//#include "rdma_proto.h"


struct connection* rdma_connect_by_addr(const char* ip, const char* port);

#endif