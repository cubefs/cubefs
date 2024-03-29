#ifndef CONNECTION_EVENT_H
#define CONNECTION_EVENT_H

//#include "wait_group.h"
//#include "queue.h"
#include "hashmap.h"
//#include "rdma_proto.h"
#include "rdma_pool.h"

#include "epoll.h"

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#define C_OK 1
#define C_ERR 0

static int rdma_comp_vector = -1; /* -1 means a random one */


int getHeaderSize();

int getResponseSize();

void destroy_connection(Connection *conn);

int build_connection(struct ConnectionEvent *conn_ev, Connection *conn);

void build_params(struct rdma_conn_param *params);

int connection_event_cb(void *ctx);

void *cm_thread(void *ctx);

#endif