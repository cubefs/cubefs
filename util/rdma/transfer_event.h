#ifndef TRANSFER_EVENT_H
#define TRANSFER_EVENT_H

#include <stdio.h>
#include <string.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include "rdma_proto.h"
#include "log.h"

#include "connection.h"

#define UNUSED(x) (void)(x)
//#define C_OK 1
//#define C_ERR 0


int process_recv_event(connection *conn, memory_entry *entry);

int process_send_event(connection *conn);

int process_read_event(connection *conn, memory_entry *entry);

void process_cq_event(struct ibv_wc *wcs, int num, worker *worker);

extern void *cq_thread(void *ctx);

#endif