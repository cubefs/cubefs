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


int process_recv_event(connection *conn, cmd_entry *entry);

int process_send_event(connection *conn, cmd_entry *entry);

int process_write_event(connection *conn);

int process_recv_imm_event(connection *conn, cmd_entry *entry, uint32_t offset_add, uint32_t byte_len);

void process_cq_event(struct ibv_wc *wcs, int num, worker *worker);

extern void *cq_thread(void *ctx);

#endif