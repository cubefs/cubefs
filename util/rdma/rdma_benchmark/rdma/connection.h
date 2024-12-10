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
#include <endian.h>
#include "rdma_proto.h"

static const int trace = 0;
#define TRACE_PRINT(fn) if (trace) fn
#define ntohu64(v) be64toh(v)
#define htonu64(v) htobe64(v)

int64_t get_time_ns();

int conn_rdma_post_recv(connection *conn, rdma_ctl_cmd *cmd);

int conn_rdma_post_send(connection *conn, rdma_ctl_cmd *cmd);

void rdma_destroy_ioBuf(connection *conn);

int rdma_setup_ioBuf(connection *conn);

int rdma_adjust_txBuf(connection *conn, uint32_t length);

void destroy_connection(connection *conn);

connection* init_connection(uint64_t nd, int conn_type);

void destroy_conn_qp(connection *conn);

int create_conn_qp(connection *conn, struct rdma_cm_id* id);

int add_conn_to_server(connection *conn, struct rdma_listener *server);

int del_conn_from_server(connection *conn, struct rdma_listener *server);

void conn_disconnect(connection *conn);

int rdma_exchange_rx(connection *conn);

int rdma_notify_buf_full(connection *conn);

int conn_app_write_external_buffer(connection *conn, void *buffer, uint32_t size);

int conn_app_write(connection *conn, data_entry *entry, uint32_t size);

void* get_pool_data_buffer(uint32_t size, int64_t *ret_size);

data_entry* get_conn_tx_data_buffer(connection *conn, uint32_t size);

rdma_ctl_cmd* get_cmd_buffer(connection *conn);

data_entry* get_recv_msg_buffer(connection *conn);

void set_conn_context(connection* conn, void* conn_context);

void set_send_timeout_us(connection* conn, int64_t timeout_us);

void set_recv_timeout_us(connection* conn, int64_t timeout_us);

int release_cmd_buffer(connection *conn, rdma_ctl_cmd *cmd);

int release_pool_data_buffer(connection *conn, void* buff, uint32_t size);

int release_conn_rx_data_buffer(connection *conn, data_entry *data);

int release_conn_tx_data_buffer(connection *conn, data_entry *data);

#endif