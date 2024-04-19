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
#include "rdma_proto.h"
//#include "rdma_proto.h"

//#include "transfer_event.h"
//#include "connection_event.h"

static const int trace = 0;
#define TRACE_PRINT(fn) if (trace) fn
#define ntohu64(v) (v)
#define htonu64(v) (v)

#define RDMA_DEFAULT_DX_SIZE  (1024*1024)
static int rdma_dx_size = RDMA_DEFAULT_DX_SIZE;

extern void DisConnectCallback(void*);

int64_t get_time_ns();

int conn_rdma_read(connection *conn, memory_entry* entry);

int conn_rdma_post_recv(connection *conn, void *block);

int conn_rdma_post_send(connection *conn, void *block, int32_t len);

void rdma_destroy_ioBuf(connection *conn);

int rdma_setup_ioBuf(connection *conn, int conn_type);

void destroy_connection(connection *conn);

connection* init_connection(uint64_t nd, int conn_type);

void destroy_conn_qp(connection *conn);

int create_conn_qp(connection *conn, struct rdma_cm_id* id);

int add_conn_to_server(connection *conn, struct rdma_listener *server);

int del_conn_from_server(connection *conn, struct rdma_listener *server);

void conn_disconnect(connection *conn);

//int DisConnect(Connection* conn, bool force);//TODO

int rdma_post_send_header(connection *conn, void* header);

int rdma_post_send_response(connection *conn, response *response);

int rdma_post_recv_header(connection* conn, void *header_ctx);

int rdma_post_recv_response(connection *conn, void *response_ctx);

int conn_app_write(connection *conn, void* buff, void *header_ctx, int32_t len);

int conn_app_send_resp(connection *conn, void* response_ctx);

void* get_data_buffer(uint32_t size, int64_t timeout_us,int64_t *ret_size);

void* get_response_buffer(connection *conn, int64_t timeout_us, int32_t *ret_size);

void* get_header_buffer(connection *conn, int64_t timeout_us, int32_t *ret_size);

memory_entry* get_recv_msg_buffer(connection *conn);

memory_entry* get_recv_response_buffer(connection *conn);

void set_conn_context(connection* conn, void* conn_context);

void set_send_timeout_us(connection* conn, int64_t timeout_us);

void set_recv_timeout_us(connection* conn, int64_t timeout_us);

int release_data_buffer(void* buff);

int release_response_buffer(connection* conn, void* buff);

int release_header_buffer(connection* conn, void* buff);

#endif