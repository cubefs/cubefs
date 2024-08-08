#ifndef RDMA_API_H_
#define RDMA_API_H_

#include <linux/module.h>
#include <linux/inet.h>
#include <linux/socket.h>
#include <linux/delay.h>
#include <linux/list.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>
#include "rdma_buffer.h"
#include "../cfs_log.h"

#define IBVSOCKET_CONN_TIMEOUT_MS 5000u
#define IBVSOCKET_RECV_TIMEOUT_MS 60000u
#define IBVSOCKET_BUFF_TIMEOUT_MS 300000u
#define BUFFER_LEN 512
#define WR_MAX_NUM 32
#define TIMEOUT_JS 5000

enum ibv_socket_conn_state {
	IBVSOCKETCONNSTATE_UNCONNECTED = 0,
	IBVSOCKETCONNSTATE_CONNECTING = 1,
	IBVSOCKETCONNSTATE_ADDRESSRESOLVED = 2,
	IBVSOCKETCONNSTATE_ROUTERESOLVED = 3,
	IBVSOCKETCONNSTATE_ESTABLISHED = 4,
	IBVSOCKETCONNSTATE_DISCONNECTED = 5,
	IBVSOCKETCONNSTATE_FAILED = 6,
	IBVSOCKETCONNSTATE_REJECTED_STALE = 7,
	IBVSOCKETCONNSTATE_DESTROYED = 8
};

struct ibv_socket {
	wait_queue_head_t event_wait_queue;
	struct rdma_cm_id *cm_id;
	struct ib_pd *pd;
	struct ib_cq *recv_cq; // recv completion queue
	struct ib_cq *send_cq; // send completion queue
	struct ib_qp *qp; // send+recv queue pair
	struct cfs_node *recv_buf[WR_MAX_NUM];
	struct list_head recv_done_list;
	atomic_t recv_count;
	struct cfs_node *send_buf[WR_MAX_NUM];
	int send_buf_index;
	struct mutex lock;
	volatile enum ibv_socket_conn_state conn_state;
	struct sockaddr_in remote_addr;
	struct cfs_log *log;
};

extern struct ibv_socket *ibv_socket_construct(struct sockaddr_in *sin, struct cfs_log *log);
extern bool ibv_socket_destruct(struct ibv_socket *this);
extern ssize_t ibv_socket_recv(struct ibv_socket *this, struct iov_iter *iter, __be64 req_id);
extern ssize_t ibv_socket_send(struct ibv_socket *this, struct iov_iter *source);
extern struct cfs_node *ibv_socket_get_data_buf(struct ibv_socket *this, size_t size);
extern void ibv_socket_free_data_buf(struct cfs_node *item);

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) < (b)) ? (b) : (a))

#endif