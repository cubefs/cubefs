/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_SOCKET_H__
#define __CFS_SOCKET_H__

#include "cfs_common.h"

#include "cfs_log.h"
#include "cfs_packet.h"
#include "rdma/rdma_api.h"

struct cfs_socket_pool;

enum cfs_socket_type {
	CFS_SOCK_TYPE_TCP = 0,
	CFS_SOCK_TYPE_RDMA = 1,
};
struct cfs_socket {
	struct hlist_node hash;
	struct list_head list;
	struct socket *sock;
	struct sockaddr_storage ss_dst;
	struct cfs_socket_pool *pool;
	struct cfs_buffer *tx_buffer;
	struct cfs_buffer *rx_buffer;
	unsigned long jiffies;
	struct cfs_log *log;
	struct IBVSocket* ibvsock;
	bool enable_rdma;
};

struct cfs_socket_ops {
	void (*sk_state_change)(struct sock *sk);
	void (*sk_data_ready)(struct sock *sk, int bytes);
	void (*sk_write_space)(struct sock *sk);
};

#define SOCK_POOL_BUCKET_COUNT 128
#define SOCK_POOL_LRU_INTERVAL_MS 60 * 1000u

struct cfs_socket_pool {
	struct hlist_head head[SOCK_POOL_BUCKET_COUNT];
	struct list_head lru;
	struct mutex lock;
	struct delayed_work work;
};

inline u32 hash_sockaddr_storage(const struct sockaddr_storage *addr);
int cfs_socket_create(enum cfs_socket_type type,
		      const struct sockaddr_storage *dst, struct cfs_log *log,
		      struct cfs_socket **cskp);
void cfs_socket_release(struct cfs_socket *csk, bool forever);
// void cfs_socket_set_callback(struct cfs_socket *csk,
// 			     const struct cfs_socket_ops *ops, void *private);
int cfs_socket_set_recv_timeout(struct cfs_socket *csk, u32 timeout_ms);
int cfs_socket_send(struct cfs_socket *csk, void *data, size_t len);
int cfs_socket_recv(struct cfs_socket *csk, void *data, size_t len);
int cfs_socket_send_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs);
int cfs_socket_recv_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs);
int cfs_socket_send_packet(struct cfs_socket *csk, struct cfs_packet *packet);
int cfs_socket_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet);
inline bool is_sock_valid(struct cfs_socket *sock);
int cfs_socket_module_init(void);
void cfs_socket_module_exit(void);
#endif
