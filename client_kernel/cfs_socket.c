/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_socket.h"

#define SOCK_POOL_BUCKET_COUNT 128
#define SOCK_POOL_LRU_INTERVAL_MS 60 * 1000u
// #define DEBUG

struct cfs_socket_pool {
	struct hlist_head head[SOCK_POOL_BUCKET_COUNT];
	struct list_head lru;
	struct mutex lock;
	struct delayed_work work;
};

static struct cfs_socket_pool *sock_pool;

static inline u32 hash_sockaddr_storage(const struct sockaddr_storage *addr)
{
	const struct sockaddr_in *in;

	switch (addr->ss_family) {
	case AF_INET:
		in = (const struct sockaddr_in *)addr;
		return in->sin_addr.s_addr | in->sin_port;
	default:
		return 0;
	}
}

int cfs_socket_create(enum cfs_socket_type type,
		      const struct sockaddr_storage *ss, struct cfs_log *log,
		      struct cfs_socket **cskp)
{
	struct cfs_socket *csk;
	u32 key;
	int ret;
	int optval;

	BUG_ON(sock_pool == NULL);

	key = hash_sockaddr_storage(ss);
	mutex_lock(&sock_pool->lock);
	hash_for_each_possible(sock_pool->head, csk, hash, key) {
		if (cfs_addr_cmp(&csk->ss_dst, ss) == 0)
			break;
	}

	if (!csk) {
		mutex_unlock(&sock_pool->lock);

		csk = kzalloc(sizeof(*csk), GFP_NOFS);
		if (!csk)
			return -ENOMEM;

		memcpy(&csk->ss_dst, ss, sizeof(*ss));
#ifdef KERNEL_HAS_SOCK_CREATE_KERN_WITH_NET
		ret = sock_create_kern(&init_net, AF_INET, SOCK_STREAM,
				       IPPROTO_TCP, &csk->sock);
#else
		ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP,
				       &csk->sock);
#endif
		if (ret < 0) {
			kfree(csk);
			return ret;
		}
		csk->sock->sk->sk_allocation = GFP_NOFS;

		ret = kernel_connect(csk->sock, (struct sockaddr *)&csk->ss_dst,
				     sizeof(csk->ss_dst), 0 /*O_NONBLOCK*/);
		if (ret < 0 && ret != -EINPROGRESS) {
			sock_release(csk->sock);
			kfree(csk);
			return ret;
		}

		csk->tx_buffer = cfs_buffer_new(0);
		csk->rx_buffer = cfs_buffer_new(0);
		if (!csk->tx_buffer || !csk->rx_buffer) {
			cfs_buffer_release(csk->tx_buffer);
			cfs_buffer_release(csk->rx_buffer);
			sock_release(csk->sock);
			kfree(csk);
			return -ENOMEM;
		}

		optval = 1;
		ret = kernel_setsockopt(csk->sock, SOL_TCP, TCP_NODELAY,
					(char *)&optval, sizeof(optval));
		if (ret < 0)
			cfs_pr_warning(
				"kernel_setsockopt TCP_NODELAY error %d\n",
				ret);

		optval = 1;
		ret = kernel_setsockopt(csk->sock, SOL_SOCKET, SO_REUSEADDR,
					(char *)&optval, sizeof(optval));
		if (ret < 0)
			cfs_pr_warning(
				"kernel_setsockopt SO_REUSEADDR error %d\n",
				ret);
		csk->pool = sock_pool;
	} else {
		hash_del(&csk->hash);
		list_del(&csk->list);
		mutex_unlock(&sock_pool->lock);
	}
	csk->log = log;
	*cskp = csk;

	return 0;
}

void cfs_socket_release(struct cfs_socket *csk, bool forever)
{
	if (!csk)
		return;
	if (forever) {
		if (csk->sock)
			sock_release(csk->sock);
		cfs_buffer_release(csk->tx_buffer);
		cfs_buffer_release(csk->rx_buffer);
		kfree(csk);
	} else {
		u32 key = hash_sockaddr_storage(&csk->ss_dst);
		mutex_lock(&sock_pool->lock);
		hash_add(sock_pool->head, &csk->hash, key);
		list_add_tail(&csk->list, &sock_pool->lru);
		csk->jiffies = jiffies;
		mutex_unlock(&sock_pool->lock);
	}
}

// void cfs_socket_set_callback(struct cfs_socket *csk,
// 			     const struct cfs_socket_ops *ops, void *private)
// {
// 	csk->sock->sk->sk_user_data = private;
// 	csk->sock->sk->sk_data_ready = ops->sk_data_ready;
// 	csk->sock->sk->sk_write_space = ops->sk_write_space;
// 	csk->sock->sk->sk_state_change = ops->sk_state_change;
// }

int cfs_socket_set_recv_timeout(struct cfs_socket *csk, u32 timeout_ms)
{
	struct timeval tv;

	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	return kernel_setsockopt(csk->sock, SOL_SOCKET, SO_RCVTIMEO,
				 (char *)&tv, sizeof(tv));
}

int cfs_socket_send(struct cfs_socket *csk, void *data, size_t len)
{
	struct iovec iov = {
		.iov_base = data,
		.iov_len = len,
	};

	return cfs_socket_send_iovec(csk, &iov, 1);
}

int cfs_socket_recv(struct cfs_socket *csk, void *data, size_t len)
{
	struct iovec iov = {
		.iov_base = data,
		.iov_len = len,
	};

	return cfs_socket_recv_iovec(csk, &iov, 1);
}

int cfs_socket_send_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs)
{
	struct iov_iter ii;
	size_t len = iov_length(iov, nr_segs);
	int ret = 0;
	sigset_t blocked, oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
#ifdef KERNEL_HAS_IOV_ITER_WITH_TAG
	iov_iter_init(&ii, WRITE, iov, nr_segs, len);
#else
	iov_iter_init(&ii, iov, nr_segs, len, 0);
#endif
	while (iov_iter_count(&ii) > 0) {
		struct msghdr msghdr = {
			.msg_flags = MSG_NOSIGNAL,
		};

		ret = kernel_sendmsg(csk->sock, &msghdr, (struct kvec *)ii.iov,
				     ii.nr_segs, iov_iter_count(&ii));
		if (ret < 0)
			break;
		iov_iter_advance(&ii, ret);
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret < 0 ? ret : (int)len;
}

int cfs_socket_recv_iovec(struct cfs_socket *csk, struct iovec *iov,
			  size_t nr_segs)
{
	struct msghdr msghdr = {
		.msg_flags = MSG_WAITALL | MSG_NOSIGNAL,
	};
	size_t len = iov_length(iov, nr_segs);
	int ret;
	sigset_t blocked, oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	ret = kernel_recvmsg(csk->sock, &msghdr, (struct kvec *)iov, nr_segs,
			     len, msghdr.msg_flags);
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret;
}

static int cfs_socket_send_pages(struct cfs_socket *csk,
				 struct cfs_page_frag *frags, size_t nr)
{
	size_t i;
	sigset_t blocked, oldset;
	int ret = 0;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	for (i = 0; i < nr; i++) {
		ret = kernel_sendpage(csk->sock, frags[i].page->page,
				      frags[i].offset, frags[i].size,
				      MSG_NOSIGNAL);
		if (ret < 0)
			break;
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret;
}

static int cfs_socket_recv_pages(struct cfs_socket *csk,
				 struct cfs_page_frag *frags, size_t nr)
{
	size_t i;
	sigset_t blocked, oldset;
	int ret = 0;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	siginitsetinv(&blocked, sigmask(SIGKILL));
	sigprocmask(SIG_SETMASK, &blocked, &oldset);
	for (i = 0; i < nr; i++) {
		struct kvec vec;
		struct msghdr msghdr = {
			.msg_flags = MSG_WAITALL | MSG_NOSIGNAL,
		};

		vec.iov_base = kmap(frags[i].page->page) + frags[i].offset;
		vec.iov_len = frags[i].size;
		ret = kernel_recvmsg(csk->sock, &msghdr, &vec, 1, vec.iov_len,
				     msghdr.msg_flags);
		kunmap(frags[i].page->page);
		if (ret < 0)
			break;
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	return ret;
}

int cfs_socket_send_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	int ret = 0;

	cfs_buffer_reset(csk->tx_buffer);
	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		break;
	default:
		ret = cfs_packet_request_data_to_json(packet, csk->tx_buffer);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, invalid request data %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
		packet->request.hdr.size =
			cpu_to_be32(cfs_buffer_size(csk->tx_buffer));
	}

#ifdef DEBUG
	cfs_pr_debug(
		"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, ext_offset=%llu, "
		"kernel_offset=%llu, arglen=%u, datalen=%u, data=%.*s\n",
		csk->sock, be64_to_cpu(packet->request.hdr.req_id),
		packet->request.hdr.opcode,
		be64_to_cpu(packet->request.hdr.pid),
		be64_to_cpu(packet->request.hdr.ext_id),
		be64_to_cpu(packet->request.hdr.ext_offset),
		be64_to_cpu(packet->request.hdr.kernel_offset),
		be32_to_cpu(packet->request.hdr.arglen),
		be32_to_cpu(packet->request.hdr.size),
		(int)cfs_buffer_size(csk->tx_buffer),
		cfs_buffer_data(csk->tx_buffer));
#endif

	/* send hdr */
	ret = cfs_socket_send(csk, &packet->request.hdr,
			      sizeof(packet->request.hdr));
	if (ret < 0) {
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, send header error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
		return ret;
	}

	/* send arg */
	if (packet->request.arg) {
		ret = cfs_socket_send(csk, cfs_buffer_data(packet->request.arg),
				      cfs_buffer_size(packet->request.arg));
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, send arg error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
	}

	/* send data */
	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
		ret = cfs_socket_send(csk, &packet->request.data.ino,
				      sizeof(packet->request.data.ino));
		break;
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		ret = cfs_socket_send_pages(csk,
					    packet->request.data.write.frags,
					    packet->request.data.write.nr);
		break;
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		break;
	default:
		if (cfs_buffer_size(csk->tx_buffer) > 0)
			ret = cfs_socket_send(csk,
					      cfs_buffer_data(csk->tx_buffer),
					      cfs_buffer_size(csk->tx_buffer));
		break;
	}
	if (ret < 0)
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, send data error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
	return ret < 0 ? ret : 0;
}

int cfs_socket_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	int ret;
	u32 arglen, datalen;

	/**
	 * packet header
	 */
	ret = cfs_socket_recv(csk, &packet->reply.hdr,
			      sizeof(packet->reply.hdr));
	if (ret < 0) {
		cfs_log_error(csk->log,
			      "so(%p) id=%llu, op=0x%x, recv header error %d\n",
			      csk->sock,
			      be64_to_cpu(packet->request.hdr.req_id),
			      packet->request.hdr.opcode, ret);
		return ret;
	}

	arglen = be32_to_cpu(packet->reply.hdr.arglen);
	datalen = be32_to_cpu(packet->reply.hdr.size);

	/**
	 * packet arg
	 */
	if (arglen > 0) {
		if (packet->reply.arg) {
			ret = cfs_buffer_resize(packet->reply.arg, arglen);
		} else if (!(packet->reply.arg = cfs_buffer_new(arglen))) {
			ret = -ENOMEM;
		}

		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, alloc reply arg oom\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode);
			return ret;
		}
		ret = cfs_socket_recv(csk, cfs_buffer_data(packet->reply.arg),
				      arglen);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, recv arg(%u) error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, arglen, ret);
			return ret;
		}
		cfs_buffer_seek(packet->reply.arg, arglen);
	}

	/**
	 * packet data
	 */
	if (datalen > 0 && packet->reply.hdr.result_code == CFS_STATUS_OK &&
	    (packet->reply.hdr.opcode == CFS_OP_STREAM_READ ||
	     packet->reply.hdr.opcode == CFS_OP_STREAM_FOLLOWER_READ)) {
#ifdef DEBUG
		cfs_pr_debug(
			"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u\n",
			csk->sock, be64_to_cpu(packet->reply.hdr.req_id),
			packet->reply.hdr.opcode,
			be64_to_cpu(packet->reply.hdr.pid),
			be64_to_cpu(packet->reply.hdr.ext_id),
			packet->reply.hdr.result_code, arglen, datalen);
#endif
		/**
		 *  reply read extent message
		 */
		ret = cfs_socket_recv_pages(csk, packet->reply.data.read.frags,
					    packet->reply.data.read.nr);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, recv data(%u) error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, datalen, ret);
			return ret;
		}
	} else if (datalen > 0) {
		/**
		 *  reply other message
		 */
		cfs_buffer_reset(csk->rx_buffer);
		if (datalen > cfs_buffer_capacity(csk->rx_buffer)) {
			size_t grow_len =
				datalen - cfs_buffer_capacity(csk->rx_buffer);
			ret = cfs_buffer_grow(csk->rx_buffer, grow_len);
			if (ret < 0) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, recv data oom\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode);
				return ret;
			}
		}

		ret = cfs_socket_recv(csk, cfs_buffer_data(csk->rx_buffer),
				      datalen);
		if (ret < 0) {
			cfs_log_error(
				csk->log,
				"so(%p) id=%llu, op=0x%x, tcp recv data error %d\n",
				csk->sock,
				be64_to_cpu(packet->request.hdr.req_id),
				packet->request.hdr.opcode, ret);
			return ret;
		}
		cfs_buffer_seek(csk->rx_buffer, datalen);

		if (packet->reply.hdr.result_code == CFS_STATUS_OK) {
			struct cfs_json *json;
#ifdef DEBUG
			cfs_pr_debug(
				"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u, data=%.*s\n",
				csk->sock,
				be64_to_cpu(packet->reply.hdr.req_id),
				packet->reply.hdr.opcode,
				be64_to_cpu(packet->reply.hdr.pid),
				be64_to_cpu(packet->reply.hdr.ext_id),
				packet->reply.hdr.result_code, arglen, datalen,
				(int)cfs_buffer_size(csk->rx_buffer),
				cfs_buffer_data(csk->rx_buffer));
#endif
			/**
			 *  reply ok message
			 */
			json = cfs_json_parse(cfs_buffer_data(csk->rx_buffer),
					      cfs_buffer_size(csk->rx_buffer));
			if (!json) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, invliad json\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode);
				return -EBADMSG;
			}

			ret = cfs_packet_reply_data_from_json(json, packet);
			if (ret < 0) {
				cfs_log_error(
					csk->log,
					"so(%p) id=%llu, op=0x%x, parse json error %d\n",
					csk->sock,
					be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode, ret);
				ret = -EBADMSG;
			}
			cfs_json_release(json);
			if (ret < 0)
				return ret;
		} else {
			/**
			 *  reply error message
			 */
			cfs_log_warn(
				csk->log,
				"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, from=%s, data=%.*s\n",
				csk->sock,
				be64_to_cpu(packet->reply.hdr.req_id),
				packet->reply.hdr.opcode,
				be64_to_cpu(packet->reply.hdr.pid),
				be64_to_cpu(packet->reply.hdr.ext_id),
				packet->reply.hdr.result_code,
				cfs_pr_addr(&csk->ss_dst),
				(int)cfs_buffer_size(csk->rx_buffer),
				cfs_buffer_data(csk->rx_buffer));
		}
	} else {
#ifdef DEBUG
		cfs_pr_debug(
			"so(%p) id=%llu, op=0x%x, pid=%llu, ext_id=%llu, rc=0x%x, arglen=%u, datalen=%u\n",
			csk->sock, be64_to_cpu(packet->reply.hdr.req_id),
			packet->reply.hdr.opcode,
			be64_to_cpu(packet->reply.hdr.pid),
			be64_to_cpu(packet->reply.hdr.ext_id),
			packet->reply.hdr.result_code, arglen, datalen);
#endif
	}

	return ret < 0 ? ret : 0;
}

static inline bool is_sock_valid(struct cfs_socket *sock)
{
	return sock->jiffies + msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS) >
	       jiffies;
}

static void socket_pool_lru_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_socket *sock;
	struct cfs_socket *tmp;

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	mutex_lock(&sock_pool->lock);
	list_for_each_entry_safe(sock, tmp, &sock_pool->lru, list) {
		if (is_sock_valid(sock))
			break;
		hash_del(&sock->hash);
		list_del(&sock->list);
		cfs_socket_release(sock, true);
	}
	mutex_unlock(&sock_pool->lock);
}

int cfs_socket_module_init(void)
{
	if (sock_pool)
		return 0;
	sock_pool = kzalloc(sizeof(*sock_pool), GFP_KERNEL);
	if (!sock_pool)
		return -ENOMEM;
	hash_init(sock_pool->head);
	INIT_LIST_HEAD(&sock_pool->lru);
	mutex_init(&sock_pool->lock);
	INIT_DELAYED_WORK(&sock_pool->work, socket_pool_lru_work_cb);
	schedule_delayed_work(&sock_pool->work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	return 0;
}

void cfs_socket_module_exit(void)
{
	struct cfs_socket *sock;
	struct hlist_node *tmp;
	int i;

	if (!sock_pool)
		return;
	cancel_delayed_work_sync(&sock_pool->work);
	hash_for_each_safe(sock_pool->head, i, tmp, sock, hash) {
		hash_del(&sock->hash);
		cfs_socket_release(sock, true);
	}
	mutex_destroy(&sock_pool->lock);
	kfree(sock_pool);
	sock_pool = NULL;
}
