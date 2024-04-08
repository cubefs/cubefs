#include "cfs_rdma_socket.h"
#include "cfs_log.h"

static struct cfs_socket_pool *rdma_sock_pool;

char* parse_sinaddr(const struct in_addr saddr)
{ 
    static char ip_str[16];
    int printed_bytes;
	u32 val;

	printed_bytes = 0;
    memset(ip_str, 0, sizeof(ip_str));
	val = ntohl(saddr.s_addr);

    printed_bytes = snprintf(ip_str, sizeof(ip_str), "%d.%d.%d.%d", (val >> 24) &0xff, (val >> 16) & 0xff, (val >> 8 ) & 0xff, val & 0xff);

    if (printed_bytes > sizeof(ip_str))
		return NULL;

    return ip_str;
}

int cfs_rdma_create(struct sockaddr_storage *ss, struct cfs_log *log,
		    struct cfs_socket **cskp, u32 rdma_port)
{
	struct cfs_socket *csk;
	u32 key;
	struct sockaddr_in dst_addr;

	BUG_ON(rdma_sock_pool == NULL);

	key = hash_sockaddr_storage(ss);
	mutex_lock(&rdma_sock_pool->lock);
	hash_for_each_possible(rdma_sock_pool->head, csk, hash, key) {
		if (cfs_addr_cmp(&csk->ss_dst, ss) == 0)
			break;
	}

	if (!csk) {
		mutex_unlock(&rdma_sock_pool->lock);

		csk = kzalloc(sizeof(*csk), GFP_NOFS);
		if (!csk)
			return -ENOMEM;
		memcpy(&csk->ss_dst, ss, sizeof(*ss));

		csk->tx_buffer = cfs_buffer_new(0);
		csk->rx_buffer = cfs_buffer_new(0);
		if (!csk->tx_buffer || !csk->rx_buffer) {
			cfs_buffer_release(csk->tx_buffer);
			cfs_buffer_release(csk->rx_buffer);
			kfree(csk);
			return -ENOMEM;
		}

		// replace the port with rdma.
		dst_addr.sin_family = AF_INET;
		dst_addr.sin_port = htons(rdma_port);
		dst_addr.sin_addr.s_addr = ((struct sockaddr_in *)ss)->sin_addr.s_addr;
		//dst_addr.sin_addr.s_addr = in_aton("127.0.0.1");
		csk->ibvsock = IBVSocket_construct(&dst_addr);
		if (IS_ERR(csk->ibvsock)) {
			cfs_pr_err("failed to connect to %s:%hu\n", parse_sinaddr(dst_addr.sin_addr), rdma_port);
			cfs_buffer_release(csk->tx_buffer);
			cfs_buffer_release(csk->rx_buffer);
			kfree(csk);
			return -EIO;
		}

		csk->pool = rdma_sock_pool;
		csk->enable_rdma = true;
	} else {
		hash_del(&csk->hash);
		list_del(&csk->list);
		mutex_unlock(&rdma_sock_pool->lock);
	}

	csk->log = log;
	*cskp = csk;

	return 0;
}

void cfs_rdma_release(struct cfs_socket *csk, bool forever)
{
	if (!csk)
		return;
	if (forever) {
		if (csk->ibvsock)
			IBVSocket_destruct(csk->ibvsock);
		cfs_buffer_release(csk->tx_buffer);
		cfs_buffer_release(csk->rx_buffer);
		kfree(csk);
	} else {
		u32 key = hash_sockaddr_storage(&csk->ss_dst);
		mutex_lock(&rdma_sock_pool->lock);
		hash_add(rdma_sock_pool->head, &csk->hash, key);
		list_add_tail(&csk->list, &rdma_sock_pool->lru);
		csk->jiffies = jiffies;
		mutex_unlock(&rdma_sock_pool->lock);
	}
}

static int cfs_rdma_pages_to_buffer(struct cfs_socket *csk, struct cfs_packet *packet) {
	struct cfs_page_frag *frags;
	struct BufferItem *pDataBuf = NULL;
	char *pStart = NULL;
	ssize_t count = 0;
	ssize_t len = 0;
	int index = 0;
	int i;

	frags = packet->request.data.write.frags;
	count = 0;
	for (i = 0; i < packet->request.data.write.nr; i++) {
		count += frags[i].size;
	}
	index = IBVSocket_get_data_buf(csk->ibvsock, count);
	if (index < 0) {
		printk("failed to allocate data buffer. size=%ld\n", count);
		return -ENOMEM;
	}
	packet->data_buf_index = index;
	pDataBuf = &csk->ibvsock->data_buf[index];
	// copy the data into data buffer.
	len = 0;
	for (i = 0; i < packet->request.data.write.nr; i++) {
		pStart = pDataBuf->pBuff+len;
		memcpy(pStart, kmap(frags[i].page->page) + frags[i].offset, frags[i].size);
		kunmap(frags[i].page->page);
		len += frags[i].size;
	}
	packet->request.hdr_padding.RdmaAddr = pDataBuf->dma_addr;
	packet->request.hdr_padding.RdmaLength = htonl(count);

	return 0;
}

static int cfs_rdma_iter_to_buffer(struct cfs_socket *csk, struct cfs_packet *packet) {
	struct BufferItem *pDataBuf = NULL;
	ssize_t size = 0;
	int index = 0;

	size = be32_to_cpu(packet->request.hdr.size);
	index = IBVSocket_get_data_buf(csk->ibvsock, size);
	if (index < 0) {
		printk("failed to allocate data buffer. size=%ld\n", size);
		return -ENOMEM;
	}
	packet->data_buf_index = index;
	pDataBuf = &csk->ibvsock->data_buf[index];
	// copy the data into data buffer.
	memcpy(pDataBuf->pBuff, packet->request.iov.iov_base, size);
	packet->request.hdr_padding.RdmaAddr = pDataBuf->dma_addr;
	packet->request.hdr_padding.RdmaLength = htonl(size);

	return 0;
}

static int cfs_rdma_attach_buffer(struct cfs_socket *csk, struct cfs_packet *packet) {
	struct cfs_page_frag *frags;
	struct BufferItem *pDataBuf = NULL;
	ssize_t count = 0;
	int index = 0;
	int i;

	frags = packet->reply.data.read.frags;
	count = 0;
	for (i = 0; i < packet->reply.data.read.nr; i++) {
		count += frags[i].size;
	}
	index = IBVSocket_get_data_buf(csk->ibvsock, count);
	if (index < 0) {
		printk("failed to allocate data buffer. size=%ld\n", count);
		return -ENOMEM;
	}
	pDataBuf = &csk->ibvsock->data_buf[index];
	packet->request.hdr_padding.RdmaAddr = pDataBuf->dma_addr;
	packet->request.hdr_padding.RdmaLength = htonl(count);

	return 0;
}

static int cfs_rdma_set_packet_data(struct cfs_socket *csk, struct cfs_packet *packet) {
	int ret = 0;

	switch(packet->pkg_data_type) {
		case CFS_PACKAGE_DATA_PAGE:
			ret = cfs_rdma_pages_to_buffer(csk, packet);
			if (ret < 0) {
				cfs_log_error(csk->log, "cfs_rdma_pages_to_buffer error: %ld\n", ret);
				return ret;
			}
			break;
		case CFS_PACKAGE_DATA_ITER:
			ret = cfs_rdma_iter_to_buffer(csk, packet);
			if (ret < 0) {
				cfs_log_error(csk->log, "cfs_rdma_iter_to_buffer error: %ld\n", ret);
				return ret;
			}
			break;
		case CFS_PACKAGE_RDMA_ITER:
			break;
		default:
			cfs_log_error(csk->log, "invalid data type in rdma send: %d\n", packet->pkg_data_type);
			return -EINVAL;
	}

	return 0;
}

int cfs_rdma_send_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	struct iov_iter iter;
	struct iovec iov;
	ssize_t len = 0;
	int ret = 0;

	if (packet->request.hdr.opcode != CFS_OP_STREAM_WRITE) {
		cfs_log_error(csk->log, "only support stream write opcode\n");
		return -EPERM;
	}

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
					csk->log, "so(%p) id=%llu, op=0x%x, invalid request data %d\n",
					csk->sock, be64_to_cpu(packet->request.hdr.req_id),
					packet->request.hdr.opcode, ret);
				return ret;
			}
			packet->request.hdr.size = cpu_to_be32(cfs_buffer_size(csk->tx_buffer));
	}

	if (packet->request.arg) {
		memcpy(packet->request.hdr_padding.arg, packet->request.arg, cfs_buffer_size(packet->request.arg));
	}

	packet->request.hdr_padding.RdmaKey = htonl(csk->ibvsock->pd->unsafe_global_rkey);
	iov.iov_base = &packet->request;
	iov.iov_len = sizeof(struct cfs_packet_hdr) + sizeof(struct request_hdr_padding);
	iov_iter_init(&iter, READ, &iov, 1, iov.iov_len);

	switch (packet->request.hdr.opcode) {
		case CFS_OP_EXTENT_CREATE:
			len = IBVSocket_send(csk->ibvsock, &iter);
			if (len < 0) {
				cfs_log_error(csk->log, "IBVSocket_send error: %ld\n", len);
			}
			break;
		case CFS_OP_STREAM_WRITE:
		case CFS_OP_STREAM_RANDOM_WRITE:
			ret = cfs_rdma_set_packet_data(csk, packet);
			if (ret < 0) {
				cfs_log_error(csk->log, "cfs_rdma_set_packet_data error: %ld\n", ret);
				return ret;
			}

			len = IBVSocket_send(csk->ibvsock, &iter);
			if (len < 0) {
				cfs_log_error(csk->log, "IBVSocket_send error: %ld\n", len);
			}
			break;
		case CFS_OP_STREAM_READ:
		case CFS_OP_STREAM_FOLLOWER_READ:
			ret = cfs_rdma_attach_buffer(csk, packet);
			if (ret < 0) {
				cfs_log_error(csk->log, "cfs_rdma_attach_buffer error: %ld\n", ret);
				return ret;
			}
			len = IBVSocket_send(csk->ibvsock, &iter);
			if (len < 0) {
				cfs_log_error(csk->log, "IBVSocket_send error: %ld\n", len);
			}
			break;
		default:
			len = IBVSocket_send(csk->ibvsock, &iter);
			if (len < 0) {
				cfs_log_error(csk->log, "IBVSocket_send error: %ld\n", len);
			}
			break;
	}

	if (len < 0) {
		return -ENOENT;
	}
	return 0;
}

static int cfs_rdma_buffer_to_pages(struct cfs_socket *csk, struct cfs_packet *packet) {
	struct cfs_page_frag *frags;
	ssize_t count = 0;
	char *pStart = NULL;
	int index = 0;
	int i;

	// copy data from buffer.
	frags = packet->reply.data.read.frags;
	count = 0;
	index = packet->data_buf_index;
	if (index < 0 || index >= DATA_BUF_NUM) {
		printk("error: invalid data_buf_index=%d\n", index);
		return -EINVAL;
	}
	if (!csk->ibvsock->data_buf[index].used) {
		printk("error: data_buf[%d] used is false\n", index);
		return -EINVAL;
	}
	for (i = 0; i < packet->reply.data.read.nr && count < csk->ibvsock->data_buf[index].size; i++) {
		pStart = csk->ibvsock->data_buf[index].pBuff + count;
		memcpy(kmap(frags[i].page->page) + frags[i].offset, pStart, frags[i].size);
		kunmap(frags[i].page->page);
		count += frags[i].size;
	}
	// release the data buffer.
	IBVSocket_free_data_buf(csk->ibvsock, index);

	return 0;
}

static int cfs_rdma_buffer_to_iter(struct cfs_socket *csk, struct cfs_packet *packet) {
	ssize_t len = 0;
	int index = 0;
	u32 datalen = 0;

	// copy data from buffer.
	index = packet->data_buf_index;
	if (index < 0 || index >= DATA_BUF_NUM) {
		printk("error: invalid data_buf_index=%d\n", index);
		return -EINVAL;
	}
	if (!csk->ibvsock->data_buf[index].used) {
		printk("error: data_buf[%d] used is false\n", index);
		return -EINVAL;
	}

	datalen = be32_to_cpu(packet->reply.hdr.size);

	len = copy_to_iter(csk->ibvsock->data_buf[index].pBuff, datalen, packet->reply.data.user_iter);
	if (len != datalen) {
		printk("warning reply hdr size=%d, copied size=%ld\n", datalen, len);
	}

	// release the data buffer.
	IBVSocket_free_data_buf(csk->ibvsock, index);

	return 0;
}

int cfs_rdma_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	struct iov_iter iter;
	struct iovec iov;
	int ret = 0;
	ssize_t len = 0;
	int arglen;

	if (packet->request.hdr.opcode != CFS_OP_STREAM_WRITE) {
		cfs_log_error(csk->log, "only support stream write opcode\n");
		return -EPERM;
	}

	iov.iov_base = &packet->reply;
	iov.iov_len = sizeof(struct cfs_packet_hdr) + sizeof(struct reply_hdr_padding);
	iov_iter_init(&iter, WRITE, &iov, 1, iov.iov_len);

	len = IBVSocket_recvT(csk->ibvsock, &iter);
	if (len < 0) {
		cfs_log_error(csk->log,
			"rdma socket receive ret: %d\n", len);
		return -ENOENT;
	}

	arglen = be32_to_cpu(packet->reply.hdr.arglen);
	if (arglen > 0) {
		if (packet->reply.arg) {
			ret = cfs_buffer_resize(packet->reply.arg, arglen);
		} else if (!(packet->reply.arg = cfs_buffer_new(arglen))) {
			ret = -ENOMEM;
		}

		if (ret < 0) {
			cfs_log_error(csk->log, "cfs buffer re-arrange error ret: %d\n", ret);
			return ret;
		}
		memcpy(packet->reply.arg, packet->reply.hdr_padding.arg, arglen);
		cfs_buffer_seek(packet->reply.arg, arglen);
	}

	//unmap the rdma memory.
	switch (packet->request.hdr.opcode) {
		case CFS_OP_STREAM_WRITE:
		case CFS_OP_STREAM_RANDOM_WRITE:
			IBVSocket_free_data_buf(csk->ibvsock, packet->data_buf_index);
			break;
		case CFS_OP_STREAM_READ:
		case CFS_OP_STREAM_FOLLOWER_READ:
			if (packet->pkg_data_type == CFS_PACKAGE_DATA_PAGE) {
				ret = cfs_rdma_buffer_to_pages(csk, packet);
				if (ret < 0) {
					cfs_log_error(csk->log, "cfs_rdma_buffer_to_pages ret: %d\n", ret);
					return ret;
				}
			} else if (packet->pkg_data_type == CFS_PACKAGE_READ_ITER) {
				ret = cfs_rdma_buffer_to_iter(csk, packet);
				if (ret < 0) {
					cfs_log_error(csk->log, "cfs_rdma_buffer_to_iter ret: %d\n", ret);
					return ret;
				}
			} else {
				cfs_log_error(csk->log, "invalid package data type: %d\n", packet->pkg_data_type);
				return -EINVAL;
			}
			break;
		default:
			break;
	}

	return ret;
}

int cfs_rdma_allocate_buffer(struct cfs_socket *csk, size_t size, struct cfs_rdma_buffer *buffer) {
	int index = -1;

	if (buffer == NULL) {
		return -EPERM;
	}

	index = IBVSocket_get_data_buf(csk->ibvsock, size);
	if (index < 0 ) {
		cfs_log_error(csk->log, "allocate rdma data buffer failed.\n");
		return -ENOMEM;
	}

	buffer->index = index;
	buffer->pBuff = csk->ibvsock->data_buf[index].pBuff;
	buffer->dma_addr = csk->ibvsock->data_buf[index].dma_addr;

	return 0;
}

static void rdma_pool_lru_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_socket *sock;
	struct cfs_socket *tmp;

	schedule_delayed_work(delayed_work, CFS_RDMA_SOCKET_TIMEOUT);
	mutex_lock(&rdma_sock_pool->lock);
	list_for_each_entry_safe(sock, tmp, &rdma_sock_pool->lru, list) {
		if (sock->jiffies + CFS_RDMA_SOCKET_TIMEOUT > jiffies) {
			continue;
		}
		hash_del(&sock->hash);
		list_del(&sock->list);
		cfs_rdma_release(sock, true);
	}
	mutex_unlock(&rdma_sock_pool->lock);
}

int cfs_rdma_module_init(void)
{
	if (rdma_sock_pool)
		return 0;
	rdma_sock_pool = kzalloc(sizeof(*rdma_sock_pool), GFP_KERNEL);
	if (!rdma_sock_pool)
		return -ENOMEM;
	hash_init(rdma_sock_pool->head);
	INIT_LIST_HEAD(&rdma_sock_pool->lru);
	mutex_init(&rdma_sock_pool->lock);
	INIT_DELAYED_WORK(&rdma_sock_pool->work, rdma_pool_lru_work_cb);
	schedule_delayed_work(&rdma_sock_pool->work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	return 0;
}

void cfs_rdma_module_exit(void)
{
	struct cfs_socket *sock;
	struct hlist_node *tmp;
	int i;

	if (!rdma_sock_pool)
		return;
	cancel_delayed_work_sync(&rdma_sock_pool->work);
	hash_for_each_safe(rdma_sock_pool->head, i, tmp, sock, hash) {
		hash_del(&sock->hash);
		cfs_rdma_release(sock, true);
	}
	mutex_destroy(&rdma_sock_pool->lock);
	kfree(rdma_sock_pool);
	rdma_sock_pool = NULL;
}
