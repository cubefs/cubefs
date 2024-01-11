#include "cfs_rdma_socket.h"
#include "cfs_log.h"

static struct cfs_socket_pool *rdma_sock_pool;

void packet_to_requestheader(struct cfs_packet *packet, Header *header)
{
	struct cfs_packet_hdr *hdr = &packet->request.hdr;
	memcpy(header, hdr, sizeof(struct cfs_packet_hdr));
	if (packet->request.arg) {
		memcpy(header->arg, packet->request.arg,
		       cfs_buffer_size(packet->request.arg));
	}
}

int response_to_packet(Response *response, struct cfs_packet *packet)
{
	int ret = 0;
	u32 arglen;
	struct cfs_packet_hdr *hdr = &packet->reply.hdr;
	memcpy(hdr, response, sizeof(struct cfs_packet_hdr));

	arglen = be32_to_cpu(packet->reply.hdr.arglen);
	if (arglen > 0) {
		if (packet->reply.arg) {
			ret = cfs_buffer_resize(packet->reply.arg, arglen);
		} else if (!(packet->reply.arg = cfs_buffer_new(arglen))) {
			ret = -ENOMEM;
		}

		if (ret < 0) {
			return ret;
		}
		memcpy(packet->reply.arg, response->arg, arglen);
		cfs_buffer_seek(packet->reply.arg, arglen);
	}

	return ret;
}

int cfs_rdma_create(struct sockaddr_storage *ss, struct cfs_log *log,
		    struct cfs_socket **cskp)
{
	struct cfs_socket *csk;
	u32 key;

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

		csk->ibvsock = IBVSocket_construct();
		IBVSocket_connectByIP(csk->ibvsock, (struct sockaddr_in *)ss);

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
			IBVSocket_shutdown(csk->ibvsock);
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

int cfs_rdma_send_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	Header *hdr;
	struct iov_iter iter;
	struct iovec iov;
	struct cfs_page_frag *frags;
	int i;
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

	hdr = (Header *)kmalloc(sizeof(Header), GFP_NOFS);
	memset(hdr, 0, sizeof(Header));
	packet_to_requestheader(packet, hdr);

	hdr->RdmaKey = csk->ibvsock->localDest.rkey;
	iov.iov_base = hdr;
	iov.iov_len = sizeof(Header);
	iter.iov = &iov;
	iter.nr_segs = 1;
	iter.count = 1;
	iter.iov_offset = 0;

	switch (packet->request.hdr.opcode) {
	case CFS_OP_EXTENT_CREATE:
		IBVSocket_send(csk->ibvsock, &iter, 0);
		break;
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		frags = packet->request.data.write.frags;
		for (i = 0; i < packet->request.data.write.nr; i++) {
			// copy the data into rdma register memory.
			frags[i].rdma_addr = ib_dma_map_page(
				csk->ibvsock->cm_id->device,
				frags[i].page->page, frags[i].offset,
				frags[i].size, DMA_TO_DEVICE);
			hdr->RdmaAddr = frags[i].rdma_addr;
			hdr->RdmaLength = frags[i].size;
			IBVSocket_send(csk->ibvsock, &iter, 0);
		}
		break;
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		frags = packet->reply.data.read.frags;
		for (i = 0; i < packet->reply.data.read.nr; i++) {
			// copy the data into rdma register memory.
			frags[i].rdma_addr = ib_dma_map_page(
				csk->ibvsock->cm_id->device,
				frags[i].page->page, frags[i].offset,
				frags[i].size, DMA_FROM_DEVICE);
			hdr->RdmaAddr = frags[i].rdma_addr;
			hdr->RdmaLength = frags[i].size;
			IBVSocket_send(csk->ibvsock, &iter, 0);
		}
		break;
	default:
		IBVSocket_send(csk->ibvsock, &iter, 0);
		break;
	}

	kfree(hdr);
	return 0;
}

int cfs_rdma_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet)
{
	Response *response;
	struct iov_iter iter;
	struct iovec iov;
	struct cfs_page_frag *frags;
	int i, ret = 0;

	response = (Response *)kmalloc(sizeof(Response), GFP_NOFS);
	memset(response, 0, sizeof(Response));
	iov.iov_base = response;
	iov.iov_len = sizeof(Response);
	iter.iov = &iov;
	iter.nr_segs = 1;
	iter.count = 1;
	iter.iov_offset = 0;

	IBVSocket_recvT(csk->ibvsock, &iter, 1000);
	ret = response_to_packet(response, packet);
	if (ret < 0) {
		cfs_log_error(csk->log,
			      "failed to convert response to packet\n");
	}

	kfree(response);

	//unmap the rdma memory.
	switch (packet->request.hdr.opcode) {
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		frags = packet->request.data.write.frags;
		for (i = 0; i < packet->request.data.write.nr; i++) {
			// unmap rdma address.
			ib_dma_unmap_page(csk->ibvsock->cm_id->device,
					  frags[i].rdma_addr, frags[i].size,
					  DMA_TO_DEVICE);
		}
		break;
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
		frags = packet->reply.data.read.frags;
		for (i = 0; i < packet->reply.data.read.nr; i++) {
			// unmap rdma address.
			ib_dma_unmap_page(csk->ibvsock->cm_id->device,
					  frags[i].rdma_addr, frags[i].size,
					  DMA_FROM_DEVICE);
		}
		break;
	default:
		break;
	}

	return ret;
}

static void rdma_pool_lru_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_socket *sock;
	struct cfs_socket *tmp;

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(SOCK_POOL_LRU_INTERVAL_MS));
	mutex_lock(&rdma_sock_pool->lock);
	list_for_each_entry_safe(sock, tmp, &rdma_sock_pool->lru, list) {
		if (is_sock_valid(sock))
			break;
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
