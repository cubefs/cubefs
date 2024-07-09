#include <linux/module.h>
#include <linux/inet.h>
#include <linux/socket.h>
#include <linux/delay.h>
#include <linux/list.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>
#include "rdma_api.h"
#include "../cfs_packet.h"

int ibv_socket_event_handler(struct rdma_cm_id *cm_id,
			   struct rdma_cm_event *event)
{
	struct ibv_socket *this = cm_id->context;
	int retVal = 0;

	if (!this) {
		ibv_print_error("this is null\n");
		return 0;
	}
	if (!event) {
		ibv_print_error("event is null\n");
		return 0;
	}
	ibv_print_debug("rdma event: %i, status: %i\n", event->event, event->status);

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		this->conn_state = IBVSOCKETCONNSTATE_ADDRESSRESOLVED;
		ibv_print_debug("receive event RDMA_CM_EVENT_ADDR_RESOLVED\n");
		break;
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
		retVal = -ENETUNREACH;
		this->conn_state = IBVSOCKETCONNSTATE_FAILED;
		ibv_print_debug("receive event RDMA_CM_EVENT_ADDR_ERROR or UNREACHABLE = %d\n", event->event);
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		this->conn_state = IBVSOCKETCONNSTATE_ROUTERESOLVED;
		ibv_print_debug("receive event RDMA_CM_EVENT_ROUTE_RESOLVED\n");
		break;

	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
		retVal = -ETIMEDOUT;
		this->conn_state = IBVSOCKETCONNSTATE_FAILED;
		ibv_print_debug("receive event RDMA_CM_EVENT_ROUTE_ERROR or CONNECT_ERROR = %d\n", event->event);
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
		ibv_print_debug("receive event RDMA_CM_EVENT_CONNECT_REQUEST\n");
		break;

	case RDMA_CM_EVENT_CONNECT_RESPONSE:
		ibv_print_debug("receive event RDMA_CM_EVENT_CONNECT_RESPONSE\n");
		break;

	case RDMA_CM_EVENT_REJECTED:
		this->conn_state = IBVSOCKETCONNSTATE_REJECTED_STALE;
		ibv_print_debug("receive event RDMA_CM_EVENT_REJECTED: %s\n", rdma_reject_msg(cm_id, event->status));
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		this->conn_state = IBVSOCKETCONNSTATE_ESTABLISHED;
		ibv_print_debug("receive event RDMA_CM_EVENT_ESTABLISHED\n");
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		this->conn_state = IBVSOCKETCONNSTATE_DISCONNECTED;
		ibv_print_debug("receive event RDMA_CM_EVENT_DISCONNECTED\n");
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		this->conn_state = IBVSOCKETCONNSTATE_DESTROYED;
		ibv_print_debug("receive event RDMA_CM_EVENT_DEVICE_REMOVAL\n");
		break;

	default:
		ibv_print_debug("Ignoring RDMA_CMA event: %d\n", event->event);
		break;
	}

	wake_up(&this->event_wait_queue);
	return retVal;
}

void ibv_socket_recv_complete_hdr(struct ib_cq *cq, void *cq_context)
{
	ibv_print_debug("ibv_socket_recv_complete_hdr\n");
}

void ibv_socket_cq_recv_event_hdr(struct ib_event *event, void *data)
{
	ibv_print_debug("ibv_socket_cq_recv_event_hdr\n");
}

void ibv_socket_qp_event_hdr(struct ib_event *event, void *data)
{
	ibv_print_debug("ibv_socket_qp_event_hdr\n");
}

void ibv_socket_send_complete_hdr(struct ib_cq *cq, void *cq_context)
{
	ibv_print_debug("ibv_socket_send_complete_hdr\n");
}

void ibv_socket_cq_send_event_hdr(struct ib_event *event, void *data)
{
	ibv_print_debug("ibv_socket_cq_send_event_hdr\n");
}

char *print_ip_addr(u32 addr) {
	static char ip_addr[16];
	sprintf(ip_addr, "%d.%d.%d.%d", (addr & 0xff000000) >> 24, (addr & 0x00ff0000) >> 16, (addr & 0x0000ff00) >> 8, (addr & 0x000000ff));
	return ip_addr;
}

void ibv_socket_ring_buffer_free(struct ibv_socket *this) {
	int i = 0;

	if (!this)
		return;

	for (i=0; i<WR_MAX_NUM; i++) {
		if (this->recv_buf[i]) {
			cfs_rdma_buffer_put(this->recv_buf[i]);
			this->recv_buf[i] = NULL;
		}
	}

	for (i=0; i<WR_MAX_NUM; i++) {
		if (this->send_buf[i]) {
			cfs_rdma_buffer_put(this->send_buf[i]);
			this->send_buf[i] = NULL;
		}
	}
}

int ibv_socket_ring_buffer_init(struct ibv_socket *this) {
	struct cfs_node *item = NULL;
	int i = 0;
	struct ib_recv_wr wr;
	const struct ib_recv_wr *bad_wr;
	struct ib_sge sge;
	int ret;

	if (!this) {
		ibv_print_error("the ibv socket is null\n");
		return -EPERM;
	}

	mutex_init(&this->lock);
	
	for (i=0; i<WR_MAX_NUM; i++) {
		ret = cfs_rdma_buffer_get(&item, BUFFER_LEN);
		if (ret < 0) {
			ret = -ENOMEM;
			goto err_out;
		}
		item->used = false;
		this->recv_buf[i] = item;
	}

	for (i=0; i<WR_MAX_NUM; i++) {
		ret = cfs_rdma_buffer_get(&item, BUFFER_LEN);
		if (ret < 0) {
			ret = -ENOMEM;
			goto err_out;
		}
		item->used = false;
		this->send_buf[i] = item;
	}

	for (i=0; i<WR_MAX_NUM; i++) {
		sge.addr = this->recv_buf[i]->dma_addr;
		sge.length = BUFFER_LEN;
		sge.lkey = this->pd->local_dma_lkey;
		wr.next = NULL;
		wr.wr_id = i;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		ret = ib_post_recv(this->qp, &wr, &bad_wr);
		if (unlikely(ret)) {
			ibv_print_error("ib_post_recv failed. ErrCode: %d\n", ret);
			ret = -EIO;
			goto err_out;
		}
	}
	this->recv_buf_index = 0;
	this->send_buf_index = 0;

	return 0;

err_out:
	ibv_socket_ring_buffer_free(this);
	return ret;
}

int ibv_socket_get_send_buf(struct ibv_socket *this) {
	int i = 0;

	mutex_lock(&this->lock);
	for (i = this->send_buf_index; i<WR_MAX_NUM; i++) {
		if (!this->send_buf[i]->used) {
			this->send_buf[i]->used = true;
			this->send_buf_index = i;
			mutex_unlock(&this->lock);
			return i;
		}
	}
	for (i = 0; i<this->send_buf_index; i++) {
		if (!this->send_buf[i]->used) {
			this->send_buf[i]->used = true;
			this->send_buf_index = i;
			mutex_unlock(&this->lock);
			return i;
		}
	}
	mutex_unlock(&this->lock);

	return -1;
}

void ibv_socket_put_send_buf(struct ibv_socket *this, int index) {
	if (index < 0 || index >= WR_MAX_NUM)
		return;

	this->send_buf[index]->used = false;
}

struct ibv_socket *ibv_socket_construct(struct sockaddr_in *sin) {
	struct ibv_socket *this;
	struct ib_cq_init_attr attrs;
	struct ib_qp_init_attr qpInitAttr;
	struct rdma_conn_param conn_param;
	int ret;

	this = kzalloc(sizeof(struct ibv_socket), GFP_KERNEL);
	if (!this) {
		ibv_print_error("kzalloc failed\n");
		return ERR_PTR(-ENOMEM);
	}

	this->conn_state = IBVSOCKETCONNSTATE_CONNECTING;
	init_waitqueue_head(&this->event_wait_queue);

	this->cm_id = rdma_create_id(&init_net, ibv_socket_event_handler, this, RDMA_PS_TCP, IB_QPT_RC);
	if (IS_ERR(this->cm_id)) {
		ibv_print_error("rdma_create_id failed: %ld\n", PTR_ERR(this->cm_id));
		goto err_free_this;
	}

	ret = rdma_resolve_addr(this->cm_id, NULL, (struct sockaddr *)sin, IBVSOCKET_CONN_TIMEOUT_MS);
	if (ret) {
		ibv_print_error("rdma_resolve_addr failed: %d\n", ret);
		goto err_destroy_cm_id;
	}

	wait_event_interruptible(this->event_wait_queue, this->conn_state != IBVSOCKETCONNSTATE_CONNECTING);

	ret = rdma_resolve_route(this->cm_id, IBVSOCKET_CONN_TIMEOUT_MS);
	if (ret) {
		ibv_print_error("rdma_resolve_route failed: %d.\n", ret);
		goto err_destroy_cm_id;
	}

	wait_event_interruptible( this->event_wait_queue, this->conn_state != IBVSOCKETCONNSTATE_ADDRESSRESOLVED);

	this->pd = ib_alloc_pd(this->cm_id->device, IB_PD_UNSAFE_GLOBAL_RKEY);
	if (IS_ERR(this->pd)) {
		ibv_print_error("Couldn't allocate PD. ErrCode: %ld\n", PTR_ERR(this->pd));
		goto err_destroy_cm_id;
	}

	attrs.cqe = WR_MAX_NUM;
	attrs.comp_vector = 0;
	attrs.flags = 0;

	this->recv_cq = ib_create_cq(this->cm_id->device, ibv_socket_recv_complete_hdr, ibv_socket_cq_recv_event_hdr, this, &attrs);
	if (IS_ERR(this->recv_cq)) {
		ibv_print_error("couldn't create recv CQ. ErrCode: %ld\n", PTR_ERR(this->recv_cq));
		goto err_dealloc_pd;
	}

	this->send_cq = ib_create_cq(this->cm_id->device, ibv_socket_send_complete_hdr, ibv_socket_cq_send_event_hdr, this, &attrs);
	if (IS_ERR(this->send_cq)) {
		ibv_print_error("couldn't create send CQ. ErrCode: %ld\n", PTR_ERR(this->send_cq));
		goto err_free_recv_cq;
	}

	memset(&qpInitAttr, 0, sizeof(qpInitAttr));
	qpInitAttr.event_handler = ibv_socket_qp_event_hdr;
	qpInitAttr.send_cq = this->send_cq;
	qpInitAttr.recv_cq = this->recv_cq;
	qpInitAttr.qp_type = IB_QPT_RC;
	//qpInitAttr.sq_sig_type = IB_SIGNAL_REQ_WR;
	qpInitAttr.sq_sig_type = IB_SIGNAL_ALL_WR;
	qpInitAttr.cap.max_send_wr = WR_MAX_NUM;
	qpInitAttr.cap.max_recv_wr = WR_MAX_NUM;
	qpInitAttr.cap.max_send_sge = 1;
	qpInitAttr.cap.max_recv_sge = 1;
	qpInitAttr.cap.max_inline_data = 0;
	qpInitAttr.qp_context = this;

	ret = rdma_create_qp(this->cm_id, this->pd, &qpInitAttr);
	if (ret) {
		ibv_print_error("couldn't create QP. ErrCode: %d\n", ret);
		goto err_free_send_cq;
	}
	this->qp = this->cm_id->qp;

	ret = ibv_socket_ring_buffer_init(this);
	if (ret < 0) {
		ibv_print_error("ibv_socket_ring_buffer_init error: %d\n", ret);
		goto err_destroy_qp;
	}

	if (ib_req_notify_cq(this->recv_cq, IB_CQ_NEXT_COMP)) {
		ibv_print_error("couldn't request recv CQ notification\n");
		goto err_destroy_qp;
	}

	if (ib_req_notify_cq(this->send_cq, IB_CQ_NEXT_COMP)) {
		ibv_print_error("couldn't request send CQ notification\n");
		goto err_destroy_qp;
	}

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;
	ret = rdma_connect(this->cm_id, &conn_param);
	if (unlikely(ret)) {
		ibv_print_error("rdma_connect failed. ErrCode: %d\n", ret);
		goto err_destroy_qp;
	}

	wait_event_interruptible_timeout(this->event_wait_queue, this->conn_state != IBVSOCKETCONNSTATE_ROUTERESOLVED, TIMEOUT_JS);
	if (this->conn_state != IBVSOCKETCONNSTATE_ESTABLISHED) {
		ibv_print_error("connection to %s:%d not established. state=%d\n",
			print_ip_addr(ntohl(sin->sin_addr.s_addr)), ntohs(sin->sin_port), this->conn_state);
		goto err_destroy_qp;
	}

	this->remote_addr.sin_family = sin->sin_family;
	this->remote_addr.sin_port = sin->sin_port;
	this->remote_addr.sin_addr.s_addr = sin->sin_addr.s_addr;
	ibv_print_debug("connect to %s:%d success\n", print_ip_addr(ntohl(sin->sin_addr.s_addr)), ntohs(sin->sin_port));

    return this;

err_destroy_qp:
	ibv_socket_ring_buffer_free(this);
	rdma_destroy_qp(this->cm_id);

err_free_send_cq:
	ib_destroy_cq(this->send_cq);

err_free_recv_cq:
	ib_destroy_cq(this->recv_cq);

err_dealloc_pd:
	ib_dealloc_pd(this->pd);

err_destroy_cm_id:
	rdma_destroy_id(this->cm_id);

err_free_this:
	kfree(this);

	return ERR_PTR(-EIO);
}

bool ibv_socket_destruct(struct ibv_socket *this) {
	if (!this) {
		return false;
	}

	ibv_print_debug("disconnect rdma link with %s:%d\n",
		print_ip_addr(ntohl(this->remote_addr.sin_addr.s_addr)), ntohs(this->remote_addr.sin_port));

	this->conn_state = IBVSOCKETCONNSTATE_DESTROYED;

    if (this->cm_id) {
        rdma_disconnect(this->cm_id);
    }

    ibv_socket_ring_buffer_free(this);

    if (this->qp) {
        rdma_destroy_qp(this->cm_id);
        this->qp = NULL;
    }
	
    if (this->send_cq) {
        ib_destroy_cq(this->send_cq);
        this->send_cq = NULL;
    }
	
    if (this->recv_cq) {
        ib_destroy_cq(this->recv_cq);
        this->recv_cq = NULL;
    }

    if (this->pd) {
        ib_dealloc_pd(this->pd);
        this->pd = NULL;
    }

	if (this->cm_id) {
        rdma_destroy_id(this->cm_id);
        this->cm_id = NULL;
    }

	kfree(this);
	this = NULL;

    return true;
}

ssize_t ibv_socket_post_recv(struct ibv_socket *this, int index) {
	struct ib_recv_wr wr;
	const struct ib_recv_wr *bad_wr;
	struct ib_sge sge;
    int ret;

	if (index < 0 || index >= WR_MAX_NUM) {
		return -EINVAL;
	}

	this->recv_buf[index]->used = false;

	sge.addr = this->recv_buf[index]->dma_addr;
	sge.length = BUFFER_LEN;
	sge.lkey = this->pd->local_dma_lkey;
    wr.next = NULL;
	wr.wr_id = index;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	ret = ib_post_recv(this->qp, &wr, &bad_wr);
	if (unlikely(ret)) {
		ibv_print_error("ib_post_recv failed. ErrCode: %d\n", ret);
		return ret;
	}

    return 0;
}

int ibv_socket_get_buffer_by_req_id(struct ibv_socket *this, __be64 req_id) {
	int i = 0;
	struct cfs_packet_hdr *hdr = NULL;

	mutex_lock(&this->lock);

	for (i = this->recv_buf_index; i<WR_MAX_NUM; i++) {
		hdr = (struct cfs_packet_hdr *)this->recv_buf[i]->pBuff;
		if (this->recv_buf[i]->used && hdr->req_id == req_id) {
			this->recv_buf_index = (i+1)%WR_MAX_NUM;
			mutex_unlock(&this->lock);
			return i;
		}
	}
	for (i=0; i<this->recv_buf_index; i++) {
		hdr = (struct cfs_packet_hdr *)this->recv_buf[i]->pBuff;
		if (this->recv_buf[i]->used && hdr->req_id == req_id) {
			this->recv_buf_index = (i+1)%WR_MAX_NUM;
			mutex_unlock(&this->lock);
			return i;
		}
	}

	mutex_unlock(&this->lock);
	return -1;
}

ssize_t ibv_socket_copy_restore(struct ibv_socket *this, struct iov_iter *iter, int index) {
    int ret;
    ssize_t isize = 0;

	if (index < 0 || index >= WR_MAX_NUM) {
		ibv_print_error("index is out of range 0-%d, index=%d\n", (WR_MAX_NUM-1), index);
		return -EINVAL;
	}

	isize = MIN(BUFFER_LEN, iter->iov->iov_len);

    memcpy(iter->iov->iov_base, this->recv_buf[index]->pBuff, isize);

	ret = ibv_socket_post_recv(this, index);
	if (unlikely(ret < 0)) {
		ibv_print_error("ibv_socket_post_recv error: %d\n", ret);
		return ret;
	}

    return isize;
}

ssize_t ibv_socket_recv(struct ibv_socket *this, struct iov_iter *iter, __be64 req_id) {
	struct ib_wc wc[8];
	int numElements;
	int i;
	int index = -1;
	unsigned long time_out_jiffies = jiffies + msecs_to_jiffies(IBVSOCKET_RECV_TIMEOUT_MS);

    while(true) {
		if (this->conn_state != IBVSOCKETCONNSTATE_ESTABLISHED) {
			ibv_print_error("rdma link state: %d\n", this->conn_state);
			return -EIO;
		}
        numElements = ib_poll_cq(this->recv_cq, 8, wc);
        if (numElements > 0) {
            for (i = 0; i < numElements; i++) {
				index = wc[i].wr_id;
				if (wc[i].status != IB_WC_SUCCESS) {
					ibv_print_error("recv status: %d, opcode: %d, wr_id: %lld\n", wc[i].status, wc[i].opcode, wc[i].wr_id);
					ibv_socket_post_recv(this, index);
					continue;
				}
				this->recv_buf[index]->used = true;
            }
        } else if (numElements < 0) {
			ibv_print_error("ib_poll_cq recv_cq failed. ErrCode: %d\n", numElements);
			return -EIO;
		}

		index = ibv_socket_get_buffer_by_req_id(this, req_id);
		if (index >= 0) {
			break;
		}
		if (time_after(jiffies, time_out_jiffies)) {
			ibv_print_error("rdma receive timeout %d seconds. req id=%lld\n", IBVSOCKET_RECV_TIMEOUT_MS/1000, be64_to_cpu(req_id));
			return -ETIMEDOUT;
		}
    }
	if (index < 0) {
		ibv_print_error("Timeout waiting for receive buffer\n");
		return -ENOMEM;
	}

    return ibv_socket_copy_restore(this, iter, index);
}

ssize_t ibv_socket_send(struct ibv_socket *this, struct iov_iter *iter) {
   	struct ib_send_wr send_wr;
	const struct ib_send_wr *send_bad_wr;
	struct ib_sge sge;
    int ret = 0;
	struct ib_wc wc[8];
	int numElements;
    int i = 0;
    ssize_t isize = 0;
	int index = -1;

	while(true) {
		if (this->conn_state != IBVSOCKETCONNSTATE_ESTABLISHED) {
			ibv_print_error("rdma link state: %d. remote: %s:%d\n", this->conn_state,
				print_ip_addr(ntohl(this->remote_addr.sin_addr.s_addr)), ntohs(this->remote_addr.sin_port));
			return -EIO;
		}

		numElements = ib_poll_cq(this->send_cq, 8, wc);
		if (numElements > 0) {
			for (i = 0; i < numElements; i++) {
				ibv_socket_put_send_buf(this, wc[i].wr_id);
				if (wc[i].status != IB_WC_SUCCESS) {
					ibv_print_error("send status: %d, opcode: %d, wr_id: %lld\n", wc[i].status, wc[i].opcode, wc[i].wr_id);
				}
			}
		} else if (numElements < 0) {
			ibv_print_error("ib_poll_cq send_cq failed. ErrCode: %d\n", numElements);
			return -EIO;
		}

		index = ibv_socket_get_send_buf(this);
		if (index >= 0) {
			break;
		}
	}

	if (index < 0) {
		ibv_print_error("Timeout waiting for send buffer\n");
		return -ENOMEM;
	}

    isize = MIN(BUFFER_LEN, iter->iov->iov_len);
    memcpy(this->send_buf[index]->pBuff, iter->iov->iov_base, isize);

	sge.addr = this->send_buf[index]->dma_addr;
	sge.length = isize;
	sge.lkey = this->pd->local_dma_lkey;
	send_wr.wr_id = index;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IB_WR_SEND;
	send_wr.send_flags = IB_SEND_SIGNALED;
	send_wr.next = NULL;
    ret = ib_post_send(this->qp, &send_wr, &send_bad_wr);
    if (unlikely(ret)) {
        ibv_print_error("ib_post_send() failed. ErrCode: %d\n", ret);
        return -EIO;
    }

    return isize;
}

struct cfs_node *ibv_socket_get_data_buf(struct ibv_socket *this, size_t size) {
	int ret = 0;
	struct cfs_node *item = NULL;

	ret = cfs_rdma_buffer_get(&item, size);
	if (ret < 0) {
		return NULL;
	}
	item->used = true;

	return item;
}

void ibv_socket_free_data_buf(struct ibv_socket *this, struct cfs_node *item) {
	if (!item->used) {
		ibv_print_info("error: the buffer is not used. ptr: %llx\n", (uint64_t)item);
		return;
	}
	item->used = false;
	cfs_rdma_buffer_put(item);
}
