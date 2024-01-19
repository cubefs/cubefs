#include <linux/module.h>
#include <linux/printk.h>
#include <linux/inet.h>
#include <linux/socket.h>
#include <linux/delay.h>
#include <linux/list.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>
#include "rdma_api.h"

int verify_rdma_event_handler(struct rdma_cm_id *cm_id,
			   struct rdma_cm_event *event)
{
	struct IBVSocket *this = cm_id->context;
	int retVal = 0;

	if (!this) {
		printk("this is null\n");
		return 0;
	}
	if (!event) {
		printk("event is null\n");
		return 0;
	}
	printk("rdma event: %i, status: %i\n", event->event, event->status);

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		this->connState = IBVSOCKETCONNSTATE_ADDRESSRESOLVED;
		printk("receive event RDMA_CM_EVENT_ADDR_RESOLVED\n");
		break;
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
		retVal = -ENETUNREACH;
		this->connState = IBVSOCKETCONNSTATE_FAILED;
		printk("receive event RDMA_CM_EVENT_ADDR_ERROR or UNREACHABLE = %d\n", event->event);
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		this->connState = IBVSOCKETCONNSTATE_ROUTERESOLVED;
		printk("receive event RDMA_CM_EVENT_ROUTE_RESOLVED\n");
		break;

	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
		retVal = -ETIMEDOUT;
		this->connState = IBVSOCKETCONNSTATE_FAILED;
		printk("receive event RDMA_CM_EVENT_ROUTE_ERROR or CONNECT_ERROR = %d\n", event->event);
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
		printk("receive event RDMA_CM_EVENT_CONNECT_REQUEST\n");
		break;

	case RDMA_CM_EVENT_CONNECT_RESPONSE:
		printk("receive event RDMA_CM_EVENT_CONNECT_RESPONSE\n");
		break;

	case RDMA_CM_EVENT_REJECTED:
		this->connState = IBVSOCKETCONNSTATE_REJECTED_STALE;
		printk("receive event RDMA_CM_EVENT_REJECTED: %s\n", rdma_reject_msg(cm_id, event->status));
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		this->connState = IBVSOCKETCONNSTATE_ESTABLISHED;
		printk("receive event RDMA_CM_EVENT_ESTABLISHED\n");
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		this->connState = IBVSOCKETCONNSTATE_UNCONNECTED;
		printk("receive event RDMA_CM_EVENT_DISCONNECTED\n");
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		this->connState = IBVSOCKETCONNSTATE_UNCONNECTED;
		printk("receive event RDMA_CM_EVENT_DEVICE_REMOVAL\n");
		break;

	default:
		printk("Ignoring RDMA_CMA event: %d\n", event->event);
		break;
	}

	wake_up(&this->eventWaitQ);
	return retVal;
}

void __IBVSocket_recvCompletionHandler(struct ib_cq *cq, void *cq_context)
{
	printk("recvCompletionHandler\n");
}

void __IBVSocket_cqRecvEventHandler(struct ib_event *event, void *data)
{
	printk("__IBVSocket_cqRecvEventHandler\n");
}

void __IBVSocket_qpEventHandler(struct ib_event *event, void *data)
{
	printk("__IBVSocket_qpEventHandler\n");
}

void __IBVSocket_sendCompletionHandler(struct ib_cq *cq, void *cq_context)
{
	printk("sendCompletionHandler\n");
}

void __IBVSocket_cqSendEventHandler(struct ib_event *event, void *data)
{
	printk("__IBVSocket_cqSendEventHandler\n");
}

void print_ip_addr(u32 addr) {
	printk("ip addr: %d.%d.%d.%d\n", (addr & 0xff000000) >> 24, (addr & 0x00ff0000) >> 16, (addr & 0x0000ff00) >> 8, (addr & 0x000000ff));
}

void IBVSocket_init(struct IBVSocket *this) {
    if (this == NULL) {
        return;
    }
	memset(this, 0, sizeof(struct IBVSocket));
    this->cm_id = NULL;
    this->pd = NULL;
    this->recvCQ = NULL;
    this->sendCQ = NULL;
    this->qp = NULL;
    this->cm_id = NULL;
}

int RingBuffer_init(struct IBVSocket *this) {
	int i = 0;
	struct ib_recv_wr wr;
	const struct ib_recv_wr *bad_wr;
	struct ib_sge sge;
	int ret;

	if (!this)
		return -1;

	mutex_init(&this->lock);
	
	for (i=0; i<BLOCK_NUM; i++) {
		this->recvBuf[i].pBuff = kzalloc(MSG_LEN, GFP_KERNEL);
		if (!this->recvBuf[i].pBuff) {
			return -ENOMEM;
		}
		this->recvBuf[i].dma_addr = ib_dma_map_single(this->cm_id->device, this->recvBuf[i].pBuff, MSG_LEN, DMA_BIDIRECTIONAL);
		this->recvBuf[i].used = false;
	}

	for (i=0; i<BLOCK_NUM; i++) {
		this->sendBuf[i].pBuff = kzalloc(MSG_LEN, GFP_KERNEL);
		if (!this->sendBuf[i].pBuff) {
			return -ENOMEM;
		}
		this->sendBuf[i].dma_addr = ib_dma_map_single(this->cm_id->device, this->sendBuf[i].pBuff, MSG_LEN, DMA_BIDIRECTIONAL);
		this->sendBuf[i].used = false;
	}

	for (i=0; i<BLOCK_NUM; i++) {
		sge.addr = this->recvBuf[i].dma_addr;
		sge.length = MSG_LEN;
		sge.lkey = this->pd->local_dma_lkey;
		wr.next = NULL;
		wr.wr_id = i;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		ret = ib_post_recv(this->qp, &wr, &bad_wr);
		if (unlikely(ret)) {
			printk("ib_post_recv failed. ErrCode: %d\n", ret);
			return -EIO;
		}
	}
	this->recvBufIndex = 0;
	this->sendBufIndex = 0;

	return 0;
}

void RingBuffer_free(struct IBVSocket *this) {
	int i = 0;

	if (!this)
		return;

	for (i=0; i<BLOCK_NUM; i++) {
		if (this->recvBuf[i].pBuff) {
			ib_dma_unmap_single(this->cm_id->device, this->recvBuf[i].dma_addr, MSG_LEN, DMA_BIDIRECTIONAL);
			kfree(this->recvBuf[i].pBuff);
			this->recvBuf[i].pBuff = NULL;
		}
	}

	for (i=0; i<BLOCK_NUM; i++) {
		if (this->sendBuf[i].pBuff) {
			ib_dma_unmap_single(this->cm_id->device, this->sendBuf[i].dma_addr, MSG_LEN, DMA_BIDIRECTIONAL);
			kfree(this->sendBuf[i].pBuff);
			this->sendBuf[i].pBuff = NULL;
		}
	}
}

int RingBuffer_alloc(struct IBVSocket *this, bool send) {
	int i = 0;

	mutex_lock(&this->lock);
	if (send) {
		for (i = this->sendBufIndex; i<BLOCK_NUM; i++) {
			if (!this->sendBuf[i].used) {
				this->sendBuf[i].used = true;
				this->sendBufIndex = i;
				mutex_unlock(&this->lock);
				return i;
			}
		}
		for (i = 0; i<this->sendBufIndex; i++) {
			if (!this->sendBuf[i].used) {
				this->sendBuf[i].used = true;
				this->sendBufIndex = i;
				mutex_unlock(&this->lock);
				return i;
			}
		}
	} else {
		for (i = this->recvBufIndex; i<BLOCK_NUM; i++) {
			if (this->recvBuf[i].used) {
				this->recvBufIndex = (i+1)%BLOCK_NUM;
				mutex_unlock(&this->lock);
				return i;
			}
		}
		for (i=0; i<this->recvBufIndex; i++) {
			if (this->recvBuf[i].used) {
				this->recvBufIndex = (i+1)%BLOCK_NUM;
				mutex_unlock(&this->lock);
				return i;
			}
		}
	}

	mutex_unlock(&this->lock);
	return -1;
}

void RingBuffer_dealloc(struct IBVSocket *this, bool send, int index) {
	if (index < 0 || index >= BLOCK_NUM)
		return;
	mutex_lock(&this->lock);
	if (send) {
		this->sendBuf[index].used = false;
	} else {
		this->recvBuf[index].used = false;
	}
	mutex_unlock(&this->lock);
}

struct IBVSocket *IBVSocket_construct(struct sockaddr_in *sin) {
	struct IBVSocket *this;
	struct ib_cq_init_attr attrs;
	struct ib_qp_init_attr qpInitAttr;
	struct rdma_conn_param conn_param;
	int ret;

	this = kzalloc(sizeof(struct IBVSocket), GFP_KERNEL);
	if (!this) {
		printk("kzalloc failed\n");
		return ERR_PTR(-ENOMEM);
	}
    IBVSocket_init(this);

	this->connState = IBVSOCKETCONNSTATE_CONNECTING;
	init_waitqueue_head(&this->eventWaitQ);

	this->cm_id = rdma_create_id(&init_net, verify_rdma_event_handler, this, RDMA_PS_TCP, IB_QPT_RC);
	if (IS_ERR(this->cm_id)) {
		printk("rdma_create_id failed: %ld\n", PTR_ERR(this->cm_id));
		goto err_free_this;
	}

	ret = rdma_resolve_addr(this->cm_id, NULL, (struct sockaddr *)sin, IBVSOCKET_CONN_TIMEOUT_MS);
	if (ret) {
		printk("rdma_resolve_addr failed: %d\n", ret);
		goto err_destroy_cm_id;
	}

	wait_event_interruptible(this->eventWaitQ, this->connState != IBVSOCKETCONNSTATE_CONNECTING);

	ret = rdma_resolve_route(this->cm_id, IBVSOCKET_CONN_TIMEOUT_MS);
	if (ret) {
		printk("rdma_resolve_route failed: %d.\n", ret);
		goto err_destroy_cm_id;
	}

	wait_event_interruptible( this->eventWaitQ, this->connState != IBVSOCKETCONNSTATE_ADDRESSRESOLVED);

	this->pd = ib_alloc_pd(this->cm_id->device, IB_PD_UNSAFE_GLOBAL_RKEY);
	if (IS_ERR(this->pd)) {
		printk("Couldn't allocate PD. ErrCode: %ld\n", PTR_ERR(this->pd));
		goto err_destroy_cm_id;
	}

	attrs.cqe = 128;
	attrs.comp_vector = 0;
	attrs.flags = 0;

	this->recvCQ = ib_create_cq(this->cm_id->device, __IBVSocket_recvCompletionHandler, __IBVSocket_cqRecvEventHandler, this, &attrs);
	if (IS_ERR(this->recvCQ)) {
		printk("couldn't create recv CQ. ErrCode: %ld\n", PTR_ERR(this->recvCQ));
		goto err_dealloc_pd;
	}

	this->sendCQ = ib_create_cq(this->cm_id->device, __IBVSocket_sendCompletionHandler, __IBVSocket_cqSendEventHandler, this, &attrs);
	if (IS_ERR(this->sendCQ)) {
		printk("couldn't create send CQ. ErrCode: %ld\n", PTR_ERR(this->sendCQ));
		goto err_free_recv_cq;
	}

	memset(&qpInitAttr, 0, sizeof(qpInitAttr));
	qpInitAttr.event_handler = __IBVSocket_qpEventHandler;
	qpInitAttr.send_cq = this->sendCQ;
	qpInitAttr.recv_cq = this->recvCQ;
	qpInitAttr.qp_type = IB_QPT_RC;
	//qpInitAttr.sq_sig_type = IB_SIGNAL_REQ_WR;
	qpInitAttr.sq_sig_type = IB_SIGNAL_ALL_WR;
	qpInitAttr.cap.max_send_wr = 128;
	qpInitAttr.cap.max_recv_wr = 128;
	qpInitAttr.cap.max_send_sge = 1;
	qpInitAttr.cap.max_recv_sge = 1;
	qpInitAttr.cap.max_inline_data = 0;
	qpInitAttr.qp_context = this;

	ret = rdma_create_qp(this->cm_id, this->pd, &qpInitAttr);
	if (ret) {
		printk("couldn't create QP. ErrCode: %d\n", ret);
		goto err_free_send_cq;
	}
	this->qp = this->cm_id->qp;

	ret = RingBuffer_init(this);
	if (ret < 0) {
		printk("RingBuffer_init error: %d\n", ret);
		goto err_destroy_qp;
	}

	if (ib_req_notify_cq(this->recvCQ, IB_CQ_NEXT_COMP)) {
		printk("couldn't request CQ notification\n");
		goto err_destroy_qp;
	}

	if (ib_req_notify_cq(this->sendCQ, IB_CQ_NEXT_COMP)) {
		printk("couldn't request CQ notification\n");
		goto err_destroy_qp;
	}

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;
	ret = rdma_connect(this->cm_id, &conn_param);
	if (unlikely(ret)) {
		printk("rdma_connect failed. ErrCode: %d\n", ret);
		goto err_destroy_qp;
	}

	wait_event_interruptible(this->eventWaitQ, this->connState != IBVSOCKETCONNSTATE_ROUTERESOLVED);

    return this;

err_destroy_qp:
	RingBuffer_free(this);
	rdma_destroy_qp(this->cm_id);

err_free_send_cq:
	ib_destroy_cq_user(this->sendCQ, NULL);

err_free_recv_cq:
	ib_destroy_cq_user(this->recvCQ, NULL);

err_dealloc_pd:
	ib_dealloc_pd(this->pd);

err_destroy_cm_id:
	rdma_destroy_id(this->cm_id);

err_free_this:
	kfree(this);

	return ERR_PTR(-EIO);
}

bool IBVSocket_destruct(struct IBVSocket *this) {
    if (this->cm_id != NULL) {
        rdma_disconnect(this->cm_id);
    }

    RingBuffer_free(this);

    if (this->qp != NULL) {
        rdma_destroy_qp(this->cm_id);
        this->qp = NULL;
    }
	
    if (this->sendCQ != NULL) {
        ib_destroy_cq_user(this->sendCQ, NULL);
        this->sendCQ = NULL;
    }
	
    if (this->recvCQ != NULL) {
        ib_destroy_cq_user(this->recvCQ, NULL);
        this->recvCQ = NULL;
    }

    if (this->pd != NULL) {
        ib_dealloc_pd(this->pd);
        this->pd = NULL;
    }

	if (this->cm_id != NULL) {
        rdma_destroy_id(this->cm_id);
        this->cm_id = NULL;
    }

    return true;
}

ssize_t IBVSocket_copy_restore(struct IBVSocket *this, struct iov_iter *iter, int index) {
	struct ib_recv_wr wr;
	const struct ib_recv_wr *bad_wr;
	struct ib_sge sge;
    int ret;
    ssize_t isize = 0;

	if (index < 0 || index >= BLOCK_NUM) {
		return -EINVAL;
	}

	isize = MIN(MSG_LEN, iter->iov->iov_len);
	
    memcpy(iter->iov->iov_base, this->recvBuf[index].pBuff, isize);
	RingBuffer_dealloc(this, false, index);

	sge.addr = this->recvBuf[index].dma_addr;
	sge.length = MSG_LEN;
	sge.lkey = this->pd->local_dma_lkey;
    wr.next = NULL;
	wr.wr_id = index;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	ret = ib_post_recv(this->qp, &wr, &bad_wr);
	if (unlikely(ret)) {
		printk("ib_post_recv failed. ErrCode: %d\n", ret);
		return -EIO;
	}

    return isize;
}

ssize_t IBVSocket_recvT(struct IBVSocket *this, struct iov_iter *iter) {
	struct ib_wc wc[8];
	int numElements;
	int i;
	int index = -1;

    do {
		if (this->connState != IBVSOCKETCONNSTATE_ESTABLISHED) {
			return -EIO;
		}
        numElements = ib_poll_cq(this->recvCQ, 8, wc);
        if (numElements > 0) {
            for (i = 0; i < numElements; i++) {
                //printk("recv status: %d, opcode: %d, wr_id: %lld\n", wc[i].status, wc[i].opcode, wc[i].wr_id);
				index = wc[i].wr_id;
				this->recvBuf[index].used = true;
            }
        } else if (numElements < 0) {
			printk("ib_poll_cq recvCQ failed. ErrCode: %d\n", numElements);
			return -EIO;
		}

		index = RingBuffer_alloc(this, false);
		if (index >= 0) {
			break;
		}
    } while(true);

    return IBVSocket_copy_restore(this, iter, index);
}

ssize_t IBVSocket_send(struct IBVSocket *this, struct iov_iter *iter) {
   	struct ib_send_wr send_wr;
	const struct ib_send_wr *send_bad_wr;
	struct ib_sge sge;
    int ret = 0;
	struct ib_wc wc[8];
	int numElements;
    int i = 0;
    ssize_t isize = 0;
	int index = -1;

	do {
		if (this->connState != IBVSOCKETCONNSTATE_ESTABLISHED) {
			return -EIO;
		}

		numElements = ib_poll_cq(this->sendCQ, 8, wc);
		if (numElements > 0) {
			for (i = 0; i < numElements; i++) {
				printk("send status: %d, opcode: %d, wr_id: %lld\n", wc[i].status, wc[i].opcode, wc[i].wr_id);
				RingBuffer_dealloc(this, true, wc[i].wr_id);
			}
		} else if (numElements < 0) {
			printk("ib_poll_cq sendCQ failed. ErrCode: %d\n", numElements);
			return -EIO;
		}

		index = RingBuffer_alloc(this, true);
		if (index >= 0) {
			break;
		}
	} while(true);

    isize = MIN(MSG_LEN, iter->iov->iov_len);
    memcpy(this->sendBuf[index].pBuff, iter->iov->iov_base, isize);

	sge.addr = this->sendBuf[index].dma_addr;
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
        printk("ib_post_send() failed. ErrCode: %d\n", ret);
        return -EIO;
    }

    return isize;
}
