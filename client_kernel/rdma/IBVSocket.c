#include <linux/in.h>
#include <linux/sched.h>
#include <linux/poll.h>
#include <linux/socket.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>

#include "IBVSocket.h"

#define IBVSOCKET_CONN_TIMEOUT_MS 5000
#define IBVSOCKET_COMPLETION_TIMEOUT_MS \
	300000 /* this also includes send completion wait times */
#define IBVSOCKET_STALE_RETRIES_NUM 128

#define ibv_print_info printk
#define ibv_print_info_ir printk
#define ibv_pwarn printk
#define ibv_print_info_debug printk
#define ibv_print_info_ir_debug printk
#define OFED_UNSAFE_GLOBAL_RKEY
#define OFED_SPLIT_WR

/* 4.19 added const qualifiers to ib_post_send and ib_post_recv. */
typedef __typeof__(__builtin_choose_expr(
	__builtin_types_compatible_p(__typeof__(&ib_post_send),
				     int (*)(struct ib_qp *, struct ib_send_wr *,
					     struct ib_send_wr **)),
	(struct ib_send_wr *)0, (const struct ib_send_wr *)0)) _bad_send_wr;
typedef __typeof__(__builtin_choose_expr(
	__builtin_types_compatible_p(__typeof__(&ib_post_recv),
				     int (*)(struct ib_qp *, struct ib_recv_wr *,
					     struct ib_recv_wr **)),
	(struct ib_recv_wr *)0, (const struct ib_recv_wr *)0)) _bad_recv_wr;

long IBV_msToJiffiesSchedulable(unsigned ms)
{
	unsigned long resultJiffies = msecs_to_jiffies(ms);

	return (resultJiffies >= MAX_SCHEDULE_TIMEOUT) ?
		       (MAX_SCHEDULE_TIMEOUT - 1) :
		       resultJiffies;
}

bool IBVSocket_init(IBVSocket *_this)
{
	memset(_this, 0, sizeof(*_this));

	_this->connState = IBVSOCKETCONNSTATE_UNCONNECTED;

	_this->typeOfService = 0;

	init_waitqueue_head(&_this->eventWaitQ);

	return __IBVSocket_createNewID(_this);
}

void IBVSocket_uninit(IBVSocket *_this)
{
	SAFE_KFREE(_this->remoteDest);

	if (_this->commContext)
		__IBVSocket_cleanupCommContext(_this->cm_id,
					       _this->commContext);

	if (_this->cm_id)
		rdma_destroy_id(_this->cm_id);
}

IBVSocket *IBVSocket_construct(void)
{
	IBVSocket *this = kmalloc(sizeof(IBVSocket), GFP_NOFS);

	if (!this || !IBVSocket_init(this)) {
		kfree(this);
		return NULL;
	}

	return this;
}

bool IBVSocket_rdmaDevicesExist()
{
	// Note: We use this (currently) just to inform the higher levels
	//    about availability of RDMA functionality
	return true;
}

/**
 * Create a new cm_id.
 * This is not only intended for new sockets, but also for stale cm_ids, so this can cleanup/replace
 * existing cm_ids and resets error states.
 */
bool __IBVSocket_createNewID(IBVSocket *_this)
{
	struct rdma_cm_id *new_cm_id;

	// We need to unconditionally destroy the old CM id.  It is unusable at this point.
	if (_this->cm_id) {
		rdma_destroy_id(_this->cm_id);
		_this->cm_id = NULL;
	}

#if defined(OFED_HAS_NETNS) || defined(rdma_create_id)
	new_cm_id = rdma_create_id(&init_net, __IBVSocket_cmaHandler, _this,
				   RDMA_PS_TCP, IB_QPT_RC);
#elif defined(OFED_HAS_RDMA_CREATE_QPTYPE)
	new_cm_id = rdma_create_id(__IBVSocket_cmaHandler, _this, RDMA_PS_TCP,
				   IB_QPT_RC);
#else
	new_cm_id = rdma_create_id(__IBVSocket_cmaHandler, _this, RDMA_PS_TCP);
#endif

	if (IS_ERR(new_cm_id)) {
		ibv_print_info("rdma_create_id failed. ErrCode: %ld\n",
			       PTR_ERR(new_cm_id));
		return false;
	}

	_this->cm_id = new_cm_id;

	_this->connState = IBVSOCKETCONNSTATE_UNCONNECTED;
	_this->errState = 0;

	return true;
}

bool IBVSocket_connectByIP(IBVSocket *_this, struct sockaddr_in *sin)
{
	/* note: rejected as stale means remote side still had an old open connection associated with
         our current cm_id. what most likely happened is that the client was reset (i.e. no clean
         disconnect) and our new cm_id after reboot now matches one of the old previous cm_ids.
         => only possible solution seems to be retrying with another cm_id. */
	int numStaleRetriesLeft = IBVSOCKET_STALE_RETRIES_NUM;

	for (;;) // stale retry loop
	{
// set type of service for this connection
#ifdef OFED_HAS_SET_SERVICE_TYPE
		if (_this->typeOfService)
			rdma_set_service_type(_this->cm_id,
					      _this->typeOfService);
#endif // OFED_HAS_SET_SERVICE_TYPE

		/* note: the rest of the connect procedure is invoked through the cmaHandler when the
            corresponding asynchronous events arrive => we just have to wait for the connState
            to change here */

		_this->connState = IBVSOCKETCONNSTATE_CONNECTING;

		// resolve IP address ...
		if (rdma_resolve_addr(_this->cm_id, NULL,
				      (struct sockaddr *)&sin,
				      IBVSOCKET_CONN_TIMEOUT_MS)) {
			ibv_print_info_debug("rdma_resolve_addr failed\n");
			goto err_invalidateSock;
		}

		// wait for async event
		wait_event_interruptible(_this->eventWaitQ,
					 _this->connState !=
						 IBVSOCKETCONNSTATE_CONNECTING);
		if (_this->connState != IBVSOCKETCONNSTATE_ADDRESSRESOLVED)
			goto err_invalidateSock;

		if (rdma_resolve_route(_this->cm_id,
				       IBVSOCKET_CONN_TIMEOUT_MS)) {
			ibv_print_info_debug("rdma_resolve_route failed.\n");
			goto err_invalidateSock;
		}

		wait_event_interruptible(
			_this->eventWaitQ,
			_this->connState != IBVSOCKETCONNSTATE_ADDRESSRESOLVED);
		if (_this->connState != IBVSOCKETCONNSTATE_ROUTERESOLVED)
			goto err_invalidateSock;

		// establish connection...

		// (handler calls rdma_connect() )
		if (__IBVSocket_routeResolvedHandler(_this, _this->cm_id,
						     &_this->commContext)) {
			ibv_print_info_debug("route resolved handler failed\n");
			goto err_invalidateSock;
		}

		// wait for async event
		wait_event_interruptible(
			_this->eventWaitQ,
			_this->connState != IBVSOCKETCONNSTATE_ROUTERESOLVED);

		// test point for failed connections
		if ((_this->connState != IBVSOCKETCONNSTATE_ESTABLISHED) &&
		    (_this->remapConnectionFailureStatus != 0))
			_this->connState = _this->remapConnectionFailureStatus;

		// check if cm_id was reported as stale by remote side
		if (_this->connState == IBVSOCKETCONNSTATE_REJECTED_STALE) {
			bool createIDRes;

			if (!numStaleRetriesLeft) { // no more stale retries left
				if (IBVSOCKET_STALE_RETRIES_NUM) // did we have any retries at all
					ibv_print_info(
						"Giving up after %d stale connection retries\n",
						IBVSOCKET_STALE_RETRIES_NUM);

				goto err_invalidateSock;
			}

			// We need to clean up the commContext created in the routeResolvedHandler because
			// the next time through the loop it will get recreated.  If this is the final try,
			// then we don't need it anymore.
			__IBVSocket_cleanupCommContext(_this->cm_id,
						       _this->commContext);
			_this->commContext = NULL;

			printk("Stale connection detected. Retrying with a new one...\n");
			createIDRes = __IBVSocket_createNewID(_this);
			if (!createIDRes)
				goto err_invalidateSock;

			numStaleRetriesLeft--;
			continue;
		}

		if (_this->connState != IBVSOCKETCONNSTATE_ESTABLISHED)
			goto err_invalidateSock;

		// connected

		if (numStaleRetriesLeft != IBVSOCKET_STALE_RETRIES_NUM) {
			ibv_print_info_debug(
				"Succeeded after %d stale connection retries\n",
				IBVSOCKET_STALE_RETRIES_NUM -
					numStaleRetriesLeft);
		}

		return true;
	}

err_invalidateSock:
	// If we have a comm context, we need to delete it since we can't use it.
	if (_this->commContext) {
		__IBVSocket_cleanupCommContext(_this->cm_id,
					       _this->commContext);
		_this->commContext = NULL;
	}

	_this->errState = -1;
	return false;
}

bool IBVSocket_bindToAddr(IBVSocket *_this, struct in_addr *ipAddr,
			  unsigned short port)
{
	struct sockaddr_in bindAddr;

	bindAddr.sin_family = AF_INET;
	bindAddr.sin_addr = *ipAddr;
	bindAddr.sin_port = htons(port);

	if (rdma_bind_addr(_this->cm_id, (struct sockaddr *)&bindAddr)) {
		_this->errState = -1;
		return false;
	}

	return true;
}

/**
 * @return true on success
 */
bool IBVSocket_listen(IBVSocket *_this)
{
	if (rdma_listen(_this->cm_id, 0)) {
		ibv_print_info("rdma_listen failed\n");
		_this->errState = -1;
		return false;
	}

	return true;
}

bool IBVSocket_shutdown(IBVSocket *_this)
{
	IBVCommContext *commContext = _this->commContext;
	unsigned numWaitWrites = 0;
	unsigned numWaitReads = 0;
	int timeoutMS = 250;

	if (_this->errState)
		return true; // true, because the conn is down anyways

	if (!commContext)
		return true; // this socket has never been connected

	if (commContext->incompleteSend
		    .numAvailable) { // wait for all incomplete sends
		int waitRes;

		waitRes = __IBVSocket_waitForTotalSendCompletion(
			_this, &commContext->incompleteSend.numAvailable,
			&numWaitWrites, &numWaitReads, timeoutMS);
		if (waitRes < 0) {
			ibv_print_info_debug(
				"Waiting for incomplete send requests failed\n");
			return false;
		}
	}

	return true;
}

/**
 * Continues an incomplete former recv() by returning immediately available data from the
 * corresponding buffer.
 */
ssize_t __IBVSocket_recvContinueIncomplete(IBVSocket *_this,
					   struct iov_iter *iter)
{
	IBVCommContext *commContext = _this->commContext;
	struct IBVIncompleteRecv *recv = &commContext->incompleteRecv;
	size_t bufIndex = recv->bufIndex;
	struct IBVBuffer *buffer = &commContext->recvBufs[bufIndex];
	size_t copyRes = 0;
	ssize_t total = 0;

	if (unlikely(_this->errState))
		return -1;

	while (iter->count > 0 && recv->totalSize != recv->completedOffset) {
		unsigned page = recv->completedOffset / buffer->bufferSize;
		unsigned offset = recv->completedOffset % buffer->bufferSize;
		unsigned fragment =
			MIN(MIN(iter->count, buffer->bufferSize - offset),
			    recv->totalSize - recv->completedOffset);

		copyRes = copy_to_iter(buffer->buffers[page] + offset, fragment,
				       iter);
		if (copyRes != fragment) {
			copyRes = 0;
			break;
		}

		total += fragment;

		recv->completedOffset += fragment;
	}

	if (recv->completedOffset == recv->totalSize) {
		int postRes;

		commContext->incompleteRecv.isAvailable = 0;

		postRes = __IBVSocket_postRecv(_this, _this->commContext,
					       bufIndex);
		if (unlikely(postRes))
			goto err_invalidateSock;
	}

	if (!copyRes)
		goto err_fault;

	return total;

err_invalidateSock:
	ibv_print_info_debug("invalidating connection\n");

err_fault:
	_this->errState = -1;
	return -EFAULT;
}

/**
 * @return number of received bytes on success, -ETIMEDOUT on timeout, -ECOMM on error
 */
ssize_t IBVSocket_recvT(IBVSocket *_this, struct iov_iter *iter, int timeoutMS)
{
	int checkRes;
	int wait = timeoutMS < 0 ? 1000 * 1000 : timeoutMS;

	do {
		checkRes = __IBVSocket_receiveCheck(_this, wait);
	} while (checkRes == 0 && timeoutMS < 0);

	if (checkRes < 0)
		return -ECOMM;

	if (checkRes == 0)
		return -ETIMEDOUT;

	return __IBVSocket_recvContinueIncomplete(_this, iter);
}

/**
 * @flags supports MSG_DONTWAIT
 * @return number of bytes sent or negative error code (-EAGAIN in case of MSG_DONTWAIT if no data
 * could be sent without blocking)
 */
ssize_t IBVSocket_send(IBVSocket *_this, struct iov_iter *iter, int flags)
{
	IBVCommContext *commContext = _this->commContext;
	size_t currentBufIndex;
	struct iov_iter source = *iter;
	int postRes;
	size_t postedLen = 0;
	ssize_t currentPostLen;
	int waitRes;
	unsigned numWaitWrites = 0;
	unsigned numWaitReads = 0;
	int timeoutMS = IBVSOCKET_COMPLETION_TIMEOUT_MS;

	if (unlikely(_this->errState))
		return -1;

	// handle flags
	if (flags &
	    MSG_DONTWAIT) { // send only as much as we can without blocking

		// note: we adapt the bufLen variable as necessary here for simplicity

		int checkSendRes;
		size_t bufNumLeft;
		size_t bufLenLeft;

		checkSendRes = __IBVSocket_nonblockingSendCheck(_this);
		if (!checkSendRes) { // we can't send non-blocking at the moment, caller shall try again later
			return -EAGAIN;
		} else if (unlikely(checkSendRes < 0))
			goto err_invalidateSock;

		// buffers available => adapt bufLen (if necessary)

		bufNumLeft =
			MIN(commContext->commCfg.bufNum -
				    commContext->incompleteSend.numAvailable,
			    commContext->numSendBufsLeft);
		bufLenLeft = bufNumLeft * commContext->commCfg.bufSize;

		iov_iter_truncate(&source, bufLenLeft);
	}

	// send data cut in buf-sized pieces...

	do {
		// note: we only poll for completed sends if forced or after we used up all (!) available bufs

		if (commContext->incompleteSend.forceWaitForAll ||
		    (commContext->incompleteSend.numAvailable ==
		     commContext->commCfg
			     .bufNum)) { // wait for all (!) incomplete sends
			waitRes = __IBVSocket_waitForTotalSendCompletion(
				_this,
				&commContext->incompleteSend.numAvailable,
				&numWaitWrites, &numWaitReads, timeoutMS);
			if (waitRes <= 0)
				goto err_invalidateSock;

			commContext->incompleteSend.forceWaitForAll = false;
		}

		currentBufIndex = commContext->incompleteSend.numAvailable;

		currentPostLen = IBVBuffer_fill(
			&commContext->sendBufs[currentBufIndex], &source);
		if (currentPostLen < 0)
			goto err_fault;

		commContext->incompleteSend
			.numAvailable++; /* inc'ed before postSend() for conn checks */

		postRes = __IBVSocket_postSend(_this, currentBufIndex);
		if (unlikely(postRes)) {
			commContext->incompleteSend.numAvailable--;
			goto err_invalidateSock;
		}

		postedLen += currentPostLen;
	} while (source.count);

	iov_iter_advance(iter, postedLen);
	return (ssize_t)postedLen;

err_invalidateSock:
	_this->errState = -1;
	return -ECOMM;

err_fault:
	_this->errState = -1;
	return -EFAULT;
}

void IBVSocket_setTypeOfService(IBVSocket *_this, int typeOfService)
{
	_this->typeOfService = typeOfService;
}

void IBVSocket_setConnectionFailureStatus(IBVSocket *_this, unsigned value)
{
	_this->remapConnectionFailureStatus = value;
}

bool __IBVSocket_createCommContext(IBVSocket *_this, struct rdma_cm_id *cm_id,
				   IBVCommContext **outCommContext)
{
	IBVCommContext *commContext;
	struct ib_qp_init_attr qpInitAttr;
	int qpRes;
	unsigned i;
	IBVCommConfig *commCfg;

	commContext = kzalloc(sizeof(*commContext), GFP_KERNEL);
	if (!commContext)
		goto err_cleanup;
	ibv_print_info_debug("Alloc CommContext @ %px\n", commContext);

	// prepare recv and send event notification

	init_waitqueue_head(&commContext->recvCompWaitQ);
	init_waitqueue_head(&commContext->sendCompWaitQ);

	atomic_set(&commContext->recvCompEventCount, 0);
	atomic_set(&commContext->sendCompEventCount, 0);

	// protection domain...

#ifndef OFED_UNSAFE_GLOBAL_RKEY
	commContext->pd = ib_alloc_pd(cm_id->device);
#else
	commContext->pd = ib_alloc_pd(cm_id->device, IB_PD_UNSAFE_GLOBAL_RKEY);
#endif
	if (IS_ERR(commContext->pd)) {
		ibv_print_info("Couldn't allocate PD. ErrCode: %ld\n",
			       PTR_ERR(commContext->pd));

		commContext->pd = NULL;
		goto err_cleanup;
	}

	// DMA system mem region...

	// (Note: IB spec says:
	//    "The consumer is not allowed to assign remote-write (or remote-atomic) to
	//    a memory region that has not been assigned local-write.")

#ifndef OFED_UNSAFE_GLOBAL_RKEY
	commContext->dmaMR = ib_get_dma_mr(
		commContext->pd, IB_ACCESS_LOCAL_WRITE | IB_ACCESS_REMOTE_READ |
					 IB_ACCESS_REMOTE_WRITE);
	if (IS_ERR(commContext->dmaMR)) {
		ibv_print_info("ib_get_dma_mr failed. ErrCode: %ld\n",
			       PTR_ERR(commContext->dmaMR));

		commContext->dmaMR = NULL;
		goto err_cleanup;
	}
#endif

	// alloc and register buffers...
	commContext->commCfg.bufNum = 128;
	commContext->commCfg.bufSize = 4 * 1024;
	commCfg = &commContext->commCfg;

	commContext->recvBufs =
		kzalloc(commCfg->bufNum * sizeof(struct IBVBuffer), GFP_KERNEL);
	if (!commContext->recvBufs) {
		ibv_print_info("couldn't prepare receive buffer list\n");
		goto err_cleanup;
	}

	for (i = 0; i < commCfg->bufNum; i++) {
		if (!IBVBuffer_init(&commContext->recvBufs[i], commContext,
				    commCfg->bufSize)) {
			ibv_print_info("couldn't prepare recvBuf #%d\n", i + 1);
			goto err_cleanup;
		}
	}

	commContext->sendBufs =
		kzalloc(commCfg->bufNum * sizeof(struct IBVBuffer), GFP_KERNEL);
	if (!commContext->sendBufs) {
		ibv_print_info("couldn't prepare send buffer list\n");
		goto err_cleanup;
	}

	for (i = 0; i < commCfg->bufNum; i++) {
		if (!IBVBuffer_init(&commContext->sendBufs[i], commContext,
				    commCfg->bufSize)) {
			ibv_print_info("couldn't prepare sendBuf #%d\n", i + 1);
			goto err_cleanup;
		}
	}

	if (!IBVBuffer_init(&commContext->checkConBuffer, commContext,
			    sizeof(u64))) {
		ibv_print_info("couldn't alloc dma control memory region\n");
		goto err_cleanup;
	}

	/* note: we use -1 because the last buf might not be read by the user (eg during
      nonblockingRecvCheck) and so it might not be immediately available again. */
	commContext->numReceivedBufsLeft = commCfg->bufNum - 1;
	commContext->numSendBufsLeft = commCfg->bufNum - 1;

	// create completion queues...

	commContext->recvCQ = __IBVSocket_createCompletionQueue(
		cm_id->device, __IBVSocket_recvCompletionHandler,
		__IBVSocket_cqRecvEventHandler, _this, commCfg->bufNum);
	if (IS_ERR(commContext->recvCQ)) {
		ibv_print_info("couldn't create recv CQ. ErrCode: %ld\n",
			       PTR_ERR(commContext->recvCQ));

		commContext->recvCQ = NULL;
		goto err_cleanup;
	}

	// note: 1+commCfg->bufNum here for the checkConnection() RDMA read
	commContext->sendCQ = __IBVSocket_createCompletionQueue(
		cm_id->device, __IBVSocket_sendCompletionHandler,
		__IBVSocket_cqSendEventHandler, _this, 1 + commCfg->bufNum);
	if (IS_ERR(commContext->sendCQ)) {
		ibv_print_info("couldn't create send CQ. ErrCode: %ld\n",
			       PTR_ERR(commContext->sendCQ));

		commContext->sendCQ = NULL;
		goto err_cleanup;
	}

	// note: 1+commCfg->bufNum here for the checkConnection() RDMA read
	memset(&qpInitAttr, 0, sizeof(qpInitAttr));

	qpInitAttr.event_handler = __IBVSocket_qpEventHandler;
	qpInitAttr.send_cq = commContext->sendCQ;
	qpInitAttr.recv_cq = commContext->recvCQ;
	qpInitAttr.qp_type = IB_QPT_RC;
	qpInitAttr.sq_sig_type = IB_SIGNAL_REQ_WR;
	qpInitAttr.cap.max_send_wr = 1 + commCfg->bufNum;
	qpInitAttr.cap.max_recv_wr = commCfg->bufNum;
	qpInitAttr.cap.max_send_sge = commCfg->bufSize / IBV_FRAGMENT_SIZE + 1;
	qpInitAttr.cap.max_recv_sge = commCfg->bufSize / IBV_FRAGMENT_SIZE + 1;
	qpInitAttr.cap.max_inline_data = 0;

	qpRes = rdma_create_qp(cm_id, commContext->pd, &qpInitAttr);
	if (qpRes) {
		ibv_print_info("couldn't create QP. ErrCode: %d\n", qpRes);
		goto err_cleanup;
	}

	commContext->qp = cm_id->qp;

	// post initial recv buffers...

	for (i = 0; i < commCfg->bufNum; i++) {
		if (__IBVSocket_postRecv(_this, commContext, i)) {
			ibv_print_info(
				"couldn't post recv buffer with index %d\n", i);
			goto err_cleanup;
		}
	}

	// prepare event notification...

	// initial event notification requests
	if (ib_req_notify_cq(commContext->recvCQ, IB_CQ_NEXT_COMP)) {
		ibv_print_info("couldn't request CQ notification\n");
		goto err_cleanup;
	}

	if (ib_req_notify_cq(commContext->sendCQ, IB_CQ_NEXT_COMP)) {
		ibv_print_info("couldn't request CQ notification\n");
		goto err_cleanup;
	}

	*outCommContext = commContext;
	return true;

	//  error handling

err_cleanup:
	__IBVSocket_cleanupCommContext(cm_id, commContext);

	*outCommContext = NULL;
	return false;
}

void __IBVSocket_cleanupCommContext(struct rdma_cm_id *cm_id,
				    IBVCommContext *commContext)
{
	unsigned i;
	struct ib_device *dev;

	if (!commContext)
		return;

	dev = commContext->pd ? commContext->pd->device : NULL;

	if (!dev)
		goto cleanup_no_dev;

	if (cm_id && commContext->qp && cm_id->qp)
		rdma_destroy_qp(cm_id);

	if (commContext->sendCQ)
#ifdef OFED_IB_DESTROY_CQ_IS_VOID
		ib_destroy_cq(commContext->sendCQ);
#else
	{
		int destroyRes = ib_destroy_cq(commContext->sendCQ);
		if (unlikely(destroyRes)) {
			ibv_pwarn("sendCQ destroy failed: %d\n", destroyRes);
			dump_stack();
		}
	}
#endif

	if (commContext->recvCQ)
#ifdef OFED_IB_DESTROY_CQ_IS_VOID
		ib_destroy_cq(commContext->recvCQ);
#else
	{
		int destroyRes = ib_destroy_cq(commContext->recvCQ);
		if (unlikely(destroyRes)) {
			ibv_pwarn("recvCQ destroy failed: %d\n", destroyRes);
			dump_stack();
		}
	}
#endif

	IBVBuffer_free(&commContext->checkConBuffer, commContext);

	for (i = 0; i < commContext->commCfg.bufNum; i++) {
		if (commContext->recvBufs)
			IBVBuffer_free(&commContext->recvBufs[i], commContext);

		if (commContext->sendBufs)
			IBVBuffer_free(&commContext->sendBufs[i], commContext);
	}

	SAFE_KFREE(commContext->recvBufs);
	SAFE_KFREE(commContext->sendBufs);

#ifndef OFED_UNSAFE_GLOBAL_RKEY
	if (commContext->dmaMR)
		ib_dereg_mr(commContext->dmaMR);
#endif

	if (commContext->pd)
		ib_dealloc_pd(commContext->pd);

cleanup_no_dev:

	ibv_print_info_debug("Free CommContext @ %px\n", commContext);
	kfree(commContext);
}

/**
 * Initializes a (local) IBVCommDest.
 */
void __IBVSocket_initCommDest(IBVCommContext *commContext, IBVCommDest *outDest)
{
	memcpy(outDest->verificationStr, IBVSOCKET_PRIVATEDATA_STR,
	       IBVSOCKET_PRIVATEDATA_STR_LEN);
	outDest->protocolVersion =
		cpu_to_le64(IBVSOCKET_PRIVATEDATA_PROTOCOL_VER);
#ifndef OFED_UNSAFE_GLOBAL_RKEY
	outDest->rkey = cpu_to_le32(commContext->dmaMR->rkey);
#else
	outDest->rkey = cpu_to_le32(commContext->pd->unsafe_global_rkey);
#endif
	outDest->vaddr = cpu_to_le64(commContext->checkConBuffer.lists[0].addr);
	outDest->recvBufNum = cpu_to_le32(commContext->commCfg.bufNum);
	outDest->recvBufSize = cpu_to_le32(commContext->commCfg.bufSize);
}

/**
 * Parses and checks a (remote) IBVCommDest.
 *
 * @param buf should usually be the private_data of the connection handshake
 * @param outDest will be kmalloced (if true is returned) and needs to be kfree'd by the
 * caller
 * @return true if data is okay, false otherwise
 */
bool __IBVSocket_parseCommDest(const void *buf, size_t bufLen,
			       IBVCommDest **outDest)
{
	const IBVCommDest *src = buf;
	IBVCommDest *dest = NULL;

	// Note: "bufLen < ..." (and not "!="), because there might be some extra padding
	if (!buf || (bufLen < sizeof(*dest))) {
		ibv_print_info("Bad private data size. length: %zu\n", bufLen);
		return false;
	}

	dest = kmalloc(sizeof(*dest), GFP_ATOMIC);
	if (!dest)
		return false;

	*outDest = dest;
	*dest = *src;

	dest->protocolVersion = le64_to_cpu(dest->protocolVersion);
	dest->vaddr = le64_to_cpu(dest->vaddr);
	dest->rkey = le32_to_cpu(dest->rkey);
	dest->recvBufNum = le32_to_cpu(dest->recvBufNum);
	dest->recvBufSize = le32_to_cpu(dest->recvBufSize);

	if (memcmp(dest->verificationStr, IBVSOCKET_PRIVATEDATA_STR,
		   IBVSOCKET_PRIVATEDATA_STR_LEN))
		goto err_cleanup;

	if (dest->protocolVersion != IBVSOCKET_PRIVATEDATA_PROTOCOL_VER)
		goto err_cleanup;

	return true;

err_cleanup:
	kfree(dest);
	*outDest = NULL;
	return false;
}

/**
 * Append buffer to receive queue.
 *
 * @param commContext passed seperately because it's not the _this->commContext during
 *    accept() of incoming connections
 * @return 0 on success, -1 on error
 */
int __IBVSocket_postRecv(IBVSocket *_this, IBVCommContext *commContext,
			 size_t bufIndex)
{
	struct ib_recv_wr wr;
	_bad_recv_wr bad_wr;
	int postRes;

	commContext->sendBufs[bufIndex].lists[0].length =
		commContext->commCfg.bufSize;

	wr.next = NULL;
	wr.wr_id = bufIndex;
	wr.sg_list = commContext->recvBufs[bufIndex].lists;
	wr.num_sge = commContext->recvBufs[bufIndex].listLength;

	postRes = ib_post_recv(commContext->qp, &wr, &bad_wr);
	if (unlikely(postRes)) {
		ibv_print_info("ib_post_recv failed. ErrCode: %d\n", postRes);

		return -1;
	}

	return 0;
}

int IBVSocket_checkConnection(IBVSocket *_this)
{
	IBVCommContext *commContext = _this->commContext;
#ifdef OFED_SPLIT_WR
#define rdma_of(wr) (wr)
#define wr_of(wr) (wr.wr)
	struct ib_rdma_wr wr;
#else
#define rdma_of(wr) (wr.wr.rdma)
#define wr_of(wr) (wr)
	struct ib_send_wr wr;
#endif
	_bad_send_wr bad_wr;
	int postRes;
	int waitRes;
	int timeoutMS = IBVSOCKET_COMPLETION_TIMEOUT_MS;
	unsigned numWaitWrites = 0;
	unsigned numWaitReads = 1;

	rdma_of(wr).remote_addr = _this->remoteDest->vaddr;
	rdma_of(wr).rkey = _this->remoteDest->rkey;

	wr_of(wr).wr_id = 0;
	wr_of(wr).sg_list = commContext->checkConBuffer.lists;
	wr_of(wr).num_sge = commContext->checkConBuffer.listLength;
	wr_of(wr).opcode = IB_WR_RDMA_READ;
	wr_of(wr).send_flags = IB_SEND_SIGNALED;
	wr_of(wr).next = NULL;

	postRes = ib_post_send(commContext->qp, &wr_of(wr), &bad_wr);
	if (unlikely(postRes)) {
		ibv_print_info("ib_post_send() failed. ErrCode: %d\n", postRes);

		goto error;
	}

	waitRes = __IBVSocket_waitForTotalSendCompletion(
		_this, &commContext->incompleteSend.numAvailable,
		&numWaitWrites, &numWaitReads, timeoutMS);
	if (unlikely(waitRes <= 0))
		goto error;

	commContext->incompleteSend.forceWaitForAll = false;

	return 0;

#undef rdma_of
#undef wr_of

error:
	_this->errState = -1;
	return -1;
}

/**
 * Note: 
 *
 * @return 0 on success, -1 on error
 */
int __IBVSocket_postSend(IBVSocket *_this, size_t bufIndex)
{
	IBVCommContext *commContext = _this->commContext;
	struct ib_send_wr wr;
	_bad_send_wr bad_wr;
	int postRes;

	wr.wr_id = bufIndex;
	wr.sg_list = commContext->sendBufs[bufIndex].lists;
	wr.num_sge = commContext->sendBufs[bufIndex].listLength;
	wr.opcode = IB_WR_SEND;
	wr.send_flags = IB_SEND_SIGNALED;
	wr.next = NULL;

	postRes = ib_post_send(commContext->qp, &wr, &bad_wr);
	if (unlikely(postRes)) {
		ibv_print_info("ib_post_send() failed. ErrCode: %d\n", postRes);

		return -1;
	}

	return 0;
}

/**
 * Receive work completion.
 *
 * Note:
 *
 * @param timeoutMS 0 for non-blocking
 * @return 1 on success, 0 on timeout, <0 on error
 */
int __IBVSocket_recvWC(IBVSocket *_this, int timeoutMS, struct ib_wc *outWC)
{
	IBVCommContext *commContext = _this->commContext;
	int waitRes;
	size_t bufIndex;

	waitRes =
		__IBVSocket_waitForRecvCompletionEvent(_this, timeoutMS, outWC);
	if (waitRes <=
	    0) { // (note: waitRes==0 can often happen, because we call this with timeoutMS==0)

		if (unlikely(waitRes < 0)) {
			if (waitRes !=
			    -ERESTARTSYS) { // only print message if user didn't press "<CTRL>-C"
				ibv_print_info(
					"retrieval of completion event failed. result: %d\n",
					waitRes);
			}
		}

		return waitRes;
	}

	// we got something...

	if (unlikely(outWC->status != IB_WC_SUCCESS)) {
		printk("%s: Connection error (wc_status: %d; msg: %s)\n",
		       "IBVSocket (recv work completion)", (int)outWC->status,
		       __IBVSocket_wcStatusStr(outWC->status));
		return -1;
	}

	bufIndex = outWC->wr_id;

	if (unlikely(bufIndex >= commContext->commCfg.bufNum)) {
		ibv_print_info("Completion for unknown/invalid wr_id %d\n",
			       (int)outWC->wr_id);
		return -1;
	}

	// receive completed
	return 1;
}

/**
 * @return 1 on available data, 0 on timeout, <0 on error
 */
int __IBVSocket_waitForRecvCompletionEvent(IBVSocket *_this, int timeoutMS,
					   struct ib_wc *outWC)
{
	IBVCommContext *commContext = _this->commContext;
	long waitRes;
	int numEvents = 0;
	int checkRes;

	// special quick path: other than in the userspace version of this method, we only need the
	//    quick path when timeoutMS==0, because then we might have been called from a special
	//    context, in which we don't want to sleep
	if (!timeoutMS)
		return ib_poll_cq(commContext->recvCQ, 1, outWC);

	while (timeoutMS != 0) {
		/* note: we use pollTimeoutMS to check the conn every few secs (otherwise we might
         wait for a very long time in case the other side disconnected silently) */

		int pollTimeoutMS = MIN(10000, timeoutMS);
		long pollTimeoutJiffies =
			IBV_msToJiffiesSchedulable(pollTimeoutMS);

		/* note: don't think about ib_peek_cq here, because it is not implemented in the drivers. */

		waitRes = wait_event_timeout(
			commContext->recvCompWaitQ,
			(numEvents = ib_poll_cq(commContext->recvCQ, 1, outWC)),
			pollTimeoutJiffies);

		if (unlikely(waitRes == -ERESTARTSYS ||
			     fatal_signal_pending(current))) { // signal pending
			ibv_print_info_debug(
				"wait for recvCompEvent ended by pending signal\n");

			return waitRes;
		}

		if (likely(numEvents)) { // we got something
			return numEvents;
		}

		// timeout

		checkRes = IBVSocket_checkConnection(_this);
		if (checkRes < 0)
			return -ECONNRESET;

		timeoutMS -= pollTimeoutMS;
	} // end of for-loop

	return 0;
}

/**
 * @param oldSendCount old sendCompEventCount
 * @return 1 on available data, 0 on timeout, -1 on error
 */
int __IBVSocket_waitForSendCompletionEvent(IBVSocket *_this, int oldSendCount,
					   int timeoutMS)
{
	IBVCommContext *commContext = _this->commContext;
	long waitRes;

	while (timeoutMS != 0) {
		// Note: We use pollTimeoutMS to check the conn every few secs (otherwise we might
		//    wait for a very long time in case the other side disconnected silently)
		int pollTimeoutMS = MIN(10000, timeoutMS);
		long pollTimeoutJiffies =
			IBV_msToJiffiesSchedulable(pollTimeoutMS);

		waitRes = wait_event_timeout(
			commContext->sendCompWaitQ,
			atomic_read(&commContext->sendCompEventCount) !=
				oldSendCount,
			pollTimeoutJiffies);

		if (unlikely(waitRes == -ERESTARTSYS ||
			     fatal_signal_pending(current))) { // signal pending
			ibv_print_info_debug(
				"wait for sendCompEvent ended by pending signal\n");

			return -1;
		}

		if (likely(atomic_read(&commContext->sendCompEventCount) !=
			   oldSendCount))
			return 1;

		// timeout
		timeoutMS -= pollTimeoutMS;
	}

	return 0;
}

/**
 * @param numSendElements also used as out-param to return the remaining number
 * @param timeoutMS 0 for non-blocking; this is a soft timeout that is reset after each received
 * completion
 * @return 1 if all completions received, 0 if completions missing (in case you wanted non-blocking)
 * or -1 in case of an error.
 */
int __IBVSocket_waitForTotalSendCompletion(IBVSocket *_this,
					   unsigned *numSendElements,
					   unsigned *numWriteElements,
					   unsigned *numReadElements,
					   int timeoutMS)
{
	IBVCommContext *commContext = _this->commContext;
	int numElements;
	int waitRes;
	int oldSendCount;
	int i;
	size_t bufIndex;
	struct ib_wc wc[2];

	do {
		oldSendCount = atomic_read(&commContext->sendCompEventCount);

		numElements = ib_poll_cq(commContext->sendCQ, 2, wc);
		if (unlikely(numElements < 0)) {
			ibv_print_info("bad ib_poll_cq result: %d\n",
				       numElements);

			return -1;
		} else if (!numElements) { // no completions available yet => wait
			if (!timeoutMS)
				return 0;

			waitRes = __IBVSocket_waitForSendCompletionEvent(
				_this, oldSendCount, timeoutMS);
			if (likely(waitRes > 0))
				continue;

			return waitRes;
		}

		// we got something...

		// for each completion element
		for (i = 0; i < numElements; i++) {
			if (unlikely(wc[i].status != IB_WC_SUCCESS)) {
				printk("%s: Connection error (wc_status: %d; msg: %s)\n",
				       "IBVSocket (wait for total send completion)",
				       (int)(wc[i].status),
				       __IBVSocket_wcStatusStr(wc[i].status));

				return -1;
			}

			switch (wc[i].opcode) {
			case IB_WC_SEND: {
				bufIndex = wc[i].wr_id;

				if (unlikely(bufIndex >=
					     commContext->commCfg.bufNum)) {
					ibv_print_info(
						"bad send completion wr_id 0x%x\n",
						(int)wc[i].wr_id);

					return -1;
				}

				if (likely(*numSendElements))
					(*numSendElements)--;
				else {
					ibv_print_info(
						"received bad/unexpected send completion\n");

					return -1;
				}

			} break;

			case IB_WC_RDMA_READ: {
				if (unlikely(wc[i].wr_id != 0)) {
					ibv_print_info(
						"bad read completion wr_id 0x%x\n",
						(int)wc[i].wr_id);

					return -1;
				}

				if (likely(*numReadElements))
					(*numReadElements)--;
				else {
					ibv_print_info(
						"received bad/unexpected RDMA read completion\n");

					return -1;
				}
			} break;

			default: {
				ibv_print_info(
					"received bad/unexpected completion opcode %d\n",
					wc[i].opcode);

				return -1;
			} break;

			} // end of switch

		} // end of for-loop

	} while (*numSendElements || *numWriteElements || *numReadElements);

	return 1;
}

/**
 * @return <0 on error, 0 if recv would block, >0 if recv would not block
 */
int __IBVSocket_receiveCheck(IBVSocket *_this, int timeoutMS)
{
	IBVCommContext *commContext = _this->commContext;
	struct ib_wc wc;
	int recvRes;

	if (unlikely(_this->errState))
		return -1;

	if (commContext->incompleteRecv.isAvailable)
		return 1;

	// recv one packet (if available) and add it as incompleteRecv
	recvRes = __IBVSocket_recvWC(_this, timeoutMS, &wc);
	if (unlikely(recvRes < 0)) {
		ibv_print_info_debug(
			"got an error from __IBVSocket_recvWC(). ErrCode: %d\n",
			recvRes);
		goto err_invalidateSock;
	}

	if (!recvRes)
		return 0;

	// we got something => prepare to continue later

	commContext->incompleteRecv.totalSize = wc.byte_len;
	commContext->incompleteRecv.bufIndex = wc.wr_id;
	commContext->incompleteRecv.completedOffset = 0;
	commContext->incompleteRecv.isAvailable = 1;

	return 1;

err_invalidateSock:
	ibv_print_info_debug("invalidating connection\n");
	_this->errState = -1;
	return -1;
}

/**
 * @return <0 on error, 0 if send would block, >0 if send would not block
 */
int __IBVSocket_nonblockingSendCheck(IBVSocket *_this)
{
	IBVCommContext *commContext = _this->commContext;
	int waitRes;
	unsigned numWaitWrites = 0;
	unsigned numWaitReads = 0;
	int timeoutMS = 0;

	if (unlikely(_this->errState))
		return -1;

	if (!commContext->incompleteSend.forceWaitForAll &&
	    (commContext->incompleteSend.numAvailable <
	     commContext->commCfg.bufNum))
		return 1;

	commContext->incompleteSend.forceWaitForAll =
		true; // always setting saves an "if" below

	// we have to wait for completions before we can send...

	waitRes = __IBVSocket_waitForTotalSendCompletion(
		_this, &commContext->incompleteSend.numAvailable,
		&numWaitWrites, &numWaitReads, timeoutMS);
	if (unlikely(waitRes < 0))
		goto err_invalidateSock;

	if (waitRes > 0)
		commContext->incompleteSend.forceWaitForAll =
			false; // no more completions peding

	return waitRes;

err_invalidateSock:
	ibv_print_info_debug("invalidating connection\n");

	_this->errState = -1;
	return -1;
}

/**
 * Handle connection manager event callbacks from interrupt handler.
 *
 * Note: Sleeping (e.g. mutex locking) is not allowed in callbacks like this one!
 *
 * @return negative Linux error code on error, 0 otherwise; in case of return!=0, rdma_cm will
 * automatically call rdma_destroy_id().
 */
int __IBVSocket_cmaHandler(struct rdma_cm_id *cm_id,
			   struct rdma_cm_event *event)
{
	IBVSocket *_this = cm_id->context;
	int retVal = 0;

	if (unlikely(!_this)) {
		ibv_print_info_ir_debug("cm_id is being torn down. Event: %d\n",
					event->event);
		return (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) ?
			       -EINVAL :
			       0;
	}

	ibv_print_info_ir_debug("rdma event: %i, status: %i\n", event->event,
				event->status);

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		_this->connState = IBVSOCKETCONNSTATE_ADDRESSRESOLVED;
		break;

	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
		retVal = -ENETUNREACH;
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		_this->connState = IBVSOCKETCONNSTATE_ROUTERESOLVED;
		break;

	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
		retVal = -ETIMEDOUT;
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
// incoming connections not supported => reject all
#ifdef OFED_RDMA_REJECT_NEEDS_REASON
		rdma_reject(cm_id, NULL, 0, 0);
#else
		rdma_reject(cm_id, NULL, 0);
#endif // OFED_RDMA_REJECT_NEEDS_REASON
		break;

	case RDMA_CM_EVENT_CONNECT_RESPONSE:
		retVal = rdma_accept(cm_id, NULL);
		break;

	case RDMA_CM_EVENT_REJECTED:
		retVal = event->status == IB_CM_REJ_STALE_CONN ? -ESTALE :
								 -ECONNREFUSED;
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		retVal = __IBVSocket_connectedHandler(_this, event);
		_this->connState = IBVSOCKETCONNSTATE_ESTABLISHED;
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		rdma_disconnect(cm_id);
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		// note: all associated ressources have to be released here immediately
		__IBVSocket_cleanupCommContext(cm_id, _this->commContext);
		_this->commContext = NULL;
		_this->cm_id = NULL;
		retVal = -ENETRESET;
		goto exit;

	default:
		ibv_print_info_ir_debug("Ignoring RDMA_CMA event: %d\n",
					event->event);
		break;
	}

	if (unlikely(retVal)) {
		if (retVal == -ESTALE)
			_this->connState = IBVSOCKETCONNSTATE_REJECTED_STALE;
		else
			_this->connState = IBVSOCKETCONNSTATE_FAILED;

		_this->errState = -1;
		// free connection resources later. freeing everything here may race with send/recv
		// operations in case of a connection breakage.
		retVal = 0;
	}

exit:
	wake_up(&_this->eventWaitQ);
	return retVal;
}

/**
 * Invoked when an asynchronous event not associated with a completion occurs on the CQ.
 */
void __IBVSocket_cqSendEventHandler(struct ib_event *event, void *data)
{
	ibv_print_info_ir_debug("called. event type: %d (not handled)\n",
				event->event);
}

/**
 * Invoked when a completion event occurs on the CQ.
 *
 * @param cq_context IBVSocket* _this
 */
void __IBVSocket_sendCompletionHandler(struct ib_cq *cq, void *cq_context)
{
	IBVSocket *_this = cq_context;
	IBVCommContext *commContext = _this->commContext;
	int reqNotifySendRes;

	atomic_inc(&commContext->sendCompEventCount);

	reqNotifySendRes =
		ib_req_notify_cq(commContext->sendCQ, IB_CQ_NEXT_COMP);
	if (unlikely(reqNotifySendRes))
		ibv_print_info_ir("Couldn't request CQ notification\n");

	wake_up(&commContext->sendCompWaitQ);
}

/**
 * Invoked when an asynchronous event not associated with a completion occurs on the CQ.
 */
void __IBVSocket_cqRecvEventHandler(struct ib_event *event, void *data)
{
	ibv_print_info_ir_debug("called. event type: %d (not handled)\n",
				event->event);
}

/**
 * Invoked when a completion event occurs on the CQ.
 *
 * @param cq_context IBVSocket* _this
 */
void __IBVSocket_recvCompletionHandler(struct ib_cq *cq, void *cq_context)
{
	IBVSocket *_this = cq_context;
	IBVCommContext *commContext = _this->commContext;
	int reqNotifyRecvRes;

	atomic_inc(&commContext->recvCompEventCount);

	reqNotifyRecvRes =
		ib_req_notify_cq(commContext->recvCQ, IB_CQ_NEXT_COMP);
	if (unlikely(reqNotifyRecvRes))
		ibv_print_info_ir("Couldn't request CQ notification\n");

	wake_up(&commContext->recvCompWaitQ);
}

void __IBVSocket_qpEventHandler(struct ib_event *event, void *data)
{
	ibv_print_info_ir_debug("called. event type: %d (not handled)\n",
				event->event);
}

int __IBVSocket_routeResolvedHandler(IBVSocket *_this, struct rdma_cm_id *cm_id,
				     IBVCommContext **outCommContext)
{
	bool createContextRes;
	struct rdma_conn_param conn_param;

	createContextRes = __IBVSocket_createCommContext(_this, _this->cm_id,
							 &_this->commContext);
	if (!createContextRes) {
		ibv_print_info_ir("creation of CommContext failed\n");
		_this->errState = -1;

		return -EPERM;
	}

	// establish connection...

	__IBVSocket_initCommDest(_this->commContext, &_this->localDest);

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.flow_control = 0;
	conn_param.retry_count = 7; // (3 bits)
	conn_param.rnr_retry_count =
		7; // rnr = receiver not ready (3 bits, 7 means infinity)
	conn_param.private_data = &_this->localDest;
	conn_param.private_data_len = sizeof(_this->localDest);

	return rdma_connect(_this->cm_id, &conn_param);
}

int __IBVSocket_connectedHandler(IBVSocket *_this, struct rdma_cm_event *event)
{
	int retVal = 0;
	bool parseCommDestRes;

	const void *private_data;
	u8 private_data_len;

#if defined BEEGFS_OFED_1_2_API && (BEEGFS_OFED_1_2_API == 1)
	private_data = event->private_data;
	private_data_len = event->private_data_len;
#else // OFED 1.2.5 or higher API
	private_data = event->param.conn.private_data;
	private_data_len = event->param.conn.private_data_len;
#endif

	parseCommDestRes = __IBVSocket_parseCommDest(
		private_data, private_data_len, &_this->remoteDest);
	if (!parseCommDestRes) {
		ibv_print_info_ir("bad private data received. len: %d\n",
				  private_data_len);

		retVal = -EOPNOTSUPP;
		goto err_invalidateSock;
	}

	return retVal;

err_invalidateSock:
	_this->errState = -1;

	return retVal;
}

struct ib_cq *__IBVSocket_createCompletionQueue(
	struct ib_device *device, ib_comp_handler comp_handler,
	void (*event_handler)(struct ib_event *, void *), void *cq_context,
	int cqe)
{
#if defined(BEEGFS_OFED_1_2_API) && BEEGFS_OFED_1_2_API >= 1
	return ib_create_cq(device, comp_handler, event_handler, cq_context,
			    cqe);
#elif defined OFED_HAS_IB_CREATE_CQATTR || defined ib_create_cq
	struct ib_cq_init_attr attrs = {
		.cqe = cqe,
		.comp_vector = get_random_int() % device->num_comp_vectors,
	};

	return ib_create_cq(device, comp_handler, event_handler, cq_context,
			    &attrs);
#else // OFED 1.2.5 or higher API
	return ib_create_cq(device, comp_handler, event_handler, cq_context,
			    cqe, 0);
#endif
}

/**
 * @return pointer to static buffer with human readable string for a wc status code
 */
const char *__IBVSocket_wcStatusStr(int wcStatusCode)
{
	switch (wcStatusCode) {
	case IB_WC_WR_FLUSH_ERR:
		return "work request flush error";
	case IB_WC_RETRY_EXC_ERR:
		return "retries exceeded error";
	case IB_WC_RESP_TIMEOUT_ERR:
		return "response timeout error";

	default:
		return "<undefined>";
	}
}
