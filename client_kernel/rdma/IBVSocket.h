#ifndef OPENTK_IBVSOCKET_H_
#define OPENTK_IBVSOCKET_H_

#include <linux/in.h>
#include <linux/inet.h>
#include <linux/sched.h>
#include <linux/types.h>
#include <linux/wait.h>
#include <net/sock.h>
#include <net/inet_common.h>
#include <asm/atomic.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>

#include "IBVBuffer.h"
#include "iov_iter.h"

#define IBVSOCKET_PRIVATEDATA_STR "fhgfs0 " // must be exactly(!!) 8 bytes long
#define IBVSOCKET_PRIVATEDATA_STR_LEN 8
#define IBVSOCKET_PRIVATEDATA_PROTOCOL_VER 1

struct IBVIncompleteRecv;
typedef struct IBVIncompleteRecv IBVIncompleteRecv;
struct IBVIncompleteSend;
typedef struct IBVIncompleteSend IBVIncompleteSend;

struct IBVCommContext;
typedef struct IBVCommContext IBVCommContext;

struct IBVCommDest;
typedef struct IBVCommDest IBVCommDest;

struct IBVSocket; // forward declaration
typedef struct IBVSocket IBVSocket;

struct IBVCommConfig;
typedef struct IBVCommConfig IBVCommConfig;

enum IBVSocketConnState;
typedef enum IBVSocketConnState IBVSocketConnState_t;

// construction/destruction
extern __must_check bool IBVSocket_init(IBVSocket *_this);
extern void IBVSocket_uninit(IBVSocket *_this);
// static
extern bool IBVSocket_rdmaDevicesExist(void);

// methods
extern IBVSocket *IBVSocket_construct(void);
extern bool IBVSocket_connectByIP(IBVSocket *_this, struct sockaddr_in *sin);
extern bool IBVSocket_bindToAddr(IBVSocket *_this, struct in_addr *ipAddr,
				 unsigned short port);
extern bool IBVSocket_listen(IBVSocket *_this);
extern bool IBVSocket_shutdown(IBVSocket *_this);

extern ssize_t IBVSocket_recvT(IBVSocket *_this, struct iov_iter *iter,
			       int timeoutMS);
extern ssize_t IBVSocket_send(IBVSocket *_this, struct iov_iter *iter,
			      int flags);

extern int IBVSocket_checkConnection(IBVSocket *_this);

// getters & setters
extern void IBVSocket_setTypeOfService(IBVSocket *_this, int typeOfService);
extern void IBVSocket_setConnectionFailureStatus(IBVSocket *_this,
						 unsigned value);

extern bool __IBVSocket_createNewID(IBVSocket *_this);
extern bool __IBVSocket_createCommContext(IBVSocket *_this,
					  struct rdma_cm_id *cm_id,
					  IBVCommContext **outCommContext);
extern void __IBVSocket_cleanupCommContext(struct rdma_cm_id *cm_id,
					   IBVCommContext *commContext);

extern void __IBVSocket_initCommDest(IBVCommContext *commContext,
				     IBVCommDest *outDest);
extern bool __IBVSocket_parseCommDest(const void *buf, size_t bufLen,
				      IBVCommDest **outDest);

extern int __IBVSocket_receiveCheck(IBVSocket *_this, int timeoutMS);
extern int __IBVSocket_nonblockingSendCheck(IBVSocket *_this);

extern int __IBVSocket_postRecv(IBVSocket *_this, IBVCommContext *commContext,
				size_t bufIndex);
extern int __IBVSocket_postSend(IBVSocket *_this, size_t bufIndex);
extern int __IBVSocket_recvWC(IBVSocket *_this, int timeoutMS,
			      struct ib_wc *outWC);

extern int __IBVSocket_waitForRecvCompletionEvent(IBVSocket *_this,
						  int timeoutMS,
						  struct ib_wc *outWC);
extern int __IBVSocket_waitForSendCompletionEvent(IBVSocket *_this,
						  int oldSendCount,
						  int timeoutMS);
extern int __IBVSocket_waitForTotalSendCompletion(IBVSocket *_this,
						  unsigned *numSendElements,
						  unsigned *numWriteElements,
						  unsigned *numReadElements,
						  int timeoutMS);

extern ssize_t __IBVSocket_recvContinueIncomplete(IBVSocket *_this,
						  struct iov_iter *iter);

extern int __IBVSocket_cmaHandler(struct rdma_cm_id *cm_id,
				  struct rdma_cm_event *event);
extern void __IBVSocket_cqSendEventHandler(struct ib_event *event, void *data);
extern void __IBVSocket_sendCompletionHandler(struct ib_cq *cq,
					      void *cq_context);
extern void __IBVSocket_cqRecvEventHandler(struct ib_event *event, void *data);
extern void __IBVSocket_recvCompletionHandler(struct ib_cq *cq,
					      void *cq_context);
extern void __IBVSocket_qpEventHandler(struct ib_event *event, void *data);
extern int __IBVSocket_routeResolvedHandler(IBVSocket *_this,
					    struct rdma_cm_id *cm_id,
					    IBVCommContext **outCommContext);
extern int __IBVSocket_connectedHandler(IBVSocket *_this,
					struct rdma_cm_event *event);

struct ib_cq *__IBVSocket_createCompletionQueue(
	struct ib_device *device, ib_comp_handler comp_handler,
	void (*event_handler)(struct ib_event *, void *), void *cq_context,
	int cqe);

const char *__IBVSocket_wcStatusStr(int wcStatusCode);

struct IBVCommConfig {
	unsigned bufNum; // number of available buffers
	unsigned bufSize; // size of each buffer
};

enum IBVSocketConnState {
	IBVSOCKETCONNSTATE_UNCONNECTED = 0,
	IBVSOCKETCONNSTATE_CONNECTING = 1,
	IBVSOCKETCONNSTATE_ADDRESSRESOLVED = 2,
	IBVSOCKETCONNSTATE_ROUTERESOLVED = 3,
	IBVSOCKETCONNSTATE_ESTABLISHED = 4,
	IBVSOCKETCONNSTATE_FAILED = 5,
	IBVSOCKETCONNSTATE_REJECTED_STALE = 6
};

struct IBVIncompleteRecv {
	int isAvailable;
	int completedOffset;
	int bufIndex;
	int totalSize;
};

struct IBVIncompleteSend {
	unsigned numAvailable;
	bool forceWaitForAll; // true if we received only some completions and need
		//    to wait for the rest before we can send more data
};

struct IBVCommContext {
	struct ib_pd *pd; // protection domain
#ifndef OFED_UNSAFE_GLOBAL_RKEY
	struct ib_mr *dmaMR; // dma mem region keys
#endif

	atomic_t recvCompEventCount; // incremented on incoming event notification
	wait_queue_head_t recvCompWaitQ; // for recvCompEvents
	wait_queue_t recvWait;
	bool recvWaitInitialized; // true if init_wait was called for the thread
	atomic_t sendCompEventCount; // incremented on incoming event notification
	wait_queue_head_t sendCompWaitQ; // for sendCompEvents
	wait_queue_t sendWait;
	bool sendWaitInitialized; // true if init_wait was called for the thread

	struct ib_cq *recvCQ; // recv completion queue
	struct ib_cq *sendCQ; // send completion queue
	struct ib_qp *qp; // send+recv queue pair

	IBVCommConfig commCfg;
	struct IBVBuffer *sendBufs;
	struct IBVBuffer *recvBufs;
	struct IBVBuffer checkConBuffer;
	unsigned numReceivedBufsLeft; // flow control v2 to avoid IB rnr timeout
	unsigned numSendBufsLeft; // flow control v2 to avoid IB rnr timeout

	IBVIncompleteRecv incompleteRecv;
	IBVIncompleteSend incompleteSend;
};

#pragma pack(push, 1)
// Note: Make sure this struct has the same size on all architectures (because we use
//    sizeof(IBVCommDest) for private_data during handshake)
struct IBVCommDest {
	char verificationStr[IBVSOCKET_PRIVATEDATA_STR_LEN];
	uint64_t protocolVersion;
	uint64_t vaddr;
	unsigned rkey;
	unsigned recvBufNum;
	unsigned recvBufSize;
};
#pragma pack(pop)

struct IBVSocket {
	wait_queue_head_t
		eventWaitQ; // used to wait for connState change during connect

	struct rdma_cm_id *cm_id;

	IBVCommDest localDest;
	IBVCommDest *remoteDest;

	IBVCommContext *commContext;

	int errState; // 0 = <no error>; -1 = <unspecified error>

	volatile IBVSocketConnState_t connState;

	int typeOfService; // Set the type of service associated with the RDMA operations
	unsigned remapConnectionFailureStatus;
};

#endif /*OPENTK_IBVSOCKET_H_*/
