//go:build linux && rdma

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm
#include "rdma.h"
*/
import "C"

import (
	"fmt"
	"net"
	"unsafe"
)

const maxPollBatch = 16

// openDevice opens the RDMA device by name (e.g. "mlx5_0").
// If name is empty, the first available device is used.
func openDevice(name string) (*C.struct_ibv_context, error) {
	var numDevs C.int
	devList := C.ibv_get_device_list(&numDevs)
	if devList == nil || numDevs == 0 {
		return nil, fmt.Errorf("rdma: no devices found")
	}
	defer C.ibv_free_device_list(devList)

	devSlice := (*[1 << 10]*C.struct_ibv_device)(unsafe.Pointer(devList))[:numDevs:numDevs]
	for _, dev := range devSlice {
		devName := C.GoString(C.ibv_get_device_name(dev))
		if name == "" || devName == name {
			ctx := C.ibv_open_device(dev)
			if ctx == nil {
				return nil, fmt.Errorf("rdma: failed to open device %q", devName)
			}
			return ctx, nil
		}
	}
	return nil, fmt.Errorf("rdma: device %q not found", name)
}

// allocPD allocates a Protection Domain on ctx.
func allocPD(ctx *C.struct_ibv_context) (*C.struct_ibv_pd, error) {
	pd := C.ibv_alloc_pd(ctx)
	if pd == nil {
		return nil, fmt.Errorf("rdma: ibv_alloc_pd failed")
	}
	return pd, nil
}

// createCompChannel creates a completion channel bound to ctx. The channel
// is later associated with the conn's CQ; goroutines block on it via
// waitCQEvent for the P2 sleep phase.
func createCompChannel(ctx *C.struct_ibv_context) (*C.struct_ibv_comp_channel, error) {
	ch := C.cubefs_create_comp_channel(ctx)
	if ch == nil {
		return nil, fmt.Errorf("rdma: ibv_create_comp_channel failed")
	}
	return ch, nil
}

// destroyCompChannel tears down a completion channel. Idempotent on nil.
func destroyCompChannel(ch *C.struct_ibv_comp_channel) {
	if ch != nil {
		C.cubefs_destroy_comp_channel(ch)
	}
}

// createCQ creates a Completion Queue with at least size entries, bound to
// the given comp_channel so consumers can sleep on it.
//
// Pass channel=nil to retain the legacy "no notifications" behaviour; doing
// so disables the P2 sleep phase for that CQ.
func createCQ(ctx *C.struct_ibv_context, size int, channel *C.struct_ibv_comp_channel) (*C.struct_ibv_cq, error) {
	cq := C.cubefs_create_cq_with_channel(ctx, C.int(size), channel)
	if cq == nil {
		return nil, fmt.Errorf("rdma: ibv_create_cq failed")
	}
	return cq, nil
}

// reqNotifyCQ arms cq so the next completion fires an event on its
// comp_channel. solicitedOnly=false means any completion arms.
func reqNotifyCQ(cq *C.struct_ibv_cq, solicitedOnly bool) error {
	flag := C.int(0)
	if solicitedOnly {
		flag = 1
	}
	if errno := C.cubefs_req_notify_cq(cq, flag); errno != 0 {
		return fmt.Errorf("rdma: ibv_req_notify_cq failed: errno %d", errno)
	}
	return nil
}

// waitCQEvent blocks until a completion event arrives on ch. Caller is
// responsible for calling ackCQEvents on the returned CQ before re-arming.
func waitCQEvent(ch *C.struct_ibv_comp_channel) (*C.struct_ibv_cq, error) {
	var cq *C.struct_ibv_cq
	if errno := C.cubefs_get_cq_event(ch, &cq); errno != 0 {
		return nil, fmt.Errorf("rdma: ibv_get_cq_event failed: errno %d", errno)
	}
	return cq, nil
}

// ackCQEvents acknowledges n previously delivered events on cq. Required
// to release the kernel's reference; not calling it leaks events and
// eventually wedges the channel.
func ackCQEvents(cq *C.struct_ibv_cq, n uint) {
	C.cubefs_ack_cq_events(cq, C.uint(n))
}

// createQP creates an RC QP on id using the given pd and cq.
// maxSendWR / maxRecvWR are the queue depths.
func createQP(id *C.struct_rdma_cm_id, pd *C.struct_ibv_pd, cq *C.struct_ibv_cq, maxSendWR, maxRecvWR int) error {
	if ret := C.cubefs_create_qp(id, pd, cq, C.uint32_t(maxSendWR), C.uint32_t(maxRecvWR)); ret != 0 {
		return fmt.Errorf("rdma: rdma_create_qp failed: %d", ret)
	}
	return nil
}

// regMR registers a memory region for RDMA access.
// flags: IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
func regMR(pd *C.struct_ibv_pd, buf unsafe.Pointer, size int) (*C.struct_ibv_mr, error) {
	flags := C.IBV_ACCESS_LOCAL_WRITE | C.IBV_ACCESS_REMOTE_WRITE
	mr := C.ibv_reg_mr(pd, buf, C.size_t(size), C.int(flags))
	if mr == nil {
		return nil, fmt.Errorf("rdma: ibv_reg_mr failed (size=%d)", size)
	}
	return mr, nil
}

// deregMR deregisters and frees the MR.
func deregMR(mr *C.struct_ibv_mr) {
	C.ibv_dereg_mr(mr)
}

// postRDMAWrite posts a single RDMA Write WR to qp.
// signaled=true causes a CQE on completion.
func postRDMAWrite(qp *C.struct_ibv_qp, laddr uint64, lkey uint32, length uint32,
	raddr uint64, rkey uint32, wrID uint64, signaled bool) error {
	sig := C.int(0)
	if signaled {
		sig = 1
	}
	ret := C.cubefs_post_rdma_write(qp,
		C.uint64_t(laddr), C.uint32_t(lkey), C.uint32_t(length),
		C.uint64_t(raddr), C.uint32_t(rkey),
		C.uint64_t(wrID), sig)
	if ret != 0 {
		return fmt.Errorf("rdma: ibv_post_send failed: errno %d", ret)
	}
	return nil
}

// postRDMAWriteWithImm posts a single RDMA Write-with-Immediate WR. The
// immediate data is delivered to the peer's recv-side CQE, allowing the
// peer's polling goroutine to wake from a comp_channel block (P2 phase 3).
//
// The receiver MUST have at least one recv WR queued at the time the WR
// arrives; otherwise the QP transitions to ERR. Connection-level recv
// pool maintenance is handled in conn.go.
func postRDMAWriteWithImm(qp *C.struct_ibv_qp, laddr uint64, lkey uint32, length uint32,
	raddr uint64, rkey uint32, wrID uint64, immData uint32, signaled bool) error {
	sig := C.int(0)
	if signaled {
		sig = 1
	}
	ret := C.cubefs_post_rdma_write_with_imm(qp,
		C.uint64_t(laddr), C.uint32_t(lkey), C.uint32_t(length),
		C.uint64_t(raddr), C.uint32_t(rkey),
		C.uint64_t(wrID), C.uint32_t(immData), sig)
	if ret != 0 {
		return fmt.Errorf("rdma: ibv_post_send (with imm) failed: errno %d", ret)
	}
	return nil
}

// postRecv enqueues a recv WR. The dummy buffer (laddr/length) is required
// by the verbs API but unused for RDMA_WRITE_WITH_IMM completions, where
// the only useful information is the imm_data carried in the CQE.
func postRecv(qp *C.struct_ibv_qp, laddr uint64, lkey uint32, length uint32, wrID uint64) error {
	ret := C.cubefs_post_recv(qp,
		C.uint64_t(laddr), C.uint32_t(lkey), C.uint32_t(length),
		C.uint64_t(wrID))
	if ret != 0 {
		return fmt.Errorf("rdma: ibv_post_recv failed: errno %d", ret)
	}
	return nil
}

// CompletionEvent describes one drained CQE in a polling-friendly form so
// callers don't have to import "C" or know the IBV_WC_* enum values.
type CompletionEvent struct {
	WRID    uint64
	Status  int    // raw IBV_WC_* status; 0 = IBV_WC_SUCCESS
	IsRecv  bool   // true if opcode is IBV_WC_RECV / IBV_WC_RECV_RDMA_WITH_IMM
	HasImm  bool   // true when the completion carries a 32-bit imm_data
	ImmData uint32 // host order, valid only when HasImm
}

// Success reports whether the completion's NIC status is IBV_WC_SUCCESS.
// Non-success completions arrive on QP teardown ("flush" errors) and on
// genuine transport faults; callers should distinguish based on connection
// state rather than treating them as fatal here.
func (e CompletionEvent) Success() bool { return e.Status == 0 }

// pollCQEvents polls up to maxPollBatch completions. Unlike the legacy
// pollCQ helper it returns the full CompletionEvent slice including any
// non-success entries, leaving error policy to the caller (the drainer
// goroutine logs flush errors during teardown without aborting).
func pollCQEvents(cq *C.struct_ibv_cq) ([]CompletionEvent, error) {
	var wcs [maxPollBatch]C.struct_ibv_wc
	n := int(C.cubefs_poll_cq(cq, maxPollBatch, &wcs[0]))
	if n < 0 {
		return nil, fmt.Errorf("rdma: ibv_poll_cq failed: %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]CompletionEvent, 0, n)
	for i := 0; i < n; i++ {
		wc := &wcs[i]
		op := int(C.cubefs_wc_opcode(wc))
		ev := CompletionEvent{
			WRID:   uint64(wc.wr_id),
			Status: int(wc.status),
		}
		// IBV_WC_RECV (128) and IBV_WC_RECV_RDMA_WITH_IMM (129) both arrive
		// on the recv side. The numeric constants are stable in the verbs
		// ABI; comparing >= 128 (IBV_WC_RECV) is the canonical "is recv" check.
		if op >= int(C.IBV_WC_RECV) {
			ev.IsRecv = true
		}
		if C.cubefs_wc_has_imm(wc) != 0 {
			ev.HasImm = true
			ev.ImmData = uint32(C.cubefs_wc_imm_data(wc))
		}
		out = append(out, ev)
	}
	return out, nil
}

// pollCQ is retained for callers that only need the IDs of completed send
// WRs (legacy P0/P2 paths). Skips non-success completions for backward
// behavioural compatibility.
func pollCQ(cq *C.struct_ibv_cq) ([]uint64, error) {
	evs, err := pollCQEvents(cq)
	if err != nil {
		return nil, err
	}
	if len(evs) == 0 {
		return nil, nil
	}
	ids := make([]uint64, 0, len(evs))
	for _, e := range evs {
		if !e.Success() {
			return ids, fmt.Errorf("rdma: WC error status=%d wr_id=0x%x", e.Status, e.WRID)
		}
		ids = append(ids, e.WRID)
	}
	return ids, nil
}

// ---- rdma_cm helpers ----

func createEventChannel() (*C.struct_rdma_event_channel, error) {
	ch, err := C.rdma_create_event_channel()
	if ch == nil {
		// errno hints: ENOENT/ENODEV → rdma_ucm not loaded or /dev/infiniband/rdma_cm missing;
		// EACCES → device exists but permission denied (container device passthrough?).
		return nil, fmt.Errorf("rdma: rdma_create_event_channel failed: %w (check /dev/infiniband/rdma_cm and rdma_ucm module)", err)
	}
	return ch, nil
}

func destroyEventChannel(ch *C.struct_rdma_event_channel) {
	C.rdma_destroy_event_channel(ch)
}

func createCMID(ch *C.struct_rdma_event_channel) (*C.struct_rdma_cm_id, error) {
	var id *C.struct_rdma_cm_id
	ret := C.rdma_create_id(ch, &id, nil, C.RDMA_PS_TCP)
	if ret != 0 {
		return nil, fmt.Errorf("rdma: rdma_create_id failed: %d", ret)
	}
	return id, nil
}

func destroyCMID(id *C.struct_rdma_cm_id) {
	C.rdma_destroy_id(id)
}

// resolveAddr resolves the destination address. timeoutMS in milliseconds.
func resolveAddr(id *C.struct_rdma_cm_id, addr string, timeoutMS int) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("rdma: invalid addr %q: %w", addr, err)
	}
	dst := C.CString(host)
	defer C.free(unsafe.Pointer(dst))
	svc := C.CString(port)
	defer C.free(unsafe.Pointer(svc))

	var hints C.struct_addrinfo
	hints.ai_family = C.AF_INET
	hints.ai_socktype = C.SOCK_STREAM

	var res *C.struct_addrinfo
	if ret := C.getaddrinfo(dst, svc, &hints, &res); ret != 0 {
		return fmt.Errorf("rdma: getaddrinfo(%s:%s) failed: %d", host, port, ret)
	}
	defer C.freeaddrinfo(res)

	ret := C.rdma_resolve_addr(id, nil, res.ai_addr, C.int(timeoutMS))
	if ret != 0 {
		return fmt.Errorf("rdma: rdma_resolve_addr failed: %d", ret)
	}
	return waitCMEvent(id, C.RDMA_CM_EVENT_ADDR_RESOLVED)
}

// resolveRoute resolves the route to the destination. timeoutMS in milliseconds.
func resolveRoute(id *C.struct_rdma_cm_id, timeoutMS int) error {
	ret := C.rdma_resolve_route(id, C.int(timeoutMS))
	if ret != 0 {
		return fmt.Errorf("rdma: rdma_resolve_route failed: %d", ret)
	}
	return waitCMEvent(id, C.RDMA_CM_EVENT_ROUTE_RESOLVED)
}

// bindAndListen creates a server listening id on the given port.
func bindAndListen(ch *C.struct_rdma_event_channel, port int) (*C.struct_rdma_cm_id, error) {
	id, err := createCMID(ch)
	if err != nil {
		return nil, err
	}

	var sin C.struct_sockaddr_in
	sin.sin_family = C.AF_INET
	sin.sin_addr.s_addr = C.INADDR_ANY
	sin.sin_port = C.htons(C.uint16_t(port))

	if ret := C.rdma_bind_addr(id, (*C.struct_sockaddr)(unsafe.Pointer(&sin))); ret != 0 {
		C.rdma_destroy_id(id)
		return nil, fmt.Errorf("rdma: rdma_bind_addr port %d failed: %d", port, ret)
	}
	if ret := C.rdma_listen(id, 128); ret != 0 {
		C.rdma_destroy_id(id)
		return nil, fmt.Errorf("rdma: rdma_listen failed: %d", ret)
	}
	return id, nil
}

// getRequest waits for an incoming connection request on listenID.
// Returns the new cm_id for this connection and the client's private_data bytes.
func getRequest(listenID *C.struct_rdma_cm_id) (*C.struct_rdma_cm_id, []byte, error) {
	var event *C.struct_rdma_cm_event
	if ret := C.rdma_get_cm_event(listenID.channel, &event); ret != 0 {
		return nil, nil, fmt.Errorf("rdma: rdma_get_cm_event failed: %d", ret)
	}
	if event.event != C.RDMA_CM_EVENT_CONNECT_REQUEST {
		evType := event.event
		C.rdma_ack_cm_event(event)
		return nil, nil, fmt.Errorf("rdma: expected CONNECT_REQUEST, got %d", evType)
	}
	connID := event.id
	var privData []byte
	if cp := C.cubefs_event_conn_param(event); cp.private_data_len > 0 {
		privData = C.GoBytes(unsafe.Pointer(cp.private_data), C.int(cp.private_data_len))
	}
	C.rdma_ack_cm_event(event)
	return connID, privData, nil
}

// acceptConn accepts a connection on connID with AcceptInfo as private_data.
// Waits for RDMA_CM_EVENT_ESTABLISHED on connID's channel.
func acceptConn(connID *C.struct_rdma_cm_id, privData []byte) error {
	var param C.struct_rdma_conn_param
	if len(privData) > 0 {
		param.private_data = unsafe.Pointer(&privData[0])
		param.private_data_len = C.uint8_t(len(privData))
	}
	param.responder_resources = 1
	param.initiator_depth = 1
	param.retry_count = 7
	param.rnr_retry_count = 7

	if ret := C.rdma_accept(connID, &param); ret != 0 {
		return fmt.Errorf("rdma: rdma_accept failed: %d", ret)
	}
	return waitCMEvent(connID, C.RDMA_CM_EVENT_ESTABLISHED)
}

// connectTo establishes the RC connection to the server.
// privData is sent to the server; returns server's private_data bytes on ESTABLISHED.
func connectTo(id *C.struct_rdma_cm_id, privData []byte) ([]byte, error) {
	var param C.struct_rdma_conn_param
	if len(privData) > 0 {
		param.private_data = unsafe.Pointer(&privData[0])
		param.private_data_len = C.uint8_t(len(privData))
	}
	param.responder_resources = 1
	param.initiator_depth = 1
	param.retry_count = 7
	param.rnr_retry_count = 7

	if ret := C.rdma_connect(id, &param); ret != 0 {
		return nil, fmt.Errorf("rdma: rdma_connect failed: %d", ret)
	}

	var event *C.struct_rdma_cm_event
	if ret := C.rdma_get_cm_event(id.channel, &event); ret != 0 {
		return nil, fmt.Errorf("rdma: rdma_get_cm_event (ESTABLISHED) failed: %d", ret)
	}
	defer C.rdma_ack_cm_event(event)
	if event.event != C.RDMA_CM_EVENT_ESTABLISHED {
		return nil, fmt.Errorf("rdma: expected ESTABLISHED, got %d", event.event)
	}
	var serverData []byte
	if cp := C.cubefs_event_conn_param(event); cp.private_data_len > 0 {
		serverData = C.GoBytes(unsafe.Pointer(cp.private_data), C.int(cp.private_data_len))
	}
	return serverData, nil
}

// waitCMEvent reads one event from id's channel and verifies it matches expected.
func waitCMEvent(id *C.struct_rdma_cm_id, expected C.enum_rdma_cm_event_type) error {
	var event *C.struct_rdma_cm_event
	if ret := C.rdma_get_cm_event(id.channel, &event); ret != 0 {
		return fmt.Errorf("rdma: rdma_get_cm_event failed: %d", ret)
	}
	defer C.rdma_ack_cm_event(event)
	if event.event != expected {
		return fmt.Errorf("rdma: expected event %d, got %d", expected, event.event)
	}
	return nil
}

// getPDFromCMID returns the Protection Domain from an already-connected cm_id.
func getPDFromCMID(id *C.struct_rdma_cm_id) *C.struct_ibv_pd {
	return id.pd
}

// getQPFromCMID returns the QP from an already-configured cm_id.
func getQPFromCMID(id *C.struct_rdma_cm_id) *C.struct_ibv_qp {
	return id.qp
}

// getCQSizeAttr returns the CQ context pointer (for completion polling).
func getCTXFromCMID(id *C.struct_rdma_cm_id) *C.struct_ibv_context {
	return id.verbs
}
