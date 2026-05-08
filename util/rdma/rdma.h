#ifndef CUBEFS_RDMA_H
#define CUBEFS_RDMA_H

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>

/* Post a single RDMA Write WR to the QP.
 * signaled=1 → IBV_SEND_SIGNALED (generates a CQE on completion).
 * Returns 0 on success, errno on failure. */
static inline int cubefs_post_rdma_write(
    struct ibv_qp  *qp,
    uint64_t        laddr,
    uint32_t        lkey,
    uint32_t        len,
    uint64_t        raddr,
    uint32_t        rkey,
    uint64_t        wr_id,
    int             signaled)
{
    struct ibv_sge sge;
    sge.addr   = laddr;
    sge.length = len;
    sge.lkey   = lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id               = wr_id;
    wr.sg_list             = &sge;
    wr.num_sge             = 1;
    wr.opcode              = IBV_WR_RDMA_WRITE;
    wr.send_flags          = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey        = rkey;

    struct ibv_send_wr *bad_wr = NULL;
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    return ret ? errno : 0;
}

/* Post a single RDMA Write-with-Immediate WR. The receiver consumes one
 * recv WR and gets a CQE carrying imm_data, allowing the receiver to wake
 * from a comp_channel block. signaled is treated identically to plain
 * RDMA Write. Returns 0 on success, errno on failure. */
static inline int cubefs_post_rdma_write_with_imm(
    struct ibv_qp  *qp,
    uint64_t        laddr,
    uint32_t        lkey,
    uint32_t        len,
    uint64_t        raddr,
    uint32_t        rkey,
    uint64_t        wr_id,
    uint32_t        imm_data,
    int             signaled)
{
    struct ibv_sge sge;
    sge.addr   = laddr;
    sge.length = len;
    sge.lkey   = lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id               = wr_id;
    wr.sg_list             = &sge;
    wr.num_sge             = 1;
    wr.opcode              = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags          = signaled ? IBV_SEND_SIGNALED : 0;
    wr.imm_data            = htonl(imm_data);
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey        = rkey;

    struct ibv_send_wr *bad_wr = NULL;
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    return ret ? errno : 0;
}

/* Post a single recv WR with a 1-element SGE pointing at a small dummy
 * buffer. The buffer is needed even though we only care about the imm_data
 * in the resulting CQE (an RDMA_WRITE_WITH_IMM consumes a recv WR but does
 * not actually deliver bytes to the SGE). Returns 0 / errno. */
static inline int cubefs_post_recv(
    struct ibv_qp *qp,
    uint64_t       laddr,
    uint32_t       lkey,
    uint32_t       len,
    uint64_t       wr_id)
{
    struct ibv_sge sge;
    sge.addr   = laddr;
    sge.length = len;
    sge.lkey   = lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad_wr = NULL;
    int ret = ibv_post_recv(qp, &wr, &bad_wr);
    return ret ? errno : 0;
}

/* Create a QP of type RC on the given CM id.
 * sq_sig_all=0: only signal when IBV_SEND_SIGNALED is set.
 * max_recv_wr controls the size of the recv queue, sized to absorb one
 * incoming WRITE_WITH_IMM per outstanding slot. */
static inline int cubefs_create_qp(
    struct rdma_cm_id  *id,
    struct ibv_pd      *pd,
    struct ibv_cq      *cq,
    uint32_t            max_send_wr,
    uint32_t            max_recv_wr)
{
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq        = cq;
    attr.recv_cq        = cq;
    attr.qp_type        = IBV_QPT_RC;
    attr.cap.max_send_wr  = max_send_wr;
    attr.cap.max_recv_wr  = max_recv_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.sq_sig_all       = 0;
    return rdma_create_qp(id, pd, &attr);
}

/* Poll up to max completions from cq into wcs[].
 * Returns number of completions (>=0) or negative on error. */
static inline int cubefs_poll_cq(struct ibv_cq *cq, int max, struct ibv_wc *wcs)
{
    return ibv_poll_cq(cq, max, wcs);
}

/* Read imm_data from a completion in network byte order, returning host order.
 * Only valid when wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM and wc has the
 * IBV_WC_WITH_IMM flag set. */
static inline uint32_t cubefs_wc_imm_data(struct ibv_wc *wc)
{
    return ntohl(wc->imm_data);
}

/* Test whether a wc carries imm_data. Hides the bitflag check from cgo. */
static inline int cubefs_wc_has_imm(struct ibv_wc *wc)
{
    return (wc->wc_flags & IBV_WC_WITH_IMM) ? 1 : 0;
}

/* Return wc->opcode as a plain int so callers can compare against the
 * IBV_WC_* enum values without dragging the union into cgo. */
static inline int cubefs_wc_opcode(struct ibv_wc *wc)
{
    return (int)wc->opcode;
}

/* Create a completion channel bound to ctx. The channel can be associated
 * with one or more CQs; ibv_get_cq_event blocks until any associated CQ
 * fires a completion (after ibv_req_notify_cq has been called on it). */
static inline struct ibv_comp_channel *cubefs_create_comp_channel(struct ibv_context *ctx)
{
    return ibv_create_comp_channel(ctx);
}

static inline int cubefs_destroy_comp_channel(struct ibv_comp_channel *ch)
{
    return ibv_destroy_comp_channel(ch);
}

/* Create a CQ associated with comp_channel so callers can sleep on it.
 * If comp_channel is NULL, this behaves like the legacy ibv_create_cq path. */
static inline struct ibv_cq *cubefs_create_cq_with_channel(
    struct ibv_context        *ctx,
    int                        cqe,
    struct ibv_comp_channel   *channel)
{
    return ibv_create_cq(ctx, cqe, NULL, channel, 0);
}

/* Arm the CQ so the next completion fires an event on its comp_channel.
 * solicited_only=0 means any completion arms the channel.
 * Returns 0 on success, errno on failure. */
static inline int cubefs_req_notify_cq(struct ibv_cq *cq, int solicited_only)
{
    int ret = ibv_req_notify_cq(cq, solicited_only);
    return ret ? errno : 0;
}

/* Block until a completion event arrives on ch. Writes the firing CQ into
 * *cq_out (in this transport, every conn has exactly one CQ — but we keep
 * the API faithful to ibv_get_cq_event for clarity).
 * Returns 0 / errno. */
static inline int cubefs_get_cq_event(
    struct ibv_comp_channel  *ch,
    struct ibv_cq           **cq_out)
{
    void *unused_ctx = NULL;
    int ret = ibv_get_cq_event(ch, cq_out, &unused_ctx);
    return ret ? errno : 0;
}

/* Acknowledge n previously delivered events on cq. Caller MUST batch this
 * to avoid per-event lock contention; passing nevents=N after consuming N
 * events is idiomatic. */
static inline void cubefs_ack_cq_events(struct ibv_cq *cq, unsigned int nevents)
{
    ibv_ack_cq_events(cq, nevents);
}

/* Extract the conn param pointer from an event (param is a union; CGO cannot
 * access union fields directly — it represents them as [N]byte). */
static inline struct rdma_conn_param *cubefs_event_conn_param(struct rdma_cm_event *event) {
    return &event->param.conn;
}

#endif /* CUBEFS_RDMA_H */
