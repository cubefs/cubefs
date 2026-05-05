#ifndef CUBEFS_RDMA_H
#define CUBEFS_RDMA_H

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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

/* Create a QP of type RC on the given CM id.
 * sq_sig_all=0: only signal when IBV_SEND_SIGNALED is set. */
static inline int cubefs_create_qp(
    struct rdma_cm_id  *id,
    struct ibv_pd      *pd,
    struct ibv_cq      *cq,
    uint32_t            max_send_wr)
{
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq        = cq;
    attr.recv_cq        = cq;
    attr.qp_type        = IBV_QPT_RC;
    attr.cap.max_send_wr  = max_send_wr;
    attr.cap.max_recv_wr  = 1;  /* One-sided: recv path not used */
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

#endif /* CUBEFS_RDMA_H */
