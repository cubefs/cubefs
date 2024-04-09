#ifndef __CFS_RDMA_SOCKET_H__
#define __CFS_RDMA_SOCKET_H__

#include "rdma/rdma_api.h"
#include "cfs_common.h"
#include "cfs_packet.h"
#include "cfs_socket.h"

#define CFS_RDMA_SOCKET_TIMEOUT 12000

int cfs_rdma_create(struct sockaddr_storage *ss, struct cfs_log *log,
		    struct cfs_socket **cskp, u32 rdma_port);
void cfs_rdma_release(struct cfs_socket *csk, bool forever);
int cfs_rdma_send_packet(struct cfs_socket *csk, struct cfs_packet *packet);
int cfs_rdma_recv_packet(struct cfs_socket *csk, struct cfs_packet *packet);
int cfs_rdma_module_init(void);
void cfs_rdma_module_exit(void);

#endif