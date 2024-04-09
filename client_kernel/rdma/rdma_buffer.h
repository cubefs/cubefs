#ifndef __CFS_RDMA_BUFFER_H__
#define __CFS_RDMA_BUFFER_H__

#include <linux/list.h>
#include <linux/types.h>
#include <linux/mutex.h>
#include <linux/wait.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>

#define BUFFER_4K_SIZE 4096
#define BUFFER_4K_NUM 100
#define BUFFER_128K_SIZE 128 * 1024
#define BUFFER_128K_NUM 100
#define BUFFER_1M_SIZE 1024 * 1024
#define BUFFER_1M_NUM 10

struct BufferItem {
	char *pBuff;
	u64 dma_addr;
	bool used;
	size_t size;
    struct list_head list;
    struct list_head all_list;
};

struct cfs_rdma_buffer {
    struct list_head lru;
    struct mutex lock;
    size_t size;
};

struct cfs_rdma_buffer_pool {
	struct rdma_cm_id *cm_id;
    wait_queue_head_t eventWaitQ;
	struct cfs_rdma_buffer buffer[3];
    struct list_head all_list;
    struct mutex all_lock;
};

int rdma_buffer_new(u32 rdma_port);
void rdma_buffer_release(void);
int rdma_buffer_get(struct BufferItem **item, size_t size);
void rdma_buffer_put(struct BufferItem *item);

#endif
