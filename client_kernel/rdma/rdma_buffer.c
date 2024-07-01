#include <linux/inet.h>
#include <linux/in.h>
#include "rdma_buffer.h"

static struct cfs_rdma_buffer_pool *rdma_pool = NULL;

int cfs_rdma_buffer_allocate(struct cfs_node **item, struct cfs_rdma_buffer *buffer) {
	struct cfs_node *tmp = NULL;

    // search the list.
    mutex_lock(&buffer->lock);
    if (!list_empty(&buffer->lru)) {
        tmp = list_first_entry(&buffer->lru, struct cfs_node, list);
        list_del(&tmp->list);
    }
    mutex_unlock(&buffer->lock);

    if (!tmp) {
        tmp = kzalloc(sizeof(*tmp), GFP_KERNEL);
        if (!tmp) {
            return -ENOMEM;
        }
        tmp->pBuff = kmalloc(buffer->size, GFP_KERNEL);
        if (!tmp->pBuff) {
            kfree(tmp);
            return -ENOMEM;
        }
        tmp->size = buffer->size;
        tmp->used = false;
        tmp->is_tmp = true;
        tmp->dma_addr = ib_dma_map_single(rdma_pool->cm_id->device, tmp->pBuff, buffer->size, DMA_BIDIRECTIONAL);
        mutex_lock(&rdma_pool->all_lock);
        list_add_tail(&tmp->all_list, &rdma_pool->all_list);
        mutex_unlock(&rdma_pool->all_lock);
    }
    *item = tmp;

    return 0;
}

int cfs_rdma_buffer_get(struct cfs_node **item, size_t size) {
    int index = -1;

    if (size <= BUFFER_4K_SIZE) {
        index = 0;
    } else if (size <= BUFFER_128K_SIZE) {
        index = 1;
    } else if (size <= BUFFER_1M_SIZE) {
        index = 2;
    } else {
        ibv_print_error("size=%ld > %d\n", size, BUFFER_1M_SIZE);
        return -EPERM;
    }

    return cfs_rdma_buffer_allocate(item, &(rdma_pool->buffer[index]));
}

void cfs_rdma_buffer_put(struct cfs_node *item) {
    struct cfs_rdma_buffer *buffer = NULL;
    int index = -1;

    if (item->size <= BUFFER_4K_SIZE) {
        index = 0;
    } else if (item->size <= BUFFER_128K_SIZE) {
        index = 1;
    } else if (item->size <= BUFFER_1M_SIZE) {
        index = 2;
    } else {
        ibv_print_error("size=%ld > %d\n", item->size, BUFFER_1M_SIZE);
        return;
    }
    if (item->is_tmp) {
        mutex_lock(&rdma_pool->all_lock);
        list_del(&item->all_list);
        mutex_unlock(&rdma_pool->all_lock);
        ib_dma_unmap_single(rdma_pool->cm_id->device, item->dma_addr, item->size, DMA_BIDIRECTIONAL);
        kfree(item->pBuff);
        kfree(item);
        return;
    }

    buffer = &(rdma_pool->buffer[index]);
    mutex_lock(&buffer->lock);
    list_add_tail(&item->list, &buffer->lru);
    mutex_unlock(&buffer->lock);
}

int cfs_rdma_buffer_event_handler(struct rdma_cm_id *cm_id, struct rdma_cm_event *event) {
	wake_up(&rdma_pool->event_wait_queue);
	return 0;
}

void cfs_rdma_buffer_free_all(void) {
	struct cfs_node *item = NULL;
	struct cfs_node *tmp = NULL;
    if (!rdma_pool) {
        return;
    }

    mutex_lock(&rdma_pool->all_lock);
	list_for_each_entry_safe(item, tmp, &rdma_pool->all_list, all_list) {
        if (!item) {
            continue;
        }
        list_del(&item->all_list);
        ib_dma_unmap_single(rdma_pool->cm_id->device, item->dma_addr, item->size, DMA_BIDIRECTIONAL);
        kfree(item->pBuff);
        kfree(item);
	}
    mutex_unlock(&rdma_pool->all_lock);
}

int cfs_rdma_buffer_create(struct cfs_rdma_buffer *buffer) {
    int i = 0;
    struct cfs_node *item = NULL;
    int buffer_num = 0;

    switch(buffer->size) {
        case BUFFER_4K_SIZE:
            buffer_num = BUFFER_4K_NUM;
            break;
        case BUFFER_128K_SIZE:
            buffer_num = BUFFER_128K_NUM;
            break;
        case BUFFER_1M_SIZE:
            buffer_num = BUFFER_1M_NUM;
            break;
        default:
            buffer_num = BUFFER_1M_NUM;
    }

    for (i = 0; i < buffer_num; i++) {
        item = kzalloc(sizeof(*item), GFP_KERNEL);
        if (!item) {
            return -ENOMEM;
        }
        item->pBuff = kmalloc(buffer->size, GFP_KERNEL);
        if (!item->pBuff) {
            kfree(item);
            return -ENOMEM;
        }
        item->size = buffer->size;
        item->used = false;
        item->is_tmp = false;
        item->dma_addr = ib_dma_map_single(rdma_pool->cm_id->device, item->pBuff, buffer->size, DMA_BIDIRECTIONAL);
        list_add_tail(&item->list, &buffer->lru);
        list_add_tail(&item->all_list, &rdma_pool->all_list);
    }
    return 0;
}

int cfs_rdma_buffer_new(void) {
    int ret;
    struct sockaddr_in sin;
    int i = 0;

    if (rdma_pool) {
        return 0;
    }

    rdma_pool = kzalloc(sizeof(*rdma_pool), GFP_KERNEL);
    if (!rdma_pool) {
        return -ENOMEM;
    }

    INIT_LIST_HEAD(&rdma_pool->all_list);
    mutex_init(&rdma_pool->all_lock);
    for (i = 0; i < 3; i++) {
        INIT_LIST_HEAD(&rdma_pool->buffer[i].lru);
        mutex_init(&rdma_pool->buffer[i].lock);
    }
    init_waitqueue_head(&rdma_pool->event_wait_queue);

    rdma_pool->cm_id = rdma_create_id(&init_net, cfs_rdma_buffer_event_handler, NULL, RDMA_PS_TCP, IB_QPT_RC);
    if (IS_ERR(rdma_pool->cm_id)) {
        ibv_print_error("rdma_create_id failed\n");
        return -EPERM;
    }

    sin.sin_family = AF_INET;
    sin.sin_port = htons(DEFAULT_RDMA_PORT);
    sin.sin_addr.s_addr = in_aton("127.0.0.1");
    ret = rdma_resolve_addr(rdma_pool->cm_id, NULL, (struct sockaddr *)&sin, 500);
    if (ret) {
        ibv_print_error("rdma_resolve_addr failed\n");
        ret = -EPERM;
        goto err_out;
    }

    wait_event_interruptible(rdma_pool->event_wait_queue, true);

    rdma_pool->buffer[0].size = BUFFER_4K_SIZE;
    rdma_pool->buffer[1].size = BUFFER_128K_SIZE;
    rdma_pool->buffer[2].size = BUFFER_1M_SIZE;

    for (i = 0; i < 3; i++) {
        ret = cfs_rdma_buffer_create(&(rdma_pool->buffer[i]));
        if (ret < 0) {
            ibv_print_error("cfs_rdma_buffer_create failed\n");
            goto err_out;
        }
    }

    return 0;

err_out:
    cfs_rdma_buffer_free_all();
    rdma_destroy_id(rdma_pool->cm_id);
    rdma_pool = NULL;
    return ret;
}

void cfs_rdma_buffer_release(void) {
    if (!rdma_pool) {
        return;
    }

    cfs_rdma_buffer_free_all();
    rdma_destroy_id(rdma_pool->cm_id);
    kfree(rdma_pool);
    rdma_pool = NULL;
}
