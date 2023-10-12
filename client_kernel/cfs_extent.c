#include "cfs_extent.h"
#include "cfs_meta.h"

/**
 * Max number of items per btree+ node
 */
#define EXTENT_ITEM_COUNT_PRE_BTREE_NODE 128

#define EXTENT_RECV_TIMEOUT_MS 5000u

#define EXTENT_BLOCK_COUNT 1024UL
#define EXTENT_BLOCK_SIZE 131072UL
#define EXTENT_SIZE (EXTENT_BLOCK_COUNT * EXTENT_BLOCK_SIZE)

#define EXTENT_UPDATE_DP_INTERVAL_MS 5 * 60 * 1000u

static struct workqueue_struct *extent_work_queue;

static void __page_io_set(struct page *page, size_t bytes)
{
	atomic_long_t *unwritten = (atomic_long_t *)&page_private(page);

	// cfs_log_debug("page(%lu) writeback start\n", page->index);
	SetPagePrivate(page);
	atomic_long_set(unwritten, bytes);
}

static void __page_io_clear(struct page *page)
{
	// cfs_log_debug("page(%lu) writeback end\n", page->index);
	ClearPagePrivate(page);
}

static void __page_io_bytes(struct page *page, size_t bytes)
{
	atomic_long_t *unwritten = (atomic_long_t *)&page_private(page);
	atomic_long_sub(bytes, unwritten);
}

static bool __page_io_done(struct page *page)
{
	atomic_long_t *unwritten = (atomic_long_t *)&page_private(page);
	return atomic_long_read(unwritten) == 0;
}

struct page_iter {
	struct page **pages;
	size_t nr_pages;
	size_t first_page_offset;
	size_t last_page_size;
};

static void page_iter_init(struct page_iter *iter, struct page **pages,
			   size_t nr_pages, size_t first_page_offset,
			   size_t last_page_size)
{
	iter->pages = pages;
	iter->nr_pages = nr_pages;
	iter->first_page_offset = first_page_offset;
	iter->last_page_size = last_page_size;
}

static size_t page_iter_size(struct page_iter *iter)
{
	if (iter->nr_pages == 0)
		return 0;
	return (iter->nr_pages - 1) * PAGE_CACHE_SIZE -
	       iter->first_page_offset + iter->last_page_size;
}

static void page_iter_advance(struct page_iter *iter, size_t size)
{
	size_t page_offset;

	BUG_ON(page_iter_size(iter) < size);

	page_offset = iter->first_page_offset + size;
	while (page_offset >= PAGE_CACHE_SIZE) {
		page_offset -= PAGE_CACHE_SIZE;
		iter->pages++;
		iter->nr_pages--;
	}
	iter->first_page_offset = page_offset;
}

static size_t page_copy(const struct page_iter *src, struct page_frag *dst,
			size_t nr_dst, size_t *size)
{
	size_t i;
	size_t copied = 0;
	size_t page_offset = src->first_page_offset;

	if (*size == 0 || src->nr_pages == 0 || nr_dst == 0)
		return 0;
	for (i = 0; i < src->nr_pages && i < nr_dst; i++) {
		dst[i].page = src->pages[i];
		dst[i].offset = page_offset;
		if (i + 1 == src->nr_pages)
			dst[i].size = src->last_page_size - page_offset;
		else
			dst[i].size = PAGE_CACHE_SIZE - page_offset;
		copied += dst[i].size;
		if (copied >= *size) {
			dst[i].size -= copied - *size;
			copied = *size;
			break;
		}
		page_offset = 0;
	}
	*size = copied;
	return i + 1;
}

/**
 * @param nr_dst [in] the number of dst iovec
 * @param size [in/out] copy/copied size
 * @return the copied number of dst iovec
 */
static size_t iov_copy(const struct iov_iter *src, struct iovec *dst,
		       size_t nr_dst, size_t *size)
{
	size_t i;
	size_t offset = src->iov_offset;
	size_t copied = 0;

	for (i = 0; i < src->nr_segs && i < nr_dst && copied < *size; i++) {
		dst[i].iov_base = src->iov[i].iov_base + offset;
		dst[i].iov_len = src->iov[i].iov_len - offset;
		if (copied + dst[i].iov_len > *size)
			dst[i].iov_len = *size - copied;
		copied += dst[i].iov_len;
		offset += dst[i].iov_len;
		if (offset == src->iov[i].iov_len)
			offset = 0;
	}
	*size = copied;
	return i;
}

enum extent_write_type {
	EXTENT_WRITE_TYPE_RANDOM = 0,
	EXTENT_WRITE_TYPE_TINY,
	EXTENT_WRITE_TYPE_NORMAL,
};

struct extent_io_info {
	struct list_head list;
	struct cfs_packet_extent ext;
	loff_t offset; /* request file offset */
	size_t size; /* request file size */
	bool hole;
};

static struct extent_io_info *
extent_io_info_new(loff_t offset, size_t size,
		   const struct cfs_packet_extent *ext)
{
	struct extent_io_info *io_info;

	io_info = kzalloc(sizeof(*io_info), GFP_NOFS);
	if (!io_info)
		return NULL;
	if (ext)
		io_info->ext = *ext;
	else
		io_info->hole = true;
	io_info->offset = offset;
	io_info->size = size;
	return io_info;
}

static enum extent_write_type extent_io_type(struct extent_io_info *io_info)
{
	if (!io_info->hole)
		return EXTENT_WRITE_TYPE_RANDOM;
	if (io_info->offset > 0 || io_info->offset + io_info->size > 1048576)
		return EXTENT_WRITE_TYPE_NORMAL;
	return EXTENT_WRITE_TYPE_TINY;
}

static int extent_item_cmp(const void *a, const void *b, void *udata)
{
	const struct cfs_packet_extent *extent1 = a;
	const struct cfs_packet_extent *extent2 = b;

	(void)udata;
	if (extent1->file_offset == extent2->file_offset)
		return 0;
	else if (extent1->file_offset < extent2->file_offset)
		return -1;
	else
		return 1;
}

/**
 * Extent is in sync with cfs metanode server.
 */
static bool extent_in_sync(const struct cfs_packet_extent *extent)
{
	return extent->pid != 0 && extent->ext_id != 0;
}

static bool extent_is_equal(const struct cfs_packet_extent *extent1,
			    const struct cfs_packet_extent *extent2)
{
	return extent1->pid == extent2->pid &&
	       extent1->ext_id == extent2->ext_id &&
	       extent1->ext_offset == extent2->ext_offset;
}

static int do_extent_request(struct sockaddr_storage *host,
			     struct cfs_packet *packet)
{
	struct cfs_socket *sock;
	int err;

	err = cfs_socket_create(CFS_SOCK_TYPE_TCP, host, &sock);
	if (err) {
		cfs_log_err("socket(%s) create error %d\n", cfs_pr_addr(host),
			    err);
		return err;
	}

	err = cfs_socket_set_recv_timeout(sock, EXTENT_RECV_TIMEOUT_MS);
	if (err) {
		cfs_log_err("socket(%s) set recv timeout error %d\n",
			    cfs_pr_addr(host), err);
		goto out;
	}

	err = cfs_socket_send_packet(sock, packet);
	if (err < 0) {
		cfs_log_err("socket(%s) send packet error %d\n",
			    cfs_pr_addr(host), err);
		goto out;
	}

	err = cfs_socket_recv_packet(sock, packet);
	if (err) {
		cfs_log_err("socket(%s) recv packet error %d\n",
			    cfs_pr_addr(host), err);
		goto out;
	}

out:
	cfs_socket_release(sock, !!err);
	return err;
}

static int extent_cache_init(struct cfs_extent_cache *cache,
			     struct cfs_extent_stream *es)
{
	cache->generation = 0;
	cache->size = 0;
	cache->extents = btree_new(sizeof(struct cfs_packet_extent),
				   EXTENT_ITEM_COUNT_PRE_BTREE_NODE,
				   extent_item_cmp, NULL);
	if (!cache->extents)
		return -ENOMEM;
	cache->discard = btree_new(sizeof(struct cfs_packet_extent),
				   EXTENT_ITEM_COUNT_PRE_BTREE_NODE,
				   extent_item_cmp, NULL);
	if (!cache->discard) {
		btree_free(cache->extents);
		return -ENOMEM;
	}
	mutex_init(&cache->lock);
	cache->es = es;
	return 0;
}

static void extent_cache_clear(struct cfs_extent_cache *cache)
{
	if (!cache)
		return;
	if (cache->extents)
		btree_free(cache->extents);
	if (cache->discard)
		btree_free(cache->discard);
	mutex_destroy(&cache->lock);
}

enum {
	EBTREE_GET_FIRST,
	EBTREE_GET_PREV,
	EBTREE_GET_NEXT,
	EBTREE_GET_LE,
	EBTREE_GET_GE,
};

struct extent_btree_context {
	const struct cfs_packet_extent *extent;
	u64 key;
	int type;
};

static bool extent_btree_iter(const void *item, void *udata)
{
	const struct cfs_packet_extent *extent = item;
	struct extent_btree_context *ctx = udata;

	switch (ctx->type) {
	case EBTREE_GET_FIRST:
	case EBTREE_GET_LE:
	case EBTREE_GET_GE:
		ctx->extent = extent;
		return false;

	case EBTREE_GET_PREV:
	case EBTREE_GET_NEXT:
		if (extent->file_offset != ctx->key) {
			ctx->extent = extent;
			return false;
		}
		return true;
	default:
		return false;
	}
}

/**
 *  Returns the first item.
 */
static const struct cfs_packet_extent *
extent_btree_get_first(const struct btree *btree)
{
	struct extent_btree_context ctx = {
		.extent = NULL,
		.type = EBTREE_GET_FIRST,
	};
	btree_ascend(btree, NULL, extent_btree_iter, &ctx);
	return ctx.extent;
}

/**
 *  Returns the last item which key less than the provided key.
 */
static const struct cfs_packet_extent *
extent_btree_get_prev(const struct btree *btree, u64 key)
{
	struct cfs_packet_extent pivot = {
		.file_offset = key,
	};
	struct extent_btree_context ctx = {
		.extent = NULL,
		.key = key,
		.type = EBTREE_GET_PREV,
	};
	btree_descend(btree, &pivot, extent_btree_iter, &ctx);
	return ctx.extent;
}

/**
 * Returns the first item which key greater than the provided key.
 */
static const struct cfs_packet_extent *
extent_btree_get_next(const struct btree *btree, u64 key)
{
	struct cfs_packet_extent pivot = {
		.file_offset = key,
	};
	struct extent_btree_context ctx = {
		.extent = NULL,
		.key = key,
		.type = EBTREE_GET_NEXT,
	};
	btree_ascend(btree, &pivot, extent_btree_iter, &ctx);
	return ctx.extent;
}

/**
 *  Returns the last item which key less than or equal to the provided key.
 */
static const struct cfs_packet_extent *
extent_btree_get_le(const struct btree *btree, u64 key)
{
	struct cfs_packet_extent pivot = {
		.file_offset = key,
	};
	struct extent_btree_context ctx = {
		.extent = NULL,
		.key = key,
		.type = EBTREE_GET_LE,
	};
	btree_descend(btree, &pivot, extent_btree_iter, &ctx);
	return ctx.extent;
}

/**
 *  Returns the first item which key greater than or equal to the provided key.
 */
static const struct cfs_packet_extent *
extent_btree_get_ge(const struct btree *btree, u64 key)
{
	struct cfs_packet_extent pivot = {
		.file_offset = key,
	};
	struct extent_btree_context ctx = {
		.extent = NULL,
		.key = key,
		.type = EBTREE_GET_GE,
	};
	btree_ascend(btree, &pivot, extent_btree_iter, &ctx);
	return ctx.extent;
}

// clang-format off

#define extent_btree_for_ascend_range(btree, low, high, val)    \
	for (val = extent_btree_get_ge(btree, low); 				\
	     val && val->file_offset < high;						\
		 val = extent_btree_get_next(btree, val->file_offset))

// clang-format on

static void extent_cache_set_size(struct cfs_extent_cache *cache, loff_t size,
				  bool sync)
{
	mutex_lock(&cache->lock);
	cache->size = size;
	if (sync)
		cache->generation++;
	mutex_unlock(&cache->lock);
}

static loff_t extent_cache_get_size(struct cfs_extent_cache *cache)
{
	loff_t size;

	mutex_lock(&cache->lock);
	size = cache->size;
	mutex_unlock(&cache->lock);
	return size;
}

static bool extent_cache_get_end(struct cfs_extent_cache *cache, u64 offset,
				 struct cfs_packet_extent *extent)
{
	const struct cfs_packet_extent *ext;
	bool found = false;

	mutex_lock(&cache->lock);
	for (ext = extent_btree_get_le(cache->extents, offset); ext;
	     ext = extent_btree_get_prev(cache->extents, ext->file_offset)) {
		if (ext->file_offset != offset &&
		    ext->file_offset + ext->size == offset) {
			*extent = *ext;
			found = true;
			break;
		}
	}
	mutex_unlock(&cache->lock);
	return found;
}

static void extent_cache_truncate(struct cfs_extent_cache *cache, loff_t size)
{
	const struct cfs_packet_extent *ext;

	mutex_lock(&cache->lock);
	for (ext = extent_btree_get_ge(cache->discard, size); ext;
	     ext = extent_btree_get_next(cache->discard, ext->file_offset)) {
		ext = btree_delete(cache->discard, ext);
	}
	mutex_unlock(&cache->lock);
}

static int extent_cache_refresh(struct cfs_extent_cache *cache, bool force)
{
	struct cfs_extent_stream *es = cache->es;
	struct cfs_extent_client *ec = es->ec;
	struct cfs_packet_extent_array extents = { 0 };
	u64 gen, size;
	size_t i;
	int ret;

	mutex_lock(&cache->lock);
	if (!force && btree_count(cache->extents) > 0) {
		ret = 0;
		goto out;
	}
	ret = cfs_meta_list_extent(ec->meta, es->ino, &gen, &size, &extents);
	if (ret < 0)
		goto out;
	if (cache->generation != 0 && cache->generation >= gen)
		goto out;
	cache->generation = gen;
	cache->size = size;
	btree_clear(cache->extents);
	for (i = 0; i < extents.num; i++) {
		btree_set(cache->extents, &extents.base[i]);
		if (btree_oom(cache->extents)) {
			ret = -ENOMEM;
			goto out;
		}
	}

out:
	mutex_unlock(&cache->lock);
	cfs_packet_extent_array_clear(&extents);
	return ret;
}

/**
 * @param extent [in] append to local extent cache
 * @param sync [in]
 * @param discard_extents [out] discard extents that will be sync to cfs metanode server
 */
static int extent_cache_append(struct cfs_extent_cache *cache,
			       struct cfs_packet_extent *extent, bool sync,
			       struct cfs_packet_extent_array *discard_extents)
{
	u64 low_key = extent->file_offset;
	u64 high_key = extent->file_offset + extent->size;
	const struct cfs_packet_extent *deleted = NULL;
	int ret = 0;

	mutex_lock(&cache->lock);
	extent_btree_for_ascend_range(cache->extents, low_key, high_key,
				      deleted)
	{
		/**
		 * 1. Deleted extent is in sync with metanode
		 * 2. Deleted extent is not equal to new extent
		 * 3. New extent is out sync with metanode
		 */
		if (extent_in_sync(deleted) &&
		    !extent_is_equal(deleted, extent) &&
		    (sync || !extent_in_sync(extent))) {
			btree_set(cache->discard, deleted);
			if (btree_oom(cache->discard)) {
				ret = -ENOMEM;
				goto out;
			}
		}

		deleted = btree_delete(cache->extents, deleted);
	}

	btree_set(cache->extents, extent);
	if (btree_oom(cache->extents)) {
		ret = -ENOMEM;
		goto out;
	}

	cfs_log_debug(
		"pid=%llu, ext_id=%llu, ext_offset=%llu, file_offset=%llu, ext_size=%u\n",
		extent->pid, extent->ext_id, extent->ext_offset,
		extent->file_offset, extent->size);

	if (sync) {
		cache->generation++;
		ret = cfs_packet_extent_array_init(discard_extents,
						   btree_count(cache->discard));
		if (ret < 0)
			goto out;
		extent_btree_for_ascend_range(cache->discard, low_key, high_key,
					      deleted)
		{
			if (!extent_is_equal(extent, deleted))
				discard_extents->base[discard_extents->num++] =
					*deleted;
		}
	}
	if (high_key > cache->size)
		cache->size = high_key;

out:
	mutex_unlock(&cache->lock);
	if (ret)
		cfs_log_err("oom\n");
	return ret;
}

static void
extent_cache_remove_discard(struct cfs_extent_cache *cache,
			    struct cfs_packet_extent_array *discard_extents)
{
	size_t i;

	mutex_lock(&cache->lock);
	for (i = 0; i < discard_extents->num; i++) {
		btree_delete(cache->discard, &discard_extents->base[i]);
	}
	mutex_unlock(&cache->lock);
}

/**
 * @param offset [in] request file offset
 * @param size [in] request file size
 */
static int prepare_extent_io_list(struct cfs_extent_cache *cache, loff_t offset,
				  size_t size, struct list_head *io_info_list)
{
	const struct cfs_packet_extent *ext;
	struct extent_io_info *io_info;
	size_t start = offset;
	size_t end = offset + size;

	mutex_lock(&cache->lock);
	ext = extent_btree_get_le(cache->extents, offset);
	if (!ext)
		ext = extent_btree_get_first(cache->extents);
	for (; ext;
	     ext = extent_btree_get_next(cache->extents, ext->file_offset)) {
		size_t ext_start = ext->file_offset;
		size_t ext_end = ext->file_offset + ext->size;

		if (start < ext_start) {
			if (end <= ext_start)
				break;
			if (end <= ext_end) {
				/* add hole(start, ext_start) */
				io_info = extent_io_info_new(
					start, ext_start - start, NULL);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);

				/* add non-hole(ext_start, end) */
				io_info = extent_io_info_new(
					ext_start, end - ext_start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = end;
				break;
			} else {
				/* add hole(start, ext_start) */
				io_info = extent_io_info_new(
					start, ext_start - start, NULL);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);

				/* add non-hole(ext_start, ext_end) */
				io_info = extent_io_info_new(
					ext_start, ext_end - ext_start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = ext_end;
			}
		} else if (start < ext_end) {
			if (end <= ext_end) {
				/* add non-hole(start, end) */
				io_info = extent_io_info_new(start, end - start,
							     ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = end;
				break;
			} else {
				/* add non-hole(start, ext_end) */
				io_info = extent_io_info_new(
					start, ext_end - start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = ext_end;
			}
		}
	}

	if (start < end) {
		/* add hole(start, end) */
		io_info = extent_io_info_new(start, end - start, NULL);
		if (!io_info)
			goto oom;
		list_add_tail(&io_info->list, io_info_list);
	}
	mutex_unlock(&cache->lock);
	return 0;

oom:
	mutex_unlock(&cache->lock);
	while (!list_empty(io_info_list)) {
		io_info = list_first_entry(io_info_list, struct extent_io_info,
					   list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	return -ENOMEM;
}

static int extent_id_new(struct cfs_extent_stream *es,
			 struct cfs_data_partition *dp, u64 *ext_id)
{
	u8 op = CFS_OP_EXTENT_CREATE;
	struct cfs_packet *packet;
	int ret;

	BUG_ON(!dp->follower_addrs);

	packet = cfs_packet_new(op, dp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;
	packet->request.hdr.ext_type = CFS_EXTENT_TYPE_NORMAL;
	packet->request.hdr.remaining_followers = dp->nr_followers;
	packet->request.arg = dp->follower_addrs;
	packet->request.hdr.arglen =
		cpu_to_be32(cfs_buffer_size(packet->request.arg));
	packet->request.data.ino = cpu_to_be64(es->ino);
	packet->request.hdr.size = cpu_to_be32(sizeof(es->ino));

	ret = do_extent_request(&dp->members.base[0], packet);
	if (ret < 0) {
		cfs_log_err("request extent error %d\n", ret);
		cfs_packet_release(packet);
		return ret;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	if (ret == 0)
		*ext_id = be64_to_cpu(packet->reply.hdr.ext_id);
	cfs_packet_release(packet);
	return ret;
}

static void extent_async_io_work_cb(struct work_struct *work);

static int extent_async_io_init(struct cfs_extent_async_io *async, int rw,
				struct cfs_data_partition *dp)
{
	mutex_init(&async->lock);
	INIT_LIST_HEAD(&async->requests);
	INIT_WORK(&async->work, extent_async_io_work_cb);
	init_waitqueue_head(&async->wq);
	atomic_set(&async->inflight, 0);
	async->dp = dp;
	async->rw = rw;
	return cfs_socket_create(CFS_SOCK_TYPE_TCP,
				 rw == READ ? dp->leader : &dp->members.base[0],
				 &async->sock);
}

static void extent_async_io_clear(struct cfs_extent_async_io *async)
{
	cfs_socket_release(async->sock, async->io_err);
	cfs_data_partition_release(async->dp);
}

static inline void extent_async_io_wait(struct cfs_extent_async_io *async)
{
	wait_event(async->wq, atomic_read(&async->inflight) == 0);
}

static int extent_async_io_do_request(struct cfs_extent_async_io *async,
				      struct cfs_packet *packet)
{
	struct sockaddr_storage *ss;
	int retry_cnt = 3;
	int ret;

	if (!async->sock)
		return -1;
	if (!async->io_err)
		goto retry_send;

retry_connect:
	extent_async_io_wait(async);
	cfs_socket_release(async->sock, true);
	async->sock = NULL;
	ss = async->rw == READ ? async->dp->leader :
				 &async->dp->members.base[0];
	ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, ss, &async->sock);
	if (ret < 0) {
		cfs_log_err("create socket error %d\n", ret);
		return ret;
	}
	if (async->io_err)
		async->io_err = false;

retry_send:
	if (retry_cnt-- == 0)
		return -1;
	ret = cfs_socket_send_packet(async->sock, packet);
	if (ret < 0)
		goto retry_connect;

	cfs_packet_get(packet);
	mutex_lock(&async->lock);
	list_add_tail(&packet->list, &async->requests);
	mutex_unlock(&async->lock);
	atomic_inc(&async->inflight);
	queue_work(extent_work_queue, &async->work);
	return 0;
}

static void extent_async_io_work_cb(struct work_struct *work)
{
	struct cfs_extent_async_io *async =
		container_of(work, struct cfs_extent_async_io, work);
	struct cfs_packet *packet;
	int cnt = 0;
	int ret = 0;

	while (true) {
		mutex_lock(&async->lock);
		packet = list_first_entry_or_null(&async->requests,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		mutex_unlock(&async->lock);
		if (!packet)
			break;

		if (!async->io_err)
			ret = cfs_socket_recv_packet(async->sock, packet);
		else
			ret = -EIO;

		packet->error = ret;
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_complete_reply(packet);
		cfs_packet_release(packet);

		if (ret < 0) {
			async->io_err = true;
			ret = -EIO;
		}
	}
	atomic_sub(cnt, &async->inflight);
	wake_up(&async->wq);
}

static struct cfs_data_partition *
cfs_extent_select_partition(struct cfs_extent_client *ec);
static u32 cfs_extent_get_partition_count(struct cfs_extent_client *ec);
static struct cfs_data_partition *
cfs_extent_get_partition(struct cfs_extent_client *ec, u64 id);

static struct cfs_extent_writer *
extent_writer_new(struct cfs_extent_stream *es, struct cfs_data_partition *dp,
		  u64 ext_id, u32 ext_size, loff_t file_offset)
{
	struct cfs_extent_writer *writer;
	int ret;

	writer = kzalloc(sizeof(*writer), GFP_NOFS);
	if (!writer)
		return ERR_PTR(-ENOMEM);
	ret = extent_async_io_init(&writer->async, WRITE, dp);
	if (ret < 0) {
		kfree(writer);
		return ERR_PTR(ret);
	}
	writer->file_offset = file_offset;
	writer->ext_id = ext_id;
	writer->ext_size = ext_size;
	return writer;
}

static void extent_writer_release(struct cfs_extent_writer *writer)
{
	if (!writer)
		return;
	extent_async_io_clear(&writer->async);
	kfree(writer);
}

static inline void extent_writer_set_dirty(struct cfs_extent_writer *writer)
{
	writer->dirty = true;
}

static inline void extent_writer_clear_dirty(struct cfs_extent_writer *writer)
{
	writer->dirty = false;
}

static inline bool extent_writer_test_dirty(struct cfs_extent_writer *writer)
{
	return writer->dirty;
}

static int extent_writer_flush(struct cfs_extent_stream *es,
			       struct cfs_extent_writer *writer)
{
	struct cfs_meta_client *meta = es->ec->meta;
	struct cfs_data_partition *dp = writer->async.dp;
	struct cfs_packet_extent_array discard_extents = { 0 };
	struct cfs_packet_extent ext;
	int ret;

	if (!extent_writer_test_dirty(writer))
		return 0;
	extent_async_io_wait(&writer->async);
	cfs_packet_extent_init(&ext, writer->file_offset, dp->id,
			       writer->ext_id, 0, writer->ext_size);
	ret = extent_cache_append(&es->cache, &ext, true, &discard_extents);
	if (unlikely(ret < 0))
		return ret;
	ret = cfs_meta_append_extent(meta, es->ino, &ext, &discard_extents);
	if (ret < 0) {
		cfs_log_err("extent data loss\n");
		cfs_packet_extent_array_clear(&discard_extents);
		return ret;
	}
	extent_cache_remove_discard(&es->cache, &discard_extents);
	cfs_packet_extent_array_clear(&discard_extents);
	extent_writer_clear_dirty(writer);
	return ret;
}

static inline void extent_writer_write_bytes(struct cfs_extent_writer *writer,
					     size_t size)
{
	writer->ext_size += size;
}

static int extent_writer_do_request_async(struct cfs_extent_writer *writer,
					  struct cfs_packet *packet)
{
	return extent_async_io_do_request(&writer->async, packet);
}

static int extent_writer_do_request(struct cfs_extent_writer *writer,
				    struct cfs_packet *packet)
{
	int ret;

	ret = extent_async_io_do_request(&writer->async, packet);
	if (ret < 0)
		return ret;
	cfs_packet_wait_reply(packet);
	if (packet->error) {
		ret = packet->error;
		return ret;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	return ret;
}

/**
 * @param offset [in] file offset
 * @param size [in] write size, must less than EXTENT_SIZE
 */
static int extent_writer_get_or_create(struct cfs_extent_stream *es,
				       loff_t offset, size_t size,
				       struct cfs_extent_writer **writerp)
{
	if (es->writer &&
	    ((es->writer->file_offset + es->writer->ext_size != offset) ||
	     (es->writer->ext_size + size > EXTENT_SIZE))) {
		extent_writer_flush(es, es->writer);
		extent_writer_release(es->writer);
		es->writer = NULL;
	}

	if (!es->writer) {
		struct cfs_extent_writer *writer;
		struct cfs_data_partition *dp = NULL;
		struct cfs_packet_extent extent;
		bool new_ext = true;
		int ret;

		if (extent_cache_get_end(&es->cache, offset, &extent)) {
			if (extent.ext_id > 64 &&
			    extent.size + size <= EXTENT_SIZE) {
				dp = cfs_extent_get_partition(es->ec,
							      extent.pid);
				if (!dp)
					return -ENOENT;
				new_ext = false;
			}
		}

		if (new_ext) {
			u32 retry = cfs_extent_get_partition_count(es->ec);

			while (retry-- > 0) {
				dp = cfs_extent_select_partition(es->ec);
				if (!dp)
					return -ENOENT;
				cfs_packet_extent_init(&extent, offset, dp->id,
						       0, 0, 0);
				ret = extent_id_new(es, dp, &extent.ext_id);
				if (ret == 0)
					break;
				cfs_data_partition_release(dp);
				cfs_log_warning(
					"create extent error %d, retry %d\n",
					ret, retry);
			}
		}

		writer = extent_writer_new(es, dp, extent.ext_id, extent.size,
					   extent.file_offset);
		if (IS_ERR(writer)) {
			cfs_data_partition_release(dp);
			return PTR_ERR(writer);
		}
		es->writer = writer;
	}
	*writerp = es->writer;
	return 0;
}

static int extent_reader_do_request_async(struct cfs_extent_reader *reader,
					  struct cfs_packet *packet)
{
	return extent_async_io_do_request(&reader->async, packet);
}

static struct cfs_extent_reader *
extent_reader_new(struct cfs_extent_stream *es,
		  const struct cfs_packet_extent *extent)
{
	struct cfs_extent_reader *reader;
	struct cfs_data_partition *dp;
	int ret;

	reader = kzalloc(sizeof(*reader), GFP_NOFS);
	if (!reader)
		return ERR_PTR(-ENOMEM);
	dp = cfs_extent_get_partition(es->ec, extent->pid);
	if (!dp) {
		kfree(reader);
		return ERR_PTR(-ENOENT);
	}
	ret = extent_async_io_init(&reader->async, READ, dp);
	if (ret < 0) {
		cfs_data_partition_release(dp);
		kfree(reader);
		return ERR_PTR(ret);
	}
	reader->file_offset = extent->file_offset;
	reader->ext_id = extent->ext_id;
	reader->ext_offset = extent->ext_offset;
	reader->ext_size = extent->size;
	return reader;
}

static void extent_reader_release(struct cfs_extent_reader *reader)
{
	if (!reader)
		return;
	extent_async_io_clear(&reader->async);
	kfree(reader);
}

static int extent_reader_get_or_create(struct cfs_extent_stream *es,
				       const struct cfs_packet_extent *extent,
				       struct cfs_extent_reader **readerp)
{
	if (es->reader) {
		struct cfs_data_partition *dp = es->reader->async.dp;

		if (dp->id != extent->pid ||
		    es->reader->ext_id != extent->ext_id) {
			extent_async_io_wait(&es->reader->async);
			extent_reader_release(es->reader);
			es->reader = NULL;
		}
	}
	if (!es->reader) {
		struct cfs_extent_reader *reader =
			extent_reader_new(es, extent);

		if (IS_ERR(reader))
			return PTR_ERR(reader);
		es->reader = reader;
	}
	*readerp = es->reader;
	return 0;
}

static struct cfs_packet *extent_packet_new(u8 op, u8 ext_type,
					    u8 remaining_followers, u64 pid,
					    u64 ext_id, u64 ext_offset,
					    u64 file_offset)
{
	struct cfs_packet *packet;

	packet = cfs_packet_new(op, pid, NULL, NULL);
	if (!packet)
		return NULL;
	packet->request.hdr.ext_type = ext_type;
	packet->request.hdr.remaining_followers = remaining_followers;
	packet->request.hdr.ext_id = cpu_to_be64(ext_id);
	packet->request.hdr.ext_offset = cpu_to_be64(ext_offset);
	packet->request.hdr.kernel_offset = cpu_to_be64(file_offset);
	return packet;
}

static void cfs_packet_set_write_data(struct cfs_packet *packet,
				      struct page_iter *pi, struct iov_iter *ii,
				      size_t *size)
{
	if (pi) {
		packet->request.data.write.is_page = true;
		packet->request.data.write.pages = packet->rw.pages;
		packet->request.data.write.nr_segs =
			page_copy(pi, packet->request.data.write.pages,
				  CFS_PAGE_VEC_NUM, size);
	} else {
		packet->request.data.write.is_page = false;
		packet->request.data.write.iov = packet->rw.iov;
		packet->request.data.write.nr_segs =
			iov_copy(ii, packet->request.data.write.iov,
				 CFS_PAGE_VEC_NUM, size);
	}
	packet->request.hdr.size = cpu_to_be32(*size);
}

static void cfs_packet_set_read_data(struct cfs_packet *packet,
				     struct page_iter *pi, struct iov_iter *ii,
				     size_t *size)
{
	if (pi) {
		packet->reply.data.read.is_page = true;
		packet->reply.data.read.pages = packet->rw.pages;
		packet->reply.data.read.nr_segs =
			page_copy(pi, packet->reply.data.read.pages,
				  CFS_PAGE_VEC_NUM, size);
	} else {
		packet->reply.data.read.is_page = false;
		packet->reply.data.read.iov = packet->rw.iov;
		packet->reply.data.read.nr_segs =
			iov_copy(ii, packet->reply.data.read.iov,
				 CFS_PAGE_VEC_NUM, size);
	}
	packet->request.hdr.size = cpu_to_be32(*size);
}

static int extent_write_pages_random(struct cfs_extent_stream *es,
				     struct extent_io_info *io_info,
				     struct page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct page_frag *frag;
	size_t w_len;
	size_t send_bytes = 0;
	size_t i;
	int ret = 0;

	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_err("cannot get data partition(%llu)\n",
			    io_info->ext.pid);
		ret = -ENOENT;
		return ret;
	}
	while (send_bytes < io_info->size) {
		cfs_log_debug(
			"pages(%lu~%lu) first_page_offset(%zu) last_page_size(%zu)\n",
			iter->pages[0]->index,
			iter->pages[iter->nr_pages - 1]->index,
			iter->first_page_offset, iter->last_page_size);

		w_len = min(io_info->size - send_bytes, EXTENT_BLOCK_SIZE);
		packet = extent_packet_new(
			CFS_OP_STREAM_RANDOM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			dp->nr_followers, dp->id, io_info->ext.ext_id,
			io_info->offset - io_info->ext.file_offset +
				io_info->ext.ext_offset + send_bytes,
			io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}
		cfs_packet_set_write_data(packet, iter, NULL, &w_len);

		ret = do_extent_request(dp->leader, packet);
		if (ret < 0) {
			cfs_log_err("write extent error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}
		ret = -cfs_parse_status(packet->reply.hdr.result_code);
		if (ret < 0) {
			cfs_log_err("write reply error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}

		for (i = 0; i < packet->request.data.write.nr_segs; i++) {
			frag = &packet->request.data.write.pages[i];
			__page_io_bytes(frag->page, frag->size);
			if (__page_io_done(frag->page)) {
				__page_io_clear(frag->page);
				page_endio(frag->page, WRITE, 0);
				unlock_page(frag->page);
			}
		}
		cfs_packet_release(packet);
		page_iter_advance(iter, w_len);
		send_bytes += w_len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

static int extent_write_pages_tiny(struct cfs_extent_stream *es,
				   struct extent_io_info *io_info,
				   struct page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	struct page_frag *frag;
	size_t i;
	int ret = -1;
	u32 retry = cfs_extent_get_partition_count(es->ec);

	BUG_ON(iter->nr_pages > CFS_PAGE_VEC_NUM);

	cfs_log_debug(
		"pages(%lu~%lu) first_page_offset(%zu) last_page_size(%zu)\n",
		iter->pages[0]->index, iter->pages[iter->nr_pages - 1]->index,
		iter->first_page_offset, iter->last_page_size);

	while (retry-- > 0) {
		dp = cfs_extent_select_partition(es->ec);
		if (!dp) {
			cfs_log_err("cannot select data partition\n");
			ret = -ENOENT;
			return ret;
		}
		packet = extent_packet_new(CFS_OP_STREAM_WRITE,
					   CFS_EXTENT_TYPE_TINY,
					   dp->nr_followers, dp->id, 0, 0, 0);
		if (!packet) {
			cfs_data_partition_release(dp);
			ret = -ENOMEM;
			return ret;
		}
		cfs_packet_set_request_arg(packet, dp->follower_addrs);
		cfs_packet_set_write_data(packet, iter, NULL, &io_info->size);

		ret = do_extent_request(&dp->members.base[0], packet);
		if (ret < 0) {
			cfs_log_err("write extent error %d\n", ret);
			cfs_packet_release(packet);
			cfs_data_partition_release(dp);
			continue;
		}
		ret = -cfs_parse_status(packet->reply.hdr.result_code);
		if (ret < 0) {
			cfs_log_err("write extent reply error %d\n", ret);
			cfs_packet_release(packet);
			cfs_data_partition_release(dp);
			continue;
		}

		cfs_data_partition_release(dp);

		cfs_packet_extent_init(
			&extent, 0, be64_to_cpu(packet->reply.hdr.pid),
			be64_to_cpu(packet->reply.hdr.ext_id),
			be64_to_cpu(packet->reply.hdr.ext_offset),
			io_info->size);
		ret = extent_cache_append(&es->cache, &extent, false, NULL);
		if (unlikely(ret < 0)) {
			cfs_log_err("append extent cache error %d\n", ret);
			cfs_packet_release(packet);
			return ret;
		}
		ret = cfs_meta_append_extent(es->ec->meta, es->ino, &extent,
					     NULL);
		if (ret < 0) {
			cfs_log_err("sync extent cache error %d\n", ret);
			cfs_packet_release(packet);
			return ret;
		}

		for (i = 0; i < packet->request.data.write.nr_segs; i++) {
			frag = &packet->request.data.write.pages[i];
			__page_io_bytes(frag->page, frag->size);
			if (__page_io_done(frag->page)) {
				__page_io_clear(frag->page);
				page_endio(frag->page, WRITE, 0);
				unlock_page(frag->page);
			}
		}
		page_iter_advance(iter, io_info->size);
		cfs_packet_release(packet);
		return 0;
	}
	return ret;
}

static void extent_write_pages_reply_cb(struct cfs_packet *packet)
{
	// struct cfs_extent_writer *writer = packet->private;
	struct page_frag *frag;
	size_t i;
	int err;

	if (packet->error)
		err = packet->error;
	else
		err = -cfs_parse_status(packet->reply.hdr.result_code);
	if (err)
		cfs_log_err("reply error %d\n", err);
	// else
	// 	writer->extent.size += be32_to_cpu(packet->request.hdr.size);

	for (i = 0; i < packet->request.data.write.nr_segs; i++) {
		frag = &packet->request.data.write.pages[i];
		if (err) {
			SetPageError(frag->page);
			if (frag->page->mapping)
				mapping_set_error(frag->page->mapping, err);
		}
		__page_io_bytes(frag->page, frag->size);
		if (__page_io_done(frag->page)) {
			__page_io_clear(frag->page);
			end_page_writeback(frag->page);
			unlock_page(frag->page);
		}
	}
}

static int extent_write_pages_normal(struct cfs_extent_stream *es,
				     struct extent_io_info *io_info,
				     struct page_iter *iter)
{
	struct cfs_extent_writer *writer;
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	struct page_frag *frag;
	loff_t offset = io_info->offset;
	size_t send_bytes = 0, total_bytes = io_info->size;
	size_t w_len, i;
	int ret = 0;

	while (send_bytes < total_bytes) {
		cfs_log_debug(
			"pages(%lu~%lu) first_page_offset(%zu) last_page_size(%zu) send_bytes(%zu) total_bytes(%zu)\n",
			iter->pages[0]->index,
			iter->pages[iter->nr_pages - 1]->index,
			iter->first_page_offset, iter->last_page_size,
			send_bytes, total_bytes);

		w_len = min(total_bytes - send_bytes, EXTENT_BLOCK_SIZE);
		ret = extent_writer_get_or_create(es, offset, w_len, &writer);
		if (ret < 0)
			return ret;
		dp = writer->async.dp;
		packet = extent_packet_new(
			CFS_OP_STREAM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			dp->nr_followers, dp->id, writer->ext_id,
			offset - writer->file_offset, offset);
		if (!packet) {
			ret = -ENOMEM;
			return ret;
		}
		cfs_packet_set_callback(packet, extent_write_pages_reply_cb,
					writer);
		cfs_packet_set_request_arg(packet, dp->follower_addrs);
		cfs_packet_set_write_data(packet, iter, NULL, &w_len);

		ret = extent_writer_do_request_async(writer, packet);
		if (ret < 0) {
			cfs_packet_release(packet);
			return ret;
		}

		page_iter_advance(iter, w_len);

		cfs_packet_extent_init(&extent, writer->file_offset, 0, 0, 0,
				       writer->ext_size + w_len);
		ret = extent_cache_append(&es->cache, &extent, false, NULL);
		if (unlikely(ret < 0)) {
			for (i = 0; i < packet->request.data.write.nr_segs;
			     i++) {
				frag = &packet->request.data.write.pages[i];
				SetPageError(frag->page);
				if (frag->page->mapping)
					mapping_set_error(frag->page->mapping,
							  ret);
			}
			cfs_packet_release(packet);
			return ret;
		}
		extent_writer_set_dirty(writer);
		extent_writer_write_bytes(writer, w_len);
		cfs_packet_release(packet);
		send_bytes += w_len;
		offset += w_len;
	}
	return ret;
}

int cfs_extent_write_pages(struct cfs_extent_stream *es, struct page **pages,
			   size_t nr_pages, size_t last_page_size)
{
	LIST_HEAD(io_info_list);
	struct extent_io_info *io_info;
	struct page_iter iter;
	size_t i;
	int ret;

	if (nr_pages == 0)
		return 0;

	cfs_log_debug("pages(%lu~%lu) last_page_size(%zu)\n", pages[0]->index,
		      pages[nr_pages - 1]->index, last_page_size);

	page_iter_init(&iter, pages, nr_pages, 0, last_page_size);
	for (i = 0; i < iter.nr_pages - 1; i++)
		__page_io_set(iter.pages[i], PAGE_CACHE_SIZE);
	__page_io_set(iter.pages[i], last_page_size);

	ret = extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_err("extent cache refresh error %d\n", ret);
		goto err_page;
	}
	ret = prepare_extent_io_list(&es->cache, page_offset(pages[0]),
				     page_iter_size(&iter), &io_info_list);
	if (ret < 0) {
		cfs_log_err("prepare extent write request error %d\n", ret);
		goto err_page;
	}

	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		switch (extent_io_type(io_info)) {
		case EXTENT_WRITE_TYPE_RANDOM:
			ret = extent_write_pages_random(es, io_info, &iter);
			break;
		case EXTENT_WRITE_TYPE_TINY:
			ret = extent_write_pages_tiny(es, io_info, &iter);
			break;
		case EXTENT_WRITE_TYPE_NORMAL:
			ret = extent_write_pages_normal(es, io_info, &iter);
			break;
		}
		list_del(&io_info->list);
		kfree(io_info);
		if (ret < 0)
			goto err_page;
	}
	return 0;

err_page:
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	if (iter.nr_pages > 0) {
		struct page *page = iter.pages[0];
		size_t firs_page_size = iter.nr_pages == 1 ?
						iter.last_page_size :
						PAGE_CACHE_SIZE;

		BUG_ON(ret == 0);
		SetPageError(page);
		if (page->mapping)
			mapping_set_error(page->mapping, ret);
		__page_io_bytes(page, firs_page_size - iter.first_page_offset);
		if (__page_io_done(page)) {
			__page_io_clear(page);
			end_page_writeback(page);
			unlock_page(page);
		}
		for (i = 1; i < iter.nr_pages; i++) {
			page = iter.pages[i];
			__page_io_clear(page);
			page_endio(page, WRITE, ret);
			unlock_page(page);
		}
	}
	return ret;
}

static void extent_read_pages_reply_cb(struct cfs_packet *packet)
{
	struct page_frag *frag;
	size_t i;
	int err;

	if (packet->error)
		err = packet->error;
	else
		err = -cfs_parse_status(packet->reply.hdr.result_code);
	if (err)
		cfs_log_err("reply error %d\n", err);

	for (i = 0; i < packet->reply.data.read.nr_segs; i++) {
		frag = &packet->reply.data.read.pages[i];
		if (err)
			SetPageError(frag->page);
		__page_io_bytes(frag->page, frag->size);
		if (__page_io_done(frag->page)) {
			__page_io_clear(frag->page);
			if (err)
				ClearPageUptodate(frag->page);
			else
				SetPageUptodate(frag->page);
			unlock_page(frag->page);
		}
	}
}

static int extent_read_pages_internal(struct cfs_extent_stream *es,
				      struct extent_io_info *io_info,
				      struct page_iter *iter)
{
	struct cfs_extent_reader *reader = NULL;
	struct cfs_packet *packet;
	size_t read_bytes = 0, total_bytes = io_info->size;
	size_t r_len;
	int ret;

	ret = extent_reader_get_or_create(es, &io_info->ext, &reader);
	if (ret < 0)
		return ret;
	while (read_bytes < total_bytes) {
		r_len = min(total_bytes - read_bytes, EXTENT_BLOCK_SIZE);
		packet = extent_packet_new(
			CFS_OP_STREAM_READ, CFS_EXTENT_TYPE_NORMAL, 0,
			reader->async.dp->id, reader->ext_id,
			io_info->offset - reader->file_offset +
				reader->ext_offset + read_bytes,
			reader->file_offset);
		if (!packet)
			return -ENOMEM;
		cfs_packet_set_callback(packet, extent_read_pages_reply_cb,
					NULL);
		cfs_packet_set_read_data(packet, iter, NULL, &r_len);

		ret = extent_reader_do_request_async(reader, packet);
		cfs_packet_release(packet);
		if (ret < 0)
			return ret;
		page_iter_advance(iter, r_len);
		read_bytes += r_len;
	}
	return 0;
}

int cfs_extent_read_pages(struct cfs_extent_stream *es, struct page **pages,
			  size_t nr_pages)
{
	LIST_HEAD(io_info_list);
	struct extent_io_info *io_info;
	struct page_iter iter;
	size_t i;
	int ret;

	if (nr_pages == 0)
		return 0;

	cfs_log_debug("pages(%lu~%lu)\n", pages[0]->index,
		      pages[nr_pages - 1]->index);

	page_iter_init(&iter, pages, nr_pages, 0, PAGE_CACHE_SIZE);
	for (i = 0; i < iter.nr_pages; i++)
		__page_io_set(iter.pages[i], PAGE_CACHE_SIZE);

	ret = extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_err("extent cache refresh error %d\n", ret);
		goto err_page;
	}
	ret = prepare_extent_io_list(&es->cache, page_offset(pages[0]),
				     page_iter_size(&iter), &io_info_list);
	if (ret < 0) {
		cfs_log_err("prepare extent write request error %d\n", ret);
		goto err_page;
	}
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		if (io_info->hole) {
			size_t read_bytes = 0, total_bytes = io_info->size;

			while (read_bytes < total_bytes) {
				struct page_frag frag = { 0 };
				size_t len = total_bytes - read_bytes;

				page_copy(&iter, &frag, 1, &len);
				BUG_ON(!frag.page);
				zero_user(frag.page, frag.offset, frag.size);
				page_iter_advance(&iter, len);
				__page_io_bytes(frag.page, len);
				if (__page_io_done(frag.page)) {
					__page_io_clear(frag.page);
					page_endio(frag.page, READ, 0);
				}
				read_bytes += len;
			}
			goto next;
		}
		ret = extent_read_pages_internal(es, io_info, &iter);
		if (ret < 0) {
			cfs_log_debug("read pages error %d\n", ret);
			goto err_page;
		}

next:
		list_del(&io_info->list);
		kfree(io_info);
	}

err_page:
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	if (iter.nr_pages > 0) {
		struct page *page = iter.pages[0];
		size_t firs_page_size = iter.nr_pages == 1 ?
						iter.last_page_size :
						PAGE_CACHE_SIZE;

		BUG_ON(ret == 0);
		SetPageError(page);
		__page_io_bytes(page, firs_page_size - iter.first_page_offset);
		if (__page_io_done(page)) {
			__page_io_clear(page);
			ClearPageUptodate(page);
			unlock_page(page);
		}
		for (i = 1; i < iter.nr_pages; i++) {
			page = iter.pages[i];
			__page_io_clear(page);
			page_endio(page, READ, ret);
		}
	}
	return ret;
}

static int extent_dio_read_internal(struct cfs_extent_stream *es,
				    const struct extent_io_info *io_info,
				    struct iov_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t r_len;
	size_t read_bytes = 0, total_bytes = io_info->size;
	size_t ext_offset;
	int ret = 0;

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu, pid=%llu, "
		      "ext_id=%llu, ext_offset=%llu, ext_size=%u\n",
		      es->ino, io_info->offset, io_info->size, io_info->ext.pid,
		      io_info->ext.ext_id, io_info->ext.ext_offset,
		      io_info->ext.size);

	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp)
		return -ENOENT;
	ext_offset = io_info->offset - io_info->ext.file_offset +
		     io_info->ext.ext_offset;
	while (read_bytes < total_bytes) {
		r_len = min(total_bytes - read_bytes, EXTENT_BLOCK_SIZE);
		packet = extent_packet_new(CFS_OP_STREAM_READ,
					   CFS_EXTENT_TYPE_NORMAL, 0, dp->id,
					   io_info->ext.ext_id,
					   ext_offset + read_bytes,
					   io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}
		cfs_packet_set_read_data(packet, NULL, iter, &r_len);

		ret = do_extent_request(dp->leader, packet);
		if (ret < 0) {
			cfs_log_err("extent reader request error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}
		ret = -cfs_parse_status(packet->reply.hdr.result_code);
		if (ret < 0) {
			cfs_log_err("write reply error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}
		cfs_packet_release(packet);
		iov_iter_advance(iter, r_len);
		read_bytes += r_len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

int cfs_extent_dio_read(struct cfs_extent_stream *es, struct iov_iter *iter,
			loff_t offset)
{
	LIST_HEAD(io_info_list);
	struct extent_io_info *io_info;
	size_t total_bytes = iov_iter_count(iter);
	int ret;

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu\n", es->ino, offset,
		      total_bytes);

	ret = extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_err("extent cache refresh error %d\n", ret);
		return ret;
	}
	ret = prepare_extent_io_list(&es->cache, offset, total_bytes,
				     &io_info_list);
	if (ret < 0) {
		cfs_log_err("prepare extent read error %d\n", ret);
		return ret;
	}
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		if (io_info->hole) {
			iov_iter_advance(iter, io_info->size);
			goto next;
		}
		ret = extent_dio_read_internal(es, io_info, iter);
		if (ret < 0)
			goto out;

next:
		list_del(&io_info->list);
		kfree(io_info);
	}

out:
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	return ret < 0 ? ret : total_bytes;
}

static int extent_dio_write_random(struct cfs_extent_stream *es,
				   const struct extent_io_info *io_info,
				   struct iov_iter *ii)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t w_len;
	size_t send_bytes = 0, total_bytes = io_info->size;
	int ret = 0;

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu, pid=%llu, "
		      "ext_id=%llu, ext_offset=%llu, ext_size=%u\n",
		      es->ino, io_info->offset, io_info->size, io_info->ext.pid,
		      io_info->ext.ext_id, io_info->ext.ext_offset,
		      io_info->ext.size);
	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_err("not found data partition(%llu)\n",
			    io_info->ext.pid);
		return -ENOENT;
	}
	while (send_bytes < total_bytes) {
		w_len = min(total_bytes - send_bytes, EXTENT_BLOCK_SIZE);
		packet = extent_packet_new(
			CFS_OP_STREAM_RANDOM_WRITE, CFS_EXTENT_TYPE_NORMAL, 0,
			io_info->ext.pid, io_info->ext.ext_id,
			io_info->offset - io_info->ext.file_offset +
				io_info->ext.ext_offset + send_bytes,
			io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}

		cfs_packet_set_write_data(packet, NULL, ii, &w_len);

		ret = do_extent_request(dp->leader, packet);
		if (ret < 0) {
			cfs_log_err("write extent error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}
		ret = -cfs_parse_status(packet->reply.hdr.result_code);
		if (ret < 0) {
			cfs_log_err("write reply error %d\n", ret);
			cfs_packet_release(packet);
			goto out;
		}
		cfs_packet_release(packet);
		iov_iter_advance(ii, w_len);
		send_bytes += w_len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

static int extent_dio_write_tiny(struct cfs_extent_stream *es,
				 struct extent_io_info *io_info,
				 struct iov_iter *ii)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet = NULL;
	struct cfs_packet_extent extent;
	int ret = -1;
	u32 retry = cfs_extent_get_partition_count(es->ec);

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu\n", es->ino,
		      io_info->offset, io_info->size);

	while (retry-- > 0) {
		dp = cfs_extent_select_partition(es->ec);
		if (!dp) {
			cfs_log_err("cannot select data partition\n");
			return -ENOENT;
		}
		packet = extent_packet_new(CFS_OP_STREAM_WRITE,
					   CFS_EXTENT_TYPE_TINY,
					   dp->nr_followers, dp->id, 0, 0, 0);
		if (!packet) {
			cfs_data_partition_release(dp);
			return -ENOMEM;
		}

		cfs_packet_set_request_arg(packet, dp->follower_addrs);
		cfs_packet_set_write_data(packet, NULL, ii, &io_info->size);

		ret = do_extent_request(&dp->members.base[0], packet);
		if (ret < 0) {
			cfs_log_err("write extent error %d\n", ret);
			cfs_packet_release(packet);
			cfs_data_partition_release(dp);
			continue;
		}
		ret = -cfs_parse_status(packet->reply.hdr.result_code);
		if (ret < 0) {
			cfs_log_err("write extent reply error %d\n", ret);
			cfs_packet_release(packet);
			cfs_data_partition_release(dp);
			continue;
		}

		cfs_data_partition_release(dp);

		cfs_packet_extent_init(&extent, 0,
				       be64_to_cpu(packet->reply.hdr.pid),
				       be64_to_cpu(packet->reply.hdr.ext_id), 0,
				       io_info->size);
		ret = extent_cache_append(&es->cache, &extent, false, NULL);
		if (unlikely(ret < 0)) {
			cfs_log_err("append extent cache error %d\n", ret);
			cfs_packet_release(packet);
			return ret;
		}
		ret = cfs_meta_append_extent(es->ec->meta, es->ino, &extent,
					     NULL);
		if (ret < 0) {
			cfs_log_err("sync extent cache error %d\n", ret);
			cfs_packet_release(packet);
			return ret;
		}

		cfs_packet_release(packet);
		iov_iter_advance(ii, io_info->size);
		return 0;
	}
	return ret;
}

static int extent_dio_write_normal(struct cfs_extent_stream *es,
				   const struct extent_io_info *io_info,
				   struct iov_iter *ii)
{
	struct cfs_extent_writer *writer;
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t send_bytes = 0, total_bytes = io_info->size;
	size_t w_len;
	int ret;

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu\n", es->ino,
		      io_info->offset, io_info->size);
	ret = extent_writer_get_or_create(es, io_info->offset, io_info->size,
					  &writer);
	if (ret < 0) {
		cfs_log_err("es(%p) ino(%llu)\n", es, es->ino);
		return ret;
	}
	dp = writer->async.dp;
	while (send_bytes < total_bytes) {
		w_len = min(total_bytes - send_bytes, EXTENT_BLOCK_SIZE);
		packet = extent_packet_new(
			CFS_OP_STREAM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			dp->nr_followers, dp->id, writer->ext_id,
			io_info->offset - writer->file_offset + send_bytes,
			io_info->offset);
		if (!packet)
			return -ENOMEM;
		cfs_packet_set_request_arg(packet, dp->follower_addrs);
		cfs_packet_set_write_data(packet, NULL, ii, &w_len);

		ret = extent_writer_do_request(writer, packet);
		cfs_packet_release(packet);
		if (ret < 0)
			return ret;

		extent_writer_set_dirty(writer);
		extent_writer_write_bytes(writer, w_len);
		iov_iter_advance(ii, w_len);
		send_bytes += w_len;
	}
	return 0;
}

int cfs_extent_dio_write(struct cfs_extent_stream *es, struct iov_iter *iter,
			 loff_t offset)
{
	LIST_HEAD(io_info_list);
	struct extent_io_info *io_info;
	size_t total_bytes = iov_iter_count(iter);
	int ret;

	cfs_log_debug("ino=%llu, offset=%lld, size=%zu\n", es->ino, offset,
		      total_bytes);

	ret = extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_err("extent cache refresh error %d\n", ret);
		return ret;
	}
	mutex_lock(&es->w_lock);
	ret = prepare_extent_io_list(&es->cache, offset, total_bytes,
				     &io_info_list);
	if (ret < 0) {
		cfs_log_err("prepare extent write error %d\n", ret);
		goto out;
	}
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		switch (extent_io_type(io_info)) {
		case EXTENT_WRITE_TYPE_RANDOM:
			ret = extent_dio_write_random(es, io_info, iter);
			break;
		case EXTENT_WRITE_TYPE_TINY:
			ret = extent_dio_write_tiny(es, io_info, iter);
			break;
		case EXTENT_WRITE_TYPE_NORMAL:
			ret = extent_dio_write_normal(es, io_info, iter);
			break;
		}
		list_del(&io_info->list);
		kfree(io_info);
		if (ret < 0)
			goto out;
	}
	ret = cfs_extent_stream_flush(es);

out:
	mutex_unlock(&es->w_lock);
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list, struct extent_io_info,
					   list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	return ret < 0 ? ret : total_bytes;
}

static int cfs_extent_update_partition(struct cfs_extent_client *ec)
{
	struct cfs_data_partition_view_array dp_views;
	struct cfs_data_partition_view *dp_view;
	struct cfs_data_partition *dp;
	struct hlist_node *tmp;
	u32 i;
	int ret;

	ret = cfs_master_get_data_partitions(ec->master, &dp_views);
	if (ret < 0)
		return ret;
	write_lock(&ec->lock);
	while (!list_empty(&ec->rw_partitions)) {
		dp = list_first_entry(&ec->rw_partitions,
				      struct cfs_data_partition, list);
		list_del(&dp->list);
	}
	hash_for_each_safe(ec->data_partitions, i, tmp, dp, hash) {
		hash_del(&dp->hash);
		cfs_data_partition_release(dp);
	}
	ec->select_dp = NULL;
	ec->nr_rw_partitions = 0;
	for (i = 0; i < dp_views.num; i++) {
		dp_view = &dp_views.base[i];
		dp = cfs_data_partition_new(dp_view);
		if (!dp) {
			ret = -ENOMEM;
			goto unlock;
		}
		hash_add(ec->data_partitions, &dp->hash, dp->id);
		if (dp->status == CFS_DP_STATUS_READWRITE) {
			list_add_tail(&dp->list, &ec->rw_partitions);
			ec->nr_rw_partitions++;
		}
	}

unlock:
	write_unlock(&ec->lock);
	cfs_data_partition_view_array_clear(&dp_views);
	return ret;
}

static struct cfs_data_partition *
cfs_extent_get_partition(struct cfs_extent_client *ec, u64 id)
{
	struct cfs_data_partition *dp = NULL;

	read_lock(&ec->lock);
	hash_for_each_possible(ec->data_partitions, dp, hash, id) {
		if (dp->id == id) {
			atomic_inc(&dp->refcnt);
			break;
		}
	}
	read_unlock(&ec->lock);
	return dp;
}

static u32 cfs_extent_get_partition_count(struct cfs_extent_client *ec)
{
	u32 nr;

	read_lock(&ec->lock);
	nr = ec->nr_rw_partitions;
	read_unlock(&ec->lock);
	return nr;
}

static struct cfs_data_partition *
cfs_extent_select_partition(struct cfs_extent_client *ec)
{
	struct cfs_data_partition *select_dp;
	u32 step;

	read_lock(&ec->lock);
	mutex_lock(&ec->select_lock);
	if (ec->select_dp)
		step = 1;
	else
		step = prandom_u32() % ec->nr_rw_partitions;
	step = max_t(u32, step, 1);

	while (step-- > 0) {
		if (!ec->select_dp)
			ec->select_dp = list_first_entry_or_null(
				&ec->rw_partitions, struct cfs_data_partition,
				list);
		else if (list_is_last(&ec->rw_partitions, &ec->select_dp->list))
			ec->select_dp = list_first_entry_or_null(
				&ec->rw_partitions, struct cfs_data_partition,
				list);
		else
			ec->select_dp = list_next_entry(ec->select_dp, list);
		if (!ec->select_dp)
			break;
	}
	select_dp = ec->select_dp;
	if (select_dp)
		atomic_inc(&select_dp->refcnt);
	mutex_unlock(&ec->select_lock);
	read_unlock(&ec->lock);
	return select_dp;
}

static void extent_update_partition_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_extent_client *ec = container_of(
		delayed_work, struct cfs_extent_client, update_dp_work);

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(EXTENT_UPDATE_DP_INTERVAL_MS));
	cfs_extent_update_partition(ec);
}

struct cfs_extent_client *
cfs_extent_client_new(struct cfs_master_client *master,
		      struct cfs_meta_client *meta)
{
	struct cfs_extent_client *ec;
	int ret;

	ec = kzalloc(sizeof(*ec), GFP_NOFS);
	if (!ec)
		return NULL;
	ec->master = master;
	ec->meta = meta;
	hash_init(ec->streams);
	hash_init(ec->data_partitions);
	INIT_LIST_HEAD(&ec->rw_partitions);
	rwlock_init(&ec->lock);
	mutex_init(&ec->select_lock);

	ret = cfs_extent_update_partition(ec);
	if (ret < 0) {
		kfree(ec);
		return ERR_PTR(ret);
	}
	INIT_DELAYED_WORK(&ec->update_dp_work, extent_update_partition_work_cb);
	schedule_delayed_work(&ec->update_dp_work,
			      msecs_to_jiffies(EXTENT_UPDATE_DP_INTERVAL_MS));
	return ec;
}

void cfs_extent_client_release(struct cfs_extent_client *ec)
{
	struct cfs_data_partition *dp;
	struct hlist_node *tmp;
	int i;

	if (!ec)
		return;
	cancel_delayed_work_sync(&ec->update_dp_work);
	hash_for_each_safe(ec->data_partitions, i, tmp, dp, hash) {
		hash_del(&dp->hash);
		cfs_data_partition_release(dp);
	}
	kfree(ec);
}

struct cfs_extent_stream *cfs_extent_stream_new(struct cfs_extent_client *ec,
						u64 ino)
{
	struct cfs_extent_stream *es;
	int ret;

	es = kzalloc(sizeof(*es), GFP_NOFS);
	if (!es)
		return NULL;
	ret = extent_cache_init(&es->cache, es);
	if (ret < 0) {
		kfree(es);
		return NULL;
	}
	es->ec = ec;
	es->ino = ino;
	hash_add(ec->streams, &es->hash, ino);
	mutex_init(&es->w_lock);
	return es;
}

void cfs_extent_stream_release(struct cfs_extent_stream *es)
{
	if (!es)
		return;
	cfs_extent_stream_flush(es);
	extent_writer_release(es->writer);
	extent_reader_release(es->reader);
	extent_cache_clear(&es->cache);
	hash_del(&es->hash);
	kfree(es);
}

int cfs_extent_stream_flush(struct cfs_extent_stream *es)
{
	if (es->writer)
		extent_writer_flush(es, es->writer);
	return 0;
}

int cfs_extent_stream_truncate(struct cfs_extent_stream *es, loff_t size)
{
	loff_t old_size;
	int ret;

	ret = cfs_extent_stream_flush(es);
	if (ret < 0) {
		cfs_log_err("extent stream flush error %d\n", ret);
		return ret;
	}
	extent_writer_release(es->writer);
	es->writer = NULL;

	ret = cfs_meta_truncate(es->ec->meta, es->ino, size);
	if (ret < 0) {
		cfs_log_err("meta turncate error %d\n", ret);
		return ret;
	}

	old_size = extent_cache_get_size(&es->cache);
	if (old_size <= size) {
		extent_cache_set_size(&es->cache, size, true);
		return 0;
	}

	extent_cache_truncate(&es->cache, size);
	ret = extent_cache_refresh(&es->cache, true);
	if (ret < 0)
		cfs_log_err("extent cache refresh error %d\n", ret);
	return ret;
}

int cfs_extent_module_init(void)
{
	if (extent_work_queue)
		return 0;
	extent_work_queue = alloc_workqueue(
		"extent_wq", WQ_NON_REENTRANT | WQ_MEM_RECLAIM, 0);
	if (!extent_work_queue) {
		return -ENOMEM;
	}
	return 0;
}

void cfs_extent_module_exit(void)
{
	if (extent_work_queue) {
		destroy_workqueue(extent_work_queue);
		extent_work_queue = NULL;
	}
}
