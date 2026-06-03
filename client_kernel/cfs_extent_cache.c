/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"

/**
 * Max number of items per btree+ node
 */
#define EXTENT_ITEM_COUNT_PRE_BTREE_NODE 128

struct cfs_extent_io_info *
cfs_extent_io_info_new(loff_t offset, size_t size,
		       const struct cfs_packet_extent *ext)
{
	struct cfs_extent_io_info *io_info;

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

void cfs_extent_io_info_release(struct cfs_extent_io_info *io_info)
{
	if (io_info)
		kfree(io_info);
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

int cfs_extent_cache_init(struct cfs_extent_cache *cache,
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

void cfs_extent_cache_clear(struct cfs_extent_cache *cache)
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

void cfs_extent_cache_set_size(struct cfs_extent_cache *cache, loff_t size,
			       bool sync)
{
	mutex_lock(&cache->lock);
	cache->size = size;
	if (sync)
		cache->generation++;
	mutex_unlock(&cache->lock);
}

loff_t cfs_extent_cache_get_size(struct cfs_extent_cache *cache)
{
	loff_t size;

	mutex_lock(&cache->lock);
	size = cache->size;
	mutex_unlock(&cache->lock);
	return size;
}

bool cfs_extent_cache_get_end(struct cfs_extent_cache *cache, u64 offset,
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

void cfs_extent_cache_truncate(struct cfs_extent_cache *cache, loff_t size)
{
	const struct cfs_packet_extent *ext;

	mutex_lock(&cache->lock);
	for (ext = extent_btree_get_ge(cache->discard, size); ext;
	     ext = extent_btree_get_next(cache->discard, ext->file_offset)) {
		ext = btree_delete(cache->discard, ext);
	}
	mutex_unlock(&cache->lock);
}

int cfs_extent_cache_refresh(struct cfs_extent_cache *cache, bool force)
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
int cfs_extent_cache_append(struct cfs_extent_cache *cache,
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

#ifdef DEBUG
	cfs_pr_debug(
		"pid=%llu, ext_id=%llu, ext_offset=%llu, file_offset=%llu, ext_size=%u\n",
		extent->pid, extent->ext_id, extent->ext_offset,
		extent->file_offset, extent->size);
#endif
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
	return ret;
}

void cfs_extent_cache_remove_discard(
	struct cfs_extent_cache *cache,
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
int cfs_prepare_extent_io_list(struct cfs_extent_cache *cache, loff_t offset,
			       size_t size, struct list_head *io_info_list)
{
	const struct cfs_packet_extent *ext;
	struct cfs_extent_io_info *io_info;
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
				io_info = cfs_extent_io_info_new(
					start, ext_start - start, NULL);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);

				/* add non-hole(ext_start, end) */
				io_info = cfs_extent_io_info_new(
					ext_start, end - ext_start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = end;
				break;
			} else {
				/* add hole(start, ext_start) */
				io_info = cfs_extent_io_info_new(
					start, ext_start - start, NULL);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);

				/* add non-hole(ext_start, ext_end) */
				io_info = cfs_extent_io_info_new(
					ext_start, ext_end - ext_start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = ext_end;
			}
		} else if (start < ext_end) {
			if (end <= ext_end) {
				/* add non-hole(start, end) */
				io_info = cfs_extent_io_info_new(
					start, end - start, ext);
				if (!io_info)
					goto oom;
				list_add_tail(&io_info->list, io_info_list);
				start = end;
				break;
			} else {
				/* add non-hole(start, ext_end) */
				io_info = cfs_extent_io_info_new(
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
		io_info = cfs_extent_io_info_new(start, end - start, NULL);
		if (!io_info)
			goto oom;
		list_add_tail(&io_info->list, io_info_list);
	}
	mutex_unlock(&cache->lock);
	return 0;

oom:
	mutex_unlock(&cache->lock);
	while (!list_empty(io_info_list)) {
		io_info = list_first_entry(io_info_list,
					   struct cfs_extent_io_info, list);
		list_del(&io_info->list);
		kfree(io_info);
	}
	return -ENOMEM;
}
