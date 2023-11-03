#include "cfs_page.h"
#include "crc32.h"

static struct kmem_cache *cpage_cache;
static struct kmem_cache *page_vec_cache;

u32 cfs_page_frags_crc32(const struct cfs_page_frag *frags, size_t nr)
{
	crc32_t crc = 0;
	size_t i;

	for (i = 0; i < nr; i++) {
		crc32_recalculate(kmap(frags[i].page->page) + frags[i].offset,
				  frags[i].size, &crc);
		kunmap(frags[i].page->page);
	}
	return crc;
}

struct cfs_page *cfs_page_new(struct page *page)
{
	struct cfs_page *cpage;

	cpage = kmem_cache_zalloc(cpage_cache, GFP_NOFS);
	if (cpage) {
		get_page(page);
		cpage->page = page;
		atomic_set(&cpage->refcnt, 1);
	}
	return cpage;
}

void cfs_page_release(struct cfs_page *cpage)
{
	if (cpage && atomic_dec_and_test(&cpage->refcnt)) {
		put_page(cpage->page);
		kmem_cache_free(cpage_cache, cpage);
	}
}

// size_t cfs_page_iter_copy_to_vec(struct cfs_page_iter *iter,
// 				 struct cfs_page *dst, size_t nr_dst,
// 				 size_t *len)
// {
// 	size_t i;
// 	size_t copied = 0;
// 	size_t page_offset;

// 	if (*len == 0 || iter->nr == 0 || nr_dst == 0) {
// 		*len = 0;
// 		return 0;
// 	}
// 	page_offset = iter->page_offset;
// 	for (i = 0; i < iter->nr && i < nr_dst; i++) {
// 		dst[i].page = iter->pages[i]->page;
// 		dst[i].offset = iter->pages[i]->offset + page_offset;
// 		dst[i].size = iter->pages[i]->size - page_offset;
// 		copied += dst[i].size;
// 		if (copied >= *len) {
// 			dst[i].size -= copied - *len;
// 			copied = *len;
// 			break;
// 		}
// 		page_offset = 0;
// 	}
// 	*len = copied;
// 	return i + 1;
// }

size_t cfs_page_iter_get_frags(struct cfs_page_iter *iter,
			       struct cfs_page_frag *dst, size_t nr_dst,
			       size_t *len)
{
	size_t i;
	size_t copied = 0;
	size_t page_offset;

	if (*len == 0 || iter->nr == 0 || nr_dst == 0) {
		*len = 0;
		return 0;
	}
	page_offset = iter->first_page_offset;
	for (i = 0; i < iter->nr && i < nr_dst; i++) {
		dst[i].page = iter->pages[i];
		dst[i].offset = page_offset;
		if (i + 1 == iter->nr)
			dst[i].size = iter->end_page_size - page_offset;
		else
			dst[i].size = PAGE_SIZE - page_offset;
		copied += dst[i].size;
		if (copied >= *len) {
			dst[i].size -= copied - *len;
			copied = *len;
			break;
		}
		page_offset = 0;
	}
	*len = copied;
	return i + 1;
}

void cfs_page_iter_advance(struct cfs_page_iter *iter, size_t bytes)
{
	BUG_ON(cfs_page_iter_count(iter) < bytes);

	bytes += iter->first_page_offset;
	while (bytes >= PAGE_SIZE) {
		bytes -= PAGE_SIZE;
		iter->pages++;
		iter->nr--;
	}
	iter->first_page_offset = bytes;
}

struct cfs_page_vec *cfs_page_vec_new(void)
{
	return kmem_cache_zalloc(page_vec_cache, GFP_NOFS);
}

void cfs_page_vec_release(struct cfs_page_vec *vec)
{
	kmem_cache_free(page_vec_cache, vec);
}

int cfs_page_module_init(void)
{
	if (!cpage_cache) {
		cpage_cache = KMEM_CACHE(cfs_page, SLAB_MEM_SPREAD);
		if (!cpage_cache)
			goto oom;
	}
	if (!page_vec_cache) {
		page_vec_cache = KMEM_CACHE(cfs_page_vec, SLAB_MEM_SPREAD);
		if (!page_vec_cache)
			goto oom;
	}
	return 0;

oom:
	cfs_page_module_exit();
	return -ENOMEM;
}

void cfs_page_module_exit(void)
{
	if (cpage_cache) {
		kmem_cache_destroy(cpage_cache);
		cpage_cache = NULL;
	}
	if (page_vec_cache) {
		kmem_cache_destroy(page_vec_cache);
		page_vec_cache = NULL;
	}
}