#ifndef __CFS_PAGE_H__
#define __CFS_PAGE_H__

#include "cfs_common.h"

/**
 * Define the maximum number of pages transferred to server.
 * The Maximum write size of tiny extent is 1MB,
 * the Maximum write size of normal extent is 128KB,
 * so the maximum number of pages is 1MB / PAGE_SIZE.
 */
#define CFS_PAGE_VEC_NUM ((1024 * 1024 + PAGE_SIZE - 1) / PAGE_SIZE)

struct cfs_page {
	struct page *page;
	atomic_t io_bytes;
};

struct cfs_page *cfs_page_new(struct page *page);
void cfs_page_release(struct cfs_page *cpage);

static inline void cfs_page_io_set(struct cfs_page *cpage, int bytes)
{
	atomic_set(&cpage->io_bytes, bytes);
}

static inline bool cfs_page_io_account(struct cfs_page *cpage, int bytes)
{
	return atomic_sub_and_test(bytes, &cpage->io_bytes);
}

struct cfs_page_frag {
	struct cfs_page *page;
#if (BITS_PER_LONG > 32) || (PAGE_SIZE >= 65536)
	u32 offset;
	u32 size;
#else
	u16 offset;
	u16 size;
#endif
};

u32 cfs_page_frags_crc32(const struct cfs_page_frag *frags, size_t nr);

struct cfs_page_iter {
	struct cfs_page **pages;
	size_t nr;
	size_t count;
#if (BITS_PER_LONG > 32) || (PAGE_SIZE >= 65536)
	u32 first_page_offset;
	u32 end_page_size;
#else
	u16 first_page_offset;
	u16 end_page_size;
#endif
};

void cfs_page_iter_advance(struct cfs_page_iter *iter, size_t bytes);

static inline void cfs_page_iter_init(struct cfs_page_iter *iter,
				      struct cfs_page **pages, size_t nr,
				      size_t first_page_offset,
				      size_t end_page_size)
{
	iter->pages = pages;
	iter->nr = nr;
	iter->first_page_offset = first_page_offset;
	iter->end_page_size = end_page_size;
}

static inline size_t cfs_page_iter_count(struct cfs_page_iter *iter)
{
	if (iter->nr == 0)
		return 0;
	return (iter->nr - 1) * PAGE_SIZE - iter->first_page_offset +
	       iter->end_page_size;
}

size_t cfs_page_iter_get_frags(struct cfs_page_iter *iter,
			       struct cfs_page_frag *dst, size_t nr_dst,
			       size_t *len);

struct cfs_page_vec {
	size_t nr;
	struct page *pages[CFS_PAGE_VEC_NUM +
			   ((128 * 1024 + PAGE_SIZE - 1) / PAGE_SIZE)];
};

struct cfs_page_vec *cfs_page_vec_new(void);
void cfs_page_vec_release(struct cfs_page_vec *vec);

static inline bool cfs_page_vec_append(struct cfs_page_vec *vec,
				       struct page *page)
{
	if (vec->nr == ARRAY_SIZE(vec->pages))
		return false;
	if (vec->nr > 0 && vec->pages[vec->nr - 1]->index + 1 != page->index)
		return false;
	vec->pages[vec->nr++] = page;
	return true;
}

static inline bool cfs_page_vec_empty(struct cfs_page_vec *vec)
{
	return vec->nr == 0;
}

static inline void cfs_page_vec_clear(struct cfs_page_vec *vec)
{
	vec->nr = 0;
}

int cfs_page_module_init(void);
void cfs_page_module_exit(void);

#endif