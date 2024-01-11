/*
 * compatibility for older kernels. this code is mostly taken from include/linux/uio.h,
 * include/linuxfs/fs.h and associated .c files.
 *
 * the originals are licensed as:
 *
 *		This program is free software; you can redistribute it and/or
 *		modify it under the terms of the GNU General Public License
 *		as published by the Free Software Foundation; either version
 *		2 of the License, or (at your option) any later version.
 */

#ifndef os_iov_iter_h_gkoNxI6c8tqzi7GeSKSVlb
#define os_iov_iter_h_gkoNxI6c8tqzi7GeSKSVlb

#define KERNEL_HAS_IOV_ITER_IN_FS
#define KERNEL_HAS_IOV_ITER_TRUNCATE

#include <linux/kernel.h>
#include <linux/uio.h>
#include <linux/version.h>
#include <linux/uaccess.h>

#if defined(KERNEL_HAS_IOV_ITER_IN_FS)
#include <linux/fs.h>
#else
#include <linux/uio.h>
#endif

#ifndef KERNEL_HAS_IOV_ITER_TYPE
#ifdef KERNEL_HAS_ITER_KVEC
static inline int iov_iter_type(const struct iov_iter *i)
{
	return i->type & ~(READ | WRITE);
}
#endif
#endif

#ifdef KERNEL_HAS_ITER_KVEC
#define beegfs_iov_for_each(iov, iter, start)                              \
	if ((iov_iter_type(start) == ITER_IOVEC) ||                        \
	    (iov_iter_type(start) == ITER_KVEC))                           \
		for (iter = (*start);                                      \
		     (iter).count && ((iov = iov_iter_iovec(&(iter))), 1); \
		     iov_iter_advance(&(iter), (iov).iov_len))
#else
#define beegfs_iov_for_each(iov, iter, start)                      \
	for (iter = (*start);                                      \
	     (iter).count && ((iov = iov_iter_iovec(&(iter))), 1); \
	     iov_iter_advance(&(iter), (iov).iov_len))
#endif

#if !defined(KERNEL_HAS_IOV_ITER_IOVEC)
static inline struct iovec iov_iter_iovec(const struct iov_iter *iter)
{
	return (struct iovec){
		.iov_base = iter->iov->iov_base + iter->iov_offset,
		.iov_len =
			min(iter->count, iter->iov->iov_len - iter->iov_offset),
	};
}
#endif

#ifndef KERNEL_HAS_IOV_ITER_TRUNCATE
static inline void iov_iter_truncate(struct iov_iter *i, size_t count)
{
	if (i->count > count)
		i->count = count;
}
#endif

static inline void BEEGFS_IOV_ITER_INIT(struct iov_iter *iter, int direction,
					const struct iovec *iov,
					unsigned long nr_segs, size_t count)
{
#ifdef KERNEL_HAS_IOV_ITER_INIT_DIR
	iov_iter_init(iter, direction, iov, nr_segs, count);
#else
	iov_iter_init(iter, iov, nr_segs, count, 0);
#endif
}

#if !defined(KERNEL_HAS_ITER_BVEC)
static inline bool iter_is_iovec(struct iov_iter *i)
{
	return true;
}
#elif !defined(KERNEL_HAS_ITER_IS_IOVEC)
static inline bool iter_is_iovec(struct iov_iter *i)
{
	return !(i->type & (ITER_BVEC | ITER_KVEC));
}
#endif

#ifndef KERNEL_HAS_COPY_FROM_ITER
static inline size_t copy_from_iter(void *to, size_t bytes, struct iov_iter *i)
{
	/* FIXME: check for != IOV iters */

	size_t copy, left, wanted;
	struct iovec iov;
	char __user *buf;

	if (unlikely(bytes > i->count))
		bytes = i->count;

	if (unlikely(!bytes))
		return 0;

	wanted = bytes;
	iov = iov_iter_iovec(i);
	buf = iov.iov_base;
	copy = min(bytes, iov.iov_len);

	left = __copy_from_user(to, buf, copy);
	copy -= left;
	to += copy;
	bytes -= copy;
	while (unlikely(!left && bytes)) {
		iov_iter_advance(i, copy);
		iov = iov_iter_iovec(i);
		buf = iov.iov_base;
		copy = min(bytes, iov.iov_len);
		left = __copy_from_user(to, buf, copy);
		copy -= left;
		to += copy;
		bytes -= copy;
	}

	iov_iter_advance(i, copy);
	return wanted - bytes;
}

static inline size_t copy_to_iter(void *from, size_t bytes, struct iov_iter *i)
{
	/* FIXME: check for != IOV iters */

	size_t copy, left, wanted;
	struct iovec iov;
	char __user *buf;

	if (unlikely(bytes > i->count))
		bytes = i->count;

	if (unlikely(!bytes))
		return 0;

	wanted = bytes;
	iov = iov_iter_iovec(i);
	buf = iov.iov_base;
	copy = min(bytes, iov.iov_len);

	left = __copy_to_user(buf, from, copy);
	copy -= left;
	from += copy;
	bytes -= copy;
	while (unlikely(!left && bytes)) {
		iov_iter_advance(i, copy);
		iov = iov_iter_iovec(i);
		copy = min(bytes, iov.iov_len);
		left = __copy_to_user(buf, from, copy);
		copy -= left;
		from += copy;
		bytes -= copy;
	}

	iov_iter_advance(i, copy);
	return wanted - bytes;
}
#endif

#endif
