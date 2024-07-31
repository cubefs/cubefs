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

#include <linux/kernel.h>
#include <linux/uio.h>
#include <linux/version.h>
#include <linux/uaccess.h>

#ifdef KERNEL_HAS_IOV_ITER_IN_FS
#include <linux/fs.h>
#else
#include <linux/uio.h>
#endif

#ifndef KERNEL_HAS_IOV_ITER_IOVEC
static inline struct iovec iov_iter_iovec(const struct iov_iter *iter)
{
   return (struct iovec) {
      .iov_base = iter->iov->iov_base + iter->iov_offset,
      .iov_len = min(iter->count, iter->iov->iov_len - iter->iov_offset),
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


#ifndef KERNEL_HAS_ITER_IS_IOVEC
static inline bool iter_is_iovec(struct iov_iter* i)
{
   return true;
}
#endif

#ifndef KERNEL_HAS_COPY_FROM_ITER_FULL
static inline bool copy_from_iter_full(void* to, size_t bytes, struct iov_iter* i)
{
   /* FIXME: check for != IOV iters */

   size_t copy, left;
   struct iovec iov;
   char __user* buf;
   void *dest = to;

   if (unlikely(bytes > i->count) )
      bytes = i->count;

   if (unlikely(!bytes) )
      return false;

   iov = iov_iter_iovec(i);
   buf = iov.iov_base;
   copy = min(bytes, iov.iov_len);
   left = __copy_from_user(dest, buf, copy);
   copy -= left;
   dest += copy;
   bytes -= copy;
   iov_iter_advance(i, copy);

   while (unlikely(left && bytes) ) {
      iov = iov_iter_iovec(i);
      buf = iov.iov_base;
      copy = min(bytes, iov.iov_len);
      left = __copy_from_user(dest, buf, copy);
      copy -= left;
      dest += copy;
      bytes -= copy;
      iov_iter_advance(i, copy);
   }

   return true;
}

static inline size_t copy_to_iter(void* from, size_t bytes, struct iov_iter* i)
{
   /* FIXME: check for != IOV iters */

   size_t copy, left, wanted;
   struct iovec iov;
   char __user *buf;
   void *source = from;

   if (unlikely(bytes > i->count) )
      bytes = i->count;

   if (unlikely(!bytes) )
      return 0;

   wanted = bytes;

   iov = iov_iter_iovec(i);
   buf = iov.iov_base;
   copy = min(bytes, iov.iov_len);
   left = __copy_to_user(buf, source, copy);
   copy -= left;
   source += copy;
   bytes -= copy;
   iov_iter_advance(i, copy);

   while (unlikely(left && bytes) ) {
      iov = iov_iter_iovec(i);
      buf = iov.iov_base;
      copy = min(bytes, iov.iov_len);
      left = __copy_to_user(buf, source, copy);
      copy -= left;
      source += copy;
      bytes -= copy;
      iov_iter_advance(i, copy);
   }

   return wanted - bytes;
}


//XXX this code is written to work for ITER_IOVEC but will also work (due to same layout)
// for ITER_KVEC. The type punning is probably illegal as far as C is concerned, but that's
// how it is done even in the Linux kernel.
#define iterate_iovec(i, n, __v, __p, skip, STEP) {	\
	size_t left;					\
	size_t wanted = n;				\
	__p = i->iov;					\
	__v.iov_len = min(n, __p->iov_len - skip);	\
	if (likely(__v.iov_len)) {			\
		__v.iov_base = __p->iov_base + skip;	\
		left = (STEP);				\
		__v.iov_len -= left;			\
		skip += __v.iov_len;			\
		n -= __v.iov_len;			\
	} else {					\
		left = 0;				\
	}						\
	while (unlikely(!left && n)) {			\
		__p++;					\
		__v.iov_len = min(n, __p->iov_len);	\
		if (unlikely(!__v.iov_len))		\
			continue;			\
		__v.iov_base = __p->iov_base;		\
		left = (STEP);				\
		__v.iov_len -= left;			\
		skip = __v.iov_len;			\
		n -= __v.iov_len;			\
	}						\
	n = wanted - n;					\
}

//XXX this code is written to work for ITER_IOVEC -- see comment above.
#define iterate_and_advance(i, n, v, I, B, K) {			\
	size_t skip = i->iov_offset;				\
   const struct iovec *iov;			\
   struct iovec v;					\
   iterate_iovec(i, n, v, iov, skip, (I))		\
   if (skip == iov->iov_len) {			\
      iov++;					\
      skip = 0;				\
   }						\
   i->nr_segs -= iov - i->iov;			\
   i->iov = iov;					\
	i->count -= n;						\
	i->iov_offset = skip;					\
}

static inline size_t iov_iter_zero(size_t bytes, struct iov_iter *i)
{
	iterate_and_advance(i, bytes, v,
		clear_user(v.iov_base, v.iov_len),
		memzero_page(v.bv_page, v.bv_offset, v.bv_len),
		memset(v.iov_base, 0, v.iov_len)
	)

	return bytes;
}
#endif

#endif
