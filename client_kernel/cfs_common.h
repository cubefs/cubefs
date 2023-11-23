#ifndef __CFS_COMMON_H__
#define __CFS_COMMON_H__

#include <crypto/hash.h>
#include <crypto/md5.h>
#include <linux/crc32.h>
#include <linux/fs.h>
#include <linux/hashtable.h>
#include <linux/inet.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/list_lru.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/net.h>
#include <linux/pagemap.h>
#include <linux/pagevec.h>
#include <linux/poll.h>
#include <linux/printk.h>
#include <linux/signal.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/spinlock.h>
#include <linux/statfs.h>
#include <linux/string.h>
#include <linux/tcp.h>
#include <linux/vmalloc.h>
#include <linux/writeback.h>
#include <linux/xattr.h>
#ifdef KERNEL_HAS_SOCK_CREATE_KERN_WITH_NET
#include <net/net_namespace.h>
#endif

#include "config.h"

#undef pr_fmt
#define pr_fmt(fmt) "cfs: %s() " fmt

#define cfs_pr_err(fmt, ...) pr_err(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_warning(fmt, ...) pr_warning(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_notice(fmt, ...) pr_notice(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_info(fmt, ...) pr_info(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_pr_debug(fmt, ...) \
	printk(KERN_DEBUG pr_fmt(fmt), __FUNCTION__, ##__VA_ARGS__)

#define cfs_move(p, v) \
	p;             \
	p = v

/* define array */

#define DEFINE_ARRAY(type, name)                                               \
	struct name##_array {                                                  \
		type name *base;                                               \
		size_t num;                                                    \
		size_t cap;                                                    \
	};                                                                     \
	static inline int name##_array_init(struct name##_array *array,        \
					    size_t cap)                        \
	{                                                                      \
		array->base = NULL;                                            \
		array->num = 0;                                                \
		array->cap = cap;                                              \
		if (array->cap == 0)                                           \
			return 0;                                              \
		if (!(array->base = kcalloc(                                   \
			      array->cap, sizeof(array->base[0]), GFP_NOFS)))  \
			return -ENOMEM;                                        \
		return 0;                                                      \
	}                                                                      \
	static inline void name##_array_clear(struct name##_array *array)      \
	{                                                                      \
		if (!array || !array->base)                                    \
			return;                                                \
		while (array->num-- > 0) {                                     \
			name##_clear(&array->base[array->num]);                \
		}                                                              \
		kfree(array->base);                                            \
		array->base = NULL;                                            \
		array->cap = 0;                                                \
	}                                                                      \
	static inline void name##_array_move(struct name##_array *dst,         \
					     struct name##_array *src)         \
	{                                                                      \
		BUG_ON(!dst || !src);                                          \
		dst->base = cfs_move(src->base, NULL);                         \
		dst->num = cfs_move(src->num, 0);                              \
		dst->cap = cfs_move(src->cap, 0);                              \
	}                                                                      \
	static inline int name##_array_clone(struct name##_array *dst,         \
					     const struct name##_array *src)   \
	{                                                                      \
		int ret;                                                       \
		BUG_ON(!dst || !src);                                          \
		ret = name##_array_init(dst, src->num);                        \
		if (ret < 0)                                                   \
			return ret;                                            \
		dst->num = src->num;                                           \
		memcpy(dst->base, src->base, sizeof(src->base[0]) * src->num); \
		return 0;                                                      \
	}

/**
 * define u64_array.
 */
static inline void u64_clear(u64 *u)
{
	*u = 0;
}

DEFINE_ARRAY(, u64)

/**
 * define string_array.
 */
typedef char *string;
static inline void string_clear(string *s)
{
	kfree(*s);
	*s = NULL;
}

DEFINE_ARRAY(, string)

/**
 * define sockaddr_storage_array.
 */
static inline void sockaddr_storage_clear(struct sockaddr_storage *ss)
{
	(void)ss;
}

DEFINE_ARRAY(struct, sockaddr_storage)

/**
 * define default block size of cubefs.
 */
#define CFS_DEFAULT_BLK_SIZE (1u << 12)

/**
 * define string parse function.
 */
#define CFS_MAX_U64_STRING_LEN 128
static inline int cfs_kstrntou64(const char *start, size_t len,
				 unsigned int base, u64 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou64(buf, base, res);
}

static inline int cfs_kstrntos64(const char *start, size_t len,
				 unsigned int base, s64 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtos64(buf, base, res);
}

static inline int cfs_kstrntou32(const char *start, size_t len,
				 unsigned int base, u32 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou32(buf, base, res);
}

static inline int cfs_kstrntou16(const char *start, size_t len,
				 unsigned int base, u16 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou16(buf, base, res);
}

static inline int cfs_kstrntou8(const char *start, size_t len,
				unsigned int base, u8 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtou8(buf, base, res);
}

static inline int cfs_kstrntos8(const char *start, size_t len,
				unsigned int base, s8 *res)
{
	char buf[CFS_MAX_U64_STRING_LEN];

	if (len >= CFS_MAX_U64_STRING_LEN)
		return -EOVERFLOW;
	strncpy(buf, start, len);
	buf[len] = '\0';
	return kstrtos8(buf, base, res);
}

static inline int cfs_kstrntobool(const char *start, size_t len, bool *res)
{
	if (strncasecmp(start, "false", len) == 0) {
		*res = false;
		return 0;
	} else if (strncasecmp(start, "true", len) == 0) {
		*res = true;
		return 0;
	} else {
		return -EINVAL;
	}
}

const char *cfs_pr_addr(const struct sockaddr_storage *ss);
int cfs_parse_addr(const char *str, size_t len, struct sockaddr_storage *ss);
int cfs_addr_cmp(const struct sockaddr_storage *ss1,
		 const struct sockaddr_storage *ss2);
const char *cfs_pr_time(struct timespec *time);
int cfs_parse_time(const char *str, size_t len, struct timespec *time);

int cfs_base64_encode(const char *str, size_t len, char **base64);
int cfs_base64_decode(const char *base64, size_t base64_len, char **str);

#endif
