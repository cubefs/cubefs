#ifndef __CFS_COMMON_H__
#define __CFS_COMMON_H__

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
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/statfs.h>
#include <linux/string.h>
#include <linux/tcp.h>
#include <linux/vmalloc.h>
#include <linux/writeback.h>

#undef pr_fmt
#define pr_fmt(fmt) "cfs: %s() " fmt

#define cfs_log_err(fmt, ...) pr_err(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_log_warning(fmt, ...) pr_warning(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_log_notice(fmt, ...) pr_notice(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_log_info(fmt, ...) pr_info(fmt, __FUNCTION__, ##__VA_ARGS__)
#define cfs_log_debug(fmt, ...) \
	printk(KERN_DEBUG pr_fmt(fmt), __FUNCTION__, ##__VA_ARGS__)

#define cfs_move(p, v) \
	p;             \
	p = v

/* define array */

#define DEFINE_ARRAY(type, name)                                              \
	struct name##_array {                                                 \
		type name *base;                                              \
		size_t num;                                                   \
	};                                                                    \
	static inline int name##_array_init(struct name##_array *array,       \
					    size_t num)                       \
	{                                                                     \
		array->num = num;                                             \
		if (array->num == 0)                                          \
			return 0;                                             \
		if (!(array->base = kcalloc(                                  \
			      array->num, sizeof(array->base[0]), GFP_NOFS))) \
			return -ENOMEM;                                       \
		return 0;                                                     \
	}                                                                     \
	static inline void name##_array_clear(struct name##_array *array)     \
	{                                                                     \
		if (!array || !array->base)                                   \
			return;                                               \
		for (; array->num > 0; array->num--) {                        \
			name##_clear(&array->base[array->num - 1]);           \
		}                                                             \
		kfree(array->base);                                           \
		array->base = NULL;                                           \
	}                                                                     \
	static inline void name##_array_move(struct name##_array *array1,     \
					     struct name##_array *array2)     \
	{                                                                     \
		BUG_ON(!array1 || !array2);                                   \
		array1->base = cfs_move(array2->base, NULL);                  \
		array1->num = cfs_move(array2->num, 0);                       \
	}                                                                     \
	static inline int name##_array_clone(                                 \
		struct name##_array *array1,                                  \
		const struct name##_array *array2)                            \
	{                                                                     \
		int ret;                                                      \
		BUG_ON(!array1 || !array2);                                   \
		ret = name##_array_init(array1, array2->num);                 \
		if (ret == 0)                                                 \
			memcpy(array1->base, array2->base,                    \
			       sizeof(array2->base[0]) * array2->num);        \
		return ret;                                                   \
	}

/**
 * define u32 array.
 */
static inline void u32_clear(u32 *u)
{
	*u = 0;
}

DEFINE_ARRAY(, u32)

/**
 * define char array.
 */
typedef char *string;
static inline void string_clear(string *s)
{
	kfree(*s);
	*s = NULL;
}

DEFINE_ARRAY(, string)

/**
 * define sockaddr_storage array.
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
