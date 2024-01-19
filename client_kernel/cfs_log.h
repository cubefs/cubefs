/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_LOG_H__
#define __CFS_LOG_H__

#include <linux/time.h>
#include <linux/timex.h>
#include <linux/rtc.h>

#include "cfs_common.h"

#define CFS_LOG_BUF_LEN (1 << 20)

enum cfs_log_level {
	CFS_LOG_ERROR = 0,
	CFS_LOG_WARN,
	CFS_LOG_INFO,
	CFS_LOG_DEBUG,
};

#define CFS_LOG_AUDIT CFS_LOG_INFO

struct cfs_log {
	char buf[CFS_LOG_BUF_LEN];
	u32 head;
	u32 tail;
	u32 size;
	spinlock_t lock;
	enum cfs_log_level level;
	wait_queue_head_t wait;
};

struct cfs_log *cfs_log_new(void);
void cfs_log_release(struct cfs_log *log);
void cfs_log_write(struct cfs_log *log, const char *fmt, ...);
int cfs_log_read(struct cfs_log *log, char __user *buf, size_t len);

static inline u32 cfs_log_size(struct cfs_log *log)
{
	u32 size;

	spin_lock(&log->lock);
	size = log->size;
	spin_unlock(&log->lock);
	return size;
}

static inline void cfs_log_set_level(struct cfs_log *log,
				     enum cfs_log_level level)
{
	log->level = level;
}

static inline enum cfs_log_level cfs_log_get_level(struct cfs_log *log)
{
	return log->level;
}

#define CFS_LOG_PREFIX "%lld [%s] %s:%u %s() "

static inline s64 cfs_log_time(void)
{
#ifdef KERNEL_HAS_TIME64_TO_TM
	return jiffies_to_msecs(jiffies);
#else
	struct timeval tv;

	do_gettimeofday(&tv);
	return tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

#define cfs_log_debug(log, fmt, ...)                                           \
	do {                                                                   \
		if (cfs_log_get_level(log) >= CFS_LOG_DEBUG)                   \
			cfs_log_write(log, CFS_LOG_PREFIX fmt, cfs_log_time(), \
				      "DEBUG", strrchr(__FILE__, '/') + 1,     \
				      __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
	} while (0)

#define cfs_log_info(log, fmt, ...)                                            \
	do {                                                                   \
		if (cfs_log_get_level(log) >= CFS_LOG_INFO)                    \
			cfs_log_write(log, CFS_LOG_PREFIX fmt, cfs_log_time(), \
				      "INFO", strrchr(__FILE__, '/') + 1,      \
				      __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
	} while (0)

#define cfs_log_warn(log, fmt, ...)                                            \
	do {                                                                   \
		if (cfs_log_get_level(log) >= CFS_LOG_WARN)                    \
			cfs_log_write(log, CFS_LOG_PREFIX fmt, cfs_log_time(), \
				      "WARN", strrchr(__FILE__, '/') + 1,      \
				      __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
	} while (0)

#define cfs_log_error(log, fmt, ...)                                           \
	do {                                                                   \
		if (cfs_log_get_level(log) >= CFS_LOG_ERROR)                   \
			cfs_log_write(log, CFS_LOG_PREFIX fmt, cfs_log_time(), \
				      "ERROR", strrchr(__FILE__, '/') + 1,     \
				      __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
	} while (0)

static inline void
cfs_log_audit_inline(struct cfs_log *log, const char *file, unsigned line,
		     const char *func, const char *op,
		     struct dentry *src_dentry, struct dentry *dst_dentry,
		     int err, u64 latency_us, u64 src_ino, u64 dst_ino)
{
	char *src_buf = NULL, *src_path = NULL;
	char *dst_buf = NULL, *dst_path = NULL;

	if (src_dentry) {
		if (!(src_buf = kzalloc(PATH_MAX, GFP_KERNEL))) {
			cfs_pr_err("oom\n");
			return;
		}
		src_path = dentry_path_raw(src_dentry, src_buf, PATH_MAX);
		if (IS_ERR(src_path)) {
			cfs_pr_err("build dentry path error %ld\n",
				   PTR_ERR(src_path));
			kfree(src_buf);
			return;
		}
	}

	if (dst_dentry) {
		if (!(dst_buf = kzalloc(PATH_MAX, GFP_KERNEL))) {
			cfs_pr_err("oom\n");
			if (src_buf)
				kfree(src_buf);
			return;
		}
		dst_path = dentry_path_raw(dst_dentry, dst_buf, PATH_MAX);
		if (IS_ERR(dst_path)) {
			cfs_pr_err("build dentry path error %ld\n",
				   PTR_ERR(dst_path));
			kfree(dst_buf);
			if (src_buf)
				kfree(src_buf);
			return;
		}
	}

	cfs_log_write(log,
		      CFS_LOG_PREFIX "%s, %s, %s, %d, %llu us, %llu, %llu\n",
		      cfs_log_time(), "AUDIT", strrchr(file, '/') + 1, line,
		      func, op, src_path ? src_path : "nil",
		      dst_path ? dst_path : "nil", err, latency_us, src_ino,
		      dst_ino);

	if (src_buf)
		kfree(src_buf);
	if (dst_buf)
		kfree(dst_buf);
}

#define cfs_log_audit(log, op, src_dentry, dst_dentry, error, latency_us,   \
		      src_ino, dst_ino)                                     \
	do {                                                                \
		if (cfs_log_get_level(log) >= CFS_LOG_AUDIT)                \
			cfs_log_audit_inline(log, __FILE__, __LINE__,       \
					     __FUNCTION__, op, src_dentry,  \
					     dst_dentry, error, latency_us, \
					     src_ino, dst_ino);             \
	} while (0)
#endif
