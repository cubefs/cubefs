/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_log.h"

struct cfs_log *cfs_log_new(void)
{
	struct cfs_log *log;

	log = kvzalloc(sizeof(*log), GFP_KERNEL);
	if (!log)
		return ERR_PTR(-ENOMEM);
	spin_lock_init(&log->lock);
	log->level = CFS_LOG_DEBUG;
	init_waitqueue_head(&log->wait);
	return log;
}

void cfs_log_release(struct cfs_log *log)
{
	if (!log)
		return;
	kvfree(log);
}

void cfs_log_write(struct cfs_log *log, const char *fmt, ...)
{
	char *text;
	u32 nr_text, offset = 0;
	u32 copy;
	va_list args;

	va_start(args, fmt);
	nr_text = vsnprintf(NULL, 0, fmt, args);
	va_end(args);

	nr_text++;
	text = kvmalloc(nr_text, GFP_KERNEL);
	if (!text) {
		cfs_pr_err("oom\n");
		return;
	}

	va_start(args, fmt);
	nr_text = vsnprintf(text, nr_text, fmt, args);
	va_end(args);

	spin_lock(&log->lock);
	while (nr_text > 0) {
		copy = min_t(u32, CFS_LOG_BUF_LEN - log->head, nr_text);
		memcpy(log->buf + log->head, text + offset, copy);
		log->head = (log->head + copy) % CFS_LOG_BUF_LEN;
		log->size = min_t(u32, log->size + copy, CFS_LOG_BUF_LEN);
		if (log->size == CFS_LOG_BUF_LEN)
			log->tail =
				(log->head - CFS_LOG_BUF_LEN) % CFS_LOG_BUF_LEN;
		offset += copy;
		nr_text -= copy;
	}
	spin_unlock(&log->lock);
	wake_up(&log->wait);
	kvfree(text);
}

int cfs_log_read(struct cfs_log *log, char __user *buf, size_t len)
{
	u32 offset = 0;
	u32 copy;

	spin_lock(&log->lock);
	len = min_t(u32, len, log->size);
	while (len > 0) {
		copy = min_t(u32, CFS_LOG_BUF_LEN - log->tail, len);
		if (copy_to_user(buf + offset, log->buf + log->tail, copy)) {
			spin_unlock(&log->lock);
			return -EFAULT;
		}
		log->tail = (log->tail + copy) % CFS_LOG_BUF_LEN;
		log->size -= copy;
		offset += copy;
		len -= copy;
	}
	spin_unlock(&log->lock);
	return offset;
}
