/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_BUFFER_H__
#define __CFS_BUFFER_H__

#include "cfs_common.h"

struct cfs_buffer {
	size_t capacity;
	size_t pos;
	char *data;
};

struct cfs_buffer *cfs_buffer_new(size_t size);
void cfs_buffer_release(struct cfs_buffer *buffer);

static inline char *cfs_buffer_data(struct cfs_buffer *buffer)
{
	return buffer->data;
}

static inline void cfs_buffer_seek(struct cfs_buffer *buffer, int offset)
{
	if (offset > 0) {
		if (buffer->pos > buffer->capacity - offset)
			buffer->pos = buffer->capacity;
		else
			buffer->pos += offset;
	} else {
		if (buffer->pos < -offset)
			buffer->pos = 0;
		else
			buffer->pos += offset;
	}
}

static inline size_t cfs_buffer_size(struct cfs_buffer *buffer)
{
	return buffer->pos;
}

static inline size_t cfs_buffer_capacity(struct cfs_buffer *buffer)
{
	return buffer->capacity;
}

static inline size_t cfs_buffer_avail_size(struct cfs_buffer *buffer)
{
	return buffer->capacity - buffer->pos;
}

static inline void cfs_buffer_reset(struct cfs_buffer *buffer)
{
	buffer->pos = 0;
}

int cfs_buffer_resize(struct cfs_buffer *buffer, size_t n);
int cfs_buffer_grow(struct cfs_buffer *buffer, size_t n);
int cfs_buffer_write(struct cfs_buffer *buffer, const char *fmt, ...);
int cfs_buffer_init(struct cfs_buffer *buffer, size_t n);
#endif
