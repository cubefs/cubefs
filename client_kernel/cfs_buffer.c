/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_buffer.h"

#define CFS_BUFFER_SIZE 1024

struct cfs_buffer *cfs_buffer_new(size_t size)
{
	struct cfs_buffer *buffer;

	buffer = kvzalloc(sizeof(*buffer), GFP_KERNEL);
	if (!buffer)
		return NULL;
	size = max_t(size_t, size, CFS_BUFFER_SIZE);
	buffer->data = kvmalloc(size, GFP_KERNEL);
	if (!buffer->data) {
		kvfree(buffer);
		return NULL;
	}
	buffer->capacity = size;
	buffer->pos = 0;
	return buffer;
}

void cfs_buffer_release(struct cfs_buffer *buffer)
{
	if (!buffer)
		return;
	kvfree(buffer->data);
	kvfree(buffer);
}

int cfs_buffer_resize(struct cfs_buffer *buffer, size_t n)
{
	char *data;

	n = max_t(size_t, n, CFS_BUFFER_SIZE);
	if (n > buffer->capacity) {
		data = kvmalloc(n, GFP_KERNEL);
		if (!data)
			return -ENOMEM;
		kvfree(buffer->data);
		buffer->data = data;
		buffer->capacity = n;
	}
	buffer->pos = 0;
	return 0;
}

int cfs_buffer_grow(struct cfs_buffer *buffer, size_t n)
{
	char *data;

	n = max_t(size_t, n, CFS_BUFFER_SIZE);
	data = kvmalloc(buffer->capacity + n, GFP_KERNEL);
	if (!data)
		return -ENOMEM;
	memcpy(data, buffer->data, buffer->pos);
	kvfree(buffer->data);
	buffer->data = data;
	buffer->capacity += n;
	return 0;
}

int cfs_buffer_write(struct cfs_buffer *buffer, const char *fmt, ...)
{
	size_t avail_size;
	va_list args;
	int ret;

	avail_size = buffer->capacity - buffer->pos;
	va_start(args, fmt);
	ret = vsnprintf(buffer->data + buffer->pos, avail_size, fmt, args);
	va_end(args);

	if (ret < 0)
		return ret;
	if (ret >= avail_size) {
		ret = cfs_buffer_grow(buffer, ret - avail_size);
		if (ret < 0)
			return -ENOMEM;

		avail_size = buffer->capacity - buffer->pos;
		va_start(args, fmt);
		ret = vsnprintf(buffer->data + buffer->pos, avail_size, fmt,
				args);
		va_end(args);
		BUG_ON(ret < 0 || ret >= avail_size);
	}
	buffer->pos += ret;

	return ret;
}
