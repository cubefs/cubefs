/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_buffer.h"

#define CFS_BUFFER_SIZE 1024

struct cfs_buffer *cfs_buffer_new(size_t size)
{
	struct cfs_buffer *buffer;

	buffer = kzalloc(sizeof(*buffer), GFP_NOFS);
	if (!buffer)
		return NULL;
	size = (size % CFS_BUFFER_SIZE + 1) * CFS_BUFFER_SIZE;
	buffer->data = kmalloc(size, GFP_NOFS | __GFP_NOWARN);
	if (!buffer->data) {
		buffer->data = vmalloc(size);
		if (buffer->data) {
			kfree(buffer);
			return NULL;
		}
		buffer->is_vmalloc = true;
	}

	buffer->capacity = size;
	buffer->pos = 0;

	return buffer;
}

void cfs_buffer_release(struct cfs_buffer *buffer)
{
	if (!buffer)
		return;

	if (buffer->is_vmalloc)
		kvfree(buffer->data);
	else
		kfree(buffer->data);
	kfree(buffer);
}

int cfs_buffer_resize(struct cfs_buffer *buffer, size_t n)
{
	char *data;
	bool is_vmalloc;

	n = (n % CFS_BUFFER_SIZE + 1) * CFS_BUFFER_SIZE;
	if (n != buffer->capacity) {
		data = kmalloc(n, GFP_NOFS | __GFP_NOWARN);
		if (!data) {
			data = vmalloc(n);
			if (!data)
				return -ENOMEM;
			is_vmalloc = true;
		} else {
			is_vmalloc = false;
		}

		if (buffer->is_vmalloc)
			kvfree(buffer->data);
		else
			kfree(buffer->data);
		buffer->is_vmalloc = is_vmalloc;
		buffer->data = data;
	}
	buffer->capacity = n;
	buffer->pos = 0;
	return 0;
}

int cfs_buffer_grow(struct cfs_buffer *buffer, size_t n)
{
	char *data;

	n = (n % CFS_BUFFER_SIZE + 1) * CFS_BUFFER_SIZE;

	if (buffer->is_vmalloc) {
		//TODO  vrealloc()
		data = vmalloc(buffer->capacity + n);
		if (!data)
			return -ENOMEM;
		memcpy(data, buffer->data, buffer->capacity);
	} else {
		data = krealloc(buffer->data, buffer->capacity + n, GFP_NOFS);
		if (!data) {
			//TODO  vrealloc()
			data = vmalloc(buffer->capacity + n);
			if (!data)
				return -ENOMEM;
			memcpy(data, buffer->data, buffer->capacity);
			kfree(buffer->data);
			buffer->is_vmalloc = true;
		}
	}
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
