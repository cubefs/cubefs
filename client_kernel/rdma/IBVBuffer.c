#include "IBVBuffer.h"
#include "IBVSocket.h"

bool IBVBuffer_init(IBVBuffer *buffer, IBVCommContext *ctx, size_t bufLen)
{
	unsigned count = (bufLen + IBV_FRAGMENT_SIZE - 1) / IBV_FRAGMENT_SIZE;
	unsigned i;

	bufLen = MIN(IBV_FRAGMENT_SIZE, bufLen);

	buffer->buffers = kzalloc(count * sizeof(*buffer->buffers), GFP_KERNEL);
	buffer->lists = kzalloc(count * sizeof(*buffer->lists), GFP_KERNEL);
	if (!buffer->buffers || !buffer->lists)
		goto fail_alloc;

	for (i = 0; i < count; i++) {
#ifndef OFED_UNSAFE_GLOBAL_RKEY
		buffer->lists[i].lkey = ctx->dmaMR->lkey;
#else
		buffer->lists[i].lkey = ctx->pd->local_dma_lkey;
#endif
		buffer->lists[i].length = bufLen;
		buffer->buffers[i] =
			dma_alloc_coherent(ctx->pd->device->dma_device, bufLen,
					   &buffer->lists[i].addr, GFP_KERNEL);
		if (!buffer->buffers[i])
			goto fail_dma;
	}

	buffer->bufferSize = bufLen;
	buffer->listLength = count;
	buffer->bufferCount = count;
	return true;

fail_dma:
	for (i = 0; i < count; i++) {
		if (buffer->buffers[i])
			dma_free_coherent(ctx->pd->device->dma_device,
					  buffer->bufferSize,
					  buffer->buffers[i],
					  buffer->lists[i].addr);
	}

fail_alloc:
	kfree(buffer->buffers);
	kfree(buffer->lists);
	return false;
}

void IBVBuffer_free(IBVBuffer *buffer, IBVCommContext *ctx)
{
	unsigned i;

	for (i = 0; i < buffer->bufferCount; i++) {
		dma_free_coherent(ctx->pd->device->dma_device,
				  buffer->bufferSize, buffer->buffers[i],
				  buffer->lists[i].addr);
	}

	kfree(buffer->buffers);
	kfree(buffer->lists);
}

ssize_t IBVBuffer_fill(IBVBuffer *buffer, struct iov_iter *iter)
{
	ssize_t total = 0;
	unsigned i;

	for (i = 0; i < buffer->bufferCount && iter->count > 0; i++) {
		size_t fragment =
			MIN(MIN(iter->count, buffer->bufferSize), 0xFFFFFFFF);

		if (copy_from_iter(buffer->buffers[i], fragment, iter) !=
		    fragment)
			return -EFAULT;

		buffer->lists[i].length = fragment;
		buffer->listLength = i + 1;

		total += fragment;
	}

	return total;
}
