#include "cfs_extent.h"

extern struct workqueue_struct *extent_work_queue;

static void extent_writer_work_cb(struct work_struct *work);

struct cfs_extent_writer *cfs_extent_writer_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						loff_t file_offset, u64 ext_id,
						u64 ext_offset, u32 ext_size)
{
	struct cfs_extent_writer *writer;
	int ret;

	BUG_ON(dp == NULL);
	writer = kzalloc(sizeof(*writer), GFP_NOFS);
	if (!writer)
		return ERR_PTR(-ENOMEM);
	ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, &dp->members.base[0],
				&writer->sock);
	if (ret < 0) {
		kfree(writer);
		return ERR_PTR(ret);
	}
	writer->es = es;
	writer->dp = dp;
	writer->file_offset = file_offset;
	writer->ext_id = ext_id;
	writer->ext_offset = ext_offset;
	writer->ext_size = ext_size;
	writer->w_size = ext_size;
	mutex_init(&writer->lock);
	INIT_LIST_HEAD(&writer->requests);
	INIT_WORK(&writer->work, extent_writer_work_cb);
	init_waitqueue_head(&writer->wq);
	atomic_set(&writer->inflight, 0);
	return writer;
}

void cfs_extent_writer_release(struct cfs_extent_writer *writer)
{
	if (!writer)
		return;
	cancel_work_sync(&writer->work);
	cfs_data_partition_release(writer->dp);
	cfs_socket_release(writer->sock, true);
	kfree(writer);
}

int cfs_extent_writer_flush(struct cfs_extent_writer *writer)
{
	struct cfs_extent_stream *es = writer->es;
	struct cfs_meta_client *meta = es->ec->meta;
	struct cfs_data_partition *dp = writer->dp;
	struct cfs_packet_extent_array discard_extents = { 0 };
	struct cfs_packet_extent ext;
	int ret;

	if (!cfs_extent_writer_test_dirty(writer))
		return 0;
	wait_event(writer->wq, atomic_read(&writer->inflight) == 0);
	cfs_packet_extent_init(&ext, writer->file_offset, dp->id,
			       writer->ext_id, 0, writer->ext_size);
	ret = cfs_extent_cache_append(&es->cache, &ext, true, &discard_extents);
	if (unlikely(ret < 0)) {
		cfs_log_err("es(%p) append extent cache error %d\n", es, ret);
		return ret;
	}
	ret = cfs_meta_append_extent(meta, es->ino, &ext, &discard_extents);
	if (ret < 0) {
		cfs_log_err("es(%p) sync extent cache error %d\n", es, ret);
		cfs_packet_extent_array_clear(&discard_extents);
		return ret;
	}
	cfs_extent_cache_remove_discard(&es->cache, &discard_extents);
	cfs_packet_extent_array_clear(&discard_extents);
	cfs_extent_writer_clear_dirty(writer);
	return 0;
}

void cfs_extent_writer_request(struct cfs_extent_writer *writer,
			       struct cfs_packet *packet)
{
	if (!(writer->flags &
	      (EXTENT_WRITER_F_ERROR | EXTENT_WRITER_F_RECOVER))) {
		int ret = cfs_socket_send_packet(writer->sock, packet);
		if (ret < 0)
			writer->flags |= EXTENT_WRITER_F_RECOVER;
	}
	mutex_lock(&writer->lock);
	list_add_tail(&packet->list, &writer->requests);
	mutex_unlock(&writer->lock);
	atomic_inc(&writer->inflight);
	cfs_extent_writer_set_dirty(writer);
	cfs_extent_writer_write_bytes(writer,
				      be32_to_cpu(packet->request.hdr.size));
	queue_work(extent_work_queue, &writer->work);
}

static void extent_writer_work_cb(struct work_struct *work)
{
	struct cfs_extent_writer *writer =
		container_of(work, struct cfs_extent_writer, work);
	struct cfs_extent_stream *es = writer->es;
	struct cfs_extent_writer *recover = writer->recover;
	struct cfs_packet *packet;
	int cnt = 0;
	int ret;

	while (true) {
		mutex_lock(&writer->lock);
		packet = list_first_entry_or_null(&writer->requests,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		mutex_unlock(&writer->lock);
		if (!packet)
			break;

		if (writer->flags & EXTENT_WRITER_F_ERROR) {
			packet->error = -EIO;
			goto handle_packet;
		}

		if (writer->flags & EXTENT_WRITER_F_RECOVER)
			goto recover_packet;

		ret = cfs_socket_recv_packet(writer->sock, packet);
		if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
			writer->flags |= EXTENT_WRITER_F_RECOVER;
			goto recover_packet;
		}
		goto handle_packet;

recover_packet:
		if (!recover) {
			struct cfs_data_partition *dp;
			u64 ext_id;

			mutex_lock(&es->lock_writers);
			if (es->nr_writers >= es->max_writers) {
				mutex_unlock(&es->lock_writers);
				writer->flags |= EXTENT_WRITER_F_ERROR;
				packet->error = -EPERM;
				goto handle_packet;
			}
			mutex_unlock(&es->lock_writers);

			ret = cfs_extent_id_new(es, &dp, &ext_id);
			if (ret < 0) {
				writer->flags |= EXTENT_WRITER_F_ERROR;
				packet->error = ret;
				goto handle_packet;
			}
			recover = cfs_extent_writer_new(
				es, dp,
				be64_to_cpu(packet->request.hdr.kernel_offset),
				ext_id, 0, 0);
			if (!recover) {
				cfs_data_partition_release(dp);
				writer->flags |= EXTENT_WRITER_F_ERROR;
				packet->error = -ENOMEM;
				goto handle_packet;
			}

			mutex_lock(&es->lock_writers);
			list_add_tail(&recover->list, &es->writers);
			es->nr_writers++;
			mutex_unlock(&es->lock_writers);
			writer->recover = recover;
		}

		packet->request.hdr.pid = be64_to_cpu(recover->dp->id);
		packet->request.hdr.ext_id = be64_to_cpu(recover->ext_id);
		packet->request.hdr.ext_offset = cpu_to_be64(
			be64_to_cpu(packet->request.hdr.kernel_offset) -
			recover->file_offset);
		packet->request.hdr.remaining_followers =
			recover->dp->nr_followers;
		cfs_packet_set_request_arg(packet, recover->dp->follower_addrs);
		cfs_packet_set_callback(packet, packet->handle_reply, recover);

		cfs_extent_writer_request(recover, packet);
		continue;

handle_packet:
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
	}
	atomic_sub(cnt, &writer->inflight);
	wake_up(&writer->wq);
}
