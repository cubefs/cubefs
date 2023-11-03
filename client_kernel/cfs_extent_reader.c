#include "cfs_extent.h"

extern struct workqueue_struct *extent_work_queue;

static void extent_reader_work_cb(struct work_struct *work);

struct cfs_extent_reader *cfs_extent_reader_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						u32 host_idx, u64 ext_id)
{
	struct cfs_extent_reader *reader;
	int ret;

	BUG_ON(dp == NULL);
	reader = kzalloc(sizeof(*reader), GFP_NOFS);
	if (!reader)
		return ERR_PTR(-ENOMEM);
	host_idx = host_idx % dp->members.num;
	ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, &dp->members.base[host_idx],
				&reader->sock);
	if (ret < 0) {
		kfree(reader);
		return ERR_PTR(ret);
	}
	reader->es = es;
	reader->dp = dp;
	reader->ext_id = ext_id;
	mutex_init(&reader->lock_requests);
	INIT_LIST_HEAD(&reader->requests);
	INIT_WORK(&reader->work, extent_reader_work_cb);
	init_waitqueue_head(&reader->wq);
	atomic_set(&reader->inflight, 0);
	reader->host_idx = host_idx;
	return reader;
}

void cfs_extent_reader_release(struct cfs_extent_reader *reader)
{
	if (!reader)
		return;
	cancel_work_sync(&reader->work);
	cfs_data_partition_release(reader->dp);
	cfs_socket_release(reader->sock, true);
	kfree(reader);
}

void cfs_extent_reader_flush(struct cfs_extent_reader *reader)
{
	wait_event(reader->wq, atomic_read(&reader->inflight) == 0);
}

void cfs_extent_reader_request(struct cfs_extent_reader *reader,
			       struct cfs_packet *packet)
{
	if (!(reader->flags &
	      (EXTENT_READER_F_ERROR | EXTENT_READER_F_RECOVER))) {
		int ret = cfs_socket_send_packet(reader->sock, packet);
		if (ret == -ENOMEM)
			reader->flags |= EXTENT_WRITER_F_ERROR;
		else if (ret < 0)
			reader->flags |= EXTENT_WRITER_F_RECOVER;
	}
	mutex_lock(&reader->lock_requests);
	list_add_tail(&packet->list, &reader->requests);
	mutex_unlock(&reader->lock_requests);
	atomic_inc(&reader->inflight);
	queue_work(extent_work_queue, &reader->work);
}

static void extent_reader_work_cb(struct work_struct *work)
{
	struct cfs_extent_reader *reader =
		container_of(work, struct cfs_extent_reader, work);
	struct cfs_extent_stream *es = reader->es;
	struct cfs_extent_reader *recover = reader->recover;
	struct cfs_packet *packet;
	int cnt = 0;
	int ret;

	while (true) {
		mutex_lock(&reader->lock_requests);
		packet = list_first_entry_or_null(&reader->requests,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		mutex_unlock(&reader->lock_requests);
		if (!packet)
			break;

		if (reader->flags & EXTENT_READER_F_ERROR) {
			packet->error = -EIO;
			goto handle_packet;
		}

		if (reader->flags & EXTENT_READER_F_RECOVER)
			goto recover_packet;

		ret = cfs_socket_recv_packet(reader->sock, packet);
		if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
			reader->flags |= EXTENT_READER_F_RECOVER;
			goto recover_packet;
		}
		goto handle_packet;

recover_packet:
		if (!recover) {
			mutex_lock(&es->lock_readers);
			if (es->nr_readers >= es->max_readers) {
				mutex_unlock(&es->lock_readers);
				reader->flags |= EXTENT_READER_F_ERROR;
				packet->error = -EPERM;
				goto handle_packet;
			}
			mutex_unlock(&es->lock_readers);

			cfs_data_partition_get(reader->dp);
			recover = cfs_extent_reader_new(es, reader->dp,
							reader->host_idx + 1,
							reader->ext_id);
			if (!recover) {
				cfs_data_partition_put(reader->dp);
				reader->flags |= EXTENT_WRITER_F_ERROR;
				packet->error = -ENOMEM;
				goto handle_packet;
			}

			mutex_lock(&es->lock_readers);
			list_add_tail(&recover->list, &es->readers);
			es->nr_readers++;
			mutex_unlock(&es->lock_readers);
			reader->recover = recover;
		}

		cfs_packet_set_callback(packet, packet->handle_reply, recover);
		msleep(100);
		cfs_extent_reader_request(recover, packet);
		continue;

handle_packet:
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
	}
	atomic_sub(cnt, &reader->inflight);
	wake_up(&reader->wq);
}
