/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"
#include <linux/kthread.h>

extern struct workqueue_struct *extent_work_queue;
extern struct workqueue_struct *cfs_flush_workqueue;

/*
 * 异步 flush 背压上限:每个 pending writer 持 1 socket + 1 recv kthread，
 * numjobs=32 高速 buffered 写时 writer 切换(每 128MB)远快于 extent commit
 * (meta RPC)，无背压则 pending writer 无限堆积、socket/kthread/内存耗尽拖垮
 * 节点。pending 达此上限时 submit 在 flusher 上下文短暂等一个 flush 腾位
 * (flush work 在独立 cfs_flush_workqueue 完成、不依赖本上下文，不会回到死锁)。
 */
#define CFS_MAX_PENDING_FLUSH 16

static void extent_writer_tx_work_cb(struct work_struct *work);
static void extent_writer_flush_work_cb(struct work_struct *work);
/*
 * rx 改为 per-writer 专用 recv kthread（替代原共用 extent_work_queue 的
 * rx_work）。原 rx_work 同步阻塞 cfs_socket_recv_packet，高并发下大量
 * rx_work 占满 wq worker，而 cgwb flusher 回写时 cfs_extent_writer_flush
 * 同步 wait_event(rx_inflight==0) 等这些 work 完成 → wq 饱和则 flusher
 * 永久卡、dirty 不回写、节点被拖垮。专用 recv 线程让 recv 不占 wq、
 * 各 socket 互不饿死。
 */
static int extent_writer_rx_thread_fn(void *data);

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
				es->ec->log, &writer->sock);
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
	spin_lock_init(&writer->lock_tx);
	spin_lock_init(&writer->lock_rx);
	INIT_LIST_HEAD(&writer->tx_packets);
	INIT_LIST_HEAD(&writer->rx_packets);
	INIT_WORK(&writer->tx_work, extent_writer_tx_work_cb);
	INIT_WORK(&writer->flush_work, extent_writer_flush_work_cb);
	init_waitqueue_head(&writer->tx_wq);
	init_waitqueue_head(&writer->rx_wq);
	init_waitqueue_head(&writer->rx_pending_wq);
	atomic_set(&writer->tx_inflight, 0);
	atomic_set(&writer->rx_inflight, 0);
	writer->rx_thread = kthread_run(extent_writer_rx_thread_fn, writer,
					"cfs-wrx-%llu", ext_id);
	if (IS_ERR(writer->rx_thread)) {
		cfs_socket_release(writer->sock, true);
		kfree(writer);
		return NULL;
	}
	return writer;
}

void cfs_extent_writer_release(struct cfs_extent_writer *writer)
{
	if (!writer)
		return;
	/* 先停 tx（不再产生新 rx_packets），再停 recv 线程。 */
	cancel_work_sync(&writer->tx_work);
	if (writer->rx_thread)
		kthread_stop(writer->rx_thread);
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
	wait_event(writer->tx_wq, atomic_read(&writer->tx_inflight) == 0);
	wait_event(writer->rx_wq, atomic_read(&writer->rx_inflight) == 0);
	cfs_packet_extent_init(&ext, writer->file_offset, dp->id,
			       writer->ext_id, 0, writer->ext_size);
	ret = cfs_extent_cache_append(&es->cache, &ext, true, &discard_extents);
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) append extent cache error %d\n",
			      es->ino, ret);
		return ret;
	}
	ret = cfs_meta_append_extent(meta, es->ino, &ext, &discard_extents);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) sync extent cache error %d\n", es->ino,
			      ret);
		cfs_packet_extent_array_clear(&discard_extents);
		return ret;
	}
	cfs_extent_cache_remove_discard(&es->cache, &discard_extents);
	cfs_packet_extent_array_clear(&discard_extents);
	cfs_extent_writer_clear_dirty(writer);
	return 0;
}

/*
 * 异步 flush 回调：在 cfs_flush_workqueue 上下文执行原同步 flush（等 tx/rx
 * 网络往返清零 + meta commit），完成后从 pending_flush 摘除并 release writer。
 * 不在 cgwb flusher 上下文，flusher 提交后立即返回、不卡 → 根治 writeback
 * 死锁。一致性约束全保留在 cfs_extent_writer_flush 内（ext_size 在 rx 清零后
 * 读、cache_append(sync=true)+meta_commit 连续）。meta commit 失败的 page
 * error 已在 reply_cb 处理。
 * 注：cb 内 cfs_extent_writer_release 会 kfree(writer)（含本 flush_work）——
 * 内核允许 work 回调内释放自身 work（process_one_work 在调 cb 前已 clear
 * pending、cb 返回后不再访问 work data）。
 */
static void extent_writer_flush_work_cb(struct work_struct *work)
{
	struct cfs_extent_writer *writer =
		container_of(work, struct cfs_extent_writer, flush_work);
	struct cfs_extent_stream *es = writer->es;

	cfs_extent_writer_flush(writer);
	spin_lock(&es->lock_pending);
	list_del(&writer->list);
	spin_unlock(&es->lock_pending);
	cfs_extent_writer_release(writer);
	atomic_dec(&es->nr_pending_flush);
	wake_up(&es->flush_wq);
}

/*
 * 把需要 flush 的旧 writer 交后台异步 flush。由 extent_stream_get_writer 在
 * cgwb flusher 上下文调用：writer 此前已从 es->writers 摘出，这里挂到
 * es->pending_flush 并入队 cfs_flush_workqueue，flusher 不再同步等网络往返。
 * fsync/close/stream_release 经 cfs_extent_stream_flush 等 nr_pending_flush==0。
 */
void extent_writer_submit_async_flush(struct cfs_extent_writer *writer)
{
	struct cfs_extent_stream *es = writer->es;

	/*
	 * 背压：pending writer 达上限时，在此等一个 flush 完成腾位再提交。
	 * flush work 在独立 cfs_flush_workqueue 完成（atomic_dec+wake flush_wq），
	 * 不依赖本上下文，故不会回到 flusher 同步等网络往返的死锁；只限制
	 * pending writer 数（=socket/recv kthread 上限），避免资源耗尽拖垮节点。
	 */
	wait_event(es->flush_wq,
		   atomic_read(&es->nr_pending_flush) < CFS_MAX_PENDING_FLUSH);
	spin_lock(&es->lock_pending);
	list_add_tail(&writer->list, &es->pending_flush);
	spin_unlock(&es->lock_pending);
	atomic_inc(&es->nr_pending_flush);
	queue_work(cfs_flush_workqueue, &writer->flush_work);
}

void cfs_extent_writer_request(struct cfs_extent_writer *writer,
			       struct cfs_packet *packet)
{
	cfs_extent_writer_set_dirty(writer);
	cfs_extent_writer_write_bytes(writer,
				      be32_to_cpu(packet->request.hdr.size));
	spin_lock(&writer->lock_tx);
	list_add_tail(&packet->list, &writer->tx_packets);
	spin_unlock(&writer->lock_tx);
	atomic_inc(&writer->tx_inflight);
	queue_work(extent_work_queue, &writer->tx_work);
}

static void extent_writer_tx_work_cb(struct work_struct *work)
{
	struct cfs_extent_writer *writer =
		container_of(work, struct cfs_extent_writer, tx_work);
	struct cfs_packet *packet;
	int cnt = 0;

	while (true) {
		spin_lock(&writer->lock_tx);
		packet = list_first_entry_or_null(&writer->tx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&writer->lock_tx);
		if (!packet)
			break;

		if (!packet->request.hdr.crc)
			packet->request.hdr.crc =
				cpu_to_be32(cfs_page_frags_crc32(
					packet->request.data.write.frags,
					packet->request.data.write.nr));

		if (!(writer->flags &
		      (EXTENT_WRITER_F_ERROR | EXTENT_WRITER_F_RECOVER))) {
			int ret = cfs_socket_send_packet(writer->sock, packet);
			if (ret < 0)
				writer->flags |= EXTENT_WRITER_F_RECOVER;
		}
		spin_lock(&writer->lock_rx);
		list_add_tail(&packet->list, &writer->rx_packets);
		spin_unlock(&writer->lock_rx);
		atomic_inc(&writer->rx_inflight);
		wake_up(&writer->rx_pending_wq);
	}
	atomic_sub(cnt, &writer->tx_inflight);
	wake_up(&writer->tx_wq);
}

static int extent_writer_rx_thread_fn(void *data)
{
	struct cfs_extent_writer *writer = data;
	struct cfs_extent_stream *es = writer->es;
	struct cfs_extent_writer *recover;
	struct cfs_packet *packet;
	int cnt;
	int ret;

	while (!kthread_should_stop()) {
	wait_event(writer->rx_pending_wq,
		   !list_empty_careful(&writer->rx_packets) ||
			   kthread_should_stop());
	recover = writer->recover;
	cnt = 0;
	while (true) {
		spin_lock(&writer->lock_rx);
		packet = list_first_entry_or_null(&writer->rx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&writer->lock_rx);
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
	atomic_sub(cnt, &writer->rx_inflight);
	wake_up(&writer->rx_wq);
	}
	return 0;
}
