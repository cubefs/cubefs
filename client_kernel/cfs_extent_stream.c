/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"

#define EXTENT_RECV_TIMEOUT_MS 5000u

#define EXTENT_BLOCK_COUNT 1024UL
#define EXTENT_BLOCK_SIZE 131072UL
#define EXTENT_SIZE (EXTENT_BLOCK_COUNT * EXTENT_BLOCK_SIZE)
#define EXTENT_TINY_MAX_ID 64
#define EXTENT_TINY_SIZE 1048576

#define EXTENT_REQ_RETRY_MAX_COUNT 64
#define EXTENT_WRITER_MAX_COUNT EXTENT_REQ_RETRY_MAX_COUNT
#define EXTENT_READER_MAX_COUNT EXTENT_REQ_RETRY_MAX_COUNT

static enum extent_write_type extent_io_type(struct cfs_extent_io_info *io_info)
{
	if (!io_info->hole)
		return EXTENT_WRITE_TYPE_RANDOM;
	if (io_info->offset > 0 ||
	    io_info->offset + io_info->size > EXTENT_TINY_SIZE)
		return EXTENT_WRITE_TYPE_NORMAL;
	return EXTENT_WRITE_TYPE_TINY;
}

static int do_extent_request_rdma(struct cfs_extent_stream *es,
				  struct sockaddr_storage *host,
				  struct cfs_packet *packet)
{
	struct cfs_socket *sock;
	int err;

	err = cfs_rdma_create(host, es->ec->log, &sock, es->rdma_port);
	if (err < 0) {
		cfs_log_error(es->ec->log, "rdma(%s) create error %d\n",
			      cfs_pr_addr(host), err);
		return err;
	}

	err = cfs_rdma_send_packet(sock, packet);
	if (err < 0) {
		cfs_log_error(es->ec->log, "rdma(%s) send packet error %d\n",
			      cfs_pr_addr(host), err);
		goto out;
	}

	err = cfs_rdma_recv_packet(sock, packet);
	if (err) {
		cfs_log_error(es->ec->log, "rdma(%s) recv packet error %d\n",
			      cfs_pr_addr(host), err);
		goto out;
	}

out:
	if (err || packet->reply.hdr.result_code != CFS_STATUS_OK)
		cfs_rdma_release(sock, true);
	else
		cfs_rdma_release(sock, false);

	return err;
}

static int do_extent_request(struct cfs_extent_stream *es,
			     struct sockaddr_storage *host,
			     struct cfs_packet *packet)
{
	struct cfs_socket *sock;
	int err;

	err = cfs_socket_create(CFS_SOCK_TYPE_TCP, host, es->ec->log, &sock);
	if (err) {
		cfs_log_error(es->ec->log, "socket(%s) create error %d\n",
			      cfs_pr_addr(host), err);
		return err;
	}

	err = cfs_socket_set_recv_timeout(sock, EXTENT_RECV_TIMEOUT_MS);
	if (err) {
		cfs_log_error(es->ec->log,
			      "socket(%s) set recv timeout error %d\n",
			      cfs_pr_addr(host), err);
		goto out;
	}

	err = cfs_socket_send_packet(sock, packet);
	if (err < 0) {
		cfs_log_error(es->ec->log, "socket(%s) send packet error %d\n",
			      cfs_pr_addr(host), err);
		goto out;
	}

	err = cfs_socket_recv_packet(sock, packet);
	if (err) {
		cfs_log_error(es->ec->log, "socket(%s) recv packet error %d\n",
			      cfs_pr_addr(host), err);
		goto out;
	}

out:
	if (err || packet->reply.hdr.result_code != CFS_STATUS_OK)
		cfs_socket_release(sock, true);
	else
		cfs_socket_release(sock, false);

	return err;
}

/**
 * Try to send request to each member of the dp, until request success.
 * @param host_id [in] the request start from host_id
 * @return negative number if all request failed, return the member id
 * if request success
 */
static int do_extent_request_retry(struct cfs_extent_stream *es,
				   struct cfs_data_partition *dp,
				   struct cfs_packet *packet, u32 host_id)
{
	int again_cnt = 200;
	int host_retry_cnt = EXTENT_REQ_RETRY_MAX_COUNT;
	struct sockaddr_storage *host;
	int ret = -1;

retry:
	if (host_retry_cnt == 0)
		return ret;
	host = &dp->members.base[host_id % dp->members.num];
	// Only support write by RDMA now. The read is not supported yet.
	if (es->enable_rdma && packet->request.hdr.opcode == CFS_OP_STREAM_WRITE) {
		ret = do_extent_request_rdma(es, host, packet);
	} else {
		ret = do_extent_request(es, host, packet);
	}

	if (ret < 0) {
		/* try other host */
		host_id++;
		host_retry_cnt--;
		msleep(100);
		goto retry;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	switch (packet->reply.hdr.result_code) {
	case CFS_STATUS_OK:
		break;
	case CFS_STATUS_AGAIN:
		/* try the host */
		again_cnt--;
		if (again_cnt > 0) {
			msleep(100);
			goto retry;
		}
#ifdef KERNEL_SUPPORT_SWITCH_FALLTHROUGH
		fallthrough;
#endif
		/* else try other host */
	default:
		/* try other host */
		host_id++;
		host_retry_cnt--;
		msleep(100);
		goto retry;
	}
	return (int)(host_id % dp->members.num);
}

int cfs_extent_id_new(struct cfs_extent_stream *es,
		      struct cfs_data_partition **dpp, u64 *ext_id)
{
	u8 op = CFS_OP_EXTENT_CREATE;
	u32 retry_cnt = cfs_extent_get_partition_count(es->ec);
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	int ret = -1;

retry:
	if (retry_cnt == 0)
		return ret;

	dp = cfs_extent_select_partition(es->ec);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) cannot select data partition\n",
			      es->ino);
		return -ENOENT;
	}

	packet = cfs_extent_packet_new(op, CFS_EXTENT_TYPE_NORMAL,
				       dp->nr_followers, dp->id, 0, 0, 0);
	if (!packet) {
		cfs_data_partition_release(dp);
		return -ENOMEM;
	}
	cfs_packet_set_request_arg(packet, dp->follower_addrs);
	packet->request.data.ino = cpu_to_be64(es->ino);
	packet->request.hdr.size = cpu_to_be32(sizeof(es->ino));

	ret = do_extent_request(es, &dp->members.base[0], packet);
	if (ret < 0) {
		cfs_log_error(es->ec->log, "ino(%llu) create extent error %d\n",
			      es->ino, ret);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) create extent reply error code 0x%x\n",
			      es->ino, packet->reply.hdr.result_code);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}

	*dpp = dp;
	*ext_id = be64_to_cpu(packet->reply.hdr.ext_id);
	cfs_packet_release(packet);
	return 0;
}

/**
 * @param offset [in] file offset
 * @param size [in] write size, must less than EXTENT_SIZE
 */
static struct cfs_extent_writer *
extent_stream_get_writer(struct cfs_extent_stream *es, loff_t offset,
			 size_t size)
{
	struct cfs_extent_writer *writer = NULL;
	struct cfs_packet_extent extent;

	while (true) {
		mutex_lock(&es->lock_writers);
		writer = list_first_entry_or_null(
			&es->writers, struct cfs_extent_writer, list);
		if (!writer) {
			mutex_unlock(&es->lock_writers);
			break;
		}

		if (writer->flags &
		    (EXTENT_WRITER_F_RECOVER | EXTENT_WRITER_F_ERROR)) {
			list_del(&writer->list);
			es->nr_writers--;
			mutex_unlock(&es->lock_writers);
		} else if ((writer->file_offset + writer->w_size != offset) ||
			   (writer->w_size + size > EXTENT_SIZE)) {
			list_del(&writer->list);
			es->nr_writers--;
			mutex_unlock(&es->lock_writers);
		} else {
			mutex_unlock(&es->lock_writers);
			return writer;
		}
		cfs_extent_writer_flush(writer);
		cfs_extent_writer_release(writer);
	}

	if (cfs_extent_cache_get_end(&es->cache, offset, &extent) &&
	    (extent.ext_id > EXTENT_TINY_MAX_ID &&
	     extent.size + size <= EXTENT_SIZE)) {
		struct cfs_data_partition *dp =
			cfs_extent_get_partition(es->ec, extent.pid);
		if (!dp) {
			cfs_log_error(
				es->ec->log,
				"ino(%llu) not found data partition(%llu)\n",
				es->ino, extent.pid);
			return ERR_PTR(-ENOENT);
		}
		writer = cfs_extent_writer_new(es, dp, extent.file_offset,
					       extent.ext_id, extent.ext_offset,
					       extent.size);
		if (IS_ERR(writer)) {
			cfs_data_partition_release(dp);
			return ERR_PTR(-ENOMEM);
		}
	} else {
		struct cfs_data_partition *dp;
		u64 ext_id;
		int ret;

		ret = cfs_extent_id_new(es, &dp, &ext_id);
		if (ret < 0)
			return ERR_PTR(ret);
		writer = cfs_extent_writer_new(es, dp, offset, ext_id, 0, 0);
		if (IS_ERR(writer)) {
			cfs_data_partition_release(dp);
			return ERR_PTR(-ENOMEM);
		}
	}
	mutex_lock(&es->lock_writers);
	list_add_tail(&writer->list, &es->writers);
	es->nr_writers++;
	mutex_unlock(&es->lock_writers);
	return writer;
}

static int extent_write_pages_random(struct cfs_extent_stream *es,
				     struct cfs_extent_io_info *io_info,
				     struct cfs_page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct cfs_page_frag *frag;
	size_t w_len;
	size_t send_bytes = 0;
	size_t i;
	int ret = 0;

	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) cannot get data partition(%llu)\n",
			      es->ino, io_info->ext.pid);
		ret = -ENOENT;
		return ret;
	}
	while (send_bytes < io_info->size) {
		w_len = min(io_info->size - send_bytes, EXTENT_BLOCK_SIZE);
		packet = cfs_extent_packet_new(
			CFS_OP_STREAM_RANDOM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			dp->nr_followers, dp->id, io_info->ext.ext_id,
			io_info->offset - io_info->ext.file_offset +
				io_info->ext.ext_offset + send_bytes,
			io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}
		cfs_packet_set_write_data(packet, iter, &w_len);
		packet->request.hdr.crc = cpu_to_be32(cfs_page_frags_crc32(packet->request.data.write.frags, packet->request.data.write.nr));

		ret = do_extent_request_retry(es, dp, packet, dp->leader_idx);
		if (ret < 0) {
			cfs_log_error(
				es->ec->log,
				"ino(%llu) send packet(%llu) to dp(%llu) error %d\n",
				es->ino,
				be64_to_cpu(packet->request.hdr.req_id), dp->id,
				ret);
			cfs_packet_release(packet);
			goto out;
		}
		cfs_data_partition_set_leader(dp, ret);

		for (i = 0; i < packet->request.data.write.nr; i++) {
			frag = &packet->request.data.write.frags[i];
			if (cfs_page_io_account(frag->page, frag->size)) {
				end_page_writeback(frag->page->page);
				unlock_page(frag->page->page);
				cfs_page_release(frag->page);
			}
		}
		cfs_packet_release(packet);
		cfs_page_iter_advance(iter, w_len);
		send_bytes += w_len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

static int extent_write_pages_tiny(struct cfs_extent_stream *es,
				   struct cfs_extent_io_info *io_info,
				   struct cfs_page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	struct cfs_page_frag *frag;
	size_t i;
	int ret = -1;
	u32 retry_cnt = cfs_extent_get_partition_count(es->ec);

	BUG_ON(iter->nr > CFS_PAGE_VEC_NUM);

retry:
	if (retry_cnt == 0)
		return ret;

	dp = cfs_extent_select_partition(es->ec);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) cannot select data partition\n",
			      es->ino);
		ret = -ENOENT;
		return ret;
	}

	packet = cfs_extent_packet_new(CFS_OP_STREAM_WRITE,
				       CFS_EXTENT_TYPE_TINY, dp->nr_followers,
				       dp->id, 0, 0, 0);
	if (!packet) {
		cfs_data_partition_release(dp);
		ret = -ENOMEM;
		return ret;
	}

	if (es->enable_rdma) {
		cfs_packet_set_request_arg(packet, dp->rdma_follower_addrs);
	} else {
		cfs_packet_set_request_arg(packet, dp->follower_addrs);
	}
	cfs_packet_set_write_data(packet, iter, &io_info->size);
	packet->request.hdr.crc = cpu_to_be32(cfs_page_frags_crc32(packet->request.data.write.frags, packet->request.data.write.nr));

	if (es->enable_rdma) {
		ret = do_extent_request_rdma(es, &dp->members.base[0], packet);
	} else {
		ret = do_extent_request(es, &dp->members.base[0], packet);
	}

	if (ret < 0) {
		if (retry_cnt == 1)
			cfs_log_error(es->ec->log,
				      "ino(%llu) write extent error %d\n",
				      es->ino, ret);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	if (ret < 0) {
		if (retry_cnt == 1)
			cfs_log_error(
				es->ec->log,
				"ino(%llu) write extent reply error code 0x%x\n",
				es->ino, packet->reply.hdr.result_code);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}

	cfs_data_partition_release(dp);

	cfs_packet_extent_init(&extent, 0, be64_to_cpu(packet->reply.hdr.pid),
			       be64_to_cpu(packet->reply.hdr.ext_id),
			       be64_to_cpu(packet->reply.hdr.ext_offset),
			       io_info->size);
	ret = cfs_extent_cache_append(&es->cache, &extent, false, NULL);
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) append extent cache error %d\n",
			      es->ino, ret);
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_meta_append_extent(es->ec->meta, es->ino, &extent, NULL);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) sync extent cache error %d\n", es->ino,
			      ret);
		cfs_packet_release(packet);
		return ret;
	}

	for (i = 0; i < packet->request.data.write.nr; i++) {
		frag = &packet->request.data.write.frags[i];
		if (cfs_page_io_account(frag->page, frag->size)) {
			end_page_writeback(frag->page->page);
			unlock_page(frag->page->page);
			cfs_page_release(frag->page);
		}
	}
	cfs_page_iter_advance(iter, io_info->size);
	cfs_packet_release(packet);
	return 0;
}

static void extent_write_pages_reply_cb(struct cfs_packet *packet)
{
	struct cfs_extent_writer *writer = packet->private;
	struct cfs_log *log = writer->es->ec->log;
	struct cfs_page_frag *frag;
	size_t i;
	int err;

	if (packet->error) {
		err = packet->error;
		cfs_log_error(log, "ino(%llu) io error %d\n", writer->es->ino,
			      err);
	} else {
		err = -cfs_parse_status(packet->reply.hdr.result_code);
		if (err)
			cfs_log_error(log, "ino(%llu) reply error %d\n",
				      writer->es->ino, err);
	}
	if (!err)
		cfs_extent_writer_ack_bytes(
			writer, be32_to_cpu(packet->request.hdr.size));

	for (i = 0; i < packet->request.data.write.nr; i++) {
		frag = &packet->request.data.write.frags[i];
		if (err) {
			SetPageError(frag->page->page);
			if (frag->page->page->mapping &&
			    !PageAnon(frag->page->page))
				mapping_set_error(frag->page->page->mapping,
						  err);
		}
		if (cfs_page_io_account(frag->page, frag->size)) {
			end_page_writeback(frag->page->page);
			unlock_page(frag->page->page);
			cfs_page_release(frag->page);
		}
	}
}

static int extent_write_pages_normal(struct cfs_extent_stream *es,
				     struct cfs_extent_io_info *io_info,
				     struct cfs_page_iter *iter)
{
	struct cfs_extent_writer *writer;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	loff_t offset = io_info->offset;
	size_t send_bytes = 0, total_bytes = io_info->size;
	size_t w_len;
	int ret;

	while (send_bytes < total_bytes) {
		w_len = min(total_bytes - send_bytes, EXTENT_BLOCK_SIZE);
		writer = extent_stream_get_writer(es, offset, w_len);
		if (IS_ERR(writer))
			return PTR_ERR(writer);

		packet = cfs_extent_packet_new(
			CFS_OP_STREAM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			writer->dp->nr_followers, writer->dp->id,
			writer->ext_id, offset - writer->file_offset, offset);
		if (!packet) {
			cfs_log_error(es->ec->log, "ino(%llu) oom\n", es->ino);
			ret = -ENOMEM;
			return ret;
		}
		cfs_packet_set_callback(packet, extent_write_pages_reply_cb,
					writer);
		if (es->enable_rdma) {
			cfs_packet_set_request_arg(packet, writer->dp->rdma_follower_addrs);
		} else {
			cfs_packet_set_request_arg(packet, writer->dp->follower_addrs);
		}
		cfs_packet_set_write_data(packet, iter, &w_len);
		packet->request.hdr.crc = cpu_to_be32(cfs_page_frags_crc32(packet->request.data.write.frags, packet->request.data.write.nr));

		cfs_packet_extent_init(&extent, writer->file_offset, 0, 0, 0,
				       writer->w_size + w_len);
		ret = cfs_extent_cache_append(&es->cache, &extent, false, NULL);
		if (unlikely(ret < 0)) {
			cfs_log_error(es->ec->log, "ino(%llu) oom\n", es->ino);
			cfs_packet_release(packet);
			return ret;
		}
		cfs_extent_writer_request(writer, packet);

		cfs_page_iter_advance(iter, w_len);
		send_bytes += w_len;
		offset += w_len;
	}
	return 0;
}

int cfs_extent_write_pages(struct cfs_extent_stream *es, struct page **pages,
			   size_t nr_pages, loff_t file_offset,
			   size_t first_page_offset, size_t end_page_size)
{
	struct cfs_page **cpages;
	struct cfs_page_iter iter;
	LIST_HEAD(io_info_list);
	struct cfs_extent_io_info *io_info;
	size_t i;
	int ret;

	BUG_ON(nr_pages == 0);

#ifdef DEBUG
	cfs_pr_debug(
		"ino(%llu) nr_pages=%lu, file_offset=%llu, first_page_offset=%lu, end_page_size=%lu\n",
		es->ino, nr_pages, file_offset, first_page_offset,
		end_page_size);
#endif
	cpages = kvmalloc(sizeof(*cpages) * nr_pages, GFP_KERNEL);
	if (!cpages) {
		for (i = 0; i < nr_pages; i++) {
			SetPageError(pages[i]);
			if (pages[i]->mapping && !PageAnon(pages[i]))
				mapping_set_error(pages[i]->mapping, -ENOMEM);
			end_page_writeback(pages[i]);
			unlock_page(pages[i]);
		}
		return -ENOMEM;
	}
	for (i = 0; i < nr_pages; i++) {
		cpages[i] = cfs_page_new(pages[i]);
		if (!cpages[i]) {
			while (i-- > 0) {
				SetPageError(pages[i]);
				if (pages[i]->mapping && !PageAnon(pages[i]))
					mapping_set_error(pages[i]->mapping,
							  -ENOMEM);
				end_page_writeback(pages[i]);
				unlock_page(pages[i]);
				cfs_page_release(cpages[i]);
			}
			kvfree(cpages);
			return -ENOMEM;
		}
	}
	if (nr_pages == 1) {
		cfs_page_io_set(cpages[0], end_page_size - first_page_offset);
	} else {
		cfs_page_io_set(cpages[0], PAGE_SIZE - first_page_offset);
		for (i = 1; i < nr_pages - 1; i++)
			cfs_page_io_set(cpages[i], PAGE_SIZE);
		cfs_page_io_set(cpages[i], end_page_size);
	}

	cfs_page_iter_init(&iter, cpages, nr_pages, first_page_offset,
			   end_page_size);

	ret = cfs_extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) extent cache refresh error %d\n",
			      es->ino, ret);
		goto err_page;
	}
	mutex_lock(&es->lock_io);
	ret = cfs_prepare_extent_io_list(&es->cache, file_offset,
					 cfs_page_iter_count(&iter),
					 &io_info_list);
	if (ret < 0) {
		mutex_unlock(&es->lock_io);
		cfs_log_error(
			es->ec->log,
			"ino(%llu) prepare extent write request error %d\n",
			es->ino, ret);
		goto err_page;
	}

	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list,
					   struct cfs_extent_io_info, list);
		switch (extent_io_type(io_info)) {
		case EXTENT_WRITE_TYPE_RANDOM:
			ret = extent_write_pages_random(es, io_info, &iter);
			break;
		case EXTENT_WRITE_TYPE_TINY:
			ret = extent_write_pages_tiny(es, io_info, &iter);
			break;
		case EXTENT_WRITE_TYPE_NORMAL:
			ret = extent_write_pages_normal(es, io_info, &iter);
			break;
		}
		list_del(&io_info->list);
		cfs_extent_io_info_release(io_info);
		if (ret < 0) {
			mutex_unlock(&es->lock_io);
			cfs_log_error(es->ec->log,
				      "ino(%llu) write page error %d\n",
				      es->ino, ret);
			goto err_page;
		}
	}
	mutex_unlock(&es->lock_io);
	return 0;

err_page:
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list,
					   struct cfs_extent_io_info, list);
		list_del(&io_info->list);
		cfs_extent_io_info_release(io_info);
	}
	if (iter.nr > 0) {
		struct cfs_page *cpage = iter.pages[0];
		size_t first_page_size;

		BUG_ON(ret == 0);
		first_page_size =
			iter.nr == 1 ?
				iter.end_page_size - iter.first_page_offset :
				PAGE_SIZE - iter.first_page_offset;
		SetPageError(cpage->page);
		if (cpage->page->mapping && !PageAnon(cpage->page))
			mapping_set_error(cpage->page->mapping, ret);
		if (cfs_page_io_account(cpage, first_page_size)) {
			end_page_writeback(cpage->page);
			unlock_page(cpage->page);
			cfs_page_release(cpage);
		}
		for (i = 1; i < iter.nr; i++) {
			cpage = iter.pages[i];
			SetPageError(cpage->page);
			if (cpage->page->mapping && !PageAnon(cpage->page))
				mapping_set_error(cpage->page->mapping, ret);
			end_page_writeback(cpage->page);
			unlock_page(cpage->page);
			cfs_page_release(cpage);
		}
	}
	kvfree(cpages);
	return ret;
}

static struct cfs_extent_reader *
extent_stream_get_reader(struct cfs_extent_stream *es,
			 struct cfs_packet_extent *ext)
{
	struct cfs_extent_reader *reader = NULL;
	struct cfs_data_partition *dp;

	while (true) {
		mutex_lock(&es->lock_readers);
		reader = list_first_entry_or_null(
			&es->readers, struct cfs_extent_reader, list);
		if (!reader) {
			mutex_unlock(&es->lock_readers);
			break;
		}

		if (reader->flags &
		    (EXTENT_WRITER_F_RECOVER | EXTENT_WRITER_F_ERROR)) {
			list_del(&reader->list);
			es->nr_readers--;
			mutex_unlock(&es->lock_readers);
		} else if (reader->dp->id != ext->pid ||
			   reader->ext_id != ext->ext_id) {
			list_del(&reader->list);
			es->nr_readers--;
			mutex_unlock(&es->lock_readers);
		} else {
			mutex_unlock(&es->lock_readers);
			return reader;
		}
		cfs_extent_reader_flush(reader);
		cfs_extent_reader_release(reader);
	}

	dp = cfs_extent_get_partition(es->ec, ext->pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) not found data partition(%llu)\n",
			      es->ino, ext->pid);
		return ERR_PTR(-ENOENT);
	}

	reader = cfs_extent_reader_new(es, dp, dp->leader_idx, ext->ext_id);
	if (!reader) {
		cfs_data_partition_put(dp);
		return ERR_PTR(-ENOMEM);
	}

	mutex_lock(&es->lock_readers);
	list_add_tail(&reader->list, &es->readers);
	es->nr_readers++;
	mutex_unlock(&es->lock_readers);
	return reader;
}

static void extent_read_pages_reply_cb(struct cfs_packet *packet)
{
	struct cfs_extent_reader *reader = packet->private;
	struct cfs_extent_stream *es = reader->es;
	struct cfs_log *log = es->ec->log;
	struct cfs_page_frag *frag;
	size_t i;
	int err;

	if (packet->error)
		err = packet->error;
	else
		err = -cfs_parse_status(packet->reply.hdr.result_code);
	if (err)
		cfs_log_error(log, "ino(%llu) reply error %d\n", es->ino, err);

	for (i = 0; i < packet->reply.data.read.nr; i++) {
		frag = &packet->reply.data.read.frags[i];
		if (err)
			SetPageError(frag->page->page);
		if (cfs_page_io_account(frag->page, frag->size)) {
			if (err)
				ClearPageUptodate(frag->page->page);
			else
				SetPageUptodate(frag->page->page);
			unlock_page(frag->page->page);
			cfs_page_release(frag->page);
		}
	}
}

static int extent_read_pages_async(struct cfs_extent_stream *es,
				   struct cfs_extent_io_info *io_info,
				   struct cfs_page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_extent_reader *reader;
	struct cfs_packet *packet;
	size_t len;
	size_t read_bytes = 0, total_bytes = io_info->size;
	size_t read_offset = io_info->offset - io_info->ext.file_offset +
			     io_info->ext.ext_offset;
	int ret = 0;

#ifdef DEBUG
	cfs_pr_debug("ino(%llu) offset=%lld, size=%zu, pid=%llu, "
		     "ext_id=%llu, ext_offset=%llu, ext_size=%u\n",
		     es->ino, io_info->offset, io_info->size, io_info->ext.pid,
		     io_info->ext.ext_id, io_info->ext.ext_offset,
		     io_info->ext.size);
#endif
	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) not found data partition(%llu)\n",
			      es->ino, io_info->ext.pid);
		return -ENOENT;
	}
	while (read_bytes < total_bytes) {
		reader = extent_stream_get_reader(es, &io_info->ext);
		if (IS_ERR(reader)) {
			ret = PTR_ERR(reader);
			goto out;
		}

		len = min(total_bytes - read_bytes, EXTENT_BLOCK_SIZE);
		packet = cfs_extent_packet_new(CFS_OP_STREAM_READ,
					       CFS_EXTENT_TYPE_NORMAL, 0,
					       dp->id, io_info->ext.ext_id,
					       read_offset + read_bytes,
					       io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}
		cfs_packet_set_callback(packet, extent_read_pages_reply_cb,
					reader);
		cfs_packet_set_read_data(packet, iter, &len);

		cfs_extent_reader_request(reader, packet);

		cfs_page_iter_advance(iter, len);
		read_bytes += len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

static int extent_read_pages_sync(struct cfs_extent_stream *es,
				  struct cfs_extent_io_info *io_info,
				  struct cfs_page_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t len;
	size_t read_bytes = 0, total_bytes = io_info->size;
	size_t read_offset = io_info->offset - io_info->ext.file_offset +
			     io_info->ext.ext_offset;
	int ret = 0;
	int i;

#ifdef DEBUG
	cfs_pr_debug("ino(%llu) offset=%lld, size=%zu, pid=%llu, "
		     "ext_id=%llu, ext_offset=%llu, ext_size=%u\n",
		     es->ino, io_info->offset, io_info->size, io_info->ext.pid,
		     io_info->ext.ext_id, io_info->ext.ext_offset,
		     io_info->ext.size);
#endif
	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) not found data partition(%llu)\n",
			      es->ino, io_info->ext.pid);
		return -ENOENT;
	}
	while (read_bytes < total_bytes) {
		len = min(total_bytes - read_bytes, EXTENT_BLOCK_SIZE);
		packet = cfs_extent_packet_new(CFS_OP_STREAM_READ,
					       CFS_EXTENT_TYPE_NORMAL, 0,
					       dp->id, io_info->ext.ext_id,
					       read_offset + read_bytes,
					       io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}
		cfs_packet_set_read_data(packet, iter, &len);

		ret = do_extent_request_retry(es, dp, packet, dp->leader_idx);
		if (ret < 0) {
			cfs_log_error(
				es->ec->log,
				"ino(%llu) send packet(%llu) to dp(%llu) error %d\n",
				es->ino,
				be64_to_cpu(packet->request.hdr.req_id), dp->id,
				ret);
			cfs_packet_release(packet);
			goto out;
		}
		cfs_data_partition_set_leader(dp, ret);

		for (i = 0; i < packet->reply.data.read.nr; i++) {
			struct cfs_page_frag *frag =
				&packet->reply.data.read.frags[i];
			if (cfs_page_io_account(frag->page, frag->size)) {
				SetPageUptodate(frag->page->page);
				unlock_page(frag->page->page);
				cfs_page_release(frag->page);
			}
		}
		cfs_packet_release(packet);
		cfs_page_iter_advance(iter, len);
		read_bytes += len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

int cfs_extent_read_pages(struct cfs_extent_stream *es, bool direct_io,
			  struct page **pages, size_t nr_pages,
			  loff_t file_offset, size_t first_page_offset,
			  size_t end_page_size)
{
	struct cfs_page **cpages;
	LIST_HEAD(io_info_list);
	struct cfs_extent_io_info *io_info;
	struct cfs_page_iter iter;
	size_t i;
	int ret;

#ifdef DEBUG
	cfs_pr_debug(
		"ino(%llu) nr_pages=%lu, file_offset=%llu, first_page_offset=%lu, end_page_size=%lu\n",
		es->ino, nr_pages, file_offset, first_page_offset,
		end_page_size);
#endif
	BUG_ON(nr_pages == 0);
	cpages = kvmalloc(sizeof(*cpages) * nr_pages, GFP_KERNEL);
	if (!cpages) {
		for (i = 0; i < nr_pages; i++) {
			ClearPageUptodate(pages[i]);
			SetPageError(pages[i]);
			unlock_page(pages[i]);
		}
		return -ENOMEM;
	}
	for (i = 0; i < nr_pages; i++) {
		cpages[i] = cfs_page_new(pages[i]);
		if (!cpages[i]) {
			while (i-- > 0) {
				ClearPageUptodate(pages[i]);
				SetPageError(pages[i]);
				unlock_page(pages[i]);
				cfs_page_release(cpages[i]);
			}
			kvfree(cpages);
			return -ENOMEM;
		}
	}
	if (nr_pages == 1) {
		cfs_page_io_set(cpages[0], end_page_size - first_page_offset);
	} else {
		cfs_page_io_set(cpages[0], PAGE_SIZE - first_page_offset);
		for (i = 1; i < nr_pages - 1; i++)
			cfs_page_io_set(cpages[i], PAGE_SIZE);
		cfs_page_io_set(cpages[i], end_page_size);
	}

	cfs_page_iter_init(&iter, cpages, nr_pages, first_page_offset,
			   end_page_size);

	ret = cfs_extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) extent cache refresh error %d\n",
			      es->ino, ret);
		goto err_page;
	}
	mutex_lock(&es->lock_io);
	ret = cfs_prepare_extent_io_list(&es->cache, file_offset,
					 cfs_page_iter_count(&iter),
					 &io_info_list);
	if (ret < 0) {
		mutex_unlock(&es->lock_io);
		cfs_log_error(
			es->ec->log,
			"ino(%llu) prepare extent write request error %d\n",
			es->ino, ret);
		goto err_page;
	}
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list,
					   struct cfs_extent_io_info, list);
		if (io_info->hole) {
			size_t read_bytes = 0, total_bytes = io_info->size;

			while (read_bytes < total_bytes) {
				struct cfs_page_frag frag = { 0 };
				size_t len = total_bytes - read_bytes;

				cfs_page_iter_get_frags(&iter, &frag, 1, &len);
				zero_user(frag.page->page, frag.offset,
					  frag.size);
				cfs_page_iter_advance(&iter, len);
				if (cfs_page_io_account(frag.page, len)) {
					SetPageUptodate(frag.page->page);
					unlock_page(frag.page->page);
					cfs_page_release(frag.page);
				}
				read_bytes += len;
			}
			goto next;
		}
		if (direct_io)
			ret = extent_read_pages_sync(es, io_info, &iter);
		else
			ret = extent_read_pages_async(es, io_info, &iter);
		if (ret < 0) {
			mutex_unlock(&es->lock_io);
			cfs_log_error(es->ec->log,
				      "ino(%llu) read pages error %d\n",
				      es->ino, ret);
			goto err_page;
		}

next:
		list_del(&io_info->list);
		cfs_extent_io_info_release(io_info);
	}
	mutex_unlock(&es->lock_io);

err_page:
	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list,
					   struct cfs_extent_io_info, list);
		list_del(&io_info->list);
		cfs_extent_io_info_release(io_info);
	}
	if (iter.nr > 0) {
		struct cfs_page *cpage = iter.pages[0];
		size_t first_page_size;

		BUG_ON(ret == 0);
		first_page_size =
			iter.nr == 1 ?
				iter.end_page_size - iter.first_page_offset :
				PAGE_SIZE - iter.first_page_offset;
		SetPageError(cpage->page);
		if (cfs_page_io_account(cpage, first_page_size)) {
			ClearPageUptodate(cpage->page);
			unlock_page(cpage->page);
			cfs_page_release(cpage);
		}
		for (i = 1; i < iter.nr; i++) {
			cpage = iter.pages[i];
			SetPageError(cpage->page);
			ClearPageUptodate(cpage->page);
			unlock_page(cpage->page);
			cfs_page_release(cpage);
		}
	}
	kvfree(cpages);
	return ret;
}

static void extent_dio_pages_release(struct page **pages, int num_pages,
				     bool dirty)
{
	int i;

	for (i = 0; i < num_pages; i++) {
		if (dirty)
			set_page_dirty_lock(pages[i]);
		put_page(pages[i]);
	}
	kvfree(pages);
}

static struct page **extent_dio_pages_alloc(struct iov_iter *iter, int type,
					    size_t *nr_pages,
					    size_t *first_page_offset,
					    size_t *end_page_size)
{
	unsigned long start;
	size_t nbytes;
	int npages;
	struct page **pages;
	int i;
	int ret;

	start = (unsigned long)(iter->iov->iov_base + iter->iov_offset);
	nbytes = iter->iov->iov_len - iter->iov_offset;
	npages = ((start & ~PAGE_MASK) + nbytes + PAGE_SIZE - 1) >> PAGE_SHIFT;
	pages = kvzalloc(sizeof(*pages) * npages, GFP_KERNEL);
	if (!pages)
		return ERR_PTR(-ENOMEM);

	ret = get_user_pages_fast(start, npages, type == READ, pages);
	if (ret != npages) {
		for (i = 0; i < ret; i++)
			put_page(pages[i]);
		kvfree(pages);
		return ERR_PTR(-ENOMEM);
	}
	iov_iter_advance(iter, nbytes);

	*nr_pages = npages;
	*first_page_offset = start & ~PAGE_MASK;
	*end_page_size = ((start & ~PAGE_MASK) + nbytes) & ~PAGE_MASK;
	if (*end_page_size == 0)
		*end_page_size = PAGE_SIZE;
	return pages;
}

ssize_t cfs_extent_dio_read_write(struct cfs_extent_stream *es, int type,
			      struct iov_iter *iter, loff_t offset)
{
	struct page **pages;
	size_t nr_pages;
	size_t first_page_offset;
	size_t end_page_size;
	size_t i;
	ssize_t ret = 0;

#ifdef DEBUG
	cfs_pr_debug("ino(%llu) type=%d offset=%lld, size=%lu\n", es->ino, type,
		     offset, iov_iter_count(iter));
#endif
	pages = extent_dio_pages_alloc(iter, type, &nr_pages,
				       &first_page_offset, &end_page_size);
	if (IS_ERR(pages)) {
		cfs_log_error(es->ec->log, "ino(%llu) alloc pages error %ld\n",
			      es->ino, PTR_ERR(pages));
		return PTR_ERR(pages);
	}

	BUG_ON(nr_pages == 0);
	for (i = 0; i < nr_pages; i++) {
		lock_page(pages[i]);
		if (type == WRITE)
			set_page_writeback(pages[i]);
	}
	if (type == WRITE)
		cfs_extent_write_pages(es, pages, nr_pages, offset,
				       first_page_offset, end_page_size);
	else
		cfs_extent_read_pages(es, true, pages, nr_pages, offset,
				      first_page_offset, end_page_size);
	cfs_extent_stream_flush(es);

	for (i = 0; i < nr_pages; i++) {
		wait_on_page_locked(pages[i]);
		if (TestClearPageError(pages[i]))
			ret = -EIO;
	}
	extent_dio_pages_release(pages, nr_pages, type == READ);
	return ret < 0 ? ret : iov_iter_count(iter);
}

struct cfs_extent_stream *cfs_extent_stream_new(struct cfs_extent_client *ec,
						u64 ino)
{
	struct cfs_extent_stream *es;
	int ret;

	es = kzalloc(sizeof(*es), GFP_NOFS);
	if (!es)
		return NULL;
	ret = cfs_extent_cache_init(&es->cache, es);
	if (ret < 0) {
		kfree(es);
		return NULL;
	}
	es->ec = ec;
	es->ino = ino;
	INIT_LIST_HEAD(&es->writers);
	es->max_writers = EXTENT_WRITER_MAX_COUNT;
	INIT_LIST_HEAD(&es->readers);
	es->max_readers = EXTENT_READER_MAX_COUNT;
	hash_add(ec->streams, &es->hash, ino);
	mutex_init(&es->lock_writers);
	mutex_init(&es->lock_readers);
	mutex_init(&es->lock_io);
	es->enable_rdma = ec->enable_rdma;
	es->rdma_port = ec->rdma_port;
	return es;
}

void cfs_extent_stream_release(struct cfs_extent_stream *es)
{
	if (!es)
		return;
	cfs_extent_stream_flush(es);
	cfs_extent_cache_clear(&es->cache);
	hash_del(&es->hash);
	kfree(es);
}

int cfs_extent_stream_flush(struct cfs_extent_stream *es)
{
	struct cfs_extent_writer *writer;
	struct cfs_extent_reader *reader;

	while (true) {
		mutex_lock(&es->lock_writers);
		writer = list_first_entry_or_null(
			&es->writers, struct cfs_extent_writer, list);
		if (!writer) {
			mutex_unlock(&es->lock_writers);
			break;
		}
		list_del(&writer->list);
		es->nr_writers--;
		mutex_unlock(&es->lock_writers);

		cfs_extent_writer_flush(writer);
		cfs_extent_writer_release(writer);
	}

	while (true) {
		mutex_lock(&es->lock_readers);
		reader = list_first_entry_or_null(
			&es->readers, struct cfs_extent_reader, list);
		if (!reader) {
			mutex_unlock(&es->lock_readers);
			break;
		}
		list_del(&reader->list);
		es->nr_readers--;
		mutex_unlock(&es->lock_readers);

		cfs_extent_reader_flush(reader);
		cfs_extent_reader_release(reader);
	}
	return 0;
}

int cfs_extent_stream_truncate(struct cfs_extent_stream *es, loff_t size)
{
	loff_t old_size;
	int ret;

	ret = cfs_extent_stream_flush(es);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) extent stream flush error %d\n",
			      es->ino, ret);
		return ret;
	}

	ret = cfs_meta_truncate(es->ec->meta, es->ino, size);
	if (ret < 0) {
		cfs_log_error(es->ec->log, "ino(%llu) meta turncate error %d\n",
			      es->ino, ret);
		return ret;
	}

	old_size = cfs_extent_cache_get_size(&es->cache);
	if (old_size <= size) {
		cfs_extent_cache_set_size(&es->cache, size, true);
		return 0;
	}

	cfs_extent_cache_truncate(&es->cache, size);
	ret = cfs_extent_cache_refresh(&es->cache, true);
	if (ret < 0)
		cfs_log_error(es->ec->log,
			      "ino(%llu) extent cache refresh error %d\n",
			      es->ino, ret);
	return ret;
}

static void extent_write_iter_reply_cb(struct cfs_packet *packet)
{
	struct cfs_extent_writer *writer = packet->private;
	struct cfs_log *log = writer->es->ec->log;
	int err;

	if (packet->error) {
		err = packet->error;
		cfs_log_error(log, "ino(%llu) io error %d\n", writer->es->ino,
			      err);
	} else {
		err = -cfs_parse_status(packet->reply.hdr.result_code);
		if (err)
			cfs_log_error(log, "ino(%llu) reply error %d\n",
				      writer->es->ino, err);
	}
	if (!err)
		cfs_extent_writer_ack_bytes(
			writer, be32_to_cpu(packet->request.hdr.size));
}

static int cfs_set_packet_iter_crc(struct cfs_packet *packet, struct iov_iter *iter, size_t size) {
	u32 crc = 0;
	bool ret = false;

	packet->request.iov.iov_base = kvmalloc(size, GFP_KERNEL);
	if (!packet->request.iov.iov_base) {
		return -ENOMEM;
	}
	packet->request.iov.iov_len = size;
	ret = copy_from_iter_full(packet->request.iov.iov_base, size, iter);
	if (!ret) {
		kfree(packet->request.iov.iov_base);
		return -EIO;
	}

	crc ^= 0xffffffffUL;
	crc = crc32_le(crc, packet->request.iov.iov_base, size);
	crc ^= 0xffffffffUL;

#ifdef KERNEL_HAS_IOV_ITER_WITH_TAG
	iov_iter_init(&(packet->request.data.iter), WRITE, &(packet->request.iov), 1, size);
#else
	iov_iter_init(&(packet->request.data.iter), &(packet->request.iov), 1, size, 0);
#endif

	packet->request.hdr.crc = cpu_to_be32(crc);
	packet->request.hdr.size = cpu_to_be32(size);
	packet->pkg_data_type = CFS_PACKAGE_DATA_ITER;

	return 0;
}

static size_t extent_write_iter_random(struct cfs_extent_stream *es, struct cfs_extent_io_info *io_info, struct iov_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t w_len;
	size_t send_bytes = 0;
	size_t ret = 0;

	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) cannot get data partition(%llu)\n",
			      es->ino, io_info->ext.pid);
		ret = -ENOENT;
		return ret;
	}
	while (send_bytes < io_info->size) {
		w_len = min(io_info->size - send_bytes, EXTENT_BLOCK_SIZE);
		packet = cfs_extent_packet_new(
			CFS_OP_STREAM_RANDOM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			dp->nr_followers, dp->id, io_info->ext.ext_id,
			io_info->offset - io_info->ext.file_offset +
				io_info->ext.ext_offset + send_bytes,
			io_info->offset);
		if (!packet) {
			ret = -ENOMEM;
			goto out;
		}

		ret = cfs_set_packet_iter_crc(packet, iter, w_len);
		if (unlikely(ret < 0)) {
			cfs_log_error(es->ec->log, "ino(%llu) cfs_set_packet_iter_crc error ret(%d)\n", es->ino, ret);
			cfs_packet_release(packet);
			goto out;
		}

		ret = do_extent_request_retry(es, dp, packet, dp->leader_idx);
		if (ret < 0) {
			cfs_log_error(
				es->ec->log,
				"ino(%llu) send packet(%llu) to dp(%llu) error %d\n",
				es->ino,
				be64_to_cpu(packet->request.hdr.req_id), dp->id,
				ret);
			cfs_packet_release(packet);
			goto out;
		}
		cfs_data_partition_set_leader(dp, ret);

		cfs_packet_release(packet);
		send_bytes += w_len;
	}

out:
	cfs_data_partition_release(dp);
	return ret;
}

static size_t extent_write_iter_tiny(struct cfs_extent_stream *es, struct cfs_extent_io_info *io_info, struct iov_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	size_t ret = -1;
	u32 retry_cnt = cfs_extent_get_partition_count(es->ec);

retry:
	if (retry_cnt == 0)
		return ret;

	dp = cfs_extent_select_partition(es->ec);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) cannot select data partition\n",
			      es->ino);
		ret = -ENOENT;
		return ret;
	}

	packet = cfs_extent_packet_new(CFS_OP_STREAM_WRITE,
				       CFS_EXTENT_TYPE_TINY, dp->nr_followers,
				       dp->id, 0, 0, 0);
	if (!packet) {
		cfs_data_partition_release(dp);
		ret = -ENOMEM;
		return ret;
	}

	if (es->enable_rdma) {
		cfs_packet_set_request_arg(packet, dp->rdma_follower_addrs);
	} else {
		cfs_packet_set_request_arg(packet, dp->follower_addrs);
	}

	ret = cfs_set_packet_iter_crc(packet, iter, io_info->size);
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log, "ino(%llu) cfs_set_packet_iter_crc error ret(%d)\n", es->ino, ret);
		cfs_packet_release(packet);
		return ret;
	}

	if (es->enable_rdma) {
		ret = do_extent_request_rdma(es, &dp->members.base[0], packet);
	} else {
		ret = do_extent_request(es, &dp->members.base[0], packet);
	}

	if (ret < 0) {
		if (retry_cnt == 1)
			cfs_log_error(es->ec->log,
				      "ino(%llu) write extent error %d\n",
				      es->ino, ret);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}
	ret = -cfs_parse_status(packet->reply.hdr.result_code);
	if (ret < 0) {
		if (retry_cnt == 1)
			cfs_log_error(
				es->ec->log,
				"ino(%llu) write extent reply error code 0x%x\n",
				es->ino, packet->reply.hdr.result_code);
		cfs_packet_release(packet);
		cfs_data_partition_release(dp);
		retry_cnt--;
		goto retry;
	}

	cfs_data_partition_release(dp);

	cfs_packet_extent_init(&extent, 0, be64_to_cpu(packet->reply.hdr.pid),
			       be64_to_cpu(packet->reply.hdr.ext_id),
			       be64_to_cpu(packet->reply.hdr.ext_offset),
			       io_info->size);
	ret = cfs_extent_cache_append(&es->cache, &extent, false, NULL);
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) append extent cache error %d\n",
			      es->ino, ret);
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_meta_append_extent(es->ec->meta, es->ino, &extent, NULL);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) sync extent cache error %d\n", es->ino,
			      ret);
		cfs_packet_release(packet);
		return ret;
	}

	cfs_packet_release(packet);
	return 0;
}

static int cfs_set_packet_rdma_buffer_crc(struct cfs_extent_writer *writer, struct cfs_packet *packet, struct iov_iter *iter, size_t size) {
	struct BufferItem *pDataBuf = NULL;
	u32 crc = 0;
	bool ret = false;

	pDataBuf = IBVSocket_get_data_buf(writer->sock->ibvsock, size);
	if (!pDataBuf) {
		return -ENOMEM;
	}

	ret = copy_from_iter_full(pDataBuf->pBuff, size, iter);
	if (!ret) {
		IBVSocket_free_data_buf(writer->sock->ibvsock, pDataBuf);
		return -EIO;
	}

	crc ^= 0xffffffffUL;
	crc = crc32_le(crc, pDataBuf->pBuff, size);
	crc ^= 0xffffffffUL;

	packet->request.hdr.crc = cpu_to_be32(crc);
	packet->request.hdr.size = cpu_to_be32(size);
	packet->pkg_data_type = CFS_PACKAGE_RDMA_ITER;
	packet->data_buffer = pDataBuf;
	packet->request.hdr_padding.RdmaAddr = pDataBuf->dma_addr;
	packet->request.hdr_padding.RdmaLength = htonl(size);

	return 0;
}

static size_t extent_write_iter_normal(struct cfs_extent_stream *es,
				     struct cfs_extent_io_info *io_info,
				     struct iov_iter *iter)
{
	struct cfs_extent_writer *writer;
	struct cfs_packet *packet;
	struct cfs_packet_extent extent;
	loff_t offset = io_info->offset;
	size_t send_bytes = 0, total_bytes = io_info->size;
	size_t w_len;
	int ret;

	while (send_bytes < total_bytes) {
		w_len = min(total_bytes - send_bytes, EXTENT_BLOCK_SIZE);
		writer = extent_stream_get_writer(es, offset, w_len);
		if (IS_ERR(writer))
			return PTR_ERR(writer);

		packet = cfs_extent_packet_new(
			CFS_OP_STREAM_WRITE, CFS_EXTENT_TYPE_NORMAL,
			writer->dp->nr_followers, writer->dp->id,
			writer->ext_id, offset - writer->file_offset, offset);
		if (!packet) {
			cfs_log_error(es->ec->log, "ino(%llu) oom\n", es->ino);
			return -ENOMEM;
		}
		cfs_packet_set_callback(packet, extent_write_iter_reply_cb,
					writer);
		if (es->enable_rdma) {
			cfs_packet_set_request_arg(packet, writer->dp->rdma_follower_addrs);
		} else {
			cfs_packet_set_request_arg(packet, writer->dp->follower_addrs);
		}

		if (es->enable_rdma) {
			ret = cfs_set_packet_rdma_buffer_crc(writer, packet, iter, w_len);
		} else {
			ret = cfs_set_packet_iter_crc(packet, iter, w_len);
		}
		if (unlikely(ret < 0)) {
			cfs_log_error(es->ec->log, "ino(%llu) set packet iter and crc error ret(%d)\n", es->ino, ret);
			cfs_packet_release(packet);
			return ret;
		}

		cfs_packet_extent_init(&extent, writer->file_offset, 0, 0, 0,
				       writer->w_size + w_len);
		ret = cfs_extent_cache_append(&es->cache, &extent, false, NULL);
		if (unlikely(ret < 0)) {
			cfs_log_error(es->ec->log, "ino(%llu) oom\n", es->ino);
			cfs_packet_release(packet);
			return ret;
		}
		cfs_extent_writer_request(writer, packet);

		send_bytes += w_len;
		offset += w_len;
	}

	return send_bytes;
}

static size_t cfs_extent_write_iter(struct cfs_extent_stream *es, struct cfs_extent_io_info *io_info, struct iov_iter *iter) {
	size_t ret = 0;

	switch (extent_io_type(io_info))
	{
		case EXTENT_WRITE_TYPE_RANDOM:
			ret = extent_write_iter_random(es, io_info, iter);
			break;
		case EXTENT_WRITE_TYPE_TINY:
			ret = extent_write_iter_tiny(es, io_info, iter);
			break;
		case EXTENT_WRITE_TYPE_NORMAL:
			ret = extent_write_iter_normal(es, io_info, iter);
			break;
	}

	return ret;
}

static inline void cfs_packet_set_read_iter(struct cfs_packet *packet,
					    struct iov_iter *iter, size_t size)
{
	packet->pkg_data_type = CFS_PACKAGE_READ_ITER;
	packet->request.hdr.size = cpu_to_be32(size);
	packet->reply.data.user_iter = iter;
}

static size_t cfs_extent_read_iter(struct cfs_extent_stream *es,
				  struct cfs_extent_io_info *io_info, struct iov_iter *iter)
{
	struct cfs_data_partition *dp;
	struct cfs_packet *packet;
	size_t len;
	size_t read_bytes = 0, total_bytes = io_info->size;
	size_t read_offset = io_info->offset - io_info->ext.file_offset +
			     io_info->ext.ext_offset;
	int ret = 0;

	if (io_info->hole) {
		return 0;
	}

	dp = cfs_extent_get_partition(es->ec, io_info->ext.pid);
	if (!dp) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) not found data partition(%llu)\n",
			      es->ino, io_info->ext.pid);
		return -ENOENT;
	}
	while (read_bytes < total_bytes) {
		len = min(total_bytes - read_bytes, EXTENT_BLOCK_SIZE);
		packet = cfs_extent_packet_new(CFS_OP_STREAM_READ,
					       CFS_EXTENT_TYPE_NORMAL, 0,
					       dp->id, io_info->ext.ext_id,
					       read_offset + read_bytes,
					       io_info->offset);
		if (!packet) {
			cfs_data_partition_release(dp);
			return -ENOMEM;
		}
		cfs_packet_set_read_iter(packet, iter, len);

		ret = do_extent_request_retry(es, dp, packet, dp->leader_idx);
		if (ret < 0) {
			cfs_log_error(
				es->ec->log,
				"ino(%llu) send packet(%llu) to dp(%llu) error %d\n",
				es->ino,
				be64_to_cpu(packet->request.hdr.req_id), dp->id,
				ret);
			cfs_packet_release(packet);
			cfs_data_partition_release(dp);
			return ret;
		}
		cfs_data_partition_set_leader(dp, ret);

		cfs_packet_release(packet);
		iov_iter_advance(iter, len);
		read_bytes += len;
	}

	cfs_data_partition_release(dp);
	return read_bytes;
}

ssize_t cfs_extent_direct_io(struct cfs_extent_stream *es, struct iov_iter *iter, loff_t offset)
{
	LIST_HEAD(io_info_list);
	struct cfs_extent_io_info *io_info;
	size_t io_ret;
	int ret;
	ssize_t io_bytes = 0;

	ret = cfs_extent_cache_refresh(&es->cache, false);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) extent cache refresh error %d\n",
			      es->ino, ret);
		return ret;
	}

	mutex_lock(&es->lock_io);
	ret = cfs_prepare_extent_io_list(&es->cache, offset,
					 iov_iter_count(iter), &io_info_list);
	if (ret < 0) {
		mutex_unlock(&es->lock_io);
		cfs_log_error(
			es->ec->log,
			"ino(%llu) prepare extent write request error %d\n",
			es->ino, ret);
		return ret;
	}

	while (!list_empty(&io_info_list)) {
		io_info = list_first_entry(&io_info_list,
					   struct cfs_extent_io_info, list);
		list_del(&io_info->list);
		if (iov_iter_rw(iter) == WRITE) {
			io_ret = cfs_extent_write_iter(es, io_info, iter);
		} else {
			io_ret = cfs_extent_read_iter(es, io_info, iter);
		}
		cfs_extent_io_info_release(io_info);
		if (io_ret < 0) {
			mutex_unlock(&es->lock_io);
			cfs_log_error(es->ec->log,
				      "ino(%llu) direct io error %d\n",
				      es->ino, ret);
			return io_ret;
		}
		io_bytes += io_ret;
	}
	mutex_unlock(&es->lock_io);

	cfs_extent_stream_flush(es);

	return io_bytes;
}
