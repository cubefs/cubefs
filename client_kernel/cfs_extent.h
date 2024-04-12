/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_EXTENT_H__
#define __CFS_EXTENT_H__

#include "cfs_common.h"
#include "cfs_log.h"
#include "cfs_master.h"
#include "cfs_meta.h"
#include "cfs_page.h"
#include "cfs_option.h"
#include "cfs_rdma_socket.h"

#define EXTENT_STREAM_BUCKET_MAX_NUM 128
#define EXTENT_DP_BUCKET_MAX_NUM 128

#define EXTENT_WRITER_F_DIRTY (0x1 << 0)
#define EXTENT_WRITER_F_RECOVER (0x1 << 1)
#define EXTENT_WRITER_F_ERROR (0x1 << 2)

#define EXTENT_READER_F_RECOVER (0x1 << 0)
#define EXTENT_READER_F_ERROR (0x1 << 1)

struct cfs_extent_client;
struct cfs_extent_stream;

enum extent_write_type {
	EXTENT_WRITE_TYPE_RANDOM = 0,
	EXTENT_WRITE_TYPE_TINY,
	EXTENT_WRITE_TYPE_NORMAL,
};

struct cfs_data_partition {
	struct hlist_node hash;
	struct list_head list;
	u32 type;
	u64 id;
	s8 status;
	u8 replica_num;
	struct sockaddr_storage_array members;
	u32 leader_idx;
	u64 epoch;
	s64 ttl;
	bool is_recover;
	bool is_discard;
	struct cfs_buffer *follower_addrs;
	struct cfs_buffer *rdma_follower_addrs;
	u8 nr_followers;
	atomic_t refcnt;
};

struct cfs_extent_io_info {
	struct list_head list;
	struct cfs_packet_extent ext;
	loff_t offset; /* request file offset */
	size_t size; /* request file size */
	bool hole;
};

struct cfs_extent_cache {
	u64 generation;
	loff_t size;
	struct btree *extents;
	struct btree *discard;
	struct mutex lock;
	struct cfs_extent_stream *es;
};

struct cfs_extent_writer {
	struct list_head list;
	struct cfs_extent_stream *es;
	struct cfs_data_partition *dp;
	struct cfs_socket *sock;
	struct list_head tx_packets;
	struct list_head rx_packets;
	spinlock_t lock_tx;
	spinlock_t lock_rx;
	struct work_struct tx_work;
	struct work_struct rx_work;
	wait_queue_head_t tx_wq;
	wait_queue_head_t rx_wq;
	atomic_t tx_inflight;
	atomic_t rx_inflight;
	u64 ext_id;
	u64 file_offset; /* extent file offset */
	u64 ext_offset;
	u32 ext_size; /* acked write size */
	u32 w_size; /* write size */
	volatile unsigned flags;
	struct cfs_extent_writer *recover;
};

struct cfs_extent_reader {
	struct list_head list;
	struct cfs_extent_stream *es;
	struct cfs_data_partition *dp;
	struct cfs_socket *sock;
	struct list_head tx_packets;
	struct list_head rx_packets;
	spinlock_t lock_tx;
	spinlock_t lock_rx;
	struct work_struct tx_work;
	struct work_struct rx_work;
	wait_queue_head_t rx_wq;
	wait_queue_head_t tx_wq;
	atomic_t rx_inflight;
	atomic_t tx_inflight;
	u64 ext_id;
	volatile unsigned flags;
	struct cfs_extent_reader *recover;
	u32 recover_cnt;
	u32 host_idx;
};

struct cfs_extent_stream {
	struct hlist_node hash;
	struct cfs_extent_client *ec;
	struct cfs_extent_cache cache;
	struct list_head writers;
	u32 nr_writers;
	u32 max_writers;
	struct mutex lock_writers;
	struct mutex lock_io;
	struct list_head readers;
	u32 nr_readers;
	u32 max_readers;
	struct mutex lock_readers;
	u64 ino;
	bool enable_rdma;
	u32 rdma_port;
};

struct cfs_extent_client {
	struct cfs_master_client *master;
	struct cfs_meta_client *meta;
	rwlock_t lock;
	struct hlist_head streams[EXTENT_STREAM_BUCKET_MAX_NUM];
	struct hlist_head data_partitions[EXTENT_DP_BUCKET_MAX_NUM];
	struct list_head rw_partitions;
	u32 nr_rw_partitions;
	struct cfs_data_partition *select_dp;
	struct mutex select_lock;
	struct delayed_work update_dp_work;
	struct cfs_log *log;
	bool enable_rdma;
	u32 rdma_port;
};

struct cfs_extent_io_info *
cfs_extent_io_info_new(loff_t offset, size_t size,
		       const struct cfs_packet_extent *ext);
void cfs_extent_io_info_release(struct cfs_extent_io_info *io_info);

struct cfs_data_partition *
cfs_data_partition_new(struct cfs_data_partition_view *dp_view, u32 rdma_port);
void cfs_data_partition_release(struct cfs_data_partition *dp);
#define cfs_data_partition_put cfs_data_partition_release

static inline void cfs_data_partition_get(struct cfs_data_partition *dp)
{
	atomic_inc(&dp->refcnt);
}

static inline void cfs_data_partition_set_leader(struct cfs_data_partition *dp,
						 u32 leader)
{
	dp->leader_idx = leader % dp->members.num;
}

int cfs_extent_cache_init(struct cfs_extent_cache *cache,
			  struct cfs_extent_stream *es);
void cfs_extent_cache_clear(struct cfs_extent_cache *cache);
void cfs_extent_cache_set_size(struct cfs_extent_cache *cache, loff_t size,
			       bool sync);
loff_t cfs_extent_cache_get_size(struct cfs_extent_cache *cache);
bool cfs_extent_cache_get_end(struct cfs_extent_cache *cache, u64 offset,
			      struct cfs_packet_extent *extent);
void cfs_extent_cache_truncate(struct cfs_extent_cache *cache, loff_t size);
int cfs_extent_cache_refresh(struct cfs_extent_cache *cache, bool force);
int cfs_extent_cache_append(struct cfs_extent_cache *cache,
			    struct cfs_packet_extent *extent, bool sync,
			    struct cfs_packet_extent_array *discard_extents);
void cfs_extent_cache_remove_discard(
	struct cfs_extent_cache *cache,
	struct cfs_packet_extent_array *discard_extents);
int cfs_prepare_extent_io_list(struct cfs_extent_cache *cache, loff_t offset,
			       size_t size, struct list_head *io_info_list);

struct cfs_extent_writer *cfs_extent_writer_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						loff_t file_offset, u64 ext_id,
						u64 ext_offset, u32 ext_size);
void cfs_extent_writer_release(struct cfs_extent_writer *writer);
int cfs_extent_writer_flush(struct cfs_extent_writer *writer);
void cfs_extent_writer_request(struct cfs_extent_writer *writer,
			       struct cfs_packet *packet);
static inline void cfs_extent_writer_set_dirty(struct cfs_extent_writer *writer)
{
	writer->flags |= EXTENT_WRITER_F_DIRTY;
}

static inline void
cfs_extent_writer_clear_dirty(struct cfs_extent_writer *writer)
{
	writer->flags &= ~EXTENT_WRITER_F_DIRTY;
}

static inline bool
cfs_extent_writer_test_dirty(struct cfs_extent_writer *writer)
{
	return writer->flags & EXTENT_WRITER_F_DIRTY;
}

static inline void
cfs_extent_writer_write_bytes(struct cfs_extent_writer *writer, size_t size)
{
	writer->w_size += size;
}

static inline void cfs_extent_writer_ack_bytes(struct cfs_extent_writer *writer,
					       size_t size)
{
	writer->ext_size += size;
}

struct cfs_extent_reader *cfs_extent_reader_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						u32 host_idx, u64 ext_id);
void cfs_extent_reader_release(struct cfs_extent_reader *reader);
void cfs_extent_reader_flush(struct cfs_extent_reader *reader);
void cfs_extent_reader_request(struct cfs_extent_reader *reader,
			       struct cfs_packet *packet);

struct cfs_extent_client *cfs_extent_client_new(struct cfs_mount_info *cmi);
void cfs_extent_client_release(struct cfs_extent_client *ec);
int cfs_extent_update_partition(struct cfs_extent_client *ec);
struct cfs_data_partition *
cfs_extent_get_partition(struct cfs_extent_client *ec, u64 id);
u32 cfs_extent_get_partition_count(struct cfs_extent_client *ec);
struct cfs_data_partition *
cfs_extent_select_partition(struct cfs_extent_client *ec);

int cfs_extent_id_new(struct cfs_extent_stream *es,
		      struct cfs_data_partition **dpp, u64 *ext_id);

struct cfs_extent_stream *cfs_extent_stream_new(struct cfs_extent_client *ec,
						u64 ino);
void cfs_extent_stream_release(struct cfs_extent_stream *es);
int cfs_extent_stream_flush(struct cfs_extent_stream *es);
int cfs_extent_stream_truncate(struct cfs_extent_stream *es, loff_t size);

int cfs_extent_read_pages(struct cfs_extent_stream *es, bool direct_io,
			  struct page **pages, size_t nr_pages,
			  loff_t file_offset, size_t first_page_offset,
			  size_t end_page_size);
int cfs_extent_write_pages(struct cfs_extent_stream *es, struct page **pages,
			   size_t nr_pages, loff_t file_offset,
			   size_t first_page_offset, size_t end_page_size);
ssize_t cfs_extent_dio_read_write(struct cfs_extent_stream *es, int type,
			      struct iov_iter *iter, loff_t offset);
ssize_t cfs_extent_direct_io(struct cfs_extent_stream *es, struct iov_iter *iter, loff_t offset);
int cfs_extent_module_init(void);
void cfs_extent_module_exit(void);

static inline struct cfs_packet *
cfs_extent_packet_new(u8 op, u8 ext_type, u8 remaining_followers, u64 pid,
		      u64 ext_id, u64 ext_offset, u64 file_offset)
{
	struct cfs_packet *packet;

	packet = cfs_packet_new(op, pid, NULL, NULL);
	if (!packet)
		return NULL;
	packet->request.hdr.ext_type = ext_type;
	packet->request.hdr.remaining_followers = remaining_followers;
	packet->request.hdr.ext_id = cpu_to_be64(ext_id);
	packet->request.hdr.ext_offset = cpu_to_be64(ext_offset);
	packet->request.hdr.kernel_offset = cpu_to_be64(file_offset);
	return packet;
}

static inline void cfs_packet_set_write_data(struct cfs_packet *packet,
					     struct cfs_page_iter *pi,
					     size_t *size)
{
	packet->pkg_data_type = CFS_PACKAGE_DATA_PAGE;
	packet->request.data.write.frags = packet->rw.frags;
	packet->request.data.write.nr = cfs_page_iter_get_frags(
		pi, packet->request.data.write.frags, CFS_PAGE_VEC_NUM, size);
	packet->request.hdr.size = cpu_to_be32(*size);
}

static inline void cfs_packet_set_read_data(struct cfs_packet *packet,
					    struct cfs_page_iter *pi,
					    size_t *size)
{
	packet->pkg_data_type = CFS_PACKAGE_DATA_PAGE;
	packet->reply.data.read.frags = packet->rw.frags;
	packet->reply.data.read.nr = cfs_page_iter_get_frags(
		pi, packet->reply.data.read.frags, CFS_PAGE_VEC_NUM, size);
	packet->request.hdr.size = cpu_to_be32(*size);
}

#endif
