#ifndef __CFS_EXTENT_H__
#define __CFS_EXTENT_H__

#include "cfs_common.h"

#include "cfs_master.h"
#include "cfs_meta.h"

#define CFS_MAX_NUM_ES_BUCKET 128
#define CFS_MAX_NUM_DP_BUCKET 128

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
	u8 nr_followers;
	atomic_t refcnt;
};

struct cfs_extent_cache {
	u64 generation;
	loff_t size;
	struct btree *extents;
	struct btree *discard;
	struct mutex lock;
	struct cfs_extent_stream *es;
};

struct cfs_extent_async_io {
	struct cfs_data_partition *dp;
	struct cfs_socket *sock;
	struct list_head requests;
	struct mutex lock; /* lock requests */
	struct work_struct work;
	wait_queue_head_t wq;
	atomic_t inflight;
	int rw;
	bool io_err;
};

struct cfs_extent_writer {
	struct list_head list;
	struct cfs_extent_async_io async;
	u64 ext_id;
	u64 file_offset; /* extent file offset */
	size_t ext_size;
	bool dirty;
};

struct cfs_extent_reader {
	struct cfs_extent_async_io async;
	u64 ext_id;
	u64 file_offset; /* extent file offset */
	u64 ext_offset;
	u32 ext_size;
};

struct cfs_extent_client;

struct cfs_extent_stream {
	struct hlist_node hash;
	struct cfs_extent_client *ec;
	struct cfs_extent_cache cache;
	struct mutex w_lock;
	struct cfs_extent_writer *writer;
	struct cfs_extent_reader *reader;
	u64 ino;
};

struct cfs_extent_client {
	struct cfs_master_client *master;
	struct cfs_meta_client *meta;
	rwlock_t lock;
	struct hlist_head streams[CFS_MAX_NUM_ES_BUCKET];
	struct hlist_head data_partitions[CFS_MAX_NUM_DP_BUCKET];
	struct list_head rw_partitions;
	u32 nr_rw_partitions;
	struct cfs_data_partition *select_dp;
	struct mutex select_lock;
	struct delayed_work update_dp_work;
};

struct cfs_extent_client *
cfs_extent_client_new(struct cfs_master_client *master,
		      struct cfs_meta_client *meta);
void cfs_extent_client_release(struct cfs_extent_client *ec);

struct cfs_extent_stream *cfs_extent_stream_new(struct cfs_extent_client *ec,
						u64 ino);
void cfs_extent_stream_release(struct cfs_extent_stream *es);
int cfs_extent_stream_flush(struct cfs_extent_stream *es);
int cfs_extent_stream_truncate(struct cfs_extent_stream *es, loff_t size);

int cfs_extent_read_pages(struct cfs_extent_stream *es, struct page **pages,
			  size_t nr_pages);
int cfs_extent_write_pages(struct cfs_extent_stream *es, struct page **pages,
			   size_t nr_pages, size_t last_page_size);
int cfs_extent_dio_read(struct cfs_extent_stream *es, struct iov_iter *iter,
			loff_t offset);
int cfs_extent_dio_write(struct cfs_extent_stream *es, struct iov_iter *iter,
			 loff_t offset);
int cfs_extent_module_init(void);
void cfs_extent_module_exit(void);

#endif
