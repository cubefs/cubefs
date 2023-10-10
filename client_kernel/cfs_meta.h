#ifndef __CFS_META_H__
#define __CFS_META_H__

#include "cfs_common.h"

#include "btree.h"
#include "cfs_master.h"
#include "cfs_packet.h"
#include "cfs_socket.h"

struct cfs_meta_partition {
	struct hlist_node hash;
	struct list_head list;
	u64 id;
	u64 start_ino;
	u64 end_ino;
	struct sockaddr_storage_array members;
	size_t leader_idx;
	s8 status;
};

static inline struct cfs_meta_partition *
cfs_meta_partition_new(struct cfs_meta_partition_view *mp_view)
{
	struct cfs_meta_partition *mp;

	mp = kzalloc(sizeof(*mp), GFP_NOFS);
	if (!mp)
		return NULL;
	mp->id = mp_view->id;
	mp->start_ino = mp_view->start_ino;
	mp->end_ino = mp_view->end_ino;
	for (mp->leader_idx = 0; mp->leader_idx < mp_view->members.num;
	     mp->leader_idx++) {
		if (cfs_addr_cmp(&mp_view->members.base[mp->leader_idx],
				 mp_view->leader) == 0)
			break;
	}
	sockaddr_storage_array_move(&mp->members, &mp_view->members);
	mp->status = mp_view->status;
	return mp;
}

static inline void cfs_meta_partition_clear(struct cfs_meta_partition *mp)
{
	if (!mp)
		return;
	sockaddr_storage_array_clear(&mp->members);
	memset(mp, 0, sizeof(*mp));
}

static inline void cfs_meta_partition_release(struct cfs_meta_partition *mp)
{
	if (!mp)
		return;
	cfs_meta_partition_clear(mp);
	kfree(mp);
}

struct cfs_uniqid_range {
	struct hlist_node hash;
	u64 pid;
	u64 cur;
	u64 end;
};

static inline struct cfs_uniqid_range *cfs_uniqid_range_new(u64 pid, u64 cur,
							    u64 end)
{
	struct cfs_uniqid_range *range;

	range = kzalloc(sizeof(*range), GFP_NOFS);
	if (!range)
		return NULL;
	range->pid = pid;
	range->cur = cur;
	range->end = end;
	return range;
}

static inline void cfs_uniqid_range_release(struct cfs_uniqid_range *range)
{
	if (!range)
		return;
	kfree(range);
}

static inline bool cfs_uniqid_range_next_id(struct cfs_uniqid_range *range,
					    u64 *uniqid)
{
	if (range->cur >= range->end)
		return false;
	*uniqid = range->cur++;
	return true;
}

struct cfs_meta_client {
	char *volume;
	struct cfs_master_client *master;
	rwlock_t lock;
#define META_PARTITION_BUCKET_COUNT 128
	struct hlist_head paritions[META_PARTITION_BUCKET_COUNT];
	struct btree *partition_ranges;
	struct list_head rw_partitions;
	u32 nr_rw_partitions;
	struct cfs_meta_partition *select_mp;
	struct mutex select_lock;
	struct delayed_work update_mp_work;

#define META_UNIQID_BUCKET_COUNT 128
	struct hlist_head uniqid_ranges[META_UNIQID_BUCKET_COUNT];
	struct mutex uniqid_lock;
};

struct cfs_meta_client *cfs_meta_client_new(struct cfs_master_client *master,
					    const char *vol_name);
void cfs_meta_client_release(struct cfs_meta_client *mc);

int cfs_meta_create(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, umode_t mode, uid_t uid, gid_t gid,
		    const char *target,
		    struct cfs_quota_info_array *quota_infos,
		    struct cfs_packet_inode **iinfo);
int cfs_meta_link(struct cfs_meta_client *mc, u64 parent_ino, struct qstr *name,
		  u64 ino, struct cfs_packet_inode **iinfo);
int cfs_meta_delete(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, bool is_dir);
int cfs_meta_rename(struct cfs_meta_client *mc, u64 src_parent_ino,
		    struct qstr *src_name, u64 dst_parent_ino,
		    struct qstr *dst_name, bool over_written);
int cfs_meta_lookup(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, struct cfs_packet_inode **iinfo);
int cfs_meta_lookup_path(struct cfs_meta_client *mc, const char *path,
			 struct cfs_packet_inode **iinfo);
int cfs_meta_get(struct cfs_meta_client *mc, u64 ino,
		 struct cfs_packet_inode **iinfo);
int cfs_meta_readdir(struct cfs_meta_client *mc, u64 parent_ino,
		     const char *from, u64 limit,
		     struct cfs_packet_dentry_array *dentries);
int cfs_meta_set_attr(struct cfs_meta_client *mc, u64 ino, struct iattr *attr);
int cfs_meta_set_xattr(struct cfs_meta_client *mc, u64 ino, const char *name,
		       const void *value, size_t len, int flags);
ssize_t cfs_meta_get_xattr(struct cfs_meta_client *mc, u64 ino,
			   const char *name, void *value, size_t size);
ssize_t cfs_meta_list_xattr(struct cfs_meta_client *mc, u64 ino, char *names,
			    size_t size);
int cfs_meta_remove_xattr(struct cfs_meta_client *mc, u64 ino,
			  const char *name);
int cfs_meta_list_extent(struct cfs_meta_client *mc, u64 ino, u64 *gen,
			 u64 *size, struct cfs_packet_extent_array *extents);
int cfs_meta_append_extent(struct cfs_meta_client *mc, u64 ino,
			   struct cfs_packet_extent *extent,
			   struct cfs_packet_extent_array *discard_extents);
int cfs_meta_truncate(struct cfs_meta_client *mc, u64 ino, loff_t size);
#endif
