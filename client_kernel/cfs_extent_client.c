/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"
#include "rdma/rdma_buffer.h"

#define EXTENT_UPDATE_DP_INTERVAL_MS 5 * 60 * 1000u

struct workqueue_struct *extent_work_queue;

struct cfs_data_partition *
cfs_data_partition_new(struct cfs_data_partition_view *dp_view, u32 rdma_port)
{
	struct cfs_data_partition *dp;
	u32 i;

	dp = kzalloc(sizeof(*dp), GFP_NOFS);
	if (!dp)
		return NULL;
	dp->follower_addrs = cfs_buffer_new(0);
	if (!dp->follower_addrs) {
		kfree(dp);
		return NULL;
	}
	dp->rdma_follower_addrs = cfs_buffer_new(0);
	if (!dp->rdma_follower_addrs) {
		cfs_buffer_release(dp->follower_addrs);
		kfree(dp);
		return NULL;
	}
	for (i = 1; i < dp_view->members.num; i++) {
		if (cfs_buffer_write(dp->follower_addrs, "%s/",
				     cfs_pr_addr(&dp_view->members.base[i])) <
		    0) {
			cfs_buffer_release(dp->follower_addrs);
			cfs_buffer_release(dp->rdma_follower_addrs);
			kfree(dp);
			return NULL;
		}
		if (cfs_buffer_write(
			    dp->rdma_follower_addrs, "%s/",
			    cfs_pr_addr_rdma(&dp_view->members.base[i], rdma_port)) < 0) {
			cfs_buffer_release(dp->follower_addrs);
			cfs_buffer_release(dp->rdma_follower_addrs);
			kfree(dp);
			return NULL;
		}
	}
	dp->nr_followers = dp_view->members.num ? dp_view->members.num - 1 :
						  127;
	dp->type = dp_view->type;
	dp->id = dp_view->id;
	dp->status = dp_view->status;
	dp->replica_num = dp_view->replica_num;
	sockaddr_storage_array_move(&dp->members, &dp_view->members);
	for (i = 0; i < dp->members.num; i++) {
		if (dp_view->leader &&
		    cfs_addr_cmp(dp_view->leader, &dp->members.base[i]) == 0) {
			dp->leader_idx = i;
			break;
		}
	}
	dp->type = dp_view->type;
	dp->epoch = dp_view->epoch;
	dp->ttl = dp_view->ttl;
	dp->is_recover = dp_view->is_recover;
	dp->is_discard = dp_view->is_discard;
	atomic_set(&dp->refcnt, 1);
	return dp;
}

void cfs_data_partition_release(struct cfs_data_partition *dp)
{
	if (!dp)
		return;
	if (!atomic_dec_and_test(&dp->refcnt))
		return;
	sockaddr_storage_array_clear(&dp->members);
	if (dp->follower_addrs)
		cfs_buffer_release(dp->follower_addrs);
	if (dp->rdma_follower_addrs)
		cfs_buffer_release(dp->rdma_follower_addrs);
	kfree(dp);
}

static void update_dp_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_extent_client *ec = container_of(
		delayed_work, struct cfs_extent_client, update_dp_work);

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(EXTENT_UPDATE_DP_INTERVAL_MS));
	cfs_extent_update_partition(ec);
}

struct cfs_extent_client *
cfs_extent_client_new(struct cfs_mount_info *cmi)
{
	struct cfs_extent_client *ec;
	int ret;

	ec = kzalloc(sizeof(*ec), GFP_NOFS);
	if (!ec)
		return NULL;
	ec->master = cmi->master;
	ec->meta = cmi->meta;
	ec->log = cmi->log;
	ec->enable_rdma = cmi->options->enable_rdma;
	ec->rdma_port = cmi->options->rdma_port;

	hash_init(ec->streams);
	hash_init(ec->data_partitions);
	INIT_LIST_HEAD(&ec->rw_partitions);
	rwlock_init(&ec->lock);
	mutex_init(&ec->select_lock);

	ret = cfs_extent_update_partition(ec);
	if (ret < 0) {
		kfree(ec);
		return ERR_PTR(ret);
	}
	INIT_DELAYED_WORK(&ec->update_dp_work, update_dp_work_cb);
	schedule_delayed_work(&ec->update_dp_work,
			      msecs_to_jiffies(EXTENT_UPDATE_DP_INTERVAL_MS));
	if (ec->enable_rdma) {
		ret = rdma_buffer_new(ec->rdma_port);
		if (ret < 0) {
			printk("error to call rdma_buffer_new\n");
			kfree(ec);
			return ERR_PTR(ret);
		}
	}
	return ec;
}

void cfs_extent_client_release(struct cfs_extent_client *ec)
{
	struct cfs_data_partition *dp;
	struct hlist_node *tmp;
	int i;

	if (!ec)
		return;
	cancel_delayed_work_sync(&ec->update_dp_work);
	hash_for_each_safe(ec->data_partitions, i, tmp, dp, hash) {
		hash_del(&dp->hash);
		cfs_data_partition_release(dp);
	}
	if (ec->enable_rdma) {
		cfs_rdma_clean_sockets_in_exit();
		rdma_buffer_release();
	}
	kfree(ec);
}

int cfs_extent_update_partition(struct cfs_extent_client *ec)
{
	struct cfs_data_partition_view_array dp_views;
	struct cfs_data_partition_view *dp_view;
	struct cfs_data_partition *dp;
	struct hlist_node *tmp;
	u32 i;
	int ret;

	ret = cfs_master_get_data_partitions(ec->master, &dp_views);
	if (ret < 0)
		return ret;
	write_lock(&ec->lock);
	while (!list_empty(&ec->rw_partitions)) {
		dp = list_first_entry(&ec->rw_partitions,
				      struct cfs_data_partition, list);
		list_del(&dp->list);
	}
	hash_for_each_safe(ec->data_partitions, i, tmp, dp, hash) {
		hash_del(&dp->hash);
		cfs_data_partition_release(dp);
	}
	ec->select_dp = NULL;
	ec->nr_rw_partitions = 0;
	for (i = 0; i < dp_views.num; i++) {
		dp_view = &dp_views.base[i];
		dp = cfs_data_partition_new(dp_view, ec->rdma_port);
		if (!dp) {
			ret = -ENOMEM;
			goto unlock;
		}
		hash_add(ec->data_partitions, &dp->hash, dp->id);
		if (dp->status == CFS_DP_STATUS_READWRITE) {
			list_add_tail(&dp->list, &ec->rw_partitions);
			ec->nr_rw_partitions++;
		}
	}

unlock:
	write_unlock(&ec->lock);
	cfs_data_partition_view_array_clear(&dp_views);
	return ret;
}

struct cfs_data_partition *
cfs_extent_get_partition(struct cfs_extent_client *ec, u64 id)
{
	struct cfs_data_partition *dp = NULL;

	read_lock(&ec->lock);
	hash_for_each_possible(ec->data_partitions, dp, hash, id) {
		if (dp->id == id) {
			atomic_inc(&dp->refcnt);
			break;
		}
	}
	read_unlock(&ec->lock);
	return dp;
}

u32 cfs_extent_get_partition_count(struct cfs_extent_client *ec)
{
	u32 nr;

	read_lock(&ec->lock);
	nr = ec->nr_rw_partitions;
	read_unlock(&ec->lock);
	return nr;
}

struct cfs_data_partition *
cfs_extent_select_partition(struct cfs_extent_client *ec)
{
	struct cfs_data_partition *select_dp;
	u32 step;

	read_lock(&ec->lock);
	mutex_lock(&ec->select_lock);
	if (ec->select_dp)
		step = 1;
	else
		step = prandom_u32() % ec->nr_rw_partitions;
	step = max_t(u32, step, 1);

	while (step-- > 0) {
		if (!ec->select_dp)
			ec->select_dp = list_first_entry_or_null(
				&ec->rw_partitions, struct cfs_data_partition,
				list);
		else if (list_is_last(&ec->select_dp->list, &ec->rw_partitions))
			ec->select_dp = list_first_entry_or_null(
				&ec->rw_partitions, struct cfs_data_partition,
				list);
		else
			ec->select_dp = list_next_entry(ec->select_dp, list);
		if (!ec->select_dp)
			break;
	}
	select_dp = ec->select_dp;
	if (select_dp)
		atomic_inc(&select_dp->refcnt);
	mutex_unlock(&ec->select_lock);
	read_unlock(&ec->lock);
	return select_dp;
}

int cfs_extent_module_init(void)
{
	if (extent_work_queue)
		return 0;
	extent_work_queue =
		alloc_workqueue("extent_wq", WQ_UNBOUND | WQ_MEM_RECLAIM, 0);
	if (!extent_work_queue) {
		return -ENOMEM;
	}
	return 0;
}

void cfs_extent_module_exit(void)
{
	if (extent_work_queue) {
		destroy_workqueue(extent_work_queue);
		extent_work_queue = NULL;
	}
}