#include "cfs_meta.h"

#define META_RECV_TIMEOUT_MS 5000u
#define META_UPDATE_MP_INTERVAL_MS 5 * 60 * 1000u

static int do_meta_request_internal(struct cfs_meta_client *mc,
				    struct sockaddr_storage *host,
				    struct cfs_packet *packet)
{
	struct cfs_socket *sock;
	int ret;

	ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, host, mc->log, &sock);
	if (ret < 0) {
		cfs_log_error(mc->log, "socket(%s) create error %d\n",
			      cfs_pr_addr(host), ret);
		return ret;
	}

	ret = cfs_socket_set_recv_timeout(sock, META_RECV_TIMEOUT_MS);
	if (ret < 0) {
		cfs_log_error(mc->log, "socket(%s) set recv timeout error %d\n",
			      cfs_pr_addr(host), ret);
		cfs_socket_release(sock, true);
		return ret;
	}

	ret = cfs_socket_send_packet(sock, packet);
	if (ret < 0) {
		cfs_log_error(mc->log, "socket(%s) send packet error %d\n",
			      cfs_pr_addr(host), ret);
		cfs_socket_release(sock, true);
		return ret;
	}

	ret = cfs_socket_recv_packet(sock, packet);
	if (ret < 0) {
		cfs_log_error(mc->log, "socket(%s) recv packet error %d\n",
			      cfs_pr_addr(host), ret);
		cfs_socket_release(sock, true);
		return ret;
	}
	cfs_socket_release(sock, false);
	return ret;
}

/**
 * @return 0 on success, < 0 if client request failed
 */
static int do_meta_request(struct cfs_meta_client *mc,
			   struct cfs_meta_partition *mp,
			   struct cfs_packet *packet)
{
	size_t max = mp->members.num;
	size_t i = min(mp->leader_idx, mp->members.num - 1);
	int ret = -1;

	while (max-- > 0) {
		struct sockaddr_storage *host;

		host = &mp->members.base[i];
		i = (i + 1) % mp->members.num;

		ret = do_meta_request_internal(mc, host, packet);
		if (ret < 0)
			continue;

		if (packet->request.hdr.req_id != packet->reply.hdr.req_id ||
		    packet->request.hdr.opcode != packet->reply.hdr.opcode) {
			cfs_log_error(
				mc->log,
				"reply packet mismatch with request, req(%llu,%llu), opcode(0x%x,0x%x)\n",
				packet->request.hdr.req_id,
				packet->reply.hdr.req_id,
				packet->request.hdr.opcode,
				packet->reply.hdr.opcode);
			return -EBADMSG;
		}

		if (packet->reply.hdr.result_code == CFS_STATUS_AGAIN ||
		    packet->reply.hdr.result_code == CFS_STATUS_ERR)
			continue;

		return 0;
	}
	return ret;
}

static int meta_parition_cmp(const void *a, const void *b, void *udata)
{
	struct cfs_meta_partition *mp1 = (void *)((*(uintptr_t *)a));
	struct cfs_meta_partition *mp2 = (void *)((*(uintptr_t *)b));

	(void)udata;

	if (mp1->start_ino == mp2->start_ino)
		return 0;
	else if (mp1->start_ino > mp2->start_ino)
		return 1;
	else
		return -1;
}

static int cfs_meta_update_partition(struct cfs_meta_client *mc)
{
	struct cfs_volume_view vol_view;
	struct cfs_meta_partition_view *mp_view;
	struct cfs_meta_partition *mp;
	struct hlist_node *tmp;
	uintptr_t ptr;
	size_t i;
	int ret;

	ret = cfs_master_get_volume(mc->master, &vol_view);
	if (ret < 0) {
		cfs_log_error(mc->log, "get meta partitions error %d\n", ret);
		return ret;
	}
	write_lock(&mc->lock);
	btree_clear(mc->partition_ranges);
	while (!list_empty(&mc->rw_partitions)) {
		mp = list_first_entry(&mc->rw_partitions,
				      struct cfs_meta_partition, list);
		list_del(&mp->list);
	}
	hash_for_each_safe(mc->paritions, i, tmp, mp, hash) {
		hash_del(&mp->hash);
		cfs_meta_partition_release(mp);
	}
	mc->select_mp = NULL;
	mc->nr_rw_partitions = 0;

	for (i = 0; i < vol_view.meta_partitions.num; i++) {
		mp_view = &vol_view.meta_partitions.base[i];
		mp = cfs_meta_partition_new(mp_view);
		if (!mp) {
			cfs_pr_err("oom\n");
			ret = -ENOMEM;
			goto unlock;
		}
		ptr = (uintptr_t)mp;
		btree_set(mc->partition_ranges, &ptr);
		if (btree_oom(mc->partition_ranges)) {
			cfs_pr_err("oom\n");
			cfs_meta_partition_release(mp);
			ret = -ENOMEM;
			goto unlock;
		}
		hash_add(mc->paritions, &mp->hash, mp->id);
		if (mp->status == CFS_MP_STATUS_READWRITE) {
			list_add(&mp->list, &mc->rw_partitions);
			mc->nr_rw_partitions++;
		}
	}

unlock:
	write_unlock(&mc->lock);
	cfs_volume_view_clear(&vol_view);
	return 0;
}

static void meta_update_partition_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_meta_client *mc = container_of(
		delayed_work, struct cfs_meta_client, update_mp_work);

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(META_UPDATE_MP_INTERVAL_MS));
	cfs_meta_update_partition(mc);
}

#define META_ITEM_COUNT_PRE_BTREE_NODE 1024
struct cfs_meta_client *cfs_meta_client_new(struct cfs_master_client *master,
					    const char *vol_name,
					    struct cfs_log *log)
{
	struct cfs_meta_client *mc;
	int ret;

	mc = kzalloc(sizeof(*mc), GFP_NOFS);
	if (!mc)
		return ERR_PTR(-ENOMEM);
	mc->volume = kstrdup(vol_name, GFP_NOFS);
	if (!mc->volume) {
		ret = -ENOMEM;
		goto err_volume;
	}
	mc->log = log;
	mc->master = master;
	rwlock_init(&mc->lock);
	hash_init(mc->paritions);
	mc->partition_ranges = btree_new(sizeof(uintptr_t),
					 META_ITEM_COUNT_PRE_BTREE_NODE,
					 meta_parition_cmp, NULL);
	if (!mc->partition_ranges) {
		ret = -ENOMEM;
		goto err_btree;
	}
	INIT_LIST_HEAD(&mc->rw_partitions);
	mutex_init(&mc->select_lock);
	hash_init(mc->uniqid_ranges);
	mutex_init(&mc->uniqid_lock);

	ret = cfs_meta_update_partition(mc);
	if (ret < 0) {
		cfs_pr_err("update partition error %d\n", ret);
		goto err_update;
	}
	INIT_DELAYED_WORK(&mc->update_mp_work, meta_update_partition_work_cb);
	schedule_delayed_work(&mc->update_mp_work,
			      msecs_to_jiffies(META_UPDATE_MP_INTERVAL_MS));
	return mc;

err_update:
	btree_free(mc->partition_ranges);
err_btree:
	kfree(mc->volume);
err_volume:
	kfree(mc);
	return ERR_PTR(ret);
}

void cfs_meta_client_release(struct cfs_meta_client *mc)
{
	struct cfs_meta_partition *mp;
	struct cfs_uniqid_range *uniqid_range;
	struct hlist_node *tmp;
	int i;

	if (!mc)
		return;
	cancel_delayed_work_sync(&mc->update_mp_work);
	hash_for_each_safe(mc->paritions, i, tmp, mp, hash) {
		hash_del(&mp->hash);
		cfs_meta_partition_release(mp);
	}
	btree_free(mc->partition_ranges);
	hash_for_each_safe(mc->uniqid_ranges, i, tmp, uniqid_range, hash) {
		hash_del(&uniqid_range->hash);
		cfs_uniqid_range_release(uniqid_range);
	}
	kfree(mc);
}

struct meta_get_partition_context {
	u64 ino;
	struct cfs_meta_partition *found;
};

static bool meta_get_parition_iter(const void *item, void *udata)
{
	struct cfs_meta_partition *mp = (void *)((*(uintptr_t *)item));
	struct meta_get_partition_context *ctx = udata;

	if (mp->start_ino <= ctx->ino && ctx->ino <= mp->end_ino)
		ctx->found = mp;
	else
		ctx->found = NULL;
	return false;
}

/**
 * Caller must holes read lock.
 */
static struct cfs_meta_partition *
cfs_meta_get_partition_by_inode(struct cfs_meta_client *mc, u64 ino)
{
	struct cfs_meta_partition pivot = { .start_ino = ino };
	struct meta_get_partition_context ctx = { .ino = ino };
	uintptr_t ptr;

	ptr = (uintptr_t)&pivot;
	btree_descend(mc->partition_ranges, &ptr, meta_get_parition_iter, &ctx);
	return ctx.found;
}

/**
 * Caller must holes read lock.
 */
static struct cfs_meta_partition *
cfs_meta_select_partition(struct cfs_meta_client *mc)
{
	struct cfs_meta_partition *select_mp;
	u32 step;

	mutex_lock(&mc->select_lock);
	if (mc->select_mp)
		step = 1;
	else
		step = prandom_u32() % mc->nr_rw_partitions;
	step = max_t(u32, step, 1);

	while (step-- > 0) {
		if (!mc->select_mp)
			mc->select_mp = list_first_entry_or_null(
				&mc->rw_partitions, struct cfs_meta_partition,
				list);
		else if (list_is_last(&mc->select_mp->list, &mc->rw_partitions))
			mc->select_mp = list_first_entry_or_null(
				&mc->rw_partitions, struct cfs_meta_partition,
				list);
		else
			mc->select_mp = list_next_entry(mc->select_mp, list);
		if (!mc->select_mp)
			break;
	}
	select_mp = mc->select_mp;
	mutex_unlock(&mc->select_lock);
	return select_mp;
}

#define META_UNIQID_NUM 5000
static int cfs_meta_get_uniqid_internal(struct cfs_meta_client *mc,
					struct cfs_meta_partition *mp,
					u64 *uniqid)
{
	u8 op = CFS_OP_UNIQID_GET;
	struct cfs_packet *packet;
	struct cfs_packet_uniqid_request *request_data;
	struct cfs_packet_uniqid_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;
	request_data = &packet->request.data.uniqid;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->num = META_UNIQID_NUM;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.uniqid;
	*uniqid = reply_data->start;
	cfs_packet_release(packet);
	return 0;
}

static int cfs_meta_get_uniqid(struct cfs_meta_client *mc,
			       struct cfs_meta_partition *mp, u64 *uniqid)
{
	struct cfs_uniqid_range *range;
	u64 start;
	int ret;

	mutex_lock(&mc->uniqid_lock);
	hash_for_each_possible(mc->uniqid_ranges, range, hash, mp->id) {
		if (range->pid == mp->id)
			break;
	}
	if (range) {
		if (cfs_uniqid_range_next_id(range, uniqid)) {
			mutex_unlock(&mc->uniqid_lock);
			return 0;
		}
		hash_del(&range->hash);
		cfs_uniqid_range_release(range);
	}
	ret = cfs_meta_get_uniqid_internal(mc, mp, &start);
	if (ret != 0) {
		mutex_unlock(&mc->uniqid_lock);
		return ret < 0 ? ret : -ret;
	}
	range = cfs_uniqid_range_new(mp->id, start, start + META_UNIQID_NUM);
	if (!range) {
		mutex_unlock(&mc->uniqid_lock);
		return -ENOMEM;
	}
	hash_add(mc->uniqid_ranges, &range->hash, mp->id);
	cfs_uniqid_range_next_id(range, uniqid);
	mutex_unlock(&mc->uniqid_lock);
	return 0;
}

#if 0
/**
 * @param quota_infos [out]
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_get_quota_internal(struct cfs_meta_client *mc,
				       struct cfs_meta_partition *mp, u64 ino,
				       struct cfs_quota_info_array *quota_infos)
{
	u8 op = CFS_OP_QUOTA_INODE_GET;
	struct cfs_packet *packet;
	struct cfs_packet_gquota_request *request_data;
	struct cfs_packet_gquota_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;
	request_data = &packet->request.data.gquota;
	request_data->pid = mp->id;
	request_data->ino = ino;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.gquota;
	cfs_quota_info_array_move(quota_infos, &reply_data->quota_infos);
	cfs_packet_release(packet);
	return 0;
}
#endif

/**
 * @param target [in] option
 * @param quota_infos [in] option
 * @param iinfo [out]
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_icreate_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp,
				     umode_t mode, uid_t uid, gid_t gid,
				     const char *target,
				     struct cfs_quota_info_array *quota_infos,
				     struct cfs_packet_inode **iinfo)
{
	u8 op = quota_infos ? CFS_OP_QUOTA_INODE_CREATE : CFS_OP_INODE_CREATE;
	struct cfs_packet *packet;
	struct cfs_packet_icreate_request *request_data;
	struct cfs_packet_icreate_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;
	request_data = &packet->request.data.icreate;
	request_data->vol = mc->volume;
	request_data->target = target;
	request_data->pid = mp->id;
	request_data->mode = mode;
	request_data->uid = uid;
	request_data->gid = gid;
	if (quota_infos)
		request_data->quota_infos = *quota_infos;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.icreate;
	if (iinfo) {
		if (S_ISLNK(reply_data->info->mode))
			reply_data->info->size =
				strlen(reply_data->info->target);
		*iinfo = cfs_move(reply_data->info, NULL);
	}
	cfs_packet_release(packet);
	return 0;
}

static int cfs_meta_iget_internal(struct cfs_meta_client *mc,
				  struct cfs_meta_partition *mp, u64 inode,
				  struct cfs_packet_inode **iinfo)
{
	u8 op = CFS_OP_INODE_GET;
	struct cfs_packet *packet;
	struct cfs_packet_iget_request *request_data;
	struct cfs_packet_iget_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.iget;
	request_data->vol = mc->volume;
	request_data->ino = inode;
	request_data->pid = mp->id;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.iget;
	if (iinfo) {
		if (S_ISLNK(reply_data->info->mode) && reply_data->info->target)
			reply_data->info->size =
				strlen(reply_data->info->target);
		*iinfo = cfs_move(reply_data->info, NULL);
	}

	cfs_packet_release(packet);
	return 0;
}

static int cfs_meta_batch_iget_internal(
	struct cfs_meta_client *mc, struct cfs_meta_partition *mp,
	struct u64_array *ino_vec, struct cfs_packet_inode_ptr_array *iinfo_vec)
{
	u8 op = CFS_OP_INODE_BATCH_GET;
	struct cfs_packet *packet;
	struct cfs_packet_batch_iget_request *request_data;
	struct cfs_packet_batch_iget_reply *reply_data;
	size_t i;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.batch_iget;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino_vec = *ino_vec;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.batch_iget;
	cfs_packet_inode_ptr_array_move(iinfo_vec, &reply_data->info_vec);
	for (i = 0; i < iinfo_vec->num; i++) {
		if (S_ISLNK(iinfo_vec->base[i]->mode) &&
		    iinfo_vec->base[i]->target)
			iinfo_vec->base[i]->size =
				strlen(iinfo_vec->base[i]->target);
	}
	cfs_packet_release(packet);
	return 0;
}

static int cfs_meta_lookup_internal(struct cfs_meta_client *mc,
				    struct cfs_meta_partition *mp,
				    u64 parent_ino, struct qstr *name, u64 *ino,
				    umode_t *mode)
{
	u8 op = CFS_OP_LOOKUP;
	struct cfs_packet *packet;
	struct cfs_packet_lookup_request *request_data;
	struct cfs_packet_lookup_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.ilookup;
	request_data->vol_name = mc->volume;
	request_data->parent_ino = parent_ino;
	request_data->pid = mp->id;
	request_data->name = name;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.ilookup;
	if (ino)
		*ino = reply_data->ino;
	if (mode)
		*mode = reply_data->mode;

	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_dcreate_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp,
				     u64 parent_ino, struct qstr *name, u64 ino,
				     umode_t mode,
				     struct cfs_quota_info_array *quota_infos)
{
	u8 op = quota_infos ? CFS_OP_QUOTA_DENTRY_CREATE : CFS_OP_DENTRY_CREATE;
	struct cfs_packet *packet;
	struct cfs_packet_dcreate_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.dcreate;
	request_data->vol_name = mc->volume;
	request_data->parent_ino = parent_ino;
	request_data->pid = mp->id;
	request_data->name = name;
	request_data->ino = ino;
	request_data->mode = mode;
	if (quota_infos)
		request_data->quota_infos = *quota_infos;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_ddelete_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp,
				     u64 parent_ino, struct qstr *name,
				     u64 *res_ino)
{
	u8 op = CFS_OP_DENTRY_DELETE;
	struct cfs_packet *packet;
	struct cfs_packet_ddelete_request *request_data;
	struct cfs_packet_ddelete_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.ddelete;
	request_data->vol_name = mc->volume;
	request_data->parent_ino = parent_ino;
	request_data->pid = mp->id;
	request_data->name = name;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.ddelete;
	if (res_ino)
		*res_ino = reply_data->ino;

	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_dupdate_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp,
				     u64 parent_ino, struct qstr *name, u64 ino,
				     u64 *res_ino)
{
	u8 op = CFS_OP_DENTRY_UPDATE;
	struct cfs_packet *packet;
	struct cfs_packet_dupdate_request *request_data;
	struct cfs_packet_dupdate_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.dupdate;
	request_data->vol_name = mc->volume;
	request_data->parent_ino = parent_ino;
	request_data->pid = mp->id;
	request_data->name = name;
	request_data->ino = ino;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.dupdate;
	if (res_ino)
		*res_ino = reply_data->ino;

	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_ilink_internal(struct cfs_meta_client *mc,
				   struct cfs_meta_partition *mp, u64 ino,
				   struct cfs_packet_inode **iinfo)
{
	u8 op = CFS_OP_INODE_LINK;
	struct cfs_packet *packet;
	struct cfs_packet_ilink_request *request_data;
	struct cfs_packet_ilink_reply *reply_data;
	u64 uniqid;
	int ret;

	ret = cfs_meta_get_uniqid(mc, mp, &uniqid);
	if (ret < 0)
		return ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.ilink;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->uniqid = uniqid;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.ilink;
	if (iinfo) {
		if (S_ISLNK(reply_data->info->mode))
			reply_data->info->size =
				strlen(reply_data->info->target);
		*iinfo = cfs_move(reply_data->info, NULL);
	}
	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_iunlink_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp, u64 ino,
				     struct cfs_packet_inode **iinfo)
{
	u8 op = CFS_OP_INODE_UNLINK;
	struct cfs_packet *packet;
	struct cfs_packet_iunlink_request *request_data;
	struct cfs_packet_iunlink_reply *reply_data;
	u64 uniqid;
	int ret;

	ret = cfs_meta_get_uniqid(mc, mp, &uniqid);
	if (ret < 0)
		return ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.iunlink;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->uniqid = uniqid;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.iunlink;
	if (iinfo) {
		if (S_ISLNK(reply_data->info->mode))
			reply_data->info->size =
				strlen(reply_data->info->target);
		*iinfo = cfs_move(reply_data->info, NULL);
	}

	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_ievict_internal(struct cfs_meta_client *mc,
				    struct cfs_meta_partition *mp, u64 ino)
{
	u8 op = CFS_OP_INODE_EVICT;
	struct cfs_packet *packet;
	struct cfs_packet_ievict_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.ievict;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	cfs_packet_release(packet);
	return ret;
}

static int cfs_meta_readdir_internal(struct cfs_meta_client *mc,
				     struct cfs_meta_partition *mp,
				     u64 parent_ino, const char *from,
				     u64 limit,
				     struct cfs_packet_dentry_array *dentries)
{
	u8 op = CFS_OP_READDIR_LIMIT;
	struct cfs_packet *packet;
	struct cfs_packet_readdir_request *request_data;
	struct cfs_packet_readdir_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.readdir;
	request_data->vol_name = mc->volume;
	request_data->parent_ino = parent_ino;
	request_data->pid = mp->id;
	request_data->marker = from;
	request_data->limit = limit;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.readdir;
	if (dentries)
		cfs_packet_dentry_array_move(dentries, &reply_data->children);

	cfs_packet_release(packet);
	return 0;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_set_attr_internal(struct cfs_meta_client *mc,
				      struct cfs_meta_partition *mp, u64 ino,
				      struct iattr *attr)
{
	u8 op = CFS_OP_ATTR_SET;
	struct cfs_packet *packet;
	struct cfs_packet_sattr_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.sattr;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->mode = attr->ia_mode;
	request_data->uid = __kuid_val(attr->ia_uid);
	request_data->gid = __kgid_val(attr->ia_gid);
	request_data->modify_time = attr->ia_mtime;
	request_data->access_time = attr->ia_atime;
	request_data->valid = ia_valid_to_u32(attr->ia_valid);

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0)
		goto out;
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0)
		goto out;

out:
	cfs_packet_release(packet);
	return ret;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_set_xattr_internal(struct cfs_meta_client *mc,
				       struct cfs_meta_partition *mp, u64 ino,
				       const char *name, const char *value,
				       size_t len, int flags)
{
	u8 op = flags & XATTR_CREATE ? CFS_OP_XATTR_SET : CFS_OP_XATTR_UPDATE;
	struct cfs_packet *packet;
	struct cfs_packet_sxattr_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.sxattr;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->key = name;
	request_data->value = value;
	request_data->value_len = len;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	cfs_packet_release(packet);
	return ret;
}

/**
 * @param out_len out, the size of name list
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_get_xattr_internal(struct cfs_meta_client *mc,
				       struct cfs_meta_partition *mp, u64 ino,
				       const char *name, char *value,
				       size_t size, size_t *out_len)
{
	u8 op = CFS_OP_XATTR_GET;
	struct cfs_packet *packet;
	struct cfs_packet_gxattr_request *request_data;
	struct cfs_packet_gxattr_reply *reply_data;
	size_t n;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.gxattr;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->key = name;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.gxattr;
	n = strlen(reply_data->value) + 1;
	if (value) {
		if (n > size) {
			ret = -ERANGE;
		} else {
			memcpy(value, reply_data->value, n);
		}
	}
	if (out_len)
		*out_len = n;

	cfs_packet_release(packet);
	return ret;
}

/**
 * @param out_len out, the size of name list
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_list_xattr_internal(struct cfs_meta_client *mc,
					struct cfs_meta_partition *mp, u64 ino,
					char *names, size_t size,
					size_t *out_len)
{
	u8 op = CFS_OP_XATTR_LIST;
	struct cfs_packet *packet;
	struct cfs_packet_lxattr_request *request_data;
	struct cfs_packet_lxattr_reply *reply_data;
	size_t i;
	size_t total_len;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.lxattr;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.lxattr;
	total_len = 0;
	for (i = 0; i < reply_data->xattrs.num; i++) {
		size_t n = strlen(reply_data->xattrs.base[i]) + 1;
		if (names) {
			if (total_len + n > size) {
				ret = -ERANGE;
				break;
			}
			memcpy(names + total_len, reply_data->xattrs.base[i],
			       n);
		}
		total_len += n;
	}
	if (out_len)
		*out_len = total_len;

	cfs_packet_release(packet);
	return ret;
}

/**
 * @return 0 on success, < 0 on client request failed, > 0 on server reply failed
 */
static int cfs_meta_remove_xattr_internal(struct cfs_meta_client *mc,
					  struct cfs_meta_partition *mp,
					  u64 ino, const char *name)
{
	u8 op = CFS_OP_XATTR_REMOVE;
	struct cfs_packet *packet;
	struct cfs_packet_rxattr_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.rxattr;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->key = name;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	cfs_packet_release(packet);
	return ret;
}

/**
 * create regular files, directories, symlinks.
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_create(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, umode_t mode, uid_t uid, gid_t gid,
		    const char *target,
		    struct cfs_quota_info_array *quota_infos,
		    struct cfs_packet_inode **iinfo)
{
	struct cfs_meta_partition *parent_mp;
	struct cfs_meta_partition *mp;
	u32 retry;
	int ret = 0;

	read_lock(&mc->lock);
	parent_mp = cfs_meta_get_partition_by_inode(mc, parent_ino);
	if (!parent_mp) {
		ret = -ENOENT;
		goto unlock;
	}

	retry = mc->nr_rw_partitions;
	while (retry-- > 0) {
		mp = cfs_meta_select_partition(mc);
		if (!mp) {
			ret = -ENOENT;
			goto unlock;
		}
		ret = cfs_meta_icreate_internal(mc, mp, mode, uid, gid, target,
						quota_infos, iinfo);
		if (ret == 0)
			break;
	}
	if (ret != 0) {
		cfs_log_error(mc->log, "create inode error %d\n", ret);
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

	ret = cfs_meta_dcreate_internal(mc, parent_mp, parent_ino, name,
					(*iinfo)->ino, mode, quota_infos);
	if (ret != 0) {
		cfs_log_error(mc->log, "create dentry error %d\n", ret);
		cfs_packet_inode_release(*iinfo);
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @param parent_ino in, parent directory inode of target dentry
 * @param name in, target dentry name
 * @param ino in, source inode
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_link(struct cfs_meta_client *mc, u64 parent_ino, struct qstr *name,
		  u64 ino, struct cfs_packet_inode **iinfop)
{
	struct cfs_meta_partition *parent_mp;
	struct cfs_meta_partition *mp;
	struct cfs_packet_inode *iinfo;
	int ret;

	read_lock(&mc->lock);
	parent_mp = cfs_meta_get_partition_by_inode(mc, parent_ino);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!parent_mp || !mp) {
		ret = -ENOENT;
		goto unlock;
	}

	ret = cfs_meta_ilink_internal(mc, mp, ino, &iinfo);
	if (ret != 0) {
		cfs_log_error(mc->log, "create inode error %d\n", ret);
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	ret = cfs_meta_dcreate_internal(mc, parent_mp, parent_ino, name,
					iinfo->ino, iinfo->mode, NULL);
	if (ret > 0) {
		cfs_log_error(mc->log, "create dentry error %d\n", ret);
		cfs_packet_inode_release(iinfo);
		cfs_meta_iunlink_internal(mc, mp, ino, NULL);
		goto unlock;
	} else if (ret < 0) {
		cfs_log_error(mc->log, "create dentry error %d\n", ret);
		cfs_packet_inode_release(iinfo);
		goto unlock;
	}
	if (iinfop)
		*iinfop = iinfo;

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_delete(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, bool is_dir, u64 *ret_ino)
{
	struct cfs_meta_partition *mp;
	u64 ino;
	int ret;

	if (is_dir) {
		struct cfs_packet_inode *iinfo;
		ret = cfs_meta_lookup(mc, parent_ino, name, &iinfo);
		if (ret < 0)
			return ret;
		if (iinfo->nlink > 2) {
			cfs_packet_inode_release(iinfo);
			return -ENOTEMPTY;
		}
		cfs_packet_inode_release(iinfo);
	}
	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, parent_ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_ddelete_internal(mc, mp, parent_ino, name, &ino);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_iunlink_internal(mc, mp, ino, NULL);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	ret = cfs_meta_ievict_internal(mc, mp, ino);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	if (ret_ino)
		*ret_ino = ino;

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_rename(struct cfs_meta_client *mc, u64 src_parent_ino,
		    struct qstr *src_name, u64 dst_parent_ino,
		    struct qstr *dst_name, bool over_written)
{
	struct cfs_meta_partition *src_parent_mp, *dst_parent_mp;
	struct cfs_meta_partition *src_mp;
	u64 src_ino, old_ino = 0;
	umode_t mode;
	int ret;

	read_lock(&mc->lock);
	src_parent_mp = cfs_meta_get_partition_by_inode(mc, src_parent_ino);
	dst_parent_mp = cfs_meta_get_partition_by_inode(mc, dst_parent_ino);
	if (!src_parent_mp || !dst_parent_mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_lookup_internal(mc, src_parent_mp, src_parent_ino,
				       src_name, &src_ino, &mode);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	src_mp = cfs_meta_get_partition_by_inode(mc, src_ino);
	if (!src_mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_ilink_internal(mc, src_mp, src_ino, NULL);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	ret = cfs_meta_dcreate_internal(mc, dst_parent_mp, dst_parent_ino,
					dst_name, src_ino, mode, NULL);
	if (ret == EEXIST && (S_ISREG(mode) || S_ISLNK(mode)) && over_written)
		ret = cfs_meta_dupdate_internal(mc, dst_parent_mp,
						dst_parent_ino, dst_name,
						src_ino, &old_ino);
	if (ret > 0) {
		cfs_meta_iunlink_internal(mc, src_parent_mp, src_ino, NULL);
		ret = -ret;
		goto unlock;
	} else if (ret < 0)
		goto unlock;

	ret = cfs_meta_ddelete_internal(mc, src_parent_mp, src_parent_ino,
					src_name, NULL);
	if (ret > 0) {
		int old_ret = ret;
		if (old_ino == 0)
			ret = cfs_meta_ddelete_internal(mc, dst_parent_mp,
							dst_parent_ino,
							dst_name, NULL);
		else
			ret = cfs_meta_dupdate_internal(mc, dst_parent_mp,
							dst_parent_ino,
							dst_name, old_ino,
							NULL);
		if (ret >= 0)
			cfs_meta_iunlink_internal(mc, src_parent_mp, src_ino,
						  NULL);
		ret = -old_ret;
		goto unlock;
	} else if (ret < 0)
		goto unlock;

	cfs_meta_iunlink_internal(mc, src_parent_mp, src_ino, NULL);
	if (old_ino != 0) {
		struct cfs_meta_partition *old_mp;
		old_mp = cfs_meta_get_partition_by_inode(mc, old_ino);
		if (old_mp) {
			cfs_meta_iunlink_internal(mc, old_mp, old_ino, NULL);
			cfs_meta_ievict_internal(mc, old_mp, old_ino);
		}
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_lookup(struct cfs_meta_client *mc, u64 parent_ino,
		    struct qstr *name, struct cfs_packet_inode **iinfo)
{
	struct cfs_meta_partition *mp;
	u64 ino;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, parent_ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_lookup_internal(mc, mp, parent_ino, name, &ino, NULL);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_iget_internal(mc, mp, ino, iinfo);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

int cfs_meta_lookup_path(struct cfs_meta_client *mc, const char *path,
			 struct cfs_packet_inode **iinfo)
{
	struct cfs_meta_partition *mp;
	const char *ch;
	struct qstr name;
	u64 ino = 1;
	int ret;

	read_lock(&mc->lock);
	while (path) {
		ch = strchr(path, '/');
		name.len = ch ? ch - path : strlen(path);
		name.name = path;

		if (strncmp(name.name, ".", name.len) == 0 ||
		    strncmp(name.name, "..", name.len) == 0) {
			ret = -EINVAL;
			goto unlock;
		}

		if (name.len > 0) {
			mp = cfs_meta_get_partition_by_inode(mc, ino);
			if (!mp) {
				ret = -ENOENT;
				goto unlock;
			}
			ret = cfs_meta_lookup_internal(mc, mp, ino, &name, &ino,
						       NULL);
			if (ret != 0) {
				ret = ret < 0 ? ret : -ret;
				goto unlock;
			}
		}
		path = ch ? ch + 1 : NULL;
	}

	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_iget_internal(mc, mp, ino, iinfo);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_get(struct cfs_meta_client *mc, u64 ino,
		 struct cfs_packet_inode **iinfo)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_iget_internal(mc, mp, ino, iinfo);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

struct batch_iget_task {
	struct work_struct work;
	struct completion done;
	struct hlist_node hash;
	struct cfs_meta_client *mc;
	struct cfs_meta_partition *mp;
	struct u64_array ino_vec;
	struct cfs_packet_inode_ptr_array iinfo_vec;
};

static void batch_iget_work_cb(struct work_struct *work)
{
	struct batch_iget_task *task =
		container_of(work, struct batch_iget_task, work);

	cfs_meta_batch_iget_internal(task->mc, task->mp, &task->ino_vec,
				     &task->iinfo_vec);
	complete(&task->done);
}

static struct batch_iget_task *
batch_iget_task_new(struct cfs_meta_client *mc, struct cfs_meta_partition *mp,
		    size_t ino_num)
{
	struct batch_iget_task *task;

	task = kzalloc(sizeof(*task), GFP_NOFS);
	if (!task)
		return NULL;
	task->mc = mc;
	task->mp = mp;
	INIT_WORK(&task->work, batch_iget_work_cb);
	init_completion(&task->done);
	u64_array_init(&task->ino_vec, ino_num);
	return task;
}

static void batch_iget_task_release(struct batch_iget_task *task)
{
	if (!task)
		return;
	u64_array_clear(&task->ino_vec);
	cfs_packet_inode_ptr_array_clear(&task->iinfo_vec);
	kfree(task);
}

#define BATCH_GET_TASK_BUCKET 32

/**
 * @param iinfo_vec [out]
 */
int cfs_meta_batch_get(struct cfs_meta_client *mc, struct u64_array *ino_vec,
		       struct cfs_packet_inode_ptr_array *iinfo_vec)
{
	struct cfs_meta_partition *mp;
	struct hlist_head tasks[BATCH_GET_TASK_BUCKET];
	struct batch_iget_task *task;
	struct hlist_node *tmp;
	size_t i;
	int ret;

	hash_init(tasks);
	read_lock(&mc->lock);
	for (i = 0; i < ino_vec->num; i++) {
		mp = cfs_meta_get_partition_by_inode(mc, ino_vec->base[i]);
		if (!mp)
			continue;
		hash_for_each_possible(tasks, task, hash, mp->id) {
			if (task->mp == mp)
				break;
		}
		if (!task) {
			task = batch_iget_task_new(mc, mp, ino_vec->num);
			if (!task) {
				ret = -ENOMEM;
				goto unlock;
			}
			hash_add(tasks, &task->hash, mp->id);
		}
		task->ino_vec.base[task->ino_vec.num++] = ino_vec->base[i];
	}

	hash_for_each(tasks, i, task, hash) {
		schedule_work(&task->work);
	}
	hash_for_each(tasks, i, task, hash) {
		wait_for_completion(&task->done);
	}

	ret = cfs_packet_inode_ptr_array_init(iinfo_vec, ino_vec->num);
	if (ret < 0)
		goto unlock;
	hash_for_each(tasks, i, task, hash) {
		while (task->iinfo_vec.num-- > 0) {
			iinfo_vec->base[iinfo_vec->num++] =
				task->iinfo_vec.base[task->iinfo_vec.num];
		}
	}

unlock:
	read_unlock(&mc->lock);
	hash_for_each_safe(tasks, i, tmp, task, hash) {
		hash_del(&task->hash);
		batch_iget_task_release(task);
	}
	return ret;
}

/**
 * @param dentries out
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_readdir(struct cfs_meta_client *mc, u64 parent_ino,
		     const char *from, u64 limit,
		     struct cfs_packet_dentry_array *dentries)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, parent_ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_readdir_internal(mc, mp, parent_ino, from, limit,
					dentries);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_set_attr(struct cfs_meta_client *mc, u64 ino, struct iattr *attr)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_set_attr_internal(mc, mp, ino, attr);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return 0 on success, < 0 on failed
 */
int cfs_meta_set_xattr(struct cfs_meta_client *mc, u64 ino, const char *name,
		       const void *value, size_t value_len, int flags)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_set_xattr_internal(mc, mp, ino, name, value, value_len,
					  flags);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return >= 0 on success, < 0 on failed
 */
ssize_t cfs_meta_get_xattr(struct cfs_meta_client *mc, u64 ino,
			   const char *name, void *value, size_t size)
{
	struct cfs_meta_partition *mp;
	size_t out_len;
	ssize_t ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_get_xattr_internal(mc, mp, ino, name, value, size,
					  &out_len);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	ret = out_len;

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return >= 0 on success, < 0 on failed
 */
ssize_t cfs_meta_list_xattr(struct cfs_meta_client *mc, u64 ino, char *names,
			    size_t size)
{
	struct cfs_meta_partition *mp;
	size_t out_len;
	ssize_t ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_list_xattr_internal(mc, mp, ino, names, size, &out_len);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}
	ret = out_len;

unlock:
	read_unlock(&mc->lock);
	return ret;
}

/**
 * @return >= 0 on success, < 0 on failed
 */
int cfs_meta_remove_xattr(struct cfs_meta_client *mc, u64 ino, const char *name)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_remove_xattr_internal(mc, mp, ino, name);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

static int cfs_meta_list_extent_internal(
	struct cfs_meta_client *mc, struct cfs_meta_partition *mp, u64 ino,
	u64 *gen, u64 *size, struct cfs_packet_extent_array *extents)
{
	u8 op = CFS_OP_EXTENT_LIST;
	struct cfs_packet *packet;
	struct cfs_packet_lextent_request *request_data;
	struct cfs_packet_lextent_reply *reply_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.lextent;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	reply_data = &packet->reply.data.lextent;
	if (gen)
		*gen = reply_data->generation;
	if (size)
		*size = reply_data->size;
	if (extents)
		cfs_packet_extent_array_move(extents, &reply_data->extents);
	cfs_packet_release(packet);
	return 0;
}

/**
 * @param gen [out]
 * @param size [out]
 * @param extents [out]
 * @return >= 0 on success, < 0 on failed
 */
int cfs_meta_list_extent(struct cfs_meta_client *mc, u64 ino, u64 *gen,
			 u64 *size, struct cfs_packet_extent_array *extents)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_list_extent_internal(mc, mp, ino, gen, size, extents);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

static int
cfs_meta_append_extent_internal(struct cfs_meta_client *mc,
				struct cfs_meta_partition *mp, u64 ino,
				struct cfs_packet_extent *extent,
				struct cfs_packet_extent_array *discard)
{
	u8 op = CFS_OP_EXTENT_ADD_WITH_CHECK;
	struct cfs_packet *packet;
	struct cfs_packet_aextent_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.aextent;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->extent = *extent;
	if (discard)
		request_data->discard_extents = *discard;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0) {
		cfs_packet_release(packet);
		return ret;
	}
	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0) {
		cfs_packet_release(packet);
		return ret;
	}
	cfs_packet_release(packet);
	return 0;
}

int cfs_meta_append_extent(struct cfs_meta_client *mc, u64 ino,
			   struct cfs_packet_extent *extent,
			   struct cfs_packet_extent_array *discard_extents)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_append_extent_internal(mc, mp, ino, extent,
					      discard_extents);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}

static int cfs_meta_truncate_internal(struct cfs_meta_client *mc,
				      struct cfs_meta_partition *mp, u64 ino,
				      loff_t size)
{
	u8 op = CFS_OP_TRUNCATE;
	struct cfs_packet *packet;
	struct cfs_packet_truncate_request *request_data;
	int ret;

	packet = cfs_packet_new(op, mp->id, NULL, NULL);
	if (!packet)
		return -ENOMEM;

	request_data = &packet->request.data.truncate;
	request_data->vol_name = mc->volume;
	request_data->pid = mp->id;
	request_data->ino = ino;
	request_data->size = size;

	ret = do_meta_request(mc, mp, packet);
	if (ret < 0)
		goto out;

	ret = cfs_parse_status(packet->reply.hdr.result_code);
	if (ret > 0)
		goto out;

out:
	cfs_packet_release(packet);
	return ret;
}

int cfs_meta_truncate(struct cfs_meta_client *mc, u64 ino, loff_t size)
{
	struct cfs_meta_partition *mp;
	int ret;

	read_lock(&mc->lock);
	mp = cfs_meta_get_partition_by_inode(mc, ino);
	if (!mp) {
		ret = -ENOENT;
		goto unlock;
	}
	ret = cfs_meta_truncate_internal(mc, mp, ino, size);
	if (ret != 0) {
		ret = ret < 0 ? ret : -ret;
		goto unlock;
	}

unlock:
	read_unlock(&mc->lock);
	return ret;
}
