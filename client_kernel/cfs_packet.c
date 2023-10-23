#include "cfs_packet.h"
#include "cfs_json.h"

#define CHECK(ret)                  \
	do {                        \
		int r__ = (ret);    \
		if (r__ < 0)        \
			return r__; \
	} while (0)

#define CHECK_GOTO(ret, key, ...)                                    \
	if (ret < 0) {                                               \
		cfs_log_err(key ", error %d\n", ##__VA_ARGS__, ret); \
		goto failed;                                         \
	}

static atomic_t packet_seq = ATOMIC_INIT(0);
static struct kmem_cache *packet_cache;
static struct kmem_cache *packet_inode_cache;

static u64 cfs_packet_generate_id(void)
{
	return atomic_inc_return(&packet_seq);
}

struct cfs_packet *cfs_packet_new(u8 op, u64 pid,
				  void (*handle_reply)(struct cfs_packet *),
				  void *private)
{
	struct cfs_packet *packet;

	packet = kmem_cache_zalloc(packet_cache, GFP_NOFS);
	if (!packet)
		return NULL;
	packet->request.hdr.magic = CFS_PACKET_MAGIC;
	packet->request.hdr.opcode = op;
	packet->request.hdr.pid = cpu_to_be64(pid);
	packet->request.hdr.req_id = cpu_to_be64(cfs_packet_generate_id());
	atomic_set(&packet->refcnt, 1);
	init_completion(&packet->done);
	packet->handle_reply = handle_reply;
	packet->private = private;
	return packet;
}

void cfs_packet_release(struct cfs_packet *packet)
{
	if (!packet)
		return;
	if (!atomic_dec_and_test(&packet->refcnt))
		return;
	cfs_packet_clear(packet);
	kmem_cache_free(packet_cache, packet);
}

struct cfs_packet_inode *cfs_packet_inode_new(void)
{
	return kmem_cache_zalloc(packet_inode_cache, GFP_NOFS);
}

void cfs_packet_inode_release(struct cfs_packet_inode *iinfo)
{
	if (!iinfo)
		return;
	cfs_packet_inode_clear(iinfo);
	kmem_cache_free(packet_inode_cache, iinfo);
}

static int
cfs_quota_info_array_from_json(cfs_json_t *json,
			       struct cfs_quota_info_array *quota_infos)
{
	cfs_json_t json_quota_info;
	struct cfs_quota_info *info;
	char *key;
	size_t len;
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_array_size(json);
	if (ret >= 0) {
		ret = cfs_quota_info_array_init(quota_infos, ret);
		if (ret < 0)
			goto failed;
		for (; quota_infos->num < quota_infos->cap;
		     quota_infos->num++) {
			info = &quota_infos->base[quota_infos->num];
			ret = cfs_json_get_array_item(json, quota_infos->num,
						      &json_quota_info);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_json_get_object_key_ptr(
				&json_quota_info, (const char **)&key, &len);
			if (ret < 0) {
				goto failed;
			}
			ret = cfs_kstrntou32(key, len, 10, &info->id);
			if (ret < 0) {
				goto failed;
			}
			ret = cfs_json_get_object_value(&json_quota_info,
							&json_val);
			if (ret < 0) {
				goto failed;
			}
			ret = cfs_json_get_bool(&json_val, "rid", &info->root);
			if (ret < 0) {
				goto failed;
			}
		}
	}
	return 0;

failed:
	cfs_quota_info_array_clear(quota_infos);
	return ret;
}

static int cfs_packet_inode_from_json(cfs_json_t *json,
				      struct cfs_packet_inode *info)
{
	cfs_json_t json_quotas;
	u32 mode;
	const char *val;
	size_t len;
	int ret;

	memset(info, 0, sizeof(*info));
	ret = cfs_json_get_u64(json, "ino", &info->ino);
	CHECK_GOTO(ret, "not found ino");

	ret = cfs_json_get_u32(json, "mode", &mode);
	CHECK_GOTO(ret, "not found mode");
	info->mode = umode_from_u32(mode);

	ret = cfs_json_get_u32(json, "nlink", &info->nlink);
	CHECK_GOTO(ret, "not found nlink");

	ret = cfs_json_get_u64(json, "sz", &info->size);
	CHECK_GOTO(ret, "not found sz");

	ret = cfs_json_get_u32(json, "uid", &info->uid);
	CHECK_GOTO(ret, "not found uid");

	ret = cfs_json_get_u32(json, "gid", &info->gid);
	CHECK_GOTO(ret, "not found gid");

	ret = cfs_json_get_u64(json, "gen", &info->generation);
	CHECK_GOTO(ret, "not found gen");

	ret = cfs_json_get_string_ptr(json, "mt", &val, &len);
	CHECK_GOTO(ret, "not found mt");
	ret = cfs_parse_time(val, len, &info->modify_time);
	CHECK_GOTO(ret, "failed to parse mt");

	ret = cfs_json_get_string_ptr(json, "ct", &val, &len);
	CHECK_GOTO(ret, "not found ct");
	ret = cfs_parse_time(val, len, &info->create_time);
	CHECK_GOTO(ret, "failed to parse ct");

	ret = cfs_json_get_string_ptr(json, "at", &val, &len);
	CHECK_GOTO(ret, "not found at");
	ret = cfs_parse_time(val, len, &info->access_time);
	CHECK_GOTO(ret, "failed to parse at");

	ret = cfs_json_get_string_ptr(json, "tgt", &val, &len);
	if (ret == 0) {
		ret = cfs_base64_decode(val, len, &info->target);
		CHECK_GOTO(ret, "failed to parse tgt");
	}

	ret = cfs_json_get_object(json, "qifs", &json_quotas);
	if (ret == 0) {
		ret = cfs_quota_info_array_from_json(&json_quotas,
						     &info->quota_infos);
		CHECK_GOTO(ret, "failed to parse qifs");
	}
	return 0;

failed:
	cfs_packet_inode_clear(info);
	return ret;
}

static int cfs_packet_dentry_from_json(cfs_json_t *json,
				       struct cfs_packet_dentry *dentry)
{
	int ret;
	u32 mode;

	memset(dentry, 0, sizeof(*dentry));
	ret = cfs_json_get_string(json, "name", &dentry->name);
	CHECK_GOTO(ret, "not found name");

	ret = cfs_json_get_u64(json, "ino", &dentry->ino);
	CHECK_GOTO(ret, "not found ino");

	ret = cfs_json_get_u32(json, "type", &mode);
	CHECK_GOTO(ret, "not found type");
	dentry->type = umode_from_u32(mode);
	return 0;

failed:
	cfs_packet_dentry_clear(dentry);
	return ret;
}

static int cfs_packet_extent_to_json(struct cfs_packet_extent *extent,
				     struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"FileOffset\":%llu,",
			       extent->file_offset));
	CHECK(cfs_buffer_write(buffer, "\"PartitionId\":%llu,", extent->pid));
	CHECK(cfs_buffer_write(buffer, "\"ExtentId\":%llu,", extent->ext_id));
	CHECK(cfs_buffer_write(buffer, "\"ExtentOffset\":%llu,",
			       extent->ext_offset));
	CHECK(cfs_buffer_write(buffer, "\"Size\":%u,", extent->size));
	CHECK(cfs_buffer_write(buffer, "\"CRC\":%u", extent->crc));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int cfs_packet_extent_from_json(cfs_json_t *json,
				       struct cfs_packet_extent *extent)
{
	int ret;

	memset(extent, 0, sizeof(*extent));
	ret = cfs_json_get_u64(json, "FileOffset", &extent->file_offset);
	CHECK_GOTO(ret, "not found FileOffset");

	ret = cfs_json_get_u64(json, "PartitionId", &extent->pid);
	CHECK_GOTO(ret, "not found PartitionId");

	ret = cfs_json_get_u64(json, "ExtentId", &extent->ext_id);
	CHECK_GOTO(ret, "not found ExtentId");

	ret = cfs_json_get_u64(json, "ExtentOffset", &extent->ext_offset);
	CHECK_GOTO(ret, "not found ExtentOffset");

	ret = cfs_json_get_u32(json, "Size", &extent->size);
	CHECK_GOTO(ret, "not found Size");

	ret = cfs_json_get_u32(json, "CRC", &extent->crc);
	CHECK_GOTO(ret, "not found CRC");
	return 0;

failed:
	cfs_packet_extent_clear(extent);
	return ret;
}

static int
cfs_packet_icreate_request_to_json(struct cfs_packet_icreate_request *req,
				   struct cfs_buffer *buffer)
{
	int i;
	int ret;

	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"mode\":%u,",
			       umode_to_u32(req->mode)));
	CHECK(cfs_buffer_write(buffer, "\"uid\":%u,", req->uid));
	CHECK(cfs_buffer_write(buffer, "\"gid\":%u,", req->gid));
	if (req->target) {
		char *base64;
		CHECK(cfs_base64_encode(req->target, strlen(req->target),
					&base64));
		ret = cfs_buffer_write(buffer, "\"tgt\":\"%s\",", base64);
		kfree(base64);
		CHECK(ret);
	}
	CHECK(cfs_buffer_write(buffer, "\"qids\":"));
	CHECK(cfs_buffer_write(buffer, "["));
	for (i = 0; i < req->quota_infos.num; i++) {
		if (i == 0)
			CHECK(cfs_buffer_write(buffer, "%u",
					       req->quota_infos.base[i].id));
		else
			CHECK(cfs_buffer_write(buffer, ",%u",
					       req->quota_infos.base[i].id));
	}
	CHECK(cfs_buffer_write(buffer, "]"));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_icreate_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_icreate_reply *res)
{
	cfs_json_t json_info;
	int ret;

	ret = cfs_json_get_object(json, "info", &json_info);
	if (ret < 0) {
		cfs_log_err("not found iget.info, error %d\n", ret);
		return ret;
	}
	res->info = cfs_packet_inode_new();
	if (!res->info)
		return -ENOMEM;
	ret = cfs_packet_inode_from_json(&json_info, res->info);
	if (ret < 0) {
		kfree(res->info);
		res->info = NULL;
	}
	return ret;
}

static int cfs_packet_iget_request_to_json(struct cfs_packet_iget_request *req,
					   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu", req->ino));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int cfs_packet_iget_reply_from_json(cfs_json_t *json,
					   struct cfs_packet_iget_reply *res)
{
	cfs_json_t json_info;
	int ret;

	ret = cfs_json_get_object(json, "info", &json_info);
	if (ret < 0) {
		cfs_log_err("not found iget.info");
		return ret;
	}
	res->info = cfs_packet_inode_new();
	if (!res->info)
		return -ENOMEM;
	ret = cfs_packet_inode_from_json(&json_info, res->info);
	if (ret < 0) {
		kfree(res->info);
		res->info = NULL;
	}
	return ret;
}

static int
cfs_packet_batch_iget_request_to_json(struct cfs_packet_batch_iget_request *req,
				      struct cfs_buffer *buffer)
{
	size_t i;

	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"inos\":[", req->pid));
	for (i = 0; i < req->ino_vec.num; i++) {
		CHECK(cfs_buffer_write(buffer, i == 0 ? "%llu" : ",%llu",
				       req->ino_vec.base[i]));
	}
	CHECK(cfs_buffer_write(buffer, "]}"));
	return 0;
}

static int
cfs_packet_batch_iget_reply_from_json(cfs_json_t *json,
				      struct cfs_packet_batch_iget_reply *res)
{
	cfs_json_t json_infos, json_info;
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_object(json, "infos", &json_infos);
	CHECK_GOTO(ret, "not found iget.infos");

	ret = cfs_json_get_array_size(&json_infos);
	if (ret >= 0) {
		ret = cfs_packet_inode_ptr_array_init(&res->info_vec, ret);
		if (ret < 0)
			goto failed;
		for (; res->info_vec.num < res->info_vec.cap;
		     res->info_vec.num++) {
			ret = cfs_json_get_array_item(
				&json_infos, res->info_vec.num, &json_info);
			if (unlikely(ret < 0))
				goto failed;
			res->info_vec.base[res->info_vec.num] =
				cfs_packet_inode_new();
			if (!res->info_vec.base[res->info_vec.num]) {
				ret = -ENOMEM;
				goto failed;
			}
			ret = cfs_packet_inode_from_json(
				&json_info,
				res->info_vec.base[res->info_vec.num]);
			CHECK_GOTO(ret, "failed to parse info");
		}
	}
failed:
	cfs_packet_batch_iget_reply_clear(res);
	return ret;
}

static int
cfs_packet_lookup_request_to_json(struct cfs_packet_lookup_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"pino\":%llu,", req->parent_ino));
	CHECK(cfs_buffer_write(buffer, "\"name\":\"%.*s\"", req->name->len,
			       req->name->name));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_lookup_reply_from_json(cfs_json_t *json,
				  struct cfs_packet_lookup_reply *res)
{
	int ret;
	u32 mode;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_u64(json, "ino", &res->ino);
	CHECK_GOTO(ret, "not found ino");

	ret = cfs_json_get_u32(json, "mode", &mode);
	CHECK_GOTO(ret, "not found mode");
	res->mode = umode_from_u32(mode);
	return 0;

failed:
	cfs_packet_lookup_reply_clear(res);
	return ret;
}

static int
cfs_packet_readdir_request_to_json(struct cfs_packet_readdir_request *req,
				   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"pino\":%llu,", req->parent_ino));
	CHECK(cfs_buffer_write(buffer, "\"marker\":\"%s\",", req->marker));
	CHECK(cfs_buffer_write(buffer, "\"limit\":%llu", req->limit));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_readdir_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_readdir_reply *res)
{
	cfs_json_t json_children, json_child;
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_object(json, "children", &json_children);
	CHECK_GOTO(ret, "not found children");

	ret = cfs_json_get_array_size(&json_children);
	if (ret >= 0) {
		ret = cfs_packet_dentry_array_init(&res->children, ret);
		if (ret < 0)
			goto failed;
		for (; res->children.num < res->children.cap;
		     res->children.num++) {
			ret = cfs_json_get_array_item(
				&json_children, res->children.num, &json_child);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_packet_dentry_from_json(
				&json_child,
				&res->children.base[res->children.num]);
			CHECK_GOTO(ret, "failed to parse children");
		}
	}
	return 0;

failed:
	cfs_packet_readdir_reply_clear(res);
	return ret;
}

static int
cfs_packet_dcreate_request_to_json(struct cfs_packet_dcreate_request *req,
				   struct cfs_buffer *buffer)
{
	size_t i;

	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"pino\":%llu,", req->parent_ino));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"name\":\"%.*s\",", req->name->len,
			       req->name->name));
	CHECK(cfs_buffer_write(buffer, "\"mode\":%u,",
			       umode_to_u32(req->mode)));
	CHECK(cfs_buffer_write(buffer, "\"qids\":["));
	for (i = 0; i < req->quota_infos.num; i++) {
		if (i == 0)
			CHECK(cfs_buffer_write(buffer, "%u",
					       req->quota_infos.base[i].id));
		else
			CHECK(cfs_buffer_write(buffer, ",%u",
					       req->quota_infos.base[i].id));
	}
	CHECK(cfs_buffer_write(buffer, "]}"));
	return 0;
}

static int
cfs_packet_ddelete_request_to_json(struct cfs_packet_ddelete_request *req,
				   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"pino\":%llu,", req->parent_ino));
	CHECK(cfs_buffer_write(buffer, "\"name\":\"%.*s\"", req->name->len,
			       req->name->name));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_ddelete_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_ddelete_reply *res)
{
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_u64(json, "ino", &res->ino);
	CHECK_GOTO(ret, "not found ino");
	return 0;

failed:
	cfs_packet_ddelete_reply_clear(res);
	return ret;
}

static int
cfs_packet_dupdate_request_to_json(struct cfs_packet_dupdate_request *req,
				   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"pino\":%llu,", req->parent_ino));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"name\":\"%.*s\"", req->name->len,
			       req->name->name));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_dupdate_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_dupdate_reply *res)
{
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_u64(json, "ino", &res->ino);
	CHECK_GOTO(ret, "not found ino");
	return 0;

failed:
	cfs_packet_dupdate_reply_clear(res);
	return ret;
}

static int
cfs_packet_ilink_request_to_json(struct cfs_packet_ilink_request *req,
				 struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"uid\":%llu", req->uniqid));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int cfs_packet_ilink_reply_from_json(cfs_json_t *json,
					    struct cfs_packet_ilink_reply *res)
{
	cfs_json_t json_info;
	int ret;

	ret = cfs_json_get_object(json, "info", &json_info);
	if (ret < 0) {
		cfs_log_err("not found iget.info");
		return ret;
	}
	res->info = cfs_packet_inode_new();
	if (!res->info)
		return -ENOMEM;
	ret = cfs_packet_inode_from_json(&json_info, res->info);
	if (ret < 0) {
		kfree(res->info);
		res->info = NULL;
	}
	return ret;
}

static int
cfs_packet_iunlink_request_to_json(struct cfs_packet_iunlink_request *req,
				   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"uid\":%llu", req->uniqid));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_iunlink_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_iunlink_reply *res)
{
	cfs_json_t json_info;
	int ret;

	ret = cfs_json_get_object(json, "info", &json_info);
	if (ret < 0) {
		cfs_log_err("not found iget.info");
		return ret;
	}
	res->info = cfs_packet_inode_new();
	if (!res->info)
		return -ENOMEM;
	ret = cfs_packet_inode_from_json(&json_info, res->info);
	if (ret < 0) {
		kfree(res->info);
		res->info = NULL;
	}
	return ret;
}

static int
cfs_packet_ievict_request_to_json(struct cfs_packet_ievict_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu", req->ino));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_sattr_request_to_json(struct cfs_packet_sattr_request *req,
				 struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"mode\":%u,",
			       umode_to_u32(req->mode)));
	CHECK(cfs_buffer_write(buffer, "\"uid\":%u,", req->uid));
	CHECK(cfs_buffer_write(buffer, "\"gid\":%u,", req->gid));
	CHECK(cfs_buffer_write(buffer, "\"mt\":%lu,", req->modify_time.tv_sec));
	CHECK(cfs_buffer_write(buffer, "\"at\":%lu,", req->access_time.tv_sec));
	CHECK(cfs_buffer_write(buffer, "\"valid\":%u", req->valid));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_sxattr_request_to_json(struct cfs_packet_sxattr_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"key\":\"%s\",", req->key));
	CHECK(cfs_buffer_write(buffer, "\"value\":\"%.*s\"", req->value_len,
			       req->value));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_gxattr_request_to_json(struct cfs_packet_gxattr_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"key\":\"%s\"", req->key));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_gxattr_reply_from_json(cfs_json_t *json,
				  struct cfs_packet_gxattr_reply *res)
{
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_string(json, "vol", &res->vol_name);
	CHECK_GOTO(ret, "not found vol");
	ret = cfs_json_get_u64(json, "pid", &res->pid);
	CHECK_GOTO(ret, "not found pid");
	ret = cfs_json_get_u64(json, "ino", &res->ino);
	CHECK_GOTO(ret, "not found ino");
	ret = cfs_json_get_string(json, "key", &res->key);
	CHECK_GOTO(ret, "not found key");
	ret = cfs_json_get_string(json, "val", &res->value);
	CHECK_GOTO(ret, "not found val");
	return 0;
failed:
	cfs_packet_gxattr_reply_clear(res);
	return ret;
}

static int
cfs_packet_rxattr_request_to_json(struct cfs_packet_rxattr_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"key\":\"%s\"", req->key));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_lxattr_request_to_json(struct cfs_packet_lxattr_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu", req->ino));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_lxattr_reply_from_json(cfs_json_t *json,
				  struct cfs_packet_lxattr_reply *res)
{
	cfs_json_t json_xattrs, json_xattr;
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_string(json, "vol", &res->vol_name);
	CHECK_GOTO(ret, "not found vol");
	ret = cfs_json_get_u64(json, "pid", &res->pid);
	CHECK_GOTO(ret, "not found pid");
	ret = cfs_json_get_u64(json, "ino", &res->ino);
	CHECK_GOTO(ret, "not found ino");

	ret = cfs_json_get_object(json, "xattrs", &json_xattrs);
	if (ret == 0)
		ret = cfs_json_get_array_size(&json_xattrs);
	if (ret >= 0) {
		ret = string_array_init(&res->xattrs, ret);
		if (ret < 0)
			goto failed;
		for (; res->xattrs.num < res->xattrs.cap; res->xattrs.num++) {
			ret = cfs_json_get_array_item(
				&json_xattrs, res->xattrs.num, &json_xattr);
			if (ret < 0)
				goto failed;
			ret = cfs_json_get_value_string(
				&json_xattr,
				&res->xattrs.base[res->xattrs.num]);
			CHECK_GOTO(ret, "failed to parse xattrs");
		}
	}
	return 0;
failed:
	cfs_packet_lxattr_reply_clear(res);
	return ret;
}

static int
cfs_packet_lextent_request_to_json(struct cfs_packet_lextent_request *req,
				   struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu", req->ino));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_lextent_reply_from_json(cfs_json_t *json,
				   struct cfs_packet_lextent_reply *res)
{
	cfs_json_t json_extents, json_extent;
	int ret;

	memset(res, 0, sizeof(*res));
	ret = cfs_json_get_u64(json, "gen", &res->generation);
	CHECK_GOTO(ret, "not found gen");
	ret = cfs_json_get_u64(json, "sz", &res->size);
	CHECK_GOTO(ret, "not found sz");

	ret = cfs_json_get_object(json, "eks", &json_extents);
	if (ret == 0)
		ret = cfs_json_get_array_size(&json_extents);
	if (ret >= 0) {
		ret = cfs_packet_extent_array_init(&res->extents, ret);
		if (ret < 0)
			goto failed;
		for (; res->extents.num < res->extents.cap;
		     res->extents.num++) {
			ret = cfs_json_get_array_item(
				&json_extents, res->extents.num, &json_extent);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_packet_extent_from_json(
				&json_extent,
				&res->extents.base[res->extents.num]);
			CHECK_GOTO(ret, "failed to parse eks");
		}
	}
	return 0;
failed:
	cfs_packet_lextent_reply_clear(res);
	return ret;
}

static int
cfs_packet_aextent_request_to_json(struct cfs_packet_aextent_request *req,
				   struct cfs_buffer *buffer)
{
	size_t i;

	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"ek\":"));
	CHECK(cfs_packet_extent_to_json(&req->extent, buffer));
	CHECK(cfs_buffer_write(buffer, ","));
	CHECK(cfs_buffer_write(buffer, "\"dek\":["));
	for (i = 0; i < req->discard_extents.num; i++) {
		if (i > 0)
			CHECK(cfs_buffer_write(buffer, ","));
		CHECK(cfs_packet_extent_to_json(&req->discard_extents.base[i],
						buffer));
	}
	CHECK(cfs_buffer_write(buffer, "]}"));
	return 0;
}

static int
cfs_packet_truncate_request_to_json(struct cfs_packet_truncate_request *req,
				    struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%llu,", req->ino));
	CHECK(cfs_buffer_write(buffer, "\"sz\":%llu", req->size));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_uniqid_request_to_json(struct cfs_packet_uniqid_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"vol\":\"%s\",", req->vol_name));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"num\":%u", req->num));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_uniqid_reply_from_json(cfs_json_t *json,
				  struct cfs_packet_uniqid_reply *res)
{
	int ret;

	cfs_packet_uniqid_reply_clear(res);
	ret = cfs_json_get_u64(json, "start", &res->start);
	CHECK_GOTO(ret, "not found pid");
	return 0;

failed:
	cfs_packet_uniqid_reply_clear(res);
	return ret;
}

static int
cfs_packet_gquota_request_to_json(struct cfs_packet_gquota_request *req,
				  struct cfs_buffer *buffer)
{
	CHECK(cfs_buffer_write(buffer, "{"));
	CHECK(cfs_buffer_write(buffer, "\"pid\":%llu,", req->pid));
	CHECK(cfs_buffer_write(buffer, "\"ino\":%u", req->ino));
	CHECK(cfs_buffer_write(buffer, "}"));
	return 0;
}

static int
cfs_packet_gquota_reply_from_json(cfs_json_t *json,
				  struct cfs_packet_gquota_reply *res)
{
	cfs_json_t json_quota_infos;
	int ret;

	cfs_packet_gquota_reply_init(res);
	ret = cfs_json_get_object(json, "MetaQuotaInfoMap", &json_quota_infos);
	CHECK_GOTO(ret, "not found MetaQuotaInfoMap");

	ret = cfs_quota_info_array_from_json(&json_quota_infos,
					     &res->quota_infos);
	CHECK_GOTO(ret, "failed to parse MetaQuotaInfoMap");
	return 0;

failed:
	cfs_packet_gquota_reply_clear(res);
	return ret;
}

int cfs_packet_request_data_to_json(struct cfs_packet *packet,
				    struct cfs_buffer *buffer)
{
	switch (packet->request.hdr.opcode) {
	case CFS_OP_INODE_CREATE:
		return cfs_packet_icreate_request_to_json(
			&packet->request.data.icreate, buffer);
	case CFS_OP_INODE_GET:
		return cfs_packet_iget_request_to_json(
			&packet->request.data.iget, buffer);
	case CFS_OP_INODE_BATCH_GET:
		return cfs_packet_batch_iget_request_to_json(
			&packet->request.data.batch_iget, buffer);
	case CFS_OP_LOOKUP:
		return cfs_packet_lookup_request_to_json(
			&packet->request.data.ilookup, buffer);
	case CFS_OP_READDIR_LIMIT:
		return cfs_packet_readdir_request_to_json(
			&packet->request.data.readdir, buffer);
	case CFS_OP_DENTRY_CREATE:
		return cfs_packet_dcreate_request_to_json(
			&packet->request.data.dcreate, buffer);
	case CFS_OP_DENTRY_DELETE:
		return cfs_packet_ddelete_request_to_json(
			&packet->request.data.ddelete, buffer);
	case CFS_OP_DENTRY_UPDATE:
		return cfs_packet_dupdate_request_to_json(
			&packet->request.data.dupdate, buffer);
	case CFS_OP_INODE_LINK:
		return cfs_packet_ilink_request_to_json(
			&packet->request.data.ilink, buffer);
	case CFS_OP_INODE_UNLINK:
		return cfs_packet_iunlink_request_to_json(
			&packet->request.data.iunlink, buffer);
	case CFS_OP_INODE_EVICT:
		return cfs_packet_ievict_request_to_json(
			&packet->request.data.ievict, buffer);
	case CFS_OP_ATTR_SET:
		return cfs_packet_sattr_request_to_json(
			&packet->request.data.sattr, buffer);
	case CFS_OP_XATTR_SET:
	case CFS_OP_XATTR_UPDATE:
		return cfs_packet_sxattr_request_to_json(
			&packet->request.data.sxattr, buffer);
	case CFS_OP_XATTR_GET:
		return cfs_packet_gxattr_request_to_json(
			&packet->request.data.gxattr, buffer);
	case CFS_OP_XATTR_REMOVE:
		return cfs_packet_rxattr_request_to_json(
			&packet->request.data.rxattr, buffer);
	case CFS_OP_XATTR_LIST:
		return cfs_packet_lxattr_request_to_json(
			&packet->request.data.lxattr, buffer);
	case CFS_OP_EXTENT_LIST:
		return cfs_packet_lextent_request_to_json(
			&packet->request.data.lextent, buffer);
	case CFS_OP_EXTENT_ADD_WITH_CHECK:
		return cfs_packet_aextent_request_to_json(
			&packet->request.data.aextent, buffer);
	case CFS_OP_TRUNCATE:
		return cfs_packet_truncate_request_to_json(
			&packet->request.data.truncate, buffer);
	case CFS_OP_UNIQID_GET:
		return cfs_packet_uniqid_request_to_json(
			&packet->request.data.uniqid, buffer);
	case CFS_OP_QUOTA_INODE_GET:
		return cfs_packet_gquota_request_to_json(
			&packet->request.data.gquota, buffer);
	default:
		return -EPERM;
	}
}

int cfs_packet_reply_data_from_json(cfs_json_t *json, struct cfs_packet *packet)
{
	switch (packet->reply.hdr.opcode) {
	case CFS_OP_INODE_CREATE:
		return cfs_packet_icreate_reply_from_json(
			json, &packet->reply.data.icreate);
	case CFS_OP_INODE_GET:
		return cfs_packet_iget_reply_from_json(
			json, &packet->reply.data.iget);
	case CFS_OP_INODE_BATCH_GET:
		return cfs_packet_batch_iget_reply_from_json(
			json, &packet->reply.data.batch_iget);
	case CFS_OP_LOOKUP:
		return cfs_packet_lookup_reply_from_json(
			json, &packet->reply.data.ilookup);
	case CFS_OP_READDIR_LIMIT:
		return cfs_packet_readdir_reply_from_json(
			json, &packet->reply.data.readdir);
	case CFS_OP_DENTRY_CREATE:
		return 0;
	case CFS_OP_DENTRY_DELETE:
		return cfs_packet_ddelete_reply_from_json(
			json, &packet->reply.data.ddelete);
	case CFS_OP_DENTRY_UPDATE:
		return cfs_packet_dupdate_reply_from_json(
			json, &packet->reply.data.dupdate);
	case CFS_OP_INODE_LINK:
		return cfs_packet_ilink_reply_from_json(
			json, &packet->reply.data.ilink);
	case CFS_OP_INODE_UNLINK:
		return cfs_packet_iunlink_reply_from_json(
			json, &packet->reply.data.iunlink);
	case CFS_OP_INODE_EVICT:
		return 0;
	case CFS_OP_ATTR_SET:
		return 0;
	case CFS_OP_XATTR_SET:
	case CFS_OP_XATTR_UPDATE:
		return 0;
	case CFS_OP_XATTR_GET:
		return cfs_packet_gxattr_reply_from_json(
			json, &packet->reply.data.gxattr);
	case CFS_OP_XATTR_REMOVE:
		return 0;
	case CFS_OP_XATTR_LIST:
		return cfs_packet_lxattr_reply_from_json(
			json, &packet->reply.data.lxattr);
	case CFS_OP_EXTENT_LIST:
		return cfs_packet_lextent_reply_from_json(
			json, &packet->reply.data.lextent);
	case CFS_OP_UNIQID_GET:
		return cfs_packet_uniqid_reply_from_json(
			json, &packet->reply.data.uniqid);
	case CFS_OP_QUOTA_INODE_GET:
		return cfs_packet_gquota_reply_from_json(
			json, &packet->reply.data.gquota);
	default:
		return -EPERM;
	}
}

void cfs_packet_request_data_clear(struct cfs_packet *packet)
{
	switch (packet->request.hdr.opcode) {
	case CFS_OP_INODE_CREATE:
		cfs_packet_icreate_request_clear(&packet->request.data.icreate);
		break;
	case CFS_OP_INODE_GET:
		cfs_packet_iget_request_clear(&packet->request.data.iget);
		break;
	case CFS_OP_INODE_BATCH_GET:
		cfs_packet_batch_iget_request_clear(
			&packet->request.data.batch_iget);
		break;
	case CFS_OP_LOOKUP:
		cfs_packet_lookup_request_clear(&packet->request.data.ilookup);
		break;
	case CFS_OP_READDIR_LIMIT:
		cfs_packet_readdir_request_clear(&packet->request.data.readdir);
		break;
	case CFS_OP_DENTRY_CREATE:
		cfs_packet_dcreate_request_clear(&packet->request.data.dcreate);
		break;
	case CFS_OP_DENTRY_DELETE:
		cfs_packet_ddelete_request_clear(&packet->request.data.ddelete);
		break;
	case CFS_OP_DENTRY_UPDATE:
		cfs_packet_dupdate_request_clear(&packet->request.data.dupdate);
		break;
	case CFS_OP_INODE_LINK:
		cfs_packet_ilink_request_clear(&packet->request.data.ilink);
		break;
	case CFS_OP_INODE_UNLINK:
		cfs_packet_iunlink_request_clear(&packet->request.data.iunlink);
		break;
	case CFS_OP_INODE_EVICT:
		cfs_packet_ievict_request_clear(&packet->request.data.ievict);
		break;
	case CFS_OP_ATTR_SET:
		cfs_packet_sattr_request_clear(&packet->request.data.sattr);
		break;
	case CFS_OP_XATTR_SET:
	case CFS_OP_XATTR_UPDATE:
		cfs_packet_sxattr_request_clear(&packet->request.data.sxattr);
		break;
	case CFS_OP_XATTR_GET:
		cfs_packet_gxattr_request_clear(&packet->request.data.gxattr);
		break;
	case CFS_OP_XATTR_REMOVE:
		cfs_packet_rxattr_request_clear(&packet->request.data.rxattr);
		break;
	case CFS_OP_XATTR_LIST:
		cfs_packet_lxattr_request_clear(&packet->request.data.lxattr);
		break;
	case CFS_OP_EXTENT_LIST:
		cfs_packet_lextent_request_clear(&packet->request.data.lextent);
		break;
	case CFS_OP_EXTENT_ADD_WITH_CHECK:
		cfs_packet_aextent_request_clear(&packet->request.data.aextent);
		break;
	case CFS_OP_TRUNCATE:
		cfs_packet_truncate_request_clear(
			&packet->request.data.truncate);
		break;
	case CFS_OP_UNIQID_GET:
		cfs_packet_uniqid_request_clear(&packet->request.data.uniqid);
		break;
	case CFS_OP_QUOTA_INODE_GET:
		cfs_packet_gquota_request_clear(&packet->request.data.gquota);
		break;
	case CFS_OP_EXTENT_CREATE:
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		break;
	default:
		cfs_log_err("operations(0x%x) not implemented\n",
			    packet->request.hdr.opcode);
		break;
	}
}

void cfs_packet_reply_data_clear(struct cfs_packet *packet)
{
	switch (packet->reply.hdr.opcode) {
	case CFS_OP_INODE_CREATE:
		cfs_packet_icreate_reply_clear(&packet->reply.data.icreate);
		break;
	case CFS_OP_INODE_GET:
		cfs_packet_iget_reply_clear(&packet->reply.data.iget);
		break;
	case CFS_OP_INODE_BATCH_GET:
		cfs_packet_batch_iget_reply_clear(
			&packet->reply.data.batch_iget);
		break;
	case CFS_OP_LOOKUP:
		cfs_packet_lookup_reply_clear(&packet->reply.data.ilookup);
		break;
	case CFS_OP_READDIR_LIMIT:
		cfs_packet_readdir_reply_clear(&packet->reply.data.readdir);
		break;
	case CFS_OP_DENTRY_CREATE:
		break;
	case CFS_OP_DENTRY_DELETE:
		cfs_packet_ddelete_reply_clear(&packet->reply.data.ddelete);
		break;
	case CFS_OP_DENTRY_UPDATE:
		cfs_packet_dupdate_reply_clear(&packet->reply.data.dupdate);
		break;
	case CFS_OP_INODE_LINK:
		cfs_packet_ilink_reply_clear(&packet->reply.data.ilink);
		break;
	case CFS_OP_INODE_UNLINK:
		cfs_packet_iunlink_reply_clear(&packet->reply.data.iunlink);
		break;
	case CFS_OP_INODE_EVICT:
		break;
	case CFS_OP_ATTR_SET:
		break;
	case CFS_OP_XATTR_GET:
		cfs_packet_gxattr_reply_clear(&packet->reply.data.gxattr);
		break;
	case CFS_OP_XATTR_REMOVE:
		break;
	case CFS_OP_XATTR_LIST:
		cfs_packet_lxattr_reply_clear(&packet->reply.data.lxattr);
		break;
	case CFS_OP_EXTENT_LIST:
		cfs_packet_lextent_reply_clear(&packet->reply.data.lextent);
		break;
	case CFS_OP_UNIQID_GET:
		cfs_packet_uniqid_reply_clear(&packet->reply.data.uniqid);
		break;
	case CFS_OP_QUOTA_INODE_GET:
		cfs_packet_gquota_reply_clear(&packet->reply.data.gquota);
		break;
	case CFS_OP_EXTENT_ADD_WITH_CHECK:
	case CFS_OP_TRUNCATE:
	case CFS_OP_EXTENT_CREATE:
	case CFS_OP_STREAM_READ:
	case CFS_OP_STREAM_FOLLOWER_READ:
	case CFS_OP_STREAM_WRITE:
	case CFS_OP_STREAM_RANDOM_WRITE:
		break;
	case 0:
		/* reply is not used after alloced */
		break;
	default:
		cfs_log_err("operations(0x%x) not implemented\n",
			    packet->reply.hdr.opcode);
		break;
	}
}

int cfs_volume_view_from_json(cfs_json_t *json,
			      struct cfs_volume_view *vol_view)
{
	cfs_json_t json_mps, json_mp;
	int ret;

	cfs_volume_view_init(vol_view);
	ret = cfs_json_get_string(json, "Name", &vol_view->name);
	CHECK_GOTO(ret, "not found Name");

	ret = cfs_json_get_string(json, "Owner", &vol_view->owner);
	CHECK_GOTO(ret, "not found Owner");

	ret = cfs_json_get_u8(json, "Status", &vol_view->status);
	CHECK_GOTO(ret, "not found Status");

	ret = cfs_json_get_bool(json, "FollowerRead", &vol_view->follower_read);
	CHECK_GOTO(ret, "not found FollowerRead");

	ret = cfs_json_get_object(json, "MetaPartitions", &json_mps);
	CHECK_GOTO(ret, "not found MetaPartitions");

	ret = cfs_json_get_array_size(&json_mps);
	if (ret >= 0) {
		ret = cfs_meta_partition_view_array_init(
			&vol_view->meta_partitions, ret);
		if (ret < 0)
			goto failed;
		for (; vol_view->meta_partitions.num <
		       vol_view->meta_partitions.cap;
		     vol_view->meta_partitions.num++) {
			ret = cfs_json_get_array_item(
				&json_mps, vol_view->meta_partitions.num,
				&json_mp);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_meta_partition_view_from_json(
				&json_mp,
				&vol_view->meta_partitions
					 .base[vol_view->meta_partitions.num]);
			CHECK_GOTO(ret, "failed to parse MetaPartitions");
		}
	}
	return ret;

failed:
	cfs_volume_view_clear(vol_view);
	return ret;
}

int cfs_meta_partition_view_from_json(cfs_json_t *json,
				      struct cfs_meta_partition_view *mp_view)
{
	cfs_json_t json_addrs, json_addr;
	const char *val;
	size_t len;
	int ret;

	cfs_meta_partition_view_init(mp_view);
	ret = cfs_json_get_u64(json, "PartitionID", &mp_view->id);
	CHECK_GOTO(ret, "not found PartitionID");

	ret = cfs_json_get_u64(json, "Start", &mp_view->start_ino);
	CHECK_GOTO(ret, "not found Start");

	ret = cfs_json_get_u64(json, "End", &mp_view->end_ino);
	CHECK_GOTO(ret, "not found End");

	ret = cfs_json_get_u64(json, "MaxInodeID", &mp_view->max_ino);
	CHECK_GOTO(ret, "not found MaxInodeID");

	ret = cfs_json_get_u64(json, "InodeCount", &mp_view->inode_count);
	CHECK_GOTO(ret, "not found InodeCount");

	ret = cfs_json_get_u64(json, "DentryCount", &mp_view->dentry_count);
	CHECK_GOTO(ret, "not found DentryCount");

	ret = cfs_json_get_bool(json, "IsRecover", &mp_view->is_recover);
	CHECK_GOTO(ret, "not found IsRecover");

	ret = cfs_json_get_s8(json, "Status", &mp_view->status);
	CHECK_GOTO(ret, "not found Status");

	ret = cfs_json_get_string_ptr(json, "LeaderAddr", &val, &len);
	if (ret == 0 && len > 0) {
		mp_view->leader = kmalloc(sizeof(*mp_view->leader), GFP_NOFS);
		if (!mp_view->leader) {
			ret = -ENOMEM;
			goto failed;
		}
		ret = cfs_parse_addr(val, len, mp_view->leader);
		CHECK_GOTO(ret, "failed to parse LeaderAddr(%.*s)", (int)len,
			   val);
	}

	ret = cfs_json_get_object(json, "Members", &json_addrs);
	if (ret == 0)
		ret = cfs_json_get_array_size(&json_addrs);
	if (ret >= 0) {
		ret = sockaddr_storage_array_init(&mp_view->members, ret);
		if (ret < 0)
			goto failed;
		for (; mp_view->members.num < mp_view->members.cap;
		     mp_view->members.num++) {
			ret = cfs_json_get_array_item(
				&json_addrs, mp_view->members.num, &json_addr);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_json_get_value_string_ptr(&json_addr, &val,
							    &len);
			CHECK_GOTO(ret, "failed to parse Members");

			ret = cfs_parse_addr(
				val, len,
				&mp_view->members.base[mp_view->members.num]);
			CHECK_GOTO(ret, "failed to parse Members");
		}
	}
	return 0;

failed:
	cfs_meta_partition_view_clear(mp_view);
	return ret;
}

int cfs_data_partition_view_from_json(cfs_json_t *json,
				      struct cfs_data_partition_view *dp_view)
{
	cfs_json_t json_addrs, json_addr;
	const char *val;
	size_t len;
	int ret;

	cfs_data_partition_view_init(dp_view);
	ret = cfs_json_get_u64(json, "PartitionID", &dp_view->id);
	CHECK_GOTO(ret, "not found PartitionID");

	ret = cfs_json_get_u32(json, "PartitionType", &dp_view->type);
	CHECK_GOTO(ret, "not found PartitionType");

	ret = cfs_json_get_s8(json, "Status", &dp_view->status);
	CHECK_GOTO(ret, "not found End");

	ret = cfs_json_get_u8(json, "ReplicaNum", &dp_view->replica_num);
	CHECK_GOTO(ret, "not found ReplicaNum");

	ret = cfs_json_get_u64(json, "Epoch", &dp_view->epoch);
	CHECK_GOTO(ret, "not found Epoch");

	ret = cfs_json_get_s64(json, "PartitionTTL", &dp_view->ttl);
	CHECK_GOTO(ret, "not found PartitionTTL");

	ret = cfs_json_get_bool(json, "IsRecover", &dp_view->is_recover);
	CHECK_GOTO(ret, "not found IsRecover");

	ret = cfs_json_get_bool(json, "IsDiscard", &dp_view->is_discard);
	CHECK_GOTO(ret, "not found IsDiscard");

	ret = cfs_json_get_string_ptr(json, "LeaderAddr", &val, &len);
	if (ret == 0 && len > 0) {
		dp_view->leader = kmalloc(sizeof(*dp_view->leader), GFP_NOFS);
		if (!dp_view->leader) {
			ret = -ENOMEM;
			goto failed;
		}
		ret = cfs_parse_addr(val, len, dp_view->leader);
		CHECK_GOTO(ret, "failed to parse LeaderAddr(%.*s)", (int)len,
			   val);
	}

	ret = cfs_json_get_object(json, "Hosts", &json_addrs);
	if (ret == 0)
		ret = cfs_json_get_array_size(&json_addrs);
	if (ret >= 0) {
		ret = sockaddr_storage_array_init(&dp_view->members, ret);
		if (ret < 0)
			goto failed;
		for (; dp_view->members.num < dp_view->members.cap;
		     dp_view->members.num++) {
			ret = cfs_json_get_array_item(
				&json_addrs, dp_view->members.num, &json_addr);
			if (unlikely(ret < 0))
				goto failed;
			ret = cfs_json_get_value_string_ptr(&json_addr, &val,
							    &len);
			CHECK_GOTO(ret, "failed to parse Hosts");

			ret = cfs_parse_addr(
				val, len,
				&dp_view->members.base[dp_view->members.num]);
			CHECK_GOTO(ret, "failed to parse Hosts");
		}
	}
	return 0;

failed:
	cfs_data_partition_view_clear(dp_view);
	return ret;
}

int cfs_volume_stat_from_json(cfs_json_t *json, struct cfs_volume_stat *stat)
{
	int ret;

	cfs_volume_stat_init(stat);
	ret = cfs_json_get_string(json, "Name", &stat->name);
	CHECK_GOTO(ret, "not found Name");

	ret = cfs_json_get_u64(json, "TotalSize", &stat->total_size);
	CHECK_GOTO(ret, "not found TotalSize");

	ret = cfs_json_get_u64(json, "UsedSize", &stat->used_size);
	CHECK_GOTO(ret, "not found UsedSize");

	ret = cfs_json_get_string(json, "UsedRatio", &stat->used_ratio);
	CHECK_GOTO(ret, "not found UsedRatio");

	ret = cfs_json_get_u64(json, "CacheTotalSize", &stat->cache_total_size);
	CHECK_GOTO(ret, "not found CacheTotalSize");

	ret = cfs_json_get_u64(json, "CacheUsedSize", &stat->cache_used_size);
	CHECK_GOTO(ret, "not found CacheUsedSize");

	ret = cfs_json_get_string(json, "CacheUsedRatio",
				  &stat->cache_used_ratio);
	CHECK_GOTO(ret, "not found CacheUsedRatio");

	ret = cfs_json_get_bool(json, "EnableToken", &stat->enable_token);
	CHECK_GOTO(ret, "not found EnableToken");

	ret = cfs_json_get_u64(json, "InodeCount", &stat->inode_count);
	CHECK_GOTO(ret, "not found InodeCount");

	ret = cfs_json_get_bool(json, "DpReadOnlyWhenVolFull",
				&stat->dp_read_only_when_vol_full);
	CHECK_GOTO(ret, "not found DpReadOnlyWhenVolFull");

	return 0;

failed:
	cfs_volume_stat_clear(stat);
	return ret;
}

int cfs_cluster_info_from_json(cfs_json_t *json, struct cfs_cluster_info *info)
{
	int ret;

	cfs_cluster_info_init(info);
	ret = cfs_json_get_u32(json, "DirChildrenNumLimit", &info->links_limit);
	CHECK_GOTO(ret, "not found DirChildrenNumLimit");

failed:
	cfs_cluster_info_clear(info);
	return ret;
}

int cfs_packet_module_init(void)
{
	if (!packet_cache) {
		packet_cache = KMEM_CACHE(cfs_packet, SLAB_MEM_SPREAD);
		if (!packet_cache)
			goto oom;
	}
	if (!packet_inode_cache) {
		packet_inode_cache =
			KMEM_CACHE(cfs_packet_inode, SLAB_MEM_SPREAD);
		if (!packet_inode_cache)
			goto oom;
	}
	return 0;

oom:
	cfs_packet_module_exit();
	return -ENOMEM;
}

void cfs_packet_module_exit(void)
{
	if (packet_cache) {
		kmem_cache_destroy(packet_cache);
		packet_cache = NULL;
	}
	if (packet_inode_cache) {
		kmem_cache_destroy(packet_inode_cache);
		packet_inode_cache = NULL;
	}
}
