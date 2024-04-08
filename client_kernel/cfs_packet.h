/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_PACKET_H__
#define __CFS_PACKET_H__

#include "cfs_buffer.h"
#include "cfs_common.h"
#include "cfs_json.h"
#include "cfs_page.h"

// clang-format off
#define CFS_PACKET_MAGIC                0xFF

/**
 * cfs operations
 */
#define CFS_OP_EXTENT_CREATE            0x01
#define CFS_OP_STREAM_WRITE             0x03
#define CFS_OP_STREAM_READ              0x05
#define CFS_OP_STREAM_FOLLOWER_READ     0x06
#define CFS_OP_STREAM_RANDOM_WRITE 	    0x0F

#define CFS_OP_INODE_CREATE             0x20
#define CFS_OP_INODE_UNLINK             0x21
#define CFS_OP_DENTRY_CREATE            0x22
#define CFS_OP_DENTRY_DELETE            0x23
#define CFS_OP_OPEN                     0x24
#define CFS_OP_LOOKUP                   0x25
#define CFS_OP_READDIR_LIMIT            0x3D
#define CFS_OP_INODE_GET                0x27
#define CFS_OP_INODE_BATCH_GET          0x28
#define CFS_OP_EXTENT_ADD_WITH_CHECK    0x3A
#define CFS_OP_EXTENT_DEL               0x2A
#define CFS_OP_EXTENT_LIST              0x2B
#define CFS_OP_DENTRY_UPDATE            0x2C
#define CFS_OP_TRUNCATE                 0x2D
#define CFS_OP_INODE_LINK               0x2E
#define CFS_OP_INODE_EVICT              0x2F
#define CFS_OP_ATTR_SET                 0x30
#define CFS_OP_RELEASE_OPEN             0x31

#define CFS_OP_XATTR_SET                0x35
#define CFS_OP_XATTR_GET                0x36
#define CFS_OP_XATTR_REMOVE             0x37
#define CFS_OP_XATTR_LIST               0x38
#define CFS_OP_XATTR_UPDATE             0x3B

#define CFS_OP_QUOTA_INODE_BATCH_SET    0x50
#define CFS_OP_QUOTA_INODE_BATCH_DELETE 0x51
#define CFS_OP_QUOTA_INODE_GET          0x52
#define CFS_OP_QUOTA_INODE_CREATE       0x53
#define CFS_OP_QUOTA_DENTRY_CREATE      0x54

#define CFS_OP_UNIQID_GET               0xAC

/**
 * cfs reply status
 */
#define CFS_STATUS_NO_SPACE             0xEE
#define CFS_STATUS_OK                   0xF0
#define CFS_STATUS_DIR_QUOTA            0xF1
#define CFS_STATUS_CONFLICT_EXTENTS     0xF2
#define CFS_STATUS_INTRA_GROUP_NET      0xF3
#define CFS_STATUS_ARG_MISMATCH         0xF4
#define CFS_STATUS_NOT_EXIST            0xF5
#define CFS_STATUS_DISK_NO_SPACE        0xF6
#define CFS_STATUS_DISK_ERR             0xF7
#define CFS_STATUS_ERR                  0xF8
#define CFS_STATUS_AGAIN                0xF9
#define CFS_STATUS_EXIST                0xFA
#define CFS_STATUS_INODE_FULL           0xFB
#define CFS_STATUS_TRY_OTHER_ADDR       0xFC
#define CFS_STATUS_NOT_PERM             0xFD
#define CFS_STATUS_NOT_EMPTY            0xFE
// clang-format on

static inline int cfs_parse_status(u8 status)
{
	switch (status) {
	case CFS_STATUS_OK:
		return 0;
	case CFS_STATUS_NO_SPACE:
		return ENOSPC;
	case CFS_STATUS_NOT_EXIST:
		return ENOENT;
	case CFS_STATUS_DISK_NO_SPACE:
		return ENOSPC;
	case CFS_STATUS_EXIST:
		return EEXIST;
	case CFS_STATUS_DIR_QUOTA:
		return EDQUOT;
	case CFS_STATUS_INODE_FULL:
		return ENOMEM;
	case CFS_STATUS_ERR:
		return EAGAIN;
	default:
		return EPERM;
	}
}

/* cfs extent type */
#define CFS_EXTENT_TYPE_TINY 0
#define CFS_EXTENT_TYPE_NORMAL 1

/* cfs data partition type */
#define CFS_DP_TYPE_NORMAL 0
#define CFS_DP_TYPE_CACHE 1
#define CFS_DP_TYPE_PRELOAD 2

/**
 * Define data partition status.
 */
#define CFS_DP_STATUS_READONLY 1
#define CFS_DP_STATUS_READWRITE 2
#define CFS_DP_STATUS_UNAVAILABLE -1

/**
 * Define meta partition status.
 */
#define CFS_MP_STATUS_READONLY 1
#define CFS_MP_STATUS_READWRITE 2
#define CFS_MP_STATUS_UNAVAILABLE -1


/**
 * Define the package data type.
 */
#define CFS_PACKAGE_DATA_PAGE 0
#define CFS_PACKAGE_DATA_ITER 1
#define CFS_PACKAGE_READ_ITER 2
#define CFS_PACKAGE_RDMA_ITER 3

/**
 *  Define cubefs file mode, refer to "https://pkg.go.dev/io/fs#FileMode".
 */
#define CFS_MODE_DIR (1 << 31)
#define CFS_MODE_APPEND (1 << 30)
#define CFS_MODE_EXCLUSIVE (1 << 29)
#define CFS_MODE_TEMPORARY (1 << 28)
#define CFS_MODE_SYMLINK (1 << 27)
#define CFS_MODE_DEV (1 << 26)
#define CFS_MODE_NAMED_PIPE (1 << 25)
#define CFS_MODE_SOCKET (1 << 24)
#define CFS_MODE_SET_UID (1 << 23)
#define CFS_MODE_SET_GID (1 << 22)
#define CFS_MODE_CHAR_DEV (1 << 21)
#define CFS_MODE_STICKY (1 << 20)
#define CFS_MODE_IRREGULAR (1 << 19)

#define CFS_MODE_TYPE                                            \
	(CFS_MODE_DIR | CFS_MODE_SYMLINK | CFS_MODE_NAMED_PIPE | \
	 CFS_MODE_SOCKET | CFS_MODE_DEV | CFS_MODE_CHAR_DEV |    \
	 CFS_MODE_IRREGULAR)
#define CFS_MODE_PERM (0777)

static inline umode_t umode_from_u32(u32 mode)
{
	umode_t umode = mode & CFS_MODE_PERM;

	if (mode & CFS_MODE_DIR)
		umode |= S_IFDIR;
	if (mode & CFS_MODE_SYMLINK)
		umode |= S_IFLNK;
	if (!(mode & CFS_MODE_TYPE))
		umode |= S_IFREG;
	if (mode & CFS_MODE_NAMED_PIPE)
		umode |= S_IFIFO;
	if (mode & CFS_MODE_SET_UID)
		umode |= S_ISUID;
	if (mode & CFS_MODE_SET_GID)
		umode |= S_ISGID;
	if (mode & CFS_MODE_STICKY)
		umode |= S_ISVTX;
	return umode;
}

static inline u32 umode_to_u32(umode_t umode)
{
	u32 mode = umode & (S_IRWXU | S_IRWXG | S_IRWXO);

	if (S_ISDIR(umode))
		mode |= CFS_MODE_DIR;
	if (S_ISLNK(umode))
		mode |= CFS_MODE_SYMLINK;
	if (S_ISFIFO(umode))
		mode |= CFS_MODE_NAMED_PIPE;
	if (umode & S_ISUID)
		mode |= CFS_MODE_SET_UID;
	if (umode & S_ISGID)
		mode |= CFS_MODE_SET_GID;
	if (umode & S_ISVTX)
		mode |= CFS_MODE_STICKY;
	return mode;
}

/**
 * Define cubefs attribute flags.
 */
#define CFS_ATTR_MODE (1u << 0)
#define CFS_ATTR_UID (1u << 1)
#define CFS_ATTR_GID (1u << 2)
#define CFS_ATTR_MTIME (1u << 3)
#define CFS_ATTR_ATIME (1u << 4)

/**
 * Cubefs attribute flags to linux attribute flags.
 */
static inline unsigned int ia_valid_from_u32(u32 flags)
{
	unsigned int ia_valid = 0;

	if (flags & CFS_ATTR_MODE)
		ia_valid |= ATTR_MODE;
	if (flags & CFS_ATTR_UID)
		ia_valid |= ATTR_UID;
	if (flags & CFS_ATTR_GID)
		ia_valid |= ATTR_GID;
	if (flags & CFS_ATTR_MTIME)
		ia_valid |= ATTR_MTIME_SET;
	if (flags & CFS_ATTR_ATIME)
		ia_valid |= ATTR_ATIME_SET;
	return ia_valid;
}

/**
 * Linux attribute flags to cubefs attribute flags.
 */
static inline u32 ia_valid_to_u32(unsigned int ia_valid)
{
	unsigned int flags = 0;

	if (ia_valid & ATTR_MODE)
		flags |= ATTR_MODE;
	if (ia_valid & ATTR_UID)
		flags |= ATTR_UID;
	if (ia_valid & ATTR_GID)
		flags |= ATTR_GID;
	if (ia_valid & ATTR_MTIME_SET)
		flags |= ATTR_MTIME_SET;
	if (ia_valid & ATTR_ATIME_SET)
		flags |= ATTR_ATIME_SET;
	return flags;
}

struct cfs_packet_hdr {
	u8 magic;
	u8 ext_type;
	u8 opcode;
	u8 result_code;
	u8 remaining_followers;
	__be32 crc;
	__be32 size;
	__be32 arglen;
	__be64 pid;
	__be64 ext_id;
	__be64 ext_offset;
	__be64 req_id;
	__be64 kernel_offset;
} __attribute__((packed));

struct cfs_quota_info {
	u32 id;
	bool root;
};

static inline void cfs_quota_info_clear(struct cfs_quota_info *info)
{
	info->id = 0;
	info->root = 0;
}

DEFINE_ARRAY(struct, cfs_quota_info)

struct cfs_packet_inode {
	u64 ino; /* json: ino */
	umode_t mode; /* json: mode */
	u32 nlink; /* json: nlink */
	u64 size; /* json: sz */
	uid_t uid; /* json: uid */
	gid_t gid; /* json: gid */
	u64 generation; /* json: gen */
#ifdef KERNEL_HAS_TIME64_TO_TM
	struct timespec64 modify_time; /* json: mt */
	struct timespec64 create_time; /* json: ct */
	struct timespec64 access_time; /* json: at */
#else
	struct timespec modify_time; /* json: mt */
	struct timespec create_time; /* json: ct */
	struct timespec access_time; /* json: at */
#endif
	char *target; /* json: tgt */
	struct cfs_quota_info_array quota_infos; /* json: qifs */
};

static inline void cfs_packet_inode_clear(struct cfs_packet_inode *info)
{
	if (!info)
		return;
	if (info->target)
		kfree(info->target);
	cfs_quota_info_array_clear(&info->quota_infos);
	memset(info, 0, sizeof(*info));
}

struct cfs_packet_inode *cfs_packet_inode_new(void);
void cfs_packet_inode_release(struct cfs_packet_inode *info);

typedef struct cfs_packet_inode *cfs_packet_inode_ptr;

static inline void cfs_packet_inode_ptr_clear(cfs_packet_inode_ptr *info_ptr)
{
	cfs_packet_inode_release(*info_ptr);
}

DEFINE_ARRAY(, cfs_packet_inode_ptr)

struct cfs_packet_dentry {
	char *name; /* json: name */
	u64 ino; /* json: ino */
	umode_t type; /* json: type */
};

static inline void cfs_packet_dentry_clear(struct cfs_packet_dentry *dentry)
{
	if (!dentry)
		return;
	kfree(dentry->name);
	memset(dentry, 0, sizeof(*dentry));
}

DEFINE_ARRAY(struct, cfs_packet_dentry)

struct cfs_packet_extent {
	u64 file_offset; /* json: FileOffset */
	u64 pid; /* json: PartitionId */
	u64 ext_id; /* json: ExtentId */
	u64 ext_offset; /* json: ExtentOffset */
	u32 size; /* json: Size */
	u32 crc; /* json: CRC */
};

static inline void cfs_packet_extent_init(struct cfs_packet_extent *ext,
					  u64 file_offset, u64 pid, u64 ext_id,
					  u64 ext_offset, u32 size)
{
	ext->file_offset = file_offset;
	ext->pid = pid;
	ext->ext_id = ext_id;
	ext->ext_offset = ext_offset;
	ext->size = size;
	ext->crc = 0;
}

static inline void cfs_packet_extent_clear(struct cfs_packet_extent *extent)
{
	(void)extent;
}

DEFINE_ARRAY(struct, cfs_packet_extent)

struct cfs_packet_icreate_request {
	const char *vol; /* json: vol */
	u64 pid; /* json: pid */
	umode_t mode; /* json: mode */
	uid_t uid; /* json: uid */
	gid_t gid; /* json: gid */
	const char *target; /* json: tgt */
	struct cfs_quota_info_array quota_infos; /* json: qids */
};

static inline void
cfs_packet_icreate_request_clear(struct cfs_packet_icreate_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_icreate_reply {
	struct cfs_packet_inode *info;
};

static inline void
cfs_packet_icreate_reply_clear(struct cfs_packet_icreate_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_inode_release(reply->info);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_iget_request {
	const char *vol;
	u64 pid;
	u64 ino;
};

static inline void
cfs_packet_iget_request_clear(struct cfs_packet_iget_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_iget_reply {
	struct cfs_packet_inode *info;
};

static inline void
cfs_packet_iget_reply_clear(struct cfs_packet_iget_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_inode_release(reply->info);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_batch_iget_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	struct u64_array ino_vec; /* json: inos */
};

static inline void cfs_packet_batch_iget_request_clear(
	struct cfs_packet_batch_iget_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_batch_iget_reply {
	struct cfs_packet_inode_ptr_array info_vec; /* json: infos */
};

static inline void
cfs_packet_batch_iget_reply_clear(struct cfs_packet_batch_iget_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_inode_ptr_array_clear(&reply->info_vec);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_lookup_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 parent_ino; /* json: pino */
	struct qstr *name; /* json: name */
};

static inline void
cfs_packet_lookup_request_clear(struct cfs_packet_lookup_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_lookup_reply {
	u64 ino; /* json: ino */
	umode_t mode; /* json: mode */
};

static inline void
cfs_packet_lookup_reply_clear(struct cfs_packet_lookup_reply *reply)
{
	if (!reply)
		return;
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_readdir_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 parent_ino; /* json: pino */
	const char *marker; /* json: marker */
	u64 limit; /* json: limit */
};

static inline void
cfs_packet_readdir_request_clear(struct cfs_packet_readdir_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_readdir_reply {
	struct cfs_packet_dentry_array children; /* json: children */
};

static inline void
cfs_packet_readdir_reply_clear(struct cfs_packet_readdir_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_dentry_array_clear(&reply->children);
}

struct cfs_packet_dcreate_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 parent_ino; /* json : pino */
	u64 ino; /* json : ino */
	struct qstr *name; /* json : name */
	umode_t mode; /* json : mode */
	struct cfs_quota_info_array quota_infos; /* json : qids */
};

static inline void
cfs_packet_dcreate_request_clear(struct cfs_packet_dcreate_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_ddelete_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 parent_ino; /* json : pino */
	struct qstr *name; /* json : name */
};

static inline void
cfs_packet_ddelete_request_clear(struct cfs_packet_ddelete_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_ddelete_reply {
	u64 ino; /* json : ino */
};

static inline void
cfs_packet_ddelete_reply_clear(struct cfs_packet_ddelete_reply *reply)
{
	if (!reply)
		return;
	reply->ino = 0;
}

struct cfs_packet_dupdate_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 parent_ino; /* json : pino */
	u64 ino; /* json : ino */
	struct qstr *name; /* json : name */
};

static inline void
cfs_packet_dupdate_request_clear(struct cfs_packet_dupdate_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_dupdate_reply {
	u64 ino; /* json : ino */
};

static inline void
cfs_packet_dupdate_reply_clear(struct cfs_packet_dupdate_reply *reply)
{
	if (!reply)
		return;
	reply->ino = 0;
}

struct cfs_packet_ilink_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 ino; /* json : ino */
	u64 uniqid; /* json : uid */
};

static inline void
cfs_packet_ilink_request_clear(struct cfs_packet_ilink_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_ilink_reply {
	struct cfs_packet_inode *info; /* json : info */
};

struct cfs_packet_iunlink_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 ino; /* json : ino */
	u64 uniqid; /* json : uid */
};

static inline void
cfs_packet_iunlink_request_clear(struct cfs_packet_iunlink_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_iunlink_reply {
	struct cfs_packet_inode *info; /* json : info */
};

static inline void
cfs_packet_ilink_reply_clear(struct cfs_packet_ilink_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_inode_release(reply->info);
	reply->info = NULL;
}

static inline void
cfs_packet_iunlink_reply_clear(struct cfs_packet_iunlink_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_inode_release(reply->info);
	reply->info = NULL;
}

struct cfs_packet_ievict_request {
	const char *vol_name; /* json : vol */
	u64 pid; /* json : pid */
	u64 ino; /* json : ino */
};

static inline void
cfs_packet_ievict_request_clear(struct cfs_packet_ievict_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_sattr_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	umode_t mode; /* json: mode */
	uid_t uid; /* json: uid */
	gid_t gid; /* json: gid */
#ifdef KERNEL_HAS_TIME64_TO_TM
	struct timespec64 modify_time; /* json: mt, s64 */
	struct timespec64 access_time; /* json: at, s64 */
#else
	struct timespec modify_time; /* json: mt, s64 */
	struct timespec access_time; /* json: at, s64 */
#endif
	u32 valid; /* json: valid */
};

static inline void
cfs_packet_sattr_request_clear(struct cfs_packet_sattr_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_sxattr_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	const char *key; /* json: key */
	const char *value; /* json: value */
	size_t value_len;
};

static inline void
cfs_packet_sxattr_request_clear(struct cfs_packet_sxattr_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_gxattr_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	const char *key; /* json: key */
};

static inline void
cfs_packet_gxattr_request_clear(struct cfs_packet_gxattr_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_gxattr_reply {
	char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	char *key; /* json: key */
	char *value; /* json: value */
};

static inline void
cfs_packet_gxattr_reply_clear(struct cfs_packet_gxattr_reply *reply)
{
	if (!reply)
		return;
	if (reply->vol_name)
		kfree(reply->vol_name);
	if (reply->key)
		kfree(reply->key);
	if (reply->value)
		kfree(reply->value);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_rxattr_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	const char *key; /* json: key */
};

static inline void
cfs_packet_rxattr_request_clear(struct cfs_packet_rxattr_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_lxattr_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
};

static inline void
cfs_packet_lxattr_request_clear(struct cfs_packet_lxattr_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_lxattr_reply {
	char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	struct string_array xattrs; /* json: xattrs */
};

static inline void
cfs_packet_lxattr_reply_clear(struct cfs_packet_lxattr_reply *reply)
{
	if (!reply)
		return;
	if (reply->vol_name)
		kfree(reply->vol_name);
	string_array_clear(&reply->xattrs);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_truncate_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	u64 size; /* json: sz */
};

static inline void
cfs_packet_truncate_request_clear(struct cfs_packet_truncate_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_lextent_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
};

static inline void
cfs_packet_lextent_request_clear(struct cfs_packet_lextent_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_lextent_reply {
	u64 generation; /* json: gen */
	u64 size; /* json: sz */
	struct cfs_packet_extent_array extents; /* json: eks */
};

static inline void
cfs_packet_lextent_reply_clear(struct cfs_packet_lextent_reply *reply)
{
	if (!reply)
		return;
	cfs_packet_extent_array_clear(&reply->extents);
	memset(reply, 0, sizeof(*reply));
}

struct cfs_packet_aextent_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
	struct cfs_packet_extent extent; /* json: ek */
	struct cfs_packet_extent_array discard_extents; /* json: dek */
};

static inline void
cfs_packet_aextent_request_clear(struct cfs_packet_aextent_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_uniqid_request {
	const char *vol_name; /* json: vol */
	u64 pid; /* json: pid */
	u32 num; /* json: num */
};

static inline void
cfs_packet_uniqid_request_clear(struct cfs_packet_uniqid_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_uniqid_reply {
	u64 start; /* json: start */
};

static inline void
cfs_packet_uniqid_reply_clear(struct cfs_packet_uniqid_reply *reply)
{
	if (!reply)
		return;
	reply->start = 0;
}

struct cfs_packet_gquota_request {
	u64 pid; /* json: pid */
	u64 ino; /* json: ino */
};

static inline void
cfs_packet_gquota_request_clear(struct cfs_packet_gquota_request *request)
{
	if (!request)
		return;
	memset(request, 0, sizeof(*request));
}

struct cfs_packet_gquota_reply {
	struct cfs_quota_info_array quota_infos; /* json: MetaQuotaInfoMap */
};

static inline void
cfs_packet_gquota_reply_init(struct cfs_packet_gquota_reply *reply)
{
	if (!reply)
		return;
	memset(reply, 0, sizeof(*reply));
}

static inline void
cfs_packet_gquota_reply_clear(struct cfs_packet_gquota_reply *reply)
{
	if (!reply)
		return;
	cfs_quota_info_array_clear(&reply->quota_infos);
}

struct request_hdr_padding {
	uint64_t VerSeq; // only used in mod request to datanode
	unsigned char
		arg[40]; // for create or append ops, the data contains the address
	unsigned char list[40];
	uint8_t RdmaVersion; //rdma version
	uint64_t RdmaAddr;
	uint32_t RdmaLength;
	uint32_t RdmaKey;
}__attribute__((packed)); //

struct reply_hdr_padding {
	uint64_t VerSeq; // only used in mod request to datanode
	unsigned char
		arg[40]; // for create or append ops, the data contains the address
	unsigned char data[500];
	unsigned char list[40];
	uint8_t RdmaVersion; //rdma version
	uint64_t RdmaAddr;
	uint32_t RdmaLength;
	uint32_t RdmaKey;
}__attribute__((packed));

struct cfs_packet {
	struct {
		struct cfs_packet_hdr hdr;
		struct request_hdr_padding hdr_padding;
		struct cfs_buffer *arg;
		union {
			struct cfs_packet_icreate_request icreate;
			struct cfs_packet_iget_request iget;
			struct cfs_packet_batch_iget_request batch_iget;
			struct cfs_packet_lookup_request ilookup;
			struct cfs_packet_readdir_request readdir;
			struct cfs_packet_dcreate_request dcreate;
			struct cfs_packet_ddelete_request ddelete;
			struct cfs_packet_dupdate_request dupdate;
			struct cfs_packet_ilink_request ilink;
			struct cfs_packet_iunlink_request iunlink;
			struct cfs_packet_ievict_request ievict;
			struct cfs_packet_sattr_request sattr;
			struct cfs_packet_sxattr_request sxattr;
			struct cfs_packet_gxattr_request gxattr;
			struct cfs_packet_rxattr_request rxattr;
			struct cfs_packet_lxattr_request lxattr;
			struct cfs_packet_lextent_request lextent;
			struct cfs_packet_aextent_request aextent;
			struct cfs_packet_truncate_request truncate;
			struct cfs_packet_uniqid_request uniqid;
			struct cfs_packet_gquota_request gquota;
			__be64 ino; /* extent create */
			struct {
				struct cfs_page_frag *frags;
				size_t nr;
			} write;
			struct iov_iter iter;
		} data;
		struct iovec iov;
	} __attribute__((packed)) request;
	struct {
		struct cfs_packet_hdr hdr;
		struct reply_hdr_padding hdr_padding;
		struct cfs_buffer *arg;
		union {
			struct cfs_packet_icreate_reply icreate;
			struct cfs_packet_iget_reply iget;
			struct cfs_packet_batch_iget_reply batch_iget;
			struct cfs_packet_lookup_reply ilookup;
			struct cfs_packet_readdir_reply readdir;
			struct cfs_packet_ddelete_reply ddelete;
			struct cfs_packet_dupdate_reply dupdate;
			struct cfs_packet_ilink_reply ilink;
			struct cfs_packet_iunlink_reply iunlink;
			struct cfs_packet_gxattr_reply gxattr;
			struct cfs_packet_lxattr_reply lxattr;
			struct cfs_packet_lextent_reply lextent;
			struct cfs_packet_uniqid_reply uniqid;
			struct cfs_packet_gquota_reply gquota;
			struct {
				struct cfs_page_frag *frags;
				size_t nr;
			} read;
			struct iov_iter *user_iter;
		} data;
	} __attribute__((packed)) reply;
	struct list_head list;
	atomic_t refcnt;
	struct completion done;
	int error;
	void (*handle_reply)(struct cfs_packet *);
	void *private;
	union {
		struct cfs_page_frag frags[CFS_PAGE_VEC_NUM];
	} rw;
	int data_buf_index;
	int pkg_data_type;
};

struct cfs_packet *cfs_packet_new(u8 op, u64 pid,
				  void (*handle_reply)(struct cfs_packet *),
				  void *private);
void cfs_packet_release(struct cfs_packet *packet);

static inline void cfs_packet_get(struct cfs_packet *packet)
{
	atomic_inc(&packet->refcnt);
}

#define cfs_packet_put(packet) cfs_packet_release(packet)

static inline void cfs_packet_complete_reply(struct cfs_packet *packet)
{
	complete(&packet->done);
}

static inline void cfs_packet_wait_reply(struct cfs_packet *packet)
{
	wait_for_completion(&packet->done);
}

static inline void cfs_packet_set_callback(struct cfs_packet *packet,
					   void (*cb)(struct cfs_packet *),
					   void *private)
{
	packet->handle_reply = cb;
	packet->private = private;
}

static inline void cfs_packet_set_request_arg(struct cfs_packet *packet,
					      struct cfs_buffer *arg)
{
	packet->request.arg = arg;
	packet->request.hdr.arglen = cpu_to_be32(cfs_buffer_size(arg));
}

/**
 * free memory.
 */
void cfs_packet_request_data_clear(struct cfs_packet *packet);

/**
 * free memory.
 */
void cfs_packet_reply_data_clear(struct cfs_packet *packet);

/**
 * @return 0 on success, -ENOMEM if alloc memory failed.
 */
int cfs_packet_request_data_to_json(struct cfs_packet *packet,
				    struct cfs_buffer *buffer);

int cfs_packet_reply_data_from_json(cfs_json_t *json,
				    struct cfs_packet *packet);

static inline void cfs_packet_clear(struct cfs_packet *packet)
{
	if (!packet)
		return;
	if (packet->reply.arg)
		cfs_buffer_release(packet->reply.arg);
	if (packet->pkg_data_type == CFS_PACKAGE_DATA_ITER) {
		kfree(packet->request.iov.iov_base);
	}
	cfs_packet_request_data_clear(packet);
	cfs_packet_reply_data_clear(packet);
	memset(packet, 0, sizeof(*packet));
}

/************************************************************************************
 * Master
 ************************************************************************************/

struct cfs_meta_partition_view {
	u64 id; /* json: PartitionID */
	u64 start_ino; /* json: Start */
	u64 end_ino; /* json: End */
	u64 max_ino; /* json: MaxInodeID */
	u64 inode_count; /* json: InodeCount */
	u64 dentry_count; /* json: DentryCount */
	struct sockaddr_storage *leader; /* json: LeaderAddr */
	struct sockaddr_storage_array members; /* json: Members */
	bool is_recover; /* json: IsRecover */
	s8 status; /* json: Status */
};

static inline void
cfs_meta_partition_view_init(struct cfs_meta_partition_view *mp_view)
{
	memset(mp_view, 0, sizeof(*mp_view));
}

static inline void
cfs_meta_partition_view_clear(struct cfs_meta_partition_view *mp_view)
{
	if (!mp_view)
		return;
	if (mp_view->leader)
		kfree(mp_view->leader);
	sockaddr_storage_array_clear(&mp_view->members);
	memset(mp_view, 0, sizeof(*mp_view));
}

int cfs_meta_partition_view_from_json(cfs_json_t *json,
				      struct cfs_meta_partition_view *mp_view);

DEFINE_ARRAY(struct, cfs_meta_partition_view)
struct cfs_data_partition_view {
	u32 type; /* json: PartitionType */
	u64 id; /* json: PartitionID */
	s8 status; /* json: Status */
	u8 replica_num; /* json: ReplicaNum */
	struct sockaddr_storage *leader; /* json: LeaderAddr */
	struct sockaddr_storage_array members; /* json: Hosts */
	u64 epoch; /* json: Epoch */
	s64 ttl; /* json: PartitionTTL */
	bool is_recover; /* json: IsRecover */
	bool is_discard; /* json: IsDiscard */
};

static inline void
cfs_data_partition_view_init(struct cfs_data_partition_view *dp_view)
{
	memset(dp_view, 0, sizeof(*dp_view));
}

static inline void
cfs_data_partition_view_clear(struct cfs_data_partition_view *dp_view)
{
	if (!dp_view)
		return;
	if (dp_view->leader)
		kfree(dp_view->leader);
	sockaddr_storage_array_clear(&dp_view->members);
	memset(dp_view, 0, sizeof(*dp_view));
}

int cfs_data_partition_view_from_json(cfs_json_t *json,
				      struct cfs_data_partition_view *dp_view);

DEFINE_ARRAY(struct, cfs_data_partition_view)

struct cfs_volume_view {
	char *name; /* json: Name */
	char *owner; /* json: Owner */
	u8 status; /* json: Status */
	bool follower_read; /* json: FollowerRead */
	struct cfs_meta_partition_view_array
		meta_partitions; /* json: MetaPartitions */
};

static inline void cfs_volume_view_init(struct cfs_volume_view *vol_view)
{
	memset(vol_view, 0, sizeof(*vol_view));
}

static inline void cfs_volume_view_clear(struct cfs_volume_view *vol_view)
{
	if (!vol_view)
		return;
	if (vol_view->name)
		kfree(vol_view->name);
	if (vol_view->owner)
		kfree(vol_view->owner);
	cfs_meta_partition_view_array_clear(&vol_view->meta_partitions);
	memset(vol_view, 0, sizeof(*vol_view));
}

int cfs_volume_view_from_json(cfs_json_t *json,
			      struct cfs_volume_view *vol_view);

struct cfs_volume_stat {
	char *name; /* json: Name */
	u64 total_size; /* json: TotalSize */
	u64 used_size; /* json: UsedSize */
	char *used_ratio; /* json: UsedRatio */
	u64 cache_total_size; /* json: CacheTotalSize */
	u64 cache_used_size; /* json: CacheUsedSize */
	char *cache_used_ratio; /* json: CacheUsedRatio */
	bool enable_token; /* json: EnableToken */
	u64 inode_count; /* json: InodeCount */
	bool dp_read_only_when_vol_full; /* json: DpReadOnlyWhenVolFull */
};

static inline void cfs_volume_stat_init(struct cfs_volume_stat *stat)
{
	memset(stat, 0, sizeof(*stat));
}
static inline void cfs_volume_stat_clear(struct cfs_volume_stat *stat)
{
	if (!stat)
		return;
	kfree(stat->name);
	kfree(stat->used_ratio);
	kfree(stat->cache_used_ratio);
	memset(stat, 0, sizeof(*stat));
}

int cfs_volume_stat_from_json(cfs_json_t *json, struct cfs_volume_stat *stat);

struct cfs_cluster_info {
	u32 links_limit; /* json: DirChildrenNumLimit */
};

static inline void cfs_cluster_info_init(struct cfs_cluster_info *info)
{
	memset(info, 0, sizeof(*info));
}

static inline void cfs_cluster_info_clear(struct cfs_cluster_info *info)
{
	if (!info)
		return;
	memset(info, 0, sizeof(*info));
}

int cfs_cluster_info_from_json(cfs_json_t *json, struct cfs_cluster_info *info);

int cfs_packet_module_init(void);
void cfs_packet_module_exit(void);
#endif
