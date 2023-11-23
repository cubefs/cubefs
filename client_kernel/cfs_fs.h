#ifndef __CFS_FS_H__
#define __CFS_FS_H__

#include "cfs_common.h"

#include "cfs_extent.h"
#include "cfs_log.h"
#include "cfs_master.h"
#include "cfs_meta.h"
#include "cfs_option.h"

extern const struct address_space_operations cfs_address_ops;
extern const struct file_operations cfs_file_fops;
extern const struct inode_operations cfs_file_iops;
extern const struct file_operations cfs_dir_fops;
extern const struct inode_operations cfs_dir_iops;
extern const struct inode_operations cfs_symlink_iops;
extern const struct inode_operations cfs_special_iops;
extern const struct dentry_operations cfs_dentry_ops;
extern const struct super_operations cfs_super_ops;
extern struct file_system_type cfs_fs_type;

struct cfs_mount_info {
	struct cfs_options *options;
	struct proc_dir_entry *proc_dir;
	struct proc_dir_entry *proc_log;
	struct cfs_log *log;
	struct cfs_master_client *master;
	struct cfs_meta_client *meta;
	struct cfs_extent_client *ec;
	atomic_long_t links_limit;
	struct delayed_work update_limit_work;
};

struct cfs_mount_info *cfs_mount_info_new(struct cfs_options *options);
void cfs_mount_info_release(struct cfs_mount_info *cmi);
int cfs_fs_module_init(void);
void cfs_fs_module_exit(void);
#endif
