/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_stats.h"

#include <linux/proc_fs.h>
#include <linux/seq_file.h>

const u64 cfs_lat_bound_us[CFS_LAT_NBUCKET] = {
	1000, 5000, 10000, 50000, 100000, 500000, 1000000, 0 /* +Inf */
};

const char *const cfs_op_name[CFS_VOP_MAX] = {
	"open",	    "release",	 "fsync",    "flush",	 "setattr",
	"getattr",  "lookup",	 "create",   "link",	 "symlink",
	"mkdir",    "rmdir",	 "mknod",    "rename",	 "unlink",
	"readdir",  "listxattr", "getxattr", "setxattr",
};

struct cfs_stats *cfs_stats_new(void)
{
	struct cfs_stats *stats;

	/* kzalloc 全零位即 atomic64 初值 0 */
	stats = kzalloc(sizeof(*stats), GFP_KERNEL);
	if (!stats)
		return ERR_PTR(-ENOMEM);
	return stats;
}

void cfs_stats_release(struct cfs_stats *stats)
{
	kfree(stats);
}

/*
 * /proc/fs/cubefs/<vol>/stats 快照(纯数值,exporter 按空格分列解析):
 *   version 1
 *   buckets 1000 5000 10000 50000 100000 500000 1000000 +Inf
 *   op <name> <count> <sum_us> <errs> <b0> ... <b7>
 *   io read  <ops> <bytes>
 *   io write <ops> <bytes>
 */
static int cfs_stats_seq_show(struct seq_file *m, void *v)
{
	struct cfs_stats *stats = m->private;
	int op, i;

	seq_puts(m, "version 1\n");
	seq_puts(m, "buckets");
	for (i = 0; i < CFS_LAT_NBUCKET; i++) {
		if (cfs_lat_bound_us[i] == 0)
			seq_puts(m, " +Inf");
		else
			seq_printf(m, " %llu", cfs_lat_bound_us[i]);
	}
	seq_putc(m, '\n');

	for (op = 0; op < CFS_VOP_MAX; op++) {
		struct cfs_op_stat *o = &stats->op[op];

		seq_printf(m, "op %s %lld %lld %lld", cfs_op_name[op],
			   atomic64_read(&o->count), atomic64_read(&o->sum_us),
			   atomic64_read(&o->errs));
		for (i = 0; i < CFS_LAT_NBUCKET; i++)
			seq_printf(m, " %lld", atomic64_read(&o->bucket[i]));
		seq_putc(m, '\n');
	}

	seq_printf(m, "io read %lld %lld\n", atomic64_read(&stats->io_read_ops),
		   atomic64_read(&stats->io_read_bytes));
	seq_printf(m, "io write %lld %lld\n",
		   atomic64_read(&stats->io_write_ops),
		   atomic64_read(&stats->io_write_bytes));
	return 0;
}

static int cfs_stats_proc_open(struct inode *inode, struct file *file)
{
	return single_open(file, cfs_stats_seq_show, PDE_DATA(inode));
}

const struct proc_ops cfs_stats_fops = {
	.proc_open = cfs_stats_proc_open,
	.proc_read = seq_read,
	.proc_lseek = seq_lseek,
	.proc_release = single_release,
};
