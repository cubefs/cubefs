/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_STATS_H__
#define __CFS_STATS_H__

#include "cfs_common.h"

/*
 * VFS op 分类:序号即 /proc/fs/cubefs/<vol>/stats 的输出顺序。新增 op 只能追加到
 * CFS_VOP_MAX 之前,以保持 exporter 按名解析的前后兼容(旧 exporter 忽略未知名)。
 */
enum cfs_op {
	CFS_VOP_OPEN = 0,
	CFS_VOP_RELEASE,
	CFS_VOP_FSYNC,
	CFS_VOP_FLUSH,
	CFS_VOP_SETATTR,
	CFS_VOP_GETATTR,
	CFS_VOP_LOOKUP,
	CFS_VOP_CREATE,
	CFS_VOP_LINK,
	CFS_VOP_SYMLINK,
	CFS_VOP_MKDIR,
	CFS_VOP_RMDIR,
	CFS_VOP_MKNOD,
	CFS_VOP_RENAME,
	CFS_VOP_UNLINK,
	CFS_VOP_READDIR,
	CFS_VOP_LISTXATTR,
	CFS_VOP_GETXATTR,
	CFS_VOP_SETXATTR,
	CFS_VOP_MAX,
};

/*
 * latency 直方图桶上界(微秒);末项哨兵 0 表示 +Inf。bucket 存"落在该区间"的次数
 * (互斥分桶,非累积),exporter 侧做前缀和转成 Prometheus 累积 le 桶。改桶边界只需
 * 同步改此宏与 cfs_stats.c 的 cfs_lat_bound_us 表,exporter 经 proc "buckets" 行跟随。
 */
#define CFS_LAT_NBUCKET 8

extern const u64 cfs_lat_bound_us[CFS_LAT_NBUCKET];
extern const char *const cfs_op_name[CFS_VOP_MAX];

struct cfs_op_stat {
	atomic64_t count; /* 调用次数(含失败) */
	atomic64_t sum_us; /* 累计延迟(微秒) */
	atomic64_t errs; /* ret < 0 次数 */
	atomic64_t bucket[CFS_LAT_NBUCKET]; /* 互斥分桶计数 */
};

struct cfs_stats {
	struct cfs_op_stat op[CFS_VOP_MAX];
	atomic64_t io_read_bytes;
	atomic64_t io_read_ops;
	atomic64_t io_write_bytes;
	atomic64_t io_write_ops;
};

struct cfs_stats *cfs_stats_new(void);
void cfs_stats_release(struct cfs_stats *stats);

struct proc_ops;
extern const struct proc_ops cfs_stats_fops;

/*
 * 元数据 op 打点:在各 op 末尾(已算出 elapsed)调用。纯 atomic64、零分配、不睡眠、
 * O(CFS_LAT_NBUCKET) 常数 —— 内核进程上下文热路径安全。stats 为空(分配失败)跳过。
 */
static inline void cfs_stat_record(struct cfs_stats *stats, enum cfs_op op,
				   u64 us, int err)
{
	struct cfs_op_stat *o;
	int i;

	if (unlikely(!stats) || unlikely((unsigned int)op >= CFS_VOP_MAX))
		return;
	o = &stats->op[op];
	atomic64_inc(&o->count);
	atomic64_add(us, &o->sum_us);
	if (err < 0)
		atomic64_inc(&o->errs);
	for (i = 0; i < CFS_LAT_NBUCKET; i++) {
		if (cfs_lat_bound_us[i] == 0 || us <= cfs_lat_bound_us[i]) {
			atomic64_inc(&o->bucket[i]);
			break;
		}
	}
}

/* 数据 IO 吞吐打点:read/write 字节数 + 操作次数(不做 per-page latency)。 */
static inline void cfs_stat_io(struct cfs_stats *stats, bool is_write,
			       u64 bytes)
{
	if (unlikely(!stats))
		return;
	if (is_write) {
		atomic64_inc(&stats->io_write_ops);
		atomic64_add(bytes, &stats->io_write_bytes);
	} else {
		atomic64_inc(&stats->io_read_ops);
		atomic64_add(bytes, &stats->io_read_bytes);
	}
}

#endif
