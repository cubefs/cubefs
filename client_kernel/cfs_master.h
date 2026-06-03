/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_MASTER_H__
#define __CFS_MASTER_H__

#include "cfs_common.h"

#include "cfs_log.h"
#include "cfs_packet.h"

struct cfs_master_client {
	char *volume;
	char auth_key[MD5_DIGEST_SIZE * 2 + 1];
	struct sockaddr_storage_array hosts;
	struct cfs_log *log;
};

struct cfs_master_client *
cfs_master_client_new(const struct sockaddr_storage_array *hosts,
		      const char *volume, const char *owner,
		      struct cfs_log *log);
void cfs_master_client_release(struct cfs_master_client *mc);
int cfs_master_get_volume(struct cfs_master_client *mc,
			  struct cfs_volume_view *vol_view);
int cfs_master_get_volume_stat(struct cfs_master_client *mc,
			       struct cfs_volume_stat *stat);
int cfs_master_get_data_partitions(
	struct cfs_master_client *mc,
	struct cfs_data_partition_view_array *dp_views);
int cfs_master_get_cluster_info(struct cfs_master_client *mc,
				struct cfs_cluster_info *info);
#endif
