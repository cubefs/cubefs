/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_fs.h"
#include "cfs_packet.h"
#include "cfs_socket.h"
#include "cfs_rdma_socket.h"

static int __init cfs_init(void)
{
	struct proc_dir_entry *proc_dir = NULL;
	int ret;

	ret = cfs_socket_module_init();
	if (ret < 0) {
		cfs_pr_err("init socket module error %d\n", ret);
		goto exit;
	}

	ret = cfs_rdma_module_init();
	if (ret < 0) {
		cfs_pr_err("init rdma module error %d\n", ret);
		goto exit;
	}

	ret = cfs_packet_module_init();
	if (ret < 0) {
		cfs_pr_err("init packet module error %d\n", ret);
		goto exit;
	}

	ret = cfs_extent_module_init();
	if (ret < 0) {
		cfs_pr_err("init extent module error %d\n", ret);
		goto exit;
	}

	ret = cfs_fs_module_init();
	if (ret < 0) {
		cfs_pr_err("init fs module error %d\n", ret);
		goto exit;
	}

	ret = cfs_page_module_init();
	if (ret < 0) {
		cfs_pr_err("init page module error %d\n", ret);
		goto exit;
	}

	proc_dir = proc_mkdir("fs/cubefs", NULL);
	if (!proc_dir) {
		cfs_pr_err("mkdir /proc/fs/cubefs error\n");
		goto exit;
	}

	ret = register_filesystem(&cfs_fs_type);
	if (ret < 0) {
		cfs_pr_err("register file system error %d\n", ret);
		goto exit;
	}
	cfs_pr_info("init\n");
	return 0;

exit:
	cfs_socket_module_exit();
	cfs_rdma_module_exit();
	cfs_packet_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
	cfs_page_module_exit();
	if (proc_dir)
		proc_remove(proc_dir);
	return ret;
}

static void __exit cfs_exit(void)
{
	int ret;

	ret = unregister_filesystem(&cfs_fs_type);
	if (ret < 0) {
		cfs_pr_err("unregister file system error %d\n", ret);
		return;
	}
	remove_proc_entry("fs/cubefs", NULL);
	cfs_packet_module_exit();
	cfs_socket_module_exit();
	cfs_rdma_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
	cfs_page_module_exit();
	cfs_pr_info("exit\n");
}

module_init(cfs_init);
module_exit(cfs_exit);

MODULE_VERSION("0.1");
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("CubeFS");
