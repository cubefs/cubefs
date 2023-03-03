package main

import "fmt"

var (
	umpKeyVolArr     []string
	umpKeyClusterArr []string
)

const (
	ump_cfs_close = iota
	ump_cfs_open
	ump_cfs_rename
	ump_cfs_truncate
	ump_cfs_ftruncate
	ump_cfs_fallocate
	ump_cfs_posix_fallocate
	ump_cfs_flush
	ump_cfs_flush_redolog
	ump_cfs_flush_binlog
	ump_cfs_flush_relaylog
	ump_cfs_mkdirs
	ump_cfs_rmdir
	ump_cfs_link
	ump_cfs_symlink
	ump_cfs_unlink
	ump_cfs_readlink
	ump_cfs_stat
	ump_cfs_stat64
	ump_cfs_chmod
	ump_cfs_fchmod
	ump_cfs_chown
	ump_cfs_fchown
	ump_cfs_utimens
	ump_cfs_faccessat
	ump_cfs_read
	ump_cfs_read_binlog
	ump_cfs_read_relaylog
	ump_cfs_write
	ump_cfs_write_redolog
	ump_cfs_write_binlog
	ump_cfs_write_relaylog
)

func init() {
	umpKeyVolArr = make([]string, 0)
	umpKeyClusterArr = make([]string, 0)
}

func (c *client) initUmpKeys() {
	volKeyPrefix := fmt.Sprintf("%s_%s_", c.mw.Cluster(), c.volName)
	clusterKeyPrefix := fmt.Sprintf("%s_%s_", c.mw.Cluster(), gClientManager.moduleName)
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_close", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_open", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_rename", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_truncate", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_ftruncate", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_fallocate", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_posix_fallocate", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_flush", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_flush_redolog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_flush_binlog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_flush_relaylog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_mkdirs", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_rmdir", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_link", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_symlink", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_unlink", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_readlink", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_stat", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_stat64", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_chmod", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_fchmod", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_chown", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_fchown", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_utimens", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_faccessat", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_read", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_read_binlog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_read_relaylog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_write", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_write_redolog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_write_binlog", volKeyPrefix))
	umpKeyVolArr = append(umpKeyVolArr, fmt.Sprintf("%vcfs_write_relaylog", volKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_close", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_open", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_rename", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_truncate", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_ftruncate", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_fallocate", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_posix_fallocate", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_flush", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_redolog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_binlog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_relaylog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_mkdirs", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_rmdir", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_link", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_symlink", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_unlink", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_readlink", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_stat", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_stat64", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_chmod", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_fchmod", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_chown", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_fchown", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_utimens", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_faccessat", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_read", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_read_binlog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_read_relaylog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_write", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_write_redolog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_write_binlog", clusterKeyPrefix))
	umpKeyClusterArr = append(umpKeyClusterArr, fmt.Sprintf("%vcfs_write_relaylog", clusterKeyPrefix))
}

func (c *client) umpFunctionKeyFast(act int) string {
	return umpKeyVolArr[act]
}

func (c *client) umpFunctionGeneralKeyFast(act int) string {
	return umpKeyClusterArr[act]
}
