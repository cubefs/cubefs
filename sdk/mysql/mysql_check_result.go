package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
)

const (
	ColumnInodeID       = "inode_id"
	ColumnExtentID      = "extent_id"
	ColumnExtentOffset  = "extent_offset"
	ColumnSize          = "size"
	ColumnAlreadyNotify = "already_notify"
	ColumnOwnerInodes   = "owner_inodes"
	ColumnExtentCount   = "extent_count"
)

func RecordBatchNormalEKCheckResult(clusterID, volName string, eks []*proto.ExtentInfoWithInode) (err error) {
	sqlCmd := fmt.Sprintf("insert ignore into normal_ek_check_result(%s) values", normalEkCheckResultColumns())
	args := make([]interface{}, 0)
	for index := 0; index < len(eks); index++ {
		sqlCmd += "(?, ?, ?, ?, ?, ?, ?, ?)"
		if index != len(eks) - 1 {
			sqlCmd += ","
		}
		args = append(args, clusterID)
		args = append(args, volName)
		args = append(args, eks[index].Inode)
		args = append(args, eks[index].EK.PartitionId)
		args = append(args, eks[index].EK.ExtentId)
		args = append(args, eks[index].EK.ExtentOffset)
		args = append(args, eks[index].EK.Size)
		args = append(args, false)
	}

	if _, err = Transaction(sqlCmd, args); err != nil {
		return
	}
	return
}

func UpdateAlreadyNotifyNormalEKCheckResult(clusterID string) (err error) {
	sqlCmd := fmt.Sprintf("update normal_ek_check_result set %s = ? where %s = ?", ColumnAlreadyNotify, ColumnClusterName)
	args := make([]interface{}, 0)
	args = append(args, true)
	args = append(args, clusterID)
	if _, err = Transaction(sqlCmd, args); err != nil {
		return
	}
	return
}

func SelectNormalEKCheckResult(clusterID string) (r []*proto.NormalEKCheckResult, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s, %s, %s, %s, %s, %s  from normal_ek_check_result where %s = ? and %s = ?",
		ColumnVolumeName, ColumnInodeID, ColumnDPId, ColumnExtentID, ColumnExtentOffset, ColumnSize, ColumnClusterName, ColumnAlreadyNotify)
	rows, err = db.Query(sqlCmd, clusterID, false)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var checkResult = &proto.NormalEKCheckResult{}
		if err = rows.Scan(&checkResult.VolName, &checkResult.ExtInfo.Inode, &checkResult.ExtInfo.EK.PartitionId,
			&checkResult.ExtInfo.EK.ExtentId, &checkResult.ExtInfo.EK.ExtentOffset, &checkResult.ExtInfo.EK.Size); err != nil {
			return
		}
		r = append(r, checkResult)
	}
	return
}

func SelectAlreadyNotifyCheckResultByDpIdAndExtentId(clusterID, volName string, dpID, extentID uint64) (count int, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select count(*) from normal_ek_check_result where %s = ? and %s = ? and %s = ? and %s = ? and %s = ?",
		ColumnClusterName, ColumnVolumeName, ColumnDPId, ColumnExtentID, ColumnAlreadyNotify)
	rows, err = db.Query(sqlCmd, clusterID, volName, dpID, extentID, true)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&count); err != nil {
			return
		}
		return
	}
	return
}

func RecordNormalEKSearchFailedResult(clusterID, volName string, dpID, extentID uint64) (err error) {
	sqlCmd := fmt.Sprintf("insert into normal_ek_search_failed_result(%s, %s, %s, %s) values(?, ?, ?, ?)",
		ColumnClusterName, ColumnVolumeName, ColumnDPId, ColumnExtentID)
	args := make([]interface{}, 0)
	args = append(args, clusterID)
	args = append(args, volName)
	args = append(args, dpID)
	args = append(args, extentID)

	if _, err = Transaction(sqlCmd, args); err != nil {
		return
	}
	return
}

func SelectNormalEKSearchFailedResult(clusterID string) (r []*proto.NormalEKOwnerInodeSearchFailedResult, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s, %s, %s from normal_ek_search_failed_result where %s = ?", ColumnVolumeName,
		ColumnDPId, ColumnExtentID, ColumnClusterName)
	rows, err = db.Query(sqlCmd, clusterID)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ekSearchFailed = &proto.NormalEKOwnerInodeSearchFailedResult{}
		if err = rows.Scan(&ekSearchFailed.VolName, &ekSearchFailed.DataPartitionID, &ekSearchFailed.ExtentID); err != nil {
			return
		}
		r = append(r, ekSearchFailed)
	}
	return
}

func RecordNormalEKCheckFailedResult(clusterID, volumeName, failedInfo string) (err error) {
	sqlCmd := fmt.Sprintf("insert into normal_ek_check_failed_result(%s, %s, %s) values(?, ?, ?)", ColumnClusterName, ColumnVolumeName, ColumnTaskInfo)
	args := make([]interface{}, 0)
	args = append(args, clusterID)
	args = append(args, volumeName)
	args = append(args, failedInfo)
	_, err = Transaction(sqlCmd, args)
	return
}

func SelectNormalEkCheckFailedResult(clusterID string) (r []*proto.NormalEKCheckFailed, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s, %s from normal_ek_check_failed_result where %s = ?", ColumnVolumeName, ColumnTaskInfo, ColumnClusterName)
	rows, err = db.Query(sqlCmd, clusterID)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var checkFailedInfo = &proto.NormalEKCheckFailed{}
		if err = rows.Scan(&checkFailedInfo.VolName, &checkFailedInfo.FailedInfo); err != nil {
			return
		}
		r = append(r, checkFailedInfo)
	}
	return
}

func RecordNormalEKAllocateConflict(clusterID, volName string, dpID, extentID uint64, ownerInodes []uint64) (err error) {
	sqlCmd := fmt.Sprintf("insert into normal_ek_conflict_result(%s, %s, %s, %s, %s) values(?, ?, ?, ?, ?)",
		ColumnClusterName, ColumnVolumeName, ColumnDPId, ColumnExtentID, ColumnOwnerInodes)
	ownerInodesStr := make([]string, 0, len(ownerInodes))
	for _, inodeID := range ownerInodes {
		ownerInodesStr = append(ownerInodesStr, fmt.Sprintf("%v", inodeID))
	}
	args := make([]interface{}, 0)
	args = append(args, clusterID)
	args = append(args, volName)
	args = append(args, dpID)
	args = append(args, extentID)
	args = append(args, strings.Join(ownerInodesStr, ","))
	_, err = Transaction(sqlCmd, args)
	if _, err = Transaction(sqlCmd, args); err != nil {
		return
	}
	return
}

func SelectNormalEKAllocateConflict(clusterID string) (r []*proto.NormalEKAllocateConflict, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s, %s, %s, %s from normal_ek_conflict_result where %s = ?",
		ColumnVolumeName, ColumnDPId, ColumnExtentID, ColumnOwnerInodes, ColumnClusterName)
	rows, err = db.Query(sqlCmd, clusterID)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var allocateConflictInfo = &proto.NormalEKAllocateConflict{}
		var ownerInodesStr string
		if err = rows.Scan(&allocateConflictInfo.VolName, &allocateConflictInfo.DataPartitionID, &allocateConflictInfo.ExtentID, ownerInodesStr); err != nil {
			return
		}
		for _, inodeStr := range strings.Split(ownerInodesStr, ",") {
			var inodeID uint64
			if inodeID, err = strconv.ParseUint(inodeStr, 10, 64); err != nil {
				return
			}
			allocateConflictInfo.OwnerInodes = append(allocateConflictInfo.OwnerInodes, inodeID)
		}
		r = append(r, allocateConflictInfo)
	}
	return
}

func RecordEKCountOverThresholdCheckResult(clusterID, volName string, partitionID uint64, info []*proto.InodeEKCountRecord) (err error) {
	sqlCmd := fmt.Sprintf("insert ignore into ek_count_over_threshold_check_result(%s) values", ekCountOverThresholdCheckResultColumns())
	args := make([]interface{}, 0)
	for index := 0; index < len(info); index++ {
		sqlCmd += "(?, ?, ?, ?, ?, ?)"
		if index != len(info) - 1 {
			sqlCmd += ","
		}
		args = append(args, clusterID)
		args = append(args, volName)
		args = append(args, partitionID)
		args = append(args, info[index].InodeID)
		args = append(args, info[index].EKCount)
		args = append(args, false)
	}

	if _, err = Transaction(sqlCmd, args); err != nil {
		return
	}
	return
}

func SelectEKCountOverThresholdResult(clusterID string) (r []*proto.InodeEKCountRecord, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s, %s, %s, %s from ek_count_over_threshold_check_result where %s = ?",
		ColumnVolumeName, ColumnInodeID, ColumnMPId, ColumnExtentCount, ColumnClusterName)
	rows, err = db.Query(sqlCmd, clusterID)
	if err != nil || rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ekCountOverThresholdResult = &proto.InodeEKCountRecord{}
		if err = rows.Scan(&ekCountOverThresholdResult.VolName, &ekCountOverThresholdResult.InodeID,
			&ekCountOverThresholdResult.PartitionID, &ekCountOverThresholdResult.EKCount); err != nil {
			return
		}
		r = append(r, ekCountOverThresholdResult)
	}
	return
}

func CleanNormalEKCheckResult(clusterID string) (err error) {
	sqlCmd := fmt.Sprintf("delete from normal_ek_search_failed_result where %s = ? ", ColumnClusterName)
	args := make([]interface{}, 0)
	args = append(args, clusterID)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete check result failed, cluster(%v)", clusterID)
		return
	}

	sqlCmd = fmt.Sprintf("delete from normal_ek_check_failed_result where %s = ? ", ColumnClusterName)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete check result failed, cluster(%v)", clusterID)
		return
	}

	sqlCmd = fmt.Sprintf("delete from normal_ek_conflict_result where %s = ? ", ColumnClusterName)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete check result failed, cluster(%v)", clusterID)
		return
	}

	sqlCmd = fmt.Sprintf("delete from ek_count_over_threshold_check_result where %s = ?", ColumnClusterName)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete ek count over threshold failed, cluster(%v)", clusterID)
		return
	}
	return
}


func ekCountOverThresholdCheckResultColumns() string {
	columns := make([]string, 0)
	columns = append(columns, ColumnClusterName)
	columns = append(columns, ColumnVolumeName)
	columns = append(columns, ColumnMPId)
	columns = append(columns, ColumnInodeID)
	columns = append(columns, ColumnExtentCount)
	columns = append(columns, ColumnAlreadyNotify)
	return strings.Join(columns, ",")
}

func normalEkCheckResultColumns() string {
	columns := make([]string, 0)
	columns = append(columns, ColumnClusterName)
	columns = append(columns, ColumnVolumeName)
	columns = append(columns, ColumnInodeID)
	columns = append(columns, ColumnDPId)
	columns = append(columns, ColumnExtentID)
	columns = append(columns, ColumnExtentOffset)
	columns = append(columns, ColumnSize)
	columns = append(columns, ColumnAlreadyNotify)
	return strings.Join(columns, ",")
}