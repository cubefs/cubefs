package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

const (
	ColumnConvertType = "convert_type"
	ColumnOldAddr     = "old_addr"
	ColumnNewAddr     = "new_addr"
	ColumnStoreMode   = "store_mode"
)

func SelectConvertTask(cType int, taskID uint64) (oldAddr, newAddr string, storeMode int, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select old_addr, new_addr, store_mode from convert_task where convert_type = ? and task_id = ?")
	rows, err = db.Query(sqlCmd, cType, taskID)
	if rows == nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&oldAddr, &newAddr, &storeMode)
		return
	}
	return
}

func AddConvertTaskInfo(task *proto.Task, cType int, oldAddr, newAddr string, storeMode int) (err error) {
	sqlCmd := fmt.Sprintf("insert into convert_task(%s) values(?,?,?,?,?,?,?,?,?)", ConvertTaskColumns())
	args := make([]interface{}, 0)
	args = append(args, task.TaskId)
	args = append(args, cType)
	args = append(args, task.Cluster)
	args = append(args, task.VolName)
	args = append(args, task.MpId)
	args = append(args, oldAddr)
	args = append(args, newAddr)
	args = append(args, storeMode)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddConvertTaskInfo] add convert task info failed, cluster(%v), volName(%v), partitionID(%v), err(%v)", task.Cluster, task.VolName, task.MpId, err)
		return
	}
	return
}

func ConvertTaskColumns() string {
	columns := make([]string, 0)
	columns = append(columns, ColumnTaskId)
	columns = append(columns, ColumnConvertType)
	columns = append(columns, ColumnClusterName)
	columns = append(columns, ColumnVolumeName)
	columns = append(columns, ColumnMPId)
	columns = append(columns, ColumnOldAddr)
	columns = append(columns, ColumnNewAddr)
	columns = append(columns, ColumnStoreMode)
	return strings.Join(columns, ",")
}