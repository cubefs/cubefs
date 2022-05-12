package mysql

import (
	"database/sql"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"time"
)

const (
	ColumnTaskCreateTime    = "task_create_time"
	ColumnTaskUpdateTime    = "task_update_time"
)

// add task to task history table
func AddTaskToHistory(task *proto.Task) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddTaskToHistory)
	defer metrics.Set(err)

	sqlCmd := "insert into task_history(task_id, task_type, cluster_name, vol_name, dp_id, mp_id, task_info, worker_addr, task_status, exception_info, task_create_time, task_update_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, task.TaskId)
	args = append(args, int8(task.TaskType))
	args = append(args, task.Cluster)
	args = append(args, task.VolName)
	args = append(args, task.DpId)
	args = append(args, task.MpId)
	args = append(args, task.TaskInfo)
	args = append(args, task.WorkerAddr)
	args = append(args, task.Status)
	args = append(args, task.ExceptionInfo)
	args = append(args, task.CreateTime)
	args = append(args, task.UpdateTime)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddTaskToHistory] add task failed, cluster(%v), volName(%v), taskInfo(%v), err(%v)", task.Cluster, task.VolName, task.TaskInfo, err)
		return
	}
	return
}

func AddTasksToHistory(tasks []*proto.Task) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddTasksToHistory)
	defer metrics.Set(err)

	sqlCmd := "insert into task_history(task_id, task_type, cluster_name, vol_name, dp_id, mp_id, task_info, worker_addr, task_status, exception_info, task_create_time, task_update_time) values"
	args := make([]interface{}, 0)

	for index, task := range tasks {
		sqlCmd += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		if index < len(tasks)-1 {
			sqlCmd += ","
		}
		args = append(args, task.TaskId)
		args = append(args, int8(task.TaskType))
		args = append(args, task.Cluster)
		args = append(args, task.VolName)
		args = append(args, task.DpId)
		args = append(args, task.MpId)
		args = append(args, task.TaskInfo)
		args = append(args, task.WorkerAddr)
		args = append(args, task.Status)
		args = append(args, task.ExceptionInfo)
		args = append(args, task.CreateTime)
		args = append(args, task.UpdateTime)
	}
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddTasksToHistory] add exception worker failed, workers(%v), err(%v)", tasks, err)
		return
	}
	return
}

func SelectTaskHistory(cluster, volume string, dpId, mpId uint64, taskType, limit, offset int) (taskHistories []*proto.TaskHistory, err error) {
	sqlCmd := fmt.Sprintf("select %s from task_history", taskHistoryColumns())
	conditions := make([]string, 0)
	values := make([]interface{}, 0)
	if !util.IsStrEmpty(cluster) {
		conditions = append(conditions, " cluster_name = ?")
		values = append(values, cluster)
	}
	if !util.IsStrEmpty(volume) {
		conditions = append(conditions, " vol_name = ?")
		values = append(values, volume)
	}
	if taskType != 0 {
		conditions = append(conditions, " task_type = ?")
		values = append(values, taskType)
	}
	if dpId != 0 {
		conditions = append(conditions, " dp_id = ?")
		values = append(values, dpId)
	}
	if mpId != 0 {
		conditions = append(conditions, " mp_id = ?")
		values = append(values, mpId)
	}
	if len(conditions) > 0 {
		sqlCmd += " where"
		for index, condition := range conditions {
			if index != 0 {
				sqlCmd += " and"
			}
			sqlCmd += condition
		}
	}
	sqlCmd += fmt.Sprintf(" limit ? offset ?")
	values = append(values, limit)
	values = append(values, offset)

	var rows *sql.Rows
	rows, err = db.Query(sqlCmd, values...)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.TaskHistory{}
		var tct string
		var tut string
		var ct string
		var taskCreateTime time.Time
		var taskUpdateTime time.Time
		var createTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &tct, &tut, &ct)
		if err != nil {
			return
		}
		if taskCreateTime, err = FormatTime(tct); err != nil {
			return
		}
		if taskUpdateTime, err = FormatTime(tut); err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		t.TaskCreateTime = taskCreateTime
		t.TaskUpdateTime = taskUpdateTime
		t.CreateTime = createTime
		taskHistories = append(taskHistories, t)
	}
	return
}

func DeleteTaskHistories(ths []*proto.TaskHistory) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteTaskHistory)
	defer metrics.Set(err)

	sbPlaceholder := strings.Builder{}
	args := make([]interface{}, 0)
	for index, th := range ths {
		args = append(args, th.TaskId)
		sbPlaceholder.WriteString("?")
		if index != len(ths)-1 {
			sbPlaceholder.WriteString(",")
		}
	}
	sqlCmd := fmt.Sprintf("delete from task_history where task_id in (%s)", sbPlaceholder.String())
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[DeleteTaskHistory] delete task histories via task id failed, taskIds(%v)", args)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	log.LogInfof("[DeleteTaskHistory] delete task histories rows: %v, args(%v)", nums, args)
	return
}

func taskHistoryColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, ColumnTaskId)
	columnSlice = append(columnSlice, ColumnTaskType)
	columnSlice = append(columnSlice, ColumnClusterName)
	columnSlice = append(columnSlice, ColumnVolumeName)
	columnSlice = append(columnSlice, ColumnDPId)
	columnSlice = append(columnSlice, ColumnMPId)
	columnSlice = append(columnSlice, ColumnTaskInfo)
	columnSlice = append(columnSlice, ColumnWorkerAddr)
	columnSlice = append(columnSlice, ColumnTaskStatus)
	columnSlice = append(columnSlice, ColumnExceptionInfo)
	columnSlice = append(columnSlice, ColumnTaskCreateTime)
	columnSlice = append(columnSlice, ColumnTaskUpdateTime)
	columnSlice = append(columnSlice, ColumnCreateTime)
	return strings.Join(columnSlice, ",")
}
