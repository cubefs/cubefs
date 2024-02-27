package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ColumnFinishedTime = "finished_time"
)

func AddMainCheckTask(task *proto.MainCheckTask) (taskID uint64, err error) {
	var (
		rs sql.Result
		id int64
	)

	sqlCmd := fmt.Sprintf("insert into main_tasks(%v, %v, %v) values(?, ?, ?)", ColumnTaskType, ColumnClusterName, ColumnTaskStatus)
	args := make([]interface{}, 0)
	args = append(args, int8(task.TaskType))
	args = append(args, task.Cluster)
	args = append(args, task.Status)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("add main check task failed, cluster(%v), taskType(%v), err(%v)", task.Cluster, task.TaskType, err)
		return
	}
	if id, err = rs.LastInsertId(); err != nil {
		log.LogErrorf("add main task failed when got task id, cluster(%v), taskType(%v), err(%v)", task.Cluster, task.TaskType, err)
		return
	}
	taskID = uint64(id)
	return
}

func UpdateMainCheckTaskStatusToFinished(taskID uint64) (err error) {
	var (
		rs   sql.Result
		nums int64
	)

	var sqlCmd = fmt.Sprintf("update main_tasks set %v = ?, %v = now() where %v = ?",
		ColumnTaskStatus, ColumnFinishedTime, ColumnTaskId)
	args := make([]interface{}, 0)
	args = append(args, proto.MainCheckTaskStatusFinished)
	args = append(args, taskID)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task status failed, taskId(%v), targetStatus(%v), err(%v)", taskID, proto.MainCheckTaskStatusFinished, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		return fmt.Errorf("UpdateMainCheckTaskStatus: affected rows less then one")
	}
	return
}

func SelectMainCheckTask(clusterID string, taskType int) (t *proto.MainCheckTask, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %v, %v, %v from main_tasks where %v = ? and %v = ? order by %v desc limit 1",
		ColumnTaskId, ColumnTaskStatus, ColumnCreateTime, ColumnTaskType, ColumnClusterName, ColumnCreateTime)
	rows, err = db.Query(sqlCmd, taskType, clusterID)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t = &proto.MainCheckTask{}
		var createTimeStr string
		err = rows.Scan(&t.TaskId, &t.Status, &createTimeStr)
		if err != nil {
			return
		}

		if t.CreateTime, err = FormatTime(createTimeStr); err != nil {
			return
		}
		return
	}
	return
}

