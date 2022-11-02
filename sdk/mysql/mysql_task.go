package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	stringutil "github.com/chubaofs/chubaofs/util/string"
	"strconv"
	"strings"
	"time"
)

const (
	ColumnTaskId        = "task_id"
	ColumnTaskType      = "task_type"
	ColumnClusterName   = "cluster_name"
	ColumnVolumeName    = "vol_name"
	ColumnDPId          = "dp_id"
	ColumnMPId          = "mp_id"
	ColumnTaskInfo      = "task_info"
	ColumnWorkerAddr    = "worker_addr"
	ColumnTaskStatus    = "task_status"
	ColumnExceptionInfo = "exception_info"
	ColumnCreateTime    = "create_time"
	ColumnUpdateTime    = "update_time"
)

const (
	DefaultTaskExceptionInfoLength = 1024
	DefaultTaskTaskInfoLength      = 1024
)

// add task to database
func AddTask(task *proto.Task) (taskId uint64, err error) {
	var (
		rs sql.Result
		id int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddTask)
	defer metrics.Set(err)

	sqlCmd := "insert into tasks(task_type, cluster_name, vol_name, dp_id, mp_id, task_info, task_status) values(?, ?, ?, ?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, int8(task.TaskType))
	args = append(args, task.Cluster)
	args = append(args, task.VolName)
	args = append(args, task.DpId)
	args = append(args, task.MpId)
	args = append(args, task.TaskInfo)
	args = append(args, task.Status)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("add task failed, cluster(%v), volName(%v), taskInfo(%v), err(%v)", task.Cluster, task.VolName, task.TaskInfo, err)
		return
	}
	if id, err = rs.LastInsertId(); err != nil {
		log.LogErrorf("add task failed when got task id, cluster(%v), volName(%v), taskInfo(%v), err(%v)", task.Cluster, task.VolName, task.TaskInfo, err)
		return
	}
	return uint64(id), nil
}

// allocated task to worker, change task status to Allocated
func AllocateTask(task *proto.Task, workerAddr string) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAllocateTask)
	defer metrics.Set(err)

	sqlCmd := "update tasks set task_status = ?, worker_addr = ?, update_time = now() where task_id =?"
	args := make([]interface{}, 0)
	args = append(args, proto.TaskStatusAllocated)
	args = append(args, workerAddr)
	args = append(args, task.TaskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("allocate task to worker failed, taskId(%v), workerAddr(%v), err(%v)", task.TaskId, workerAddr, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		return errors.New("affected rows less then one")
	}
	return
}

// update task status
func UpdateTaskStatus(task *proto.Task) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateTaskStatus)
	defer metrics.Set(err)

	var sqlCmd string
	if task.Status == proto.TaskStatusSucceed {
		sqlCmd = "update tasks set task_status = ?, update_time = now(), exception_info = '' where task_id =?"
	} else {
		sqlCmd = "update tasks set task_status = ?, update_time = now() where task_id =?"
	}
	args := make([]interface{}, 0)
	args = append(args, task.Status)
	args = append(args, task.TaskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task status failed, taskId(%v), targetStatus(%v), err(%v)", task.TaskId, task.Status, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		return errors.New("affected rows less then one")
	}
	return
}

// update task status
func UpdateTasksStatus(tasks []*proto.Task, sourceStatus, targetStatus proto.TaskStatus) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateTasksStatusViaSource)
	defer metrics.Set(err)

	taskIds := JoinTaskIds(tasks)
	sqlCmd := fmt.Sprintf("update tasks set task_status = ?, update_time = now() where task_status = ? and task_id in (%v)", taskIds)
	args := make([]interface{}, 0)
	args = append(args, targetStatus)
	args = append(args, sourceStatus)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task status failed, taskIds(%v), sourceStatus(%v), targetStatus(%v), err(%v)", taskIds, sourceStatus, targetStatus, err)
		return
	}
	return
}

// update task worker addr and task status
func UpdateTaskWorkerAddr(task *proto.Task) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateTaskWorkerAddr)
	defer metrics.Set(err)

	sqlCmd := "update tasks set worker_addr = ?, task_status = ?, update_time = now() where task_id =? and task_status = ?"
	args := make([]interface{}, 0)
	args = append(args, task.WorkerAddr)
	args = append(args, task.Status)
	args = append(args, task.TaskId)
	args = append(args, proto.TaskStatusUnallocated)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task worker addr failed, taskId(%v), workerAddr(%v), err(%v)", task.TaskId, task.WorkerAddr, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("update task worker addr failed, affected rows less then one, taskId(%v), workerAddr(%v)", task.TaskId, task.WorkerAddr)
		return errors.New("affected rows less then one")
	}
	return nil
}

// update task status to be failed, and exception info
func UpdateTaskFailed(task *proto.Task, exceptionInfo string) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateTaskFailed)
	defer metrics.Set(err)

	exceptionInfo = stringutil.SubStringByLength(exceptionInfo, DefaultTaskExceptionInfoLength)

	sqlCmd := "update tasks set task_status = ?, exception_info = ?, update_time = now() where task_id =?"
	args := make([]interface{}, 0)
	args = append(args, proto.TaskStatusFailed)
	args = append(args, exceptionInfo)
	args = append(args, task.TaskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task to be failed has exception, taskId(%v), exceptionInfo(%v), err(%v)", task.TaskId, exceptionInfo, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("update task to be failed has exception, affected rows less then one, taskId(%v), exceptionInfo(%v)", task.TaskId, exceptionInfo)
		return errors.New("affected rows less then one")
	}
	return
}

func UpdateTaskInfo(taskId uint64, taskInfo string) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateTaskInfo)
	defer metrics.Set(err)

	taskInfo = stringutil.SubStringByLength(taskInfo, DefaultTaskTaskInfoLength)
	sqlCmd := "update tasks set task_info = ?, update_time = now() where task_id =?"
	args := make([]interface{}, 0)
	args = append(args, taskInfo)
	args = append(args, taskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update task info has exception, taskId(%v), taskInfo(%v), err(%v)", taskId, taskInfo, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("update task info has exception, affected rows less then one, taskId(%v), taskInfo(%v)", taskId, taskInfo)
		return errors.New("affected rows less then one")
	}
	return
}

// select task task via task id
func SelectTask(taskId int64) (task *proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectTask)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_id = ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskId)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		return t, nil
	}
	return
}

// select allocated tasks and not finished, including not successful and not failed
func SelectNotFinishedTask(workerAddr string, taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectAllocatedTask)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_status < ? and task_type = ? and worker_addr = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, proto.TaskStatusSucceed, taskType, workerAddr, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// select allocated tasks via worker address and task type
func SelectAllocatedTask(workerAddr string, taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectAllocatedTask)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_status = ? and task_type = ? and worker_addr = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, proto.TaskStatusAllocated, taskType, workerAddr, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// select tasks with specified task type, add support to query by page add param limit and offset
func SelectTasksWithType(taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectTasksWithType)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

func SelectUnallocatedTasks(taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectUnallocatedTasks)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? and task_status = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, proto.TaskStatusUnallocated, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// select specified type tasks that has been allocated and not finished
func SelectRunningTasks(taskType int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectRunningTasks)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? and task_status < ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, proto.TaskStatusSucceed)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// select specified type tasks that has been finished successfully
func SelectSucceedTasks(taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectSucceedTasks)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? and task_status = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, proto.TaskStatusSucceed, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// select failed tasks
func SelectExceptionTasks(taskType, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectExceptionTasks)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? and task_status = ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, proto.TaskStatusFailed, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// 在一定时间内还没有正常结束的任务，则认为其为异常任务
func SelectNotModifiedForLongTime(taskType, hours, limit, offset int) (tasks []*proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectNotModifiedForLongTime)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where task_type = ? and task_status != ? and TIMESTAMPDIFF(HOUR, update_time, now()) > ? limit ? offset ?", taskColumns())
	rows, err = db.Query(sqlCmd, taskType, proto.TaskStatusSucceed, hours, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

// delete task via task id
func DeleteTask(taskId int64) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteTask)
	defer metrics.Set(err)

	sqlCmd := "delete from tasks where task_id =?"
	args := make([]interface{}, 0)
	args = append(args, taskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete task failed, taskId(%v)", taskId)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("delete task failed, affected rows less then one, taskId(%v)", taskId)
		return errors.New("affected rows less then one")
	}
	return
}

// delete task via task id
func DeleteTaskByVolumeAndId(cluster, volume string, taskType int, taskId int64) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteTaskByVolumeAndId)
	defer metrics.Set(err)
	sqlCmd := "delete from tasks where cluster_name = ? and vol_name = ? and task_type = ? and task_id =?"
	args := make([]interface{}, 0)
	args = append(args, cluster)
	args = append(args, volume)
	args = append(args, taskType)
	args = append(args, taskId)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete task failed, cluster(%v), volume(%v), taskType(%v), taskId(%v)", cluster, volume, taskType, taskId)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("delete task failed, affected rows less then one, taskId(%v)", taskId)
		return errors.New("affected rows less then one")
	}
	return
}

// delete task via task id
func DeleteTasks(tasks []*proto.Task) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteTasks)
	defer metrics.Set(err)

	sbPlaceholder := strings.Builder{}
	args := make([]interface{}, 0)
	for index, task := range tasks {
		args = append(args, task.TaskId)
		sbPlaceholder.WriteString("?")
		if index != len(tasks)-1 {
			sbPlaceholder.WriteString(",")
		}
	}
	sqlCmd := fmt.Sprintf("delete from tasks where task_id in (%s)", sbPlaceholder.String())
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete tasks via task id failed, taskIds(%v)", args)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	log.LogInfof("[DeleteTasks] delete tasks rows: %v, args(%v)", nums, args)
	return
}

func CheckDPTaskExist(cluster, volumeName string, taskType int, dpId uint64) (res bool, task *proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlCheckDPTaskExist)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where cluster_name = ? and vol_name = ? and task_type = ? and dp_id = ?", taskColumns())
	rows, err = db.Query(sqlCmd, cluster, volumeName, taskType, dpId)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		task = &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&task.TaskId, &task.TaskType, &task.Cluster, &task.VolName, &task.DpId, &task.MpId, &task.TaskInfo, &task.WorkerAddr, &task.Status, &task.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		task.CreateTime = createTime
		task.UpdateTime = updateTime
		return true, task, nil
	}
	return
}

func CheckMPTaskExist(cluster, volumeName string, taskType int, mpId uint64) (res bool, task *proto.Task, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlCheckMPTaskExist)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from tasks where cluster_name = ? and vol_name = ? and task_type = ? and mp_id = ?", taskColumns())
	rows, err = db.Query(sqlCmd, cluster, volumeName, taskType, mpId)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		task = &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&task.TaskId, &task.TaskType, &task.Cluster, &task.VolName, &task.DpId, &task.MpId, &task.TaskInfo, &task.WorkerAddr, &task.Status, &task.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		task.CreateTime = createTime
		task.UpdateTime = updateTime
		return true, task, nil
	}
	return
}

func SelectTasks(cluster, volume string, dpId, mpId uint64, taskType, limit, offset int) (tasks []*proto.Task, err error) {
	sqlCmd := fmt.Sprintf("select %s from tasks", taskColumns())
	conditions := make([]string, 0)
	values := make([]interface{}, 0)
	if !stringutil.IsStrEmpty(cluster) {
		conditions = append(conditions, " cluster_name = ?")
		values = append(values, cluster)
	}
	if !stringutil.IsStrEmpty(volume) {
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
		t := &proto.Task{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&t.TaskId, &t.TaskType, &t.Cluster, &t.VolName, &t.DpId, &t.MpId, &t.TaskInfo, &t.WorkerAddr, &t.Status, &t.ExceptionInfo, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		t.CreateTime = createTime
		t.UpdateTime = updateTime
		tasks = append(tasks, t)
	}
	return
}

func JoinTaskIds(taskIds []*proto.Task) string {
	sb := strings.Builder{}
	for index, task := range taskIds {
		sb.WriteString(strconv.FormatUint(task.TaskId, 10))
		if index < len(taskIds)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func taskColumns() (columns string) {
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
	columnSlice = append(columnSlice, ColumnCreateTime)
	columnSlice = append(columnSlice, ColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
