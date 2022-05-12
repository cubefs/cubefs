package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"time"
)

const (
	ColumnWorkerId   = "worker_id"
	ColumnWorkerType = "worker_type"
)

func SelectWorkerNode(workerType proto.WorkerType, workerAddr string) (workerNode *proto.WorkerNode, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectWorkerNode)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from workers where worker_type = ? and worker_addr = ?", workerColumns())
	rows, err = db.Query(sqlCmd, workerType, workerAddr)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		workerNode = &proto.WorkerNode{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&workerNode.WorkerId, &workerNode.WorkerType, &workerNode.WorkerAddr, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		workerNode.CreateTime = createTime
		workerNode.UpdateTime = updateTime
		return workerNode, nil
	}
	return
}

func CheckWorkerExist(worker *proto.WorkerNode) (res bool, err error) {
	// select count(*) from workers where worker_type = 1 and local_ip = '11.97.57.231'
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectWorkersAll)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select count(*) as count from workers where worker_type = ? and worker_addr = ?")
	rows, err = db.Query(sqlCmd, worker.WorkerType, worker.WorkerAddr)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		if err != nil {
			return
		}
		return count > 0, nil
	}
	return
}

func AddWorker(worker *proto.WorkerNode) (workerId uint64, err error) {
	var (
		rs sql.Result
		id int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddWorker)
	defer metrics.Set(err)

	sqlCmd := "insert into workers(worker_type, worker_addr) values(?, ?)"
	args := make([]interface{}, 0)
	args = append(args, int8(worker.WorkerType))
	args = append(args, worker.WorkerAddr)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("add worker failed, workerType(%v), workerAddr(%v), err(%v)", worker.WorkerType, worker.WorkerAddr, err)
		return
	}
	if id, err = rs.LastInsertId(); err != nil {
		log.LogErrorf("add worker failed when got worker id, workerType(%v), workerAddr(%v), err(%v)", worker.WorkerType, worker.WorkerAddr, err)
		return
	}
	return uint64(id), nil
}

func UpdateWorkerHeartbeat(worker *proto.WorkerNode) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateWorkerHeartbeat)
	defer metrics.Set(err)

	sqlCmd := "update workers set update_time = now() where worker_id =? and worker_addr = ? and worker_type = ?"
	args := make([]interface{}, 0)
	args = append(args, worker.WorkerId)
	args = append(args, worker.WorkerAddr)
	args = append(args, worker.WorkerType)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update worker failed, workerId(%v), workerAddr(%v), err(%v)", worker.WorkerId, worker.WorkerAddr, err)
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

// select workers with specified worker type, add support to query by page add param limit and offset
func SelectWorker(workerType, limit, offset int) (workers []*proto.WorkerNode, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectWorker)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from workers where worker_type = ? limit ? offset ?", workerColumns())
	rows, err = db.Query(sqlCmd, workerType, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		w := &proto.WorkerNode{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&w.WorkerId, &w.WorkerType, &w.WorkerAddr, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		w.CreateTime = createTime
		w.UpdateTime = updateTime
		workers = append(workers, w)
	}
	return
}

// select all the workers of specified worker type
func SelectAllWorkers(workerType int) (workers []*proto.WorkerNode, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectWorkersAll)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from workers where worker_type = ?", workerColumns())
	rows, err = db.Query(sqlCmd, workerType)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		w := &proto.WorkerNode{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&w.WorkerId, &w.WorkerType, &w.WorkerAddr, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		w.CreateTime = createTime
		w.UpdateTime = updateTime
		workers = append(workers, w)
	}
	return
}

// 查询异常的worker nodes，如果长时间没有更新，认为节点异常
// offlineDuration: 单位/秒
func SelectExceptionWorkers(workerType, offlineDuration int) (workers []*proto.WorkerNode, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectExceptionWorkers)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from workers where worker_type = ? and TIMESTAMPDIFF(SECOND, update_time, now()) > ?", workerColumns())
	rows, err = db.Query(sqlCmd, workerType, offlineDuration)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		w := &proto.WorkerNode{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&w.WorkerId, &w.WorkerType, &w.WorkerAddr, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		w.CreateTime = createTime
		w.UpdateTime = updateTime
		workers = append(workers, w)
	}
	return
}

func AddExceptionWorkersToHistory(workers []*proto.WorkerNode) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddExceptionWorkersToHistory)
	defer metrics.Set(err)

	sqlCmd := "insert into worker_history(worker_id, worker_type, worker_addr, worker_create_time, worker_update_time) values"
	args := make([]interface{}, 0)

	for index, worker := range workers {
		sqlCmd += "(?, ?, ?, ?, ?)"
		if index < len(workers)-1 {
			sqlCmd += ","
		}
		args = append(args, worker.WorkerId, worker.WorkerType, worker.WorkerAddr, worker.CreateTime, worker.UpdateTime)
	}
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("add exception worker failed, workers(%v), err(%v)", workers, err)
		return
	}
	return
}

func DeleteExceptionWorkers(workers []*proto.WorkerNode) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteExceptionWorkers)
	defer metrics.Set(err)

	sbPlaceholder := strings.Builder{}
	args := make([]interface{}, 0)
	for index, wn := range workers {
		args = append(args, wn.WorkerId)
		sbPlaceholder.WriteString("?")
		if index != len(workers)-1 {
			sbPlaceholder.WriteString(",")
		}
	}
	sqlCmd := fmt.Sprintf("delete from workers where worker_id in (%s)", sbPlaceholder.String())
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete exception worker nodes via node id failed, taskIds(%v), err(%v)", args, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	log.LogInfof("[DeleteTasks] delete exception worker node rows: %v, args(%v), err(%v)", nums, args, err)
	return
}

func SelectWorkers(workerType int, workerAddr string, limit, offset int) (workers []*proto.WorkerNode, err error) {
	sqlCmd := fmt.Sprintf("select %s from workers", workerColumns())
	conditions := make([]string, 0)
	values := make([]interface{}, 0)
	if workerType != 0 {
		conditions = append(conditions, " worker_type = ?")
		values = append(values, workerType)
	}
	if !util.IsStrEmpty(workerAddr) {
		conditions = append(conditions, " worker_addr = ?")
		values = append(values, workerAddr)
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
		w := &proto.WorkerNode{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&w.WorkerId, &w.WorkerType, &w.WorkerAddr, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		w.CreateTime = createTime
		w.UpdateTime = updateTime
		workers = append(workers, w)
	}
	return
}

func workerColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, ColumnWorkerId)
	columnSlice = append(columnSlice, ColumnWorkerType)
	columnSlice = append(columnSlice, ColumnWorkerAddr)
	columnSlice = append(columnSlice, ColumnCreateTime)
	columnSlice = append(columnSlice, ColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
