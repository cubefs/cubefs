package mysql

import (
	"database/sql"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"time"
)

const (
	FlowColumnTaskType    = "task_type"
	FlowColumnFlowType    = "flow_type"
	FlowColumnFlowValue   = "flow_value"
	FlowColumnMaxNum      = "max_num"
	FlowColumnCreateTime  = "create_time"
	FlowColumnUpdateTime  = "update_time"
)

func AddFlowControl(flow *proto.FlowControl) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddFlowControl)
	defer metrics.Set(err)

	sqlCmd := "insert into flow_control(task_type, flow_type, flow_value, max_num) values(?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, int8(flow.WorkerType))
	args = append(args, flow.FlowType)
	args = append(args, flow.FlowValue)
	args = append(args, flow.MaxNums)

	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("add flow control failed, workerType(%v), flowType(%v), flowValue(%v), maxNum(%v), err(%v)",
			proto.WorkerTypeToName(flow.WorkerType), flow.FlowType, flow.FlowValue, flow.MaxNums, err)
		return
	}
	return
}

func UpdateFlowControl(flow *proto.FlowControl) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlModifyFlowControl)
	defer metrics.Set(err)

	sqlCmd := "update flow_control set max_num = ?, update_time = now() where task_type =? and flow_type = ? and flow_value = ?"
	args := make([]interface{}, 0)
	args = append(args, flow.MaxNums)
	args = append(args, int8(flow.WorkerType))
	args = append(args, flow.FlowType)
	args = append(args, flow.FlowValue)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("modify flow control info failed, workerType(%v), flowType(%v), flowValue(%v), maxNum(%v), err(%v)",
			proto.WorkerTypeToName(flow.WorkerType), flow.FlowType, flow.FlowValue, flow.MaxNums, err)
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

func DeleteFlowControl(flow *proto.FlowControl) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteFlowControl)
	defer metrics.Set(err)

	sqlCmd := "delete from flow_control where task_type =? and flow_type = ? and flow_value = ?"
	args := make([]interface{}, 0)
	args = append(args, int8(flow.WorkerType))
	args = append(args, flow.FlowType)
	args = append(args, flow.FlowValue)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete flow control failed, workerType(%v), flowType(%v), flowValue(%v)",
			proto.WorkerTypeToName(flow.WorkerType), flow.FlowType, flow.FlowValue)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	log.LogInfof("delete flow control, workerType(%v), flowType(%v), flowValue(%v), affectedRows(%v)",
		proto.WorkerTypeToName(flow.WorkerType), flow.FlowType, flow.FlowValue, nums)
	return nil
}

func SelectFlowControls() (flows []*proto.FlowControl, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlListFlowControl)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from flow_control", flowControlColumns())
	rows, err = db.Query(sqlCmd)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		f := &proto.FlowControl{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&f.WorkerType, &f.FlowType, &f.FlowValue, &f.MaxNums, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		f.CreateTime = createTime
		f.UpdateTime = updateTime
		flows = append(flows, f)
	}
	return
}

func SelectFlowControlsViaType(wt proto.WorkerType) (flows []*proto.FlowControl, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectFlowControlsViaType)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from flow_control where task_type = ?", flowControlColumns())
	rows, err = db.Query(sqlCmd, int(wt))
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		f := &proto.FlowControl{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&f.WorkerType, &f.FlowType, &f.FlowValue, &f.MaxNums, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		f.CreateTime = createTime
		f.UpdateTime = updateTime
		flows = append(flows, f)
	}
	return
}

func flowControlColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, FlowColumnTaskType)
	columnSlice = append(columnSlice, FlowColumnFlowType)
	columnSlice = append(columnSlice, FlowColumnFlowValue)
	columnSlice = append(columnSlice, FlowColumnMaxNum)
	columnSlice = append(columnSlice, FlowColumnCreateTime)
	columnSlice = append(columnSlice, FlowColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
