package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

func InsertOrUpdateEmailMembers(info proto.MailToMemberInfo) (err error) {
	var (
		oldMember []string
		sqlCmd    string
		args      []interface{}
		r         sql.Result
		nums      int64
	)
	oldMember, err = SelectEmailMembers(info.WorkerType)
	if len(oldMember) == 0 {
		sqlCmd = "insert into email_members (task_type, members) values(?, ?)"
		args = make([]interface{}, 0)
		args = append(args, int8(info.WorkerType))
		args = append(args, info.Members)
	} else {
		sqlCmd = "update email_members set members = ? where task_type = ?"
		args = make([]interface{}, 0)
		args = append(args, info.Members)
		args = append(args, int8(info.WorkerType))
	}
	if r, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update email to members failed, workerType(%v) members(%s), err(%v)",
			proto.WorkerTypeToName(info.WorkerType), info.Members, err)
		return
	}
	if nums, err = r.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		return errors.New("affected rows less then one")
	}
	return
}

func SelectEmailMembers(workerType proto.WorkerType) (members []string, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select members from email_members where task_type = ?")
	rows, err = db.Query(sqlCmd, int8(workerType))
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var membersStr string
		err = rows.Scan(&membersStr)
		if err != nil {
			return
		}
		members = strings.Split(membersStr, ",")
	}
	return

}

func InsertOrUpdateNotifyMembers(info proto.NotifyMembers) (err error) {
	var (
		oldEmailMembers, oldAlarmMembers, oldCallMembers []string
		sqlCmd                                           string
		args                                             []interface{}
		r                                                sql.Result
		nums                                             int64
	)
	oldEmailMembers, oldAlarmMembers, oldCallMembers, err = SelectNotifyMembers(info.WorkerType)
	if len(oldEmailMembers) == 0 && len(oldAlarmMembers) == 0 && len(oldCallMembers) == 0 {
		sqlCmd = "insert into notify_members (task_type, email_members, alarm_members, call_members) values(?, ?, ?, ?)"
		args = make([]interface{}, 0)
		args = append(args, int8(info.WorkerType))
		args = append(args, info.EmailMembers)
		args = append(args, info.AlarmMembers)
		args = append(args, info.CallMembers)
	} else {
		sqlCmd = "update notify_members set email_members = ?, alarm_members = ?, call_members = ? where task_type = ?"
		args = make([]interface{}, 0)
		args = append(args, info.EmailMembers)
		args = append(args, info.AlarmMembers)
		args = append(args, info.CallMembers)
		args = append(args, int8(info.WorkerType))
	}
	if r, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update notify members failed, workerType(%v) members(%s,%s,%s), err(%v)",
			proto.WorkerTypeToName(info.WorkerType), info.EmailMembers, info.AlarmMembers, info.CallMembers, err)
		return
	}
	if nums, err = r.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		return errors.New("affected rows less then one")
	}
	return
}

func SelectNotifyMembers(workerType proto.WorkerType) (emailMembers, alarmMembers, callMembers []string, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select email_members, alarm_members, call_members from notify_members where task_type = ?")
	rows, err = db.Query(sqlCmd, int8(workerType))
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var notifyMembers = new(proto.NotifyMembers)
		err = rows.Scan(&notifyMembers.EmailMembers, &notifyMembers.AlarmMembers, &notifyMembers.CallMembers)
		if err != nil {
			return
		}
		emailMembers = strings.Split(notifyMembers.EmailMembers, ",")
		alarmMembers = strings.Split(notifyMembers.AlarmMembers, ",")
		callMembers = strings.Split(notifyMembers.CallMembers, ",")
	}
	return

}