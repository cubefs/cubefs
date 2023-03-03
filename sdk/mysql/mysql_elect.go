package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"time"
)

const (
	ColumnTerm     = "term"
	ColumnLeaderIp = "leader_ip"
)

func GetLeader(expire int) (le *proto.LeaderElect, err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlGetLeader)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from leader_election where term = (select max(term) from leader_election) and unix_timestamp(now()) - unix_timestamp(update_time) < ?", electColumns())
	rows, err = db.Query(sqlCmd, expire)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		le := &proto.LeaderElect{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&le.Term, &le.LeaderAddr, &ct, &ut)
		if err != nil {
			return nil, err
		}
		if createTime, err = FormatTime(ct); err != nil {
			return nil, err
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return nil, err
		}
		le.CreateTime = createTime
		le.UpdateTime = updateTime
		return le, nil
	}
	return
}

func GetMaxTerm() (term uint64, err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlGetMaxTerm)
	defer metrics.Set(err)

	var rows *sql.Rows
	// select max(term) from leader_election
	sqlCmd := fmt.Sprintf("select ifnull(max(term), 0) from leader_election")
	rows, err = db.Query(sqlCmd)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		err = rows.Scan(&term)
		if err != nil {
			return 0, err
		}
		return term, nil
	}
	return
}

func UpdateLeaderHeartbeat(term uint64, localIp string) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewModuleTP(proto.MonitorMysqlUpdateLeaderHeartbeat)
	defer metrics.Set(err)

	sqlCmd := "update leader_election set update_time = now() where term = ? and leader_ip = ?"
	args := make([]interface{}, 0)
	args = append(args, term)
	args = append(args, localIp)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update leader heartbeat failed, term(%v), localIp(%v), err(%v)", term, localIp, err)
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

func AddElectTerm(le *proto.LeaderElect) (isLeader bool, err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlAddElectTerm)
	defer metrics.Set(err)

	sqlCmd := "insert into leader_election(term, leader_ip) values(?, ?)"
	args := make([]interface{}, 0)
	args = append(args, le.Term)
	args = append(args, le.LeaderAddr)

	if _, err = Transaction(sqlCmd, args); err == nil {
		return true, nil
	}
	log.LogErrorf("[AddElectTerm] add new term to leader elect table failed, term(%v), leaderIp(%v), err(%v)", le.Term, le.LeaderAddr, err)
	// Duplicate entry '3' for key 'PRIMARY'
	if strings.Contains(strings.ToLower(err.Error()), "duplicate entry") && strings.Contains(strings.ToLower(err.Error()), "primary") {
		return false, nil
	}
	return
}

func electColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, ColumnTerm)
	columnSlice = append(columnSlice, ColumnLeaderIp)
	columnSlice = append(columnSlice, ColumnCreateTime)
	columnSlice = append(columnSlice, ColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
