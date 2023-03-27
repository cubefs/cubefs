package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"time"
)

const (
	ColumnConfigId    = "id"
	ColumnConfigType  = "config_type"
	ColumnConfigKey   = "config_key"
	ColumnConfigValue = "config_value"
)

func AddScheduleConfig(sc *proto.ScheduleConfig) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlAddScheduleConfig)
	defer metrics.Set(err)

	sqlCmd := "insert into config(config_type, config_key, config_value) values(?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, sc.ConfigType)
	args = append(args, sc.ConfigKey)
	args = append(args, sc.ConfigValue)

	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddScheduleConfig] add schedule config failed, configType(%v), configKey(%v), configValue(%v), err(%v)",
			sc.ConfigType, sc.ConfigKey, sc.ConfigValue, err)
		return err
	}
	return
}

func SelectScheduleConfig(st proto.ScheduleConfigType) (scs []*proto.ScheduleConfig, err error) {
	metrics := exporter.NewTPCnt(proto.MonitorMysqlSelectScheduleConfig)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from config where config_type = ?", configColumns())
	rows, err = db.Query(sqlCmd, st)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		sc := &proto.ScheduleConfig{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&sc.ConfigId, &sc.ConfigType, &sc.ConfigKey, &sc.ConfigValue, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		sc.CreateTime = createTime
		sc.UpdateTime = updateTime
		scs = append(scs, sc)
	}
	return
}

func UpdateScheduleConfig(sc *proto.ScheduleConfig) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlUpdateScheduleConfig)
	defer metrics.Set(err)

	sqlCmd := "update config set config_value = ?, update_time = now() where config_type =? and config_key = ?"
	args := make([]interface{}, 0)
	args = append(args, sc.ConfigValue)
	args = append(args, sc.ConfigType)
	args = append(args, sc.ConfigKey)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("update schedule config info has exception, configType(%v), configKey(%v), configValue(%v), err(%v)",
			sc.ConfigType, sc.ConfigKey, sc.ConfigValue, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("update schedule config info has exception, affected rows less then one, configType(%v), configKey(%v), configValue(%v)", sc.ConfigType, sc.ConfigKey, sc.ConfigValue)
		return errors.New("affected rows less then one")
	}
	return
}

func DeleteScheduleConfig(sc *proto.ScheduleConfig) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewTPCnt(proto.MonitorMysqlDeleteScheduleConfig)
	defer metrics.Set(err)
	sqlCmd := "delete from config where config_type = ? and config_key = ?"
	args := make([]interface{}, 0)
	args = append(args, sc.ConfigType)
	args = append(args, sc.ConfigKey)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete schedule config failed, configType(%v), configKey(%v), err(%v)",
			sc.ConfigType, sc.ConfigKey, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("delete schedule config failed, affected rows less then one, configType(%v), configKey(%v)",
			sc.ConfigType, sc.ConfigKey)
		return errors.New("affected rows less then one")
	}
	return
}

func configColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, ColumnConfigId)
	columnSlice = append(columnSlice, ColumnConfigType)
	columnSlice = append(columnSlice, ColumnConfigKey)
	columnSlice = append(columnSlice, ColumnConfigValue)
	columnSlice = append(columnSlice, ColumnCreateTime)
	columnSlice = append(columnSlice, ColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
