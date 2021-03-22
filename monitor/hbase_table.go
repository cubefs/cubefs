package monitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
)

var (
	TablePrefix    = "CFS_MONITOR"
	TableRotate    = 1 * time.Hour // todo discuss
	TableClearTime = 3 * 24 * time.Hour
)

func (m *Monitor) initHBaseTable() (err error) {
	// 1. Check table on the hour
	initHourTime := getTimeOnHour(time.Now())
	curTableName := getTableName(m.cluster, initHourTime)
	// TODO test create duplicate table
	if err = m.hbase.CreateTable(curTableName, []string{CFMonitor}); err != nil {
		if !strings.Contains(err.Error(), TableExistError.Error()) {
			log.LogErrorf("initHBaseTable failed: create table err[%v] table name[%v]", err, curTableName)
			return
		}
	}
	nextTime := time.Unix(initHourTime, 0).Add(TableRotate).Unix()
	nextTableName := getTableName(m.cluster, nextTime)
	if err = m.hbase.CreateTable(nextTableName, []string{CFMonitor}); err != nil {
		if !strings.Contains(err.Error(), TableExistError.Error()) {
			log.LogErrorf("initHBaseTable failed: create next table err[%v] table name[%v]", err, nextTableName)
			return
		}
	}
	// 2. Create a new table on hour in advance
	go m.scheduleTableTask(nextTime)
	return nil
}

func (m *Monitor) scheduleTableTask(lastTime int64) {
	createTicker := time.NewTicker(TableRotate)
	for {
		select {
		case <-createTicker.C:
			createTime := time.Unix(lastTime, 0).Add(TableRotate).Unix()
			tableName := getTableName(m.cluster, createTime)
			if err := m.hbase.CreateTable(tableName, []string{CFMonitor}); err != nil {
				// todo 1. check duplicate; 2. retry; 3. alarm if failed
				if !strings.Contains(err.Error(), TableExistError.Error()) {
					log.LogErrorf("initHBaseTable failed: create next table err[%v] table name[%v]", err, tableName)
				}
			}
			lastTime = createTime
			m.deleteExpiresTables()
		case <-m.stopC:
			return
		}
	}
}

func (m *Monitor) deleteExpiresTables() {
	var (
		tableNames []string
		timestamp  int64
		err        error
	)
	if tableNames, err = m.hbase.ListTables(); err != nil {
		log.LogErrorf("deleteExpiresTables failed: list tables err[%v], namespace[%v]", err, m.hbase.namespace)
		return
	}
	log.LogDebugf("deleteExpiresTables begin")
	for _, name := range tableNames {
		if !strings.HasPrefix(name, TablePrefix+"_"+m.cluster+"_") {
			continue
		}
		var timeStr string
		timeIndex := strings.LastIndex(name, "_")
		if timeIndex < len(name) {
			timeStr = name[timeIndex+1:]
			if timestamp, err = strconv.ParseInt(timeStr, 10, 64); err != nil {
				log.LogErrorf("deleteExpiresTables failed: parse int err[%v], timeStr[%v]", err, timeStr)
				continue
			}
			if time.Since(time.Unix(timestamp, 0)) > TableClearTime {
				log.LogDebugf("deleteExpiresTables: delete table[%v]", name)
				if err = m.hbase.DeleteTable(name); err != nil {
					if !strings.Contains(err.Error(), TableNotFoundError.Error()) {
						log.LogErrorf("deleteExpiresTables failed: delete table err[%v], table[%v]", err, name)
					}
					continue
				}
			}
		}
	}
	log.LogDebugf("deleteExpiresTables exit")
}

// get table name according to timestamp
func getTableName(cluster string, timestamp int64) string {
	return fmt.Sprintf("%v_%v_%v", TablePrefix, cluster, timestamp)
}

func getTimeOnHour(now time.Time) int64 {
	return now.Unix() - int64(now.Second()) - int64(60*now.Minute())
}

func getTableNameByTimeStr(cluster, timeStr string) (tableName string, err error) {
	var t time.Time
	if t, err = time.ParseInLocation(timeLayout, timeStr, time.Local); err != nil {
		return
	}
	tableName = getTableName(cluster, getTimeOnHour(t))
	return
}

func getOpTableName(cluster string, timeUnit string) (tableNames []string) {
	for _, option := range statistics.ActionDataMap {
		tableName := fmt.Sprintf("%v_%v_%v_TABLE", strings.ToUpper(cluster), strings.ToUpper(option), strings.ToUpper(timeUnit))
		tableNames = append(tableNames, tableName)
	}
	for _, option := range statistics.ActionMetaMap {
		tableName := fmt.Sprintf("%v_%v_%v_TABLE", strings.ToUpper(cluster), strings.ToUpper(option), strings.ToUpper(timeUnit))
		tableNames = append(tableNames, tableName)
	}
	return
}
