package monitor

import (
	"fmt"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/util/hbase"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	TablePrefix    = "CFS_MONITOR"
	TableRotate    = 1 * time.Hour // todo discuss
	TableClearTime = 7 * 24 * time.Hour
)

func (m *Monitor) initHBaseTable() (err error) {
	// 1. Check table on the hour
	initHourTime := getTimeOnHour(time.Now())
	curTableName := getTableName(initHourTime)
	var thriftClient *hbase.THBaseServiceClient
	thriftClient, err = hbase.OpenHBaseClient(m.thriftAddr)
	if err != nil {
		log.LogErrorf("initHBaseTable: open thrift client failed (%v)", err)
		return
	}
	if err = thriftClient.CreateHBaseTable(m.namespace, curTableName, []string{CFMonitor}); err != nil {
		log.LogErrorf("initHBaseTable failed: create table err[%v] table name[%v]", err, curTableName)
	}
	nextTime := time.Unix(initHourTime, 0).Add(TableRotate).Unix()
	nextTableName := getTableName(nextTime)
	if err = thriftClient.CreateHBaseTable(m.namespace, nextTableName, []string{CFMonitor}); err != nil {
		log.LogErrorf("initHBaseTable failed: create next table err[%v] table name[%v]", err, nextTableName)
	}
	if err = thriftClient.CloseHBaseClient(); err != nil {
		log.LogErrorf("initHBaseTable: close thrift client err (%v)", err)
	}
	// 2. Create a new table on hour in advance
	go m.scheduleTableTask(nextTime)
	return nil
}

func (m *Monitor) scheduleTableTask(lastTime int64) {
	createTicker := time.NewTicker(TableRotate)
	defer createTicker.Stop()
	log.LogInfof("scheduleTableTask: start, rotate time(%v), clear time(%v)", TableRotate, TableClearTime)
	for {
		select {
		case <-createTicker.C:
			createTime := time.Unix(lastTime, 0).Add(TableRotate).Unix()
			tableName := getTableName(createTime)
			thriftClient, err := hbase.OpenHBaseClient(m.thriftAddr)
			if err != nil {
				log.LogErrorf("scheduleTableTask: open thrift client failed (%v)", err)
				continue
			}
			if err = thriftClient.CreateHBaseTable(m.namespace, tableName, []string{CFMonitor}); err != nil {
				// todo 1. check duplicate; 2. retry; 3. alarm if failed
				log.LogErrorf("scheduleTableTask failed: create next table err(%v) table name(%v)", err, tableName)
			}
			lastTime = createTime
			m.deleteExpiresTables(thriftClient)
			if err = thriftClient.CloseHBaseClient(); err != nil {
				log.LogErrorf("scheduleTableTask: close thrift client err (%v)", err)
			}
		case <-m.stopC:
			log.LogInfof("scheduleTableTask: stop, last time(%v)", lastTime)
			return
		}
	}
}

func (m *Monitor) deleteExpiresTables(thriftClient *hbase.THBaseServiceClient) {
	var (
		tables    []*hbase.TTableName
		tableTime time.Time
		err       error
	)
	if tables, err = thriftClient.ListHBaseTables(m.namespace); err != nil {
		log.LogErrorf("deleteExpiresTables failed: list tables err[%v], namespace[%v]", err, m.namespace)
		return
	}
	log.LogDebugf("deleteExpiresTables begin")
	for _, table := range tables {
		name := string(table.Qualifier)
		if !strings.HasPrefix(name, TablePrefix+"_") {
			continue
		}
		var timeStr string
		timeIndex := strings.LastIndex(name, "_")
		if timeIndex < len(name) {
			timeStr = name[timeIndex+1:]
			if tableTime, err = time.ParseInLocation(timeLayout, timeStr, time.Local); err != nil {
				continue
			}
			if time.Since(tableTime) > TableClearTime {
				log.LogDebugf("deleteExpiresTables: delete table[%v]", name)
				if err = thriftClient.DeleteHBaseTable(m.namespace, name); err != nil {
					log.LogErrorf("deleteExpiresTables failed: delete table err[%v], table[%v]", err, name)
					continue
				}
			}
		}
	}
	log.LogDebugf("deleteExpiresTables exit")
}

// get table name according to timestamp
func getTableName(timestamp int64) string {
	timeStr := time.Unix(timestamp, 0).Format(timeLayout)
	return fmt.Sprintf("%v_%v", TablePrefix, timeStr)
}

func getTimeOnHour(now time.Time) int64 {
	return now.Unix() - int64(now.Second()) - int64(60*now.Minute())
}

func getTableNameByTimeStr(timeStr string) (tableName string, err error) {
	var t time.Time
	if t, err = time.ParseInLocation(timeLayout, timeStr, time.Local); err != nil {
		return
	}
	tableName = getTableName(getTimeOnHour(t))
	return
}
