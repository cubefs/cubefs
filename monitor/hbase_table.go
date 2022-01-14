package monitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/util/hbase"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
)

const (
	CFMonitor = "MONITOR"
)

var (
	TablePrefix    = "CFS_MONITOR"
	TableRotate    = 1 * time.Hour // todo discuss
	TableClearTime = 7 * 24 * time.Hour
	OneHourSeconds = int64(60 * 60)
)

func (m *Monitor) initHBaseTable() (err error) {
	// 1. Check table on the hour
	initHourTime := getTimeOnHour(time.Now())
	curTableName, splitKeys := m.getCreateTableInfo(initHourTime)
	var thriftClient *hbase.THBaseServiceClient
	thriftClient, err = hbase.OpenHBaseClient(m.thriftAddr)
	if err != nil {
		log.LogErrorf("initHBaseTable: open thrift client failed (%v)", err)
		return
	}
	log.LogInfof("initHBaseTable: table(%v) splitKeys(%v)", curTableName, splitKeys)
	if err = thriftClient.CreateHBaseTable(m.namespace, curTableName, []string{CFMonitor}, splitKeys); err != nil {
		log.LogErrorf("initHBaseTable failed: create table err[%v] table name[%v]", err, curTableName)
	}
	nextTime := time.Unix(initHourTime, 0).Add(TableRotate).Unix()
	nextTableName, nextSplitKeys := m.getCreateTableInfo(nextTime)
	log.LogInfof("initHBaseTable: table(%v) splitKeys(%v)", nextTableName, nextSplitKeys)
	if err = thriftClient.CreateHBaseTable(m.namespace, nextTableName, []string{CFMonitor}, nextSplitKeys); err != nil {
		log.LogErrorf("initHBaseTable failed: create next table err[%v] table name[%v]", err, nextTableName)
	}
	if err = thriftClient.CloseHBaseClient(); err != nil {
		log.LogErrorf("initHBaseTable: close thrift client err (%v)", err)
	}
	// 2. Create a new table on hour in advance
	go m.scheduleTableTask(nextTime)
	return nil
}

func getSplitRules(rules []string) (splitRules map[string]int64) {
	splitRules = make(map[string]int64)
	for _, rule := range rules {
		splits := strings.Split(rule, ":")
		if len(splits) != 2 {
			log.LogErrorf("getSplitKeys failed: splits string(%v)", splits)
			continue
		}
		clusterName := splits[0]
		splitNum, err := strconv.ParseInt(splits[1], 10, 8)
		if err != nil {
			log.LogErrorf("getSplitKeys failed: err(%v) parse string(%v)", err, splits[1])
			continue
		}
		if splitNum <= 0 {
			log.LogWarnf("getSplitRules: split num(%v) need larger than 0", splitNum)
			continue
		}
		splitRules[clusterName] = splitNum
	}
	return
}

func (m *Monitor) getSplitKeys(initTime int64) []string {
	splitKeys := make([]string, 0)
	for clusterName, splitNum := range m.splitRegionRules {
		splitStep := OneHourSeconds / splitNum
		for i := int64(0); i < splitNum; i++ {
			splitTime := initTime + i*splitStep
			timeStr := time.Unix(splitTime, 0).Format(timeLayout)
			splitKeys = append(splitKeys, clusterName + "," + statistics.ModelDataNode + "," + timeStr)
		}
		splitKeys = append(splitKeys, clusterName + "," + statistics.ModelMetaNode)
	}
	return splitKeys
}

func (m *Monitor) scheduleTableTask(lastTime int64) {
	createTicker := time.NewTicker(TableRotate)
	defer createTicker.Stop()
	log.LogInfof("scheduleTableTask: start, rotate time(%v), clear time(%v)", TableRotate, TableClearTime)
	for {
		select {
		case <-createTicker.C:
			createTime := time.Unix(lastTime, 0).Add(TableRotate).Unix()
			tableName, splitKeys := m.getCreateTableInfo(createTime)
			thriftClient, err := hbase.OpenHBaseClient(m.thriftAddr)
			if err != nil {
				log.LogErrorf("scheduleTableTask: open thrift client failed (%v)", err)
				continue
			}
			log.LogInfof("initHBaseTable: table(%v) splitKeys(%v)", tableName, splitKeys)
			if err = thriftClient.CreateHBaseTable(m.namespace, tableName, []string{CFMonitor}, splitKeys); err != nil {
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

func (m *Monitor) getCreateTableInfo(timestamp int64) (tableName string, splitKeys []string) {
	tableName = getTableName(timestamp)
	splitKeys = m.getSplitKeys(timestamp)
	return
}

// get table name according to timestamp
func getTableName(timestamp int64) string {
	timeStr := time.Unix(timestamp, 0).Format(timeLayout)
	return fmt.Sprintf("%v_%v", TablePrefix, timeStr)
}

func getTimeOnHour(now time.Time) int64 {
	return now.Unix() - int64(now.Second()) - int64(60*now.Minute())
}
