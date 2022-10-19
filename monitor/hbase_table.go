package monitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/hbase"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
)

const (
	CFMonitor 	= "MONITOR"
	CF1 		= "CF1"
	CF2 		= "CF2"
)

var (
	DataTablePrefix = "CFS_MONITOR"
	VolTablePrefix  = "CFS_VOL_MONITOR"
	HourTablePrefix = []string{DataTablePrefix, VolTablePrefix}
	DayTablePrefix  = []string{"CFS_IP_MONITOR", "CFS_CLUSTER_MONITOR"}
	TableHourRotate = 1 * time.Hour
	TableDayRotate	= 24 * time.Hour
	TableClearTime  = 7 * 24 * time.Hour
	OneHourSeconds  = int64(60 * 60)
)

type TableInfo struct {
	name		string
	splitKeys	[]string
	cFamilies	[]string
}

func (m *Monitor) initHBaseTable() (err error) {
	// 1. Check table on the hour
	initHourTime := getTimeOnHour(time.Now())
	tables := m.getHourCreateTableInfo(initHourTime)
	tables = append(tables, m.getDayCreateTableInfo(initHourTime)...)
	var thriftClient *hbase.THBaseServiceClient
	thriftClient, err = hbase.OpenHBaseClient(m.thriftAddr)
	if err != nil {
		log.LogErrorf("initHBaseTable: open thrift client failed (%v)", err)
		return
	}
	for _, table := range tables {
		log.LogInfof("initHBaseTable: table(%v) splitKeys(%v) cf(%v)", table.name, table.splitKeys, table.cFamilies)
		if err = thriftClient.CreateHBaseTable(m.namespace, table.name, table.cFamilies, table.splitKeys); err != nil {
			log.LogErrorf("initHBaseTable failed: create table err(%v) table name(%v)", err, table.name)
		}
	}
	nextTime := time.Unix(initHourTime, 0).Add(TableHourRotate).Unix()
	nextTables := m.getHourCreateTableInfo(nextTime)
	nextDayTime := time.Unix(initHourTime, 0).Add(TableDayRotate).Unix()
	nextTables = append(nextTables, m.getDayCreateTableInfo(nextDayTime)...)
	for _, nextTable := range nextTables {
		log.LogInfof("initHBaseTable: table(%v) splitKeys(%v) cf(%v)", nextTable.name, nextTable.splitKeys, nextTable.cFamilies)
		if err = thriftClient.CreateHBaseTable(m.namespace, nextTable.name, nextTable.cFamilies, nextTable.splitKeys); err != nil {
			log.LogErrorf("initHBaseTable failed: create next table err(%v) table name(%v)", err, nextTable.name)
		}
	}
	if err = thriftClient.CloseHBaseClient(); err != nil {
		log.LogErrorf("initHBaseTable: close thrift client err (%v)", err)
	}
	// 2. Create a new table on hour in advance
	go m.scheduleTableTask(nextTime, nextDayTime)
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

func (m *Monitor) getTimeSplitKeys(initTime int64) []string {
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

func (m *Monitor) getDataSplitKeys() []string {
	splitKeys := make([]string, 0)
	for i := 0; i < 10; i++ {
		for clusterName := range m.splitRegionRules {
			splitKeys = append(splitKeys, fmt.Sprintf("%02d,%s", i, clusterName))
		}
	}
	return splitKeys
}

func (m *Monitor) scheduleTableTask(lastTime, lastDayTime int64) {
	createHourTicker := time.NewTicker(TableHourRotate)
	defer createHourTicker.Stop()
	createDayTicker := time.NewTicker(TableDayRotate)
	defer createDayTicker.Stop()
	log.LogInfof("scheduleTableTask: start, rotate time(%v), clear time(%v)", TableHourRotate, TableClearTime)
	for {
		select {
		case <-createHourTicker.C:
			createTime := time.Unix(lastTime, 0).Add(TableHourRotate).Unix()
			tables := m.getHourCreateTableInfo(createTime)
			thriftClient, err := hbase.OpenHBaseClient(m.thriftAddr)
			if err != nil {
				log.LogErrorf("scheduleTableTask: open thrift client failed (%v)", err)
				continue
			}
			for _, table := range tables {
				log.LogInfof("initHBaseTable: table(%v) splitKeys(%v) cf(%v)", table.name, table.splitKeys, table.cFamilies)
				if err = thriftClient.CreateHBaseTable(m.namespace, table.name, table.cFamilies, table.splitKeys); err != nil {
					log.LogErrorf("scheduleTableTask failed: create next table err(%v) table name(%v)", err, table.name)
				}
			}
			lastTime = createTime
			m.deleteExpiresTables(thriftClient)
			if err = thriftClient.CloseHBaseClient(); err != nil {
				log.LogErrorf("scheduleTableTask: close thrift client err (%v)", err)
			}
		case <-createDayTicker.C:
			createTime := time.Unix(lastDayTime, 0).Add(TableDayRotate).Unix()
			tables := m.getDayCreateTableInfo(createTime)
			thriftClient, err := hbase.OpenHBaseClient(m.thriftAddr)
			if err != nil {
				log.LogErrorf("scheduleTableTask: open thrift client failed (%v)", err)
				continue
			}
			for _, table := range tables {
				log.LogInfof("initHBaseTable: table(%v) splitKeys(%v) cf(%v)", table.name, table.splitKeys, table.cFamilies)
				if err = thriftClient.CreateHBaseTable(m.namespace, table.name, table.cFamilies, table.splitKeys); err != nil {
					log.LogErrorf("scheduleTableTask failed: create next table err(%v) table name(%v)", err, table.name)
				}
			}
			lastDayTime = createTime
			if err = thriftClient.CloseHBaseClient(); err != nil {
				log.LogErrorf("scheduleTableTask: close thrift client err (%v)", err)
			}
		case <-m.stopC:
			log.LogInfof("scheduleTableTask: stop, last time(%v) day time(%v)", lastTime, lastDayTime)
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
		if !m.hasPrefixTable(name, HourTablePrefix) && !m.hasPrefixTable(name, DayTablePrefix) {
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

func (m *Monitor) getHourCreateTableInfo(timestamp int64) (tables []*TableInfo) {
	for _, prefix := range HourTablePrefix {
		tableInfo := &TableInfo{}
		tableInfo.name = getTableName(timestamp, prefix)
		switch prefix {
		case DataTablePrefix:
			tableInfo.splitKeys = m.getDataSplitKeys()
			tableInfo.cFamilies = []string{CFMonitor}
		case VolTablePrefix:
			tableInfo.splitKeys = []string{"mysql,data", "mysql,meta", "mysql,z"}
			tableInfo.cFamilies = []string{CF1, CF2}
		}
		tables = append(tables, tableInfo)
	}
	return
}

func (m *Monitor) getDayCreateTableInfo(timestamp int64) (tables []*TableInfo) {
	for _, prefix := range DayTablePrefix {
		tableInfo := &TableInfo{}
		tableInfo.name = getDayTableName(timestamp, prefix)
		tableInfo.cFamilies = []string{CF1, CF2}
		tables = append(tables, tableInfo)
	}
	return
}

// get table name according to timestamp
func getTableName(timestamp int64, prefix string) string {
	timeStr := time.Unix(timestamp, 0).Format(timeLayout)
	return fmt.Sprintf("%v_%v", prefix, timeStr)
}

func getDayTableName(timestamp int64, prefix string) string {
	timeStr := time.Unix(timestamp, 0).Format(dayTimeLayout)
	return fmt.Sprintf("%v_%v", prefix, timeStr)
}

func getTimeOnHour(now time.Time) int64 {
	return now.Unix() - int64(now.Second()) - int64(60*now.Minute())
}
