package monitor

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/chubaofs/chubaofs/util/hbase"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
)

const (
	CFMonitor = "MONITOR"

	CFMonitorMax    = "MAXTIME"
	CFMonitorMin    = "MINTIME"
	CFMonitorAvg    = "AVGTIME"
	CFMonitorSize   = "SIZE"
	CFMonitorCount  = "COUNT"
	CFMonitorIP     = "IP"
	CFMonitorAction = "ACTION"
	CFMonitorVol    = "VOLUME_NAME"
	CFMonitorPid    = "PID"
)

// remember there are 3 copies data of every partition
func (m *Monitor) PutDataToHBase(info *statistics.ReportInfo) {
	start := time.Now()
	module := info.Module
	nodeAddr := info.Addr
	tableDataMap := make(map[string]map[string][]*hbase.TColumnValue) // key: table, value: k-rowkey, v-columns
	for _, data := range info.Infos {
		if data.IsTotal {
			continue
		}
		table := getTableName(getTimeOnHour(time.Unix(data.ReportTime, 0)))
		timeStr := time.Unix(data.ReportTime, 0).Format(timeLayout)
		var (
			dataMap map[string][]*hbase.TColumnValue
			exist	bool
		)
		if dataMap, exist = tableDataMap[table]; !exist {
			dataMap = make(map[string][]*hbase.TColumnValue)
		}
		if data.IsTotal {
			// row key: **total,cluster,module,ip,time,vol,pid**
			rowKey := fmt.Sprintf("total,%v,%v,%v,%v,%v,%v",
				info.Cluster, module, nodeAddr, timeStr, data.VolName, data.PartitionID)
			columns := constructThriftCols(0, data.Count, data.PartitionID, nodeAddr, data.VolName, "")
			dataMap[rowKey] = columns
		} else {
			// row key: **cluster,module,ip,time,pid,op,vol**
			rowKey := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v",
				info.Cluster, module, nodeAddr, timeStr, data.PartitionID, data.Action, data.VolName)
			columns := constructThriftCols(data.Size, data.Count, data.PartitionID, info.Addr, data.VolName, data.ActionStr)
			dataMap[rowKey] = columns
		}
		tableDataMap[table] = dataMap
	}
	thriftClient, err := hbase.OpenHBaseClient(m.thriftAddr)
	if err != nil {
		log.LogErrorf("PutDataToHBase: open thrift client failed (%v)", err)
		return
	}
	for tableName, dataMap := range tableDataMap {
		if err = thriftClient.PutMultiData(m.namespace, tableName, dataMap); err != nil {
			log.LogErrorf("PutDataToHBase failed: put multi data err(%v) table(%v) dataLen(%v)", err, tableName, len(dataMap))
		}
	}
	if err = thriftClient.CloseHBaseClient(); err != nil {
		log.LogErrorf("PutDataToHBase: close thrift client err (%v)", err)
	}
	log.LogDebugf("PutDataToHBase cost time(%v), data num(%v), cluster(%v) ip(%v) module(%v)", time.Since(start), len(info.Infos), info.Cluster, info.Addr, info.Module)
}

func (m *Monitor) countData(info *statistics.ReportInfo) {
	start := time.Now()
	dataMap := make(map[string]int)
	for _, data := range info.Infos {
		if data.IsTotal {
			continue
		}
		table := getTableName(getTimeOnHour(time.Unix(data.ReportTime, 0)))
		dataMap[table]++
	}
	for table, count := range dataMap {
		log.LogDebugf("PutDataToHBase: table(%v) count(%v) cluster(%v) module(%v) ip(%v)", table, count, info.Cluster, info.Module, info.Addr)
	}
	log.LogDebugf("PutDataToHBase cost time(%v), data num(%v), cluster(%v) ip(%v) module(%v)", time.Since(start), len(info.Infos), info.Cluster, info.Addr, info.Module)
}

func constructThriftCols(size, count, pid uint64, ip, vol, action string) []*hbase.TColumnValue {
	dataSize := make([]byte, 8)
	countNum := make([]byte, 8)
	pidByte := make([]byte, 8)
	binary.BigEndian.PutUint64(dataSize, size)
	binary.BigEndian.PutUint64(countNum, count)
	binary.BigEndian.PutUint64(pidByte, pid)
	columnValue := []*hbase.TColumnValue{
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorSize),
			Value:     dataSize,
		},
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorCount),
			Value:     countNum,
		},
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorPid),
			Value:     pidByte,
		},
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorIP),
			Value:     []byte(ip),
		},
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorVol),
			Value:     []byte(vol),
		},
		&hbase.TColumnValue{
			Family:    []byte(CFMonitor),
			Qualifier: []byte(CFMonitorAction),
			Value:     []byte(action),
		},
	}
	return columnValue
}

func getTimeNum(timestamp int64) int64 {
	nowTime := time.Unix(timestamp, 0)
	hourTime := getTimeOnHour(nowTime)
	return timestamp - hourTime
}

func parseDataFromHBaseResults(results []*hbase.TResult_, timeStr string) (dataResults []*statistics.MonitorData) {
	//dataResults = make([]*statistics.MonitorData, len(results))
	for _, res := range results {
		data := &statistics.MonitorData{}
		for _, columnValue := range res.GetColumnValues() {
			column := string(columnValue.Qualifier)
			switch column {
			case CFMonitorCount:
				data.Count = binary.BigEndian.Uint64(columnValue.Value)
			case CFMonitorSize:
				data.Size = binary.BigEndian.Uint64(columnValue.Value)
			case CFMonitorAction:
				data.ActionStr = string(columnValue.Value)
			case CFMonitorVol:
				data.VolName = string(columnValue.Value)
			case CFMonitorPid:
				data.PartitionID = binary.BigEndian.Uint64(columnValue.Value)
			default:
				continue
			}
		}
		data.TimeStr = timeStr
		dataResults = append(dataResults, data)
	}
	return
}
