package monitor

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/tsuna/gohbase/hrpc"
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
	for _, data := range info.Infos {
		if data.IsTotal {
			m.putPartitionSumCount(module, nodeAddr, data)
		} else {
			// row key: **module,ip,time,pid,op,vol**
			//rowKey := constructRowKey(module, nodeAddr, data)
			//values := constructValues(data.Size, data.Count, data.PartitionID, info.Addr, data.VolName, data.ActionStr)
			//table := getTableName(m.cluster, getTimeOnHour(time.Unix(data.ReportTime, 0)))
			//log.LogDebugf("table name: %v", table)
			//if err := m.hbase.PutData(table, rowKey, values); err != nil {
			//	log.LogErrorf("PutDataToHBase failed: err(%v) table(%v) rowKey(%v)", err, table, rowKey)
			//	continue
			//}
		}
	}
	log.LogDebugf("PutDataToHBase cost time: (%v), data num(%v)", time.Since(start), len(info.Infos))
}

func (m *Monitor) putPartitionSumCount(module, ip string, data *statistics.MonitorData) {
	timeStr := time.Unix(data.ReportTime, 0).Format(timeLayout)
	// row key: **total,module,ip,time,vol,pid**
	rowKey := fmt.Sprintf("total,%v,%v,%v,%v,%v", module, ip, timeStr, data.VolName, data.PartitionID)
	values := constructValues(0, data.Count, data.PartitionID, ip, data.VolName, "")
	table := getTableName(m.cluster, getTimeOnHour(time.Unix(data.ReportTime, 0)))
	log.LogDebugf("table name: %v", table)
	if err := m.hbase.PutData(table, rowKey, values); err != nil {
		log.LogErrorf("putPartitionSumCount failed: put total data err(%v) table(%v) rowKey(%v)", err, table, rowKey)
	}
}

func constructRowKey(module, nodeIP string, data *statistics.MonitorData) string {
	timeFormat := time.Unix(data.ReportTime, 0).Format(timeLayout)
	return fmt.Sprintf("%v,%v,%v,%v,%v,%v", module, nodeIP, timeFormat, data.PartitionID, data.Action, data.VolName)
}

func constructValues(size, count, pid uint64, ip, vol, action string) map[string]map[string][]byte {
	dataSize := make([]byte, 8)
	countNum := make([]byte, 8)
	pidByte := make([]byte, 8)
	binary.BigEndian.PutUint64(dataSize, size)
	binary.BigEndian.PutUint64(countNum, count)
	binary.BigEndian.PutUint64(pidByte, pid)
	values := make(map[string]map[string][]byte)
	values[CFMonitor] = make(map[string][]byte)
	values[CFMonitor][CFMonitorSize] = dataSize
	values[CFMonitor][CFMonitorCount] = countNum
	values[CFMonitor][CFMonitorPid] = pidByte
	values[CFMonitor][CFMonitorIP] = []byte(ip)
	values[CFMonitor][CFMonitorVol] = []byte(vol)
	values[CFMonitor][CFMonitorAction] = []byte(action)
	return values
}

func getTimeNum(timestamp int64) int64 {
	nowTime := time.Unix(timestamp, 0)
	hourTime := getTimeOnHour(nowTime)
	return timestamp - hourTime
}

func parseDataFromHBaseResults(results []*hrpc.Result, timeStr string) (dataResults []*statistics.MonitorData) {
	//dataResults = make([]*statistics.MonitorData, len(results))
	for _, res := range results {
		data := &statistics.MonitorData{}
		for _, cell := range res.Cells {
			column := string(cell.Qualifier)
			switch column {
			case CFMonitorCount:
				data.Count = binary.BigEndian.Uint64(cell.Value)
			case CFMonitorSize:
				data.Size = binary.BigEndian.Uint64(cell.Value)
			case CFMonitorAction:
				data.ActionStr = string(cell.Value)
			case CFMonitorVol:
				data.VolName = string(cell.Value)
			case CFMonitorPid:
				data.PartitionID = binary.BigEndian.Uint64(cell.Value)
			default:
				continue
			}
		}
		data.TimeStr = timeStr
		dataResults = append(dataResults, data)
	}
	return
}
