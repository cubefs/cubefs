package monitor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/tsuna/gohbase/hrpc"
)

func (m *Monitor) collect(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = ioutil.ReadAll(r.Body); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	reportInfo := &statistics.ReportInfo{}
	if err = json.Unmarshal(bytes, reportInfo); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// send to jmq4
	if m.mqProducer != nil {
		select {
		case m.mqProducer.msgChan <- reportInfo:
		default:
			break
		}
	}

	// insert HBase
	m.PutDataToHBase(reportInfo)

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "insert hbase successfully"})
}

func (m *Monitor) getClusterTopIP(w http.ResponseWriter, r *http.Request) {
	var (
		table, module, start, end string
		limit                     int
		reply                     *proto.QueryHTTPReply
		err                       error
	)
	table, module, start, end, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateClusterQueryUrl("cfsIPTopN", table, module, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getClusterTopIP: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getClusterTopVol(w http.ResponseWriter, r *http.Request) {
	var (
		table, module, start, end string
		limit                     int
		reply                     *proto.QueryHTTPReply
		err                       error
	)
	table, module, start, end, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateClusterQueryUrl("cfsVolTopN", table, module, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getClusterTopVol: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getClusterTopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		table, module, start, end string
		limit                     int
		reply                     *proto.QueryHTTPReply
		err                       error
	)
	table, module, start, end, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateClusterQueryUrl("cfsPartitionTopN", table, module, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getClusterTopPartition: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getOpTopIP(w http.ResponseWriter, r *http.Request) {
	var (
		table, op, start, end string
		limit                 int
		reply                 *proto.QueryHTTPReply
		err                   error
	)
	table, op, start, end, limit, err = parseOpQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateOpQueryUrl("cfsIPOpTopN", table, op, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getOpTopIP: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getOpTopVol(w http.ResponseWriter, r *http.Request) {
	var (
		table, op, start, end string
		limit                 int
		reply                 *proto.QueryHTTPReply
		err                   error
	)
	table, op, start, end, limit, err = parseOpQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateOpQueryUrl("cfsVolOpTopN", table, op, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getOpTopVol: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getOpTopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		table, op, start, end string
		limit                 int
		reply                 *proto.QueryHTTPReply
		err                   error
	)
	table, op, start, end, limit, err = parseOpQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateOpQueryUrl("cfsPartitionOpTopN", table, op, start, end, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getOpTopPartition: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getIPTopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		ip        string
		module    string
		timeStr   string
		tableName string
		results   []*hrpc.Result
		err       error
	)
	if ip, module, timeStr, err = parseRowKeyFields(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if tableName, err = getTableNameByTimeStr(m.cluster, timeStr); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	prefixKey := fmt.Sprintf("total,%v,%v,%v", module, ip, timeStr)
	// row key: **total,module,ip,time,vol,pid**
	if results, err = m.hbase.ScanTableWithPrefixKey(m.hbase.namespace+":"+tableName, prefixKey); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	dataResults := parseDataFromHBaseResults(results, timeStr)
	log.LogDebugf("get results num(%v)", len(dataResults))
	top := selectKmaxCount(dataResults, topK)
	rangeDescAmongK(dataResults[:top])
	v := &proto.MonitorView{Monitors: dataResults[:top]}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Data: v, Msg: "getIPTopPartition successfully"})
}

func (m *Monitor) getTopPartitionOp(w http.ResponseWriter, r *http.Request) {
	var (
		ip        string
		module    string
		pid       string
		timeStr   string
		tableName string
		results   []*hrpc.Result
		err       error
	)
	if ip, module, timeStr, err = parseRowKeyFields(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if pid, err = extractPid(r); err != nil {
		return
	}
	if tableName, err = getTableNameByTimeStr(m.cluster, timeStr); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	prefixKey := fmt.Sprintf("%v,%v,%v,%v", module, ip, timeStr, pid)
	// row key: **module,ip,time,pid,op,vol**
	if results, err = m.hbase.ScanTableWithPrefixKey(m.hbase.namespace+":"+tableName, prefixKey); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	dataResults := parseDataFromHBaseResults(results, timeStr)
	log.LogDebugf("get results num(%v), res(%v)", len(dataResults), dataResults) // todo remove
	top := selectKmaxCount(dataResults, topK)
	rangeDescAmongK(dataResults[:top])
	v := &proto.MonitorView{Monitors: dataResults[:top]}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Data: v, Msg: "getTopPartitionOp successfully"})
}

func (m *Monitor) getTopVol(w http.ResponseWriter, r *http.Request) {
	var (
		module   string
		timeStr  string
		timeUnit string
		results  []*hrpc.Result
		err      error
	)
	if module, err = extractModule(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if timeStr, err = extractTimeStr(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if timeUnit, err = extractTimeUnit(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// tableName： CLUSTER_OPERATION_COUNT_SECOND_TABLE
	tableName := fmt.Sprintf("CLUSTER_OPERATION_COUNT_%v_TABLE", strings.ToUpper(timeUnit))
	prefixKey := fmt.Sprintf("%v,%v,%v,", module, timeStr, m.cluster)
	// row key: **module,timeStr,clusterName,volumeName**
	if results, err = m.hbase.ScanTableWithPrefixKey(tableName, prefixKey); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	dataResults := parseDataFromHBaseResults(results, timeStr)
	log.LogDebugf("get results num(%v), table(%v)", len(dataResults), tableName)
	top := selectKmaxCount(dataResults, topK)
	rangeDescAmongK(dataResults[:top])
	v := &proto.MonitorView{Monitors: dataResults[:top]}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Data: v, Msg: "getTopVol successfully"})
}

// find the most frequency operation of specific vol
func (m *Monitor) getTopVolOp(w http.ResponseWriter, r *http.Request) {
	var (
		vol        string
		timeStr    string
		timeUnit   string
		tableNames []string
		results    []*hrpc.Result
		err        error
	)
	if vol, err = extractVol(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if timeStr, err = extractTimeStr(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if timeUnit, err = extractTimeUnit(r); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// tableName：clusterName_action_timeUnit_Table
	tableNames = getOpTableName(m.cluster, timeUnit)
	if len(tableNames) < 1 {
		log.LogErrorf("getOpTableName: No cluster:(%v) Action Table", m.cluster)
		return
	}
	// rowKey: ** timeStr,volName**
	rowKey := fmt.Sprintf("%v,%v", timeStr, vol)

	for _, tableName := range tableNames {
		var result *hrpc.Result
		if result, err = m.hbase.GetEntireData(tableName, rowKey); err != nil {
			sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
			log.LogErrorf("getOpTableName: No This Table(%v) In Cluster(%v)", tableName, m.cluster)
			return
		}
		if result != nil {
			if result.Cells == nil {
				continue
			} else {
				results = append(results, result)
			}
		}
	}
	if len(results) < 1 {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: "get results failed, no data from OPERATION_table"})
		log.LogErrorf("getTopVolOp: get results failed, no data from OPERATION_TABLE, rowKey[%v]", rowKey)
		return
	}
	dataResults := parseDataFromHBaseResults(results, timeStr)
	log.LogDebugf("get results num(%v), results(%v)", len(dataResults), dataResults)
	top := selectKmaxCount(dataResults, topK)
	rangeDescAmongK(dataResults[:top])
	v := &proto.MonitorView{Monitors: dataResults[:top]}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Data: v, Msg: "getTopVolOp successfully"})
}

func parseRowKeyFields(r *http.Request) (ip, module string, timeStr string, err error) {
	if ip, err = extractNodeIP(r); err != nil {
		return
	}
	if module, err = extractModule(r); err != nil {
		return
	}
	if timeStr, err = extractTimeStr(r); err != nil {
		return
	}
	return
}

func swap(m []*statistics.MonitorData, i int, j int) {
	m[i], m[j] = m[j], m[i]
}

func partByPrivot(monitorData []*statistics.MonitorData, low, high int) int {
	var i, j int
	for {
		for i = low + 1; i < high; i++ {
			if monitorData[i].Count < monitorData[low].Count {
				break
			}
		}
		for j = high; j > low; j-- {
			if monitorData[j].Count >= monitorData[low].Count {
				break
			}
		}
		if i >= j {
			break
		}
		swap(monitorData, i, j)
	}
	if low != j {
		swap(monitorData, low, j)
	}
	return j
}

func selectKmaxCount(monitorData []*statistics.MonitorData, k int) (top int) {
	if len(monitorData) <= k {
		return len(monitorData)
	}
	low, high := 0, len(monitorData)-1
	for {
		privot := partByPrivot(monitorData, low, high)
		if privot < k {
			low = privot + 1
		} else if privot > k {
			high = privot - 1
		} else {
			return k
		}
	}
}

func rangeDescAmongK(dataArray []*statistics.MonitorData) {
	if len(dataArray) < 1 {
		return
	}
	sort.SliceStable(dataArray, func(i, j int) bool {
		return dataArray[i].Count > dataArray[j].Count
	})
}

func extractNodeIP(r *http.Request) (ip string, err error) {
	if ip = r.FormValue(nodeIPKey); ip == "" {
		err = keyNotFound(nodeIPKey)
		return
	}
	return
}

func extractModule(r *http.Request) (module string, err error) {
	if module = r.FormValue(moduleKey); module == "" {
		err = keyNotFound(moduleKey)
		return
	}
	return
}

func extractOperate(r *http.Request) (op string, err error) {
	if op = r.FormValue(opKey); op == "" {
		err = keyNotFound(opKey)
		return
	}
	return
}

func extractPid(r *http.Request) (pid string, err error) {
	if pid = r.FormValue(pidKey); pid == "" {
		err = keyNotFound(pidKey)
		return
	}
	return
}

func extractTimeStr(r *http.Request) (timeStr string, err error) {
	if timeStr = r.FormValue(timeKey); timeStr == "" {
		err = keyNotFound(timeKey)
		return
	}
	return
}

func extractTimeUnit(r *http.Request) (unit string, err error) {
	if unit = r.FormValue(timeUnitKey); unit == "" {
		//unit = defaultTimeUnit
		timeStr, _ := extractTimeStr(r)
		var (
			timeStrNum uint64
			count      int
		)
		timeStrNum, err = strconv.ParseUint(timeStr, 10, 64)
		if err != nil {
			return
		}

		for timeStrNum%uint64(10) == 0 {
			timeStrNum /= uint64(10)
			count += 1
		}

		if 0 <= count && count < 2 {
			unit = "second"
		} else if 2 <= count && count < 4 {
			unit = defaultTimeUnit
		} else if 4 <= count && count < 6 {
			unit = "hour"
		} else if 6 <= count && count < 8 {
			unit = "day"
		} else {
			err = wrongTimeStr(timeStr)
			return
		}
	}
	return
}

func extractVol(r *http.Request) (vol string, err error) {
	if vol = r.FormValue(volKey); vol == "" {
		err = keyNotFound(volKey)
		return
	}
	return
}

func keyNotFound(name string) (err error) {
	return errors.NewErrorf("parameter (%v) not found", name)
}
func wrongTimeStr(name string) (err error) {
	return errors.NewErrorf("timeStr(%v) is illegal", name)
}

func parseClusterQueryParams(r *http.Request) (table, module, start, end string, limit int, err error) {
	if module, err = extractModule(r); err != nil {
		return
	}
	if start, err = extractStartTime(r); err != nil {
		return
	}
	if end, err = extractEndTime(r); err != nil {
		return
	}
	table = extractTable(r)
	limit = extractLimit(r)
	return
}

func parseOpQueryParams(r *http.Request) (table, op, start, end string, limit int, err error) {
	if op, err = extractOperate(r); err != nil {
		return
	}
	if start, err = extractStartTime(r); err != nil {
		return
	}
	if end, err = extractEndTime(r); err != nil {
		return
	}
	table = extractTable(r)
	limit = extractLimit(r)
	return
}

func (m *Monitor) generateClusterQueryUrl(querySQL, table, module, start, end string, limit int) string {
	url := fmt.Sprintf("http://%v/queryJson/%v?cluster=%v&table=%v&module=%v&start=%v&end=%v&limit=%v",
		m.queryIP, querySQL, m.cluster, table, module, start, end, limit)
	return url
}

func (m *Monitor) generateOpQueryUrl(querySQL, table, op, start, end string, limit int) string {
	url := fmt.Sprintf("http://%v/queryJson/%v?cluster=%v&table=%v&op=%v&start=%v&end=%v&limit=%v",
		m.queryIP, querySQL, m.cluster, table, op, start, end, limit)
	return url
}

func extractTable(r *http.Request) (table string) {
	if table = r.FormValue(tableKey); table == "" {
		table = defaultTable
		return
	}
	return
}

func extractLimit(r *http.Request) (limit int) {
	var value string
	if value = r.FormValue(limitKey); value == "" {
		limit = defaultLimit
		return
	}
	var err error
	if limit, err = strconv.Atoi(value); err != nil || limit <= 0 {
		limit = defaultLimit
		return
	}
	return
}

func extractStartTime(r *http.Request) (start string, err error) {
	if start = r.FormValue(startKey); start == "" {
		err = keyNotFound(startKey)
		return
	}
	return
}

func extractEndTime(r *http.Request) (end string, err error) {
	if end = r.FormValue(endKey); end == "" {
		err = keyNotFound(endKey)
		return
	}
	return
}
