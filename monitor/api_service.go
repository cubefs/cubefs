package monitor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
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
	if m.mqProducer != nil && contains(m.clusters, reportInfo.Cluster) {
		epoch := atomic.AddUint64(&m.mqProducer.epoch, 1)
		index := epoch % uint64(m.mqProducer.produceNum)
		select {
		case m.mqProducer.msgChan[index] <- reportInfo:
		default:
			break
		}
	}

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "insert hbase successfully"})
}

func (m *Monitor) setCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := extractCluster(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(cluster) > 0 {
		m.clusters = strings.Split(cluster, ",")
	}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set cluster to (%v)", m.clusters)})
}

func (m *Monitor) getClusterTopIP(w http.ResponseWriter, r *http.Request) {
	var (
		tableUnit, cluster, module, start, end, order string
		limit                                         int
		reply                                         *proto.QueryHTTPReply
		err                                           error
	)
	tableUnit, cluster, module, start, end, order, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateClusterQueryUrl("cfsIPTopN", tableUnit, cluster, module, start, end, order, limit)
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
		tableUnit, cluster, module, start, end, order string
		limit                                         int
		reply                                         *proto.QueryHTTPReply
		err                                           error
	)
	tableUnit, cluster, module, start, end, order, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateClusterQueryUrl("cfsVolTopN", tableUnit, cluster, module, start, end, order, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getClusterTopVol: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getOpTopIP(w http.ResponseWriter, r *http.Request) {
	var (
		tableUnit, cluster, op, start, end, order string
		limit                                     int
		reply                                     *proto.QueryHTTPReply
		err                                       error
	)
	tableUnit, cluster, op, start, end, order, limit, err = parseOpQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateOpQueryUrl("cfsIPOpTopN", tableUnit, cluster, op, start, end, order, limit)
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
		tableUnit, cluster, op, start, end, order string
		limit                                     int
		reply                                     *proto.QueryHTTPReply
		err                                       error
	)
	tableUnit, cluster, op, start, end, order, limit, err = parseOpQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	url := m.generateOpQueryUrl("cfsVolOpTopN", tableUnit, cluster, op, start, end, order, limit)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getOpTopVol: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getTopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		table, cluster, module, start, end, order string
		limit                                     int
		reply                                     *proto.QueryHTTPReply
		err                                       error
	)
	_, cluster, module, start, end, order, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	table, group, ip, op, _, _ := parsePartitionQueryParams(r)
	if table == "" {
		if table, err = getPartitionQueryTable(start, end); err != nil {
			return
		}
	}
	url := m.generatePartitionQueryUrl("cfsPartitionTopN", table, cluster, module, start, end, limit, group, order, ip, op, "", "")
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getTopPartition: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getTopOp(w http.ResponseWriter, r *http.Request) {
	var (
		table, cluster, module, start, end, order string
		limit                                     int
		reply                                     *proto.QueryHTTPReply
		err                                       error
	)
	_, cluster, module, start, end, order, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	table, _, ip, _, pid, vol := parsePartitionQueryParams(r)
	if table == "" {
		if table, err = getPartitionQueryTable(start, end); err != nil {
			return
		}
	}
	url := m.generatePartitionQueryUrl("cfsPartitionTopOp", table, cluster, module, start, end, limit, "", order, ip, "", vol, pid)
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getTopPartition: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

func (m *Monitor) getTopIP(w http.ResponseWriter, r *http.Request) {
	var (
		table, cluster, module, start, end, order string
		limit                                     int
		reply                                     *proto.QueryHTTPReply
		err                                       error
	)
	_, cluster, module, start, end, order, limit, err = parseClusterQueryParams(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	table, _, _, op, _, vol := parsePartitionQueryParams(r)
	if table == "" {
		if table, err = getPartitionQueryTable(start, end); err != nil {
			return
		}
	}
	url := m.generatePartitionQueryUrl("cfsPartitionTopIP", table, cluster, module, start, end, limit, "", order, "", op, vol, "")
	if reply, err = requestQuery(url); err != nil {
		log.LogErrorf("getTopPartition: requestQuery failed, err(%v) url(%v)", err, url)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeHBaseOperation, Msg: err.Error()})
		return
	}
	queryView := &proto.QueryView{Data: reply.Data}
	sendReply(w, r, &proto.HTTPReply{Code: reply.Code, Data: queryView, Msg: reply.Msg})
}

// table name format: **cfs_monitor_20060102150000**
func getPartitionQueryTable(start string, end string) (table string, err error) {
	var (
		startTime	time.Time
		endTime		time.Time
	)
	if startTime, err = time.ParseInLocation(timeLayout, start, time.Local); err != nil {
		return
	}
	if endTime, err = time.ParseInLocation(timeLayout, end, time.Local); err != nil {
		return
	}
	startHour := getTimeOnHour(startTime)
	endHour := getTimeOnHour(endTime)
	if startHour == endHour {
		table = strings.ToLower(getTableName(startHour))
		return
	}
	// compare time difference, use table which has more data
	if endHour - startTime.Unix() > endTime.Unix() - endHour {
		table = strings.ToLower(getTableName(startHour))
	} else {
		table = strings.ToLower(getTableName(endHour))
	}
	return
}

func extractNodeIP(r *http.Request) (ip string, err error) {
	if ip = r.FormValue(nodeIPKey); ip == "" {
		err = keyNotFound(nodeIPKey)
		return
	}
	return
}

func extractCluster(r *http.Request) (cluster string, err error) {
	if cluster = r.FormValue(clusterKey); cluster == "" {
		err = keyNotFound(clusterKey)
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

func parseClusterQueryParams(r *http.Request) (table, cluster, module, start, end, order string, limit int, err error) {
	if cluster, err = extractCluster(r); err != nil {
		return
	}
	if module, err = extractModule(r); err != nil {
		return
	}
	if start, err = extractStartTime(r); err != nil {
		return
	}
	if end, err = extractEndTime(r); err != nil {
		return
	}
	table = extractTableUnit(r)
	limit = extractLimit(r)
	order, _ = extractOrder(r)
	return
}

func parseOpQueryParams(r *http.Request) (table, cluster, op, start, end, order string, limit int, err error) {
	if cluster, err = extractCluster(r); err != nil {
		return
	}
	if op, err = extractOperate(r); err != nil {
		return
	}
	if start, err = extractStartTime(r); err != nil {
		return
	}
	if end, err = extractEndTime(r); err != nil {
		return
	}
	table = extractTableUnit(r)
	limit = extractLimit(r)
	order, _ = extractOrder(r)
	return
}

func parsePartitionQueryParams(r *http.Request) (table, group, ip, op, pid, vol string) {
	table, _ = extractTable(r)
	group, _ = extractGroup(r)
	ip, _ = extractNodeIP(r)
	op, _ = extractOperate(r)
	pid, _ = extractPid(r)
	vol, _ = extractVol(r)
	return
}

func (m *Monitor) generateClusterQueryUrl(querySQL, table, cluster, module, start, end, order string, limit int) string {
	url := fmt.Sprintf("http://%v/queryJson/%v?cluster=%v&table=%v&module=%v&start=%v&end=%v&order=%v&limit=%v",
		m.queryIP, querySQL, cluster, table, module, start, end, order, limit)
	return url
}

func (m *Monitor) generateOpQueryUrl(querySQL, table, cluster, op, start, end, order string, limit int) string {
	url := fmt.Sprintf("http://%v/queryJson/%v?cluster=%v&table=%v&op=%v&start=%v&end=%v&order=%v&limit=%v",
		m.queryIP, querySQL, cluster, table, op, start, end, order, limit)
	return url
}

func (m *Monitor) generatePartitionQueryUrl(querySQL, table, cluster, module, start, end string, limit int, group, order, ip, op, vol, pid string) string {
	url := fmt.Sprintf("http://%v/queryJson/%v?cluster=%v&table=%v&module=%v&start=%v&end=%v&limit=%v&group=%v&order=%v&ip=%v&op=%v&vol=%v&pid=%v",
		m.queryIP, querySQL, cluster, table, module, start, end, limit, group, order, ip, op, vol, pid)
	return url
}

func extractTableUnit(r *http.Request) (tableUnit string) {
	if tableUnit = r.FormValue(tableUnitKey); tableUnit == "" {
		tableUnit = defaultTableUnit
		return
	}
	return
}

func extractTable(r *http.Request) (table string, err error) {
	if table = r.FormValue(tableKey); table == "" {
		err = keyNotFound(tableKey)
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

func extractGroup(r *http.Request) (group string, err error) {
	if group = r.FormValue(groupKey); group == "" {
		err = keyNotFound(groupKey)
		return
	}
	return
}

func extractOrder(r *http.Request) (order string, err error) {
	if order = r.FormValue(orderKey); order == "" {
		err = keyNotFound(orderKey)
		return
	}
	return
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}