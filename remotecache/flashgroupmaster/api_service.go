package flashgroupmaster

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/stat"
	"net/http"
	"strings"
)

func apiToMetricsName(api string) (reqMetricName string) {
	var builder strings.Builder
	builder.WriteString("req")
	// prometheus metric not allow '/' in name, need to transfer to '_'
	builder.WriteString(strings.Replace(api, "/", "_", -1))
	return builder.String()
}

func doStatAndMetric(statName string, metric *exporter.TimePointCount, err error, metricLabels map[string]string) {
	if metric == nil {
		return
	}
	if metricLabels == nil {
		metric.Set(err)
	} else {
		metric.SetWithLabels(err, metricLabels)
	}

	startTime := metric.GetStartTime()
	stat.EndStat(statName, err, &startTime, 1)
}

func (m *FlashGroupMaster) getCluster(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetCluster))
	defer func() {
		doStatAndMetric(proto.AdminGetCluster, metric, nil, nil)
	}()

	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	//TODO
	cv := &proto.ClusterView{
		//Name:                         m.cluster.Name,
		//CreateTime:                   time.Unix(m.cluster.CreateTime, 0).Format(proto.TimeFormat),
		//LeaderAddr:                   m.leaderInfo.addr,
		//FlashNodes:                   make([]proto.NodeView, 0),
		//FlashNodeHandleReadTimeout:   m.cluster.cfg.flashNodeHandleReadTimeout,
		//FlashNodeReadDataNodeTimeout: m.cluster.cfg.flashNodeReadDataNodeTimeout,
	}
	cv.DataNodeStatInfo = new(proto.NodeStatInfo)
	cv.MetaNodeStatInfo = new(proto.NodeStatInfo)
	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *FlashGroupMaster) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeInfo, metric, nil, nil)
	}()
	//compatible for cli tool
	resp := make(map[string]string)

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *FlashGroupMaster) getIPAddr(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetIP))
	defer func() {
		doStatAndMetric(proto.AdminGetIP, metric, nil, nil)
	}()
	cInfo := &proto.ClusterInfo{
		Cluster: m.clusterName,
		Ip:      iputil.RealIP(r),
	}
	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}
