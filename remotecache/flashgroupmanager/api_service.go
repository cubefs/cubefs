package flashgroupmanager

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

var parseArgs = common.ParseArguments

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

func (m *FlashGroupManager) getCluster(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetCluster))
	defer func() {
		doStatAndMetric(proto.AdminGetCluster, metric, nil, nil)
	}()

	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	cv := &proto.ClusterView{
		Name:                         m.cluster.Name,
		CreateTime:                   time.Unix(m.cluster.CreateTime, 0).Format(proto.TimeFormat),
		LeaderAddr:                   m.leaderInfo.addr,
		MasterNodes:                  make([]proto.NodeView, 0),
		FlashNodes:                   make([]proto.NodeView, 0),
		FlashNodeHandleReadTimeout:   m.cluster.cfg.FlashNodeHandleReadTimeout,
		FlashNodeReadDataNodeTimeout: m.cluster.cfg.FlashNodeReadDataNodeTimeout,
		FlashHotKeyMissCount:         m.cluster.cfg.FlashHotKeyMissCount,
		FlashReadFlowLimit:           m.cluster.cfg.FlashReadFlowLimit,
		FlashWriteFlowLimit:          m.cluster.cfg.FlashWriteFlowLimit,
		RemoteCacheTTL:               m.config.RemoteCacheTTL,
		RemoteCacheReadTimeout:       m.config.RemoteCacheReadTimeout,
		RemoteCacheMultiRead:         m.config.RemoteCacheMultiRead,
		FlashNodeTimeoutCount:        m.config.FlashNodeTimeoutCount,
		RemoteCacheSameZoneTimeout:   m.config.RemoteCacheSameZoneTimeout,
		RemoteCacheSameRegionTimeout: m.config.RemoteCacheSameRegionTimeout,
	}
	cv.DataNodeStatInfo = new(proto.NodeStatInfo)
	cv.MetaNodeStatInfo = new(proto.NodeStatInfo)
	cv.MasterNodes = m.cluster.allMasterNodes()
	cv.FlashNodes = m.cluster.allFlashNodes()
	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *FlashGroupManager) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeInfo, metric, nil, nil)
	}()
	// compatible for cli tool
	resp := make(map[string]string)

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *FlashGroupManager) getIPAddr(w http.ResponseWriter, r *http.Request) {
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

func (m *FlashGroupManager) getRemoteCacheConfig(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetRemoteCacheConfig))
	defer func() {
		doStatAndMetric(proto.AdminGetRemoteCacheConfig, metric, err, nil)
	}()

	config := &proto.RemoteCacheConfig{
		FlashNodeHandleReadTimeout:   m.config.FlashNodeHandleReadTimeout,
		FlashNodeReadDataNodeTimeout: m.config.FlashNodeReadDataNodeTimeout,
		RemoteCacheTTL:               m.config.RemoteCacheTTL,
		RemoteCacheReadTimeout:       m.config.RemoteCacheReadTimeout,
		RemoteCacheMultiRead:         m.config.RemoteCacheMultiRead,
		FlashNodeTimeoutCount:        m.config.FlashNodeTimeoutCount,
		RemoteCacheSameZoneTimeout:   m.config.RemoteCacheSameZoneTimeout,
		RemoteCacheSameRegionTimeout: m.config.RemoteCacheSameRegionTimeout,
		FlashHotKeyMissCount:         m.config.FlashHotKeyMissCount,
		FlashReadFlowLimit:           m.config.FlashReadFlowLimit,
		FlashWriteFlowLimit:          m.config.FlashWriteFlowLimit,
	}
	log.LogInfof("getRemoteCacheConfig config(%v)", config)
	sendOkReply(w, r, newSuccessHTTPReply(config))
}

func (m *FlashGroupManager) clientFlashGroups(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientFlashGroups))
	defer func() {
		doStatAndMetric(proto.ClientFlashGroups, metric, err, nil)
	}()

	if !m.metaReady {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("meta not ready")))
		return
	}

	cache := m.cluster.flashNodeTopo.GetClientResponse()
	if len(cache) == 0 {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("flash group response cache is empty")))
		return
	}
	send(w, r, cache)
}

func (m *FlashGroupManager) turnFlashGroup(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupTurn))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupTurn, metric, err, nil)
	}()
	var enable common.Bool
	if err = parseArgs(r, enable.Enable()); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// TODO: should raft sync?
	topo := m.cluster.flashNodeTopo
	enabled := enable.V
	topo.TurnFlashGroup(enabled)
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("turn %v", enabled)))
}

func (m *FlashGroupManager) getFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID common.Uint
		flashGroup   *FlashGroup
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupGet))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupGet, metric, err, nil)
	}()
	if err = parseArgs(r, flashGroupID.ID()); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.GetFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *FlashGroupManager) listFlashGroups(w http.ResponseWriter, r *http.Request) {
	var (
		fgStatus  proto.FlashGroupStatus
		allStatus bool
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupList))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupList, metric, err, nil)
	}()
	var active common.Bool
	if err = parseArgs(r, active.Enable().OmitEmpty().
		OnEmpty(func() error {
			allStatus = true // resp all flash groups
			return nil
		}).
		OnValue(func() error {
			fgStatus = argConvertFlashGroupStatus(active.V)
			return nil
		}),
	); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	fgv := m.cluster.flashNodeTopo.GetFlashGroupsAdminView(fgStatus, allStatus)
	sendOkReply(w, r, newSuccessHTTPReply(fgv))
}

func (m *FlashGroupManager) setFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID common.Uint
		fgStatus     proto.FlashGroupStatus
		flashGroup   *FlashGroup
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupSet))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupSet, metric, err, nil)
	}()

	var active common.Bool
	if err = parseArgs(r, flashGroupID.ID(), active.Enable().OnValue(func() error {
		fgStatus = argConvertFlashGroupStatus(active.V)
		return nil
	}),
	); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.GetFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	err = flashGroup.UpdateStatus(fgStatus, m.cluster.syncUpdateFlashGroup, m.cluster.flashNodeTopo)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *FlashGroupManager) createFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		setSlots    []uint32
		setWeight   uint32
		gradualFlag bool
		step        uint32
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupCreate))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupCreate, metric, err, nil)
	}()
	if setSlots, err = getSetSlots(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if setWeight, err = getSetWeight(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if gradualFlag, err = getGradualFlag(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if step, err = getStep(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if gradualFlag && step <= 0 {
		err = fmt.Errorf("the step size(%v) must be greater than 0 when flashGroup gradually creates the slots", step)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashGroup, err := m.cluster.createFlashGroup(setSlots, setWeight, gradualFlag, step)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *FlashGroupManager) removeFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		gradualFlag bool
		step        uint32
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupRemove))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupRemove, metric, err, nil)
	}()
	var flashGroupID common.Uint
	if err = parseArgs(r, flashGroupID.ID()); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if gradualFlag, err = getGradualFlag(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if step, err = getStep(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if gradualFlag && step <= 0 {
		err = fmt.Errorf("the step size(%v) must be greater than 0 when flashGroup gradually deletes the slots", step)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	var flashGroup *FlashGroup
	if flashGroup, err = m.cluster.flashNodeTopo.RemoveFlashGroup(flashGroupID.V, gradualFlag, step,
		m.cluster.syncUpdateFlashGroup, m.cluster.syncUpdateFlashNode, m.cluster.syncDeleteFlashGroup); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("remove flashGroup:%v successfully,Slots:%v nodeCount:%v",
		flashGroup.ID, flashGroup.GetSlots(), flashGroup.GetFlashNodesCount())))
}

func (m *FlashGroupManager) addFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr common.String
		zoneName common.String
		version  common.String
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeAdd))
	defer func() {
		doStatAndMetric(proto.FlashNodeAdd, metric, err, nil)
	}()
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr),
		zoneName.ZoneName().OmitEmpty().OnValue(func() error {
			if zoneName.V == "" {
				zoneName.V = proto.DefaultZoneName
			}
			return nil
		}),
		version.Key("version").OmitEmpty(),
	); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if id, err = m.cluster.addFlashNode(nodeAddr.V, zoneName.V, version.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *FlashGroupManager) listFlashNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeList))
	defer func() {
		doStatAndMetric(proto.FlashNodeList, metric, nil, nil)
	}()
	showAll := true
	active := false
	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, exists := r.Form["active"]; exists {
		showAll = false
		activeReq, _ := strconv.ParseInt(r.FormValue("active"), 10, 64)
		if activeReq == -1 {
			showAll = true
		} else if activeReq == 1 {
			active = true
		}
	}
	zoneFlashNodes := m.cluster.flashNodeTopo.ListFlashNodes(showAll, active)
	sendOkReply(w, r, newSuccessHTTPReply(zoneFlashNodes))
}

func (m *FlashGroupManager) setFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  common.String
		enable    bool
		workRole  string
		flashNode *FlashNode
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeSet))
	defer func() {
		doStatAndMetric(proto.FlashNodeSet, metric, err, nil)
	}()
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if _, exists := r.Form["enable"]; exists {
		enable, err = strconv.ParseBool(r.FormValue("enable"))
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
		if err = m.cluster.updateFlashNode(flashNode, enable); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if _, exists := r.Form["workRole"]; exists {
		workRole = r.FormValue("workRole")
		if err = m.cluster.updateFlashNodeWorkRole(flashNode, workRole); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply("set flashNode success"))
}

func (m *FlashGroupManager) removeFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeRemove))
	defer func() {
		doStatAndMetric(proto.FlashNodeRemove, metric, err, nil)
	}()
	var offLineAddr common.String
	if err = parseArgs(r, argParserNodeAddr(&offLineAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var node *FlashNode
	if node, err = m.cluster.peekFlashNode(offLineAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if node.FlashGroupID != UnusedFlashNodeFlashGroupID {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("to delete a flashnode, it needs to be removed from the flashgroup first")))
		return
	}

	if err = m.cluster.removeFlashNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("delete flash node [%v] successfully", offLineAddr)))
}

func (m *FlashGroupManager) removeAllInactiveFlashNodes(w http.ResponseWriter, r *http.Request) {
	var err error
	removeAddresses := []string{}
	removeNodes := m.cluster.flashNodeTopo.GetAllInactiveFlashNodes()
	for _, node := range removeNodes {
		if err = m.cluster.removeFlashNode(node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		removeAddresses = append(removeAddresses, node.Addr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(removeAddresses))
}

func (m *FlashGroupManager) getFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeGet))
	defer func() {
		doStatAndMetric(proto.FlashNodeGet, metric, err, nil)
	}()
	var nodeAddr common.String
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var flashNode *FlashNode
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashNode.GetFlashNodeViewInfo()))
}

func (m *FlashGroupManager) flashGroupAddFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupNodeAdd))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupNodeAdd, metric, err, nil)
	}()
	flashGroupID, addr, zoneName, count, err := parseArgsFlashGroupNode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var flashGroup *FlashGroup
	if flashGroup, err = m.cluster.flashNodeTopo.FlashGroupAddFlashNode(flashGroupID,
		addr, zoneName, count, m.cluster.syncUpdateFlashNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *FlashGroupManager) handleFlashNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		tr  *proto.AdminTask
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetFlashNodeTaskResponse))
	defer func() {
		doStatAndMetric(proto.GetFlashNodeTaskResponse, metric, err, nil)
	}()

	tr, err = parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleFlashNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *FlashGroupManager) flashGroupRemoveFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupNodeRemove))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupNodeRemove, metric, err, nil)
	}()
	flashGroupID, addr, zoneName, count, err := parseArgsFlashGroupNode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var flashGroup *FlashGroup
	if flashGroup, err = m.cluster.flashNodeTopo.FlashGroupRemoveFlashNode(flashGroupID,
		addr, zoneName, count, m.cluster.syncUpdateFlashNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *FlashGroupManager) addRaftNode(w http.ResponseWriter, r *http.Request) {
	var (
		id   uint64
		addr string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddRaftNode))
	defer func() {
		doStatAndMetric(proto.AddRaftNode, metric, err, nil)
		AuditLog(r, proto.AddRaftNode, fmt.Sprintf("add raft node id :%v, addr:%v", id, addr), err)
	}()

	var msg string
	id, addr, err = parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successfully \n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *FlashGroupManager) removeRaftNode(w http.ResponseWriter, r *http.Request) {
	var (
		id   uint64
		addr string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RemoveRaftNode))
	defer func() {
		doStatAndMetric(proto.RemoveRaftNode, metric, err, nil)
		AuditLog(r, proto.RemoveRaftNode, fmt.Sprintf("del raft node id :%v, addr:%v", id, addr), err)
	}()

	var msg string
	id, addr, err = parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successfully\n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *FlashGroupManager) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RaftStatus))
	defer func() {
		doStatAndMetric(proto.RaftStatus, metric, nil, nil)
	}()

	data := m.raftStore.RaftStatus(GroupID)
	log.LogInfof("get raft status, %s", data.String())
	sendOkReply(w, r, newSuccessHTTPReply(data))
}

func (m *FlashGroupManager) changeMasterLeader(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminChangeMasterLeader))
	defer func() {
		doStatAndMetric(proto.AdminChangeMasterLeader, metric, err, nil)
		AuditLog(r, proto.AdminChangeMasterLeader, "", err)
	}()

	if err = m.cluster.tryToChangeLeaderByHost(); err != nil {
		log.LogErrorf("changeMasterLeader.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rstMsg := " changeMasterLeader. command success send to dest host but need check. "
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *FlashGroupManager) setNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminSetNodeInfo, metric, err, nil)
		AuditLog(r, proto.AdminSetNodeInfo, fmt.Sprintf("params: %v", params), err)
	}()

	if params, err = parseAndExtractSetNodeInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if val, ok := params[cfgFlashNodeHandleReadTimeout]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashNodeHandleReadTimeout, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[cfgFlashNodeReadDataNodeTimeout]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashNodeReadDataNodeTimeout, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[cfgRemoteCacheTTL]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgRemoteCacheTTL, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[cfgRemoteCacheReadTimeout]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgRemoteCacheReadTimeout, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[cfgRemoteCacheMultiRead]; ok {
		if v, ok := val.(bool); ok {
			if err = m.setConfig(cfgRemoteCacheMultiRead, strconv.FormatBool(v)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[cfgFlashNodeTimeoutCount]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashNodeTimeoutCount, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[cfgRemoteCacheSameZoneTimeout]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgRemoteCacheSameZoneTimeout, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[cfgRemoteCacheSameRegionTimeout]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgRemoteCacheSameRegionTimeout, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[cfgFlashHotKeyMissCount]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashHotKeyMissCount, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[cfgFlashReadFlowLimit]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashReadFlowLimit, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[cfgFlashWriteFlowLimit]; ok {
		if v, ok := val.(int64); ok {
			if err = m.setConfig(cfgFlashWriteFlowLimit, strconv.FormatInt(v, 10)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodeinfo params %v successfully", params)))
}

func (m *FlashGroupManager) setConfig(key string, value string) (err error) {
	var (
		fnHandleReadTimeout          int
		fnReadDataNodeTimeout        int
		flashHotKeyMissCount         int
		remoteCacheTTL               int64
		remoteCacheReadTimeout       int64
		remoteCacheMultiRead         bool
		flashNodeTimeoutCount        int64
		remoteCacheSameZoneTimeout   int64
		remoteCacheSameRegionTimeout int64
		flashReadFlowLimit           int64
		flashWriteFlowLimit          int64
		oldIntValue                  int
		oldInt64Value                int64
		oldBoolValue                 bool
	)

	log.LogInfof("set config key: %s, value: %s", key, value)
	defer func() {
		if err != nil {
			log.LogErrorf("set config failed, key: %s, value: %s, err: %v", key, value, err)
		}
	}()

	switch key {
	case cfgFlashNodeHandleReadTimeout:
		fnHandleReadTimeout, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		oldIntValue = m.config.FlashNodeHandleReadTimeout
		m.config.FlashNodeHandleReadTimeout = fnHandleReadTimeout

	case cfgFlashNodeReadDataNodeTimeout:
		fnReadDataNodeTimeout, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		oldIntValue = m.config.FlashNodeReadDataNodeTimeout
		m.config.FlashNodeReadDataNodeTimeout = fnReadDataNodeTimeout
	case cfgRemoteCacheTTL:
		remoteCacheTTL, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.RemoteCacheTTL
		m.config.RemoteCacheTTL = remoteCacheTTL
	case cfgRemoteCacheReadTimeout:
		remoteCacheReadTimeout, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.RemoteCacheReadTimeout
		m.config.RemoteCacheReadTimeout = remoteCacheReadTimeout
	case cfgRemoteCacheMultiRead:
		remoteCacheMultiRead, err = strconv.ParseBool(value)
		if err != nil {
			return err
		}
		oldBoolValue = m.config.RemoteCacheMultiRead
		m.config.RemoteCacheMultiRead = remoteCacheMultiRead
	case cfgFlashNodeTimeoutCount:
		flashNodeTimeoutCount, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.FlashNodeTimeoutCount
		m.config.FlashNodeTimeoutCount = flashNodeTimeoutCount
	case cfgRemoteCacheSameZoneTimeout:
		remoteCacheSameZoneTimeout, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.RemoteCacheSameZoneTimeout
		m.config.RemoteCacheSameZoneTimeout = remoteCacheSameZoneTimeout
	case cfgRemoteCacheSameRegionTimeout:
		remoteCacheSameRegionTimeout, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.RemoteCacheSameRegionTimeout
		m.config.RemoteCacheSameRegionTimeout = remoteCacheSameRegionTimeout
	case cfgFlashHotKeyMissCount:
		flashHotKeyMissCount, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		oldIntValue = m.config.FlashHotKeyMissCount
		m.config.FlashHotKeyMissCount = flashHotKeyMissCount

	case cfgFlashReadFlowLimit:
		flashReadFlowLimit, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.FlashReadFlowLimit
		m.config.FlashReadFlowLimit = flashReadFlowLimit

	case cfgFlashWriteFlowLimit:
		flashWriteFlowLimit, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		oldInt64Value = m.config.FlashWriteFlowLimit
		m.config.FlashWriteFlowLimit = flashWriteFlowLimit

	default:
		err = keyNotFound("config")
		return err
	}

	if err = m.cluster.syncPutCluster(); err != nil {
		switch key {
		case cfgFlashNodeHandleReadTimeout:
			m.config.FlashNodeHandleReadTimeout = oldIntValue
		case cfgFlashNodeReadDataNodeTimeout:
			m.config.FlashNodeReadDataNodeTimeout = oldIntValue
		case cfgRemoteCacheTTL:
			m.config.RemoteCacheTTL = oldInt64Value
		case cfgRemoteCacheReadTimeout:
			m.config.RemoteCacheReadTimeout = oldInt64Value
		case cfgRemoteCacheMultiRead:
			m.config.RemoteCacheMultiRead = oldBoolValue
		case cfgFlashNodeTimeoutCount:
			m.config.FlashNodeTimeoutCount = oldInt64Value
		case cfgRemoteCacheSameZoneTimeout:
			m.config.RemoteCacheSameZoneTimeout = oldInt64Value
		case cfgRemoteCacheSameRegionTimeout:
			m.config.RemoteCacheSameRegionTimeout = oldInt64Value
		case cfgFlashHotKeyMissCount:
			m.config.FlashHotKeyMissCount = oldIntValue
		case cfgFlashReadFlowLimit:
			m.config.FlashReadFlowLimit = oldInt64Value
		case cfgFlashWriteFlowLimit:
			m.config.FlashWriteFlowLimit = oldInt64Value
		}
		log.LogErrorf("setConfig syncPutCluster fail err %v", err)
		return err
	}
	return err
}

func (m *FlashGroupManager) setFlashNodeReadIOLimits(w http.ResponseWriter, r *http.Request) {
	var (
		flow       common.Int
		iocc       common.Int
		factor     common.Int
		readFlow   int64
		readIocc   int64
		readFactor int64
		err        error
	)

	if err = parseArgs(r, flow.Flow().OmitEmpty().OnEmpty(func() error {
		readFlow = -1
		return nil
	}).OnValue(func() error {
		readFlow = flow.V
		return nil
	}),
		iocc.Iocc().OmitEmpty().OnEmpty(func() error {
			readIocc = -1
			return nil
		}).OnValue(func() error {
			readIocc = iocc.V
			return nil
		}),
		factor.Factor().OmitEmpty().OnEmpty(func() error {
			readFactor = -1
			return nil
		}).OnValue(func() error {
			readFactor = factor.V
			return nil
		})); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogDebugf("action[setFlashNodeReadIOLimits],flow[%v] iocc[%v] factor [%v]",
		readFlow, readIocc, readFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := m.cluster.flashNodeTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(readFlow), int(readIocc), int(readFactor), proto.OpFlashNodeSetReadIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeSetIOLimitTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set ReadIOLimits for FlashNode is submit,check it later."))
}

func (m *FlashGroupManager) setFlashNodeWriteIOLimits(w http.ResponseWriter, r *http.Request) {
	var (
		flow        common.Int
		iocc        common.Int
		factor      common.Int
		writeFlow   int64
		writeIocc   int64
		writeFactor int64
		err         error
	)

	if err = parseArgs(r, flow.Flow().OmitEmpty().OnEmpty(func() error {
		writeFlow = -1
		return nil
	}).OnValue(func() error {
		writeFlow = flow.V
		return nil
	}),
		iocc.Iocc().OmitEmpty().OnEmpty(func() error {
			writeIocc = -1
			return nil
		}).OnValue(func() error {
			writeIocc = iocc.V
			return nil
		}),
		factor.Factor().OmitEmpty().OnEmpty(func() error {
			writeFactor = -1
			return nil
		}).OnValue(func() error {
			writeFactor = factor.V
			return nil
		})); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogDebugf("action[setFlashNodeWriteIOLimits],flow[%v] iocc[%v] factor [%v]",
		writeFlow, writeIocc, writeFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := m.cluster.flashNodeTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(writeFlow), int(writeIocc), int(writeFactor), proto.OpFlashNodeSetWriteIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeSetIOLimitTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set WriteIOLimits for FlashNode is submit,check it later."))
}

func getSetSlots(r *http.Request) (slots []uint32, err error) {
	r.ParseForm()
	slots = make([]uint32, 0)
	slotStr := r.FormValue("slots")
	if slotStr != "" {
		arr := strings.Split(slotStr, ",")
		var slot uint64
		for i := 0; i < len(arr); i++ {
			slot, err = strconv.ParseUint(arr[i], 10, 32)
			if err != nil {
				return nil, err
			}
			if len(slots) >= defaultFlashGroupSlotsCount {
				return
			}
			slots = append(slots, uint32(slot))
		}
	}
	return
}

func getSetWeight(r *http.Request) (weight uint32, err error) {
	var value uint64
	r.ParseForm()
	weightStr := r.FormValue("weight")
	if weightStr != "" {
		value, err = strconv.ParseUint(weightStr, 10, 32)
		weight = uint32(value)
	}
	return
}

func getGradualFlag(r *http.Request) (gradualCreateFlag bool, err error) {
	r.ParseForm()
	flagStr := r.FormValue("gradualFlag")
	if flagStr != "" {
		gradualCreateFlag, err = strconv.ParseBool(flagStr)
	}
	return
}

func getStep(r *http.Request) (step uint32, err error) {
	var value uint64
	r.ParseForm()
	stepStr := r.FormValue("step")
	if stepStr != "" {
		value, err = strconv.ParseUint(stepStr, 10, 32)
		step = uint32(value)
	}
	return
}

func argParserNodeAddr(nodeAddr *common.String) *common.Argument {
	return nodeAddr.Addr().OnValue(func() error {
		if ipAddr, ok := util.ParseAddrToIpAddr(nodeAddr.V); ok {
			nodeAddr.V = ipAddr
			return nil
		}
		return unmatchedKey(new(common.String).Addr().Key())
	})
}

func parseArgsFlashGroupNode(r *http.Request) (id uint64, addr, zoneName string, count int, err error) {
	var (
		idV    common.Uint
		addrV  common.String
		zoneV  common.String
		countV common.Int
	)
	if err = parseArgs(r, idV.ID(), addrV.Addr()); err == nil {
		id = idV.V
		addr = addrV.V
		return
	}
	if err = parseArgs(r, idV.ID(), addrV.Addr().OmitEmpty(), zoneV.ZoneName(), countV.Count()); err == nil {
		id = idV.V
		addr = addrV.V
		zoneName = zoneV.V
		count = int(countV.V)
	}
	return
}

func AuditLog(r *http.Request, op, msg string, err error) {
	head := fmt.Sprintf("%s %s", r.RemoteAddr, op)
	auditlog.LogMasterOp(head, msg, err)
}
