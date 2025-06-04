package flashgroupmanager

import (
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/util"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/stat"
)

var (
	parseArgs = common.ParseArguments
	newArg    = common.NewArgument
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

func (m *FlashGroupManager) getCluster(w http.ResponseWriter, r *http.Request) {
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
		Name:       m.cluster.Name,
		CreateTime: time.Unix(m.cluster.CreateTime, 0).Format(proto.TimeFormat),
		//LeaderAddr:                   m.leaderInfo.addr,
		//FlashNodes:                   make([]proto.NodeView, 0),
		//FlashNodeHandleReadTimeout:   m.cluster.cfg.flashNodeHandleReadTimeout,
		//FlashNodeReadDataNodeTimeout: m.cluster.cfg.flashNodeReadDataNodeTimeout,
	}
	cv.DataNodeStatInfo = new(proto.NodeStatInfo)
	cv.MetaNodeStatInfo = new(proto.NodeStatInfo)
	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *FlashGroupManager) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeInfo, metric, nil, nil)
	}()
	//compatible for cli tool
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

func (m *FlashGroupManager) clientFlashGroups(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientFlashGroups))
	defer func() {
		doStatAndMetric(proto.ClientFlashGroups, metric, err, nil)
	}()

	//TODO
	//if !m.metaReady {
	//	sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("meta not ready")))
	//	return
	//}
	cache := m.cluster.flashNodeTopo.getClientResponse()
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
	if enabled {
		topo.clientOff.Store([]byte(nil))
	} else {
		topo.clientOff.Store(topo.clientEmpty)
	}
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
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID.V); err != nil {
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
	fgv := m.cluster.flashNodeTopo.getFlashGroupsAdminView(fgStatus, allStatus)
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
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	flashGroup.lock.Lock()
	oldStatus := flashGroup.Status
	flashGroup.Status = fgStatus
	if oldStatus != fgStatus {
		// TODO
		//if err = m.cluster.syncUpdateFlashGroup(flashGroup); err != nil {
		//	flashGroup.Status = oldStatus
		//	flashGroup.lock.Unlock()
		//	sendErrReply(w, r, newErrHTTPReply(err))
		//	return
		//}
		m.cluster.flashNodeTopo.updateClientCache()
	}
	flashGroup.lock.Unlock()

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
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup.getSlotStatus() == proto.SlotStatus_Deleting {
		err = fmt.Errorf("the flashGroup(%v) is in slotDeleting status, it cannot be deleted repeatedly", flashGroup.ID)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.removeFlashGroup(flashGroup, gradualFlag, step); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	m.cluster.flashNodeTopo.updateClientCache()
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("remove flashGroup:%v successfully,Slots:%v nodeCount:%v",
		flashGroup.ID, flashGroup.getSlots(), flashGroup.getFlashNodesCount())))
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
	zoneFlashNodes := make(map[string][]*proto.FlashNodeViewInfo)
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
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if showAll || flashNode.isActiveAndEnable() == active {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.GetFlashNodeViewInfo())
		}
		return true
	})
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
	var (
		err         error
		removeNodes []*FlashNode
	)
	removeAddresses := []string{}
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if !flashNode.isActiveAndEnable() && flashNode.FlashGroupID == UnusedFlashNodeFlashGroupID {
			removeNodes = append(removeNodes, flashNode)
		}
		return true
	})
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
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if addr != "" {
		err = m.cluster.addFlashNodeToFlashGroup(addr, flashGroup)
	} else {
		err = m.cluster.selectFlashNodesFromZoneAddToFlashGroup(zoneName, count, nil, flashGroup)
	}
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	m.cluster.flashNodeTopo.updateClientCache()
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
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if addr != "" {
		err = m.cluster.removeFlashNodeFromFlashGroup(addr, flashGroup)
	} else {
		err = m.cluster.removeFlashNodesFromTargetZone(zoneName, count, flashGroup)
	}
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	m.cluster.flashNodeTopo.updateClientCache()
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
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
