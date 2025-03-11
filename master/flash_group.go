// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/sdk/httpclient"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

var defaultWaitClientUpdateFgTimeSec = 65

type flashGroupValue struct {
	ID           uint64
	Slots        []uint32 // FlashGroup's position in hasher ring, set by cli. value is range of crc32.
	SlotStatus   proto.SlotStatus
	PendingSlots []uint32
	Step         uint32
	Weight       uint32
	Status       proto.FlashGroupStatus
}

type FlashGroup struct {
	flashGroupValue
	lock       sync.RWMutex
	flashNodes map[string]*FlashNode // key: FlashNodeAddr
}

func (fg *FlashGroup) GetStatus() (st proto.FlashGroupStatus) {
	fg.lock.RLock()
	st = fg.Status
	fg.lock.RUnlock()
	return
}

func newFlashGroup(id uint64, slots []uint32, slotStatus proto.SlotStatus, pendingSlots []uint32, step uint32, status proto.FlashGroupStatus, weight uint32) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = id
	fg.Slots = slots
	fg.SlotStatus = slotStatus
	fg.PendingSlots = pendingSlots
	fg.Step = step
	fg.Weight = weight
	fg.Status = status
	fg.flashNodes = make(map[string]*FlashNode)
	return fg
}

func newFlashGroupFromFgv(fgv flashGroupValue) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = fgv.ID
	fg.Slots = fgv.Slots
	fg.SlotStatus = fgv.SlotStatus
	fg.PendingSlots = fgv.PendingSlots
	fg.Step = fgv.Step
	fg.Weight = fgv.Weight
	fg.Status = fgv.Status
	fg.flashNodes = make(map[string]*FlashNode)
	return fg
}

func (fg *FlashGroup) putFlashNode(fn *FlashNode) {
	fg.lock.Lock()
	fg.flashNodes[fn.Addr] = fn
	fg.lock.Unlock()
}

func (fg *FlashGroup) removeFlashNode(addr string) {
	fg.lock.Lock()
	delete(fg.flashNodes, addr)
	fg.lock.Unlock()
}

func (fg *FlashGroup) getTargetZoneFlashNodeHosts(targetZone string) (hosts []string) {
	fg.lock.RLock()
	for _, flashNode := range fg.flashNodes {
		if flashNode.ZoneName == targetZone {
			hosts = append(hosts, flashNode.Addr)
		}
	}
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodeHosts(checkStatus bool) (hosts []string) {
	hosts = make([]string, 0, len(fg.flashNodes))
	fg.lock.RLock()
	for host, flashNode := range fg.flashNodes {
		if checkStatus && !flashNode.isActiveAndEnable() {
			continue
		}
		hosts = append(hosts, host)
	}
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodesCount() (count int) {
	fg.lock.RLock()
	count = len(fg.flashNodes)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getSlots() (slots []uint32) {
	fg.lock.RLock()
	slots = make([]uint32, 0, len(fg.Slots))
	slots = append(slots, fg.Slots...)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getSlotsCount() (count int) {
	fg.lock.RLock()
	count = len(fg.Slots)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getPendingSlotsCount() (count int) {
	fg.lock.RLock()
	count = len(fg.PendingSlots)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getSlotStatus() (status proto.SlotStatus) {
	fg.lock.RLock()
	status = fg.SlotStatus
	fg.lock.RUnlock()
	return
}

func (c *Cluster) syncAddFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncAddFlashGroup, flashGroup)
}

func (c *Cluster) syncDeleteFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncDeleteFlashGroup, flashGroup)
}

func (c *Cluster) syncUpdateFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncUpdateFlashGroup, flashGroup)
}

func (c *Cluster) syncPutFlashGroupInfo(opType uint32, flashGroup *FlashGroup) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashGroupPrefix + strconv.FormatUint(flashGroup.ID, 10)
	metadata.V, err = json.Marshal(flashGroup.flashGroupValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (fg *FlashGroup) GetAdminView() (view proto.FlashGroupAdminView) {
	fg.lock.RLock()
	view = proto.FlashGroupAdminView{
		ID:           fg.ID,
		Slots:        fg.Slots,
		Weight:       fg.Weight,
		Status:       fg.Status,
		SlotStatus:   fg.SlotStatus,
		PendingSlots: fg.PendingSlots,
		Step:         fg.Step,
	}
	view.ZoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	view.FlashNodeCount = len(fg.flashNodes)
	for _, flashNode := range fg.flashNodes {
		view.ZoneFlashNodes[flashNode.ZoneName] = append(view.ZoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
	}
	fg.lock.RUnlock()
	return
}

func (m *Server) turnFlashGroup(w http.ResponseWriter, r *http.Request) {
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
	topo := m.cluster.flashNodeTopo
	enabled := enable.V
	if enabled {
		topo.clientOff.Store([]byte(nil))
	} else {
		topo.clientOff.Store(topo.clientEmpty)
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("turn %v", enabled)))
}

func (m *Server) createFlashGroup(w http.ResponseWriter, r *http.Request) {
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

func (c *Cluster) createFlashGroup(setSlots []uint32, setWeight uint32, gradualFlag bool, step uint32) (fg *FlashGroup, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addFlashGroup],clusterID[%v] err:%v ", c.Name, err.Error())
		}
	}()
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	if gradualFlag {
		if fg, err = c.flashNodeTopo.gradualCreateFlashGroup(id, c, setSlots, setWeight, step); err != nil {
			return
		}
	} else {
		if fg, err = c.flashNodeTopo.createFlashGroup(id, c, setSlots, setWeight); err != nil {
			return
		}
	}

	c.flashNodeTopo.updateClientCache()
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Weight:%v Slots:%v success", c.Name, fg.ID, fg.Weight, fg.getSlots())
	return
}

func (m *Server) removeFlashGroup(w http.ResponseWriter, r *http.Request) {
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

func (c *Cluster) removeFlashGroup(flashGroup *FlashGroup, gradualFlag bool, step uint32) (err error) {
	remainingSlotsNum := uint32(flashGroup.getSlotsCount()) - step
	if gradualFlag && remainingSlotsNum > 0 {
		err = c.flashNodeTopo.gradualRemoveFlashGroup(flashGroup, c, step)
		return
	}

	// remove flash nodes then del the flash group
	err = c.removeAllFlashNodeFromFlashGroup(flashGroup)
	if err != nil {
		return
	}
	err = c.flashNodeTopo.removeFlashGroup(flashGroup, c)
	return
}

func (m *Server) setFlashGroup(w http.ResponseWriter, r *http.Request) {
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
		if err = m.cluster.syncUpdateFlashGroup(flashGroup); err != nil {
			flashGroup.Status = oldStatus
			flashGroup.lock.Unlock()
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		m.cluster.flashNodeTopo.updateClientCache()
	}
	flashGroup.lock.Unlock()

	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) getFlashGroup(w http.ResponseWriter, r *http.Request) {
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

func (m *Server) flashGroupAddFlashNode(w http.ResponseWriter, r *http.Request) {
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

func (c *Cluster) addFlashNodeToFlashGroup(addr string, flashGroup *FlashGroup) (err error) {
	var flashNode *FlashNode
	if flashNode, err = c.setFlashNodeToFlashGroup(addr, flashGroup.ID); err != nil {
		return
	}
	flashGroup.putFlashNode(flashNode)
	return
}

func (c *Cluster) setFlashNodeToFlashGroup(addr string, flashGroupID uint64) (flashNode *FlashNode, err error) {
	if flashNode, err = c.peekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != unusedFlashNodeFlashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] can not add to flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}
	if time.Since(flashNode.ReportTime) > _defaultNodeTimeoutDuration {
		flashNode.IsActive = false
		err = fmt.Errorf("flashNode[%v] is inactive lastReportTime:%v", flashNode.Addr, flashNode.ReportTime)
		return
	}
	oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = flashGroupID
	if err = c.syncUpdateFlashNode(flashNode); err != nil {
		flashNode.FlashGroupID = oldFgID
		return
	}
	log.LogInfo(fmt.Sprintf("action[setFlashNodeToFlashGroup] add flash node:%v to flashGroup:%v success", addr, flashGroupID))
	return
}

func (c *Cluster) selectFlashNodesFromZoneAddToFlashGroup(zoneName string, count int, excludeHosts []string, flashGroup *FlashGroup) (err error) {
	flashNodeZone, err := c.flashNodeTopo.getZone(zoneName)
	if err != nil {
		return
	}
	newHosts, err := flashNodeZone.selectFlashNodes(count, excludeHosts)
	if err != nil {
		return
	}
	successHost := make([]string, 0)
	for _, newHost := range newHosts {
		if err = c.addFlashNodeToFlashGroup(newHost, flashGroup); err != nil {
			err = fmt.Errorf("successHost:%v, newHosts:%v err:%v", successHost, newHosts, err)
			return
		}
		successHost = append(successHost, newHost)
	}
	log.LogInfo(fmt.Sprintf("action[selectFlashNodesFromZoneAddToFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	return
}

func (m *Server) flashGroupRemoveFlashNode(w http.ResponseWriter, r *http.Request) {
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

func (c *Cluster) removeFlashNodeFromFlashGroup(addr string, flashGroup *FlashGroup) (err error) {
	var flashNode *FlashNode
	if flashNode, err = c.setFlashNodeToUnused(addr, flashGroup.ID); err != nil {
		return
	}
	flashGroup.removeFlashNode(flashNode.Addr)
	log.LogInfo(fmt.Sprintf("action[removeFlashNodeFromFlashGroup] node:%v flashGroup:%v, success", flashNode.Addr, flashGroup.ID))
	return
}

func (c *Cluster) removeAllFlashNodeFromFlashGroup(flashGroup *FlashGroup) (err error) {
	flashNodeHosts := flashGroup.getFlashNodeHosts(false)
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = c.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup); err != nil {
			log.LogErrorf("remove flashNode from flashGroup failed, successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
	}
	log.LogInfof("action[RemoveAllFlashNodeFromFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost)
	return
}

func (c *Cluster) removeFlashNodesFromTargetZone(zoneName string, count int, flashGroup *FlashGroup) (err error) {
	flashNodeHosts := flashGroup.getTargetZoneFlashNodeHosts(zoneName)
	if len(flashNodeHosts) < count {
		return fmt.Errorf("flashNodeHostsCount:%v less than expectCount:%v,flashNodeHosts:%v", len(flashNodeHosts), count, flashNodeHosts)
	}
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = c.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup); err != nil {
			err = fmt.Errorf("successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
		if len(successHost) >= count {
			break
		}
	}
	log.LogInfo(fmt.Sprintf("action[removeFlashNodesFromTargetZone] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	return
}

func (c *Cluster) setFlashNodeToUnused(addr string, flashGroupID uint64) (flashNode *FlashNode, err error) {
	if flashNode, err = c.peekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != flashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] not equal to target flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}

	oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = unusedFlashNodeFlashGroupID
	if err = c.syncUpdateFlashNode(flashNode); err != nil {
		flashNode.FlashGroupID = oldFgID
		return
	}

	go func() {
		time.Sleep(time.Duration(defaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr = fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()

	return
}

func (m *Server) listFlashGroups(w http.ResponseWriter, r *http.Request) {
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

func (m *Server) clientFlashGroups(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientFlashGroups))
	defer func() {
		doStatAndMetric(proto.ClientFlashGroups, metric, err, nil)
	}()
	cache := m.cluster.flashNodeTopo.getClientResponse()
	if len(cache) == 0 {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("flash group response cache is empty")))
		return
	}
	send(w, r, cache)
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

func argConvertFlashGroupStatus(active bool) proto.FlashGroupStatus {
	if active {
		return proto.FlashGroupStatus_Active
	}
	return proto.FlashGroupStatus_Inactive
}
