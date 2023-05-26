// Copyright 2018 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type flashGroupValue struct {
	ID     uint64
	Slots  []uint32
	Status proto.FlashGroupStatus
}
type FlashGroup struct {
	flashGroupValue
	flashNodes     map[string]*FlashNode // key: FlashNodeAddr
	flashNodesLock sync.RWMutex
}

func newFlashGroup(id uint64, slots []uint32, status proto.FlashGroupStatus) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = id
	fg.Slots = slots
	fg.Status = status
	fg.flashNodes = make(map[string]*FlashNode, 0)
	return fg
}

func (fg *FlashGroup) putFlashNode(fn *FlashNode) {
	fg.flashNodesLock.Lock()
	fg.flashNodes[fn.Addr] = fn
	fg.flashNodesLock.Unlock()
	return
}

func (fg *FlashGroup) removeFlashNode(addr string) (view proto.FlashGroupAdminView) {
	fg.flashNodesLock.Lock()
	delete(fg.flashNodes, addr)
	fg.flashNodesLock.Unlock()
	return
}

func (fg *FlashGroup) getTargetZoneFlashNodeHosts(targetZone string) (hosts []string) {
	fg.flashNodesLock.RLock()
	defer fg.flashNodesLock.RUnlock()
	hosts = make([]string, 0)
	for _, flashNode := range fg.flashNodes {
		if flashNode.ZoneName == targetZone {
			hosts = append(hosts, flashNode.Addr)
		}
	}
	return
}
func (fg *FlashGroup) getFlashNodeHosts(checkStatus bool) (hosts []string) {
	fg.flashNodesLock.RLock()
	defer fg.flashNodesLock.RUnlock()
	hosts = make([]string, 0, len(fg.flashNodes))
	for host, flashNode := range fg.flashNodes {
		if checkStatus && !flashNode.isActiveAndEnable() {
			continue
		}
		hosts = append(hosts, host)
	}
	return
}
func (fg *FlashGroup) getFlashNodesCount() (count int) {
	fg.flashNodesLock.RLock()
	defer fg.flashNodesLock.RUnlock()
	count = len(fg.flashNodes)
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
	view = proto.FlashGroupAdminView{
		ID:     fg.ID,
		Slots:  fg.Slots,
		Status: fg.Status,
	}
	view.ZoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	fg.flashNodesLock.RLock()
	defer fg.flashNodesLock.RUnlock()
	view.FlashNodeCount = len(fg.flashNodes)
	for _, flashNode := range fg.flashNodes {
		view.ZoneFlashNodes[flashNode.ZoneName] = append(view.ZoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
	}
	return
}

func (m *Server) createFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	metrics := exporter.NewModuleTP(proto.CreateFlashGroupUmpKey)
	defer func() { metrics.Set(err) }()
	setSlots := getSetSlots(r)
	flashGroup, err := m.cluster.createFlashGroup(setSlots)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}
func (c *Cluster) createFlashGroup(setSlots []uint32) (fg *FlashGroup, err error) {
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		goto errHandler
	}
	if fg, err = c.flashNodeTopo.createFlashGroup(id, c, setSlots); err != nil {
		goto errHandler
	}
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Slots:%v success", c.Name, fg.ID, fg.Slots)
	return
errHandler:
	log.LogErrorf("action[addFlashGroup],clusterID[%v] err:%v ", c.Name, err.Error())
	return
}
func (t *flashNodeTopology) createFlashGroup(fgID uint64, c *Cluster, setSlots []uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots)
	flashGroup = newFlashGroup(fgID, slots, proto.FlashGroupStatus_Inactive)
	if err = c.syncAddFlashGroup(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}
	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	return
}

func (m *Server) removeFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID      uint64
		flashGroup        *FlashGroup
		needUpdateFGCache bool
		err               error
	)
	metrics := exporter.NewModuleTP(proto.RemoveFlashGroupUmpKey)
	defer func() { metrics.Set(err) }()
	if flashGroupID, err = extractFlashGroupID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup.Status == proto.FlashGroupStatus_Active && flashGroup.getFlashNodesCount() != 0 {
		needUpdateFGCache = true
	}
	if err = m.cluster.removeFlashGroup(flashGroup); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if needUpdateFGCache {
		m.cluster.updateFlashGroupResponseCache()
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("remove flashGroup:%v successfully,Slots:%v nodeCount:%v", flashGroup.ID, flashGroup.Slots, len(flashGroup.flashNodes))))
}
func (c *Cluster) removeFlashGroup(flashGroup *FlashGroup) (err error) {
	//remove flash nodes then del the flash group
	flashNodeHosts := flashGroup.getFlashNodeHosts(false)
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = c.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup); err != nil {
			err = fmt.Errorf("successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
	}
	log.LogInfo(fmt.Sprintf("action[removeFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	err = c.flashNodeTopo.removeFlashGroup(flashGroup, c)
	if err != nil {
		return
	}
	return
}
func (t *flashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, c *Cluster) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := flashGroup.Slots
	oldStatus := flashGroup.Status

	flashGroup.Status = proto.FlashGroupStatus_Inactive
	if err = c.syncDeleteFlashGroup(flashGroup); err != nil {
		flashGroup.Status = oldStatus
		return
	}
	t.removeSlots(slots)
	t.flashGroupMap.Delete(flashGroup.ID)
	return
}

func (m *Server) setFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID      uint64
		fgStatus          proto.FlashGroupStatus
		flashGroup        *FlashGroup
		err               error
		needUpdateFGCache bool
	)
	metrics := exporter.NewModuleTP(proto.SetFlashGroupUmpKey)
	defer func() { metrics.Set(err) }()
	if flashGroupID, fgStatus, err = parseRequestForSetFlashGroup(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	oldStatus := flashGroup.Status
	flashGroup.Status = fgStatus
	if oldStatus == proto.FlashGroupStatus_Active && fgStatus == proto.FlashGroupStatus_Inactive {
		needUpdateFGCache = true
	}
	if err = m.cluster.syncUpdateFlashGroup(flashGroup); err != nil {
		flashGroup.Status = oldStatus
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if needUpdateFGCache {
		m.cluster.updateFlashGroupResponseCache()
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) getFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID uint64
		flashGroup   *FlashGroup
		err          error
	)
	metrics := exporter.NewModuleTP(proto.GetFlashGroupUmpKey)
	defer func() { metrics.Set(err) }()
	if flashGroupID, err = extractFlashGroupID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) flashGroupAddFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroup   *FlashGroup
		flashGroupID uint64
		addr         string
		zoneName     string
		count        int
		err          error
	)
	metrics := exporter.NewModuleTP(proto.FlashGroupAddFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
	flashGroupID, addr, zoneName, count, err = parseRequestForManageFlashNodeOfFlashGroup(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
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
	if flashNode, err = c.flashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if !flashNode.isFlashNodeUnused() {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] can not add to flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}
	if time.Since(flashNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
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
	flashNodeZone.flashNodesLock.Lock()
	defer flashNodeZone.flashNodesLock.Unlock()
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
	var (
		flashGroup   *FlashGroup
		flashGroupID uint64
		addr         string
		zoneName     string
		count        int
		err          error
	)
	metrics := exporter.NewModuleTP(proto.FlashGroupRemoveFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
	flashGroupID, addr, zoneName, count, err = parseRequestForManageFlashNodeOfFlashGroup(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
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
	if flashNode, err = c.flashNode(addr); err != nil {
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
	return
}

func (m *Server) listFlashGroups(w http.ResponseWriter, r *http.Request) {
	var (
		fgStatus  proto.FlashGroupStatus
		allStatus bool
		err       error
	)
	metrics := exporter.NewModuleTP(proto.ListFlashGroupsUmpKey)
	defer func() { metrics.Set(err) }()

	if fgStatus, err = extractFlashGroupStatus(r); err != nil {
		if value := r.FormValue(enableKey); value == "" {
			allStatus = true //resp all flash groups
		} else {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	fgv := m.cluster.flashNodeTopo.getFlashGroupsAdminView(fgStatus, allStatus)
	sendOkReply(w, r, newSuccessHTTPReply(fgv))
}

func (m *Server) clientFlashGroups(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupRespCache []byte
		err                 error
	)
	metrics := exporter.NewModuleTP(proto.ClientFlashGroupsUmpKey)
	defer func() { metrics.Set(err) }()
	flashGroupRespCache, err = m.cluster.getFlashGroupResponseCache()
	if len(flashGroupRespCache) != 0 {
		send(w, r, flashGroupRespCache, proto.JsonType)
	} else {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
}
func (c *Cluster) getFlashGroupResponseCache() (flashGroupRespCache []byte, err error) {
	if len(c.flashGroupRespCache) == 0 {
		c.updateFlashGroupResponseCache()
	}
	flashGroupRespCache = c.flashGroupRespCache
	if len(flashGroupRespCache) == 0 {
		return nil, fmt.Errorf("flash group resp cache is empty")
	}
	return
}
func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.updateFlashGroupResponseCache()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
		}
	}()
}
func (c *Cluster) updateFlashGroupResponseCache() {
	fgv := c.flashNodeTopo.getFlashGroupView()
	reply := newSuccessHTTPReply(fgv)
	flashGroupRespCache, err := json.Marshal(reply)
	if err != nil {
		msg := fmt.Sprintf("action[updateFlashGroupResponseCache] json marshal err:%v", err)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	c.flashGroupRespCache = flashGroupRespCache
	return
}
func (c *Cluster) clearFlashGroupResponseCache() {
	c.flashGroupRespCache = nil
}
func (t *flashNodeTopology) getFlashGroupView() (fgv *proto.FlashGroupView) {
	fgv = new(proto.FlashGroupView)
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if fg.Status.IsActive() {
			hosts := fg.getFlashNodeHosts(true)
			if len(hosts) == 0 {
				return true
			}
			fgv.FlashGroups = append(fgv.FlashGroups, &proto.FlashGroupInfo{
				ID:    fg.ID,
				Slot:  fg.Slots,
				Hosts: hosts,
			})
			//for _, slot := range fg.Slots {
			//	fgv.FlashGroups = append(fgv.FlashGroups, proto.FlashGroupInfo{
			//		ID:    fg.ID,
			//		Slot:  slot,
			//		Hosts: hosts,
			//	})
			//}
		}
		return true
	})
	//sort.Slice(fgv.FlashGroups, func(i, j int) bool {
	//	return fgv.FlashGroups[i].Slot < fgv.FlashGroups[j].Slot
	//})
	return
}

func (t *flashNodeTopology) getFlashGroupsAdminView(fgStatus proto.FlashGroupStatus, allStatus bool) (fgv *proto.FlashGroupsAdminView) {
	fgv = new(proto.FlashGroupsAdminView)
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if allStatus || fg.Status == fgStatus {
			fgv.FlashGroups = append(fgv.FlashGroups, fg.GetAdminView())
		}
		return true
	})
	return
}

func parseRequestForManageFlashNodeOfFlashGroup(r *http.Request) (flashGroupID uint64, addr, zoneName string, count int, err error) {
	if flashGroupID, err = extractFlashGroupID(r); err != nil {
		return
	}
	if addr = r.FormValue(addrKey); addr != "" {
		return
	}

	if zoneName, err = extractZoneName(r); err != nil {
		return
	}
	if count, err = extractCount(r); err != nil {
		return
	}
	if count <= 0 {
		err = unmatchedKey(countKey)
	}
	return
}

func parseRequestForSetFlashGroup(r *http.Request) (flashGroupID uint64, fgStatus proto.FlashGroupStatus, err error) {
	if flashGroupID, err = extractFlashGroupID(r); err != nil {
		return
	}
	fgStatus, err = extractFlashGroupStatus(r)
	return
}

func extractFlashGroupStatus(r *http.Request) (fgStatus proto.FlashGroupStatus, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var status bool
	if status, err = extractStatus(r); err != nil {
		return
	}
	if status == true {
		fgStatus = proto.FlashGroupStatus_Active
	} else {
		fgStatus = proto.FlashGroupStatus_Inactive
	}
	return
}

func extractFlashGroupID(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func getSetSlots(r *http.Request) (slots []uint32) {
	slots = make([]uint32, 0)
	slotStr := r.FormValue(fgSlotsKey)
	if slotStr != "" {
		arr := strings.Split(slotStr, ",")
		for i := 0; i < len(arr); i++ {
			slot, err := strconv.ParseUint(arr[i], 10, 32)
			if err != nil {
				continue
			}
			if len(slots) >= defaultFlashGroupSlotsCount {
				continue
			}

			slots = append(slots, uint32(slot))
		}
	}
	return
}
