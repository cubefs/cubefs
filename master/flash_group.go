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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type flashGroupValue struct {
	ID     uint64
	Slots  []uint32 // FlashGroup's position in hasher ring, set by cli. value is range of crc32.
	Status proto.FlashGroupStatus
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

func newFlashGroup(id uint64, slots []uint32, status proto.FlashGroupStatus) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = id
	fg.Slots = slots
	fg.Status = status
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
		ID:     fg.ID,
		Slots:  fg.Slots,
		Status: fg.Status,
	}
	view.ZoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	view.FlashNodeCount = len(fg.flashNodes)
	for _, flashNode := range fg.flashNodes {
		view.ZoneFlashNodes[flashNode.ZoneName] = append(view.ZoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
	}
	fg.lock.RUnlock()
	return
}

func (m *Server) createFlashGroup(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupCreate))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupCreate, metric, err, nil)
	}()
	setSlots := getSetSlots(r)
	flashGroup, err := m.cluster.createFlashGroup(setSlots)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (c *Cluster) createFlashGroup(setSlots []uint32) (fg *FlashGroup, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addFlashGroup],clusterID[%v] err:%v ", c.Name, err.Error())
		}
	}()
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	if fg, err = c.flashNodeTopo.createFlashGroup(id, c, setSlots); err != nil {
		return
	}
	c.flashNodeTopo.updateClientCache()
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Slots:%v success", c.Name, fg.ID, fg.Slots)
	return
}

func (m *Server) removeFlashGroup(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupRemove))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupRemove, metric, err, nil)
	}()
	var flashGroupID uint64
	if flashGroupID, err = extractFlashGroupID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var flashGroup *FlashGroup
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.removeFlashGroup(flashGroup); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	m.cluster.flashNodeTopo.updateClientCache()
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("remove flashGroup:%v successfully,Slots:%v nodeCount:%v",
		flashGroup.ID, flashGroup.Slots, flashGroup.getFlashNodesCount())))
}

func (c *Cluster) removeFlashGroup(flashGroup *FlashGroup) (err error) {
	// remove flash nodes then del the flash group
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

func (m *Server) setFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID uint64
		fgStatus     proto.FlashGroupStatus
		flashGroup   *FlashGroup
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupSet))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupSet, metric, err, nil)
	}()
	if flashGroupID, fgStatus, err = parseRequestForSetFlashGroup(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = m.cluster.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
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
		flashGroupID uint64
		flashGroup   *FlashGroup
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupGet))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupGet, metric, err, nil)
	}()
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
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupNodeAdd))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupNodeAdd, metric, err, nil)
	}()

	flashGroupID, addr, zoneName, count, err := parseRequestForManageFlashNodeOfFlashGroup(r)
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
	flashGroupID, addr, zoneName, count, err := parseRequestForManageFlashNodeOfFlashGroup(r)
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
	if fgStatus, err = extractFlashGroupStatus(r); err != nil {
		if value := r.FormValue(enableKey); value == "" {
			allStatus = true // resp all flash groups
		} else {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
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
	cache, err := m.cluster.getFlashGroupClientResponseCache()
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	send(w, r, cache)
}

func (c *Cluster) getFlashGroupClientResponseCache() (cache []byte, err error) {
	if cache := c.flashNodeTopo.clientRespCache.Load().([]byte); len(cache) == 0 {
		c.updateFlashGroupClientCache()
	}
	if cache = c.flashNodeTopo.clientRespCache.Load().([]byte); len(cache) == 0 {
		return nil, fmt.Errorf("flash group resp cache is empty")
	}
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
	if status {
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

func extractZoneName(r *http.Request) (name string, err error) {
	if name = r.FormValue(zoneNameKey); name == "" {
		err = keyNotFound(zoneNameKey)
		return
	}
	return
}

func extractCount(r *http.Request) (count int, err error) {
	var value string
	if value = r.FormValue(countKey); value == "" {
		return
	}
	if count, err = strconv.Atoi(value); err != nil {
		err = unmatchedKey(countKey)
		return
	}
	return
}
