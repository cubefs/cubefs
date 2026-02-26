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
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) syncAddFlashGroup(flashGroup *flashgroupmanager.FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncAddFlashGroup, flashGroup)
}

func (c *Cluster) syncDeleteFlashGroup(flashGroup *flashgroupmanager.FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncDeleteFlashGroup, flashGroup)
}

func (c *Cluster) syncUpdateFlashGroup(flashGroup *flashgroupmanager.FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncUpdateFlashGroup, flashGroup)
}

func (c *Cluster) syncPutFlashGroupInfo(opType uint32, flashGroup *flashgroupmanager.FlashGroup) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashGroupPrefix + strconv.FormatUint(flashGroup.ID, 10)
	metadata.V, err = json.Marshal(flashGroup.FlashGroupValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (m *Server) turnFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashTopo *flashgroupmanager.FlashNodeTopology
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupTurn))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupTurn, metric, err, nil)
	}()
	var enable common.Bool
	if err = parseArgs(r, enable.Enable()); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	enabled := enable.V
	flashTopo.TurnFlashGroup(enabled)
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

	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// forbid operations on markDeleted topology
	if flashTopo, e := m.cluster.PeekFlashTopo(topoName); e == nil && flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	flashGroup, err := m.cluster.createFlashGroup(setSlots, setWeight, gradualFlag, step, topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (c *Cluster) createFlashGroup(setSlots []uint32, setWeight uint32, gradualFlag bool,
	step uint32, topoName string,
) (fg *flashgroupmanager.FlashGroup, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addFlashGroup],clusterID[%v] err:%v ", c.Name, err.Error())
		}
	}()
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	var flashTopo *flashgroupmanager.FlashNodeTopology
	flashTopo, err = c.PeekFlashTopo(topoName)
	if err != nil {
		return
	}

	fg, err = flashTopo.CreateFlashGroup(id, c.syncUpdateFlashGroup, c.syncAddFlashGroup, setSlots, setWeight, gradualFlag, step)
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Weight:%v Slots:%v success", c.Name, fg.ID, fg.Weight, fg.GetSlots())
	return
}

func (m *Server) removeFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		err                 error
		gradualFlag         bool
		step                uint32
		flashTopo, idleTopo *flashgroupmanager.FlashNodeTopology
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
	var topoName string
	if flashTopo, err = m.cluster.PeekFlashTopoByFgId(flashGroupID.V); err != nil {
		// Backward Compatibility
		topoName = r.FormValue(nameKey)
	} else {
		topoName = flashTopo.Name
	}
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	idleTopo, err = m.cluster.PeekFlashTopo(proto.IdleTopoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var flashGroup *flashgroupmanager.FlashGroup
	if flashGroup, err = flashTopo.RemoveFlashGroup(m.cluster.Name, idleTopo, flashGroupID.V, gradualFlag, step,
		m.cluster.syncUpdateFlashGroup, m.cluster.syncUpdateFlashNode, m.cluster.syncDeleteFlashGroup,
		m.cluster.syncDeleteFlashNode, m.cluster.syncAddFlashNode, m.cluster.syncMoveFlashNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("remove flashGroup:%v successfully,Slots:%v nodeCount:%v",
		flashGroup.ID, flashGroup.GetSlots(), flashGroup.GetFlashNodesCount())))
}

func (m *Server) setFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID common.Uint
		fgStatus     proto.FlashGroupStatus
		flashGroup   *flashgroupmanager.FlashGroup
		err          error
		flashTopo    *flashgroupmanager.FlashNodeTopology
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
	var topoName string
	if flashTopo, err = m.cluster.PeekFlashTopoByFgId(flashGroupID.V); err != nil {
		// Backward Compatibility
		topoName = r.FormValue(nameKey)
	} else {
		topoName = flashTopo.Name
	}
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	if flashGroup, err = flashTopo.GetFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	err = flashGroup.UpdateStatus(fgStatus, m.cluster.syncUpdateFlashGroup, flashTopo)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) getFlashGroup(w http.ResponseWriter, r *http.Request) {
	var (
		flashGroupID common.Uint
		flashGroup   *flashgroupmanager.FlashGroup
		err          error
		flashTopo    *flashgroupmanager.FlashNodeTopology
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupGet))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupGet, metric, err, nil)
	}()
	if err = parseArgs(r, flashGroupID.ID()); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var topoName string
	if flashTopo, err = m.cluster.PeekFlashTopoByFgId(flashGroupID.V); err != nil {
		// Backward Compatibility
		topoName = r.FormValue(nameKey)
	} else {
		topoName = flashTopo.Name
	}

	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashGroup, err = flashTopo.GetFlashGroup(flashGroupID.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) flashGroupAddFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		flashTopo  *flashgroupmanager.FlashNodeTopology
		idleTopo   *flashgroupmanager.FlashNodeTopology
		flashGroup *flashgroupmanager.FlashGroup
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupNodeAdd))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupNodeAdd, metric, err, nil)
	}()
	flashGroupID, addr, zoneName, count, err := parseArgsFlashGroupNode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var topoName string
	if flashTopo, err = m.cluster.PeekFlashTopoByFgId(flashGroupID); err != nil {
		// Backward Compatibility
		topoName = r.FormValue(nameKey)
	} else {
		topoName = flashTopo.Name
	}
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// check if target topo is exist
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	// check if target flash group is exist
	if flashGroup, err = flashTopo.GetFlashGroup(flashGroupID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	idleTopo, err = m.cluster.PeekFlashTopo(proto.IdleTopoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if addr != "" {
		_, err = idleTopo.PeekFlashNode(addr)
	}

	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = idleTopo.AddFlashNodeToFlashGroupWithTargetTopo(flashTopo, flashGroup,
		addr, zoneName, count, m.cluster.syncUpdateFlashNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) flashGroupRemoveFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		err                  error
		flashTopo, targetTop *flashgroupmanager.FlashNodeTopology
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashGroupNodeRemove))
	defer func() {
		doStatAndMetric(proto.AdminFlashGroupNodeRemove, metric, err, nil)
	}()
	flashGroupID, addr, zoneName, count, err := parseArgsFlashGroupNode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var topoName string
	if flashTopo, err = m.cluster.PeekFlashTopoByFgId(flashGroupID); err != nil {
		// Backward Compatibility
		topoName = r.FormValue(nameKey)
	} else {
		topoName = flashTopo.Name
	}
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	targetTop, err = m.cluster.PeekFlashTopo(proto.IdleTopoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	var flashGroup *flashgroupmanager.FlashGroup
	if flashGroup, err = m.cluster.RemoveFlashNodesFromFlashGroup(flashTopo, targetTop, flashGroupID,
		addr, zoneName, count); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashGroup.GetAdminView()))
}

func (m *Server) listFlashGroups(w http.ResponseWriter, r *http.Request) {
	var (
		fgStatus  proto.FlashGroupStatus
		allStatus bool
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
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
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// Whether to list across all topologies
	showAllTopo := false
	if v := r.FormValue("showAllTopo"); v != "" {
		if b, e := strconv.ParseBool(v); e == nil {
			showAllTopo = b
		}
	}

	if showAllTopo {
		// aggregate groups from all topologies
		var result proto.FlashGroupsAdminView
		m.cluster.flashNodeTopo.Range(func(_, value interface{}) bool {
			if value == nil {
				return true
			}
			topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
			if !ok {
				return true
			}
			view := topo.GetFlashGroupsAdminView(fgStatus, allStatus)
			if view != nil && len(view.FlashGroups) > 0 {
				result.FlashGroups = append(result.FlashGroups, view.FlashGroups...)
			}
			return true
		})
		sendOkReply(w, r, newSuccessHTTPReply(&result))
		return
	} else {
		flashTopo, err = m.cluster.PeekFlashTopo(topoName)
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		fgv := flashTopo.GetFlashGroupsAdminView(fgStatus, allStatus)
		sendOkReply(w, r, newSuccessHTTPReply(fgv))
		return
	}
}

func (m *Server) clientFlashGroups(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientFlashGroups))
	defer func() {
		doStatAndMetric(proto.ClientFlashGroups, metric, err, nil)
	}()

	if !m.metaReady {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("meta not ready")))
		return
	}
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	if topoName == proto.IdleTopoName {
		err = fmt.Errorf("idle topo doesn't support this option")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	cache := flashTopo.GetClientResponse()
	if len(cache) == 0 {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("flash group response cache is empty")))
		return
	}
	send(w, r, cache)
}

func (m *Server) listFlashTopo(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashTopoList))
	defer func() {
		doStatAndMetric(proto.AdminFlashTopoList, metric, nil, nil)
	}()

	ft := m.cluster.ListAllFlashTopos()
	sendOkReply(w, r, newSuccessHTTPReply(ft))
}

func (m *Server) addFlashTopo(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashTopoAdd))
	defer func() {
		doStatAndMetric(proto.AdminFlashTopoAdd, metric, nil, nil)
	}()
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	region := r.FormValue(regionKey)
	if region == "" {
		region = proto.DefaultRegion
	}
	_, err := m.cluster.PeekFlashTopo(topoName)
	if err == nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is already exist", topoName)))
		return
	}
	err = m.cluster.AddFlashTopo(topoName, region)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("add topo[%v] failed %v", topoName, err.Error())))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] is added", topoName)))
}

func (m *Server) deleteFlashTopo(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashTopoDel))
	defer func() {
		doStatAndMetric(proto.AdminFlashTopoDel, metric, nil, nil)
	}()
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	if topoName == proto.DefaultTopoName || topoName == proto.IdleTopoName {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is not allowed to deleted", topoName)))
		return
	}
	flashTopo, err := m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is not exist", topoName)))
		return
	}
	var (
		gradualFlag bool
		step        uint32
		forceDel    bool
	)
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
	// parse optional forceDel flag (default false)
	if v := r.FormValue("forceDel"); v != "" {
		if b, e := strconv.ParseBool(v); e == nil {
			forceDel = b
		}
	}
	if !forceDel && flashTopo.IsMarkDelete() {
		// already marked, return schedule hint
		var when string
		if !flashTopo.DeleteExecTime.IsZero() {
			when = flashTopo.DeleteExecTime.Format(proto.TimeFormat)
		}
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] is already markDeleted, scheduled at %v", topoName, when)))
		return
	}
	err = m.cluster.DelFlashTopo(topoName, gradualFlag, step, forceDel)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("del topo[%v] failed %v", topoName, err.Error())))
		return
	}
	if forceDel {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] is deleted", topoName)))
		return
	}
	// first time markDeleted: return hint with expected deletion time
	// re-peek to get updated DeleteExecTime persisted by MarkDelete
	if flashTopo, err = m.cluster.PeekFlashTopo(topoName); err == nil {
		var when string
		if !flashTopo.DeleteExecTime.IsZero() {
			when = flashTopo.DeleteExecTime.Format(proto.TimeFormat)
		}
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] is markDeleted, scheduled at %v", topoName, when)))
		return
	}
	// fallback
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] is markDeleted", topoName)))
}

func (m *Server) renameFlashTopo(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashTopoRename))
	defer func() {
		doStatAndMetric(proto.AdminFlashTopoRename, metric, nil, nil)
	}()
	srcName := r.FormValue(nameKey)
	if srcName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "old name should not be empty"})
		return
	}
	if srcName == proto.IdleTopoName {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "idle topo cannot be renamed"})
		return
	}
	dstName := r.FormValue(newNameKey)
	if dstName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "new name should not be empty"})
		return
	}
	srcTopo, err := m.cluster.PeekFlashTopo(srcName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is not exist", srcName)))
		return
	}
	// TODO: new topo is created, by fn and fg are not attached to the new topo before
	//  leader change
	_, err = m.cluster.PeekFlashTopo(dstName)
	if err == nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is already exist", dstName)))
		return
	}
	err = m.cluster.RenameFlashNodeTopo(srcTopo, dstName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("rename topo[%v] failed %v", srcName, err.Error())))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] rename to [%v] success", srcName, dstName)))
}

func (m *Server) cancelDeleteFlashTopo(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashTopoCancelDelete))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminFlashTopoCancelDelete, metric, err, nil)
	}()
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "name should not be empty"})
		return
	}
	flashTopo, err := m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is not exist", topoName)))
		return
	}
	// only allow cancel when in markDeleted
	if !flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is not markDeleted", topoName)))
		return
	}
	// reset status and delete params
	flashTopo.Status = proto.TopoStatusNormal
	flashTopo.DeleteExecTime = time.Time{}
	flashTopo.DeleteGradualFlag = false
	flashTopo.DeleteStep = 0
	// remove from delay queue if present
	m.cluster.deleteFlashTopoMutex.Lock()
	delete(m.cluster.delayDeleteFlashTopoInfo, topoName)
	m.cluster.deleteFlashTopoMutex.Unlock()
	// persist
	if err = m.cluster.syncUpdateFlashTopo(flashTopo); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("topo[%v] cancel markDeleted", topoName)))
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
