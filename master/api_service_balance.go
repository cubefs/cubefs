// Copyright 2025 The CubeFS Authors.
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
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type MetaReplicaInfo struct {
	MaxInodeID  uint64 `json:"MaxInodeID"`
	InodeCount  uint64 `json:"InodeCount"`
	DentryCount uint64 `json:"DentryCount"`
	FreeListLen uint64 `json:"FreeListLen"`
	TxCnt       uint64 `json:"TxCnt"`
	TxRbInoCnt  uint64 `json:"TxRbInoCnt"`
	TxRbDenCnt  uint64 `json:"TxRbDenCnt"`
}

type MigrateResult struct {
	Mp     MetaReplicaInfo `json:"mp"`
	Target MetaReplicaInfo `json:"target"`
}

type MetaPartitionPlanUserParams struct {
	Name               string          `json:"name"`
	StartID            uint64          `json:"startId"`
	EndID              uint64          `json:"endId"`
	Mode               proto.StoreMode `json:"mode"`
	Count              int             `json:"count"`
	AutoPromoteLearner bool            `json:"autoPromoteLearner"`
	SelectType         int             `json:"selectType"` // 0: not set. 1: zone name. 2: node set id. 3: node address list.
	ZoneName           string          `json:"zoneName"`
	NodeSetID          uint64          `json:"nodesetId"`
	Tag                string          `json:"tag"`
	MetaNodeAddr       string          `json:"metaNodeAddr"`
	RocksdbDir         string          `json:"rocksdbDir"`
}

func (m *Server) getMetaPartitionEmptyStatus(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionEmptyStatus))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionEmptyStatus, metric, nil, nil)
	}()

	mpsStatus := make([]proto.VolEmptyMpStats, 0, len(m.cluster.vols))
	for _, name := range m.cluster.allVolNames() {
		vol, err := m.cluster.getVol(name)
		if err != nil {
			log.LogErrorf("[getMetaPartitionEmptyStatus] getVol(%s) failed: %s", name, err.Error())
			continue
		}
		// skip the deleted volume.
		if vol.isUnavailable() {
			continue
		}
		volStatus := proto.VolEmptyMpStats{
			Name: name,
		}
		volStatus.MetaPartitions = make([]*proto.MetaPartitionView, 0, len(vol.MetaPartitions))
		volStatus.Total = len(vol.MetaPartitions)
		mps := vol.getSortMetaPartitions()
		for _, mp := range mps {
			if mp.IsMetaPartitionFreezed() || mp.IsEmptyToBeClean() {
				volStatus.EmptyCount++
				volStatus.MetaPartitions = append(volStatus.MetaPartitions, getMetaPartitionView(mp))
			}
		}
		if volStatus.EmptyCount > RsvEmptyMetaPartitionCnt {
			mpsStatus = append(mpsStatus, volStatus)
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(mpsStatus))
}

func (m *Server) freezeEmptyMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionFreezeEmpty))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionFreezeEmpty, metric, nil, nil)
	}()

	var (
		name  string
		count int
		err   error
	)
	name, count, err = parseFreeEmptyMetaPartitionParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.isUnavailable() {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted or init failed already.", name)))
		return
	}

	mps := vol.getSortMetaPartitions()
	total := 0
	for _, mp := range mps {
		if mp.IsEmptyToBeClean() {
			total++
		}
	}
	cleans := total - count
	if cleans <= 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "reserve mp number is larger than or equal empty number"})
		return
	}

	freezeList := m.SetMetaPartitionFrozen(mps, cleans)
	err = m.cluster.FreezeEmptyMetaPartitionJob(name, freezeList)

	rstMsg := fmt.Sprintf("Freeze empty volume(%s) meta partitions(%d)", name, cleans)
	AuditLog(r, "freezeEmptyMetaPartition", rstMsg, err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("Master will freeze empty meta partition of volume (%s) after 10 minutes. Task id: %s", name, name)))
}

func parseFreeEmptyMetaPartitionParam(r *http.Request) (name string, count int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if count, err = extractUint(r, countKey); err != nil {
		return
	}
	if count < RsvEmptyMetaPartitionCnt {
		// reserve 2 empty mp at least, not include the last one.
		count = RsvEmptyMetaPartitionCnt
	}

	return
}

func (m *Server) SetMetaPartitionFrozen(mps []*MetaPartition, cleans int) []*MetaPartition {
	freezeList := make([]*MetaPartition, 0, cleans)
	i := 0
	for _, mp := range mps {
		if !mp.IsEmptyToBeClean() {
			continue
		}

		mp.Freeze = proto.FreezingMetaPartition
		if mp.Status == proto.ReadWrite {
			mp.Status = proto.ReadOnly
		}
		// store the meta partition status.
		err := m.cluster.syncUpdateMetaPartition(mp)
		if err != nil {
			log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", mp.volName, mp.PartitionID, err.Error())
			continue
		}
		freezeList = append(freezeList, mp)

		i++
		if i >= cleans {
			break
		}
	}

	return freezeList
}

func (m *Server) cleanEmptyMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionCleanEmpty))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionCleanEmpty, metric, nil, nil)
	}()

	var (
		name string
		err  error
	)

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.isUnavailable() {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted or init failed already.", name)))
		return
	}

	err = m.cluster.StartCleanEmptyMetaPartition(name)

	rstMsg := fmt.Sprintf("Clean volume(%s) empty meta partitions", name)
	AuditLog(r, "cleanEmptyMetaPartition", rstMsg, err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("Clean frozen meta partition for volume (%s) in the background. It may takes several hours. task id: %s", name, name)))
}

func (m *Server) removeBackupMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionRemoveBackup))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionRemoveBackup, metric, nil, nil)
	}()

	m.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		task := proto.NewAdminTask(proto.OpRemoveBackupMetaPartition, metanode.Addr, nil)
		_, err := metanode.Sender.syncSendAdminTask(task)
		if err != nil {
			log.LogErrorf("failed to remove empty meta partition")
		}
		return true
	})

	AuditLog(r, "removeBackupMetaPartition", "clean all backup meta partitions", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Remove all backup meta partitions successfully."))
}

func (m *Server) getCleanMetaPartitionTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionGetCleanTask))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionGetCleanTask, metric, nil, nil)
	}()

	var (
		name string
		err  error
	)

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	name = r.FormValue(nameKey)

	m.cluster.mu.Lock()
	defer m.cluster.mu.Unlock()

	taskList := make([]*CleanTask, 0, len(m.cluster.cleanTask))
	if name == "" {
		for key, val := range m.cluster.cleanTask {
			task, err := m.cluster.CalculateMetaPartitionFreezeCount(key)
			if err != nil {
				log.LogWarnf("CalculateMetaPartitionFreezeCount volume(%s) err: %s", key, err.Error())
				continue
			}
			task.Status = val.Status
			taskList = append(taskList, task)
		}
	} else {
		val, ok := m.cluster.cleanTask[name]
		if !ok {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("Can't find task for volume(%s)", name)})
			return
		}
		task, err := m.cluster.CalculateMetaPartitionFreezeCount(name)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
		task.Status = val.Status
		taskList = append(taskList, task)
	}

	sendOkReply(w, r, newSuccessHTTPReply(taskList))
}

func parseMigratePartitionParam(r *http.Request) (srcAddr, targetAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	srcAddr = r.FormValue(srcAddrKey)
	if srcAddr == "" {
		err = fmt.Errorf("parseMigratePartitionParam source address is empty")
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(srcAddr); ok {
		srcAddr = ipAddr
	}

	targetAddr = r.FormValue(targetAddrKey)
	if targetAddr == "" {
		err = fmt.Errorf("parseMigratePartitionParam target address is empty")
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(targetAddr); ok {
		targetAddr = ipAddr
	}

	if srcAddr == targetAddr {
		err = fmt.Errorf("parseMigratePartitionParam srcAddr %s can't be equal to targetAddr %s", srcAddr, targetAddr)
		return
	}

	value := r.FormValue(idKey)
	if value == "" {
		err = fmt.Errorf("parseMigratePartitionParam meta partition id is needed")
		return
	}

	if id, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}

	return
}

func (m *Server) migrateMetaPartitionHandler(w http.ResponseWriter, r *http.Request) {
	var (
		srcAddr    string
		targetAddr string
		mpid       uint64
		err        error
		mp         *MetaPartition
		modeInt    int
		mode       proto.StoreMode
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.MigrateMetaPartition))
	defer func() {
		doStatAndMetric(proto.MigrateMetaPartition, metric, err, nil)
	}()

	srcAddr, targetAddr, mpid, err = parseMigratePartitionParam(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	modeInt, err = extractStoreMode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	targetNode, err := m.cluster.metaNode(targetAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMetaNodeNotExists, Msg: err.Error()})
		return
	}

	if !targetNode.IsWriteAble() || !targetNode.PartitionCntLimited() {
		err = fmt.Errorf("[%s] is not writable, can't be used as target addr for migrate", targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	mp, err = m.cluster.getMetaPartitionByID(mpid)
	if err != nil {
		err = fmt.Errorf("failed to get meta partition (%d)", mpid)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if modeInt == 0 {
		mode, err = m.cluster.getMetaPartitionStoreMode(mp, srcAddr)
		if err != nil {
			err = fmt.Errorf("getMetaPartitionStoreMode mp ID(%d) err: %s", mpid, err.Error())
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	} else {
		mode = proto.StoreMode(modeInt)
	}

	if err = m.cluster.migrateMetaPartition(srcAddr, targetAddr, mp, mode, proto.ManualDecommission); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("migrateMetaPartitionHandler id(%d) from src [%s] to target[%s] has migrate successfully", mpid, srcAddr, targetAddr)
	AuditLog(r, "MigrateMetaPartition", rstMsg, nil)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) createMetaNodeBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.CreateMetaNodeBalanceTask))
	defer func() {
		doStatAndMetric(proto.CreateMetaNodeBalanceTask, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	var plan *proto.ClusterPlan
	// search the raft storage. Only store one plan
	plan, err = m.cluster.loadBalanceTask()
	if err == nil && plan != nil {
		err = fmt.Errorf("there is a meta partition task plan already. Please remove it before create a new one")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	plan, err = m.cluster.GetMetaNodePressureView()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	if plan.Total <= 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "Not find meta node that needs partition rebalance.", Data: nil})
		return
	}
	plan.Type = ManualPlan

	// Save into raft storage.
	err = m.cluster.syncAddBalanceTask(plan)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	AuditLog(r, "createBalancePlan", "create meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) getMetaNodeBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetMetaNodeBalanceTask))
	defer func() {
		doStatAndMetric(proto.GetMetaNodeBalanceTask, metric, err, nil)
	}()

	var plan *proto.ClusterPlan
	plan, err = m.cluster.loadBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	if plan == nil || plan.Total <= 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "Meta partition migrate plan doesn't existed."})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) runMetaNodeBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RunMetaNodeBalanceTask))
	defer func() {
		doStatAndMetric(proto.RunMetaNodeBalanceTask, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	err = m.cluster.RunMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	AuditLog(r, "runBalancePlan", "start to run meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Start running balance task successfully."))
}

func (m *Server) stopMetaNodeBalancePlan(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		force bool
		value string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.StopMetaNodeBalanceTask))
	defer func() {
		doStatAndMetric(proto.StopMetaNodeBalanceTask, metric, err, nil)
	}()

	if value = r.FormValue(forceKey); value != "" {
		force, _ = strconv.ParseBool(value)
	}

	err = m.cluster.StopMetaPartitionBalanceTask(force)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	AuditLog(r, "stopBalancePlan", "stop meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Stop balance task successfully."))
}

func (m *Server) deleteMetaNodeBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DeleteMetaNodeBalanceTask))
	defer func() {
		doStatAndMetric(proto.DeleteMetaNodeBalanceTask, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	err = m.cluster.DeleteMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	AuditLog(r, "deleteBalancePlan", "Remove meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Delete balance plan task successfully."))
}

func (m *Server) offlineMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		err         error
		plan        *proto.ClusterPlan
		metaNode    *MetaNode
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OfflineMetaNode))
	defer func() {
		doStatAndMetric(proto.OfflineMetaNode, metric, err, nil)
		AuditLog(r, proto.OfflineMetaNode, rstMsg, err)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		log.LogErrorf("parse node addr failed, err: %v", err)
		sendErrReply(w, r, newErrHTTPReply(proto.ErrParamError))
		return
	}

	if metaNode, err = m.cluster.metaNode(offLineAddr); err != nil {
		log.LogWarnf("metanode(%s) is not exist", offLineAddr)
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	oldRdOnly := metaNode.RdOnly
	oldRocksdbRdOnly := metaNode.RocksdbRdOnly
	if !oldRdOnly || !oldRocksdbRdOnly {
		metaNode.RdOnly = true
		metaNode.RocksdbRdOnly = true
		if err = m.cluster.syncUpdateMetaNode(metaNode); err != nil {
			metaNode.RdOnly = oldRdOnly
			metaNode.RocksdbRdOnly = oldRocksdbRdOnly
			log.LogErrorf("syncUpdateMetaNode(%s) err: %s", offLineAddr, err.Error())
			sendErrReply(w, r, newErrHTTPReply(proto.ErrInternalError))
			return
		}
	}

	count := m.cluster.GetMpCountByMetaNode(metaNode.Addr)
	if count == 0 {
		err = m.cluster.DoMetaNodeOffline(offLineAddr)
		if err != nil {
			log.LogErrorf("DoMetaNodeOffline(%s) err: %s", offLineAddr, err.Error())
			sendErrReply(w, r, newErrHTTPReply(proto.ErrInternalError))
			return
		}
		rstMsg = fmt.Sprintf("Offline metanode %s successfully", offLineAddr)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
		return
	}

	// search the raft storage. Only store one plan
	plan, err = m.cluster.loadBalanceTask()
	if err == nil {
		if plan.Status == PlanTaskDone {
			// remove the done task.
			log.LogWarnf("remove the plan task(%v) before kick out(%s)", plan, offLineAddr)
			err = m.cluster.DeleteMetaPartitionBalanceTask()
			if err != nil {
				log.LogErrorf("failed to delete meta partition balance task: %s", err.Error())
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
				return
			}
		} else {
			log.LogWarnf("one balance task exist. clear it before kick out(%s)", offLineAddr)
			err = fmt.Errorf("there is a meta partition task plan. Clear it before kick out new metanode")
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
			return
		}
	} else if err != proto.ErrNoMpMigratePlan {
		log.LogErrorf("Failed to load balance task err: %s", err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if plan, err = m.cluster.CreateOfflineMetaNodePlan(offLineAddr); err != nil {
		log.LogErrorf(err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if plan.Total <= 0 {
		err = fmt.Errorf("kick out plan is empty for metanode(%s)", offLineAddr)
		log.LogErrorf(err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	plan.Type = OfflinePlan
	plan.Status = PlanTaskRun
	plan.StartTime = time.Now()

	// Save into raft storage.
	err = m.cluster.syncAddBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncAddBalanceTask metanode(%s) err: %s", offLineAddr, err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf("Offline metanode %s at background successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// parseMetaPartitionPlanUserParams parses and validates parameters for modifying meta partition store mode
func parseMetaPartitionPlanUserParams(r *http.Request) (param *MetaPartitionPlanUserParams, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	param = &MetaPartitionPlanUserParams{}

	param.Name = r.FormValue(nameKey)
	if param.Name != "" {
		if !volNameRegexp.MatchString(param.Name) {
			err = proto.ErrVolNameRegExpNotMatch
			return
		}
	}

	// Extract partition ID range
	startIDStr := r.FormValue(StartIdKey)
	if startIDStr != "" {
		if param.StartID, err = strconv.ParseUint(startIDStr, 10, 64); err != nil {
			err = fmt.Errorf("invalid start id")
			return
		}
	}

	endIDStr := r.FormValue(EndIdKey)
	if endIDStr != "" {
		if param.EndID, err = strconv.ParseUint(endIDStr, 10, 64); err != nil {
			err = fmt.Errorf("invalid end id")
			return
		}
	}

	if param.StartID > param.EndID && param.EndID != 0 {
		err = fmt.Errorf("start id cannot be greater than end id")
		return
	}

	// Extract store mode
	var modeInt int
	modeInt, err = extractStoreMode(r)
	if err != nil {
		return
	}
	if modeInt != 0 {
		param.Mode = proto.StoreMode(modeInt)
		if param.Mode != proto.StoreModeMem && param.Mode != proto.StoreModeRocksDb {
			err = fmt.Errorf("invalid store mode")
			return
		}
	} else {
		param.Mode = proto.StoreModeRocksDb // Default to migrate to RocksDB mode
	}

	param.Count = 0
	countStr := r.FormValue(countKey)
	if countStr != "" {
		if param.Count, err = strconv.Atoi(countStr); err != nil {
			err = fmt.Errorf("invalid count")
			return
		}
	}
	if param.Count <= 0 || param.Count > 3 {
		// default to 1
		param.Count = 1
	}

	var promote bool
	if value := r.FormValue(PromoteKey); value != "" {
		promote, err = strconv.ParseBool(value)
		if err != nil {
			err = fmt.Errorf("invalid promote")
			return
		}
		param.AutoPromoteLearner = promote
	}

	selectTypeStr := r.FormValue(SelectTypeKey)
	if selectTypeStr != "" {
		var selectType int
		selectType, err = strconv.Atoi(selectTypeStr)
		if err != nil {
			err = fmt.Errorf("invalid select type")
			return
		}
		param.SelectType = selectType
	}

	param.ZoneName = r.FormValue(zoneNameKey)
	if param.SelectType == SelectTypeZoneName && param.ZoneName == "" {
		err = fmt.Errorf("zone name is required when select type is 1")
		return
	}

	nodeSetIdStr := r.FormValue(nodesetIdKey)
	if nodeSetIdStr != "" {
		if param.NodeSetID, err = strconv.ParseUint(nodeSetIdStr, 10, 64); err != nil {
			err = fmt.Errorf("invalid node set id")
			return
		}
	}

	if param.SelectType == SelectTypeNodeSetId && param.NodeSetID == 0 {
		err = fmt.Errorf("node set id is required when select type is 2")
		return
	}

	param.Tag = r.FormValue(SelectTagKey)

	if param.SelectType == SelectTypeNodeAddrs && param.Tag == "" {
		err = fmt.Errorf("tag is required when select type is 3")
		return
	}

	param.MetaNodeAddr = r.FormValue(addrKey)
	param.RocksdbDir = r.FormValue(RocksdbDirKey)

	return
}

func (m *Server) batchMigrateMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminBatchMigrateMp))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminBatchMigrateMp, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	// search the raft storage. Only store one plan
	_, err = m.cluster.loadBalanceTask()
	if err == nil {
		err = m.cluster.DeleteMetaPartitionBalanceTask()
		if err != nil {
			log.LogErrorf("failed to delete meta partition balance task: %s", err.Error())
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
	} else if err != proto.ErrNoMpMigratePlan {
		log.LogErrorf("failed to load meta partition balance task: %s", err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	param, err := parseMetaPartitionPlanUserParams(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	plan, err := m.cluster.CreateMetaPartitionAddLearnerPlan(param)
	if err != nil {
		log.LogErrorf("addMetaPartitionLearner failed param:[%+v] err: %s", param, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	msg := fmt.Sprintf("volume(%s) start(%d) end(%d) mode(%d) count(%d) promote(%v) selectType(%d) zone(%s) nodesetId(%d) selectTag(%s)",
		param.Name, param.StartID, param.EndID, param.Mode, param.Count,
		param.AutoPromoteLearner, param.SelectType, param.ZoneName, param.NodeSetID, param.Tag)
	AuditLog(r, "batchMigrateMetaPartition", msg, nil)

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

// promoteMetaPartitionLearner promotes all rocksdb + learner metapartitions to voters within [startID, endID].
// Query/Form parameters:
// - name: volume name (optional; empty means all volumes)
// - startId: start mp id (optional; default 0)
// - endId: end mp id (optional; default 0)
func (m *Server) batchPromoteMpLearner(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminBatchPromoteMpLearner))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminBatchPromoteMpLearner, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	param, err := parseMetaPartitionPlanUserParams(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// do promote
	promotePlan, err := m.cluster.CreatePromoteLearnerPlan(param)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	msg := fmt.Sprintf("create promote learner plan: vol(%s) start(%d) end(%d) total(%d)", param.Name, param.StartID, param.EndID, promotePlan.TotalNum)
	AuditLog(r, "CreatePromoteLearnerPlan", msg, nil)
	sendOkReply(w, r, newSuccessHTTPReply(promotePlan))
}

func (m *Server) getPromoteMpLearnerPlan(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetPromoteMpLearnerPlan))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminGetPromoteMpLearnerPlan, metric, err, nil)
	}()

	plan, err := m.cluster.loadPromoteLearnerPlan()
	if err != nil && err != proto.ErrNoPromoteLearnerPlan {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	if plan == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "no promote learner plan"})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) stopPromoteMpLearnerPlan(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		force bool
		value string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminStopPromoteMpLearnerPlan))
	defer func() {
		doStatAndMetric(proto.AdminStopPromoteMpLearnerPlan, metric, err, nil)
	}()

	if value = r.FormValue(forceKey); value != "" {
		force, _ = strconv.ParseBool(value)
	}

	err = m.cluster.StopMetaPartitionBalanceTask(force)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	AuditLog(r, "stopPromoteMpLearnerPlan", "stop promote learner plan", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Stop promote learner plan successfully."))
}

func (m *Server) calcMetaPartitionMd5Sum(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCalcMetaPartitionMd5Sum))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminCalcMetaPartitionMd5Sum, metric, err, nil)
	}()

	param, err := parseMetaPartitionPlanUserParams(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	plan, err := m.cluster.loadCheckSumPlan()
	if err != nil && err != proto.ErrNoCheckSumPlan {
		log.LogErrorf("loadCheckSumPlan failed: %s", err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	if plan != nil {
		if plan.Status == PlanTaskRun {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "There is a check sum plan running, please wait for it to finish"})
			return
		}
		err = m.cluster.syncDeleteCheckSumPlan()
		if err != nil {
			log.LogErrorf("syncDeleteCheckSumPlan failed: %s", err.Error())
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
	}

	plan, err = m.cluster.CreateAndRunCheckSumPlan(param)
	if err != nil {
		log.LogErrorf("CreateAndRunCheckSumPlan failed: %s", err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) getMd5SumResult(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetMd5SumResult))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminGetMd5SumResult, metric, err, nil)
	}()

	plan, err := m.cluster.loadCheckSumPlan()
	if err != nil {
		log.LogErrorf("loadCheckSumPlan failed: %s", err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	if plan == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "no check sum plan"})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) decommissionRocksdbDir(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDecommissionRocksdbDir))
	var (
		err   error
		param *MetaPartitionPlanUserParams
	)
	defer func() {
		doStatAndMetric(proto.AdminDecommissionRocksdbDir, metric, err, nil)
	}()

	if m.cluster.IsClusterPlanNotIdle() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: m.cluster.GetClusterPlanStatusMsg()})
		return
	}

	param, err = parseMetaPartitionPlanUserParams(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if param.MetaNodeAddr == "" || param.RocksdbDir == "" {
		err = fmt.Errorf("meta node addr or rocksdb dir is required")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	plan, err := m.cluster.CreateDecommissionRocksdbDirPlan(param)
	if err != nil {
		log.LogErrorf("CreateDecommissionRocksdbDirPlan failed: %s", err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) getTagSummary(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetTagSummary))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminGetTagSummary, metric, err, nil)
	}()

	summary, err := m.cluster.getTagSummary()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(summary))
}

func (m *Server) getVolTagSummary(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetVolTagSummary))
	var err error
	defer func() {
		doStatAndMetric(proto.AdminGetVolTagSummary, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	name := r.FormValue(nameKey)
	if name == "" {
		log.LogErrorf("getVolTagSummary: name is required")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "name is required"})
		return
	}

	summary, err := m.cluster.getVolTagSummary(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(summary))
}

func (m *Server) clearTagFailedKeys(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminClearTagFailedKeys))
	defer func() {
		doStatAndMetric(proto.AdminClearTagFailedKeys, metric, nil, nil)
	}()

	MpFailedKeys = make([]string, 0)
	sendOkReply(w, r, newSuccessHTTPReply("Clear select tag failed keys successfully."))
}
