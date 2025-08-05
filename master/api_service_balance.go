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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
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
		if vol.Status == proto.VolStatusMarkDelete {
			continue
		}
		volStatus := proto.VolEmptyMpStats{
			Name: name,
		}
		volStatus.MetaPartitions = make([]*proto.MetaPartitionView, 0, len(vol.MetaPartitions))
		volStatus.Total = len(vol.MetaPartitions)
		mps := vol.getSortMetaPartitions()
		for _, mp := range mps {
			if mp.IsFreeze || mp.IsEmptyToBeClean() {
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

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if count, err = extractUint(r, countKey); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if count < RsvEmptyMetaPartitionCnt {
		// reserve 2 empty mp at least, not include the last one.
		count = RsvEmptyMetaPartitionCnt
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.Status == proto.VolStatusMarkDelete {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted already.", name)))
		return
	}

	mps := vol.getSortMetaPartitions()
	if len(mps) <= RsvEmptyMetaPartitionCnt {
		err = fmt.Errorf("the all meta partition number is less than %d", RsvEmptyMetaPartitionCnt)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

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

	freezeList := make([]*MetaPartition, 0, cleans)
	i := 0
	for j := len(mps) - 1; j >= 0; j -= 1 {
		mp := mps[j]
		if !mp.IsEmptyToBeClean() {
			continue
		}

		mp.IsFreeze = true
		if mp.Status == proto.ReadWrite {
			mp.Status = proto.ReadOnly
		}
		// store the meta partition status.
		err = m.cluster.syncUpdateMetaPartition(mp)
		if err != nil {
			log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", name, mp.PartitionID, err.Error())
			continue
		}
		freezeList = append(freezeList, mp)

		i++
		if i >= cleans {
			break
		}
	}
	err = m.cluster.FreezeEmptyMetaPartitionJob(name, freezeList)

	rstMsg := fmt.Sprintf("Freeze empty volume(%s) meta partitions(%d)", name, cleans)
	auditlog.LogMasterOp("freezeEmptyMetaPartition", rstMsg, err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("Master will freeze empty meta partition of volume (%s) after 10 minutes. Task id: %s", name, name)))
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

	if vol.Status == proto.VolStatusMarkDelete {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted already.", name)))
		return
	}

	err = m.cluster.StartCleanEmptyMetaPartition(name)

	rstMsg := fmt.Sprintf("Clean volume(%s) empty meta partitions", name)
	auditlog.LogMasterOp("cleanEmptyMetaPartition", rstMsg, err)
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

	auditlog.LogMasterOp("removeBackupMetaPartition", "clean all backup meta partitions", nil)

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

	if name == "" {
		sendOkReply(w, r, newSuccessHTTPReply(m.cluster.cleanTask))
	} else {
		task, ok := m.cluster.cleanTask[name]
		if !ok {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("Can't find task for volume(%s)", name)})
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(task))
	}
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

	targetNode, err := m.cluster.metaNode(targetAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMetaNodeNotExists, Msg: err.Error()})
		return
	}

	if !targetNode.IsWriteAble() || !targetNode.PartitionCntLimited() {
		err = fmt.Errorf("[%s] is not writable, can't used as target addr for migrate", targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	mp, err = m.cluster.getMetaPartitionByID(mpid)
	if err != nil {
		err = fmt.Errorf("Failed to get meta partition (%d)", mpid)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.migrateMetaPartition(srcAddr, targetAddr, mp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("migrateMetaPartitionHandler id(%d) from src [%s] to target[%s] has migrate successfully", mpid, srcAddr, targetAddr)
	auditlog.LogMasterOp("MigrateMetaPartition", rstMsg, nil)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func parseMigrateResultParam(r *http.Request) (targetAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	targetAddr = r.FormValue(targetAddrKey)
	if targetAddr == "" {
		err = fmt.Errorf("parseMigrateResultParam targetAddrKey is null")
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(targetAddr); ok {
		targetAddr = ipAddr
	}

	value := r.FormValue(idKey)
	if value == "" {
		err = fmt.Errorf("parseMigrateResultParam meta partition id is needed")
		return
	}

	if id, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}

	return
}

func (m *Server) migrateResultHandler(w http.ResponseWriter, r *http.Request) {
	var (
		targetAddr string
		mpid       uint64
		err        error
		mp         *MetaPartition
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.MigrateResult))
	defer func() {
		doStatAndMetric(proto.MigrateResult, metric, err, nil)
	}()

	targetAddr, mpid, err = parseMigrateResultParam(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	mp, err = m.cluster.getMetaPartitionByID(mpid)
	if err != nil {
		err = fmt.Errorf("Failed to get meta partition (%d)", mpid)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	ret := MigrateResult{
		Mp: MetaReplicaInfo{
			MaxInodeID:  mp.MaxInodeID,
			InodeCount:  mp.InodeCount,
			DentryCount: mp.DentryCount,
			FreeListLen: mp.FreeListLen,
			TxCnt:       mp.TxCnt,
			TxRbInoCnt:  mp.TxRbInoCnt,
			TxRbDenCnt:  mp.TxRbDenCnt,
		},
	}

	metaNode, err := m.cluster.metaNode(targetAddr)
	if err != nil {
		err = fmt.Errorf("Failed to get meta partition (%d)", mpid)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	for _, mpv := range metaNode.metaPartitionInfos {
		if mpv.PartitionID != mpid {
			continue
		}
		ret.Target.MaxInodeID = mpv.MaxInodeID
		ret.Target.InodeCount = mpv.InodeCnt
		ret.Target.DentryCount = mpv.DentryCnt
		ret.Target.FreeListLen = mpv.FreeListLen
		ret.Target.TxCnt = mpv.TxCnt
		ret.Target.TxRbInoCnt = mpv.TxRbInoCnt
		ret.Target.TxRbDenCnt = mpv.TxRbDenCnt
	}

	sendOkReply(w, r, newSuccessHTTPReply(ret))
}

func (m *Server) createBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.CreateBalanceTask))
	defer func() {
		doStatAndMetric(proto.CreateBalanceTask, metric, err, nil)
	}()

	var plan *proto.ClusterPlan
	// search the raft storage. Only store one plan
	plan, err = m.cluster.loadBalanceTask()
	if err == nil && plan != nil {
		err = fmt.Errorf("There is a meta partition task plan already. Please remove it before create a new one.")
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

	// Save into raft storage.
	err = m.cluster.syncAddBalanceTask(plan)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	auditlog.LogMasterOp("createBalancePlan", "create meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) getBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetBalanceTask))
	defer func() {
		doStatAndMetric(proto.GetBalanceTask, metric, err, nil)
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

func (m *Server) runBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RunBalanceTask))
	defer func() {
		doStatAndMetric(proto.RunBalanceTask, metric, err, nil)
	}()

	err = m.cluster.RunMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	auditlog.LogMasterOp("runBalancePlan", "start to run meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Start running balance task successfully."))
}

func (m *Server) stopBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.StopBalanceTask))
	defer func() {
		doStatAndMetric(proto.StopBalanceTask, metric, err, nil)
	}()

	err = m.cluster.StopMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	auditlog.LogMasterOp("stopBalancePlan", "stop meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Stop balance task successfully."))
}

func (m *Server) deleteBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DeleteBalanceTask))
	defer func() {
		doStatAndMetric(proto.DeleteBalanceTask, metric, err, nil)
	}()

	err = m.cluster.DeleteMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	auditlog.LogMasterOp("deleteBalancePlan", "Remove meta partition balance task", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Delete balance plan task successfully."))
}
