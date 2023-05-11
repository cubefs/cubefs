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
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	//"github.com/cubefs/cubefs/blobstore/util/errors"
)

type MasterQuotaManager struct {
	MpQuotaInfoMap       map[uint64][]*proto.QuotaReportInfo
	IdQuotaInfoMap       map[uint32]*proto.QuotaInfo
	FullPathQuotaInfoMap map[string]*proto.QuotaInfo
	vol                  *Vol
	c                    *Cluster
	sync.RWMutex
}

func (mqMgr *MasterQuotaManager) setQuota(req *proto.SetMasterQuotaReuqest) (err error) {
	var quotaId uint32
	mqMgr.Lock()
	defer mqMgr.Unlock()

	if len(mqMgr.IdQuotaInfoMap) >= MaxQuotaNumPerVol {
		err = errors.NewErrorf("the number of quota has reached the upper limit %v", len(mqMgr.IdQuotaInfoMap))
		return
	}
	for _, quotaInfo := range mqMgr.FullPathQuotaInfoMap {
		if req.Inode == quotaInfo.RootInode {
			err = errors.NewErrorf("inode [%v] is the same as quotaId [%v] inode [%v]",
				req.Inode, quotaInfo.QuotaId, quotaInfo.RootInode)
			return
		}
	}

	if quotaId, err = mqMgr.c.idAlloc.allocateQuotaID(); err != nil {
		return err
	}

	var quotaInfo = &proto.QuotaInfo{
		VolName:     req.VolName,
		QuotaId:     quotaId,
		Status:      proto.QuotaInit,
		CTime:       time.Now().Unix(),
		PartitionId: req.PartitionId,
		RootInode:   req.Inode,
		FullPath:    req.FullPath,
		MaxFiles:    req.MaxFiles,
		MaxBytes:    req.MaxBytes,
	}

	var value []byte
	if value, err = json.Marshal(quotaInfo); err != nil {
		log.LogErrorf("set quota [%v] marsha1 fail [%v].", quotaInfo, err)
		return
	}

	metadata := new(RaftCmd)
	metadata.Op = opSyncSetQuota
	metadata.K = quotaPrefix + strconv.FormatUint(mqMgr.vol.ID, 10) + keySeparator + strconv.FormatUint(uint64(quotaId), 10)
	metadata.V = value

	if err = mqMgr.c.submit(metadata); err != nil {
		log.LogErrorf("set quota [%v] submit fail [%v].", quotaInfo, err)
		return
	}

	var inodes = make([]uint64, 0)
	inodes = append(inodes, req.Inode)
	request := &proto.BatchSetMetaserverQuotaReuqest{
		PartitionId: req.PartitionId,
		Inodes:      inodes,
		QuotaId:     quotaId,
	}

	if err = mqMgr.setQuotaToMetaNode(request); err != nil {
		log.LogErrorf("set quota [%v] to metanode fail [%v].", quotaInfo, err)
		return
	}

	mqMgr.IdQuotaInfoMap[quotaId] = quotaInfo
	mqMgr.FullPathQuotaInfoMap[req.FullPath] = quotaInfo

	log.LogInfof("set quota [%v] success.", quotaInfo)
	return
}

func (mqMgr *MasterQuotaManager) updateQuota(req *proto.UpdateMasterQuotaReuqest) (err error) {
	var quotaId uint32
	mqMgr.Lock()
	defer mqMgr.Unlock()
	quotaInfo, isFind := mqMgr.FullPathQuotaInfoMap[req.FullPath]
	if !isFind {
		log.LogErrorf("vol [%v] quota fullpath [%v] is not exist.", mqMgr.vol.Name, req.FullPath)
		err = errors.New("quota is not exist.")
		return
	}

	if quotaInfo.RootInode != req.Inode {
		log.LogErrorf("vol [%v] update quota inode [%v] is not match.", mqMgr.vol.Name, req.Inode)
		err = errors.New("quota inode is not match.")
		return
	}

	quotaInfo.MaxFiles = req.MaxFiles
	quotaInfo.MaxBytes = req.MaxBytes

	var value []byte
	if value, err = json.Marshal(quotaInfo); err != nil {
		log.LogErrorf("update quota [%v] marsha1 fail [%v].", quotaInfo, err)
		return
	}

	metadata := new(RaftCmd)
	metadata.Op = opSyncSetQuota
	metadata.K = quotaPrefix + strconv.FormatUint(mqMgr.vol.ID, 10) + keySeparator + strconv.FormatUint(uint64(quotaId), 10)
	metadata.V = value

	if err = mqMgr.c.submit(metadata); err != nil {
		log.LogErrorf("update quota [%v] submit fail [%v].", quotaInfo, err)
		return
	}

	log.LogInfof("update quota [%v] success.", *quotaInfo)
	return
}

func (mqMgr *MasterQuotaManager) listQuota() (resp *proto.ListMasterQuotaResponse) {
	mqMgr.RLock()
	defer mqMgr.RUnlock()
	resp = &proto.ListMasterQuotaResponse{}
	resp.Quotas = make([]*proto.QuotaInfo, 0)
	for _, info := range mqMgr.IdQuotaInfoMap {
		resp.Quotas = append(resp.Quotas, info)
	}
	return
}

func (mqMgr *MasterQuotaManager) getQuota(fullPath string) (quotaInfo *proto.QuotaInfo, err error) {
	mqMgr.RLock()
	defer mqMgr.RUnlock()
	quotaInfo, isFind := mqMgr.FullPathQuotaInfoMap[fullPath]
	if !isFind {
		log.LogErrorf("vol [%v] quota fullPath [%v] is not exist.", mqMgr.vol.Name, fullPath)
		err = errors.New("quota is not exist.")
		return nil, err
	}

	return quotaInfo, nil
}

func (mqMgr *MasterQuotaManager) deleteQuota(quotaId uint32) (err error) {
	mqMgr.Lock()
	defer mqMgr.Unlock()

	quotaInfo, isFind := mqMgr.IdQuotaInfoMap[quotaId]
	if !isFind {
		log.LogErrorf("vol [%v] quota quotaId [%v] is not exist.", mqMgr.vol.Name, quotaId)
		err = errors.New("quota is not exist.")
		return
	}

	quotaInfo.Status = proto.QuotaDeleting
	var value []byte
	if value, err = json.Marshal(quotaInfo); err != nil {
		log.LogErrorf("delete quota [%v] marsha1 fail [%v].", quotaInfo, err)
		return
	}
	metadata := new(RaftCmd)
	metadata.Op = opSyncSetQuota
	metadata.K = quotaPrefix + strconv.FormatUint(mqMgr.vol.ID, 10) + keySeparator + strconv.FormatUint(uint64(quotaId), 10)
	metadata.V = value

	if err = mqMgr.c.submit(metadata); err != nil {
		log.LogErrorf("delete quota [%v] submit fail [%v].", quotaInfo, err)
		return
	}

	var inodes = make([]uint64, 0)
	inodes = append(inodes, quotaInfo.RootInode)
	request := &proto.BatchDeleteMetaserverQuotaReuqest{
		PartitionId: quotaInfo.PartitionId,
		Inodes:      inodes,
		QuotaId:     quotaId,
	}

	if err = mqMgr.DeleteQuotaToMetaNode(request); err != nil {
		log.LogErrorf("delete quota [%v] to metanode fail [%v].", quotaInfo, err)
		return
	}

	delete(mqMgr.IdQuotaInfoMap, quotaId)
	delete(mqMgr.FullPathQuotaInfoMap, quotaInfo.FullPath)
	log.LogInfof("deleteQuota: idmap len [%v] fullpathMap len [%v]",
		len(mqMgr.IdQuotaInfoMap), len(mqMgr.FullPathQuotaInfoMap))
	return
}

func (mqMgr *MasterQuotaManager) setQuotaToMetaNode(req *proto.BatchSetMetaserverQuotaReuqest) (err error) {
	var (
		mp   *MetaPartition
		mr   *MetaReplica
		node *MetaNode
	)

	if mp, err = mqMgr.c.getMetaPartitionByID(req.PartitionId); err != nil {
		log.LogErrorf("get metaPartition fail mpid [%v].", req.PartitionId)
		return
	}

	if mr, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogErrorf("get MetaReplica leader fail mpid [%v].", req.PartitionId)
		return
	}
	log.LogInfof("setQuotaToMetaNode success. addr [%v].", mr.Addr)
	if node, err = mqMgr.c.metaNode(mr.Addr); err != nil {
		log.LogErrorf("get mp leader metanode fail mpid [%v].", req.PartitionId)
		return
	}
	tasks := make([]*proto.AdminTask, 0)
	task := node.setQuotaTask(req)
	tasks = append(tasks, task)
	mqMgr.c.addMetaNodeTasks(tasks)
	log.LogInfof("setQuotaToMetaNode success. req [%v] node[%v].", req, node)
	return
}

func (mqMgr *MasterQuotaManager) DeleteQuotaToMetaNode(req *proto.BatchDeleteMetaserverQuotaReuqest) (err error) {
	var (
		mp   *MetaPartition
		mr   *MetaReplica
		node *MetaNode
	)

	if mp, err = mqMgr.c.getMetaPartitionByID(req.PartitionId); err != nil {
		log.LogErrorf("get metaPartition fail mpid [%v].", req.PartitionId)
		return
	}

	if mr, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogErrorf("get MetaReplica leader fail mpid [%v].", req.PartitionId)
		return
	}

	if node, err = mqMgr.c.metaNode(mr.Addr); err != nil {
		log.LogErrorf("get mp leader metanode fail mpid [%v].", req.PartitionId)
		return
	}

	tasks := make([]*proto.AdminTask, 0)
	task := node.deleteQuotaTask(req)
	tasks = append(tasks, task)
	mqMgr.c.addMetaNodeTasks(tasks)
	log.LogInfof("DeleteQuotaToMetaNode add task. req [%v] node[%v].", req, node)
	return
}

func (mqMgr *MasterQuotaManager) getQuotaInfoById(quotaId uint32) (quotaInfo *proto.QuotaInfo, err error) {
	var isFind bool
	mqMgr.RLock()
	defer mqMgr.RUnlock()
	if quotaInfo, isFind = mqMgr.IdQuotaInfoMap[quotaId]; isFind {
		err = nil
		return
	} else {
		err = errors.New("quota is exist.")
	}
	return
}

func (mqMgr *MasterQuotaManager) DeleteQuotaInfoById(quotaId uint32) {
	mqMgr.Lock()
	defer mqMgr.Unlock()

	quotaInfo, isFind := mqMgr.IdQuotaInfoMap[quotaId]
	if isFind {
		delete(mqMgr.FullPathQuotaInfoMap, quotaInfo.FullPath)
		delete(mqMgr.IdQuotaInfoMap, quotaId)
		log.LogInfof("DeleteQuotaInfoById delete quotaId [%v] success.", quotaId)
	}
	return
}

func (mqMgr *MasterQuotaManager) quotaUpdate(report *proto.MetaPartitionReport) {
	var (
		quotaInfo = &proto.QuotaInfo{}
		id        uint32
	)

	mqMgr.Lock()
	defer mqMgr.Unlock()

	mpId := report.PartitionID

	log.LogDebugf("[quotaUpdate] mpId [%v] QuotaReportInfos [%v] leader [%v]", mpId, report.QuotaReportInfos, report.IsLeader)
	if !report.IsLeader {
		return
	}

	mqMgr.MpQuotaInfoMap[mpId] = report.QuotaReportInfos

	for _, quotaInfo = range mqMgr.IdQuotaInfoMap {
		quotaInfo.UsedInfo.UsedFiles = 0
		quotaInfo.UsedInfo.UsedBytes = 0
	}

	for mpId, reportInfos := range mqMgr.MpQuotaInfoMap {
		log.LogDebugf("[quotaUpdate] mpId [%v], info len [%v]", mpId, len(reportInfos))
		for _, info := range reportInfos {
			if _, isFind := mqMgr.IdQuotaInfoMap[info.QuotaId]; !isFind {
				log.LogWarnf("[quotaUpdate] quotaId [%v] is delete", info.QuotaId)
				continue
			}
			log.LogDebugf("[quotaUpdate] quotaId [%v] reportinfo [%v]", info.QuotaId, info.UsedInfo)
			quotaInfo = mqMgr.IdQuotaInfoMap[info.QuotaId]
			quotaInfo.UsedInfo.Add(&info.UsedInfo)
		}
	}
	for id, quotaInfo = range mqMgr.IdQuotaInfoMap {
		if quotaInfo.IsOverQuotaFiles() {
			quotaInfo.LimitedInfo.LimitedFiles = true
		} else {
			quotaInfo.LimitedInfo.LimitedFiles = false
		}
		if quotaInfo.IsOverQuotaBytes() {
			quotaInfo.LimitedInfo.LimitedBytes = true
		} else {
			quotaInfo.LimitedInfo.LimitedBytes = false
		}
		log.LogDebugf("[quotaUpdate] quotaId [%v] quotaInfo [%v]", id, quotaInfo)
	}
	return
}

func (mqMgr *MasterQuotaManager) getQuotaHbInfos() (infos []*proto.QuotaHeartBeatInfo) {
	mqMgr.RLock()
	defer mqMgr.RUnlock()
	for quotaId, quotaInfo := range mqMgr.IdQuotaInfoMap {
		var info = &proto.QuotaHeartBeatInfo{}
		info.VolName = mqMgr.vol.Name
		info.QuotaId = quotaId
		info.LimitedInfo.LimitedFiles = quotaInfo.LimitedInfo.LimitedFiles
		info.LimitedInfo.LimitedBytes = quotaInfo.LimitedInfo.LimitedBytes
		infos = append(infos, info)
		log.LogDebugf("getQuotaHbInfos info %v", info)
	}

	return
}

func (mqMgr *MasterQuotaManager) batchModifyQuotaFullPath(changeFullPathMap map[uint32]string) {
	mqMgr.Lock()
	defer mqMgr.Unlock()

	for quotaId, newPath := range changeFullPathMap {
		quotaInfo, isFind := mqMgr.IdQuotaInfoMap[quotaId]
		if isFind {
			quotaInfo.FullPath = newPath
			var value []byte
			var err error
			if value, err = json.Marshal(quotaInfo); err != nil {
				log.LogErrorf("update quota [%v] marsha1 fail [%v].", quotaInfo, err)
				continue
			}

			metadata := new(RaftCmd)
			metadata.Op = opSyncSetQuota
			metadata.K = quotaPrefix + strconv.FormatUint(mqMgr.vol.ID, 10) + keySeparator + strconv.FormatUint(uint64(quotaId), 10)
			metadata.V = value

			if err = mqMgr.c.submit(metadata); err != nil {
				log.LogErrorf("update quota [%v] submit fail [%v].", quotaInfo, err)
				continue
			}

			delete(mqMgr.FullPathQuotaInfoMap, quotaInfo.FullPath)
			mqMgr.FullPathQuotaInfoMap[newPath] = quotaInfo
		}
	}
	log.LogInfof("batchModifyQuotaFullPath idMap [%v] pathmap [%v]", mqMgr.IdQuotaInfoMap, mqMgr.FullPathQuotaInfoMap)
}

func resetQuotaTaskID(t *proto.AdminTask, quotaId uint32) {
	t.ID = fmt.Sprintf("%v_quota[%v]", t.ID, quotaId)
}
