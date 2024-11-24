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

package metanode

import (
	"encoding/json"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) batchSetInodeQuota(req *proto.BatchSetMetaserverQuotaReuqest,
	resp *proto.BatchSetMetaserverQuotaResponse,
) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	val, err := json.Marshal(req)
	if err != nil {
		log.LogErrorf("batchSetInodeQuota marshal req [%v] failed [%v]", req, err)
		return
	}

	r, err := mp.submit(opFSMSetInodeQuotaBatch, val)
	if err != nil {
		log.LogErrorf("batchSetInodeQuota submit req [%v] failed [%v]", req, err)
		return
	}
	resp.InodeRes = r.(*proto.BatchSetMetaserverQuotaResponse).InodeRes
	return
}

func (mp *metaPartition) batchDeleteInodeQuota(req *proto.BatchDeleteMetaserverQuotaReuqest,
	resp *proto.BatchDeleteMetaserverQuotaResponse,
) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	val, err := json.Marshal(req)
	if err != nil {
		log.LogErrorf("batchDeleteInodeQuota marshal req [%v] failed [%v]", req, err)
		return
	}

	r, err := mp.submit(opFSMDeleteInodeQuotaBatch, val)
	if err != nil {
		log.LogErrorf("batchDeleteInodeQuota submit req [%v] failed [%v]", req, err)
		return
	}
	resp.InodeRes = r.(*proto.BatchDeleteMetaserverQuotaResponse).InodeRes
	return
}

func (mp *metaPartition) setQuotaHbInfo(infos []*proto.QuotaHeartBeatInfo) {
	mp.mqMgr.setQuotaHbInfo(infos)
	return
}

func (mp *metaPartition) getQuotaReportInfos() (infos []*proto.QuotaReportInfo) {
	return mp.mqMgr.getQuotaReportInfos()
}

func (mp *metaPartition) statisticExtendByLoad(extend *Extend, ino *Inode) {
	mqMgr := mp.mqMgr
	ino.Inode = extend.GetInode()
	status := mp.getInodeSimpleInfo(ino)
	if status != proto.OpOk {
		log.LogErrorf("statisticExtendByLoad get inode[%v] fail [%v].", extend.GetInode(), status)
		return
	}
	if ino.NLink == 0 {
		return
	}
	quotaIds, isFind := mp.isExistQuota(extend.GetInode())
	if isFind {
		mqMgr.rwlock.Lock()
		defer mqMgr.rwlock.Unlock()
		for _, quotaId := range quotaIds {
			var baseInfo proto.QuotaUsedInfo
			value, isFind := mqMgr.statisticBase.Load(quotaId)
			if isFind {
				baseInfo = value.(proto.QuotaUsedInfo)
			}
			baseInfo.UsedBytes += int64(ino.Size)
			baseInfo.UsedFiles += 1
			mqMgr.statisticBase.Store(quotaId, baseInfo)
			if log.EnableDebug() {
				log.LogDebugf("[statisticExtendByLoad] quotaId [%v] baseInfo [%v]", quotaId, baseInfo)
			}
		}
	}
	return
}

func (mp *metaPartition) statisticExtendByStore(extend *Extend, ino *Inode) {
	mqMgr := mp.mqMgr

	status := mp.getInodeSimpleInfo(ino)
	if status != proto.OpOk {
		log.LogDebugf("statisticExtendByStore get inode[%v] fail [%v].", extend.GetInode(), status)
		return
	}
	if ino.NLink == 0 {
		return
	}
	value := extend.Quota
	if len(value) == 0 {
		return
	}
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	if err := json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
		log.LogErrorf("statisticExtendByStore inode[%v] Unmarshal quotaInfos fail [%v]", extend.GetInode(), err)
		return
	}
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()
	for quotaId := range quotaInfos.QuotaInfoMap {
		var baseInfo proto.QuotaUsedInfo
		value, isFind := mqMgr.statisticRebuildBase.Load(quotaId)
		if isFind {
			baseInfo = value.(proto.QuotaUsedInfo)
		}
		baseInfo.UsedBytes += int64(ino.Size)
		baseInfo.UsedFiles += 1
		mqMgr.statisticRebuildBase.Store(quotaId, baseInfo)
	}
	return
}

func (mp *metaPartition) updateUsedInfo(size int64, files int64, ino uint64) {
	quotaIds, isFind := mp.isExistQuota(ino)
	if isFind {
		if log.EnableDebug() {
			log.LogDebugf("updateUsedInfo ino[%v] quotaIds [%v] size [%v] files [%v]", ino, quotaIds, size, files)
		}
		for _, quotaId := range quotaIds {
			mp.mqMgr.updateUsedInfo(size, files, quotaId)
		}
	}
	return
}

func (mp *metaPartition) isExistQuota(ino uint64) (quotaIds []uint32, isFind bool) {
	extend := NewExtend(ino)
	treeItem := mp.extendTree.Get(extend)
	if treeItem == nil {
		isFind = false
		return
	}
	extend = treeItem.(*Extend)
	value := extend.Quota
	if len(value) == 0 {
		isFind = false
		return
	}
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	if err := json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
		log.LogErrorf("set quota inode[%v] Unmarshal quotaInfos fail [%v]", ino, err)
		isFind = false
		return
	}
	isFind = true
	quotaInfos.RLock()
	for quotaId := range quotaInfos.QuotaInfoMap {
		quotaIds = append(quotaIds, quotaId)
	}
	quotaInfos.RUnlock()
	return
}

func (mp *metaPartition) isOverQuota(ino uint64, size bool, files bool) (status uint8) {
	quotaIds, isFind := mp.isExistQuota(ino)
	if isFind {
		for _, quotaId := range quotaIds {
			status = mp.mqMgr.IsOverQuota(size, files, quotaId)
			if status != 0 {
				log.LogWarnf("isOverQuota ino[%v] quotaId [%v] size [%v] files[%v] status[%v]", ino, quotaId, size, files, status)
				return
			}
		}
	}
	return
}

func (mp *metaPartition) getInodeQuota(inode uint64, p *Packet) (err error) {
	extend := NewExtend(inode)
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	var value []byte
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		goto handleRsp
	}
	extend = treeItem.(*Extend)
	value = extend.Quota
	if len(value) > 0 {
		if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
			log.LogErrorf("getInodeQuota inode[%v] Unmarshal quotaInfos fail [%v]", inode, err)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
	}
handleRsp:
	response := &proto.GetInodeQuotaResponse{}
	if log.EnableDebug() {
		log.LogDebugf("getInodeQuota inode %v ,map %v", inode, quotaInfos.QuotaInfoMap)
	}
	response.MetaQuotaInfoMap = quotaInfos.QuotaInfoMap

	encoded, err := json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) getInodeQuotaInfos(inode uint64) (quotaInfos map[uint32]*proto.MetaQuotaInfo, err error) {
	treeItem := mp.extendTree.Get(NewExtend(inode))
	if treeItem == nil {
		return
	}
	extend := treeItem.(*Extend)
	info := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	value := extend.Quota
	if len(value) > 0 {
		if err = json.Unmarshal(value, &info.QuotaInfoMap); err != nil {
			log.LogErrorf("getInodeQuota mp[%v] inode[%v] Unmarshal quotaInfos fail [%v]", mp.config.PartitionId, inode, err)
			return
		}
		quotaInfos = info.QuotaInfoMap
	}
	if log.EnableDebug() {
		log.LogDebugf("getInodeQuotaInfos mp[%v] inode[%v] quotaInfos [%v] exist [%v]", mp.config.PartitionId, inode, quotaInfos, len(value))
	}
	return
}

func (mp *metaPartition) setInodeQuota(quotaIds []uint32, inode uint64) {
	extend := NewExtendWithQuota(inode)
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	for _, quotaId := range quotaIds {
		quotaInfo := &proto.MetaQuotaInfo{
			RootInode: false,
		}
		quotaInfos.QuotaInfoMap[quotaId] = quotaInfo
	}
	value, err := json.Marshal(quotaInfos.QuotaInfoMap)
	if err != nil {
		log.LogErrorf("setInodeQuota marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
		return
	}
	extend.Quota = value
	if mp.verSeq > 0 {
		extend.setVersion(mp.verSeq)
	}
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.Merge(extend, true)
	}
	if log.EnableDebug() {
		log.LogDebugf("setInodeQuota inode[%v] quota [%v] success.", inode, quotaIds)
	}
	return
}
