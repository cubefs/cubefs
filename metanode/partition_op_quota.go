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
	"context"
	"encoding/json"

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) batchSetInodeQuota(ctx context.Context,
	req *proto.BatchSetMetaserverQuotaReuqest, resp *proto.BatchSetMetaserverQuotaResponse,
) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	span := getSpan(ctx)
	val, err := json.Marshal(req)
	if err != nil {
		span.Errorf("batchSetInodeQuota marshal req [%v] failed [%v]", req, err)
		return
	}

	r, err := mp.submit(ctx, opFSMSetInodeQuotaBatch, val)
	if err != nil {
		span.Errorf("batchSetInodeQuota submit req [%v] failed [%v]", req, err)
		return
	}
	resp.InodeRes = r.(*proto.BatchSetMetaserverQuotaResponse).InodeRes
	span.Infof("batchSetInodeQuota quotaId [%v] mp[%v] btreeLen [%v] resp [%v] success",
		req.QuotaId, mp.config.PartitionId, mp.extendTree.Len(), resp)
	return
}

func (mp *metaPartition) batchDeleteInodeQuota(ctx context.Context,
	req *proto.BatchDeleteMetaserverQuotaReuqest, resp *proto.BatchDeleteMetaserverQuotaResponse,
) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	span := getSpan(ctx)
	val, err := json.Marshal(req)
	if err != nil {
		span.Errorf("batchDeleteInodeQuota marshal req [%v] failed [%v]", req, err)
		return
	}

	r, err := mp.submit(ctx, opFSMDeleteInodeQuotaBatch, val)
	if err != nil {
		span.Errorf("batchDeleteInodeQuota submit req [%v] failed [%v]", req, err)
		return
	}
	resp.InodeRes = r.(*proto.BatchDeleteMetaserverQuotaResponse).InodeRes
	span.Infof("batchSetInodeQuota quotaId [%v] mp[%v] btreeLen [%v] resp [%v] success",
		req.QuotaId, mp.config.PartitionId, mp.extendTree.Len(), resp)
	return
}

func (mp *metaPartition) setQuotaHbInfo(ctx context.Context, infos []*proto.QuotaHeartBeatInfo) {
	mp.mqMgr.setQuotaHbInfo(ctx, infos)
}

func (mp *metaPartition) getQuotaReportInfos(ctx context.Context) (infos []*proto.QuotaReportInfo) {
	return mp.mqMgr.getQuotaReportInfos(ctx)
}

func (mp *metaPartition) statisticExtendByLoad(ctx context.Context, extend *Extend) {
	mqMgr := mp.mqMgr
	ino := NewInode(extend.GetInode(), 0)
	retMsg := mp.getInode(ctx, ino, true)
	span := getSpan(ctx)
	if retMsg.Status != proto.OpOk {
		span.Errorf("statisticExtendByLoad get inode[%v] fail [%v].", extend.GetInode(), retMsg.Status)
		return
	}
	ino = retMsg.Msg
	if ino.NLink == 0 {
		return
	}
	quotaIds, isFind := mp.isExistQuota(ctx, extend.GetInode())
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
			span.Debugf("[statisticExtendByLoad] quotaId [%v] baseInfo [%v]", quotaId, baseInfo)

		}
	}
	span.Infof("statisticExtendByLoad ino[%v] isFind [%v].", ino.Inode, isFind)
}

func (mp *metaPartition) statisticExtendByStore(ctx context.Context, extend *Extend, inodeTree *BTree) {
	mqMgr := mp.mqMgr
	ino := NewInode(extend.GetInode(), 0)
	retMsg := mp.getInode(ctx, ino, true)
	span := getSpan(ctx)
	if retMsg.Status != proto.OpOk {
		span.Errorf("statisticExtendByStore get inode[%v] fail [%v].", extend.GetInode(), retMsg.Status)
		return
	}
	ino = retMsg.Msg
	if ino.NLink == 0 {
		return
	}
	value, exist := extend.Get([]byte(proto.QuotaKey))
	if !exist {
		span.Debugf("statisticExtendByStore get quota key failed, mp[%v] inode[%v]", mp.config.PartitionId, extend.GetInode())
		return
	}
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	if err := json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
		span.Errorf("statisticExtendByStore inode[%v] Unmarshal quotaInfos fail [%v]", extend.GetInode(), err)
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
		span.Debugf("[statisticExtendByStore] mp[%v] quotaId [%v] inode[%v] baseInfo [%v]",
			mp.config.PartitionId, quotaId, extend.GetInode(), baseInfo)
	}
	span.Debugf("statisticExtendByStore mp[%v] inode[%v] success.", mp.config.PartitionId, extend.GetInode())
}

func (mp *metaPartition) updateUsedInfo(ctx context.Context, size int64, files int64, ino uint64) {
	quotaIds, isFind := mp.isExistQuota(ctx, ino)
	if isFind {
		getSpan(ctx).Infof("updateUsedInfo ino[%v] quotaIds [%v] size [%v] files [%v]", ino, quotaIds, size, files)
		for _, quotaId := range quotaIds {
			mp.mqMgr.updateUsedInfo(ctx, size, files, quotaId)
		}
	}
}

func (mp *metaPartition) isExistQuota(ctx context.Context, ino uint64) (quotaIds []uint32, isFind bool) {
	extend := NewExtend(ino)
	treeItem := mp.extendTree.Get(extend)
	if treeItem == nil {
		isFind = false
		return
	}
	extend = treeItem.(*Extend)
	value, exist := extend.Get([]byte(proto.QuotaKey))
	if !exist {
		isFind = false
		return
	}
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	span := getSpan(ctx)
	if err := json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
		span.Errorf("set quota inode[%v] Unmarshal quotaInfos fail [%v]", ino, err)
		isFind = false
		return
	}
	isFind = true
	quotaInfos.RLock()
	for quotaId := range quotaInfos.QuotaInfoMap {
		quotaIds = append(quotaIds, quotaId)
	}
	quotaInfos.RUnlock()
	span.Infof("isExistQuota inode:[%v] quotaIds [%v] isFind[%v]", ino, quotaIds, isFind)
	return
}

func (mp *metaPartition) isOverQuota(ctx context.Context, ino uint64, size bool, files bool) (status uint8) {
	quotaIds, isFind := mp.isExistQuota(ctx, ino)
	if isFind {
		for _, quotaId := range quotaIds {
			status = mp.mqMgr.IsOverQuota(ctx, size, files, quotaId)
			if status != 0 {
				getSpan(ctx).Warnf("isOverQuota ino[%v] quotaId [%v] size [%v] files[%v] status[%v]", ino, quotaId, size, files, status)
				return
			}
		}
	}
	return
}

func (mp *metaPartition) getInodeQuota(ctx context.Context, inode uint64, p *Packet) (err error) {
	extend := NewExtend(inode)
	quotaInfos := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	var (
		value []byte
		exist bool
	)
	span := getSpan(ctx)
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		goto handleRsp
	}
	extend = treeItem.(*Extend)

	value, exist = extend.Get([]byte(proto.QuotaKey))
	if exist {
		if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
			span.Errorf("getInodeQuota inode[%v] Unmarshal quotaInfos fail [%v]", inode, err)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
	}
handleRsp:
	response := &proto.GetInodeQuotaResponse{}
	span.Infof("getInodeQuota indoe %v ,map %v", inode, quotaInfos.QuotaInfoMap)
	response.MetaQuotaInfoMap = quotaInfos.QuotaInfoMap

	encoded, err := json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) getInodeQuotaInfos(ctx context.Context, inode uint64) (quotaInfos map[uint32]*proto.MetaQuotaInfo, err error) {
	span := getSpan(ctx)
	span.Infof("getInodeQuotaInfos mp[%v] treeLen[%v]", mp.config.PartitionId, mp.extendTree.Len())
	treeItem := mp.extendTree.Get(NewExtend(inode))
	if treeItem == nil {
		return
	}
	extend := treeItem.(*Extend)
	info := &proto.MetaQuotaInfos{
		QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
	}
	value, exist := extend.Get([]byte(proto.QuotaKey))
	if exist {
		if err = json.Unmarshal(value, &info.QuotaInfoMap); err != nil {
			span.Errorf("getInodeQuota inode[%v] Unmarshal quotaInfos fail [%v]", inode, err)
			return
		}
		quotaInfos = info.QuotaInfoMap
	}
	span.Infof("getInodeQuotaInfos inode[%v] quotaInfos [%v] exist [%v]", inode, quotaInfos, exist)
	return
}

func (mp *metaPartition) setInodeQuota(ctx context.Context, quotaIds []uint32, inode uint64) {
	extend := NewExtend(inode)
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
		getSpan(ctx).Errorf("setInodeQuota marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
		return
	}
	extend.Put([]byte(proto.QuotaKey), value, mp.verSeq)
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.Merge(extend, true)
	}

	getSpan(ctx).Infof("setInodeQuota inode[%v] quota [%v] success.", inode, quotaIds)
}
