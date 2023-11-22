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

package metanode

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/exporter"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) CheckQuota(inodeId uint64, p *Packet) (iParm *Inode, inode *Inode, err error) {
	iParm = NewInode(inodeId, 0)
	status := mp.isOverQuota(inodeId, true, false)
	if status != 0 {
		log.LogErrorf("CheckQuota dir quota fail inode [%v] status [%v]", inodeId, status)
		err = errors.New("CheckQuota dir quota is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	item := mp.inodeTree.Get(iParm)
	if item == nil {
		err = fmt.Errorf("inode[%v] not exist", iParm)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	inode = item.(*Inode)
	iParm.StorageClass = inode.StorageClass

	mp.uidManager.acLock.Lock()
	if mp.uidManager.getUidAcl(inode.Uid) {
		log.LogWarnf("CheckQuota UidSpace.vol %v mp[%v] uid %v be set full", mp.uidManager.mpID, mp.uidManager.volName, inode.Uid)
		mp.uidManager.acLock.Unlock()
		status = proto.OpNoSpaceErr
		err = errors.New("CheckQuota UidSpace is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}
	mp.uidManager.acLock.Unlock()
	return
}

// ExtentAppend appends an extent.
func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	ino := NewInode(req.Inode, 0)
	if _, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("ExtentAppend fail status [%v]", err)
		return
	}
	ext := req.Extent
	ino.Extents.Append(ext)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// ExtentAppendWithCheck appends an extent with discard extents check.
// Format: one valid extent key followed by non or several discard keys.
func (mp *metaPartition) ExtentAppendWithCheck(req *proto.AppendExtentKeyWithCheckRequest, p *Packet) (err error) {
	status := mp.isOverQuota(req.Inode, true, false)
	if status != 0 {
		log.LogErrorf("ExtentAppendWithCheck fail status [%v]", status)
		err = errors.New("ExtentAppendWithCheck is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}
	var (
		inoParm *Inode
		i       *Inode
	)
	if inoParm, i, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("ExtentAppendWithCheck CheckQuota fail err [%v]", err)
		return
	}

	//TODO:if storage type is not ssd , update extent key by CacheExtentAppendWithCheck
	// check volume's Type: if volume's type is cold, cbfs' extent can be modify/add only when objextent exist
	//if proto.IsCold(mp.volType) {
	if req.IsCache {
		i.RLock()
		if i.HybridCouldExtents.sortedEks == nil {
			i.HybridCouldExtents.sortedEks = NewSortedObjExtents()

		}
		ObjExtents := i.HybridCouldExtents.sortedEks.(*SortedObjExtents)
		exist, idx := ObjExtents.FindOffsetExist(req.Extent.FileOffset)
		if !exist {
			i.RUnlock()
			err = fmt.Errorf("ebs's objextent not exist with offset[%v]", req.Extent.FileOffset)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		if ObjExtents.eks[idx].Size != uint64(req.Extent.Size) {
			err = fmt.Errorf("ebs's objextent size[%v] isn't equal to the append size[%v]", ObjExtents.eks[idx].Size, req.Extent.Size)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			i.RUnlock()
			return
		}
		i.RUnlock()
	}

	ext := req.Extent

	// extent key verSeq not set value since marshal will not include verseq
	// use inode verSeq instead
	inoParm.setVer(mp.verSeq)

	if !req.IsCache {
		inoParm.StorageClass = req.StorageClass
	}

	if req.IsCache {
		inoParm.Extents.Append(ext)
	} else {
		inoParm.HybridCouldExtents.sortedEks = NewSortedExtents()
		inoParm.HybridCouldExtents.sortedEks.(*SortedExtents).Append(ext)
	}
	log.LogDebugf("ExtentAppendWithCheck: ino(%v) mp(%v) verSeq(%v) storageClass(%v)",
		req.Inode, req.PartitionID, mp.verSeq, proto.StorageClassString(inoParm.StorageClass))

	// Store discard extents right after the append extent key.
	if len(req.DiscardExtents) != 0 {
		if req.IsCache {
			inoParm.Extents.eks = append(inoParm.Extents.eks, req.DiscardExtents...)
		} else {
			extents := inoParm.HybridCouldExtents.sortedEks.(*SortedExtents)
			extents.eks = append(extents.eks, req.DiscardExtents...)
		}
	}
	val, err := inoParm.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var opFlag uint32 = opFSMExtentsAddWithCheck
	if req.IsSplit {
		opFlag = opFSMExtentSplit
	}
	resp, err := mp.submit(opFlag, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	log.LogDebugf("ExtentAppendWithCheck: ino(%v) mp(%v) verSeq (%v) req.VerSeq(%v) rspcode(%v)", req.Inode, req.PartitionID, mp.verSeq, req.VerSeq, resp.(uint8))

	if mp.verSeq > req.VerSeq {
		//reuse ExtentType to identify flag of version inconsistent between metanode and client
		//will resp to client and make client update all streamer's extent and it's verSeq
		p.ExtentType |= proto.MultiVersionFlag
		p.VerSeq = mp.verSeq
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) SetTxInfo(info []*proto.TxInfo) {
	for _, txInfo := range info {
		if txInfo.Volume != mp.config.VolName {
			continue
		}
		mp.txProcessor.mask = txInfo.Mask
		mp.txProcessor.txManager.setLimit(txInfo.OpLimitVal)
		log.LogInfof("SetTxInfo mp %v mask %v limit %v", mp.config.PartitionId, proto.GetMaskString(txInfo.Mask), txInfo.OpLimitVal)
	}
}

type VerOpData struct {
	Op      uint8
	VerSeq  uint64
	VerList []*proto.VolVersionInfo
}

func (mp *metaPartition) checkByMasterVerlist(mpVerList *proto.VolVersionInfoList, masterVerList *proto.VolVersionInfoList) (err error) {
	var currMasterSeq = masterVerList.GetLastVer()
	verMapMaster := make(map[uint64]*proto.VolVersionInfo)
	for _, ver := range masterVerList.VerList {
		verMapMaster[ver.Ver] = ver
	}
	log.LogDebugf("checkVerList. vol %v mp %v masterVerList %v mpVerList.VerList %v", mp.config.VolName, mp.config.PartitionId, masterVerList, mpVerList.VerList)
	mp.multiVersionList.Lock()
	defer mp.multiVersionList.Unlock()
	vlen := len(mpVerList.VerList)
	for id, info2 := range mpVerList.VerList {
		if id == vlen-1 {
			break
		}
		log.LogDebugf("checkVerList. vol %v mp %v ver info %v currMasterSeq %v", mp.config.VolName, mp.config.PartitionId, info2, currMasterSeq)
		_, exist := verMapMaster[info2.Ver]
		if !exist {
			if _, ok := mp.multiVersionList.TemporaryVerMap[info2.Ver]; !ok {
				log.LogInfof("checkVerList. vol %v mp %v ver info %v be consider as TemporaryVer", mp.config.VolName, mp.config.PartitionId, info2)
				mp.multiVersionList.TemporaryVerMap[info2.Ver] = info2
			}
		}
	}

	for verSeq := range mp.multiVersionList.TemporaryVerMap {
		for index, verInfo := range mp.multiVersionList.VerList {
			if verInfo.Ver == verSeq {
				log.LogInfof("checkVerList.updateVerList vol %v mp %v ver info %v be consider as TemporaryVer and do deletion verlist %v",
					mp.config.VolName, mp.config.PartitionId, verInfo, mp.multiVersionList.VerList)
				if index == len(mp.multiVersionList.VerList)-1 {
					log.LogInfof("checkVerList.updateVerList vol %v mp %v last ver info %v should not be consider as TemporaryVer and do deletion verlist %v",
						mp.config.VolName, mp.config.PartitionId, verInfo, mp.multiVersionList.VerList)
					return
				} else {
					mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:index], mp.multiVersionList.VerList[index+1:]...)
				}

				log.LogInfof("checkVerList.updateVerList vol %v mp %v verlist %v", mp.config.VolName, mp.config.PartitionId, mp.multiVersionList.VerList)
				break
			}
		}
	}
	return
}

func (mp *metaPartition) checkVerList(reqVerListInfo *proto.VolVersionInfoList, sync bool) (needUpdate bool, err error) {
	mp.multiVersionList.RLock()
	verMapLocal := make(map[uint64]*proto.VolVersionInfo)
	verMapReq := make(map[uint64]*proto.VolVersionInfo)
	for _, ver := range reqVerListInfo.VerList {
		verMapReq[ver.Ver] = ver
	}

	var (
		VerList []*proto.VolVersionInfo
	)

	for _, info2 := range mp.multiVersionList.VerList {
		log.LogDebugf("checkVerList. vol %v mp %v ver info %v", mp.config.VolName, mp.config.PartitionId, info2)
		vms, exist := verMapReq[info2.Ver]
		if !exist {
			log.LogWarnf("checkVerList. vol %v mp %v version info(%v) not exist in master (%v)",
				mp.config.VolName, mp.config.PartitionId, info2, reqVerListInfo.VerList)
		} else if info2.Status != proto.VersionNormal && info2.Status != vms.Status {
			log.LogWarnf("checkVerList. vol %v mp %v ver %v status abnormal %v", mp.config.VolName, mp.config.PartitionId, info2.Ver, info2.Status)
			info2.Status = vms.Status
			needUpdate = true
		}

		if _, ok := verMapLocal[info2.Ver]; !ok {
			verMapLocal[info2.Ver] = info2
			VerList = append(VerList, info2)
		}
	}
	mp.multiVersionList.RUnlock()

	for _, vInfo := range reqVerListInfo.VerList {
		if vInfo.Status != proto.VersionNormal {
			log.LogDebugf("checkVerList. vol %v mp %v master info %v", mp.config.VolName, mp.config.PartitionId, vInfo)
			continue
		}
		ver, exist := verMapLocal[vInfo.Ver]
		if !exist {
			expStr := fmt.Sprintf("checkVerList.vol %v mp %v not found %v in mp list and append version %v",
				mp.config.VolName, mp.config.PartitionId, vInfo.Ver, vInfo)
			log.LogWarnf("[checkVerList] vol %v", expStr)
			if vInfo.Ver < mp.multiVersionList.GetLastVer() {
				continue
			}
			exporter.Warning(expStr)
			VerList = append(VerList, vInfo)
			needUpdate = true
			verMapLocal[vInfo.Ver] = vInfo
			continue
		}
		if ver.Status != vInfo.Status {
			warn := fmt.Sprintf("checkVerList.vol %v mp %v ver %v inoraml.local status %v update to %v",
				mp.config.VolName, mp.config.PartitionId, vInfo.Status, vInfo.Ver, vInfo.Status)
			log.LogWarn(warn)
			ver.Status = vInfo.Status
		}
	}
	if needUpdate {
		var lastSeq uint64
		sort.SliceStable(VerList, func(i, j int) bool {
			if VerList[i].Ver < VerList[j].Ver {
				lastSeq = VerList[j].Ver
				return true
			}
			lastSeq = VerList[i].Ver
			return false
		})
		if err = mp.HandleVersionOp(proto.SyncBatchVersionList, lastSeq, VerList, sync); err != nil {
			return
		}
	}
	return
}

func (mp *metaPartition) HandleVersionOp(op uint8, verSeq uint64, verList []*proto.VolVersionInfo, sync bool) (err error) {

	verData := &VerOpData{
		Op:      op,
		VerSeq:  verSeq,
		VerList: verList,
	}
	data, _ := json.Marshal(verData)
	if sync {
		_, err = mp.submit(opFSMVersionOp, data)
		return
	}
	select {
	case mp.verUpdateChan <- data:
		log.LogDebugf("mp %v verSeq %v op %v be pushed to queue", mp.config.PartitionId, verSeq, op)
	default:
		err = fmt.Errorf("mp %v version update channel full, verdata %v not be executed", mp.config.PartitionId, string(data))
	}
	return
}

func (mp *metaPartition) GetAllVersionInfo(req *proto.MultiVersionOpRequest, p *Packet) (err error) {
	return
}

func (mp *metaPartition) GetSpecVersionInfo(req *proto.MultiVersionOpRequest, p *Packet) (err error) {
	return
}

func (mp *metaPartition) GetExtentByVer(ino *Inode, req *proto.GetExtentsRequest, rsp *proto.GetExtentsResponse) {
	log.LogInfof("action[GetExtentByVer] read ino %v readseq %v ino seq %v hist len %v", ino.Inode, req.VerSeq, ino.getVer(), ino.getLayerLen())
	reqVer := req.VerSeq
	if isInitSnapVer(req.VerSeq) {
		reqVer = 0
	}
	ino.DoReadFunc(func() {
		ino.Extents.Range(func(ek proto.ExtentKey) bool {
			if ek.GetSeq() <= reqVer {
				rsp.Extents = append(rsp.Extents, ek)
				log.LogInfof("action[GetExtentByVer] fresh layer.read ino %v readseq %v ino seq %v include ek %v", ino.Inode, reqVer, ino.getVer(), ek)
			} else {
				log.LogInfof("action[GetExtentByVer] fresh layer.read ino %v readseq %v ino seq %v exclude ek %v", ino.Inode, reqVer, ino.getVer(), ek)
			}
			return true
		})
		ino.RangeMultiVer(func(idx int, snapIno *Inode) bool {
			log.LogInfof("action[GetExtentByVer] read ino %v readseq %v snapIno ino seq %v", ino.Inode, reqVer, snapIno.getVer())
			for _, ek := range snapIno.Extents.eks {
				if reqVer >= ek.GetSeq() {
					log.LogInfof("action[GetExtentByVer] get extent ino %v readseq %v snapIno ino seq %v, include ek (%v)", ino.Inode, reqVer, snapIno.getVer(), ek.String())
					rsp.Extents = append(rsp.Extents, ek)
				} else {
					log.LogInfof("action[GetExtentByVer] not get extent ino %v readseq %v snapIno ino seq %v, exclude ek (%v)", ino.Inode, reqVer, snapIno.getVer(), ek.String())
				}
			}
			if reqVer >= snapIno.getVer() {
				log.LogInfof("action[GetExtentByVer] finish read ino %v readseq %v snapIno ino seq %v", ino.Inode, reqVer, snapIno.getVer())
				return false
			}
			return true
		})
		sort.SliceStable(rsp.Extents, func(i, j int) bool {
			return rsp.Extents[i].FileOffset < rsp.Extents[j].FileOffset
		})

	})

	return
}

func (mp *metaPartition) SetUidLimit(info []*proto.UidSpaceInfo) {
	mp.uidManager.volName = mp.config.VolName
	mp.uidManager.setUidAcl(info)
}

func (mp *metaPartition) GetUidInfo() (info []*proto.UidReportSpaceInfo) {
	return mp.uidManager.getAllUidSpace()
}

// ExtentsList returns the list of extents.
func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {
	log.LogDebugf("action[ExtentsList] inode %v verSeq %v", req.Inode, req.VerSeq)

	// note:don't need set reqSeq, extents get be done in next step
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInodeTopLayer(ino)

	//notice.getInode should not set verSeq due to extent need filter from the newest layer to req.VerSeq
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)

	if !ino.storeInReplicaSystem() && req.IsCache != true {
		status = proto.OpErr
		reply = []byte(fmt.Sprintf("ino(%v) storageClass(%v) IsCache(%v) not support ExtentsList",
			ino.Inode, ino.StorageClass, req.IsCache))
		p.PacketErrorWithBody(status, reply)
		return
	}

	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{}
		log.LogInfof("action[ExtentsList] inode %v request verseq %v ino ver %v ino.Size %v ino %v hist len %v",
			req.Inode, req.VerSeq, ino.getVer(), ino.Size, ino, ino.getLayerLen())
		resp.WriteGeneration = atomic.LoadUint64(&ino.WriteGeneration)
		if req.VerSeq > 0 && ino.getVer() > 0 && (req.VerSeq < ino.getVer() || isInitSnapVer(req.VerSeq)) {
			mp.GetExtentByVer(ino, req, resp)
			vIno := ino.Copy().(*Inode)
			vIno.setVerNoCheck(req.VerSeq)
			if vIno = mp.getInodeByVer(vIno); vIno != nil {
				resp.Generation = vIno.Generation
				resp.Size = vIno.Size
			}
		} else {
			if req.IsCache {
				ino.DoReadFunc(func() {
					resp.Generation = ino.Generation
					resp.Size = ino.Size
					ino.Extents.Range(func(ek proto.ExtentKey) bool {
						resp.Extents = append(resp.Extents, ek)
						log.LogInfof("action[ExtentsList] append ek %v", ek)
						return true
					})
				})
			} else {
				ino.DoReadFunc(func() {
					resp.Generation = ino.Generation
					resp.Size = ino.Size
					//ino.Extents.Range(func(ek proto.ExtentKey) bool {
					//	resp.Extents = append(resp.Extents, ek)
					//	log.LogInfof("action[ExtentsList] append ek %v", ek)
					//	return true
					//})
					if ino.HybridCouldExtents.sortedEks != nil {
						extents := ino.HybridCouldExtents.sortedEks.(*SortedExtents)
						extents.Range(func(ek proto.ExtentKey) bool {
							resp.Extents = append(resp.Extents, ek)
							log.LogInfof("action[ExtentsList] append ek %v", ek)
							return true
						})
					}
				})
			}
		}
		if req.VerAll {
			resp.LayerInfo = retMsg.Msg.getAllLayerEks()
		}

		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		} else {
			//if inode is required for writing, mark it as forbidden migration
			if req.OpenForWrite {
				var val []byte
				val, err = ino.Marshal()
				if err != nil {
					p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
					return
				}
				_, err = mp.submit(opFSMForbiddenMigrationInode, val)
				if err != nil {
					status = proto.OpErr
					reply = []byte(err.Error())
				}
			}
		}
	}
	//mark inode as ForbiddenMigration
	p.PacketErrorWithBody(status, reply)
	return
}

// ObjExtentsList returns the list of obj extents and extents.
func (mp *metaPartition) ObjExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.setVer(req.VerSeq)
	retMsg := mp.getInode(ino, false)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		if ino.StorageClass != proto.StorageClass_BlobStore {
			status = proto.OpDismatchStorageClass
			reply = []byte(fmt.Sprintf("Dismatch storage type, current storage type is %s",
				proto.StorageClassString(ino.StorageClass)))
			p.PacketErrorWithBody(status, reply)
			return
		}
		resp := &proto.GetObjExtentsResponse{}
		ino.DoReadFunc(func() {
			resp.Generation = ino.Generation
			resp.Size = ino.Size
			//cache ek
			ino.Extents.Range(func(ek proto.ExtentKey) bool {
				resp.Extents = append(resp.Extents, ek)
				return true
			})
			//from SortedHybridCloudExtents
			if ino.HybridCouldExtents.sortedEks != nil {
				objEks := ino.HybridCouldExtents.sortedEks.(*SortedObjExtents)
				objEks.Range(func(ek proto.ObjExtentKey) bool {
					resp.ObjExtents = append(resp.ObjExtents, ek)
					return true
				})
			}
			//ino.ObjExtents.Range(func(ek proto.ObjExtentKey) bool {
			//	resp.ObjExtents = append(resp.ObjExtents, ek)
			//	return true
			//})
		})

		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// ExtentsTruncate truncates an extent.
func (mp *metaPartition) ExtentsTruncate(req *ExtentsTruncateReq, p *Packet, remoteAddr string) (err error) {
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	fileSize := uint64(0)
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, fileSize)
		}()
	}
	ino := NewInode(req.Inode, proto.Mode(os.ModePerm))
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		err = fmt.Errorf("inode %v is not exist", req.Inode)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	i := item.(*Inode)
	if !i.storeInReplicaSystem() {
		err = fmt.Errorf("inode %v storageClass(%v) do not support tuncate operation", req.Inode, i.StorageClass)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	status := mp.isOverQuota(req.Inode, req.Size > i.Size, false)
	if status != 0 {
		log.LogErrorf("ExtentsTruncate fail status [%v]", status)
		err = errors.New("ExtentsTruncate is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	ino.Size = req.Size
	fileSize = ino.Size
	ino.setVer(mp.verSeq)
	ino.StorageClass = i.StorageClass
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentTruncate, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error) {
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	var ino *Inode
	if ino, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("BatchExtentAppend fail err [%v]", err)
		return
	}
	ino.StorageClass = req.StorageClass
	if !ino.storeInReplicaSystem() {
		err = fmt.Errorf("ino %v storage type %v donot support BatchExtentAppend", ino.Inode, ino.StorageClass)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	extents := req.Extents
	ino.HybridCouldExtents.sortedEks = NewSortedExtents()
	for _, extent := range extents {
		ino.HybridCouldExtents.sortedEks.(*SortedExtents).Append(extent)
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) BatchObjExtentAppend(req *proto.AppendObjExtentKeysRequest, p *Packet) (err error) {
	var ino *Inode
	if ino, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("BatchObjExtentAppend fail status [%v]", err)
		return
	}
	if ino.StorageClass != proto.StorageClass_BlobStore {
		err = errors.New(fmt.Sprintf("ino(%v) StorageClass(%v) donot support BatchObjExtentAppend",
			ino.Inode, ino.StorageClass))
		log.LogErrorf("BatchObjExtentAppend fail [%v]", err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	//can only be called when write into ebs
	ino.StorageClass = proto.StorageClass_BlobStore
	objExtents := req.Extents
	ino.HybridCouldExtents.sortedEks = NewSortedObjExtents()
	for _, objExtent := range objExtents {
		err = ino.HybridCouldExtents.sortedEks.(*SortedObjExtents).Append(objExtent)
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMObjExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// func (mp *metaPartition) ExtentsDelete(req *proto.DelExtentKeyRequest, p *Packet) (err error) {
// 	ino := NewInode(req.Inode, 0)
// 	inode := mp.inodeTree.Get(ino).(*Inode)
// 	inode.Extents.Delete(req.Extents)
// 	curTime := timeutil.GetCurrentTimeUnix()
// 	if inode.ModifyTime < curTime {
// 		inode.ModifyTime = curTime
// 	}
// 	val, err := inode.Marshal()
// 	if err != nil {
// 		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
// 		return
// 	}
// 	resp, err := mp.submit(opFSMExtentsDel, val)
// 	if err != nil {
// 		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
// 		return
// 	}
// 	p.PacketErrorWithBody(resp.(uint8), nil)
// 	return
// }

// ExtentsEmpty only use in datalake situation
func (mp *metaPartition) ExtentsOp(p *Packet, ino *Inode, op uint32) (err error) {
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(op, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) sendExtentsToChan(eks []proto.ExtentKey) (err error) {
	if len(eks) == 0 {
		return
	}

	sortExts := NewSortedExtentsFromEks(eks)
	val, err := sortExts.MarshalBinary(true)
	if err != nil {
		return fmt.Errorf("[delExtents] marshal binary fail, %s", err.Error())
	}

	_, err = mp.submit(opFSMSentToChan, val)

	return
}
