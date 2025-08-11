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

package meta

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// API implementations
//
// txIcreate create inode and tx together
func (mw *MetaWrapper) txIcreate(tx *Transaction, mp *MetaPartition, mode, uid, gid uint32,
	target []byte, quotaIds []uint32, fullPath string,
) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txIcreate", err, bgTime, 1)
	}()

	tx.SetTmID(mp.PartitionID)

	req := &proto.TxCreateInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Mode:        mode,
		Uid:         uid,
		Gid:         gid,
		Target:      target,
		QuotaIds:    quotaIds,
		TxInfo:      tx.txInfo,
		StorageType: mw.DefaultStorageClass,
	}
	req.FullPaths = []string{fullPath}

	resp := new(proto.TxCreateInodeResponse)
	defer func() {
		tx.OnExecuted(status, resp.TxInfo)
	}()

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaTxCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("txIcreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("txIcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		// set tx error msg
		err = errors.New(packet.GetResultMsg())
		if status == statusFull {
			log.LogWarnf("txIcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
			return
		}
		log.LogErrorf("txIcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("txIcreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}

	if resp.Info == nil || resp.TxInfo == nil {
		err = errors.New(fmt.Sprintf("txIcreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}

	tx.Started = true
	tx.txInfo = resp.TxInfo
	log.LogDebugf("txIcreate: packet(%v) mp(%v) req(%v) info(%v) tx(%v)", packet, mp, *req, resp.Info, resp.TxInfo)
	return status, resp.Info, nil
}

func (mw *MetaWrapper) quotaIcreate(mp *MetaPartition, mode, uid, gid uint32, target []byte, quotaIds []uint32, fullPath string) (status int,
	info *proto.InodeInfo, err error,
) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("icreate", err, bgTime, 1)
	}()

	req := &proto.QuotaCreateInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Mode:        mode,
		Uid:         uid,
		Gid:         gid,
		Target:      target,
		QuotaIds:    quotaIds,
		StorageType: mw.DefaultStorageClass,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpQuotaCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("quotaIcreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("quotaIcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("quotaIcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("quotaIcreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("quotaIcreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}
	log.LogDebugf("quotaIcreate: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) icreate(mp *MetaPartition, mode, uid, gid uint32, target []byte, fullPath string) (status int,
	info *proto.InodeInfo, err error,
) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("icreate", err, bgTime, 1)
	}()

	req := &proto.CreateInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Mode:        mode,
		Uid:         uid,
		Gid:         gid,
		Target:      target,
		StorageType: mw.DefaultStorageClass,
	}

	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("icreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("icreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("icreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("icreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("icreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}
	log.LogDebugf("icreate: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) sendToMetaPartitionWithTx(mp *MetaPartition, req *proto.Packet) (packet *proto.Packet, err error) {
	retryNum := int64(0)
	for {
		packet, err = mw.sendToMetaPartition(mp, req)
		if err != nil {
			log.LogErrorf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) err(%v)",
				string(req.Data), mp, req.GetOpMsg(), err)
			return
		}

		if packet.ResultCode != proto.OpTxConflictErr {
			break
		}

		log.LogWarnf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) result(%v), tx conflict retry: %v req(%v)",
			packet, mp, packet.GetOpMsg(), packet.GetResultMsg(), retryNum, string(req.Data))
		retryNum++
		if retryNum > mw.TxConflictRetryNum {
			log.LogErrorf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) result(%v), tx conflict retry: %v req(%v)",
				packet, mp, packet.GetOpMsg(), packet.GetResultMsg(), retryNum, string(req.Data))
			break
		}
		time.Sleep(time.Duration(mw.TxConflictRetryInterval) * time.Millisecond)
	}

	return
}

func (mw *MetaWrapper) SendTxPack(req proto.TxPack, resp interface{}, Opcode uint8, mp *MetaPartition,
	checkStatusFunc func(int, *proto.Packet) error, ignoreExistError bool,
) (status int, err error, packet *proto.Packet) {
	packet = proto.NewPacketReqID()
	packet.Opcode = Opcode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("SendTxPack reqType(%v) txInfo(%v) : err(%v)", packet.GetOpMsg(), req.GetInfo(), err)
		return
	}

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("SendTxPack: packet(%v) mp(%v) txInfo(%v) err(%v)",
			packet, mp, req.GetInfo(), err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if checkStatusFunc != nil {
		if err = checkStatusFunc(status, packet); err != nil {
			log.LogErrorf("SendTxPack: packet(%v) mp(%v) req(%v) txInfo(%v) result(%v) err(%v)",
				packet, mp, packet.GetOpMsg(), req.GetInfo(), packet.GetResultMsg(), err)
			return
		}
	} else if status != statusOK {
		if status == statusExist && !ignoreExistError {
			err = errors.New(packet.GetResultMsg())
			log.LogWarnf("SendTxPack: packet(%v) mp(%v) req(%v) txInfo(%v) result(%v)",
				packet, mp, packet.GetOpMsg(), req.GetInfo(), packet.GetResultMsg())
		}
		return
	}

	if resp == nil {
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("SendTxPack: packet(%v) mp(%v) txInfo(%v) err(%v) PacketData(%v)",
			packet, mp, req.GetInfo(), err, string(packet.Data))
		return
	}
	return
}

func (mw *MetaWrapper) txIunlink(tx *Transaction, mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txIunlink", err, bgTime, 1)
	}()

	req := &proto.TxUnlinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		TxInfo:      tx.txInfo,
	}
	req.FullPaths = []string{fullPath}
	resp := new(proto.TxUnlinkInodeResponse)
	metric := exporter.NewTPCnt("OpMetaTxUnlinkInode")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, resp, proto.OpMetaTxUnlinkInode, mp, nil, false); err != nil {
		log.LogErrorf("txIunlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("txIunlink: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) iunlink(mp *MetaPartition, inode uint64, verSeq uint64, denVerSeq uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("iunlink", err, bgTime, 1)
	}()

	// use uniq id to dedup request
	status, uniqID, err := mw.consumeUniqID(mp)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		return
	}

	req := &proto.UnlinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		UniqID:      uniqID,
		VerSeq:      verSeq,
		DenVerSeq:   denVerSeq,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaUnlinkInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("iunlink: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("iunlink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("iunlink: packet(%v) mp(%v) req(%v) result(%v) status(%v)", packet, mp, *req, packet.GetResultMsg(), status)
		return
	}

	resp := new(proto.UnlinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("iunlink: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	log.LogDebugf("iunlink: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) iclearCache(mp *MetaPartition, inode uint64) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("iclearCache", err, bgTime, 1)
	}()

	req := &proto.ClearInodeCacheRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaClearInodeCache
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("iclearCache: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("iclearCache: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("iclearCache: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("iclearCache: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return status, nil
}

func (mw *MetaWrapper) ievict(mp *MetaPartition, inode uint64, fullPath string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ievict", err, bgTime, 1)
	}()

	req := &proto.EvictInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaEvictInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("ievict: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogWarnf("ievict: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogWarnf("ievict: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("ievict exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) txDcreate(tx *Transaction, mp *MetaPartition, parentID uint64, name string, inode uint64,
	mode uint32, quotaIds []uint32, fullPath string, ignoreExist bool,
) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txDcreate", err, bgTime, 1)
	}()

	if parentID == inode {
		return statusExist, nil
	}

	req := &proto.TxCreateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
		QuotaIds:    quotaIds,
		TxInfo:      tx.txInfo,
	}
	req.FullPaths = []string{fullPath}

	metric := exporter.NewTPCnt("OpMetaTxCreateDentry")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	// statusCheckFunc := func(status int, packet *proto.Packet) (err error) {
	//	if (status != statusOK) && (status != statusExist) {
	//		err = errors.New(packet.GetResultMsg())
	//		log.LogErrorf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	//		return
	//	} else if status == statusExist {
	//		log.LogWarnf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	//	}
	//	return
	// }

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, nil, proto.OpMetaTxCreateDentry, mp, nil, ignoreExist); err != nil {
		log.LogErrorf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) quotaDcreate(mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32,
	quotaIds []uint32, fullPath string, ignoreExistError bool,
) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("dcreate", err, bgTime, 1)
	}()

	if parentID == inode {
		return statusExist, nil
	}

	req := &proto.QuotaCreateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
		QuotaIds:    quotaIds,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpQuotaCreateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("quotaDcreate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("quotaDcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if (status != statusOK) && (status != statusExist) {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	} else if status == statusExist && !ignoreExistError {
		log.LogWarnf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	log.LogDebugf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) dcreate(mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32,
	fullPath string, ignoreExistError bool,
) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("dcreate", err, bgTime, 1)
	}()

	if parentID == inode {
		return statusExist, nil
	}

	req := &proto.CreateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaCreateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("dcreate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("dcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if (status != statusOK) && (status != statusExist) {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	} else if status == statusExist && !ignoreExistError {
		log.LogWarnf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	if proto.IsDir(mode) {
		mw.AddInoInfoCache(inode, parentID, name)
	}
	log.LogDebugf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) txDupdate(tx *Transaction, mp *MetaPartition, parentID uint64, name string, newInode, oldIno uint64, fullPath string) (status int, oldInode uint64, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txDupdate", err, bgTime, 1)
	}()

	if parentID == newInode {
		return statusExist, 0, nil
	}

	req := &proto.TxUpdateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		Inode:       newInode,
		OldIno:      oldIno,
		TxInfo:      tx.txInfo,
	}
	req.FullPaths = []string{fullPath}

	resp := new(proto.TxUpdateDentryResponse)
	metric := exporter.NewTPCnt("OpMetaTxUpdateDentry")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, resp, proto.OpMetaTxUpdateDentry, mp, nil, false); err != nil {
		log.LogErrorf("txDupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("txDupdate: packet(%v) mp(%v) req(%v) oldIno(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) dupdate(mp *MetaPartition, parentID uint64, name string, newInode uint64, fullPath string) (status int, oldInode uint64, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("dupdate", err, bgTime, 1)
	}()

	if parentID == newInode {
		return statusExist, 0, nil
	}

	req := &proto.UpdateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		Inode:       newInode,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaUpdateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("dupdate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("dupdate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("dupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.UpdateDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("dupdate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("dupdate: packet(%v) mp(%v) req(%v) oldIno(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) txCreateTX(tx *Transaction, mp *MetaPartition) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txCreateTX", err, bgTime, 1)
	}()

	tx.SetTmID(mp.PartitionID)

	req := &proto.TxCreateRequest{
		VolName:         mw.volname,
		PartitionID:     mp.PartitionID,
		TransactionInfo: tx.txInfo,
	}

	resp := new(proto.TxCreateResponse)
	metric := exporter.NewTPCnt("OpMetaTxCreate")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, resp, proto.OpMetaTxCreate, mp, nil, false); err != nil {
		log.LogErrorf("txCreateTX: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	if resp.TxInfo == nil {
		err = fmt.Errorf("txCreateTX: create tx resp nil")
		log.LogError(err)
		return statusError, err
	}

	if log.EnableDebug() {
		log.LogDebugf("txCreateTX: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	}

	tx.txInfo = resp.TxInfo
	tx.Started = true
	return statusOK, nil
}

// func (mw *MetaWrapper) txPreCommit(tx *Transaction, mp *MetaPartition) (status int, err error) {
//	bgTime := stat.BeginStat()
//	defer func() {
//		stat.EndStat("txPreCommit", err, bgTime, 1)
//	}()
//
//	tx.txInfo.TmID = int64(mp.PartitionID)
//	req := &proto.TxPreCommitRequest{
//		VolName:         mw.volname,
//		PartitionID:     mp.PartitionID,
//		TransactionInfo: tx.txInfo,
//	}
//
//	metric := exporter.NewTPCnt("OpTxPreCommit")
//	defer func() {
//		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
//	}()
//
//	var packet *proto.Packet
//	if status, err, packet = mw.SendTxPack(req, nil, proto.OpTxPreCommit, mp, nil); err != nil {
//		log.LogErrorf("txPreCommit: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
//		return
//	}
//
//	if log.EnableDebug() {
//		log.LogDebugf("txPreCommit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
//	}
//
//	return statusOK, nil
// }

func (mw *MetaWrapper) txDdelete(tx *Transaction, mp *MetaPartition, parentID, ino uint64, name string, fullPath string) (status int, inode uint64, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txDdelete", err, bgTime, 1)
	}()

	req := &proto.TxDeleteDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		Ino:         ino,
		TxInfo:      tx.txInfo,
	}
	req.FullPaths = []string{fullPath}

	resp := new(proto.TxDeleteDentryResponse)

	metric := exporter.NewTPCnt("OpMetaTxDeleteDentry")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, resp, proto.OpMetaTxDeleteDentry, mp, nil, false); err != nil {
		log.LogErrorf("txDdelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("txDdelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) ddelete(mp *MetaPartition, parentID uint64, name string, inodeCreateTime int64, verSeq uint64, fullPath string) (status int, inode uint64, denVer uint64, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ddelete", err, bgTime, 1)
	}()

	req := &proto.DeleteDentryRequest{
		VolName:         mw.volname,
		PartitionID:     mp.PartitionID,
		ParentID:        parentID,
		Name:            name,
		InodeCreateTime: inodeCreateTime,
		Verseq:          verSeq,
	}
	req.FullPaths = []string{fullPath}
	log.LogDebugf("action[ddelete] %v", req)
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaDeleteDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("ddelete: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("ddelete: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("ddelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("ddelete: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("ddelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, packet.VerSeq, nil
}

func (mw *MetaWrapper) canDeleteInode(mp *MetaPartition, info *proto.InodeInfo, ino uint64) (can bool, err error) {
	createTime := info.CreateTime.Unix()
	deleteLockTime := mw.volDeleteLockTime * 60 * 60

	if deleteLockTime > 0 && createTime+deleteLockTime > time.Now().Unix() {
		err = errors.NewErrorf("the current Inode[%v] is still locked for deletion", ino)
		log.LogWarnf("canDeleteInode: mp(%v) ino(%v) err(%v)", mp, ino, err)
		return false, syscall.EPERM
	}

	return true, nil
}

func (mw *MetaWrapper) ddeletes(mp *MetaPartition, parentID uint64, dentries []proto.Dentry, fullPaths []string) (status int,
	resp *proto.BatchDeleteDentryResponse, err error,
) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ddeletes", err, bgTime, 1)
	}()

	req := &proto.BatchDeleteDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Dens:        dentries,
		FullPaths:   fullPaths,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchDeleteDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("ddeletes: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("ddeletes: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusAgain {
		err = errors.New("conflict request")
		log.LogErrorf("ddeletes: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("ddeletes: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp = new(proto.BatchDeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("ddeletes: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("ddeletes: packet(%v) mp(%v) req(%v) (%v)", packet, mp, *req, resp.Items)
	return statusOK, resp, nil
}

func (mw *MetaWrapper) lookup(mp *MetaPartition, parentID uint64, name string, verSeq uint64) (status int, inode uint64, mode uint32, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("lookup", err, bgTime, 1)
	}()

	req := &proto.LookupRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		VerSeq:      verSeq,
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaLookup
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("lookup: err(%v)", err)
		return
	}

	log.LogDebugf("lookup enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("lookup: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		errMetric := exporter.NewCounter("fileOpenFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			err = errors.New(packet.GetResultMsg())
			log.LogErrorf("lookup: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
			errMetric := exporter.NewCounter("fileOpenFailed")
			errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		} else {
			log.LogDebugf("lookup exit: packet(%v) mp(%v) req(%v) NoEntry", packet, mp, *req)
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("lookup: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		errMetric := exporter.NewCounter("fileOpenFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		return
	}
	log.LogDebugf("lookup exit: packet(%v) mp(%v) req(%v) ino(%v) mode(%v)", packet, mp, *req, resp.Inode, resp.Mode)
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(mp *MetaPartition, inode uint64, verSeq uint64) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("iget", err, bgTime, 1)
	}()

	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		VerSeq:      verSeq,
		InnerReq:    mw.InnerReq,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaInodeGet
	packet.PartitionID = mp.PartitionID

	log.LogDebugf("action[iget] pack mp id %v, req %v", mp.PartitionID, req)

	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("iget: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("iget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("iget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		log.LogErrorf("iget: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) batchIget(wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64, respCh chan []*proto.InodeInfo) {
	defer wg.Done()
	var err error

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("batchIget", err, bgTime, 1)
	}()

	req := &proto.BatchInodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inodes:      inodes,
		VerSeq:      mw.VerReadSeq,
		InnerReq:    mw.InnerReq,
	}
	log.LogDebugf("action[batchIget] req %v", req)
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchInodeGet
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchIget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.BatchInodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("action[batchIget] resp %v", resp)
	if len(resp.Infos) == 0 {
		return
	}

	select {
	case respCh <- resp.Infos:
	default:
	}
}

// read limit dentries start from
func (mw *MetaWrapper) readDirLimit(mp *MetaPartition, parentID uint64, from string, limit uint64, verSeq uint64, verOpt uint8) (status int, children []proto.Dentry, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("readDirLimit", err, bgTime, 1)
		//if err == nil && mw.RemoteCacheBloom != nil {
		//	cacheBloom := mw.RemoteCacheBloom()
		//	if cacheBloom.TestUint64(parentID) {
		//		for _, c := range children {
		//			cacheBloom.AddUint64(c.Inode)
		//		}
		//	}
		//}
	}()

	req := &proto.ReadDirLimitRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Marker:      from,
		Limit:       limit,
		VerSeq:      verSeq,
		VerOpt:      verOpt,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaReadDirLimit
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("readDirLimit: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("action[readDirLimit] mp [%v] parentId %v", mp.PartitionID, parentID)
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("readDirLimit: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		log.LogErrorf("readDirLimit: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ReadDirLimitResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("readDirLimit: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("readDirLimit: packet(%v) mp(%v) req(%v) rsp(%v)", packet, mp, *req, resp.Children)
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(mp *MetaPartition, inode uint64, extent proto.ExtentKey,
	discard []proto.ExtentKey, isSplit bool, isCache bool, storageClass uint32, isMigration bool,
) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("appendExtentKey", err, bgTime, 1)
	}()

	req := &proto.AppendExtentKeyWithCheckRequest{
		VolName:        mw.volname,
		PartitionID:    mp.PartitionID,
		Inode:          inode,
		Extent:         extent,
		DiscardExtents: discard,
		IsSplit:        isSplit,
		IsCache:        isCache,
		StorageClass:   storageClass,
		IsMigration:    isMigration,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaExtentAddWithCheck
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("appendExtentKey: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("appendExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		if status != StatusConflictExtents {
			log.LogErrorf("appendExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		}
	}
	return status, err
}

func (mw *MetaWrapper) getExtents(mp *MetaPartition, inode uint64, isCache bool, openForWrite, isMigration bool) (resp *proto.GetExtentsResponse, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getExtents", err, bgTime, 1)
	}()

	req := &proto.GetExtentsRequest{
		VolName:      mw.volname,
		PartitionID:  mp.PartitionID,
		Inode:        inode,
		VerSeq:       mw.VerReadSeq,
		IsCache:      isCache,
		OpenForWrite: openForWrite,
		IsMigration:  isMigration,
		InnerReq:     mw.InnerReq,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getExtents: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	// for debug
	// log.LogDebugf("getExtents req: id(%v) data(%v) stack[%v]", packet.GetReqID(), req, string(debug.Stack()))

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getExtents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}
	resp = &proto.GetExtentsResponse{}
	resp.Status = parseStatus(packet.ResultCode)
	if resp.Status != statusOK {
		err = errors.New(packet.GetResultMsg())
		msg := fmt.Sprintf("getExtents: packet(%v) mp(%v) result(%v)", packet, mp, packet.GetResultMsg())
		if packet.ResultCode == proto.OpMismatchStorageClass {
			log.LogWarnf(msg)
		} else {
			log.LogError(msg)
		}
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getExtents: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	return resp, nil
}

func (mw *MetaWrapper) getObjExtents(mp *MetaPartition, inode uint64) (status int, gen, size uint64, extents []proto.ExtentKey, objExtents []proto.ObjExtentKey, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getObjExtents", err, bgTime, 1)
	}()

	req := &proto.GetExtentsRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaObjExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getObjExtents: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getObjExtents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		extents = make([]proto.ExtentKey, 0)
		log.LogErrorf("getObjExtents: packet(%v) mp(%v) result(%v)", packet, mp, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetObjExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getObjExtents: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	return statusOK, resp.Generation, resp.Size, resp.Extents, resp.ObjExtents, nil
}

// func (mw *MetaWrapper) delExtentKey(mp *MetaPartition, inode uint64, extents []proto.ExtentKey) (status int, err error) {
// 	req := &proto.DelExtentKeyRequest{
// 		VolName:     mw.volname,
// 		PartitionID: mp.PartitionID,
// 		Inode:       inode,
// 		Extents:     extents,
// 	}

// 	packet := proto.NewPacketReqID()
// 	packet.Opcode = proto.OpMetaExtentsDel
// 	packet.PartitionID = mp.PartitionID
// 	err = packet.MarshalData(req)
// 	if err != nil {
// 		log.LogErrorf("delExtentKey: req(%v) err(%v)", *req, err)
// 		return
// 	}

// 	metric := exporter.NewTPCnt(packet.GetOpMsg())
// 	defer func() {
// 		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
// 	}()

// 	packet, err = mw.sendToMetaPartition(mp, packet)
// 	if err != nil {
// 		log.LogErrorf("delExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
// 		return
// 	}

// 	status = parseStatus(packet.ResultCode)
// 	if status != statusOK {
// 		log.LogErrorf("delExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
// 	}
// 	return status, nil
// }

func (mw *MetaWrapper) truncate(mp *MetaPartition, inode, size uint64, fullPath string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("truncate", err, bgTime, 1)
	}()

	req := &proto.TruncateRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Size:        size,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaTruncate
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("truncate: ino(%v) size(%v) err(%v)", inode, size, err)
		return
	}

	log.LogDebugf("truncate enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("truncate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("truncate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("truncate exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) txIlink(tx *Transaction, mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txIlink", err, bgTime, 1)
	}()

	req := &proto.TxLinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		TxInfo:      tx.txInfo,
	}
	req.FullPaths = []string{fullPath}

	resp := new(proto.TxLinkInodeResponse)
	metric := exporter.NewTPCnt("OpMetaTxLinkInode")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(req, resp, proto.OpMetaTxLinkInode, mp, nil, false); err != nil {
		log.LogErrorf("txIlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	if log.EnableDebug() {
		log.LogDebugf("txIlink exit: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) ilink(mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	return mw.ilinkWork(mp, inode, proto.OpMetaLinkInode, fullPath)
}

func (mw *MetaWrapper) ilinkWork(mp *MetaPartition, inode uint64, op uint8, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ilink", err, bgTime, 1)
	}()

	// use unique id to dedup request
	status, uniqID, err := mw.consumeUniqID(mp)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		return
	}

	req := &proto.LinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		UniqID:      uniqID,
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.Opcode = op
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("ilink: req(%v) err(%v)", *req, err)
		return
	}

	log.LogDebugf("ilink enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("ilink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("ilink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.LinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("ilink: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("ilink: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}
	log.LogDebugf("ilink exit: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) setattr(mp *MetaPartition, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("setattr", err, bgTime, 1)
	}()

	req := &proto.SetAttrRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Valid:       valid,
		Mode:        mode,
		Uid:         uid,
		Gid:         gid,
		AccessTime:  atime,
		ModifyTime:  mtime,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaSetattr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("setattr: err(%v)", err)
		return
	}

	log.LogDebugf("setattr enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("setattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("setattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("setattr exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) createMultipart(mp *MetaPartition, path string, extend map[string]string) (status int, multipartId string, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("createMultipart", err, bgTime, 1)
	}()

	req := &proto.CreateMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		Extend:      extend,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpCreateMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("createMultipart: err(%v)", err)
		return
	}

	log.LogDebugf("createMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("createMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("createMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("createMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info.ID, nil
}

func (mw *MetaWrapper) getExpiredMultipart(prefix string, days int, mp *MetaPartition) (status int, Infos []*proto.ExpiredMultipartInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getExpiredMultipart", err, bgTime, 1)
	}()

	req := &proto.GetExpiredMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Prefix:      prefix,
		Days:        days,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpGetExpiredMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("get session: err(%v)", err)
		return
	}

	log.LogDebugf("getExpiredMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetExpiredMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp.Infos, nil
}

func (mw *MetaWrapper) getMultipart(mp *MetaPartition, path, multipartId string) (status int, info *proto.MultipartInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getMultipart", err, bgTime, 1)
	}()

	req := &proto.GetMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpGetMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("get session: err(%v)", err)
		return
	}

	log.LogDebugf("getMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("getMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) addMultipartPart(mp *MetaPartition, path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (status int, oldNode uint64, updated bool, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("addMultipartPart", err, bgTime, 1)
	}()

	part := &proto.MultipartPartInfo{
		ID:         partId,
		Inode:      inodeInfo.Inode,
		MD5:        md5,
		Size:       size,
		UploadTime: time.Now(),
	}

	req := &proto.AddMultipartPartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
		Part:        part,
	}
	log.LogDebugf("addMultipartPart: part(%v), req(%v)", part, req)
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpAddMultipartPart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("addMultipartPart: marshal packet fail, err(%v)", err)
		return
	}

	log.LogDebugf("addMultipartPart entry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) err(%v)", packet, mp, req, part, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) result(%v)", packet, mp, *req, part, packet.GetResultMsg())
		return
	}
	resp := new(proto.AppendMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("appendMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return status, resp.OldInode, resp.Update, nil
}

func (mw *MetaWrapper) idelete(mp *MetaPartition, inode uint64, fullPath string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("idelete", err, bgTime, 1)
	}()

	req := &proto.DeleteInodeRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
	}
	req.FullPaths = []string{fullPath}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaDeleteInode
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("delete inode: err[%v]", err)
		return
	}
	log.LogDebugf("delete inode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(mp, packet)
	if err != nil {
		log.LogErrorf("delete inode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("idelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("idelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, inode)
	return statusOK, nil
}

func (mw *MetaWrapper) removeMultipart(mp *MetaPartition, path, multipartId string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("removeMultipart", err, bgTime, 1)
	}()

	req := &proto.RemoveMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpRemoveMultipart
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("delete session: err[%v]", err)
		return
	}
	log.LogDebugf("delete session: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("delete session: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("delete session: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("delete session: packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, packet.Data)
	return statusOK, nil
}

func (mw *MetaWrapper) appendExtentKeys(mp *MetaPartition, inode uint64, extents []proto.ExtentKey, storageClass uint32) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("appendExtentKeys", err, bgTime, 1)
	}()

	req := &proto.AppendExtentKeysRequest{
		VolName:      mw.volname,
		PartitionId:  mp.PartitionID,
		Inode:        inode,
		Extents:      extents,
		StorageClass: storageClass,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchExtentsAdd
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("batch append extent: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("appendExtentKeys: batch append extent: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batch append extent: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) appendObjExtentKeys(mp *MetaPartition, inode uint64, extents []proto.ObjExtentKey) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("appendObjExtentKeys", err, bgTime, 1)
	}()

	req := &proto.AppendObjExtentKeysRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Extents:     extents,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchObjExtentsAdd
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("batch append obj extents: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("appendObjExtentKeys: batch append obj extents: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batch append obj extents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batch append obj extents: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("batch append obj extents: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) batchSetXAttr(mp *MetaPartition, inode uint64, attrs map[string]string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("batchSetXAttr", err, bgTime, 1)
	}()

	req := &proto.BatchSetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Attrs:       make(map[string]string),
	}

	for key, val := range attrs {
		req.Attrs[key] = val
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchSetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("batchSetXAttr: matshal packet fail, err(%v)", err)
		return
	}
	log.LogDebugf("batchSetXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchSetXAttr: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batchSetXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("batchSetXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) setXAttr(mp *MetaPartition, inode uint64, name []byte, value []byte) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("setXAttr", err, bgTime, 1)
	}()

	req := &proto.SetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         string(name),
		Value:       string(value),
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaSetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("setXAttr: matshal packet fail, err(%v)", err)
		return
	}
	log.LogDebugf("setXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("setXAttr: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("setXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("setXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) getAllXAttr(mp *MetaPartition, inode uint64) (attrs map[string]string, status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getAllXAttr", err, bgTime, 1)
	}()

	req := &proto.GetAllXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaGetAllXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getAllXAttr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("getAllXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getAllXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("getAllXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetAllXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("get xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	attrs = resp.Attrs

	log.LogDebugf("getAllXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) getXAttr(mp *MetaPartition, inode uint64, name string) (value string, status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getXAttr", err, bgTime, 1)
	}()

	req := &proto.GetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         name,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaGetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("get xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("get xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	value = resp.Value

	log.LogDebugf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) removeXAttr(mp *MetaPartition, inode uint64, name string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("removeXAttr", err, bgTime, 1)
	}()

	req := &proto.RemoveXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         name,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaRemoveXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("remove xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	if packet, err = mw.sendToMetaPartition(mp, packet); err != nil {
		log.LogErrorf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listXAttr(mp *MetaPartition, inode uint64) (keys []string, status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("listXAttr", err, bgTime, 1)
	}()

	req := &proto.ListXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaListXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("list xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	if packet, err = mw.sendToMetaPartition(mp, packet); err != nil {
		log.LogErrorf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("list xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	keys = resp.XAttrs

	log.LogDebugf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listMultiparts(mp *MetaPartition, prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (status int, sessions *proto.ListMultipartResponse, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("listMultiparts", err, bgTime, 1)
	}()

	req := &proto.ListMultipartRequest{
		VolName:           mw.volname,
		PartitionId:       mp.PartitionID,
		Marker:            keyMarker,
		MultipartIdMarker: multipartIdMarker,
		Max:               maxUploads,
		Delimiter:         delimiter,
		Prefix:            prefix,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpListMultiparts
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("list sessions : err(%v)", err)
		return
	}

	log.LogDebugf("listMultiparts enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("listMultiparts: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp, nil
}

func (mw *MetaWrapper) batchGetXAttr(mp *MetaPartition, inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
	var err error

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("batchGetXAttr", err, bgTime, 1)
	}()

	req := &proto.BatchGetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inodes:      inodes,
		Keys:        keys,
		VerSeq:      mw.VerReadSeq,
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchGetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return nil, err
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchGetXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return nil, err
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return nil, err
	}

	resp := new(proto.BatchGetXAttrResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return nil, err
	}

	return resp.XAttrs, nil
}

func (mw *MetaWrapper) batchSetInodeQuota(mp *MetaPartition, inodes []uint64, quotaId uint32,
	IsRoot bool,
) (resp *proto.BatchSetMetaserverQuotaResponse, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("batchSetInodeQuota", err, bgTime, 1)
	}()

	req := &proto.BatchSetMetaserverQuotaReuqest{
		PartitionId: mp.PartitionID,
		Inodes:      inodes,
		QuotaId:     quotaId,
		IsRoot:      IsRoot,
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchSetInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("batchSetInodeQuota MarshalData req [%v] fail.", req)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp = new(proto.BatchSetMetaserverQuotaResponse)
	resp.InodeRes = make(map[uint64]uint8)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	log.LogInfof("batchSetInodeQuota inodes [%v] quota [%v] resp [%v] success.", inodes, quotaId, resp)
	return
}

func (mw *MetaWrapper) batchDeleteInodeQuota(mp *MetaPartition, inodes []uint64,
	quotaId uint32,
) (resp *proto.BatchDeleteMetaserverQuotaResponse, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("batchDeleteInodeQuota", err, bgTime, 1)
	}()
	req := &proto.BatchDeleteMetaserverQuotaReuqest{
		PartitionId: mp.PartitionID,
		Inodes:      inodes,
		QuotaId:     quotaId,
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaBatchDeleteInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("batchDeleteInodeQuota MarshalData req [%v] fail.", req)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchDeleteInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("batchDeleteInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp = new(proto.BatchDeleteMetaserverQuotaResponse)
	resp.InodeRes = make(map[uint64]uint8)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	log.LogInfof("batchDeleteInodeQuota inodes [%v] quota [%v] resp [%v] success.",
		inodes, quotaId, resp)
	return
}

func (mw *MetaWrapper) getInodeQuota(mp *MetaPartition, inode uint64) (quotaInfos map[uint32]*proto.MetaQuotaInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getInodeQuota", err, bgTime, 1)
	}()

	req := &proto.GetInodeQuotaRequest{
		PartitionId: mp.PartitionID,
		Inode:       inode,
	}
	qcInfo := mw.qc.Get(inode)
	if qcInfo != nil {
		return qcInfo.quotaInfos, nil
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaGetInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getInodeQuota: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("getInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetInodeQuotaResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogErrorf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	quotaInfos = resp.MetaQuotaInfoMap
	var qinfo QuotaCacheInfo
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos = quotaInfos
	qinfo.inode = inode
	mw.qc.Put(inode, &qinfo)
	log.LogDebugf("getInodeQuota: req(%v) resp(%v) err(%v)", *req, *resp, err)
	return
}

func (mw *MetaWrapper) applyQuota(parentIno uint64, quotaId uint32, totalInodeCount *uint64, curInodeCount *uint64, inodes *[]uint64,
	maxInodes uint64, first bool,
) (err error) {
	if first {
		var rootInodes []uint64
		var ret map[uint64]uint8
		rootInodes = append(rootInodes, parentIno)
		ret, err = mw.BatchSetInodeQuota_ll(rootInodes, quotaId, true)
		if err != nil {
			return
		}
		if status, ok := ret[parentIno]; ok {
			if status != proto.OpOk {
				if status == proto.OpNotExistErr {
					err = fmt.Errorf("apply inode %v is not exist.", parentIno)
				} else {
					err = fmt.Errorf("apply inode %v failed, status: %v.", parentIno, status)
				}

				return
			}
		}
		*totalInodeCount = *totalInodeCount + 1
	}
	var defaultReaddirLimit uint64 = 1024
	noMore := false
	from := ""
	for !noMore {
		entries, err := mw.ReadDirLimit_ll(parentIno, from, defaultReaddirLimit)
		if err != nil {
			return err
		}
		entryNum := uint64(len(entries))
		if entryNum == 0 || (from != "" && entryNum == 1) {
			break
		}

		if entryNum < defaultReaddirLimit {
			noMore = true
		}

		if from != "" {
			entries = entries[1:]
		}

		for _, entry := range entries {
			*inodes = append(*inodes, entry.Inode)
			*curInodeCount = *curInodeCount + 1
			*totalInodeCount = *totalInodeCount + 1
			if *curInodeCount >= maxInodes {
				mw.BatchSetInodeQuota_ll(*inodes, quotaId, false)
				*curInodeCount = 0
				*inodes = (*inodes)[:0]
			}
			if proto.IsDir(entry.Type) {
				err = mw.applyQuota(entry.Inode, quotaId, totalInodeCount, curInodeCount, inodes, maxInodes, false)
				if err != nil {
					return err
				}
			}
		}
		from = entries[len(entries)-1].Name
	}

	if first && *curInodeCount > 0 {
		mw.BatchSetInodeQuota_ll(*inodes, quotaId, false)
		*curInodeCount = 0
		*inodes = (*inodes)[:0]
	}
	return
}

func (mw *MetaWrapper) revokeQuota(parentIno uint64, quotaId uint32, totalInodeCount *uint64, curInodeCount *uint64, inodes *[]uint64,
	maxInodes uint64, first bool,
) (err error) {
	if first {
		var rootInodes []uint64
		rootInodes = append(rootInodes, parentIno)
		_, err = mw.BatchDeleteInodeQuota_ll(rootInodes, quotaId)
		if err != nil {
			return
		}
		*totalInodeCount = *totalInodeCount + 1
	}

	var defaultReaddirLimit uint64 = 1024
	noMore := false
	from := ""
	for !noMore {
		entries, err := mw.ReadDirLimit_ll(parentIno, from, defaultReaddirLimit)
		if err != nil {
			return err
		}
		entryNum := uint64(len(entries))
		if entryNum == 0 || (from != "" && entryNum == 1) {
			break
		}

		if entryNum < defaultReaddirLimit {
			noMore = true
		}

		if from != "" {
			entries = entries[1:]
		}

		for _, entry := range entries {
			*inodes = append(*inodes, entry.Inode)
			*curInodeCount = *curInodeCount + 1
			*totalInodeCount = *totalInodeCount + 1
			if *curInodeCount >= maxInodes {
				mw.BatchDeleteInodeQuota_ll(*inodes, quotaId)
				*curInodeCount = 0
				*inodes = (*inodes)[:0]
			}
			if proto.IsDir(entry.Type) {
				err = mw.revokeQuota(entry.Inode, quotaId, totalInodeCount, curInodeCount, inodes, maxInodes, false)
				if err != nil {
					return err
				}
			}
		}
		from = entries[len(entries)-1].Name
	}

	if first && *curInodeCount > 0 {
		mw.BatchDeleteInodeQuota_ll(*inodes, quotaId)
		*curInodeCount = 0
		*inodes = (*inodes)[:0]
	}
	return
}

func (mw *MetaWrapper) consumeUniqID(mp *MetaPartition) (status int, uniqid uint64, err error) {
	pid := mp.PartitionID
	mw.uniqidRangeMutex.Lock()
	defer mw.uniqidRangeMutex.Unlock()
	id, ok := mw.uniqidRangeMap[pid]
	if ok {
		if id.cur < id.end {
			status = statusOK
			uniqid = id.cur
			id.cur = id.cur + 1
			return
		}
	}
	status, start, err := mw.getUniqID(mp, maxUniqID)
	if err != nil || status != statusOK {
		return status, 0, err
	}
	uniqid = start
	if ok {
		id.cur = start + 1
		id.end = start + maxUniqID
	} else {
		mw.uniqidRangeMap[pid] = &uniqidRange{start + 1, start + maxUniqID}
	}
	return
}

func (mw *MetaWrapper) getUniqID(mp *MetaPartition, num uint32) (status int, start uint64, err error) {
	req := &proto.GetUniqIDRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Num:         num,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaGetUniqID
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getUniqID: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("getUniqID: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetUniqIDResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getUniqID: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	start = resp.Start
	return
}

func (mw *MetaWrapper) checkVerFromMeta(packet *proto.Packet) {
	if packet.VerSeq <= mw.Client.GetLatestVer() {
		return
	}

	log.LogDebugf("checkVerFromMeta.UpdateLatestVer.try update meta wrapper verSeq from %v to %v verlist[%v]", mw.Client.GetLatestVer(), packet.VerSeq, packet.VerList)
	mw.Client.UpdateLatestVer(&proto.VolVersionInfoList{VerList: packet.VerList})
}

func (mw *MetaWrapper) lockDir(mp *MetaPartition, inode uint64, lease uint64, lockId int64) (retLockId int64, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("lockDir", err, bgTime, 1)
	}()

	if lockId == 0 && lease != 0 {
		status, uniqId, err1 := mw.consumeUniqID(mp)
		if err1 != nil || status != statusOK {
			log.LogErrorf("lockDir: get uniq lockId failed, ino %d, status %d, err %v", inode, status, err1)
			err = statusErrToErrno(status, err1)
			return
		}
		lockId = int64(uniqId)
		log.LogDebugf("lockDir: get lockId success, id %d, ino %d", lockId, inode)
	}

	req := &proto.LockDirRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		LockId:      lockId,
		Lease:       lease,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaLockDir
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("lockDir: matshal packet fail, err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("lockDir: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = statusToErrno(status)
		log.LogWarnf("lockDir: received fail status, packet(%v) mp(%v) req(%v) result(%v) err(%v)", packet, mp, *req, packet.GetResultMsg(), err)
		return
	}

	retLockId = lockId
	log.LogDebugf("lockDir: packet(%v) mp(%v) req(%v) lockId(%v)", packet, mp, *req, lockId)
	return
}

func (mw *MetaWrapper) inodeAccessTimeGet(mp *MetaPartition, inode uint64) (status int, info *proto.InodeAccessTime, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("inodeAccessTimeGet", err, bgTime, 1)
	}()

	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaInodeAccessTimeGet
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("inodeAccessTimeGet: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("inodeAccessTimeGet: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("inodeAccessTimeGet: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp := new(proto.InodeGetAccessTimeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		log.LogErrorf("inodeAccessTimeGet: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) renewalForbiddenMigration(mp *MetaPartition, inode uint64) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("renewalForbiddenMigration", err, bgTime, 1)
	}()

	req := &proto.RenewalForbiddenMigrationRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaRenewalForbiddenMigration
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("renewalForbiddenMigration: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("renewalForbiddenMigration: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("renewalForbiddenMigration: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("renewalForbiddenMigration exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) updateExtentKeyAfterMigration(mp *MetaPartition, inode uint64, storageType uint32,
	extentKeys []proto.ObjExtentKey, leaseExpire uint64, delayDelMinute uint64, fullPath string,
) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("updateExtentKeyAfterMigration", err, bgTime, 1)
	}()
	req := &proto.UpdateExtentKeyAfterMigrationRequest{
		PartitionID:      mp.PartitionID,
		Inode:            inode,
		StorageClass:     storageType,
		NewObjExtentKeys: extentKeys,
		LeaseExpire:      leaseExpire,
	}
	req.DelayDeleteMinute = delayDelMinute
	req.FullPaths = []string{fullPath}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaUpdateExtentKeyAfterMigration
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		err = fmt.Errorf("lcNode marshal request err(%v)", err)
		log.LogErrorf("updateExtentKeyAfterMigration: ino(%v) %v", inode, err)
		return
	}

	log.LogDebugf("updateExtentKeyAfterMigration enter: packet(%v) mp(%v) req(%v)",
		packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		err = fmt.Errorf("sendToMetaPartition err(%v)", err)
		log.LogErrorf("updateExtentKeyAfterMigration: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = fmt.Errorf("%v", string(packet.Data))
		errMsg := fmt.Sprintf("updateExtentKeyAfterMigration: packet(%v) mp(%v) req(%v): %v",
			packet, mp, *req, err.Error())
		if status == statusLeaseOccupiedByOthers || status == statusLeaseGenerationNotMatch {
			log.LogWarnf(errMsg)
		} else {
			log.LogError(errMsg)
		}
		return
	}

	log.LogDebugf("updateExtentKeyAfterMigration exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) deleteMigrationExtentKey(mp *MetaPartition, inode uint64, fullPath string) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("deleteMigrationExtentKey", err, bgTime, 1)
	}()
	req := &proto.DeleteMigrationExtentKeyRequest{
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}
	req.FullPaths = []string{fullPath}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpDeleteMigrationExtentKey
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("deleteMigrationExtentKey: ino(%v) err(%v)", inode, err)
		return
	}

	log.LogDebugf("deleteMigrationExtentKey enter: packet(%v) mp(%v) req(%v)",
		packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("deleteMigrationExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("deleteMigrationExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("deleteMigrationExtentKey exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) forbiddenMigration(mp *MetaPartition, inode uint64) (status int, err error) {
	return mw.renewalForbiddenMigration(mp, inode)
}

func (mw *MetaWrapper) UpdateInodeMeta(ino uint64) (err error) {
	mp := mw.getPartitionByInode(ino)
	req := &proto.UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.PartitionID,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaUpdateInodeMeta
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("UpdateInodeMeta: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = fmt.Errorf("UpdateInodeMeta: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	return err
}
