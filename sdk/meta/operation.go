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
	"context"
	"fmt"
	"strconv"
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
func (mw *MetaWrapper) txIcreate(ctx context.Context, tx *Transaction, mp *MetaPartition, mode, uid, gid uint32,
	target []byte, quotaIds []uint32, fullPath string) (status int, info *proto.InodeInfo, err error) {
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
	}
	req.FullPaths = []string{fullPath}

	resp := new(proto.TxCreateInodeResponse)
	defer func() {
		tx.OnExecuted(status, resp.TxInfo)
	}()

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaTxCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("txIcreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("txIcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		// set tx error msg
		err = errors.New(packet.GetResultMsg())
		span.Errorf("txIcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("txIcreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}

	if resp.Info == nil || resp.TxInfo == nil {
		err = errors.New(fmt.Sprintf("txIcreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		span.Warn(err)
		return
	}

	tx.Started = true
	tx.txInfo = resp.TxInfo
	span.Debugf("txIcreate: packet(%v) mp(%v) req(%v) info(%v) tx(%v)", packet, mp, *req, resp.Info, resp.TxInfo)
	return status, resp.Info, nil
}

func (mw *MetaWrapper) quotaIcreate(ctx context.Context, mp *MetaPartition, mode, uid, gid uint32, target []byte, quotaIds []uint32, fullPath string) (status int,
	info *proto.InodeInfo, err error) {
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
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpQuotaCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("quotaIcreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("quotaIcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("quotaIcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("quotaIcreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("quotaIcreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		span.Warn(err)
		return
	}
	span.Debugf("quotaIcreate: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) icreate(ctx context.Context, mp *MetaPartition, mode, uid, gid uint32, target []byte, fullPath string) (status int,
	info *proto.InodeInfo, err error) {
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
	}
	req.FullPaths = []string{fullPath}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("icreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("icreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("icreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("icreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("icreate: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		span.Warn(err)
		return
	}
	span.Debugf("icreate: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) sendToMetaPartitionWithTx(ctx context.Context, mp *MetaPartition, req *proto.Packet) (packet *proto.Packet, err error) {
	span := proto.SpanFromContext(ctx)
	retryNum := int64(0)
	for {
		packet, err = mw.sendToMetaPartition(mp, req)
		if err != nil {
			span.Errorf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) err(%v)",
				string(req.Data), mp, req.GetOpMsg(), err)
			return
		}

		if packet.ResultCode != proto.OpTxConflictErr {
			break
		}

		span.Warnf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) result(%v), tx conflict retry: %v req(%v)",
			packet, mp, packet.GetOpMsg(), packet.GetResultMsg(), retryNum, string(req.Data))
		retryNum++
		if retryNum > mw.TxConflictRetryNum {
			span.Errorf("sendToMetaPartitionWithTx: packet(%v) mp(%v) reqType(%v) result(%v), tx conflict retry: %v req(%v)",
				packet, mp, packet.GetOpMsg(), packet.GetResultMsg(), retryNum, string(req.Data))
			break
		}
		time.Sleep(time.Duration(mw.TxConflictRetryInterval) * time.Millisecond)
	}

	return
}

func (mw *MetaWrapper) SendTxPack(ctx context.Context, req proto.TxPack, resp interface{}, Opcode uint8, mp *MetaPartition,
	checkStatusFunc func(int, *proto.Packet) error) (status int, err error, packet *proto.Packet) {
	packet = proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = Opcode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("SendTxPack reqType(%v) txInfo(%v) : err(%v)", packet.GetOpMsg(), req.GetInfo(), err)
		return
	}

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("SendTxPack: packet(%v) mp(%v) txInfo(%v) err(%v)",
			packet, mp, req.GetInfo(), err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if checkStatusFunc != nil {
		if err = checkStatusFunc(status, packet); err != nil {
			span.Errorf("SendTxPack: packet(%v) mp(%v) req(%v) txInfo(%v) result(%v) err(%v)",
				packet, mp, packet.GetOpMsg(), req.GetInfo(), packet.GetResultMsg(), err)
			return
		}
	} else if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("SendTxPack: packet(%v) mp(%v) req(%v) txInfo(%v) result(%v)",
			packet, mp, packet.GetOpMsg(), req.GetInfo(), packet.GetResultMsg())
		return
	}

	if resp == nil {
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("SendTxPack: packet(%v) mp(%v) txInfo(%v) err(%v) PacketData(%v)",
			packet, mp, req.GetInfo(), err, string(packet.Data))
		return
	}
	return
}

func (mw *MetaWrapper) txIunlink(ctx context.Context, tx *Transaction, mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	span := proto.SpanFromContext(ctx)
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
	if status, err, packet = mw.SendTxPack(ctx, req, resp, proto.OpMetaTxUnlinkInode, mp, nil); err != nil {
		span.Errorf("txIunlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("txIunlink: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) iunlink(ctx context.Context, mp *MetaPartition, inode uint64, verSeq uint64, denVerSeq uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("iunlink", err, bgTime, 1)
	}()

	// use uniq id to dedup request
	status, uniqID, err := mw.consumeUniqID(ctx, mp)
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaUnlinkInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("iunlink: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("iunlink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("iunlink: packet(%v) mp(%v) req(%v) result(%v) status(%v)", packet, mp, *req, packet.GetResultMsg(), status)
		return
	}

	resp := new(proto.UnlinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("iunlink: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	span.Debugf("iunlink: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) iclearCache(ctx context.Context, mp *MetaPartition, inode uint64) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaClearInodeCache
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("iclearCache: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("iclearCache: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("iclearCache: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("iclearCache: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return status, nil
}

func (mw *MetaWrapper) ievict(ctx context.Context, mp *MetaPartition, inode uint64, fullPath string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaEvictInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Warnf("ievict: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Warnf("ievict: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Warnf("ievict: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("ievict exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) txDcreate(ctx context.Context, tx *Transaction, mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32, quotaIds []uint32, fullPath string) (status int, err error) {
	span := proto.SpanFromContext(ctx)
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

	//statusCheckFunc := func(status int, packet *proto.Packet) (err error) {
	//	if (status != statusOK) && (status != statusExist) {
	//		err = errors.New(packet.GetResultMsg())
	//		span.Errorf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	//		return
	//	} else if status == statusExist {
	//		span.Warnf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	//	}
	//	return
	//}

	var packet *proto.Packet
	if status, err, packet = mw.SendTxPack(ctx, req, nil, proto.OpMetaTxCreateDentry, mp, nil); err != nil {
		span.Errorf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("txDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) quotaDcreate(ctx context.Context, mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32,
	quotaIds []uint32, fullPath string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpQuotaCreateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("quotaDcreate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("quotaDcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if (status != statusOK) && (status != statusExist) {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	} else if status == statusExist {
		span.Warnf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	span.Debugf("quotaDcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) dcreate(ctx context.Context, mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32, fullPath string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaCreateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("dcreate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("dcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if (status != statusOK) && (status != statusExist) {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	} else if status == statusExist {
		span.Warnf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	span.Debugf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) txDupdate(ctx context.Context, tx *Transaction, mp *MetaPartition, parentID uint64, name string, newInode, oldIno uint64, fullPath string) (status int, oldInode uint64, err error) {
	span := proto.SpanFromContext(ctx)
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
	if status, err, packet = mw.SendTxPack(ctx, req, resp, proto.OpMetaTxUpdateDentry, mp, nil); err != nil {
		span.Errorf("txDupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("txDupdate: packet(%v) mp(%v) req(%v) oldIno(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) dupdate(ctx context.Context, mp *MetaPartition, parentID uint64, name string, newInode uint64, fullPath string) (status int, oldInode uint64, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaUpdateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("dupdate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("dupdate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("dupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.UpdateDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("dupdate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("dupdate: packet(%v) mp(%v) req(%v) oldIno(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) txCreateTX(ctx context.Context, tx *Transaction, mp *MetaPartition) (status int, err error) {
	span := proto.SpanFromContext(ctx)
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
	if status, err, packet = mw.SendTxPack(ctx, req, resp, proto.OpMetaTxCreate, mp, nil); err != nil {
		span.Errorf("txCreateTX: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	if resp.TxInfo == nil {
		err = fmt.Errorf("txCreateTX: create tx resp nil")
		span.Error(err)
		return statusError, err
	}

	span.Debugf("txCreateTX: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	tx.txInfo = resp.TxInfo
	tx.Started = true
	return statusOK, nil
}

//func (mw *MetaWrapper) txPreCommit(tx *Transaction, mp *MetaPartition) (status int, err error) {
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
//		span.Errorf("txPreCommit: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
//		return
//	}
//
//	if span.EnableDebug() {
//		span.Debugf("txPreCommit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
//	}
//
//	return statusOK, nil
//}

func (mw *MetaWrapper) txDdelete(ctx context.Context, tx *Transaction, mp *MetaPartition, parentID, ino uint64, name string, fullPath string) (status int, inode uint64, err error) {
	span := proto.SpanFromContext(ctx)
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
	if status, err, packet = mw.SendTxPack(ctx, req, resp, proto.OpMetaTxDeleteDentry, mp, nil); err != nil {
		span.Errorf("txDdelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("txDdelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) ddelete(ctx context.Context, mp *MetaPartition, parentID uint64, name string, inodeCreateTime int64, verSeq uint64, fullPath string) (status int, inode uint64, denVer uint64, err error) {
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
	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	span.Debugf("action[ddelete] %v", req)
	packet.Opcode = proto.OpMetaDeleteDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("ddelete: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("ddelete: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("ddelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("ddelete: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("ddelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, packet.VerSeq, nil
}

func (mw *MetaWrapper) canDeleteInode(ctx context.Context, mp *MetaPartition, info *proto.InodeInfo, ino uint64) (can bool, err error) {
	span := proto.SpanFromContext(ctx)
	createTime := info.CreateTime.Unix()
	deleteLockTime := mw.volDeleteLockTime * 60 * 60

	if deleteLockTime > 0 && createTime+deleteLockTime > time.Now().Unix() {
		err = errors.NewErrorf("the current Inode[%v] is still locked for deletion", ino)
		span.Warnf("canDeleteInode: mp(%v) ino(%v) err(%v)", mp, ino, err)
		return false, syscall.EPERM
	}

	return true, nil
}

func (mw *MetaWrapper) ddeletes(ctx context.Context, mp *MetaPartition, parentID uint64, dentries []proto.Dentry, fullPaths []string) (status int,
	resp *proto.BatchDeleteDentryResponse, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchDeleteDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("ddeletes: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("ddeletes: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusAgain {
		err = errors.New("conflict request")
		span.Errorf("ddeletes: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("ddeletes: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp = new(proto.BatchDeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("ddeletes: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("ddeletes: packet(%v) mp(%v) req(%v) (%v)", packet, mp, *req, resp.Items)
	return statusOK, resp, nil
}

func (mw *MetaWrapper) lookup(ctx context.Context, mp *MetaPartition, parentID uint64, name string, verSeq uint64) (status int, inode uint64, mode uint32, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaLookup
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("lookup: err(%v)", err)
		return
	}

	span.Debugf("lookup enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("lookup: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		errMetric := exporter.NewCounter("fileOpenFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			err = errors.New(packet.GetResultMsg())
			span.Errorf("lookup: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
			errMetric := exporter.NewCounter("fileOpenFailed")
			errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		} else {
			span.Debugf("lookup exit: packet(%v) mp(%v) req(%v) NoEntry", packet, mp, *req)
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("lookup: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		errMetric := exporter.NewCounter("fileOpenFailed")
		errMetric.AddWithLabels(1, map[string]string{exporter.Vol: mw.volname, exporter.Err: "EIO"})
		return
	}
	span.Debugf("lookup exit: packet(%v) mp(%v) req(%v) ino(%v) mode(%v)", packet, mp, *req, resp.Inode, resp.Mode)
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(ctx context.Context, mp *MetaPartition, inode uint64, verSeq uint64) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("iget", err, bgTime, 1)
	}()

	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		VerSeq:      verSeq,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaInodeGet
	packet.PartitionID = mp.PartitionID

	span.Debugf("action[iget] pack mp id %v, req %v", mp.PartitionID, req)

	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("iget: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("iget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("iget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		span.Errorf("iget: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) batchIget(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64, respCh chan []*proto.InodeInfo) {
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
	}
	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	span.Debugf("action[batchIget] req %v", req)
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
		span.Errorf("batchIget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.BatchInodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("action[batchIget] resp %v", resp)
	if len(resp.Infos) == 0 {
		return
	}

	select {
	case respCh <- resp.Infos:
	default:
	}
}

func (mw *MetaWrapper) readDir(ctx context.Context, mp *MetaPartition, parentID uint64) (status int, children []proto.Dentry, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("readDir", err, bgTime, 1)
	}()

	req := &proto.ReadDirRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaReadDir
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("readDir: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("readDir: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		children = make([]proto.Dentry, 0)
		span.Errorf("readDir: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("readDir: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("readDir: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Children, nil
}

// read limit dentries start from
func (mw *MetaWrapper) readDirLimit(ctx context.Context, mp *MetaPartition, parentID uint64, from string, limit uint64, verSeq uint64, verOpt uint8) (status int, children []proto.Dentry, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaReadDirLimit
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("readDirLimit: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("action[readDirLimit] mp [%v] parentId %v", mp.PartitionID, parentID)
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("readDirLimit: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		span.Errorf("readDirLimit: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ReadDirLimitResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("readDirLimit: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("readDirLimit: packet(%v) mp(%v) req(%v) rsp(%v)", packet, mp, *req, resp.Children)
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(ctx context.Context, mp *MetaPartition, inode uint64, extent proto.ExtentKey, discard []proto.ExtentKey, isSplit bool) (status int, err error) {
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
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaExtentAddWithCheck
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("appendExtentKey: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("appendExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		if status != StatusConflictExtents {
			span.Errorf("appendExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		}
	}
	return status, err
}

func (mw *MetaWrapper) getExtents(ctx context.Context, mp *MetaPartition, inode uint64) (resp *proto.GetExtentsResponse, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getExtents", err, bgTime, 1)
	}()

	req := &proto.GetExtentsRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("getExtents: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getExtents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}
	resp = &proto.GetExtentsResponse{}
	resp.Status = parseStatus(packet.ResultCode)
	if resp.Status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("getExtents: packet(%v) mp(%v) result(%v)", packet, mp, packet.GetResultMsg())
		return
	}

	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("getExtents: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	return resp, nil
}

func (mw *MetaWrapper) getObjExtents(ctx context.Context, mp *MetaPartition, inode uint64) (status int, gen, size uint64, extents []proto.ExtentKey, objExtents []proto.ObjExtentKey, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaObjExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("getObjExtents: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getObjExtents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		extents = make([]proto.ExtentKey, 0)
		span.Errorf("getObjExtents: packet(%v) mp(%v) result(%v)", packet, mp, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetObjExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("getObjExtents: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
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
// 		span.Errorf("delExtentKey: req(%v) err(%v)", *req, err)
// 		return
// 	}

// 	metric := exporter.NewTPCnt(packet.GetOpMsg())
// 	defer func() {
// 		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
// 	}()

// 	packet, err = mw.sendToMetaPartition(mp, packet)
// 	if err != nil {
// 		span.Errorf("delExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
// 		return
// 	}

// 	status = parseStatus(packet.ResultCode)
// 	if status != statusOK {
// 		span.Errorf("delExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
// 	}
// 	return status, nil
// }

func (mw *MetaWrapper) truncate(ctx context.Context, mp *MetaPartition, inode, size uint64, fullPath string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaTruncate
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("truncate: ino(%v) size(%v) err(%v)", inode, size, err)
		return
	}

	span.Debugf("truncate enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("truncate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("truncate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("truncate exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) txIlink(ctx context.Context, tx *Transaction, mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	span := proto.SpanFromContext(ctx)
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
	if status, err, packet = mw.SendTxPack(ctx, req, resp, proto.OpMetaTxLinkInode, mp, nil); err != nil {
		span.Errorf("txIlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("txIlink exit: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) ilink(ctx context.Context, mp *MetaPartition, inode uint64, fullPath string) (status int, info *proto.InodeInfo, err error) {
	return mw.ilinkWork(ctx, mp, inode, proto.OpMetaLinkInode, fullPath)
}

func (mw *MetaWrapper) ilinkWork(ctx context.Context, mp *MetaPartition, inode uint64, op uint8, fullPath string) (status int, info *proto.InodeInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ilink", err, bgTime, 1)
	}()

	// use unique id to dedup request
	status, uniqID, err := mw.consumeUniqID(ctx, mp)
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = op
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("ilink: req(%v) err(%v)", *req, err)
		return
	}

	span.Debugf("ilink enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("ilink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("ilink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.LinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("ilink: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("ilink: info is nil, packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, string(packet.Data)))
		span.Warn(err)
		return
	}
	span.Debugf("ilink exit: packet(%v) mp(%v) req(%v) info(%v)", packet, mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) setattr(ctx context.Context, mp *MetaPartition, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaSetattr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("setattr: err(%v)", err)
		return
	}

	span.Debugf("setattr enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("setattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("setattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("setattr exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) createMultipart(ctx context.Context, mp *MetaPartition, path string, extend map[string]string) (status int, multipartId string, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpCreateMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("createMultipart: err(%v)", err)
		return
	}

	span.Debugf("createMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("createMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("createMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("createMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info.ID, nil
}

func (mw *MetaWrapper) getExpiredMultipart(ctx context.Context, prefix string, days int, mp *MetaPartition) (status int, Infos []*proto.ExpiredMultipartInfo, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpGetExpiredMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("get session: err(%v)", err)
		return
	}

	span.Debugf("getExpiredMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetExpiredMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("getExpiredMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp.Infos, nil
}

func (mw *MetaWrapper) getMultipart(ctx context.Context, mp *MetaPartition, path, multipartId string) (status int, info *proto.MultipartInfo, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpGetMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("get session: err(%v)", err)
		return
	}

	span.Debugf("getMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("getMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("getMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) addMultipartPart(ctx context.Context, mp *MetaPartition, path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (status int, oldNode uint64, updated bool, err error) {
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
	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	span.Debugf("addMultipartPart: part(%v), req(%v)", part, req)
	packet.Opcode = proto.OpAddMultipartPart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("addMultipartPart: marshal packet fail, err(%v)", err)
		return
	}

	span.Debugf("addMultipartPart entry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) err(%v)", packet, mp, req, part, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) result(%v)", packet, mp, *req, part, packet.GetResultMsg())
		return
	}
	resp := new(proto.AppendMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("appendMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return status, resp.OldInode, resp.Update, nil
}

func (mw *MetaWrapper) idelete(ctx context.Context, mp *MetaPartition, inode uint64, fullPath string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaDeleteInode
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		span.Errorf("delete inode: err[%v]", err)
		return
	}
	span.Debugf("delete inode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartitionWithTx(ctx, mp, packet)
	if err != nil {
		span.Errorf("delete inode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("idelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	span.Debugf("idelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, inode)
	return statusOK, nil
}

func (mw *MetaWrapper) removeMultipart(ctx context.Context, mp *MetaPartition, path, multipartId string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpRemoveMultipart
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		span.Errorf("delete session: err[%v]", err)
		return
	}
	span.Debugf("delete session: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("delete session: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("delete session: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	span.Debugf("delete session: packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, packet.Data)
	return statusOK, nil
}

func (mw *MetaWrapper) appendExtentKeys(ctx context.Context, mp *MetaPartition, inode uint64, extents []proto.ExtentKey) (status int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("appendExtentKeys", err, bgTime, 1)
	}()

	req := &proto.AppendExtentKeysRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Extents:     extents,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchExtentsAdd
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("batch append extent: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("appendExtentKeys: batch append extent: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("batch append extent: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) appendObjExtentKeys(ctx context.Context, mp *MetaPartition, inode uint64, extents []proto.ObjExtentKey) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchObjExtentsAdd
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("batch append obj extents: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("appendObjExtentKeys: batch append obj extents: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("batch append obj extents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batch append obj extents: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("batch append obj extents: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) batchSetXAttr(ctx context.Context, mp *MetaPartition, inode uint64, attrs map[string]string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchSetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("batchSetXAttr: matshal packet fail, err(%v)", err)
		return
	}
	span.Debugf("batchSetXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("batchSetXAttr: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batchSetXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("batchSetXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) setXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name []byte, value []byte) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaSetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("setXAttr: matshal packet fail, err(%v)", err)
		return
	}
	span.Debugf("setXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("setXAttr: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("setXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("setXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) getAllXAttr(ctx context.Context, mp *MetaPartition, inode uint64) (attrs map[string]string, status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaGetAllXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("getAllXAttr: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("getAllXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getAllXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("getAllXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetAllXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("get xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	attrs = resp.Attrs

	span.Debugf("getAllXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) getXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name string) (value string, status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaGetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("get xattr: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("get xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	value = resp.Value

	span.Debugf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) removeXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name string) (status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaRemoveXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		span.Errorf("remove xattr: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	if packet, err = mw.sendToMetaPartition(mp, packet); err != nil {
		span.Errorf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	span.Debugf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listXAttr(ctx context.Context, mp *MetaPartition, inode uint64) (keys []string, status int, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaListXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		span.Errorf("list xattr: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	if packet, err = mw.sendToMetaPartition(mp, packet); err != nil {
		span.Errorf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("list xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	keys = resp.XAttrs

	span.Debugf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listMultiparts(ctx context.Context, mp *MetaPartition, prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (status int, sessions *proto.ListMultipartResponse, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpListMultiparts
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("list sessions : err(%v)", err)
		return
	}

	span.Debugf("listMultiparts enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))
	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("listMultiparts: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp, nil
}

func (mw *MetaWrapper) batchGetXAttr(ctx context.Context, mp *MetaPartition, inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
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
		span.Errorf("batchGetXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return nil, err
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return nil, err
	}

	resp := new(proto.BatchGetXAttrResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return nil, err
	}

	return resp.XAttrs, nil
}

func (mw *MetaWrapper) readdironly(ctx context.Context, mp *MetaPartition, parentID uint64) (status int, children []proto.Dentry, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("readdironly", err, bgTime, 1)
	}()

	req := &proto.ReadDirOnlyRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		VerSeq:      mw.VerReadSeq,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaReadDirOnly
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("readDir: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("readDir: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		children = make([]proto.Dentry, 0)
		span.Errorf("readDir: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ReadDirOnlyResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("readDir: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	span.Debugf("readDir: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) updateXAttrs(ctx context.Context, mp *MetaPartition, inode uint64, filesInc int64, dirsInc int64, bytesInc int64) error {
	var err error

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("updateXAttrs", err, bgTime, 1)
	}()

	value := strconv.FormatInt(int64(filesInc), 10) + "," + strconv.FormatInt(int64(dirsInc), 10) + "," + strconv.FormatInt(int64(bytesInc), 10)
	req := &proto.UpdateXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         SummaryKey,
		Value:       value,
	}
	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaUpdateXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("updateXAttr: matshal packet fail, err(%v)", err)
		return err
	}
	span.Debugf("updateXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("readdironly: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return err
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("readdironly: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return err
	}

	span.Debugf("updateXAttrs: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return nil
}

func (mw *MetaWrapper) batchSetInodeQuota(ctx context.Context, mp *MetaPartition, inodes []uint64, quotaId uint32,
	IsRoot bool) (resp *proto.BatchSetMetaserverQuotaResponse, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchSetInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("batchSetInodeQuota MarshalData req [%v] fail.", req)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp = new(proto.BatchSetMetaserverQuotaResponse)
	resp.InodeRes = make(map[uint64]uint8, 0)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	span.Infof("batchSetInodeQuota inodes [%v] quota [%v] resp [%v] success.", inodes, quotaId, resp)
	return
}

func (mw *MetaWrapper) batchDeleteInodeQuota(ctx context.Context, mp *MetaPartition, inodes []uint64,
	quotaId uint32) (resp *proto.BatchDeleteMetaserverQuotaResponse, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaBatchDeleteInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("batchDeleteInodeQuota MarshalData req [%v] fail.", req)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("batchDeleteInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("batchDeleteInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	resp = new(proto.BatchDeleteMetaserverQuotaResponse)
	resp.InodeRes = make(map[uint64]uint8, 0)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("batchSetInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	span.Infof("batchDeleteInodeQuota inodes [%v] quota [%v] resp [%v] success.",
		inodes, quotaId, resp)
	return
}

func (mw *MetaWrapper) getInodeQuota(ctx context.Context, mp *MetaPartition, inode uint64) (quotaInfos map[uint32]*proto.MetaQuotaInfo, err error) {
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
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaGetInodeQuota
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("getInodeQuota: req(%v) err(%v)", *req, err)
		return
	}
	span.Debugf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("getInodeQuota: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetInodeQuotaResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		span.Errorf("getInodeQuota: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	quotaInfos = resp.MetaQuotaInfoMap
	var qinfo QuotaCacheInfo
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos = quotaInfos
	qinfo.inode = inode
	mw.qc.Put(inode, &qinfo)
	span.Debugf("getInodeQuota: req(%v) resp(%v) err(%v)", *req, *resp, err)
	return
}

func (mw *MetaWrapper) applyQuota(ctx context.Context, parentIno uint64, quotaId uint32, totalInodeCount *uint64, curInodeCount *uint64, inodes *[]uint64,
	maxInodes uint64, first bool) (err error) {
	if first {
		var rootInodes []uint64
		var ret map[uint64]uint8
		rootInodes = append(rootInodes, parentIno)
		ret, err = mw.BatchSetInodeQuota_ll(ctx, rootInodes, quotaId, true)
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
		entries, err := mw.ReadDirLimit_ll(ctx, parentIno, from, defaultReaddirLimit)
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
				mw.BatchSetInodeQuota_ll(ctx, *inodes, quotaId, false)
				*curInodeCount = 0
				*inodes = (*inodes)[:0]
			}
			if proto.IsDir(entry.Type) {
				err = mw.applyQuota(ctx, entry.Inode, quotaId, totalInodeCount, curInodeCount, inodes, maxInodes, false)
				if err != nil {
					return err
				}
			}
		}
		from = entries[len(entries)-1].Name
	}

	if first && *curInodeCount > 0 {
		mw.BatchSetInodeQuota_ll(ctx, *inodes, quotaId, false)
		*curInodeCount = 0
		*inodes = (*inodes)[:0]
	}
	return
}

func (mw *MetaWrapper) revokeQuota(ctx context.Context, parentIno uint64, quotaId uint32, totalInodeCount *uint64, curInodeCount *uint64, inodes *[]uint64,
	maxInodes uint64, first bool) (err error) {
	if first {
		var rootInodes []uint64
		rootInodes = append(rootInodes, parentIno)
		_, err = mw.BatchDeleteInodeQuota_ll(ctx, rootInodes, quotaId)
		if err != nil {
			return
		}
		*totalInodeCount = *totalInodeCount + 1
	}

	var defaultReaddirLimit uint64 = 1024
	noMore := false
	from := ""
	for !noMore {
		entries, err := mw.ReadDirLimit_ll(ctx, parentIno, from, defaultReaddirLimit)
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
				mw.BatchDeleteInodeQuota_ll(ctx, *inodes, quotaId)
				*curInodeCount = 0
				*inodes = (*inodes)[:0]
			}
			if proto.IsDir(entry.Type) {
				err = mw.revokeQuota(ctx, entry.Inode, quotaId, totalInodeCount, curInodeCount, inodes, maxInodes, false)
				if err != nil {
					return err
				}
			}
		}
		from = entries[len(entries)-1].Name
	}

	if first && *curInodeCount > 0 {
		mw.BatchDeleteInodeQuota_ll(ctx, *inodes, quotaId)
		*curInodeCount = 0
		*inodes = (*inodes)[:0]
	}
	return
}

func (mw *MetaWrapper) consumeUniqID(ctx context.Context, mp *MetaPartition) (status int, uniqid uint64, err error) {
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
	status, start, err := mw.getUniqID(ctx, mp, maxUniqID)
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

func (mw *MetaWrapper) getUniqID(ctx context.Context, mp *MetaPartition, num uint32) (status int, start uint64, err error) {
	req := &proto.GetUniqIDRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Num:         num,
	}

	packet := proto.NewPacketReqID()
	packet.ReqID = proto.RequestIDFromContext(ctx)
	span := packet.Span()
	packet.Opcode = proto.OpMetaGetUniqID
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		span.Errorf("getUniqID: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		span.Errorf("getUniqID: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetUniqIDResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		span.Errorf("getUniqID: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	start = resp.Start
	return
}

func (mw *MetaWrapper) checkVerFromMeta(packet *proto.Packet) {
	if packet.VerSeq <= mw.Client.GetLatestVer() {
		return
	}

	log.Debugf("checkVerFromMeta.UpdateLatestVer.try update meta wrapper verSeq from %v to %v verlist[%v]", mw.Client.GetLatestVer(), packet.VerSeq, packet.VerList)
	mw.Client.UpdateLatestVer(context.TODO(), &proto.VolVersionInfoList{VerList: packet.VerList})
}
