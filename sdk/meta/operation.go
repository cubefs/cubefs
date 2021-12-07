// Copyright 2018 The Chubao Authors.
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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"
)

// API implementations
//

func (mw *MetaWrapper) icreate(ctx context.Context, mp *MetaPartition, mode, uid, gid uint32, target []byte) (status int, info *proto.InodeInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.icreate")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.CreateInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Mode:        mode,
		Uid:         uid,
		Gid:         gid,
		Target:      target,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCreateInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("icreate: err(%v)", err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("icreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("icreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("icreate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
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

func (mw *MetaWrapper) iunlink(ctx context.Context, mp *MetaPartition, inode uint64,
	trashEnable bool) (status int, info *proto.InodeInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.iunlink")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.UnlinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		TrashEnable: trashEnable,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaUnlinkInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("iunlink: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("iunlink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("iunlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.iunlink(ctx, newMp, inode, trashEnable)
		}
	}
	if status != statusOK {
		log.LogWarnf("iunlink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.UnlinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("iunlink: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	log.LogDebugf("iunlink: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) ievict(ctx context.Context, mp *MetaPartition, inode uint64, trashEnable bool) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.ievict")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.EvictInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		TrashEnable: trashEnable,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaEvictInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("ievict: ino(%v) err(%v)", inode, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("ievict: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("ievict: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.ievict(ctx, newMp, inode, trashEnable)
		}
	}
	if status != statusOK {
		log.LogWarnf("ievict: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("ievict exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) dcreate(ctx context.Context, mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.dcreate")
	defer tracer.Finish()
	ctx = tracer.Context()

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

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCreateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("dcreate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	var needCheckRead bool
	packet, needCheckRead, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		if needCheckRead {
			log.LogWarnf("dcreate: check results, mp(%v) req(%v)", mp, *req)
			newStatus, newInode, newMode, newErr := mw.lookup(ctx, mp, parentID, name)
			if newErr == nil && newStatus == statusOK && newInode == inode && newMode == mode {
				log.LogWarnf("dcreate: check results successfully, mp(%v) req(%v)", mp, *req)
				return statusOK, nil
			}
		}
		log.LogWarnf("dcreate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, parentID)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.dcreate(ctx, newMp, parentID, name, inode, mode)
		}
	}
	if (status != statusOK) && (status != statusExist) {
		log.LogWarnf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		//} else if status == statusExist {
		//	log.LogWarnf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	log.LogDebugf("dcreate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) dupdate(ctx context.Context, mp *MetaPartition, parentID uint64, name string, newInode uint64) (status int, oldInode uint64, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.dupdate")
	defer tracer.Finish()
	ctx = tracer.Context()

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

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaUpdateDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("dupdate: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("dupdate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("dupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, parentID)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.dupdate(ctx, newMp, parentID, name, newInode)
		}
	}
	if status != statusOK {
		log.LogWarnf("dupdate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.UpdateDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("dupdate: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("dupdate: packet(%v) mp(%v) req(%v) oldIno(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) ddelete(ctx context.Context, mp *MetaPartition, parentID uint64, name string,
	trashEnable bool) (status int, inode uint64, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.ddelete")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.DeleteDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		TrashEnable: trashEnable,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaDeleteDentry
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("ddelete: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("ddelete: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("ddelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, parentID)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.ddelete(ctx, newMp, parentID, name, trashEnable)
		}
	}
	if status != statusOK {
		log.LogWarnf("ddelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("ddelete: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("ddelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) lookup(ctx context.Context, mp *MetaPartition, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.lookup")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.LookupRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
	}
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaLookup
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("lookup: err(%v)", err)
		return
	}

	log.LogDebugf("lookup enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("lookup: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("lookup: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, parentID)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.lookup(ctx, newMp, parentID, name)
		}
	}
	if status != statusOK {
		if status != statusNoent {
			log.LogWarnf("lookup: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		} else {
			log.LogDebugf("lookup exit: packet(%v) mp(%v) req(%v) NoEntry", packet, mp, *req)
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("lookup: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("lookup exit: packet(%v) mp(%v) req(%v) ino(%v) mode(%v)", packet, mp, *req, resp.Inode, resp.Mode)
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(ctx context.Context, mp *MetaPartition, inode uint64) (status int, info *proto.InodeInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.iget")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID(ctx)
	// add new opcode for 'InodeGet' to be compatible with old clients that can only judge 'statusNoent'
	//packet.Opcode = proto.OpMetaInodeGet
	packet.Opcode = proto.OpMetaInodeGetV2
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("iget: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("iget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("iget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.iget(ctx, newMp, inode)
		}
	}
	if status != statusOK {
		log.LogWarnf("iget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		log.LogWarnf("iget: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) batchIget(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64, respCh chan []*proto.InodeInfo) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchIget")
	defer tracer.Finish()
	ctx = tracer.Context()

	defer wg.Done()
	var (
		err error
	)
	req := &proto.BatchInodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inodes:      inodes,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchInodeGet
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("batchIget: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.BatchInodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}

	if len(resp.Infos) == 0 {
		return
	}

	select {
	case respCh <- resp.Infos:
	default:
	}
}

func (mw *MetaWrapper) readdir(ctx context.Context, mp *MetaPartition, parentID uint64) (status int, children []proto.Dentry, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.readdir")
	defer tracer.Finish()
	ctx = tracer.Context()

	marker := ""
	children = make([]proto.Dentry, 0)

	metric := exporter.NewTPCnt("OpMetaReadDir")
	defer metric.Set(err)

	for {
		req := &proto.ReadDirRequest{
			VolName:     mw.volname,
			PartitionID: mp.PartitionID,
			ParentID:    parentID,
			Marker:      marker,
			IsBatch:     true,
		}
		packet := proto.NewPacketReqID(ctx)
		packet.Opcode = proto.OpMetaReadDir
		packet.PartitionID = mp.PartitionID
		err = packet.MarshalData(req)
		if err != nil {
			log.LogWarnf("readdir: req(%v) err(%v)", *req, err)
			return
		}
		packet, err = mw.sendReadToMP(ctx, mp, packet)
		if err != nil {
			log.LogWarnf("readdir: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
			return
		}
		status = parseStatus(packet.ResultCode)
		if status == statusOutOfRange {
			log.LogWarnf("readdir: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
			newMp := mw.getRefreshMp(ctx, parentID)
			if newMp != nil && newMp.PartitionID != mp.PartitionID {
				return mw.readdir(ctx, newMp, parentID)
			}
		}
		if status != statusOK {
			log.LogWarnf("readdir: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
			return
		}
		resp := new(proto.ReadDirResponse)
		err = packet.UnmarshalData(resp)
		if err != nil {
			log.LogWarnf("readdir: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
			return
		}
		log.LogDebugf("readdir: packet(%v) mp(%v) req(%v) current dentry count(%v)", packet, mp, *req, len(resp.Children))
		children = append(children, resp.Children...)
		if resp.NextMarker == "" {
			break
		}
		marker = resp.NextMarker
	}
	return statusOK, children, nil
}

//func (mw *MetaWrapper) appendExtentKey(ctx context.Context, mp *MetaPartition, inode uint64, extent proto.ExtentKey) (status int, err error) {
//	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.appendExtentKey")
//	defer tracer.Finish()
//	ctx = tracer.Context()
//
//	req := &proto.AppendExtentKeyRequest{
//		VolName:     mw.volname,
//		PartitionID: mp.PartitionID,
//		Inode:       inode,
//		Extent:      extent,
//	}
//
//	packet := proto.NewPacketReqID(ctx)
//	packet.Opcode = proto.OpMetaExtentsAdd
//	packet.PartitionID = mp.PartitionID
//	err = packet.MarshalData(req)
//	if err != nil {
//		log.LogWarnf("appendExtentKey: req(%v) err(%v)", *req, err)
//		return
//	}
//
//	metric := exporter.NewTPCnt(packet.GetOpMsg())
//	defer metric.Set(err)
//
//	var needCheckRead bool
//	packet, needCheckRead, err = mw.sendWriteToMP(ctx, mp, packet)
//	if err != nil {
//		if needCheckRead {
//			log.LogWarnf("appendExtentKey: check results, mp(%v) req(%v)", mp, *req)
//			newStatus, _, _, newExtents, newErr := mw.getExtents(ctx, mp, inode)
//			if newErr == nil && newStatus == statusOK && containsExtent(newExtents, extent) {
//				log.LogWarnf("appendExtentKey: check results successfully, mp(%v) req(%v)", mp, *req)
//				return statusOK, nil
//			}
//		}
//		log.LogWarnf("appendExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
//		return
//	}
//
//	status = parseStatus(packet.ResultCode)
//	if status != statusOK {
//		log.LogWarnf("appendExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
//	}
//	return status, nil
//}

func (mw *MetaWrapper) insertExtentKey(ctx context.Context, mp *MetaPartition, inode uint64, ek proto.ExtentKey, isPreExtent bool) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.insertExtentKey").
		SetTag("mpID", mp.PartitionID).
		SetTag("inode", inode).
		SetTag("ek.FileOffset", ek.FileOffset).
		SetTag("ek.PartitionId", ek.PartitionId).
		SetTag("ek.ExtentId", ek.ExtentId).
		SetTag("ek.ExtentOffset", ek.ExtentOffset).
		SetTag("ek.Size", ek.Size)
	defer tracer.Finish()

	req := &proto.InsertExtentKeyRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Extent:      ek,
		IsPreExtent: isPreExtent,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaExtentsInsert
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("insertExtentKey: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	var needCheckRead bool
	packet, needCheckRead, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		if needCheckRead {
			log.LogWarnf("insertExtentKey: check results, mp(%v) req(%v)", mp, *req)
			newStatus, _, _, newExtents, newErr := mw.getExtents(ctx, mp, inode)
			if newErr == nil && newStatus == statusOK && containsExtent(newExtents, ek) {
				log.LogWarnf("insertExtentKey: check results successfully, mp(%v) req(%v)", mp, *req)
				return statusOK, nil
			}
		}
		log.LogWarnf("insertExtentKey: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("insertExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.insertExtentKey(ctx, newMp, inode, ek, isPreExtent)
		}
	}
	if status != statusOK {
		log.LogWarnf("insertExtentKey: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	return status, nil
}

func (mw *MetaWrapper) getExtents(ctx context.Context, mp *MetaPartition, inode uint64) (status int, gen, size uint64, extents []proto.ExtentKey, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.getExtents")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.GetExtentsRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("getExtents: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("getExtents: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("getExtents: packet(%v) mp(%v) inode(%v) result(%v)", packet, mp, inode, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.getExtents(ctx, newMp, inode)
		}
	}
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		log.LogWarnf("getExtents: packet(%v) mp(%v) result(%v)", packet, mp, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("getExtents: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	return statusOK, resp.Generation, resp.Size, resp.Extents, nil
}

func (mw *MetaWrapper) truncate(ctx context.Context, mp *MetaPartition, inode, oldSize, size uint64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.truncate")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.TruncateRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Size:        size,
		Version:     proto.TruncateRequestVersion_1,
		OldSize:     oldSize,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaTruncate
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("truncate: ino(%v) size(%v) err(%v)", inode, size, err)
		return
	}

	log.LogDebugf("truncate enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("truncate: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("truncate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.truncate(ctx, newMp, inode, oldSize, size)
		}
	}
	// truncate may recieve statusInval caused by repeat execution on metanode
	if status == statusInval {
		getStatus, getInfo, getErr := mw.iget(ctx, mp, inode)
		log.LogWarnf("truncate: truncate failed[packet(%v) mp(%v) req(%v)], but inode(%v) size correct",
			packet, mp, req, getInfo)
		if getErr == nil && getStatus == statusOK && getInfo.Size == size {
			log.LogWarnf("truncate: truncate failed[packet(%v) mp(%v) req(%v)], but inode(%v) size correct",
				packet, mp, req, getInfo)
			status = statusOK
		}
	}
	if status != statusOK {
		log.LogWarnf("truncate: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("truncate exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) ilink(ctx context.Context, mp *MetaPartition, inode uint64) (status int, info *proto.InodeInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.ilink")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.LinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaLinkInode
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("ilink: req(%v) err(%v)", *req, err)
		return
	}

	log.LogDebugf("ilink enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("ilink: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("ilink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.ilink(ctx, newMp, inode)
		}
	}
	if status != statusOK {
		log.LogWarnf("ilink: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.LinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("ilink: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
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

func (mw *MetaWrapper) setattr(ctx context.Context, mp *MetaPartition, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.setattr")
	defer tracer.Finish()
	ctx = tracer.Context()

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

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaSetattr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("setattr: err(%v)", err)
		return
	}

	log.LogDebugf("setattr enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("setattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("setattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.setattr(ctx, newMp, inode, valid, mode, uid, gid, atime, mtime)
		}
	}
	if status != statusOK {
		log.LogWarnf("setattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("setattr exit: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) createMultipart(ctx context.Context, mp *MetaPartition, path string, extend map[string]string) (status int, multipartId string, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.createMultipart")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.CreateMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		Extend:      extend,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpCreateMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("createMultipart: err(%v)", err)
		return
	}

	log.LogDebugf("createMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("createMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("createMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.CreateMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("createMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info.ID, nil
}

func (mw *MetaWrapper) getMultipart(ctx context.Context, mp *MetaPartition, path, multipartId string) (status int, info *proto.MultipartInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.getMultipart")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.GetMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpGetMultipart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("get session: err(%v)", err)
		return
	}

	log.LogDebugf("getMultipart enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("getMultipart: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("getMultipart: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("getMultipart: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) addMultipartPart(ctx context.Context, mp *MetaPartition, path, multipartId string, partId uint16, size uint64, md5 string, inode uint64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.addMultipartPart")
	defer tracer.Finish()
	ctx = tracer.Context()

	part := &proto.MultipartPartInfo{
		ID:    partId,
		Inode: inode,
		MD5:   md5,
		Size:  size,
	}

	req := &proto.AddMultipartPartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
		Part:        part,
	}
	log.LogDebugf("addMultipartPart: part(%v), req(%v)", part, req)
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpAddMultipartPart
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("addMultipartPart: marshal packet fail, err(%v)", err)
		return
	}

	log.LogDebugf("addMultipartPart entry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) err(%v)", packet, mp, req, part, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("addMultipartPart: packet(%v) mp(%v) req(%v) part(%v) result(%v)", packet, mp, *req, part, packet.GetResultMsg())
		return
	}

	return statusOK, nil
}

func (mw *MetaWrapper) idelete(ctx context.Context, mp *MetaPartition, inode uint64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.idelete")
	defer tracer.Finish()
	ctx = tracer.Context()
	req := &proto.DeleteInodeRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
	}
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaDeleteInode
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogWarnf("delete inode: err[%v]", err)
		return
	}
	log.LogDebugf("delete inode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("delete inode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("idelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.idelete(ctx, newMp, inode)
		}
	}
	if status != statusOK {
		log.LogWarnf("idelete: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("idelete: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, inode)
	return statusOK, nil
}

func (mw *MetaWrapper) removeMultipart(ctx context.Context, mp *MetaPartition, path, multipartId string) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.removeMultipart")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.RemoveMultipartRequest{
		PartitionId: mp.PartitionID,
		VolName:     mw.volname,
		Path:        path,
		MultipartId: multipartId,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpRemoveMultipart
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogWarnf("delete session: err[%v]", err)
		return
	}
	log.LogDebugf("delete session: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("delete session: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("delete session: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("delete session: packet(%v) mp(%v) req(%v) PacketData(%v)", packet, mp, *req, packet.Data)
	return statusOK, nil
}

func (mw *MetaWrapper) appendExtentKeys(ctx context.Context, mp *MetaPartition, inode uint64, extents []proto.ExtentKey) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.appendExtentKeys")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.AppendExtentKeysRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Extents:     extents,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchExtentsAdd
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("batch append extent: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("appendExtentKeys: batch append extent: packet(%v) mp(%v) req(%v)", packet, mp, *req)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("batch append extent: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.appendExtentKeys(ctx, newMp, inode, extents)
		}
	}
	if status != statusOK {
		log.LogWarnf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("batch append extent: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) setXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name []byte, value []byte) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.setXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.SetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         string(name),
		Value:       string(value),
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaSetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("setXAttr: matshal packet fail, err(%v)", err)
		return
	}
	log.LogDebugf("setXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("setXAttr: send to partition fail, packet(%v) mp(%v) req(%v) err(%v)",
			packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("setXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.setXAttr(ctx, newMp, inode, name, value)
		}
	}
	if status != statusOK {
		log.LogWarnf("setXAttr: received fail status, packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("setXAttr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) getXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name string) (value string, status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.getXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.GetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         name,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaGetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("get xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("get xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.getXAttr(ctx, newMp, inode, name)
		}
	}
	if status != statusOK {
		log.LogWarnf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogWarnf("get xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	value = resp.Value

	log.LogDebugf("get xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) removeXAttr(ctx context.Context, mp *MetaPartition, inode uint64, name string) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.removeXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.RemoveXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
		Key:         name,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaRemoveXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogWarnf("remove xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	if packet, _, err = mw.sendWriteToMP(ctx, mp, packet); err != nil {
		log.LogWarnf("remove xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.removeXAttr(ctx, newMp, inode, name)
		}
	}
	if status != statusOK {
		log.LogWarnf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	log.LogDebugf("remove xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listXAttr(ctx context.Context, mp *MetaPartition, inode uint64) (keys []string, status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.listXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.ListXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaListXAttr
	packet.PartitionID = mp.PartitionID
	if err = packet.MarshalData(req); err != nil {
		log.LogWarnf("list xattr: req(%v) err(%v)", *req, err)
		return
	}
	log.LogDebugf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	if packet, err = mw.sendReadToMP(ctx, mp, packet); err != nil {
		log.LogWarnf("list xattr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status == statusOutOfRange {
		log.LogWarnf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		newMp := mw.getRefreshMp(ctx, inode)
		if newMp != nil && newMp.PartitionID != mp.PartitionID {
			return mw.listXAttr(ctx, newMp, inode)
		}
	}
	if status != statusOK {
		log.LogWarnf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListXAttrResponse)
	if err = packet.UnmarshalData(resp); err != nil {
		log.LogWarnf("list xattr: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	keys = resp.XAttrs

	log.LogDebugf("list xattr: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) listMultiparts(ctx context.Context, mp *MetaPartition, prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (status int, sessions *proto.ListMultipartResponse, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.listMultiparts")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.ListMultipartRequest{
		VolName:           mw.volname,
		PartitionId:       mp.PartitionID,
		Marker:            keyMarker,
		MultipartIdMarker: multipartIdMarker,
		Max:               maxUploads,
		Delimiter:         delimiter,
		Prefix:            prefix,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpListMultiparts
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("list sessions : err(%v)", err)
		return
	}

	log.LogDebugf("listMultiparts enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("listMultiparts: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ListMultipartResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("listMultiparts: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}

	return statusOK, resp, nil
}

func (mw *MetaWrapper) batchGetXAttr(ctx context.Context, mp *MetaPartition, inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchGetXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

	var (
		err error
	)
	req := &proto.BatchGetXAttrRequest{
		VolName:     mw.volname,
		PartitionId: mp.PartitionID,
		Inodes:      inodes,
		Keys:        keys,
	}
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchGetXAttr
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		return nil, err
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogWarnf("batchGetXAttr: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return nil, err
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("batchIget: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return nil, err
	}

	resp := new(proto.BatchGetXAttrResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogWarnf("batchIget: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return nil, err
	}

	return resp.XAttrs, nil
}

func (mw *MetaWrapper) getAppliedID(ctx context.Context, mp *MetaPartition, addr string) (appliedID uint64, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.getAppliedID")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.GetAppliedIDRequest{
		PartitionId: mp.PartitionID,
	}
	packet := proto.NewPacketReqID(context.Background())
	packet.Opcode = proto.OpMetaGetAppliedID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("getAppliedID err: (%v), req(%v)", err, *req)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendToHost(ctx, mp, packet, addr)
	if err != nil || packet == nil {
		log.LogWarnf("getAppliedID: packet(%v) mp(%v) addr(%v) req(%v) err(%v)", packet, mp, addr, *req, err)
		err = errors.New("getAppliedID error")
		return
	}
	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogWarnf("getAppliedID: packet(%v) mp(%v) addr(%v) req(%v) result(%v)", packet, mp, addr, *req, packet.GetResultMsg())
		err = errors.New("getAppliedID error")
		return
	}
	appliedID = binary.BigEndian.Uint64(packet.Data)
	return
}

func containsExtent(extentKeys []proto.ExtentKey, ek proto.ExtentKey) bool {
	for _, curExtentKey := range extentKeys {
		if ek.FileOffset >= curExtentKey.FileOffset &&
			ek.FileOffset+uint64(ek.Size) <= curExtentKey.FileOffset+uint64(curExtentKey.Size) &&
			ek.PartitionId == curExtentKey.PartitionId &&
			ek.ExtentId == curExtentKey.ExtentId {
			return true
		}
	}
	return false
}

func (mw *MetaWrapper) lookupDeleted(ctx context.Context, mp *MetaPartition, parentID uint64, name string, startTime, endTime int64) (
	status int, err error, dentrys []*proto.DeletedDentry) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.lookupDeleted")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.LookupDeletedDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		StartTime:   startTime,
		EndTime:     endTime,
	}
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaLookupForDeleted
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("lookup: err(%v)", err)
		return
	}

	log.LogDebugf("lookupDeleted enter: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("lookupDeleted: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			log.LogErrorf("lookupDeleted: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		} else {
			log.LogDebugf("lookupDeleted exit: packet(%v) mp(%v) req(%v) NoEntry", packet, mp, *req)
		}
		return
	}

	resp := new(proto.LookupDeletedDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("lookupDeleted: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}

	log.LogDebugf("lookupDeleted exit: packet(%v) mp(%v) req(%v) dentrys(%v)", packet, mp, *req, len(resp.Dentrys))
	return statusOK, nil, resp.Dentrys
}

func (mw *MetaWrapper) readDeletedDir(ctx context.Context, mp *MetaPartition, parentID uint64, name string, timestamp int64) (
	status int, children []*proto.DeletedDentry, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.readDeletedDir")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.ReadDeletedDirRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		Timestamp:   timestamp,
	}
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaReadDeletedDir
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("readdir: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("readDeletedDir: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]*proto.DeletedDentry, 0)
		log.LogErrorf("readDeletedDir: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.ReadDeletedDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("readDeletedDir: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("readDeletedDir: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) recoverDentry(ctx context.Context, mp *MetaPartition,
	parentID, inode uint64, name string, timestamp int64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.recoverDentry")
	defer tracer.Finish()
	ctx = tracer.Context()

	/*
		if parentID == inode {
			return statusExist, nil
		}
	*/
	req := new(proto.RecoverDeletedDentryRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.ParentID = parentID
	req.Name = name
	req.TimeStamp = timestamp
	req.Inode = inode

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaRecoverDeletedDentry
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("recoverDentry: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("recoverDentry: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("recoverDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	log.LogDebugf("recoverDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) recoverDeletedInode(ctx context.Context, mp *MetaPartition, inode uint64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.recoverDeletedInode")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := new(proto.RecoverDeletedInodeRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inode = inode

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaRecoverDeletedInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("recoverDeletedInode: err[%v]", err)
		return
	}
	log.LogDebugf("recoverDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("recoverDeletedInode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("recoverDeletedInode: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("recoverDeletedInode: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, inode)
	return statusOK, nil
}

func (mw *MetaWrapper) batchRecoverDeletedInode(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64,
	respChan chan *proto.BatchOpDeletedINodeRsp) (status int, err error) {

	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchRecoverDeletedInode")
	defer tracer.Finish()
	ctx = tracer.Context()

	res := new(proto.BatchOpDeletedINodeRsp)
	res.Inos = make([]*proto.OpDeletedINodeRsp, 0)
	defer func() {
		if err != nil {
			for _, ino := range inodes {
				var di proto.DeletedInodeInfo
				di.Inode = ino

				var inoRsp proto.OpDeletedINodeRsp
				inoRsp.Inode = &di
				inoRsp.Status = 0
				res.Inos = append(res.Inos, &inoRsp)
			}
			respChan <- res
		}
	}()

	defer wg.Done()
	req := new(proto.BatchRecoverDeletedInodeRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inodes = inodes

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchRecoverDeletedInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchRecoverDeletedInode: err[%v]", err)
		return
	}
	log.LogDebugf("batchRecoverDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchRecoverDeletedInode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchRecoverDeletedInode: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	err = packet.UnmarshalData(res)
	if err != nil {
		log.LogErrorf("batchRecoverDeletedInode: failed to unmarshal replay, err: %v", err.Error())
		return
	}
	respChan <- res
	log.LogDebugf("batchRecoverDeletedInode: packet(%v) mp(%v) req(%v) inos(%v), res(%v)",
		packet, mp, *req, len(inodes), len(res.Inos))
	return
}

func (mw *MetaWrapper) batchRecoverDeletedDentry(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, dens []*proto.DeletedDentry,
	respChan chan *proto.BatchOpDeletedDentryRsp) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchRecoverDeletedDentry")
	defer tracer.Finish()
	ctx = tracer.Context()

	log.LogDebugf("batchRecoverDeletedDentry, mp: %v, len(dens): %v", mp.PartitionID, len(dens))
	res := new(proto.BatchOpDeletedDentryRsp)
	res.Dens = make([]*proto.OpDeletedDentryRsp, 0)
	defer func() {
		if err != nil {
			for _, den := range dens {
				var rs proto.OpDeletedDentryRsp
				rs.Status = 0
				rs.Den = new(proto.DeletedDentry)
				rs.Den.Inode = den.Inode
				res.Dens = append(res.Dens, &rs)
			}
			respChan <- res
		}
		wg.Done()
	}()

	req := new(proto.BatchRecoverDeletedDentryRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Dens = dens

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchRecoverDeletedDentry
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchRecoverDeletedDentry: err[%v]", err)
		return
	}
	log.LogDebugf("batchRecoverDeletedDentry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchRecoverDeletedDentry: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchRecoverDeletedDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	err = packet.UnmarshalData(res)
	if err != nil {
		log.LogErrorf("batchRecoverDeletedDentry: failed to unmarshal reply, err: %v", err.Error())
		return
	}
	respChan <- res
	log.LogDebugf("batchRecoverDeletedDentry: packet(%v) mp(%v) req(%v) dens(%v), res(%v)",
		packet, mp, *req, len(dens), len(res.Dens))
	return
}

func (mw *MetaWrapper) cleanDeletedDentry(ctx context.Context, mp *MetaPartition, parentID, inode uint64, name string, timestamp int64) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.cleanDeletedDentry")
	defer tracer.Finish()
	ctx = tracer.Context()

	if parentID == inode {
		return statusExist, nil
	}
	req := new(proto.CleanDeletedDentryRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.ParentID = parentID
	req.Name = name
	req.Timestamp = timestamp
	req.Inode = inode

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCleanDeletedDentry
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("cleanDeletedDentry: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("cleanDeletedDentry: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("cleanDeletedDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	}
	log.LogDebugf("cleanDeletedDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
	return
}

func (mw *MetaWrapper) batchCleanDeletedDentry(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, dens []*proto.DeletedDentry,
	respChan chan *proto.BatchOpDeletedDentryRsp) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchCleanDeletedDentry")
	defer tracer.Finish()
	ctx = tracer.Context()

	log.LogDebugf("batchRecoverDeletedDentry, mp: %v, len(dens): %v", mp.PartitionID, len(dens))
	res := new(proto.BatchOpDeletedDentryRsp)
	res.Dens = make([]*proto.OpDeletedDentryRsp, 0)
	defer func() {
		if err != nil {
			for _, den := range dens {
				var rs proto.OpDeletedDentryRsp
				rs.Status = 0
				rs.Den = new(proto.DeletedDentry)
				rs.Den.Inode = den.Inode
				res.Dens = append(res.Dens, &rs)
			}
			respChan <- res
		}
		wg.Done()
	}()

	req := new(proto.BatchCleanDeletedDentryRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Dens = dens

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchCleanDeletedDentry
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchCleanDeletedDentry: err[%v]", err)
		return
	}
	log.LogDebugf("batchCleanDeletedDentry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchCleanDeletedDentry: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchCleanDeletedDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	err = packet.UnmarshalData(res)
	if err != nil {
		log.LogErrorf("batchCleanDeletedDentry: failed to unmarshal reply, err: %v", err.Error())
		return
	}
	respChan <- res
	log.LogDebugf("batchCleanDeletedDentry: packet(%v) mp(%v) req(%v) dens(%v), res(%v)",
		packet, mp, *req, len(dens), len(res.Dens))
	return
}

func (mw *MetaWrapper) cleanDeletedInode(ctx context.Context, mp *MetaPartition, inode uint64) (status int, err error) {
	req := new(proto.CleanDeletedInodeRequest)
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.cleanDeletedInode")
	defer tracer.Finish()
	ctx = tracer.Context()

	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inode = inode
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCleanDeletedInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("cleanDeletedInode: err[%v]", err)
		return
	}
	log.LogDebugf("cleanDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("cleanDeletedInode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("cleanDeletedInode: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("cleanDeletedInode: packet(%v) mp(%v) req(%v) ino(%v)", packet, mp, *req, inode)
	return statusOK, nil
}

func (mw *MetaWrapper) batchCleanDeletedInode(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64,
	respChan chan *proto.BatchOpDeletedINodeRsp) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchCleanDeletedInode")
	defer tracer.Finish()
	ctx = tracer.Context()

	res := new(proto.BatchOpDeletedINodeRsp)
	res.Inos = make([]*proto.OpDeletedINodeRsp, 0)
	defer func() {
		if err != nil {
			for _, ino := range inodes {
				var di proto.DeletedInodeInfo
				di.Inode = ino

				var inoRsp proto.OpDeletedINodeRsp
				inoRsp.Inode = &di
				inoRsp.Status = 0
				res.Inos = append(res.Inos, &inoRsp)
			}
			respChan <- res
		}
	}()

	defer wg.Done()
	req := new(proto.BatchCleanDeletedInodeRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inodes = inodes

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchCleanDeletedInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchCleanDeletedInode: err[%v]", err)
		return
	}
	log.LogDebugf("batchCleanDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchCleanDeletedInode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchCleanDeletedInode: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	err = packet.UnmarshalData(res)
	if err != nil {
		log.LogErrorf("batchCleanDeletedInode: failed to unmarshal replay, err: %v", err.Error())
		return
	}
	respChan <- res
	log.LogDebugf("batchCleanDeletedInode: packet(%v) mp(%v) req(%v) inos(%v), res(%v)",
		packet, mp, *req, len(inodes), len(res.Inos))
	return
}

func (mw *MetaWrapper) statDeletedFileInfo(ctx context.Context, mp *MetaPartition) (resp *proto.StatDeletedFileInfoResponse, status int, err error) {
	req := new(proto.StatDeletedFileInfoRequest)
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.statDeletedFileInfo")
	defer tracer.Finish()
	ctx = tracer.Context()

	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaStatDeletedFileInfo
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("statDeletedFileInfo: err[%v]", err)
		return
	}
	log.LogDebugf("statDeletedFileInfo: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("statDeletedFileInfo: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("statDeletedFileInfo: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp = new(proto.StatDeletedFileInfoResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("statDeletedFileInfo: err[%v]", err)
		return
	}
	log.LogDebugf("statDeletedFileInfo: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return
}

func (mw *MetaWrapper) cleanExpiredDeletedInode(ctx context.Context, mp *MetaPartition, deadline uint64) (status int, err error) {
	req := new(proto.CleanExpiredInodeRequest)
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.cleanExpiredDeletedInode")
	defer tracer.Finish()
	ctx = tracer.Context()

	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Expires = deadline

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCleanExpiredInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("cleanExpiredDeletedInode: err[%v]", err)
		return
	}
	log.LogDebugf("cleanExpiredDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("cleanDeletedInode: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("cleanExpiredDeletedInode: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("cleanExpiredDeletedInode: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) cleanExpiredDeletedDentry(ctx context.Context, mp *MetaPartition, deadline uint64) (status int, err error) {
	req := new(proto.CleanExpiredDentryRequest)
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.cleanExpiredDeletedDentry")
	defer tracer.Finish()
	ctx = tracer.Context()

	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Expires = deadline

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaCleanExpiredDentry
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("cleanExpiredDeletedDentry: err[%v]", err)
		return
	}
	log.LogDebugf("cleanExpiredDeletedDentry: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("cleanExpiredDeletedDentry: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("cleanExpiredDeletedDentry: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}
	log.LogDebugf("cleanExpiredDeletedDentry: packet(%v) mp(%v) req(%v)", packet, mp, *req)
	return statusOK, nil
}

func (mw *MetaWrapper) getDeletedInodeInfo(ctx context.Context, mp *MetaPartition, inode uint64) (
	status int, info *proto.DeletedInodeInfo, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.getDeletedInodeInfo")
	defer tracer.Finish()
	ctx = tracer.Context()

	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaGetDeletedInode
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getDeletedInodeInfo: req(%v) err(%v)", *req, err)
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("getDeletedInodeInfo: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("getDeletedInodeInfo: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.GetDeletedInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		log.LogErrorf("getDeletedInodeInfo: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)", packet, mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) batchGetDeletedInodeInfo(ctx context.Context, wg *sync.WaitGroup,
	mp *MetaPartition, inodes []uint64, respCh chan []*proto.DeletedInodeInfo) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchGetDeletedInodeInfo")
	defer tracer.Finish()
	ctx = tracer.Context()
	defer wg.Done()
	var (
		err error
	)
	req := &proto.BatchGetDeletedInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inodes:      inodes,
	}

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchGetDeletedInode
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, err = mw.sendReadToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchGetDeletedInodeInfo: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchGetDeletedInodeInfo: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		return
	}

	resp := new(proto.BatchGetDeletedInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("batchGetDeletedInodeInfo: packet(%v) mp(%v) err(%v) PacketData(%v)", packet, mp, err, string(packet.Data))
		return
	}

	if len(resp.Infos) == 0 {
		return
	}

	select {
	case respCh <- resp.Infos:
	default:
	}
}

func (mw *MetaWrapper) batchUnlinkInodeUntest(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64,
	respChan chan *proto.BatchUnlinkInodeResponse, trashEnable bool) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchDeleteInodeUntest")
	defer tracer.Finish()
	ctx = tracer.Context()

	defer wg.Done()
	req := new(proto.BatchUnlinkInodeRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inodes = inodes
	req.TrashEnable = trashEnable

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchUnlinkInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchDeleteInodeUntest: err[%v]", err)
		return
	}
	log.LogDebugf("batchDeleteInodeUntest: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchDeleteInodeUntest: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchDeleteInodeUntest: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	var resp proto.BatchUnlinkInodeResponse
	resp.Items = make([]*struct {
		Info   *proto.InodeInfo `json:"info"`
		Status uint8            `json:"status"`
	}, 0)
	err = packet.UnmarshalData(&resp)
	if err != nil {
		log.LogErrorf("batchDeleteInodeUntest: failed to unmarshal replay, err: %v", err.Error())
		return
	}
	respChan <- &resp
	log.LogDebugf("batchDeleteInodeUntest: packet(%v) mp(%v) req(%v) inos(%v), res(%v)",
		packet, mp, *req, len(inodes), len(resp.Items))
	return
}

func (mw *MetaWrapper) batchEvictInodeUntest(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64,
	respChan chan int, trashEnable bool) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchEvictInodeUntest")
	defer tracer.Finish()
	ctx = tracer.Context()
	defer wg.Done()

	status = statusError
	defer func() {
		if err != nil {
			respChan <- status
		}
	}()

	req := new(proto.BatchEvictInodeRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.Inodes = inodes
	req.TrashEnable = trashEnable

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchEvictInode
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchEvictInodeUntest: err[%v]", err)
		return
	}
	log.LogDebugf("batchEvictInodeUntest: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchEvictInodeUntest: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchEvictInodeUntest: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	respChan <- status
	log.LogDebugf("batchEvictInodeUntest: packet(%v) mp(%v) req(%v) inos(%v), status(%v)",
		packet, mp, *req, len(inodes), status)
	return
}

func (mw *MetaWrapper) batchDeleteDentryUntest(ctx context.Context, wg *sync.WaitGroup, mp *MetaPartition, pid uint64, dens []proto.Dentry,
	respChan chan *proto.BatchDeleteDentryResponse, trashEnable bool) (status int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.batchDeleteDentryUntest")
	defer tracer.Finish()
	defer wg.Done()
	ctx = tracer.Context()

	log.LogDebugf("batchDeleteDentryUntest, mp: %v, len(dens): %v", mp.PartitionID, len(dens))
	req := new(proto.BatchDeleteDentryRequest)
	req.VolName = mw.volname
	req.PartitionID = mp.PartitionID
	req.ParentID = pid
	req.Dens = dens
	req.TrashEnable = trashEnable

	packet := proto.NewPacketReqID(ctx)
	packet.Opcode = proto.OpMetaBatchDeleteDentry
	if err = packet.MarshalData(req); err != nil {
		log.LogErrorf("batchDeleteDentryUntest: err[%v]", err)
		return
	}
	log.LogDebugf("batchDeleteDentryUntest: packet(%v) mp(%v) req(%v)", packet, mp, string(packet.Data))

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer metric.Set(err)

	packet, _, err = mw.sendWriteToMP(ctx, mp, packet)
	if err != nil {
		log.LogErrorf("batchDeleteDentryUntest: packet(%v) mp(%v) req(%v) err(%v)", packet, mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchDeleteDentryUntest: packet(%v) mp(%v) req(%v) result(%v)", packet, mp, *req, packet.GetResultMsg())
		err = fmt.Errorf("status: %v", status)
		return
	}

	var resp proto.BatchDeleteDentryResponse
	resp.Items = make([]*struct {
		Inode  uint64 `json:"ino"`
		Status uint8  `json:"status"`
	}, 0)
	err = packet.UnmarshalData(&resp)
	if err != nil {
		log.LogErrorf("batchDeleteDentryUntest: failed to unmarshal reply, err: %v", err.Error())
		return
	}
	respChan <- &resp
	log.LogDebugf("batchDeleteDentryUntest: packet(%v) mp(%v) req(%v) dens(%v), res(%v)",
		packet, mp, *req, len(dens), len(resp.Items))
	return
}
