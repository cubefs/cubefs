package meta

import (
	"fmt"
	"sync"

	"github.com/juju/errors"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/chubaoio/cbfs/util/ump"
)

// API implementations
//

func (mw *MetaWrapper) open(mp *MetaPartition, inode uint64) (status int, err error) {
	req := &proto.OpenRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaOpen
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("open: err(%v)", err)
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("open: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("open: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
	}
	return
}

func (mw *MetaWrapper) icreate(mp *MetaPartition, mode uint32, target []byte) (status int, info *proto.InodeInfo, err error) {
	req := &proto.CreateInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Mode:        mode,
		Target:      target,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("icreate: err(%v)", err)
		return
	}

	log.LogDebugf("icreate enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("icreate: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("icreate: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("icreate: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("icreate: info is nil, mp(%v) req(%v) PacketData(%v)", mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}
	log.LogDebugf("icreate exit: mp(%v) req(%v) info(%v)", mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) idelete(mp *MetaPartition, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	req := &proto.DeleteInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteInode
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("idelete: err(%v)", err)
		return
	}

	log.LogDebugf("idelete enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("idelete: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("idelete: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("idelete: mp(%v) err(%v) RespData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("idelete exit: mp(%v) req(%v) extents(%v)", mp, *req, resp.Extents)
	return statusOK, resp.Extents, nil
}

func (mw *MetaWrapper) dcreate(mp *MetaPartition, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	req := &proto.CreateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateDentry
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("dcreate: err(%v)", err)
		return
	}

	log.LogDebugf("dcreate enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("dcreate: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("dcreate: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
	}
	log.LogDebugf("dcreate exit: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
	return
}

func (mw *MetaWrapper) dupdate(mp *MetaPartition, parentID uint64, name string, newInode uint64) (status int, oldInode uint64, err error) {
	req := &proto.UpdateDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
		Inode:       newInode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaUpdateDentry
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("dupdate: err(%v)", err)
		return
	}

	log.LogDebugf("dupdate enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("dupdate: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("dupdate: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.UpdateDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("dupdate: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("dupdate exit: mp(%v) req(%v) oldIno(%v)", mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) ddelete(mp *MetaPartition, parentID uint64, name string) (status int, inode uint64, err error) {
	req := &proto.DeleteDentryRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteDentry
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("ddelete: err(%v)", err)
		return
	}

	log.LogDebugf("ddelete enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("ddelete: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("ddelete: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("ddelete: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("ddelete exit: mp(%v) req(%v) ino(%v)", mp, *req, resp.Inode)
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) lookup(mp *MetaPartition, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	req := &proto.LookupRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
		Name:        name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLookup
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("lookup: err(%v)", err)
		return
	}

	log.LogDebugf("lookup enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("lookup: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		if status != statusNoent {
			log.LogErrorf("lookup: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		} else {
			log.LogDebugf("lookup exit: mp(%v) req(%v) NoEntry", mp, *req)
		}
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("lookup: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("lookup exit: mp(%v) req(%v) ino(%v) mode(%v)", mp, *req, resp.Inode, resp.Mode)
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(mp *MetaPartition, inode uint64) (status int, info *proto.InodeInfo, err error) {
	req := &proto.InodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaInodeGet
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("iget: err(%v)", err)
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("iget: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("iget: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil || resp.Info == nil {
		log.LogErrorf("iget: mp(%v) req(%v) err(%v) PacketData(%v)", mp, *req, err, string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) batchIget(wg *sync.WaitGroup, mp *MetaPartition, inodes []uint64, respCh chan []*proto.InodeInfo) {
	defer wg.Done()
	var (
		err error
	)
	req := &proto.BatchInodeGetRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inodes:      inodes,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaBatchInodeGet
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("batchIget: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("batchIget: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.BatchInodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("batchIget: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
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

func (mw *MetaWrapper) readdir(mp *MetaPartition, parentID uint64) (status int, children []proto.Dentry, err error) {
	req := &proto.ReadDirRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		ParentID:    parentID,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaReadDir
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("readdir: err(%v)", err)
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("readdir: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		log.LogErrorf("readdir: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("readdir: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("readdir: mp(%v) req(%v) dentries(%v)", mp, *req, resp.Children)
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(mp *MetaPartition, inode uint64, extent proto.ExtentKey) (status int, err error) {
	req := &proto.AppendExtentKeyRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		Extent:      extent,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsAdd
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("appendExtentKey: err(%v)", err)
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("appendExtentKey: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("appendExtentKey: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
	}
	return status, nil
}

func (mw *MetaWrapper) getExtents(mp *MetaPartition, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	req := &proto.GetExtentsRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsList
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("getExtents: err(%v)", err)
		return
	}

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("getExtents: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		log.LogErrorf("getExtents: mp(%v) result(%v)", mp, packet.GetResultMesg())
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("getExtents: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	return statusOK, resp.Extents, nil
}

func (mw *MetaWrapper) truncate(mp *MetaPartition, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	req := &proto.TruncateRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
		FileOffset:  0,
		Size:        0,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaTruncate
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("truncate: err(%v)", err)
		return
	}

	log.LogDebugf("truncate enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("truncate: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("truncate: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.TruncateResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("truncate: mp(%v) err(%v) RespData(%v)", mp, err, string(packet.Data))
		return
	}
	log.LogDebugf("truncate exit: mp(%v) req(%v) RespData(%v)", mp, *req, string(packet.Data))
	return statusOK, resp.Extents, nil
}

func (mw *MetaWrapper) ilink(mp *MetaPartition, inode uint64) (status int, info *proto.InodeInfo, err error) {
	req := &proto.LinkInodeRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLinkInode
	err = packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("ilink: err(%v)", err)
		return
	}

	log.LogDebugf("ilink enter: mp(%v) req(%v)", mp, string(packet.Data))

	umpKey := mw.umpKey(packet.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	defer ump.AfterTP(tpObject, err)

	packet, err = mw.sendToMetaPartition(mp, packet)
	if err != nil {
		log.LogErrorf("ilink: mp(%v) req(%v) err(%v)", mp, *req, err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("ilink: mp(%v) req(%v) result(%v)", mp, *req, packet.GetResultMesg())
		return
	}

	resp := new(proto.LinkInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.LogErrorf("ilink: mp(%v) err(%v) PacketData(%v)", mp, err, string(packet.Data))
		return
	}
	if resp.Info == nil {
		err = errors.New(fmt.Sprintf("ilink: info is nil, mp(%v) req(%v) PacketData(%v)", mp, *req, string(packet.Data)))
		log.LogWarn(err)
		return
	}
	log.LogDebugf("ilink exit: mp(%v) req(%v) info(%v)", mp, *req, resp.Info)
	return statusOK, resp.Info, nil
}
