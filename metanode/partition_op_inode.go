package metanode

import (
	"encoding/json"
	"time"

	"github.com/chubaoio/cbfs/proto"
)

func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PackErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.LinkTarget = req.Target
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.Put(opCreateInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	var (
		status = resp.(uint8)
		reply  []byte
	)
	if status == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Generation = ino.Generation
		resp.Info.Size = ino.Size
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		resp.Info.Target = ino.LinkTarget
		resp.Info.Nlink = ino.NLink
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) DeleteInode(req *DeleteInoReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	r, err := mp.Put(opDeleteInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := r.(*ResponseInode)
	status := msg.Status
	var reply []byte
	if status == proto.OpOk && msg.Msg != nil {
		resp := &proto.DeleteInodeResponse{}
		resp.Extents = msg.Msg.Extents.Extents
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) Open(req *OpenReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opOpen, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) InodeGet(req *InodeGetReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Size = ino.Size
		resp.Info.Generation = ino.Generation
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		resp.Info.Target = ino.LinkTarget
		resp.Info.Nlink = ino.NLink
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error) {
	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		ino.Inode = inoId
		retMsg := mp.getInode(ino)
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			inoInfo.Inode = retMsg.Msg.Inode
			inoInfo.Size = retMsg.Msg.Size
			inoInfo.Mode = retMsg.Msg.Type
			inoInfo.Generation = retMsg.Msg.Generation
			inoInfo.AccessTime = time.Unix(retMsg.Msg.AccessTime, 0)
			inoInfo.ModifyTime = time.Unix(retMsg.Msg.ModifyTime, 0)
			inoInfo.CreateTime = time.Unix(retMsg.Msg.CreateTime, 0)
			inoInfo.Target = retMsg.Msg.LinkTarget
			inoInfo.Nlink = retMsg.Msg.NLink
			resp.Infos = append(resp.Infos, inoInfo)
		}
	}
	data, err := json.Marshal(resp)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackOkWithBody(data)
	return
}

func (mp *metaPartition) CreateLinkInode(req *LinkInodeReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.Put(opFSMCreateLinkInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := resp.(*ResponseInode)
	status := retMsg.Status
	var reply []byte
	if status == proto.OpOk {
		resp := &LinkInodeResp{
			Info: &proto.InodeInfo{},
		}
		resp.Info.Inode = retMsg.Msg.Inode
		resp.Info.Mode = retMsg.Msg.Type
		resp.Info.Generation = retMsg.Msg.Generation
		resp.Info.Size = retMsg.Msg.Size
		resp.Info.AccessTime = time.Unix(retMsg.Msg.AccessTime, 0)
		resp.Info.ModifyTime = time.Unix(retMsg.Msg.ModifyTime, 0)
		resp.Info.CreateTime = time.Unix(retMsg.Msg.CreateTime, 0)
		resp.Info.Nlink = retMsg.Msg.NLink
		resp.Info.Target = retMsg.Msg.LinkTarget
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}
