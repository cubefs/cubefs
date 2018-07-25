package metanode

import (
	"encoding/json"

	"github.com/tiglabs/baudstorage/proto"
)

func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.Extents.Put(req.Extent)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opExtentsAdd, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{}
		ino.Extents.Range(func(i int, ext proto.ExtentKey) bool {
			resp.Extents = append(resp.Extents, ext)
			return true
		})
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) ExtentsTruncate(req *ExtentsTruncateReq,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, proto.ModeRegular)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opFSMExtentTruncate, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*ResponseInode)
	p.ResultCode = msg.Status
	if msg.Status == proto.OpOk {
		var reply []byte
		m := &ExtentsTruncateResp{
			Extents: make([]proto.ExtentKey, 0, msg.Msg.Extents.GetExtentLen()),
		}
		msg.Msg.Extents.Range(func(i int, v proto.ExtentKey) bool {
			m.Extents = append(m.Extents, v)
			return true
		})
		if reply, err = json.Marshal(m); err != nil {
			p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		p.PackOkWithBody(reply)
	}
	return
}
