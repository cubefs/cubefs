package metanode

import (
	"encoding/json"

	"github.com/chubaoio/cbfs/proto"
)

func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		return
	}
	resp, err := mp.Put(opCreateDentry, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	r, err := mp.Put(opDeleteDentry, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := r.(*ResponseDentry)
	p.ResultCode = retMsg.Status
	dentry = retMsg.Msg
	if p.ResultCode == proto.OpOk {
		var reply []byte
		resp := &DeleteDentryResp{
			Inode: dentry.Inode,
		}
		reply, err = json.Marshal(resp)
		p.PackOkWithBody(reply)
	}
	return
}

func (mp *metaPartition) UpdateDentry(req *UpdateDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	resp, err := mp.Put(opUpdateDentry, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*ResponseDentry)
	p.ResultCode = msg.Status
	if msg.Status == proto.OpOk {
		var reply []byte
		m := &UpdateDentryResp{
			Inode: msg.Msg.Inode,
		}
		reply, err = json.Marshal(m)
		p.PackOkWithBody(reply)
	}
	return
}

func (mp *metaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackOkWithBody(reply)
	return
}

func (mp *metaPartition) Lookup(req *LookupReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	dentry, status := mp.getDentry(dentry)
	var reply []byte
	if status == proto.OpOk {
		resp := &LookupResp{
			Inode: dentry.Inode,
			Mode:  dentry.Type,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}
