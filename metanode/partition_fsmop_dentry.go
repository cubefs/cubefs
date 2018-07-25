package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/btree"
)

type ResponseDentry struct {
	Status uint8
	Msg    *Dentry
}

func NewResponseDentry() *ResponseDentry {
	return &ResponseDentry{
		Msg: &Dentry{},
	}
}

// CreateDentry insert dentry into dentry tree.
func (mp *metaPartition) createDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	mp.dentryMu.Lock()
	defer mp.dentryMu.Unlock()
	item := mp.dentryTree.Get(dentry)
	if item != nil {
		de := item.(*Dentry)
		status = proto.OpArgMismatchErr
		if de.Type == dentry.Type {
			status = proto.OpExistErr
		}
		return
	}
	mp.dentryTree.ReplaceOrInsert(dentry)
	return
}

// GetDentry query dentry from DentryTree with specified dentry info;
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8) {
	status := proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	dentry = item.(*Dentry)
	return dentry, status
}

// DeleteDentry delete dentry from dentry tree.
func (mp *metaPartition) deleteDentry(dentry *Dentry) (resp *ResponseDentry) {
	resp = NewResponseDentry()
	resp.Status = proto.OpOk
	mp.dentryMu.Lock()
	item := mp.dentryTree.Delete(dentry)
	mp.dentryMu.Unlock()
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Dentry)
	return
}

func (mp *metaPartition) updateDentry(dentry *Dentry) (resp *ResponseDentry) {
	resp = NewResponseDentry()
	resp.Status = proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	d := item.(*Dentry)
	d.Inode, dentry.Inode = dentry.Inode, d.Inode
	resp.Msg = dentry
	return
}

func (mp *metaPartition) getDentryTree() *btree.BTree {
	return mp.dentryTree
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i btree.Item) bool {
		d := i.(*Dentry)
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	return
}
