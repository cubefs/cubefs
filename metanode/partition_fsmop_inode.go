package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/btree"
)

type ResponseInode struct {
	Status uint8
	Msg    *Inode
}

func NewResponseInode() *ResponseInode {
	return &ResponseInode{
		Msg: NewInode(0, 0),
	}
}

// CreateInode create inode to inode tree.
func (mp *metaPartition) createInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	mp.inodeMu.Lock()
	defer mp.inodeMu.Unlock()
	if mp.inodeTree.Has(ino) {
		status = proto.OpExistErr
		return
	}
	mp.inodeTree.ReplaceOrInsert(ino)
	return
}

// GetInode query inode from InodeTree with specified inode info;
func (mp *metaPartition) getInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Inode)
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	ok = mp.inodeTree.Has(ino)
	return
}

func (mp *metaPartition) getInodeTree() *btree.BTree {
	return mp.inodeTree
}

func (mp *metaPartition) RangeInode(f func(i btree.Item) bool) {
	mp.inodeTree.Ascend(f)
}

// DeleteInode delete specified inode item from inode tree.
func (mp *metaPartition) deleteInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	mp.inodeMu.Lock()
	item := mp.inodeTree.Delete(ino)
	mp.inodeMu.Unlock()
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Inode)
	return
}

func (mp *metaPartition) appendExtents(ino *Inode) (status uint8) {
	exts := ino.Extents
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	modifyTime := ino.ModifyTime
	ino = item.(*Inode)
	exts.Range(func(i int, ext proto.ExtentKey) bool {
		ino.AppendExtents(ext)
		return true
	})
	ino.ModifyTime = modifyTime
	ino.Generation++
	return
}

func (mp *metaPartition) extentsTruncate(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	mp.inodeMu.Lock()
	defer mp.inodeMu.Unlock()
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.Type == proto.ModeDir {
		resp.Status = proto.OpArgMismatchErr
		return
	}
	ino.Extents = i.Extents
	i.Size = 0
	i.ModifyTime = ino.ModifyTime
	i.Generation++
	i.Extents = proto.NewStreamKey(i.Inode)
	return
}
