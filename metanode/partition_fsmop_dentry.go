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
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

func (mp *metaPartition) fsmTxCreateDentry(txDentry *TxDentry, forceUpdate bool) (status uint8) {
	//1.if mpID == -1, register transaction in transaction manager
	//if txDentry.TxInfo.TxID != "" && txDentry.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txDentry.TxInfo)
	//}
	//2.register rollback item
	txDI := proto.NewTxDentryInfo("", txDentry.Dentry.ParentId, txDentry.Dentry.Name, 0)

	//txDenInfo := mp.txProcessor.txManager.getTxDentryInfo(txDentry.TxInfo.TxID, txDI.GetKey())
	txDenInfo, ok := txDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		status = proto.OpTxDentryInfoNotExistErr
		return
	}
	rbDentry := NewTxRollbackDentry(txDentry.Dentry, txDenInfo, TxDelete)
	if err := mp.txProcessor.txResource.addTxRollbackDentry(rbDentry); err != nil {
		status = proto.OpTxConflictErr
		return
	}

	return mp.fsmCreateDentry(txDentry.Dentry, forceUpdate)
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry,
	forceUpdate bool) (status uint8) {
	status = proto.OpOk
	var parIno *Inode
	if !forceUpdate {
		item := mp.inodeTree.CopyGet(NewInode(dentry.ParentId, 0))
		if item == nil {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get nil, dentry name [%v], inode [%v]", dentry.ParentId, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		parIno = item.(*Inode)
		if parIno.ShouldDelete() {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v]", dentry.ParentId, parIno, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] type [%v] not dir, dentry name [%v], inode [%v]", dentry.ParentId, parIno.Type, dentry.Name, dentry.Inode)
			status = proto.OpArgMismatchErr
			return
		}
	}
	if item, ok := mp.dentryTree.ReplaceOrInsert(dentry, false); !ok {
		//do not allow directories and files to overwrite each
		// other when renaming
		d := item.(*Dentry)
		if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) && !proto.IsSymlink(dentry.Type) && !proto.IsSymlink(d.Type) {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v], type[%v,%v],dir[%v,%v]",
				dentry.ParentId, parIno, dentry.Name, dentry.Inode, dentry.Type, d.Type, proto.IsSymlink(dentry.Type), proto.IsSymlink(d.Type))
			status = proto.OpArgMismatchErr
			return
		}

		if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			return
		}

		status = proto.OpExistErr
	} else {
		if !forceUpdate {
			parIno.IncNLink()
			parIno.SetMtime()
		}
	}

	return
}

// Query a dentry from the dentry tree with specified dentry info.
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

func (mp *metaPartition) fsmTxDeleteDentry(txDentry *TxDentry, checkInode bool) (resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	//1.if mpID == -1, register transaction in transaction manager
	//if txDentry.TxInfo.TxID != "" && txDentry.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txDentry.TxInfo)
	//}
	//2.register rollback item
	txDI := proto.NewTxDentryInfo("", txDentry.Dentry.ParentId, txDentry.Dentry.Name, 0)
	txDenInfo, ok := txDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		resp.Status = proto.OpTxDentryInfoNotExistErr
		return
	}
	rbDentry := NewTxRollbackDentry(txDentry.Dentry, txDenInfo, TxAdd)
	if err := mp.txProcessor.txResource.addTxRollbackDentry(rbDentry); err != nil {
		resp.Status = proto.OpTxConflictErr
		return
	}

	return mp.fsmDeleteDentry(txDentry.Dentry, checkInode)
}

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(dentry *Dentry, checkInode bool) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	var item interface{}
	if checkInode {
		item = mp.dentryTree.Execute(func(tree *btree.BTree) interface{} {
			d := tree.CopyGet(dentry)
			if d == nil {
				return nil
			}
			if d.(*Dentry).Inode != dentry.Inode {
				return nil
			}
			return mp.dentryTree.tree.Delete(dentry)
		})
	} else {
		item = mp.dentryTree.Delete(dentry)
	}

	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	} else {
		mp.inodeTree.CopyFind(NewInode(dentry.ParentId, 0),
			func(item BtreeItem) {
				if item != nil {
					ino := item.(*Inode)
					if !ino.ShouldDelete() {
						item.(*Inode).DecNLink()
						item.(*Inode).SetMtime()
					}
				}
			})
	}
	resp.Msg = item.(*Dentry)
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(db DentryBatch) []*DentryResponse {
	result := make([]*DentryResponse, 0, len(db))
	for _, dentry := range db {
		result = append(result, mp.fsmDeleteDentry(dentry, true))
	}
	return result
}

func (mp *metaPartition) fsmTxUpdateDentry(txUpDateDentry *TxUpdateDentry) (resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	//1.if mpID == -1, register transaction in transaction manager
	//if txDentry.TxInfo.TxID != "" && txDentry.TxInfo.TmID == -1 {
	_ = mp.txProcessor.txManager.registerTransaction(txUpDateDentry.TxInfo)
	//}

	//2.register rollback item
	txDI := proto.NewTxDentryInfo("", txUpDateDentry.OldDentry.ParentId, txUpDateDentry.OldDentry.Name, 0)

	//txDenInfo := mp.txProcessor.txManager.getTxDentryInfo(txDentry.TxInfo.TxID, txDI.GetKey())
	txDenInfo, ok := txUpDateDentry.TxInfo.TxDentryInfos[txDI.GetKey()]
	if !ok {
		resp.Status = proto.OpTxDentryInfoNotExistErr
		return
	}
	rbDentry := NewTxRollbackDentry(txUpDateDentry.OldDentry, txDenInfo, TxUpdate)
	if err := mp.txProcessor.txResource.addTxRollbackDentry(rbDentry); err != nil {
		resp.Status = proto.OpTxConflictErr
		return
	}

	return mp.fsmUpdateDentry(txUpDateDentry.NewDentry)
}

func (mp *metaPartition) fsmUpdateDentry(dentry *Dentry) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	mp.dentryTree.CopyFind(dentry, func(item BtreeItem) {
		if item == nil {
			resp.Status = proto.OpNotExistErr
			return
		}
		d := item.(*Dentry)
		d.Inode, dentry.Inode = dentry.Inode, d.Inode
		resp.Msg = dentry
	})
	return
}

func (mp *metaPartition) readDirOnly(req *ReadDirOnlyReq) (resp *ReadDirOnlyResp) {
	resp = &ReadDirOnlyResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
		d := i.(*Dentry)
		if proto.IsDir(d.Type) {
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		return true
	})
	return
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
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

// Read dentry from btree by limit count
// if req.Marker == "" and req.Limit == 0, it becomes readDir
// else if req.Marker != "" and req.Limit == 0, return dentries from pid:name to pid+1
// else if req.Marker == "" and req.Limit != 0, return dentries from pid with limit count
// else if req.Marker != "" and req.Limit != 0, return dentries from pid:marker to pid:xxxx with limit count
//
func (mp *metaPartition) readDirLimit(req *ReadDirLimitReq) (resp *ReadDirLimitResp) {
	resp = &ReadDirLimitResp{}
	startDentry := &Dentry{
		ParentId: req.ParentID,
	}
	if len(req.Marker) > 0 {
		startDentry.Name = req.Marker
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(startDentry, endDentry, func(i BtreeItem) bool {
		d := i.(*Dentry)
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		// Limit == 0 means no limit.
		if req.Limit > 0 && uint64(len(resp.Children)) >= req.Limit {
			return false
		}
		return true
	})
	return
}
