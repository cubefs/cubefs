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

package metanode

import (
	"context"
	"fmt"
	"strings"

	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"

	"github.com/chubaofs/chubaofs/proto"
)

const readDirMax = 1000 // the maximum number of children that can be returned per operation

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func (resp *DentryResponse) String() string {
	if resp != nil {
		return fmt.Sprintf("Status(%v) Msg(%v)", resp.Status, resp.Msg)
	}
	return ""
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry,
	forceUpdate bool) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(NewInode(dentry.ParentId, 0))
	var parIno *Inode
	defer func() {
		log.LogErrorf("fsmCreateDentry dentry(%v) parIno(%v)", dentry, parIno)
	}()
	if !forceUpdate {
		if item == nil {
			status = proto.OpNotExistErr
			return
		}
		parIno = item.(*Inode)
		if parIno.ShouldDelete() {
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}
	if item, ok := mp.dentryTree.ReplaceOrInsert(dentry, false); !ok {
		//do not allow directories and files to overwrite each
		// other when renaming
		d := item.(*Dentry)
		if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) {
			status = proto.OpArgMismatchErr
			return
		}

		if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			return
		}

		status = proto.OpExistErr
	} else {
		if !forceUpdate {
			log.LogErrorf("fsmCreateDentry, before IncNLink dentry(%v) parIno(%v)", dentry, parIno)
			parIno.IncNLink()
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

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(dentry *Dentry, checkInode bool) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	var ino *Inode
	defer func() {
		log.LogErrorf("fsmDeleteDentry dentry(%v) inode(%v)", dentry, ino)
	}()
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
			return mp.dentryTree.Delete(dentry)
		})
	} else {
		item = mp.dentryTree.Delete(dentry)
	}

	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	} else {
		mp.inodeTree.CopyFind1(NewInode(dentry.ParentId, 0),
			func(item BtreeItem) {
				if item != nil {
					ino = item.(*Inode)
				}
				log.LogErrorf("fsmDeleteDentry, before DecNLink, dentry(%v) inode(%v)", dentry, ino)
				if item != nil {
					ino = item.(*Inode)
					if !ino.ShouldDelete() {
						item.(*Inode).DecNLink()
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

		if d.Inode == dentry.Inode {
			//already update, this apply will do nothing
			dentry.Inode = 0
		} else {
			//first update
			d.Inode, dentry.Inode = dentry.Inode, d.Inode
		}

		resp.Msg = dentry
	})
	return
}

func (mp *metaPartition) getDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}

func (mp *metaPartition) readDir(ctx context.Context, req *ReadDirReq) (resp *ReadDirResp) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("metaPartition.readDir")
	defer tracer.Finish()
	ctx = tracer.Context()

	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Marker,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}

	count := uint64(0)
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
		count += 1
		d := i.(*Dentry)
		if req.IsBatch && count > readDirMax {
			resp.NextMarker = d.Name
			return false
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	tracer.SetTag("count", count).SetTag("total", mp.dentryTree.Len()).SetTag("parentID", req.PartitionID)
	return
}
