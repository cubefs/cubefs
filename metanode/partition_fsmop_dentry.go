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
	"strings"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/proto"
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

// Insert a dentry into the dentry tree.
func (mp *MetaPartition) fsmCreateDentry(dentry *Dentry, forceUpdate bool) (status uint8) {
	status = proto.OpOk
	item, err := mp.inodeTree.Get(dentry.ParentId)
	if err != nil {
		log.LogErrorf("[fsmCreateDentry] get dentry error: pid: %v, dentry: %v, error: %v", mp.config.PartitionId, dentry, err.Error())
		status = proto.OpNotExistErr
		return
	}
	var parIno *Inode
	if !forceUpdate {
		if item == nil {
			status = proto.OpNotExistErr
			return
		}
		parIno = item
		if parIno.ShouldDelete() {
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}
	err = mp.dentryTree.Create(dentry)
	if err == existsError {
		//do not allow directories and files to overwrite each
		// other when renaming
		d, err := mp.dentryTree.Get(dentry.ParentId, dentry.Name)
		if err != nil || d == nil {
			status = proto.OpErr
			return
		}

		if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) {
			status = proto.OpArgMismatchErr
			return
		}

		if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			return
		}

		status = proto.OpExistErr
	} else if err != nil {
		status = proto.OpArgMismatchErr
	} else {
		if !forceUpdate {
			parIno.IncNLink()
			log.LogIfNotNil(mp.inodeTree.Update(parIno))
		}
	}

	return
}

// Query a dentry from the dentry tree with specified dentry info.
func (mp *MetaPartition) getDentry(pid uint64, name string) (*Dentry, uint8) {
	status := proto.OpOk
	dentry, err := mp.dentryTree.RefGet(pid, name)
	if dentry == nil {
		log.LogErrorf("get nil dentry: [%v]: %v", pid, name)
		status = proto.OpNotExistErr
		return nil, status
	}
	if err != nil {
		log.LogErrorf("get dentry has err:[%s]", err.Error())
		status = proto.OpNotExistErr
		return nil, status
	}
	return dentry, status
}

// Delete dentry from the dentry tree.
func (mp *MetaPartition) fsmDeleteDentry(dentry *Dentry) (resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	var item *Dentry
	old, _ := mp.dentryTree.Get(dentry.ParentId, dentry.Name)
	if old != nil {
		if dentry.Inode > 0 && old.Inode != dentry.Inode {
			log.LogDebugf("[MetaPartition] fsmDeleteDentry inode not equal old: %v, dentry: %v ", old, dentry)
			return nil
		}
		if err := mp.dentryTree.Delete(dentry.ParentId, dentry.Name); err != nil {
			log.LogErrorf("delete dentry has err:[%s]", err.Error())
			return nil
		}
		item = old
	}

	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	} else {
		inode, _ := mp.inodeTree.Get(dentry.ParentId)
		if inode != nil {
			if !inode.ShouldDelete() {
				inode.DecNLink()
				log.LogIfNotNil(mp.inodeTree.Update(inode))
			}
		}
	}
	resp.Msg = item
	return
}

// batch Delete dentry from the dentry tree.
func (mp *MetaPartition) fsmBatchDeleteDentry(db DentryBatch) []*DentryResponse {
	result := make([]*DentryResponse, 0, len(db))
	for _, dentry := range db {
		result = append(result, mp.fsmDeleteDentry(dentry))
	}
	return result
}

func (mp *MetaPartition) fsmUpdateDentry(dentry *Dentry) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	if d, _ := mp.dentryTree.Get(dentry.ParentId, dentry.Name); d != nil {
		d.Inode, dentry.Inode = dentry.Inode, d.Inode
		resp.Msg = dentry
	} else {
		resp.Status = proto.OpNotExistErr
		return
	}
	return

}

func (mp *MetaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	err := mp.dentryTree.Range(&Dentry{ParentId: req.ParentID}, &Dentry{ParentId: req.ParentID + 1}, func(v []byte) (bool, error) {
		d := &Dentry{}
		if err := d.Unmarshal(v); err != nil {
			log.LogErrorf("dentry unmarshal has err:[%s]", err.Error())
			return true, nil
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Name:  d.Name,
			Inode: d.Inode,
			Type:  d.Type,
		})
		return true, nil
	})
	if err != nil {
		log.LogErrorf("read dentry list has err:[%s]", err.Error())
	}
	return
}
