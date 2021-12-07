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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
)

type fsmOpDeletedDentryResponse struct {
	Status uint8          `json:"st"`
	Msg    *DeletedDentry `json:"den"`
}

func (mp *metaPartition) mvToDeletedDentryTree(dentry *Dentry, timestamp int64, from string) (status uint8) {
	ddentry := newDeletedDentry(dentry, timestamp, from)
	return mp.fsmCreateDeletedDentry(ddentry, false).Status
}

func (mp *metaPartition) fsmCreateDeletedDentry(ddentry *DeletedDentry, force bool) (rsp *fsmOpDeletedDentryResponse) {
	rsp = new(fsmOpDeletedDentryResponse)
	rsp.Status = proto.OpOk
	item, ok := mp.dentryDeletedTree.ReplaceOrInsert(ddentry, force)
	if !ok {
		d := item.(*DeletedDentry)
		if d.ParentId == ddentry.ParentId &&
			strings.Compare(d.Name, ddentry.Name) == 0 &&
			d.Timestamp == ddentry.Timestamp &&
			d.Inode == ddentry.Inode {
			return
		}
		rsp.Status = proto.OpErr
	}
	return
}

func (mp *metaPartition) fsmBatchCleanDeletedDentry(dens DeletedDentryBatch) (
	res []*fsmOpDeletedDentryResponse) {
	res = make([]*fsmOpDeletedDentryResponse, 0)
	for _, den := range dens {
		rsp := mp.cleanDeletedDentry(den)
		if rsp.Status != proto.OpOk {
			res = append(res, rsp)
		}
	}
	return
}

func (mp *metaPartition) fsmCleanDeletedDentry(dd *DeletedDentry) (resp *fsmOpDeletedDentryResponse) {
	return mp.cleanDeletedDentry(dd)
}

func (mp *metaPartition) cleanDeletedDentry(ddentry *DeletedDentry) (
	resp *fsmOpDeletedDentryResponse) {
	resp = new(fsmOpDeletedDentryResponse)
	resp.Msg = ddentry
	resp.Status = proto.OpOk

	defer func() {
		log.LogDebugf("[cleanDeletedDentry], dentry: %v, status: %v", ddentry, resp.Status)
	}()

	if len(ddentry.Name) == 0 {
		log.LogErrorf("[cleanDeletedDentry], not found name: %v", ddentry)
		resp.Status = proto.OpErr
		return
	}

	item := mp.dentryDeletedTree.CopyGet(ddentry)
	if item == nil {
		log.LogErrorf("[cleanDeletedDentry], not found dentry: %v", ddentry)
		resp.Status = proto.OpNotExistErr
		return
	}

	if item.(*DeletedDentry).Inode != ddentry.Inode {
		log.LogErrorf("[cleanDeletedDentry], not found dentry: %v, item: %v", ddentry, item.(*DeletedDentry))
		resp.Status = proto.OpNotExistErr
	}
	resp.Msg = item.(*DeletedDentry)
	mp.dentryDeletedTree.Delete(item.(*DeletedDentry))
	return
}

func (mp *metaPartition) fsmRecoverDeletedDentry(ddentry *DeletedDentry) (
	resp *fsmOpDeletedDentryResponse) {
	resp = new(fsmOpDeletedDentryResponse)
	resp.Msg = ddentry
	resp.Status = proto.OpOk

	defer func() {
		log.LogDebugf("[fsmRecoverDeletedDentry], dentry: %v, status: %v", ddentry, resp.Status)
	}()

	if len(ddentry.Name) == 0 {
		log.LogErrorf("[fsmRecoverDeletedDentry], not found name in  dentry: %v", ddentry)
		resp.Status = proto.OpErr
		return
	}
	den := ddentry.buildDentry()
	d := mp.dentryDeletedTree.Get(ddentry)
	if d == nil {
		item := mp.dentryTree.Get(den)
		if item != nil {
			return
		}
		den.Name = appendTimestampToName(ddentry.Name, ddentry.Timestamp)
		item = mp.dentryTree.Get(den)
		if item != nil {
			return
		}
		resp.Status = proto.OpNotExistErr
		log.LogErrorf("[fsmRecoverDeletedDentry], not found dentry: %v", ddentry)
		return
	}
	if d.(*DeletedDentry).Inode != ddentry.Inode {
		log.LogErrorf("[fsmRecoverDeletedDentry], den: %v, %v", ddentry, d.(*DeletedDentry))
		resp.Status = proto.OpNotExistErr
		return
	}

	item := mp.dentryTree.Get(den)
	if item != nil {
		newDDentry := d.Copy().(*DeletedDentry)
		newDDentry.appendTimestampToName()
		d := newDDentry.buildDentry()
		item = mp.dentryTree.CopyGet(d)
		if item != nil {
			log.LogErrorf("[fsmRecoverDeletedDentry], the dentry: %v has been exist", newDDentry)
			resp.Status = proto.OpExistErr
			return
		}

		resp.Status = mp.fsmCreateDentry(d, false)
		resp.Msg = newDDentry
		if resp.Status != proto.OpOk {
			log.LogErrorf("[fsmRecoverDeletedDentry], failed to create dentry: %v, status: %v", newDDentry, resp.Status)
			return
		}
	} else {
		entry := d.(*DeletedDentry).buildDentry()
		resp.Status = mp.fsmCreateDentry(entry, false)
		if resp.Status != proto.OpOk {
			log.LogErrorf("[fsmRecoverDeletedDentry], failed to create dentry: %v, status: %v ", entry, resp.Status)
			return
		}
		resp.Msg = d.(*DeletedDentry)
	}
	mp.dentryDeletedTree.Delete(ddentry)
	return
}

func (mp *metaPartition) fsmBatchRecoverDeletedDentry(dens DeletedDentryBatch) (
	res []*fsmOpDeletedDentryResponse) {
	res = make([]*fsmOpDeletedDentryResponse, 0)
	for _, den := range dens {
		rsp := mp.fsmRecoverDeletedDentry(den)
		if rsp.Status != proto.OpOk {
			res = append(res, rsp)
		}
	}
	return
}

func (mp *metaPartition) fsmCleanExpiredDentry(dens DeletedDentryBatch) (
	res []*fsmOpDeletedDentryResponse) {
	res = make([]*fsmOpDeletedDentryResponse, 0)
	for _, den := range dens {
		rsp := mp.cleanDeletedDentry(den)
		if rsp.Status != proto.OpOk {
			res = append(res, rsp)
		}
	}
	return
}
