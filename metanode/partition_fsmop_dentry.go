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
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"

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
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry, forceUpdate bool) (status uint8, err error) {
	var (
		parIno *Inode
		d	  *Dentry
	)
	status = proto.OpOk

	if err = mp.isInoOutOfRange(dentry.ParentId); err != nil {
		status = proto.OpInodeOutOfRange
		return
	}

	parIno, err = mp.inodeTree.Get(dentry.ParentId)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !forceUpdate {
		if parIno == nil {
			status = proto.OpNotExistErr
			return
		}
		if parIno.ShouldDelete() {
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}
	if d, err = mp.dentryTree.Get(dentry.ParentId, dentry.Name); err == nil && d != nil {
		//do not allow directories and files to overwrite each
		// other when renaming
		if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) {
			status = proto.OpArgMismatchErr
			return
		}

		if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			return
		}

		status = proto.OpExistErr
		return
	}

	if err = mp.dentryTree.Create(dentry, false); err != nil {
		status = proto.OpErr
		return
	}

	if !forceUpdate {
		parIno.IncNLink()
		if err = mp.inodeTree.Update(parIno); err != nil {
			log.LogErrorf("action[fsmCreateDentry] update parent inode err:%v", err)
			status = proto.OpErr
		}
	}
	return
}

// Query a dentry from the dentry tree with specified dentry info.
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8, error) {
	status := proto.OpOk
	if err := mp.isInoOutOfRange(dentry.ParentId); err != nil {
		status = proto.OpInodeOutOfRange
		return nil, status, nil
	}

	d, err := mp.dentryTree.RefGet(dentry.ParentId, dentry.Name)
	if err != nil {
		if err == rocksdbError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[getDentry] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" get dentry failed witch rocksdb error[dentry:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, dentry))
		}
		status = proto.OpErr
		return nil, status, err
	}
	if d == nil {
		status = proto.OpNotExistErr
		return nil, status, nil
	}
	return d, status, nil
}

//todo: lizhenzhen check logical
func (mp *metaPartition) fsmDeleteDentry(dentry *Dentry, timestamp int64, from string, checkInode bool, trashEnable bool) (
	resp *DentryResponse, err error) {
	var (
		d   *Dentry
	)
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(dentry.ParentId); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	d , err = mp.dentryTree.Get(dentry.ParentId, dentry.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if d == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	if checkInode && d.Inode != dentry.Inode{
		resp.Status = proto.OpNotExistErr
		return
	}

	if _, err = mp.dentryTree.Delete(d.ParentId, d.Name); err != nil {
		if err == notExistsError{
			resp.Status = proto.OpNotExistErr
		} else {
			resp.Status = proto.OpErr
		}
		return
	}

	var parIno *Inode
	parIno, err = mp.inodeTree.Get(dentry.ParentId)
	if err != nil {
		log.LogErrorf("action[fsmDeleteDentry] get parent inode(%v) failed:%v", dentry.ParentId, err)
	}
	if parIno != nil {
		if !parIno.ShouldDelete() {
			parIno.DecNLink()
			if err = mp.inodeTree.Update(parIno); err != nil {
				log.LogErrorf("action[fsmDeleteDentry] update parent inode(%v) info failed:%v", dentry.ParentId, err)
				resp.Status = proto.OpErr
				return
			}
		}
	}

	if trashEnable {
		resp.Status, err = mp.mvToDeletedDentryTree(d, timestamp, from)
		if err != nil {
			log.LogErrorf("action[fsmDeleteDentry] move dentry(%v) to deleted dentry tree failed(%v)", d, err)
		}
	}
	resp.Msg = d
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(db DentryBatch, timestamp int64, from string, trashEnable bool) (
	result []*DentryResponse, err error) {
	result = make([]*DentryResponse, 0, len(db))
	wrongIndex := len(db)
	defer func() {
		for index := wrongIndex; index < len(db); index++ {
			result = append(result, &DentryResponse{Status: proto.OpErr, Msg: db[index]})
		}
	}()
	for index, dentry := range db {
		var r *DentryResponse
		r, err = mp.fsmDeleteDentry(dentry, timestamp, from, true, trashEnable)
		if err == rocksdbError {
			wrongIndex = index
			break
		}
		result = append(result, r)
	}
	return
}

func (mp *metaPartition) fsmUpdateDentry(dentry *Dentry, timestamp int64, from string, trashEnable bool) (
	resp *DentryResponse, err error) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(dentry.ParentId); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}
	var d *Dentry
	d , err = mp.dentryTree.Get(dentry.ParentId, dentry.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if d == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	if d.Inode == dentry.Inode {
		//already update, this apply will do nothing
		dentry.Inode = 0
	} else {
		d.Inode, dentry.Inode = dentry.Inode, d.Inode
	}

	if err = mp.dentryTree.Put(d); err != nil {
		resp.Status = proto.OpErr
		return
	}
	resp.Msg = dentry
	return
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
	readDirLimit := ReadDirLimitNum()
	mp.dentryTree.Range(begDentry, endDentry, func(v []byte) (bool, error) {
		d := &Dentry{}
		if err := d.Unmarshal(v); err != nil {
			log.LogErrorf("dentry unmarshal has err:[%s]", err.Error())
			//todo:range error?
			return true, nil
		}
		count += 1
		if req.IsBatch && count > readDirMax {
			resp.NextMarker = d.Name
			return false, nil
		}
		if readDirLimit > 0 && count > readDirLimit {
			log.LogWarnf("readDir: parent(%v) exceeded maximum limit(%v) count(%v)", req.ParentID, readDirLimit, count)
			return false, nil
		}
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true, nil
	})
	tracer.SetTag("count", count).SetTag("total", mp.dentryTree.Count()).SetTag("parentID", req.PartitionID)
	return
}
