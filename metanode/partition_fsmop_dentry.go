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
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"strings"



	"github.com/cubefs/cubefs/proto"
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
func (mp *metaPartition) fsmCreateDentry(dbHandle interface{}, dentry *Dentry, forceUpdate bool, reqInfo *RequestInfo) (status uint8, err error) {
	var (
		parIno *Inode
		d      *Dentry
		ok     bool
	)
	status = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmCreateDentry: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
		status = previousRespCode
		return
	}
	defer func() {
		if err != nil {
			return
		}
		mp.recordRequest(reqInfo, status)
		mp.persistRequestInfoToRocksDB(dbHandle, reqInfo)
	}()

	if outOfRange, _ := mp.isInoOutOfRange(dentry.ParentId); outOfRange && !forceUpdate {
		status = proto.OpInodeOutOfRange
		return
	}

	parIno, err = mp.inodeTree.Get(dentry.ParentId)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !forceUpdate {
		if parIno == nil || parIno.ShouldDelete(){
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}
	if d, ok, err = mp.dentryTree.Create(dbHandle, dentry, false); err != nil {
		status = proto.OpErr
		return
	}

	if !ok {
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

	if !forceUpdate {
		parIno.IncNLink()
		if err = mp.inodeTree.Update(dbHandle, parIno); err != nil {
			log.LogErrorf("action[fsmCreateDentry] update parent inode err:%v", err)
			status = proto.OpErr
		}
	}
	return
}

// Query a dentry from the dentry tree with specified dentry info.
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8, error) {
	status := proto.OpOk
	if _, err := mp.isInoOutOfRange(dentry.ParentId); err != nil {
		return nil, proto.OpInodeOutOfRange, err
	}

	d, err := mp.dentryTree.RefGet(dentry.ParentId, dentry.Name)
	if err != nil {
		if err == rocksDBError {
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

func (mp *metaPartition) fsmDeleteDentry(dbHandle interface{}, dentry *Dentry, timestamp int64, from string, checkInode bool, trashEnable bool, reqInfo *RequestInfo) (
	resp *DentryResponse, err error) {
	var (
		d      *Dentry
		ok     bool
		parIno *Inode
	)
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmDeleteDentry: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
		resp.Status = previousRespCode
		return
	}
	defer func() {
		if err != nil {
			return
		}
		mp.recordRequest(reqInfo, resp.Status)
		mp.persistRequestInfoToRocksDB(dbHandle, reqInfo)
	}()

	if outOfRange, _ := mp.isInoOutOfRange(dentry.ParentId); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	d, err = mp.dentryTree.Get(dentry.ParentId, dentry.Name)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if d == nil || (checkInode && d.Inode != dentry.Inode){
		resp.Status = proto.OpNotExistErr
		return
	}

	if ok, err = mp.dentryTree.Delete(dbHandle, d.ParentId, d.Name); err != nil {
		resp.Status = proto.OpErr
		return
	}

	if !ok {
		resp.Status = proto.OpNotExistErr
		return
	}

	if parIno, err = mp.inodeTree.Get(dentry.ParentId); err != nil {
		log.LogErrorf("action[fsmDeleteDentry] get parent inode(%v) failed:%v", dentry.ParentId, err)
	}
	if parIno != nil {
		if !parIno.ShouldDelete() {
			parIno.DecNLink()
			if err = mp.inodeTree.Update(dbHandle, parIno); err != nil {
				log.LogErrorf("action[fsmDeleteDentry] update parent inode(%v) info failed:%v", dentry.ParentId, err)
				resp.Status = proto.OpErr
				return
			}
		}
	}

	if trashEnable {
		resp.Status, err = mp.mvToDeletedDentryTree(dbHandle, d, timestamp, from)
		if err != nil {
			log.LogErrorf("action[fsmDeleteDentry] move dentry(%v) to deleted dentry tree failed(%v)", d, err)
		}
	}
	resp.Msg = d
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(dbHandle interface{}, batchDentry DentryBatch, timestamp int64, from string, trashEnable bool) (
	result []*DentryResponse, err error) {
	result = make([]*DentryResponse, 0, len(batchDentry))

	var wrongIndex int
	defer func() {
		if err != nil {
			for index := wrongIndex; index < len(batchDentry); index++ {
				result = append(result, &DentryResponse{Status: proto.OpErr, Msg: batchDentry[index]})
			}
		}
	}()

	for index, dentry := range batchDentry {
		var (
			rsp           *DentryResponse
			dbWriteHandle interface{}
		)
		dbWriteHandle, err = mp.dentryTree.CreateBatchWriteHandle()
		if err != nil {
			wrongIndex = index
			break
		}
		rsp, err = mp.fsmDeleteDentry(dbWriteHandle, dentry, timestamp, from, true, trashEnable, nil)
		if err != nil {
			_ = mp.dentryTree.ReleaseBatchWriteHandle(dbWriteHandle)
			wrongIndex = index
			break
		}
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, false)
		if err != nil {
			wrongIndex = index
			break
		}
		result = append(result, rsp)
	}
	return
}

func (mp *metaPartition) fsmUpdateDentry(dbHandle interface{}, dentry *Dentry, timestamp int64, from string, trashEnable bool) (
	resp *DentryResponse, err error) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(dentry.ParentId); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}
	var d *Dentry
	d, err = mp.dentryTree.Get(dentry.ParentId, dentry.Name)
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

	if err = mp.dentryTree.Update(dbHandle, d); err != nil {
		resp.Status = proto.OpErr
		return
	}
	resp.Msg = dentry
	return
}

func (mp *metaPartition) readDir(ctx context.Context, req *ReadDirReq) (resp *ReadDirResp, err error) {

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
	err = mp.dentryTree.RangeWithPrefix(&Dentry{ParentId: req.ParentID}, begDentry, endDentry, func(d *Dentry) (bool, error) {
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
	if err != nil {
		log.LogErrorf("readDir failed:[%s]", err.Error())
		return
	}
	return
}
