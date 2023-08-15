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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(dbHandle interface{}, extend *Extend, reqInfo *RequestInfo) (resp *proto.XAttrRaftResponse, err error) {
	resp = &proto.XAttrRaftResponse{Inode: extend.inode}
	resp.Status = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmSetXAttr: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
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

	if outOfRange, _ := mp.isInoOutOfRange(extend.inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var e *Extend
	e, err = mp.extendTree.Get(extend.inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if e == nil {
		e = NewExtend(extend.inode)
	}

	e.Merge(extend, true)
	if err = mp.extendTree.Put(dbHandle, e); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

func (mp *metaPartition) fsmRemoveXAttr(dbHandle interface{}, extend *Extend, reqInfo *RequestInfo) (resp *proto.XAttrRaftResponse, err error) {
	resp = &proto.XAttrRaftResponse{Inode: extend.inode}
	resp.Status = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmRemoveXAttr: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
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

	if outOfRange, _ := mp.isInoOutOfRange(extend.inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var e *Extend
	e, err = mp.extendTree.Get(extend.inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if e == nil {
		return
	}
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	if err = mp.extendTree.Put(dbHandle, e); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}
