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

func (mp *metaPartition) fsmSetXAttr(extend *Extend, reqInfo *RequestInfo) (err error) {
	var status uint8 = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmSetXAttr: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
		status = previousRespCode
		return
	}
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.Merge(extend, true)
	}

	mp.recordRequest(reqInfo, status)

	return
}

func (mp *metaPartition) fsmRemoveXAttr(extend *Extend, reqInfo *RequestInfo) (err error) {
	var status = proto.OpOk
	if previousRespCode, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
		log.LogCriticalf("fsmRemoveXAttr: dup req:%v, previousRespCode:%v", reqInfo, previousRespCode)
		status = previousRespCode
		return
	}

	defer func() {
		mp.recordRequest(reqInfo, status)
	}()
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		return
	}
	e := treeItem.(*Extend)
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return
}
