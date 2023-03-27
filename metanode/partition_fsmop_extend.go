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

import "github.com/chubaofs/chubaofs/proto"

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(dbHandle interface{}, extend *Extend) (resp *proto.XAttrRaftResponse, err error) {
	resp = &proto.XAttrRaftResponse{Inode: extend.inode}
	resp.Status = proto.OpOk

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

func (mp *metaPartition) fsmRemoveXAttr(dbHandle interface{}, extend *Extend) (resp *proto.XAttrRaftResponse, err error) {
	resp = &proto.XAttrRaftResponse{Inode: extend.inode}
	resp.Status = proto.OpOk

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
