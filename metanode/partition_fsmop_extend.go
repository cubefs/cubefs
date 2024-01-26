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
	"fmt"
	"math"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(dbHandle interface{}, extend *Extend) (resp *proto.XAttrRaftResponse, err error) {
	extend.verSeq = mp.GetVerSeq()
	resp = &proto.XAttrRaftResponse{Inode: extend.inode, Status: proto.OpOk}
	var e *Extend
	e, err = mp.extendTree.Get(extend.inode)
	if err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmSetXAttr] failed to get xattr, ino(%v), err(%v)", extend.inode, err)
		return
	}
	if e == nil {
		e = NewExtend(extend.inode)
		goto submit
	}
	if e.verSeq != extend.verSeq {
		if extend.verSeq < e.verSeq {
			resp.Status = proto.OpNotPerm
			err = fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
			return
		}
		e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
		e.verSeq = extend.verSeq
	}
	e.Merge(extend, true)
submit:
	if err = mp.extendTree.Put(dbHandle, e); err != nil {
		log.LogErrorf("[fsmSetXAttr] failed to put xattr, ino(%v), err(%v)", extend.inode, err)
		resp.Status = proto.OpErr
		return
	}
	return
}

// todo(leon chang):check snapshot delete relation with attr
func (mp *metaPartition) fsmRemoveXAttr(dbHandle interface{}, reqExtend *Extend) (resp *proto.XAttrRaftResponse, err error) {
	var e *Extend
	resp = &proto.XAttrRaftResponse{Inode: reqExtend.inode, Status: proto.OpOk}
	e, err = mp.extendTree.Get(reqExtend.inode)
	if err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmRemoveXAttr] failed to get xattr, ino(%v), err(%v)", reqExtend.inode, err)
		return
	}
	if e == nil {
		return
	}

	if mp.GetVerSeq() == 0 || (e.verSeq == mp.GetVerSeq() && reqExtend.verSeq == 0) {
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
		goto submit
	}

	if reqExtend.verSeq == 0 {
		reqExtend.verSeq = mp.GetVerSeq()
	}
	if reqExtend.verSeq == math.MaxUint64 {
		reqExtend.verSeq = 0
	}

	e.versionMu.Lock()
	defer e.versionMu.Unlock()
	if reqExtend.verSeq < e.GetMinVer() {
		return
	}

	mp.multiVersionList.RWLock.RLock()
	defer mp.multiVersionList.RWLock.RUnlock()

	if reqExtend.verSeq > e.verSeq {
		e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
		e.verSeq = reqExtend.verSeq
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
	} else if reqExtend.verSeq == e.verSeq {
		var globalNewVer uint64
		if globalNewVer, err = mp.multiVersionList.GetNextNewerVer(reqExtend.verSeq); err != nil {
			log.LogErrorf("fsmRemoveXAttr. mp[%v] seq [%v] req ver [%v] not found newer seq", mp.config.PartitionId, mp.verSeq, reqExtend.verSeq)
			resp.Status = proto.OpNotPerm
			return
		}
		e.verSeq = globalNewVer
	} else {
		innerLastVer := e.verSeq
		for id, ele := range e.multiVers {
			if ele.verSeq > reqExtend.verSeq {
				innerLastVer = ele.verSeq
				continue
			} else if ele.verSeq < reqExtend.verSeq {
				break
			} else {
				var globalNewVer uint64
				if globalNewVer, err = mp.multiVersionList.GetNextNewerVer(ele.verSeq); err != nil {
					//todo: err code ?
					resp.Status = proto.OpNotPerm
					return
				}
				if globalNewVer < innerLastVer {
					log.LogDebugf("mp[%v] inode[%v] extent layer %v update seq [%v] to %v",
						mp.config.PartitionId, ele.inode, id, ele.verSeq, globalNewVer)
					ele.verSeq = globalNewVer
					break
				}
				e.multiVers = append(e.multiVers[:id], e.multiVers[id+1:]...)
				break
			}
		}
	}
submit:
	if err = mp.extendTree.Put(dbHandle, e); err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmRemoveXAttr] failed to put xattr, ino(%v), err(%v)", reqExtend.inode, err)
		return
	}
	return
}
