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

const innerDirLockKey = "cfs_inner_xattr_dir_lock_key"

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmLockDir(req *proto.LockDirRequest) (resp *proto.LockDirResponse) {
	if req.Lease == 0 {
		return mp.fsmUnlockDir(req)
	}

	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()

	resp = &proto.LockDirResponse{
		Status: proto.OpOk,
		LockId: req.LockId,
	}

	newExpire := req.SubmitTime.Unix() + int64(req.Lease)
	newVal := fmt.Sprintf("%d|%d", req.LockId, newExpire)

	log.LogDebugf("fsmLockDir: req info %s, val %s", req.String(), newVal)

	var newExtend = NewExtend(req.Inode)
	treeItem := mp.extendTree.CopyGet(newExtend)

	var oldValue []byte
	var existExtend *Extend

	if treeItem == nil {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal), 0)
		mp.extendTree.ReplaceOrInsert(newExtend, true)
		return
	}

	existExtend = treeItem.(*Extend)
	oldValue, _ = existExtend.Get([]byte(innerDirLockKey))
	if oldValue == nil {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal), 0)
		existExtend.Merge(newExtend, true)
		return
	}

	var oldLkId, oldExpire int64
	_, err := fmt.Sscanf(string(oldValue), "%d|%d", &oldLkId, &oldExpire)
	if err != nil {
		log.LogErrorf("fsmLockDir: parse req failed, req %s, old %s, err %s", req.String(), string(oldValue), err.Error())
		resp.Status = proto.OpExistErr
		return 
	}

	log.LogDebugf("fsmLockDir: get old lock dir info, req %v, old %d, expire %d", req, oldLkId, oldExpire)

	if req.LockId == oldLkId {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal), 0)
		existExtend.Merge(newExtend, true)
		return
	}

	if req.SubmitTime.Unix() < oldExpire {
		resp.Status = proto.OpExistErr
		return
	}

	newExtend.Put([]byte(innerDirLockKey), []byte(newVal), 0)
	existExtend.Merge(newExtend, true)
	return
}

func (mp *metaPartition) fsmUnlockDir(req *proto.LockDirRequest) (resp *proto.LockDirResponse) {
	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()

	resp = &proto.LockDirResponse{
		Status: proto.OpOk,
		LockId: req.LockId,
	}

	newExpire := req.SubmitTime.Unix() + int64(req.Lease)
	newVal := fmt.Sprintf("%d|%d", req.LockId, newExpire)
	log.LogDebugf("fsmUnlockDir: req info %s, val %s", req, newVal)

	var newExtend = NewExtend(req.Inode)
	treeItem := mp.extendTree.CopyGet(newExtend)

	var oldValue []byte
	var existExtend *Extend

	if treeItem == nil {
		log.LogWarnf("fsmUnlockDir: lock not exist, no need to unlock, req %s", req.String())
		return
	}

	existExtend = treeItem.(*Extend)
	oldValue, _ = existExtend.Get([]byte(innerDirLockKey))
	if oldValue == nil {
		log.LogWarnf("fsmUnlockDir: target lock val not exist, no need to unlock, req %s", req.String())
		return
	}

	var oldLkId, oldExpire int64
	_, err := fmt.Sscanf(string(oldValue), "%d|%d", &oldLkId, &oldExpire)
	if err != nil {
		log.LogErrorf("fsmUnlockDir: parse req failed, req %s, old %s, err %s", req.String(), string(oldValue), err.Error())
		resp.Status = proto.OpExistErr
		return
	}

	log.LogDebugf("fsmUnlockDir: get old lock dir info, req %v, old %d, expire %d", req, oldLkId, oldExpire)

	if req.LockId != oldLkId {
		log.LogWarnf("fsmUnlockDir: already been locked by other, req %v, old %d", req.String(), oldLkId)
		resp.Status = proto.OpExistErr
		return
	}

	existExtend.Remove([]byte(innerDirLockKey))
	return
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
	if mp.GetVerSeq() > 0 {
		extend.setVersion(mp.GetVerSeq())
	}
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		// attr multi-ver copy all attr for simplify management
		e = treeItem.(*Extend)
		if e.getVersion() != extend.getVersion() {
			if extend.getVersion() < e.getVersion() {
				return fmt.Errorf("seq error assign %v but less than %v", extend.getVersion(), e.getVersion())
			}
			e.genSnap()
			e.setVersion(extend.getVersion())
		}
		e.Merge(extend, true)
	}

	return
}

// todo(leon chang):check snapshot delete relation with attr
func (mp *metaPartition) fsmRemoveXAttr(reqExtend *Extend) (err error) {
	treeItem := mp.extendTree.CopyGet(reqExtend)
	if treeItem == nil {
		return
	}

	e := treeItem.(*Extend)
	if mp.GetVerSeq() == 0 || (e.getVersion() == mp.GetVerSeq() && reqExtend.getVersion() == 0) {
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
		return
	}

	if reqExtend.getVersion() == 0 {
		reqExtend.setVersion(mp.GetVerSeq())
	}
	if reqExtend.getVersion() == math.MaxUint64 {
		reqExtend.setVersion(0)
	}
	if e.multiSnap == nil {
		e.multiSnap = &ExtendMultiSnap{}
	}
	e.multiSnap.versionMu.Lock()
	defer e.multiSnap.versionMu.Unlock()

	if reqExtend.getVersion() < e.GetMinVer() {
		return
	}

	mp.multiVersionList.RWLock.RLock()
	defer mp.multiVersionList.RWLock.RUnlock()

	if reqExtend.getVersion() > e.getVersion() {
		e.genSnap()
		e.setVersion(reqExtend.getVersion())
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
	} else if reqExtend.getVersion() == e.getVersion() {
		var globalNewVer uint64
		if globalNewVer, err = mp.multiVersionList.GetNextNewerVer(reqExtend.getVersion()); err != nil {
			log.LogErrorf("fsmRemoveXAttr. mp[%v] seq [%v] req ver [%v] not found newer seq", mp.config.PartitionId, mp.verSeq, reqExtend.getVersion())
			return err
		}
		e.setVersion(globalNewVer)
	} else {
		innerLastVer := e.getVersion()
		if e.multiSnap == nil {
			return
		}
		for id, ele := range e.multiSnap.multiVers {
			if ele.getVersion() > reqExtend.getVersion() {
				innerLastVer = ele.getVersion()
				continue
			} else if ele.getVersion() < reqExtend.getVersion() {
				return
			} else {
				var globalNewVer uint64
				if globalNewVer, err = mp.multiVersionList.GetNextNewerVer(ele.getVersion()); err != nil {
					return err
				}
				if globalNewVer < innerLastVer {
					log.LogDebugf("mp[%v] inode[%v] extent layer %v update seq [%v] to %v",
						mp.config.PartitionId, ele.inode, id, ele.getVersion(), globalNewVer)
					ele.setVersion(globalNewVer)
					return
				}
				e.multiSnap.multiVers = append(e.multiSnap.multiVers[:id], e.multiSnap.multiVers[id+1:]...)
				return
			}
		}
	}

	return
}
