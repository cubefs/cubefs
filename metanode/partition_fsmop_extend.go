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
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmLockDir(req *proto.LockDirRequest) (resp *proto.LockDirResponse) {
	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()
	resp = &proto.LockDirResponse{}

	ino := req.Inode
	lockId := req.LockId
	submitTime := req.SubmitTime
	lease := req.Lease

	log.LogDebugf("fsmLockDir ino=%v, lockId=%d, submitTime=%v, lease=%d\n", ino, lockId, submitTime, lease)

	newExtend := NewExtend(ino)
	treeItem := mp.extendTree.CopyGet(newExtend)

	var firstLock bool = false
	var oldValue []byte
	var existExtend *Extend

	if treeItem == nil {
		firstLock = true
	} else {
		existExtend = treeItem.(*Extend)
		oldValue, _ = existExtend.Get([]byte("dir_lock"))
		if oldValue == nil {
			firstLock = true
		}
	}

	lockIdStr := strconv.Itoa(int(lockId))
	validTime := submitTime.Add(time.Duration(int(lease)) * time.Second)
	validTimeStr := validTime.Format("2006-01-02 15:04:05")
	value := lockIdStr + "|" + validTimeStr

	if firstLock {
		// first time lock dir
		log.LogDebugf("fsmLockDir first time\n")
		uu_id, _ := uuid.NewRandom()
		lockId = int64(uu_id.ID())
		lockIdStr = strconv.Itoa(int(lockId))
		value = lockIdStr + "|" + validTimeStr
		newExtend.Put([]byte("dir_lock"), []byte(value), 0)
		mp.extendTree.ReplaceOrInsert(newExtend, true)
	} else {
		log.LogDebugf("fsmLockDir oldValue=%s\n", oldValue)
		renewDirLock := false
		lockExpired := false
		parts := strings.Split(string(oldValue), "|")
		oldLockIdStr := parts[0]
		oldValidTimeStr := parts[1]
		oldValidTime, _ := time.Parse("2006-01-02 15:04:05", oldValidTimeStr)
		renewDirLock = (oldLockIdStr == lockIdStr)

		// convert time before compare (CST/UTC)
		submit_time, _ := time.Parse("2006-01-02 15:04:05", submitTime.Format("2006-01-02 15:04:05"))
		lockExpired = submit_time.After(oldValidTime)

		if !renewDirLock && !lockExpired {
			resp.Status = proto.OpExistErr
			log.LogDebugf("fsmLockDir failed, dir has been locked by others and in lease\n")
			return
		}

		if lockExpired {
			// if lock expired, use new lockId
			uu_id, _ := uuid.NewRandom()
			lockId = int64(uu_id.ID())
			lockIdStr = strconv.Itoa(int(lockId))
			value = lockIdStr + "|" + validTimeStr
		}

		existExtend.Remove([]byte("dir_lock"))
		newExtend.Put([]byte("dir_lock"), []byte(value), 0)
		existExtend.Merge(newExtend, true)
	}

	resp.Status = proto.OpOk
	resp.LockId = lockId
	log.LogDebugf("fsmLockDir success lockId=%d\n", lockId)
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
