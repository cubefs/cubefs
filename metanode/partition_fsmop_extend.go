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
	"strconv"
	"strings"
	"time"
	"fmt"
	"math"

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

	var newExtend = NewExtend(ino)
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
		newExtend.Put([]byte("dir_lock"), []byte(value))
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
		newExtend.Put([]byte("dir_lock"), []byte(value))
		existExtend.Merge(newExtend, true)
	}

	resp.Status = proto.OpOk
	resp.LockId = lockId
	log.LogDebugf("fsmLockDir success lockId=%d\n", lockId)
	return
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
	extend.verSeq = mp.GetVerSeq()
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		// attr multi-ver copy all attr for simplify management
		e = treeItem.(*Extend)
		if e.verSeq != extend.verSeq {
			if extend.verSeq < e.verSeq {
				return fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
			}
			e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
			e.verSeq = extend.verSeq
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
	if mp.GetVerSeq() == 0 || (e.verSeq == mp.GetVerSeq() && reqExtend.verSeq == 0) {
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
		return
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
			return err
		}
		e.verSeq = globalNewVer
	} else {
		innerLastVer := e.verSeq
		for id, ele := range e.multiVers {
			if ele.verSeq > reqExtend.verSeq {
				innerLastVer = ele.verSeq
				continue
			} else if ele.verSeq < reqExtend.verSeq {
				return
			} else {
				var globalNewVer uint64
				if globalNewVer, err = mp.multiVersionList.GetNextNewerVer(ele.verSeq); err != nil {
					return err
				}
				if globalNewVer < innerLastVer {
					log.LogDebugf("mp[%v] inode[%v] extent layer %v update seq [%v] to %v",
						mp.config.PartitionId, ele.inode, id, ele.verSeq, globalNewVer)
					ele.verSeq = globalNewVer
					return
				}
				e.multiVers = append(e.multiVers[:id], e.multiVers[id+1:]...)
				return
			}
		}
	}

	return
}
