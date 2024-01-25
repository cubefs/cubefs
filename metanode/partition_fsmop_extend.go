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
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.Merge(extend, true)
	}

	return
}

func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) (err error) {
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
