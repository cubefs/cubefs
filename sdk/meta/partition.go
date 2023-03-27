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

package meta

import (
	"context"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
)

type MetaPartition struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	Learners    []string
	LeaderAddr  proto.AtomicString
	Status      int8
}

func (this *MetaPartition) Less(than btree.Item) bool {
	that := than.(*MetaPartition)
	return this.Start < that.Start
}

func (mp *MetaPartition) Copy() btree.Item {
	return mp
}

func (mp *MetaPartition) String() string {
	if mp == nil {
		return ""
	}
	return fmt.Sprintf("PartitionID(%v) Start(%v) End(%v) Members(%v) Learners(%v) LeaderAddr(%v) Status(%v)",
		mp.PartitionID, mp.Start, mp.End, mp.Members, mp.Learners, mp.GetLeaderAddr(), mp.Status)
}

func (mp *MetaPartition) GetLeaderAddr() string {
	str, _ := mp.LeaderAddr.Load().(string)
	return str
}

func (mp *MetaPartition) SetLeaderAddr(addr string) {
	mp.LeaderAddr.Store(addr)
}

func (mw *MetaWrapper) ClearRWPartitions() {
	mw.RLock()
	defer mw.RUnlock()
	mw.rwPartitions = mw.rwPartitions[:0]
}

// Meta partition managements
//

func (mw *MetaWrapper) addPartition(mp *MetaPartition) {
	mw.partitions[mp.PartitionID] = mp
	mw.ranges.ReplaceOrInsert(mp)
}

func (mw *MetaWrapper) deletePartition(mp *MetaPartition) {
	delete(mw.partitions, mp.PartitionID)
	mw.ranges.Delete(mp)
}

func (mw *MetaWrapper) replaceOrInsertPartition(mp *MetaPartition) {
	mw.Lock()
	defer mw.Unlock()

	found, ok := mw.partitions[mp.PartitionID]
	if ok {
		mw.deletePartition(found)
	}

	mw.addPartition(mp)
	return
}

func (mw *MetaWrapper) getPartitionByID(id uint64) *MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	mp, ok := mw.partitions[id]
	if !ok {
		return nil
	}
	return mp
}

func (mw *MetaWrapper) getPartitionByIDWithAutoRefresh(id uint64) *MetaPartition {
	var mp = mw.getPartitionByID(id)
	if mp == nil {
		mw.triggerAndWaitForceUpdate()
		mp = mw.getPartitionByID(id)
	}
	return mp
}

func (mw *MetaWrapper) getPartitionByInode(ctx context.Context, ino uint64) *MetaPartition {
	var mp *MetaPartition
	mw.RLock()
	defer mw.RUnlock()

	pivot := &MetaPartition{Start: ino}
	mw.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		mp = i.(*MetaPartition)
		if ino > mp.End || ino < mp.Start {
			mp = nil
		}
		// Iterate one item is enough
		return false
	})

	return mp
}

func (mw *MetaWrapper) getRWPartitions() []*MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	tempPartitions := mw.rwPartitions
	return tempPartitions
}

func (mw *MetaWrapper) getUnavailPartitions() []*MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	tempPartitions := mw.unavailPartitions
	return tempPartitions
}

func (mw *MetaWrapper) getPartitions() []*MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	tempPartitions := make([]*MetaPartition, 0, len(mw.partitions))
	for _, mp := range mw.partitions {
		tempPartitions = append(tempPartitions, mp)
	}
	return tempPartitions
}

func (mw *MetaWrapper) getRefreshMp(ctx context.Context, inode uint64) *MetaPartition {
	mw.triggerAndWaitForceUpdate()
	return mw.getPartitionByInode(ctx, inode)
}

// GetConnect the partition whose Start is Larger than ino.
// Return nil if no successive partition.
//func (mw *MetaWrapper) getNextPartition(ino uint64) *MetaPartition {
//	var mp *MetaPartition
//	mw.RLock()
//	defer mw.RUnlock()
//
//	pivot := &MetaPartition{Start: ino + 1}
//	mw.ranges.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
//		mp = i.(*MetaPartition)
//		return false
//	})
//
//	return mp
//}
//
//func (mw *MetaWrapper) getLatestPartition() *MetaPartition {
//	mw.RLock()
//	defer mw.RUnlock()
//	return mw.ranges.Max().(*MetaPartition)
//}
