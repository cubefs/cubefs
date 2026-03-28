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
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

type MetaPartition struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
	Region      string // Region name of the leader node

	pingElapsedSortedHosts *util.PingElapsedSortedHosts
}

func (mp *MetaPartition) Less(than btree.Item) bool {
	that := than.(*MetaPartition)
	return mp.Start < that.Start
}

func (mp *MetaPartition) Quorum() int {
	return len(mp.Members)/2 + 1
}

func (mp *MetaPartition) Copy() btree.Item {
	return mp
}

func (mp *MetaPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Start(%v) End(%v) Members(%v) LeaderAddr(%v) Status(%v)", mp.PartitionID, mp.Start, mp.End, mp.Members, mp.LeaderAddr, mp.Status)
}

// SortHostsByPingElapsed sorts meta partition hosts by ping elapsed time.
// This method reuses the PingElapsedSortedHosts implementation from sdk/data/wrapper.
func (mp *MetaPartition) SortHostsByPingElapsed(mw *MetaWrapper) []string {
	if !mw.FollowerRead || !mw.NearRead {
		return mp.Members
	}
	if mp.pingElapsedSortedHosts == nil {
		getHosts := func() []string {
			return mp.Members
		}
		getElapsed := func(host string) (time.Duration, bool) {
			delay, ok := mw.HostLatency.Load(host)
			if !ok {
				return 0, false
			}
			return delay.(time.Duration), true
		}
		mp.pingElapsedSortedHosts = util.NewPingElapsedSortHosts(getHosts, getElapsed)
	}
	return mp.pingElapsedSortedHosts.GetSortedHosts()
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

func (mw *MetaWrapper) getPartitionByInode(ino uint64) *MetaPartition {
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

//func (mw *MetaWrapper) getRWPartitions() []*MetaPartition {
//	rwPartitions := make([]*MetaPartition, 0)
//	mw.RLock()
//	defer mw.RUnlock()
//	for _, mp := range mw.partitions {
//		if mp.Status == proto.ReadWrite {
//			rwPartitions = append(rwPartitions, mp)
//		}
//	}
//	return rwPartitions
//}

func (mw *MetaWrapper) getRWPartitions() []*MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	rwPartitions := mw.rwPartitions
	if len(rwPartitions) == 0 {
		rwPartitions = make([]*MetaPartition, 0)
		for _, mp := range mw.partitions {
			rwPartitions = append(rwPartitions, mp)
		}
	}
	// Filter by clientMetaRegion if specified
	if mw.defaultMetaRegion != "" {
		filtered := make([]*MetaPartition, 0)
		for _, mp := range rwPartitions {
			if mp.Status == proto.ReadWrite && mp.Region == mw.defaultMetaRegion {
				filtered = append(filtered, mp)
			}
		}
		// If no partitions found in specified region, fall back to all rw partitions
		if len(filtered) > 0 {
			return filtered
		}
		log.LogWarnf("getRWPartitions: no writable partitions found in region[%v], falling back to all rw partitions", mw.defaultMetaRegion)
	}
	return rwPartitions
}
