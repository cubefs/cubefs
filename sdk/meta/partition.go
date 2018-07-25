package meta

import (
	"fmt"

	"github.com/tiglabs/baudstorage/util/btree"

	"github.com/tiglabs/baudstorage/proto"
)

type MetaPartition struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

func (this *MetaPartition) Less(than btree.Item) bool {
	that := than.(*MetaPartition)
	return this.Start < that.Start
}

func (mp *MetaPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Start(%v) End(%v) Members(%v) LeaderAddr(%v) Status(%v)", mp.PartitionID, mp.Start, mp.End, mp.Members, mp.LeaderAddr, mp.Status)
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

func (mw *MetaWrapper) getRWPartitions() []*MetaPartition {
	rwPartitions := make([]*MetaPartition, 0)
	mw.RLock()
	defer mw.RUnlock()
	for _, mp := range mw.partitions {
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		}
	}
	return rwPartitions
}

// Get the partition whose Start is Larger than ino.
// Return nil if no successive partition.
func (mw *MetaWrapper) getNextPartition(ino uint64) *MetaPartition {
	var mp *MetaPartition
	mw.RLock()
	defer mw.RUnlock()

	pivot := &MetaPartition{Start: ino + 1}
	mw.ranges.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		mp = i.(*MetaPartition)
		return false
	})

	return mp
}

func (mw *MetaWrapper) getLatestPartition() *MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	return mw.ranges.Max().(*MetaPartition)
}
