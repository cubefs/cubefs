package metanode

import (
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ApproverToMigration  uint32 = 0
	ForbiddenToMigration uint32 = 1
)

func (mp *metaPartition) startManageForbiddenMigrationList() {
	go mp.checkForbiddenMigrationWorker()
}

func (mp *metaPartition) checkForbiddenMigrationWorker() {
	var (
		t        = time.NewTicker(proto.ForbiddenMigrationRenewalPeriod / 2)
		isLeader bool
	)
	defer t.Stop()
	log.LogDebugf("[checkForbiddenMigrationWorker] mp %v run", mp.config.PartitionId)
	for {
		select {
		case <-mp.stopC:
			log.LogDebugf("[metaPartition] checkForbiddenMigrationWorker stop partition: %v",
				mp.config.PartitionId)
			return
		case <-t.C:
			if _, isLeader = mp.IsLeader(); !isLeader {
				log.LogDebugf("[checkForbiddenMigrationWorker] mp %v nodeId not leader, quit",
					mp.config.PartitionId)
				return
			}
			log.LogDebugf("[checkForbiddenMigrationWorker] mp %v getExpiredForbiddenMigrationInodes begin", mp.config.PartitionId)
			inos := mp.fmList.getExpiredForbiddenMigrationInodes(mp.config.PartitionId)
			log.LogDebugf("[checkForbiddenMigrationWorker] mp %v getExpiredForbiddenMigrationInodes end", mp.config.PartitionId)
			//
			freeInodes := make([]*Inode, 0)
			for _, ino := range inos {
				ref := &Inode{Inode: ino}
				inode, ok := mp.inodeTree.Get(ref).(*Inode)
				if !ok {
					log.LogDebugf("[checkForbiddenMigrationWorker] . mp %v inode [%v] not found",
						mp.config.PartitionId, ino)
					continue
				}
				freeInodes = append(freeInodes, inode)
			}

			batchSize := 1024
			totalInodes := len(freeInodes)
			bufSlice := make([]byte, 0, 8*batchSize)

			for i := 0; i < totalInodes; i += batchSize {
				end := i + batchSize
				if end > totalInodes {
					end = totalInodes
				}
				batch := freeInodes[i:end]
				for _, inode := range batch {
					bufSlice = append(bufSlice, inode.MarshalKey()...)
				}
				log.LogDebugf("[checkForbiddenMigrationWorker] mp %v sync %v to follower",
					mp.config.PartitionId, len(batch))
				err := mp.syncToRaftFollowersFreeForbiddenMigrationInode(bufSlice)
				if err != nil {
					log.LogWarnf("[checkForbiddenMigrationWorker] raft commit inode list: %v, "+
						"response %s", len(batch), err.Error())
				}
				for _, inode := range batch {
					if err == nil {
						mp.freeForbiddenMigrationInode(inode)
					} else {
						mp.fmList.Put(inode.Inode)
					}
				}
				bufSlice = bufSlice[:0]
			}
		}
	}
}

func (mp *metaPartition) refreshForbiddenMigrationList() {
	log.LogDebugf("[refreshForbiddenMigrationList] pid: %v HandleLeaderChange become leader "+
		" nodeId: %v ", mp.config.PartitionId, mp.config.NodeId)
	allInos := mp.fmList.getAllForbiddenMigrationInodes(mp.config.PartitionId)
	for _, ino := range allInos {
		mp.fmList.Put(ino)
	}
	mp.startManageForbiddenMigrationList()
}
