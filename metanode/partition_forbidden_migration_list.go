package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"time"
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
	for {
		select {
		case <-mp.stopC:
			log.LogDebugf("[metaPartition] checkForbiddenMigrationWorker stop partition: %v",
				mp.config.PartitionId)
			return
		case <-t.C:
			if _, isLeader = mp.IsLeader(); !isLeader {
				log.LogDebugf("[checkForbiddenMigrationWorker] mp %v nodeId not leader, quit",
					mp.config.PartitionId, mp.config.NodeId)
				return
			}
			inos := mp.fmList.getExpiredForbiddenMigrationInodes()
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
			bufSlice := make([]byte, 0, 8*len(freeInodes))
			for _, inode := range freeInodes {
				bufSlice = append(bufSlice, inode.MarshalKey()...)
			}
			log.LogDebugf("[checkForbiddenMigrationWorker] mp %v sync %v to follower",
				mp.config.PartitionId, freeInodes)
			err := mp.syncToRaftFollowersFreeForbiddenMigrationInode(bufSlice)
			if err != nil {
				log.LogWarnf("[checkForbiddenMigrationWorker] raft commit inode list: %v, "+
					"response %s", freeInodes, err.Error())
			}
			for _, inode := range freeInodes {
				if err == nil {
					mp.freeForbiddenMigrationInode(inode)
				} else {
					mp.fmList.Put(inode.Inode)
				}
			}
		}

	}
}

func (mp *metaPartition) refreshForbiddenMigrationList() {
	log.LogDebugf("[refreshForbiddenMigrationList] pid: %v HandleLeaderChange become leader "+
		" nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId)
	allInos := mp.fmList.getAllForbiddenMigrationInodes()
	for _, ino := range allInos {
		mp.fmList.Put(ino)
	}
	mp.startManageForbiddenMigrationList()
}
