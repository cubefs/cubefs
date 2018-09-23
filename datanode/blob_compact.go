package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"time"
)

var (
	GcanCompact int
)

func (dp *dataPartition) lauchCompact() {
	if GcanCompact != CanCompact {
		return
	}
	if !dp.isLeader {
		return
	}
	blobFile, err := dp.getCompactBlobFiles()
	if err != nil {
		log.LogWarnf(err.Error())
		dp.leaderPutBlobToAvaliCh(blobFile, err)
		return
	}
	if err = dp.notifyCompactBlobFile(blobFile); err != nil {
		log.LogWarnf(err.Error())
		dp.leaderPutBlobToAvaliCh(blobFile, err)
		return
	}
	task := &CompactTask{partitionId: dp.partitionId, blobfileId: blobFile, isLeader: dp.isLeader}
	if dp.disk.hasExsitCompactTask(task.toString()) {
		return
	}
	if err = dp.disk.putCompactTask(task); err != nil {
		log.LogWarnf(err.Error())
		dp.leaderPutBlobToAvaliCh(blobFile, err)
		return
	}
}

func (dp *dataPartition) leaderPutBlobToAvaliCh(blobFile int, err error) {
	if err != nil && blobFile > -1 && blobFile <= storage.BlobFileFileCount {
		dp.blobStore.PutAvailBlobFile(blobFile)
	}
}

func (dp *dataPartition) getCompactKey(blobFile int) string {
	return fmt.Sprintf("CompactID(%v_%v_%v)", dp.partitionId, blobFile, dp.isLeader)
}

func (dp *dataPartition) getCompactBlobFiles() (blobFile int, err error) {
	store := dp.blobStore
	blobFile = -1
	blobFile, err = store.GetAvailBlobFile()
	if err != nil {
		return -1, errors.Annotatef(err, "%v cannot get avalibBlobFile", dp.getCompactKey(-1))
	}
	thresh := -1
	if dp.Status() == proto.ReadOnly {
		rand.Seed(time.Now().UnixNano())
		thresh = rand.Intn(10) + 10
	}
	isready, fileBytes, deleteBytes, deletePercent := store.IsReadyToCompact(blobFile, thresh)
	if !isready {
		err = fmt.Errorf("%v cannot compact  compactThreshold[%v] fileBytes[%v] deleteBytes[%v]"+
			" hasDeletePercnt[%v]", dp.getCompactKey(blobFile), fileBytes, deleteBytes, deletePercent)
		return
	}
	return
}

func (dp *dataPartition) notifyCompactBlobFile(blobFile int) (err error) {
	for i := 1; i < len(dp.replicaHosts); i++ {
		target := dp.replicaHosts[i]
		pkg := NewNotifyCompactPkg(uint32(blobFile), dp.partitionId)
		conn, err := gConnPool.Get(target)
		if err != nil {
			gConnPool.Put(conn,true)
			return fmt.Errorf("%v notify compact package get connect for (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
		if err = pkg.WriteToConn(conn); err != nil {
			gConnPool.Put(conn,true)
			return fmt.Errorf("%v send notify compact package to (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
		if err = pkg.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			gConnPool.Put(conn,true)
			return fmt.Errorf("%v read notify compact response from (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
		gConnPool.Put(conn,false)
	}
	return
}
