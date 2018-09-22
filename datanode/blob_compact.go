package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"time"
)

func (dp *dataPartition) lauchCompact() {
	blobFile, err := dp.getCompactBlobFiles()
	if err != nil {
		log.LogWarnf(err.Error())
		return
	}
	if err = dp.notifyCompactBlobFile(blobFile); err != nil {
		log.LogWarnf(err.Error())
		return
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
			return fmt.Errorf("%v notify compact package get connect for (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
		if err = pkg.WriteToConn(conn); err != nil {
			return fmt.Errorf("%v send notify compact package to (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
		if err = pkg.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			return fmt.Errorf("%v read notify compact response from (%v) failed (%v)",
				dp.getCompactKey(blobFile), target, err.Error())
		}
	}
}
