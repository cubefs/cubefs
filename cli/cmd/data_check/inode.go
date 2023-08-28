package data_check

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync"
)

func (checkEngine *CheckEngine) checkInodeCrc(inode uint64, mp *proto.MetaPartitionView) {
	var (
		err         error
		extentsResp *proto.GetExtentsResponse
		errCount    = 0
		wg          sync.WaitGroup
	)
	mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], checkEngine.mnProf), false)
	extentsResp, err = mtClient.GetExtentsByInode(mp.PartitionID, inode)
	if err != nil {
		checkEngine.onFail(CheckCrcFailInode, fmt.Sprintf("%v %v %v", checkEngine.currentVol, mp.PartitionID, inode))
		log.LogErrorf("CheckFail-Inode, cluster:%s, vol:%s, inode: %d, err:%v", checkEngine.cluster, checkEngine.currentVol, inode, err)
		return
	}
	log.LogInfof("begin check inode, cluster:%s, vol:%s, inode: %d, extent count: %d", checkEngine.cluster, checkEngine.currentVol, inode, len(extentsResp.Extents))
	checkExtents := make([]proto.ExtentKey, 0)
	for _, ek := range extentsResp.Extents {
		if len(checkEngine.specifyDps) > 0 && !idExist(ek.PartitionId, checkEngine.specifyDps) {
			continue
		}
		if (checkEngine.tinyOnly && proto.IsTinyExtent(ek.ExtentId)) || (!checkEngine.tinyOnly && !proto.IsTinyExtent(ek.ExtentId)) {
			checkExtents = append(checkExtents, ek)
		}
	}
	ekCh := make(chan proto.ExtentKey)
	wg.Add(len(checkExtents))
	go func() {
		for _, ek := range checkExtents {
			ekCh <- ek
		}
		close(ekCh)
	}()
	//extent may be duplicated in extentsResp.Extents
	for i := 0; i < int(checkEngine.concurrency); i++ {
		go func(ino uint64) {
			for ek := range ekCh {
				checkEngine.checkInodeEkCrc(ek, ino)
				wg.Done()
			}
		}(inode)
	}
	wg.Wait()
	log.LogInfof("finish check, cluster:%s, vol:%s, inode: %d, err count: %d\n", checkEngine.cluster, checkEngine.currentVol, inode, errCount)
}

func (checkEngine *CheckEngine) checkInodeEkCrc(ek proto.ExtentKey, ino uint64) {
	var err error
	var ekStr string
	defer func() {
		if err != nil {
			checkEngine.onFail(CheckCrcFailExtent, fmt.Sprintf("%v %v %v", checkEngine.currentVol, ek.PartitionId, ek.ExtentId))
			log.LogErrorf("CheckFail-Extent, cluster:%s, dp:%v, extent:%v, err:%v", checkEngine.cluster, ek.PartitionId, ek.ExtentId, err)
		}
	}()
	if !checkEngine.tinyOnly {
		if checkEngine.tinyInUse {
			ekStr = fmt.Sprintf("%d-%d-%d-%d", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size)
		} else {
			ekStr = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
		}
		if _, ok := checkEngine.checkedExtentsMap.LoadOrStore(ekStr, true); ok {
			return
		}
	}
	var partition *proto.DataPartitionInfo
	for j := 0; j == 0 || j < 3 && err != nil; j++ {
		partition, err = checkEngine.mc.AdminAPI().GetDataPartition("", ek.PartitionId)
	}
	if err != nil {
		return
	}
	if partition == nil {
		err = fmt.Errorf("partition not exists")
		return
	}
	err = CheckExtentReplicaInfo(checkEngine.mc.Nodes()[0], checkEngine.mc.DataNodeProfPort, partition.Replicas, &ek, ino, checkEngine.currentVol, checkEngine.repairPersist.RepairPersistCh, checkEngine.tinyOnly)
	return
}

func locateMpByInode(mps []*proto.MetaPartitionView, inode uint64) *proto.MetaPartitionView {
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			return mp
		}
	}
	return nil
}

func (checkEngine *CheckEngine) checkInodeEkNum(inode uint64, mp *proto.MetaPartitionView) {
	eks := make([]*proto.GetExtentsResponse, len(mp.Members))
	for i, host := range mp.Members {
		mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], checkEngine.mnProf), false)
		extents, err := mtClient.GetExtentsByInode(mp.PartitionID, inode)
		if err != nil {
			return
		}
		eks[i] = extents
	}
	for i := 1; i < len(mp.Members); i++ {
		if len(eks[i].Extents) != len(eks[i-1].Extents) {
			fmt.Printf("checkInodeEkNum ERROR, inode: %d, host: %s, eks len: %d, host: %s, eks len: %d", inode, mp.Members[i-1], len(eks[i-1].Extents), mp.Members[i], len(eks[i].Extents))
		}
	}
}

func (checkEngine *CheckEngine) checkInodes(mps []*proto.MetaPartitionView, inodes []uint64) {
	var wg sync.WaitGroup
	inoCh := make(chan uint64, 1000*1000)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()
	for i := 0; i < int(checkEngine.concurrency); i++ {
		go func() {
			for ino := range inoCh {
				if checkEngine.isStop() {
					wg.Done()
					continue
				}
				mp := locateMpByInode(mps, ino)
				if checkEngine.checkType == CheckTypeInodeEkNum {
					checkEngine.checkInodeEkNum(ino, mp)
				} else {
					checkEngine.checkInodeCrc(ino, mp)
				}
				wg.Done()
			}
		}()
	}
	wg.Wait()
}

func idExist(id uint64, ids []uint64) bool {
	if len(ids) == 0 {
		return false
	}
	for _, i := range ids {
		if i == id {
			return true
		}
	}
	return false
}
