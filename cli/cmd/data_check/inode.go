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
		wg          sync.WaitGroup
	)
	mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], checkEngine.mnProf), false)
	extentsResp, err = mtClient.GetExtentKeyByInodeId(mp.PartitionID, inode)
	if err != nil {
		checkEngine.onCheckFail(CheckFailInode, fmt.Sprintf("%v %v %v", checkEngine.currentVol, mp.PartitionID, inode))
		log.LogErrorf("CheckFail-Inode, cluster:%s, vol:%s, inode: %d, err:%v", checkEngine.cluster, checkEngine.currentVol, inode, err)
		return
	}
	log.LogInfof("begin check inode, cluster:%s, vol:%s, inode: %d, extent count: %d", checkEngine.cluster, checkEngine.currentVol, inode, len(extentsResp.Extents))
	checkEKs := make([]proto.ExtentKey, 0)
	for _, ek := range extentsResp.Extents {
		if len(checkEngine.config.Filter.DpFilter) > 0 && !proto.IncludeUint64(ek.PartitionId, checkEngine.config.Filter.DpFilter) {
			continue
		}
		if (checkEngine.config.CheckTiny && proto.IsTinyExtent(ek.ExtentId)) || !checkEngine.config.CheckTiny {
			checkEKs = append(checkEKs, ek)
		}
	}
	ekCh := make(chan proto.ExtentKey)
	wg.Add(len(checkEKs))
	go func() {
		for _, ek := range checkEKs {
			ekCh <- ek
		}
		close(ekCh)
	}()
	//extent may be duplicated in extentsResp.Extents
	for i := 0; i < int(checkEngine.config.Concurrency); i++ {
		go func(ino uint64) {
			for ek := range ekCh {
				checkEngine.checkInodeEk(ek, ino)
				wg.Done()
			}
		}(inode)
	}
	wg.Wait()
	log.LogInfof("finish check, cluster:%s, vol:%s, inode: %d, extents len:%v\n", checkEngine.cluster, checkEngine.currentVol, inode, len(checkEKs))
}

func (checkEngine *CheckEngine) checkInodeEk(ek proto.ExtentKey, ino uint64) {
	var err error
	var ekStr string
	defer func() {
		if err != nil {
			checkEngine.onCheckFail(CheckFailExtent, fmt.Sprintf("%v %v %v", checkEngine.currentVol, ek.PartitionId, ek.ExtentId))
			log.LogErrorf("CheckFail-Extent, cluster:%s, dp:%v, extent:%v, err:%v", checkEngine.cluster, ek.PartitionId, ek.ExtentId, err)
		}
	}()
	ekStr = fmt.Sprintf("%d-%d-%d-%d", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size)
	if _, ok := checkEngine.checkedExtentsMap.LoadOrStore(ekStr, true); ok {
		return
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
	log.LogDebugf("action[checkInodeEk] cluster:%v, volume:%v, ino:%v, begin check extent key:%v", checkEngine.cluster, checkEngine.currentVol, ino, ek)
	for _, h := range partition.Hosts {
		if len(checkEngine.config.Filter.NodeFilter) > 0 && !proto.IncludeString(h, checkEngine.config.Filter.NodeFilter) {
			log.LogDebugf("action[checkInodeEk] cluster:%v, volume:%v, ino:%v, host:%v, skip extent key:%v by node filter:%v", checkEngine.cluster, checkEngine.currentVol, ino, h, ek, len(checkEngine.config.Filter.NodeFilter))
			return
		}
		if len(checkEngine.config.Filter.NodeExcludeFilter) > 0 && proto.IncludeString(h, checkEngine.config.Filter.NodeExcludeFilter) {
			log.LogDebugf("action[checkInodeEk] cluster:%v, volume:%v, ino:%v, host:%v, skip extent key:%v by exclude node filter:%v", checkEngine.cluster, checkEngine.currentVol, ino, h, ek, len(checkEngine.config.Filter.NodeExcludeFilter))
			return
		}
	}
	var badExtentInfo BadExtentInfo
	var badExtent bool
	badExtent, badExtentInfo, err = checkInodeExtentKey(checkEngine.cluster, checkEngine.mc.DataNodeProfPort, partition.Replicas, &ek, ino, checkEngine.currentVol, checkEngine.config.QuickCheck)
	if badExtent {
		checkEngine.repairPersist.BadExtentCh <- badExtentInfo
	}
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
		extents, err := mtClient.GetExtentKeyByInodeId(mp.PartitionID, inode)
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
	for i := 0; i < int(checkEngine.config.Concurrency); i++ {
		go func() {
			for ino := range inoCh {
				if checkEngine.closed {
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
