package data_check

import (
	"fmt"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"runtime/debug"
	"strings"
	"sync"
)

var UmpWarnKey = "check_crc_server"

func (checkEngine *CheckEngine) CheckVols(vols []string) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("CheckVols error: %v", err)
		}
	}()
	lastFailedVols, err := checkEngine.repairPersist.loadFailedVols()
	if err != nil {
		return
	}
	if len(lastFailedVols) > 0 {

	}
	for _, v := range vols {
		if checkEngine.isStop() {
			break
		}
		checkEngine.currentVol = v
		checkEngine.checkVol()
	}
}

func (checkEngine *CheckEngine) CheckFailedVols() {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("CheckVols error: %v", err)
		}
	}()
	vols, err := checkEngine.repairPersist.loadFailedVols()
	if err != nil {
		return
	}
	checkEngine.repairPersist.refreshFailedFD()
	for _, v := range vols {
		if checkEngine.isStop() {
			break
		}
		checkEngine.currentVol = v
		checkEngine.checkVol()
	}
	vols, err = checkEngine.repairPersist.loadFailedVols()
	if err != nil {
		return
	}
	if len(vols) > 0 {
		exporter.WarningBySpecialUMPKey(UmpWarnKey, fmt.Sprintf("check failed after twice, failed vols:%v", vols))
	}
}

func (checkEngine *CheckEngine) checkVol() {
	var (
		wg  sync.WaitGroup
		err error
	)
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkVol, cluster:%s, vol:%s, path%s", checkEngine.cluster, checkEngine.currentVol, checkEngine.path)
			var stack string
			stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogCritical("%s%s\n", msg, stack)
		}
		log.LogInfof("finish check vol, cluster:%s, vol:%s, err:%v", checkEngine.cluster, checkEngine.currentVol, err)
	}()
	checkEngine.checkedExtentsMap = new(sync.Map)
	log.LogInfof("begin check, cluster:%s, vol:%s, path:%v\n", checkEngine.cluster, checkEngine.currentVol, checkEngine.path)
	mps, err := checkEngine.mc.ClientAPI().GetMetaPartitions(checkEngine.currentVol)
	if err != nil {
		checkEngine.onFail(CheckCrcFailVol, checkEngine.currentVol)
		log.LogErrorf("CheckFail-Volume: %s, err:%v", checkEngine.currentVol, err)
		return
	}
	if len(checkEngine.specifyInodes) == 0 && checkEngine.path != "" {
		var inodes []uint64
		inodes, err = util_sdk.GetAllInodesByPath(checkEngine.mc.Nodes(), checkEngine.currentVol, checkEngine.path)
		if err != nil {
			checkEngine.onFail(CheckCrcFailVol, checkEngine.currentVol)
			log.LogErrorf("CheckFail-Volume: %s, err:%v", checkEngine.currentVol, err)
			return
		}
		checkEngine.checkInodes(mps, inodes)
		return
	}
	if len(checkEngine.specifyInodes) > 0 {
		checkEngine.checkInodes(mps, checkEngine.specifyInodes)
		return
	}

	mpCh := make(chan uint64, 1000)
	wg.Add(len(mps))
	go func() {
		for _, mp := range mps {
			mpCh <- mp.PartitionID
		}
		close(mpCh)
	}()

	for i := 0; i < int(checkEngine.concurrency); i++ {
		go func() {
			for mp := range mpCh {
				if checkEngine.isStop() {
					wg.Done()
					continue
				}
				checkEngine.checkMp(mp, mps)
				wg.Done()
			}
		}()
	}
	wg.Wait()
}

func (checkEngine *CheckEngine) checkMp(mp uint64, mps []*proto.MetaPartitionView) {
	var mpInodes []uint64
	var err error
	log.LogInfof("begin check meta partition, cluster:%s, vol:%s, mpId: %d", checkEngine.cluster, checkEngine.currentVol, mp)
	defer func() {
		log.LogInfof("finish check, cluster:%s, vol:%s, mpId: %d, err:%v", checkEngine.cluster, checkEngine.currentVol, mp, err)
	}()
	if checkEngine.checkType == CheckTypeInodeNlink {
		checkEngine.checkVolMpNlink(mps, mp)
		return
	}
	mpInodes, err = util_sdk.GetFileInodesByMp(mps, mp, 1, checkEngine.modifyTimeMin.Unix(), checkEngine.modifyTimeMax.Unix(), checkEngine.mnProf, false)
	if err != nil {
		checkEngine.onFail(CheckCrcFailMp, fmt.Sprintf("%v %v", checkEngine.currentVol, mp))
		log.LogErrorf("CheckFail-VolumeMp, vol:%s, mp:%v", checkEngine.currentVol, mp)
		return
	}
	if len(mpInodes) > 0 {
		checkEngine.checkInodes(mps, mpInodes)
	}
}

func (checkEngine *CheckEngine) checkVolMpNlink(mps []*proto.MetaPartitionView, metaPartitionId uint64) {
	for _, mp := range mps {
		if metaPartitionId > 0 && mp.PartitionID != metaPartitionId {
			continue
		}
		var preMap map[uint64]uint32
		for i, host := range mp.Members {
			mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], checkEngine.mnProf), false)
			inodes, err := mtClient.GetAllInodes(mp.PartitionID)
			if err != nil {
				fmt.Printf(err.Error())
				return
			}
			nlinkMap := make(map[uint64]uint32)
			for _, inode := range inodes {
				if checkEngine.modifyTimeMin.Unix() > 0 && inode.ModifyTime < checkEngine.modifyTimeMin.Unix() {
					continue
				}
				if checkEngine.modifyTimeMax.Unix() > 0 && inode.ModifyTime > checkEngine.modifyTimeMax.Unix() {
					continue
				}
				nlinkMap[inode.Inode] = inode.NLink
			}
			if i == 0 {
				preMap = nlinkMap
				continue
			}

			for ino, nlink := range nlinkMap {
				preNlink, ok := preMap[ino]
				if !ok {
					fmt.Printf("checkVolMpNlink ERROR, mpId: %d, ino %d of %s not exist in %s\n", mp.PartitionID, ino, mp.Members[i], mp.Members[i-1])
				} else if nlink != preNlink {
					fmt.Printf("checkVolMpNlink ERROR, mpId: %d, ino: %d, nlink %d of %s not equals to nlink %d of %s\n", mp.PartitionID, ino, nlink, mp.Members[i], preNlink, mp.Members[i-1])
				}
			}
			for preIno := range preMap {
				_, ok := nlinkMap[preIno]
				if !ok {
					fmt.Printf("checkVolMpNlink ERROR, mpId: %d, ino %d of %s not exist in %s\n", mp.PartitionID, preIno, mp.Members[i-1], mp.Members[i])
				}
			}
			preMap = nlinkMap
		}
	}
}
