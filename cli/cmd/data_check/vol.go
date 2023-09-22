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

func (checkEngine *CheckEngine) checkVols() (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("CheckVols error: %v", err)
		}
	}()
	//todo: deal with failed vols
	_, err = checkEngine.repairPersist.loadFailedVols()
	if err != nil {
		return
	}
	checkVols, err := getVolsByFilter(checkEngine.mc, checkEngine.config.Filter)
	if err != nil {
		return
	}

	for _, v := range checkVols {
		if checkEngine.closed {
			break
		}
		checkEngine.currentVol = v
		checkEngine.checkVol()
	}
	return
}

func (checkEngine *CheckEngine) CheckFailedVols() {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("CheckVols error: %v", err)
		}
	}()
	fmt.Printf("check failed vols\n")
	vols, err := checkEngine.repairPersist.loadFailedVols()
	if err != nil {
		return
	}
	checkEngine.repairPersist.refreshFailedFD()
	for _, v := range vols {
		if checkEngine.closed {
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
		wg     sync.WaitGroup
		inodes []uint64
		err    error
	)
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkVol, cluster:%s, vol:%s, path%s", checkEngine.cluster, checkEngine.currentVol, checkEngine.path)
			var stack string
			stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogCriticalf("%s%s\n", msg, stack)
		}
		log.LogInfof("finish check vol, cluster:%s, vol:%s", checkEngine.cluster, checkEngine.currentVol)
		if err != nil {
			checkEngine.onCheckFail(CheckFailVol, checkEngine.currentVol)
			log.LogErrorf("CheckFail-Volume: %s, err:%v", checkEngine.currentVol, err)
		}
	}()
	checkEngine.checkedExtentsMap = new(sync.Map)
	log.LogInfof("begin check, cluster:%s, vol:%s, path:%v\n", checkEngine.cluster, checkEngine.currentVol, checkEngine.path)
	mps, err := checkEngine.mc.ClientAPI().GetMetaPartitions(checkEngine.currentVol)
	if err != nil {
		return
	}
	if len(checkEngine.config.Filter.InodeFilter) == 0 && checkEngine.path != "" {
		inodes, err = util_sdk.GetAllInodesByPath(checkEngine.mc.Nodes(), checkEngine.currentVol, checkEngine.path)
		if err != nil {
			return
		}
		checkEngine.checkInodes(mps, inodes)
		return
	}
	if len(checkEngine.config.Filter.InodeFilter) > 0 {
		checkEngine.checkInodes(mps, checkEngine.config.Filter.InodeFilter)
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

	for i := 0; i < int(checkEngine.config.Concurrency); i++ {
		go func() {
			for mp := range mpCh {
				if checkEngine.closed {
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
		if err != nil {
			log.LogErrorf("finish check mp with error, cluster:%s, vol:%s, mpId: %d, err:%v", checkEngine.cluster, checkEngine.currentVol, mp, err)
		}
		log.LogInfof("finish check mp, cluster:%s, vol:%s, mpId: %d", checkEngine.cluster, checkEngine.currentVol, mp)
	}()
	if checkEngine.checkType == CheckTypeInodeNlink {
		checkEngine.checkVolMpNlink(mps, mp)
		return
	}
	inodeMin, err := parseTime(checkEngine.config.InodeModifyTimeMin)
	if err != nil {
		return
	}
	inodeMax, err := parseTime(checkEngine.config.InodeModifyTimeMax)
	if err != nil {
		return
	}
	mpInodes, err = util_sdk.GetFileInodesByMp(mps, mp, 1, inodeMin.Unix(), inodeMax.Unix(), checkEngine.mnProf, false)
	if err != nil {
		checkEngine.onCheckFail(CheckFailMp, fmt.Sprintf("%v %v", checkEngine.currentVol, mp))
		return
	}
	if len(mpInodes) > 0 {
		checkEngine.checkInodes(mps, mpInodes)
	}
}

func (checkEngine *CheckEngine) checkVolMpNlink(mps []*proto.MetaPartitionView, metaPartitionId uint64) {
	inodeMin, err := parseTime(checkEngine.config.InodeModifyTimeMin)
	if err != nil {
		return
	}
	inodeMax, err := parseTime(checkEngine.config.InodeModifyTimeMax)
	if err != nil {
		return
	}
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
				if inodeMin.Unix() > 0 && inode.ModifyTime < inodeMin.Unix() {
					continue
				}
				if inodeMax.Unix() > 0 && inode.ModifyTime > inodeMax.Unix() {
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
