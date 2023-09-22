package data_check

import (
	"fmt"
	"github.com/cubefs/cubefs/cli/cmd/util"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"sync"
	"time"
)

func (checkEngine *CheckEngine) doRepairPartition(dp uint64, node string) {
	var (
		err           error
		failedExtents []uint64
	)
	defer func() {
		log.LogInfof(" check datanode:%v dp:%v end", node, dp)
		if err != nil {
			log.LogErrorf(" check datanode:%v dp:%v end, err:%v", node, dp, err)
		}
	}()
	dpMasterInfo, err := checkEngine.mc.AdminAPI().GetDataPartition("", dp)
	if err != nil {
		return
	}
	extentMin, err := parseTime(checkEngine.config.ExtentModifyTimeMin)
	if err != nil {
		return
	}
	if failedExtents, err = CheckDataPartitionReplica(dpMasterInfo, checkEngine.mc, extentMin, checkEngine.repairPersist.BadExtentCh, checkEngine.config.QuickCheck); err != nil {
		checkEngine.onCheckFail(CheckFailDp, fmt.Sprintf("%v %v", dpMasterInfo.VolName, dp))
		return
	}
	if len(failedExtents) > 0 {
		for _, e := range failedExtents {
			checkEngine.onCheckFail(CheckFailExtent, fmt.Sprintf("%v %v %v", dpMasterInfo.VolName, dp, e))
		}
		return
	}
}

func (checkEngine *CheckEngine) checkNode() (err error) {
	var dpCount int
	excludeDPs := util.LoadSpecifiedPartitions()
	if err != nil {
		return
	}
	var doCheck = func(node string) {
		defer func() {
			if err != nil {
				log.LogErrorf("checkNode error:%v\n", err)
			}
		}()
		log.LogInfof("checkNode begin, datanode:%v", node)
		var datanodeInfo *proto.DataNodeInfo
		if datanodeInfo, err = checkEngine.mc.NodeAPI().GetDataNode(node); err != nil {
			return
		}
		wg := sync.WaitGroup{}
		dpCh := make(chan uint64, 1000)
		for _, dp := range datanodeInfo.PersistenceDataPartitions {
			if idExist(dp, excludeDPs) {
				continue
			}
			dpCount++
		}
		wg.Add(dpCount)
		go func() {
			for _, dp := range datanodeInfo.PersistenceDataPartitions {
				if idExist(dp, excludeDPs) {
					continue
				}
				dpCh <- dp
			}
			close(dpCh)
		}()

		for i := 0; i < int(checkEngine.config.Concurrency); i++ {
			go func() {
				for dp := range dpCh {
					checkEngine.doRepairPartition(dp, node)
					wg.Done()
				}
			}()
		}
		wg.Wait()
		log.LogInfof("checkNode end, datanode:%v", node)
	}

	for _, node := range checkEngine.config.Filter.NodeFilter {
		doCheck(node)
	}
	return
}

func CheckDataPartitionReplica(dpMasterInfo *proto.DataPartitionInfo, mc *master.MasterClient, extentModifyTimeMin time.Time, resultCh chan BadExtentInfo, quickCheck bool) (failedExtents []uint64, err error) {
	var (
		dpDNInfo      *proto.DNDataPartitionInfo
		checkedExtent *sync.Map
	)
	var files []proto.ExtentInfoBlock
	checkedExtent = new(sync.Map)
	failedExtents = make([]uint64, 0)
	defer func() {
		if err != nil {
			log.LogErrorf("CheckDataPartitionReplica failed, PartitionId(%v) err(%v)\n", dpMasterInfo.PartitionID, err)
		}
	}()
	for _, h := range dpMasterInfo.Hosts {
		dpDNInfo, err = util_sdk.GetDataPartitionInfo(h, mc.DataNodeProfPort, dpMasterInfo.PartitionID)
		if err != nil {
			return
		}
		files = append(files, dpDNInfo.Files...)
	}

	for _, ekTmp := range files {
		if int64(ekTmp[proto.ExtentInfoModifyTime]) < extentModifyTimeMin.Unix() || ekTmp[proto.ExtentInfoSize] == 0 {
			continue
		}
		if _, ok := checkedExtent.LoadOrStore(ekTmp[proto.ExtentInfoFileID], true); ok {
			continue
		}
		ek := proto.ExtentKey{
			FileOffset:   0,
			PartitionId:  dpMasterInfo.PartitionID,
			ExtentId:     ekTmp[proto.ExtentInfoFileID],
			ExtentOffset: 0,
			Size:         uint32(ekTmp[proto.ExtentInfoSize]),
		}
		if proto.IsTinyExtent(ek.ExtentId) {
			ek.Size = ek.Size - 4*unit.KB
		}
		badExtent, badExtentInfo, err1 := CheckExtentKey(mc.Nodes()[0], mc.DataNodeProfPort, dpMasterInfo.Replicas, &ek, 0, dpMasterInfo.VolName, quickCheck)
		if err1 != nil {
			failedExtents = append(failedExtents, ek.ExtentId)
		}
		if badExtent {
			resultCh <- badExtentInfo
		}
	}
	return
}
