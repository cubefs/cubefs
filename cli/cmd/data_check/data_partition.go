package data_check

import (
	"fmt"
	"github.com/cubefs/cubefs/cli/cmd/util"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
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
	if failedExtents, err = CheckDataPartitionReplica(dpMasterInfo, checkEngine.mc, checkEngine.modifyTimeMin, checkEngine.repairPersist.RepairPersistCh, checkEngine.tinyOnly); err != nil {
		checkEngine.onFail(CheckCrcFailDp, fmt.Sprintf("%v %v", dpMasterInfo.VolName, dp))
		return
	}
	if len(failedExtents) > 0 {
		for _, e := range failedExtents {
			checkEngine.onFail(CheckCrcFailExtent, fmt.Sprintf("%v %v %v", dpMasterInfo.VolName, dp, e))
		}
		return
	}
}

func (checkEngine *CheckEngine) CheckDataNodeCrc(node string) (err error) {
	var dpCount int
	defer func() {
		if err != nil {
			fmt.Printf("CheckDataNodeCrc error:%v\n", err)
		}
	}()
	log.LogInfof("CheckDataNodeCrc begin, datanode:%v", node)
	if err != nil {
		return
	}
	var datanodeInfo *proto.DataNodeInfo
	if datanodeInfo, err = checkEngine.mc.NodeAPI().GetDataNode(node); err != nil {
		return
	}
	excludeDPs := util.LoadSpecifiedPartitions()
	if err != nil {
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

	for i := 0; i < int(checkEngine.concurrency); i++ {
		go func() {
			for dp := range dpCh {
				checkEngine.doRepairPartition(dp, node)
				wg.Done()
			}
		}()
	}
	wg.Wait()
	log.LogInfof("CheckDataNodeCrc end, datanode:%v", node)
	return
}

func CheckDataPartitionReplica(dpMasterInfo *proto.DataPartitionInfo, mc *master.MasterClient, modifyTimeMin time.Time, resultCh chan RepairExtentInfo, tinyOnly bool) (failedExtents []uint64, err error) {
	var (
		dpDNInfo      *proto.DNDataPartitionInfo
		checkedExtent *sync.Map
	)
	checkedExtent = new(sync.Map)
	failedExtents = make([]uint64, 0)
	defer func() {
		if err != nil {
			log.LogErrorf("CheckDataPartitionReplica failed, PartitionId(%v) err(%v)\n", dpMasterInfo.PartitionID, err)
		}
	}()

	dpDNInfo, err = util_sdk.GetDataPartitionInfo(dpMasterInfo.Hosts[0], mc.DataNodeProfPort, dpMasterInfo.PartitionID)
	if err != nil {
		return
	}

	for _, ekTmp := range dpDNInfo.Files {
		if int64(ekTmp[proto.ExtentInfoModifyTime]) < modifyTimeMin.Unix() || ekTmp[proto.ExtentInfoSize] == 0 {
			continue
		}
		ek := proto.ExtentKey{
			FileOffset:   0,
			PartitionId:  dpMasterInfo.PartitionID,
			ExtentId:     ekTmp[proto.ExtentInfoFileID],
			ExtentOffset: 0,
			Size:         uint32(ekTmp[proto.ExtentInfoSize]),
		}
		if _, ok := checkedExtent.LoadOrStore(fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId), true); ok {
			continue
		}
		err1 := CheckExtentReplicaInfo(mc.Nodes()[0], mc.DataNodeProfPort, dpMasterInfo.Replicas, &ek, 0, dpMasterInfo.VolName, resultCh, tinyOnly)
		if err1 != nil {
			failedExtents = append(failedExtents, ek.ExtentId)
		}
	}
	return
}
