package master

import (
	"time"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/proto"
	"math/rand"
)

var (
	dateLayout = "2006-01-02"
	randNodes  = rand.New(rand.NewSource(time.Now().Unix()))
)

func (c *Cluster) scheduleToCheckEcDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkEcDataPartitions()
				c.parityEcDataPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
		}
	}()
}

// Check the replica status of each ec partition.
func (c *Cluster) checkEcDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkEcDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkEcDataPartitions occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		readWriteEcPartitions := vol.checkEcDataPartitions(c)
		vol.ecDataPartitions.setReadWriteDataPartitions(readWriteEcPartitions, c.Name)
		msg := fmt.Sprintf("action[checkEcDataPartitions],vol[%v] can readWrite ec partitions:%v  ", vol.Name, vol.ecDataPartitions.readableAndWritableCnt)
		log.LogInfo(msg)
	}
}

func (vol *Vol) checkEcDataPartitions(c *Cluster) (cnt int){
	if vol.Status == markDelete {
		return
	}
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()
	for _, ecdp := range vol.ecDataPartitions.partitions {
		ecdp.checkMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		ecdp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec)
		ecdp.checkDiskError(c.Name, c.leaderInfo.addr)
		if ecdp.Status == proto.ReadWrite {
			cnt++
		}
	}
	return
}

func (ecdp *EcDataPartition) checkReplicaStatusOnLiveNode(liveReplicas []*EcReplica) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status != proto.ReadWrite {
			return
		}
	}

	return true
}

func (ecdp *EcDataPartition) checkDiskError(clusterID, leaderAddr string) (diskErrorAddrs []string) {
	diskErrorAddrs = make([]string, 0)
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, addr := range ecdp.Hosts {
		replica, err := ecdp.getReplica(addr)
		if err != nil {
			continue
		}
		if replica.Status == proto.Unavailable {
			diskErrorAddrs = append(diskErrorAddrs, addr)
		}
	}

	if len(diskErrorAddrs) > 0 {
		ecdp.Status = proto.ReadOnly
	}

	for _, diskAddr := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],clusterID[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost",
			checkDataPartitionDiskErr, clusterID, ecdp.PartitionID, diskAddr)
		msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, diskAddr)
		Warn(clusterID, msg)
	}

	return
}

func (c *Cluster) parityEcDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("parityEcDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"parityEcDataPartitions occurred panic")
		}
	}()

	partitions := c.getToBeParityEcDataPartitions()
	if len(partitions) == 0 {
		return
	}
	c.createTaskToParityEcDataPartition(partitions)
}

func (c *Cluster) createTaskToParityEcDataPartition(partitions []*EcDataPartition) {
	codecNodes := c.allCodecNodes()

	for _, partition := range partitions {
		randNodes.Seed(time.Now().Unix())
		index := randNodes.Int() / len(codecNodes)
		partition.createTaskToParityEcDataPartition(codecNodes[index].Addr)
	}
}

func (c *Cluster) getToBeParityEcDataPartitions() (partitions []*EcDataPartition) {
	vols := c.allVols()
	maxCount := c.cfg.numberOfDataPartitionsToLoad
	for _, vol := range vols {
		partitions = vol.getToBeParityEcDataPartitions(maxCount)
		if len(partitions) >= maxCount {
			return
		}
	}
	return
}

func (vol *Vol) getToBeParityEcDataPartitions(maxCount int) (partitions []*EcDataPartition) {
	if vol.Status == markDelete {
		return
	}
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()
	currentDay := time.Now().Format(dateLayout)
	for _, ecdp := range vol.ecDataPartitions.partitions {
		if ecdp.LastParityTime < currentDay {
			ecdp.LastParityTime = currentDay
			partitions = append(partitions, ecdp)
		}
		if len(partitions) >= maxCount {
			return
		}
	}
	return
}
