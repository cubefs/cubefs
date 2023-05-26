package master

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
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
		canReadWriteEpCnt := vol.checkEcDataPartitions(c)
		vol.ecDataPartitions.updateResponseCache(true, 0)
		msg := fmt.Sprintf("action[checkEcDataPartitions],vol[%v] can readWrite ec partitions:%v  ", vol.Name, canReadWriteEpCnt)
		log.LogDebugf(msg)
	}
}

func (ecdp *EcDataPartition) hasEcHost(addr string) (ok bool) {
	for _, host := range ecdp.Hosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func (replica *EcReplica) isMissing(interval int64) (isMissing bool) {
	if time.Now().Unix()-replica.ReportTime > interval {
		isMissing = true
	}
	return
}

func (ecdp *EcDataPartition) needToAlarmMissingEcPartition(addr string, interval int64) (shouldAlarm bool) {
	t, ok := ecdp.MissingNodes[addr]
	if !ok {
		ecdp.MissingNodes[addr] = time.Now().Unix()
		shouldAlarm = true
	} else {
		if time.Now().Unix()-t > interval {
			shouldAlarm = true
			ecdp.MissingNodes[addr] = time.Now().Unix()
		}
	}
	return
}

func (replica *EcReplica) getEcReplicaNode() (node *ECNode) {
	return replica.ecNode
}

func (ecdp *EcDataPartition) hasEcReplica(host string) (replica *EcReplica, ok bool) {
	// using loop instead of map to save the memory
	for _, replica = range ecdp.ecReplicas {
		if replica.Addr == host {
			ok = true
			break
		}
	}
	return
}

func (ecdp *EcDataPartition) hasMissingEcPartition(addr string) (isMissing bool) {
	_, ok := ecdp.hasEcReplica(addr)

	if ok == false {
		isMissing = true
	}

	return
}

// Check if there is any missing replica for a data partition.
func (ecdp *EcDataPartition) checkEcMissingReplicas(clusterID, leaderAddr string, ecPartitionMissSec, ecPartitionWarnInterval int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, replica := range ecdp.ecReplicas {
		if ecdp.hasEcHost(replica.Addr) && replica.isMissing(ecPartitionMissSec) == true && ecdp.needToAlarmMissingEcPartition(replica.Addr, ecPartitionWarnInterval) {
			ecNode := replica.getEcReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if ecNode != nil {
				lastReportTime = ecNode.ReportTime
				isActive = ecNode.isActive
			}
			msg := fmt.Sprintf("action[checkEcMissErr],clusterID[%v] paritionID:%v  on Node:%v  "+
				"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
				clusterID, ecdp.PartitionID, replica.Addr, ecPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			msg = msg + fmt.Sprintf(" decommissionEcPartitionURL is http://%v/ecPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, replica.Addr)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range ecdp.Hosts {
		if ecdp.hasMissingEcPartition(addr) == true && ecdp.needToAlarmMissingEcPartition(addr, ecPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkEcMissErr],clusterID[%v] partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, ecdp.PartitionID, addr, ecPartitionMissSec)
			msg = msg + fmt.Sprintf(" decommissionEcPartitionURL is http://%v/ecPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

// get all the live replicas from the persistent hosts
func (ecdp *EcDataPartition) getLiveEcReplicasFromHosts(timeOutSec int64) (replicas []*EcReplica) {
	replicas = make([]*EcReplica, 0)
	for _, host := range ecdp.Hosts {
		replica, ok := ecdp.hasEcReplica(host)
		if !ok {
			continue
		}
		if replica.isLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (ecdp *EcDataPartition) checkEcReplicaStatusOnLiveNode(liveReplicas []*EcReplica) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status == proto.ReadWrite {
			return true
		}
	}

	return false
}

func (ecdp *EcDataPartition) canWrite() bool {
	if ecdp.Status == proto.Unavailable {
		return false
	}
	if ecdp.EcMigrateStatus == proto.FinishEC || ecdp.EcMigrateStatus == proto.OnlyEcExist {
		return false
	}
	for _, ecReplica := range ecdp.ecReplicas {
		avail := ecReplica.Total - ecReplica.Used
		if avail > 0 {
			return true
		}
	}
	return false
}

func (ecdp *EcDataPartition) checkEcStatus(clusterName string, needLog bool, dpTimeOutSec int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	if ecdp.isRecover {
		ecdp.Status = proto.ReadOnly
		return
	}
	liveEcReplicas := ecdp.getLiveEcReplicasFromHosts(dpTimeOutSec)
	if len(ecdp.ecReplicas) > len(ecdp.Hosts) {
		ecdp.Status = proto.ReadOnly
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveEcReplicas), ecdp.Status, ecdp.Hosts)
		Warn(clusterName, msg)
		return
	}

	switch len(liveEcReplicas) {
	case (int)(ecdp.ReplicaNum):
		ecdp.Status = proto.ReadOnly
		if ecdp.checkEcReplicaStatusOnLiveNode(liveEcReplicas) == true && ecdp.canWrite() {
			ecdp.Status = proto.ReadWrite
		}
	default:
		ecdp.Status = proto.ReadOnly
	}
	if needLog == true && len(liveEcReplicas) != int(ecdp.ReplicaNum) {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveEcReplicas), ecdp.Status, ecdp.Hosts)
		log.LogInfo(msg)
		if time.Now().Unix()-ecdp.lastWarnTime > intervalToWarnDataPartition {
			Warn(clusterName, msg)
			ecdp.lastWarnTime = time.Now().Unix()
		}
	}
}

func (ecdp *EcDataPartition) deleteIllegalEcReplica() (excessAddr string, err error) {
	ecdp.Lock()
	defer ecdp.Unlock()
	for i := 0; i < len(ecdp.ecReplicas); i++ {
		replica := ecdp.ecReplicas[i]
		if ok := ecdp.hasEcHost(replica.Addr); !ok {
			excessAddr = replica.Addr
			err = proto.ErrIllegalDataReplica
			break
		}
	}
	return
}

func (ecdp *EcDataPartition) missingEcReplicaAddress() (addr string, err error) {
	ecdp.Lock()
	defer ecdp.Unlock()

	if time.Now().Unix()-ecdp.createTime < 120 {
		return
	}

	// go through all the hosts to find the missing replica
	for _, host := range ecdp.Hosts {
		if _, ok := ecdp.hasEcReplica(host); !ok {
			log.LogError(fmt.Sprintf("action[missingEcReplicaAddress],partitionID:%v lack replication:%v",
				ecdp.PartitionID, host))
			err = proto.ErrMissingReplica
			addr = host
			break
		}
	}

	return
}

func (ecdp *EcDataPartition) checkEcReplicationTask(c *Cluster) {
	var msg string
	if excessAddr, excessErr := ecdp.deleteIllegalEcReplica(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v",
			deleteIllegalReplicaErr, ecdp.PartitionID, excessAddr, excessErr.Error(), ecdp.Hosts)
		Warn(c.Name, msg)
		ecNode, _ := c.ecNode(excessAddr)
		if ecNode != nil {
			c.deleteEcReplicaFromEcNodeOptimistic(ecdp, ecNode)
		}
	}

	if lackAddr, lackErr := ecdp.missingEcReplicaAddress(); lackErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication"+
			" On :%v  Err:%v  Hosts:%v  new task to create DataReplica",
			addMissingReplicaErr, ecdp.PartitionID, lackAddr, lackErr.Error(), ecdp.Hosts)
		Warn(c.Name, msg)
		return
	}

	return
}

func (ecdp *EcDataPartition) checkEcReplicaNum(c *Cluster, vol *Vol) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	if int(ecdp.ReplicaNum) != len(ecdp.Hosts) {
		msg := fmt.Sprintf("FIX EcDataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, vol.Name, ecdp.PartitionID, ecdp.ReplicaNum)
		Warn(c.Name, msg)
	}

	volReplicaNum := vol.EcDataNum + vol.EcParityNum
	if volReplicaNum != ecdp.ReplicaNum && !vol.NeedToLowerReplica {
		vol.NeedToLowerReplica = true
	}
}

func (ecdp *EcDataPartition) checkEcReplicaStatus(timeOutSec int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, replica := range ecdp.ecReplicas {
		if !replica.isLive(timeOutSec) {
			if replica.Status != proto.Unavailable {
				replica.Status = proto.ReadOnly
			}
		}
	}
}

func (vol *Vol) checkEcDataPartitions(c *Cluster) (cnt int) {
	if vol.Status == markDelete {
		return
	}
	eps := vol.cloneEcPartitionMap()
	for _, ecdp := range eps {
		ecdp.checkEcReplicaStatus(c.cfg.DataPartitionTimeOutSec)
		ecdp.checkEcStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec)
		ecdp.checkEcMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		ecdp.checkEcDiskError(c.Name, c.leaderInfo.addr)
		ecdp.checkEcReplicaNum(c, vol)
		ecdp.checkReplicaSaveTime(c, vol)
		if ecdp.Status == proto.ReadWrite {
			cnt++
		}
		ecdp.checkEcReplicationTask(c)
	}
	return
}

func (ecdp *EcDataPartition) checkEcDiskError(clusterID, leaderAddr string) (diskErrorAddrs []string) {
	diskErrorAddrs = make([]string, 0)
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, addr := range ecdp.Hosts {
		replica, err := ecdp.getEcReplica(addr)
		if err != nil {
			continue
		}
		if replica.Status == proto.Unavailable {
			diskErrorAddrs = append(diskErrorAddrs, addr)
		}
	}

	if len(diskErrorAddrs) > int(ecdp.ParityUnitsNum) {
		ecdp.Status = proto.Unavailable
	}

	for _, diskAddr := range diskErrorAddrs {
		msg := fmt.Sprintf("action[checkEcPartitionDiskErr],clusterID[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost",
			clusterID, ecdp.PartitionID, diskAddr)
		msg = msg + fmt.Sprintf(" decommissionEcPartitionURL is http://%v/ecPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, diskAddr)
		Warn(clusterID, msg)
	}

	return
}

func (ecdp *EcDataPartition) checkReplicaSaveTime(c *Cluster, vol *Vol) {
	dp, err := vol.getDataPartitionByID(ecdp.PartitionID)
	if err != nil {
		if ecdp.EcMigrateStatus != proto.OnlyEcExist {
			log.LogErrorf("CheckReplicaSaveTime getDataPartitionByID fail: %v", err)
		}
		return
	}
	if !ecdp.canDelDp(dp.doDelDpAlreadyEcTime, vol.EcMigrationSaveTime) {
		return
	}

	tmpDp := make([]*DataPartition, 0)
	vol.dataPartitions.Lock()
	for idx, dataPartition := range vol.dataPartitions.partitions {
		if dataPartition.PartitionID != dp.PartitionID {
			continue
		}
		tmpDp = append(tmpDp, vol.dataPartitions.partitions[:idx]...)
		tmpDp = append(tmpDp, vol.dataPartitions.partitions[idx+1:]...)
		break
	}
	vol.dataPartitions.partitions = tmpDp
	delete(vol.dataPartitions.partitionMap, dp.PartitionID)
	vol.dataPartitions.Unlock()

	tasks := make([]*proto.AdminTask, 0)
	for _, host := range dp.Hosts {
		tasks = append(tasks, dp.createTaskToDeleteDataPartition(host))
	}
	c.addDataNodeTasks(tasks)
	if err = c.syncDeleteDataPartition(dp); err != nil {
		log.LogErrorf("CheckReplicaSaveTime syncDeleteDataPartition fail: %v", err)
	}

	if ecdp.EcMigrateStatus != proto.OnlyEcExist {
		ecdp.Lock()
		ecdp.EcMigrateStatus = proto.OnlyEcExist
		if err = c.syncUpdateEcDataPartition(ecdp); err != nil {
			log.LogErrorf("CheckReplicaSaveTime syncUpdateEcDataPartition fail: %v", err)
		}
		ecdp.Unlock()
	}
	vol.dataPartitions.updateResponseJsonCache(vol.ecDataPartitions, true, 0)
	vol.dataPartitions.setDataPartitionResponseProtobufCache(nil)
	log.LogInfof("CheckReplicaSaveTime delete dataPartitionId(%v) success", ecdp.PartitionID)
}
