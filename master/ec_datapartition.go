package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/json"
)

type EcDataPartition struct {
	*DataPartition
	DataUnitsNum   uint8
	ParityUnitsNum uint8
	LastParityTime string
	ecReplicas     []*EcReplica
}

type EcReplica struct {
	proto.EcReplica
	ecNode *ECNode
}

type ecDataPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Hosts       string
	Status      int8
	VolID       uint64
	VolName     string
	Replicas    []*replicaValue
}

func newEcDataPartitionValue(dp *EcDataPartition) (dpv *ecDataPartitionValue) {
	dpv = &ecDataPartitionValue{
		PartitionID: dp.PartitionID,
		ReplicaNum:  dp.ReplicaNum,
		Hosts:       dp.ecHostsToString(),
		Status:      dp.Status,
		VolID:       dp.VolID,
		VolName:     dp.VolName,
		Replicas:    make([]*replicaValue, 0),
	}
	for _, replica := range dp.ecReplicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		dpv.Replicas = append(dpv.Replicas, rv)
	}
	return
}

type EcDataPartitionCache struct {
	partitions             map[uint64]*EcDataPartition
	volName                string
	vol                    *Vol
	readableAndWritableCnt int    // number of readable and writable partitionMap
	sync.RWMutex
}

func newEcDataPartitionCache(vol *Vol) (ecdpCache *EcDataPartitionCache) {
	ecdpCache = new(EcDataPartitionCache)
	ecdpCache.partitions = make(map[uint64]*EcDataPartition)
	ecdpCache.volName = vol.Name
	ecdpCache.vol = vol
	return
}

func (ecdpCache *EcDataPartitionCache) put(ecdp *EcDataPartition) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.partitions[ecdp.PartitionID] = ecdp
}

func (ecdpCache *EcDataPartitionCache) get(partitionID uint64) (ecdp *EcDataPartition, err error) {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	ecdp, ok := ecdpCache.partitions[partitionID]
	if !ok {
		return nil, proto.ErrEcPartitionNotExists
	}
	return
}

func (ecdpCache *EcDataPartitionCache) getEcPartitionsView(minPartitionID uint64) (epResps []*proto.EcPartitionResponse) {
	epResps = make([]*proto.EcPartitionResponse, 0)
	log.LogDebugf("volName[%v] EcPartitionMapLen[%v], minPartitionID[%v]",
		ecdpCache.volName, len(ecdpCache.partitions), minPartitionID)
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	for _, ep := range ecdpCache.partitions {
		if ep.PartitionID <= minPartitionID {
			continue
		}
		epResp := ep.convertToEcPartitionResponse()
		epResps = append(epResps, epResp)
	}
	return
}

func (ecdpCache *EcDataPartitionCache) setReadWriteDataPartitions(readWrites int, clusterName string) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.readableAndWritableCnt = readWrites
}

func newEcReplica(ecNode *ECNode) (replica *EcReplica) {
	replica = new(EcReplica)
	replica.Addr = ecNode.Addr
	replica.ecNode = ecNode
	replica.ReportTime = time.Now().Unix()
	return
}
func (replica *EcReplica) setAlive() {
	replica.ReportTime = time.Now().Unix()
}

func (replica *EcReplica) isMissing(interval int64) (isMissing bool) {
	if time.Now().Unix()-replica.ReportTime > interval {
		isMissing = true
	}
	return
}

func (replica *EcReplica) isLive(timeOutSec int64) (isAvailable bool) {
	if replica.ecNode.isActive == true && replica.Status != proto.Unavailable &&
		replica.isActive(timeOutSec) == true {
		isAvailable = true
	}

	return
}

func (replica *EcReplica) isActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *EcReplica) getReplicaNode() (node Node) {
	return replica.ecNode
}

// check if the replica's location is available
func (replica *EcReplica) isLocationAvailable() (isAvailable bool) {
	ecNode := replica.getReplicaNode()
	if ecNode.IsOnline() == true && replica.isActive(defaultDataPartitionTimeOutSec) == true {
		isAvailable = true
	}
	return
}

func newEcDataPartition(ID, volID uint64, volName string, dataUnitsNum, parityUnitsNum uint8) (ecdp *EcDataPartition) {
	partition := newDataPartition(ID, dataUnitsNum+parityUnitsNum, volName, volID)
	ecdp = &EcDataPartition{DataPartition: partition}
	ecdp.ecReplicas = make([]*EcReplica, 0)
	ecdp.DataUnitsNum = dataUnitsNum
	ecdp.ParityUnitsNum = parityUnitsNum
	ecdp.ReplicaNum = dataUnitsNum + parityUnitsNum
	return
}

func (ecdp *EcDataPartition) getLeaderAddr() (leaderAddr string) {
	for _, erp := range ecdp.ecReplicas {
		if erp.IsLeader {
			return erp.Addr
		}
	}
	return
}

func (ecdp *EcDataPartition) ecHostsToString() string {
	return strings.Join(ecdp.Hosts, underlineSeparator)
}

func (ecdp *EcDataPartition) getReplica(addr string) (replica *EcReplica, err error) {
	for index := 0; index < len(ecdp.ecReplicas); index++ {
		replica = ecdp.ecReplicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogErrorf("action[getReplica],partitionID:%v,locations:%v,err:%v",
		ecdp.PartitionID, addr, dataReplicaNotFound(addr))
	return nil, errors.Trace(dataReplicaNotFound(addr), "%v not found", addr)
}

func (ecdp *EcDataPartition) addReplica(replica *EcReplica) {
	for _, r := range ecdp.ecReplicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	ecdp.ecReplicas = append(ecdp.ecReplicas, replica)
}

func (ecdp *EcDataPartition) updateMetric(vr *proto.PartitionReport, ecNode *ECNode, c *Cluster) {
	if !ecdp.hasHost(ecNode.Addr) {
		return
	}
	ecdp.Lock()
	defer ecdp.Unlock()
	replica, err := ecdp.getReplica(ecNode.Addr)
	if err != nil {
		replica = newEcReplica(ecNode)
		ecdp.addReplica(replica)
	}
	ecdp.total = vr.Total
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	ecdp.setMaxUsed()
	replica.FileCount = uint32(vr.ExtentCount)
	replica.ReportTime = time.Now().Unix()
	replica.IsLeader = vr.IsLeader
	replica.NeedsToCompare = vr.NeedCompare
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateEcDataPartition(ecdp)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}
	ecdp.checkAndRemoveMissReplica(ecNode.Addr)
}

func (ecdp *EcDataPartition) addEcReplica(replica *EcReplica) {
	for _, r := range ecdp.ecReplicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	ecdp.ecReplicas = append(ecdp.ecReplicas, replica)
}

func (ecdp *EcDataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	ecNode, err := c.ecNode(nodeAddr)
	if err != nil || ecNode == nil{
		return err
	}
	replica := newEcReplica(ecNode)
	replica.Status = proto.ReadWrite
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	ecdp.addEcReplica(replica)
	ecdp.checkAndRemoveMissReplica(replica.Addr)
	return
}

func (ecdp *EcDataPartition) createTaskToCreateEcDataPartition(addr string, dataPartitionSize uint64, hosts []string) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpCreateEcDataPartition, addr, newCreateEcDataPartitionRequest(
		ecdp.VolName, ecdp.PartitionID, int(dataPartitionSize), hosts))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) createTaskToParityEcDataPartition(addr string) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpParityEcDataPartition, addr, &proto.ParityEcDataPartitionRequest{
		PartitionId: ecdp.PartitionID,
		Hosts:       ecdp.Hosts,
	})
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) convertToEcPartitionResponse() (ecdpr *proto.EcPartitionResponse) {
	ecdpr = new(proto.EcPartitionResponse)
	ecdp.Lock()
	defer ecdp.Unlock()
	ecdpr.PartitionID = ecdp.PartitionID
	ecdpr.Status = ecdp.Status
	ecdpr.Hosts = make([]string, len(ecdp.Hosts))
	copy(ecdpr.Hosts, ecdp.Hosts)
	ecdpr.LeaderAddr = ecdp.getLeaderAddr()
	ecdpr.DataUnitsNum = ecdp.DataUnitsNum
	ecdpr.ParityUnitsNum = ecdp.ParityUnitsNum
	ecdpr.ReplicaNum = ecdp.ReplicaNum
	return
}

func (ecdp *EcDataPartition) ToProto(c *Cluster) *proto.EcPartitionInfo {
	ecdp.RLock()
	defer ecdp.RUnlock()
	var replicas = make([]*proto.EcReplica, len(ecdp.ecReplicas))
	for i, replica := range ecdp.ecReplicas {
		replicas[i] = &replica.EcReplica
	}
	zones := make([]string, len(ecdp.Hosts))
	for idx, host := range ecdp.Hosts {
		ecNode, err := c.ecNode(host)
		if err == nil {
			zones[idx] = ecNode.ZoneName
		}
	}
	partition := &proto.DataPartitionInfo{
		PartitionID:             ecdp.PartitionID,
		LastLoadedTime:          ecdp.LastLoadedTime,
		Status:                  ecdp.Status,
		Hosts:                   ecdp.Hosts,
		ReplicaNum:              ecdp.ParityUnitsNum + ecdp.DataUnitsNum,
		Peers:                   ecdp.Peers,
		Zones:                   zones,
		MissingNodes:            ecdp.MissingNodes,
		VolName:                 ecdp.VolName,
		VolID:                   ecdp.VolID,
	}
	return &proto.EcPartitionInfo{
		DataPartitionInfo: partition,
		EcReplicas:        replicas,
		ParityUnitsNum:    ecdp.ParityUnitsNum,
		DataUnitsNum:      ecdp.DataUnitsNum,
	}
}
func (ecdp *EcDataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	liveReplicas := ecdp.getLiveReplicasFromHosts(dpTimeOutSec)
	if len(ecdp.Replicas) > len(ecdp.Hosts) {
		ecdp.Status = proto.ReadOnly
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveReplicas), ecdp.Status, ecdp.Hosts)
		Warn(clusterName, msg)
		return
	}

	switch len(liveReplicas) {
	case (int)(ecdp.ReplicaNum):
		ecdp.Status = proto.ReadOnly
		if ecdp.checkReplicaStatusOnLiveNode(liveReplicas) == true && ecdp.canWrite() {
			ecdp.Status = proto.ReadWrite
		}
	default:
		ecdp.Status = proto.ReadOnly
	}
	if needLog == true && len(liveReplicas) != int(ecdp.ReplicaNum) {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveReplicas), ecdp.Status, ecdp.Hosts)
		log.LogInfo(msg)
		if time.Now().Unix()-ecdp.lastWarnTime > intervalToWarnDataPartition {
			Warn(clusterName, msg)
			ecdp.lastWarnTime = time.Now().Unix()
		}
	}
}

// Check if there is any missing replica for a ec partition.
func (ecdp *EcDataPartition) checkMissingReplicas(clusterID, leaderAddr string, ecPartitionMissSec, ecPartitionWarnInterval int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, replica := range ecdp.ecReplicas {
		if ecdp.hasHost(replica.Addr) && replica.isMissing(ecPartitionMissSec) == true && ecdp.needToAlarmMissingDataPartition(replica.Addr, ecPartitionWarnInterval) {
			ecNode := replica.getReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if ecNode != nil {
				lastReportTime = ecNode.GetReportTime()
				isActive = ecNode.IsOnline()
			}
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] paritionID:%v  on Node:%v  "+
				"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
				clusterID, ecdp.PartitionID, replica.Addr, ecPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, replica.Addr)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range ecdp.Hosts {
		if ecdp.hasMissingEcPartition(addr) == true && ecdp.needToAlarmMissingDataPartition(addr, ecPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, ecdp.PartitionID, addr, ecPartitionMissSec)
			msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/ecPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

func (ecdp *EcDataPartition)  hasMissingEcPartition(addr string) (isMissing bool) {
	_, ok := ecdp.hasReplica(addr)
	if ok == false {
		isMissing = true
	}
	return
}

func (ecdp *EcDataPartition) hasReplica(host string) (replica *EcReplica, ok bool) {
	// using loop instead of map to save the memory
	for _, replica = range ecdp.ecReplicas {
		if replica.Addr == host {
			ok = true
			break
		}
	}
	return
}

// get all the live replicas from the persistent hosts
func (ecdp *EcDataPartition) getLiveReplicasFromHosts(timeOutSec int64) (replicas []*EcReplica) {
	replicas = make([]*EcReplica, 0)
	for _, host := range ecdp.Hosts {
		replica, ok := ecdp.hasReplica(host)
		if !ok {
			continue
		}
		if replica.isLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}
	return
}

func (ecdp *EcDataPartition) canWrite() bool {
	avail := ecdp.total - ecdp.used
	if int64(avail) > 10*util.GB {
		return true
	}
	return false
}

func (c *Cluster) batchCreateEcDataPartition(vol *Vol, reqCount int) (err error) {
	for i := 0; i < reqCount; i++ {
		if c.DisableAutoAllocate {
			return
		}
		if _, err = c.createEcDataPartition(vol); err != nil {
			log.LogErrorf("action[batchCreateEcDataPartition] after create [%v] data partition,occurred error,err[%v]", i, err)
			break
		}
	}
	return
}
func (c *Cluster) createEcDataPartition(vol *Vol) (ecdp *EcDataPartition, err error) {
	var (
		targetHosts []string
		wg          sync.WaitGroup
		partitionID uint64
	)
	replicaNum := vol.EcDataBlockNum + vol.EcParityBlockNum
	// ec partition and data partition using the same id allocator
	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()
	errChannel := make(chan error, replicaNum)
	if targetHosts, err = c.chooseTargetEcNodes("", nil, int(replicaNum)); err != nil {
		goto errHandler
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errHandler
	}
	ecdp = newEcDataPartition(partitionID, vol.ID, vol.Name, vol.EcDataBlockNum, vol.EcParityBlockNum)
	ecdp.Hosts = targetHosts
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateEcDataPartitionToEcNode(host, vol.dataPartitionSize, ecdp, ecdp.Hosts); err != nil {
				errChannel <- err
				return
			}
			ecdp.Lock()
			defer ecdp.Unlock()
			if err = ecdp.afterCreation(host, diskPath, c); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		for _, host := range targetHosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				_, err := ecdp.getReplica(host)
				if err != nil {
					return
				}
				task := ecdp.createTaskToDeleteDataPartition(host)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addEcNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		goto errHandler
	default:
		ecdp.total = util.DefaultDataPartitionSize
		ecdp.Status = proto.ReadWrite
	}
	if err = c.syncAddEcDataPartition(ecdp); err != nil {
		goto errHandler
	}
	vol.ecDataPartitions.put(ecdp)
	log.LogInfof("action[createEcDataPartition] success,volName[%v],partitionId[%v],Hosts[%v]", vol.Name, partitionID, ecdp.Hosts)
	return
errHandler:
	err = fmt.Errorf("action[createEcDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, vol.Name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) chooseTargetEcNodes(excludeZone string, excludeHosts []string, replicaNum int) (hosts []string, err error) {
	excludeZones := make([]string, 0)
	if excludeZone != "" {
		excludeZones = append(excludeZones, excludeZone)
	}
	zones, err := c.t.allocZonesForEcNode(defaultEcPartitionAcrossZoneNun, replicaNum, excludeZones)
	if err != nil {
		return
	}

	for _, zone := range zones {
		selectedHosts, _, e := zone.getAvailEcNodeHosts(excludeHosts, replicaNum)
		if e != nil {
			return nil, errors.NewError(e)
		}
		hosts = append(hosts, selectedHosts...)
	}
	log.LogInfof("action[chooseTargetEcNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, defaultEcPartitionAcrossZoneNun, len(zones), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[chooseTargetDataNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, defaultEcPartitionAcrossZoneNun, len(zones), hosts)
		return nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneNum[%v],selectedZones[%v]",
			len(hosts), replicaNum, defaultEcPartitionAcrossZoneNun, len(zones))
	}
	return
}

func (c *Cluster) syncCreateEcDataPartitionToEcNode(host string, size uint64, ecdp *EcDataPartition, hosts []string) (diskPath string, err error) {
	task := ecdp.createTaskToCreateEcDataPartition(host, size, hosts)
	ecNode, err := c.ecNode(host)
	if err != nil {
		return
	}
	var resp *proto.Packet
	if resp, err = ecNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return string(resp.Data), nil
}

// key=#dp#volID#partitionID,value=json.Marshal(dataPartitionValue)
func (c *Cluster) syncAddEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncAddDataPartition, partition)
}

func (c *Cluster) syncUpdateEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncUpdateDataPartition, partition)
}

func (c *Cluster) syncDeleteEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncDeleteDataPartition, partition)
}

func (c *Cluster) putEcDataPartitionInfo(opType uint32, partition *EcDataPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = dataPartitionPrefix + strconv.FormatUint(partition.VolID, 10) + keySeparator + strconv.FormatUint(partition.PartitionID, 10)
	ecdpv := newEcDataPartitionValue(partition)
	metadata.V, err = json.Marshal(ecdpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) getEcPartitionByID(partitionID uint64) (ep *EcDataPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if ep, err = vol.getEcPartitionByID(partitionID); err == nil {
			return
		}
	}
	err = ecPartitionNotFound(partitionID)
	return
}
