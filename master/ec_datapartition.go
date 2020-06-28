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
	ecReplicas     []*EcDataReplica
}

type EcDataReplica struct {
	proto.DataReplica
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
	for _, replica := range dp.Replicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		dpv.Replicas = append(dpv.Replicas, rv)
	}
	return
}

type EcDataPartitionCache struct {
	partitions map[uint64]*EcDataPartition
	sync.RWMutex
}

func newEcDataPartitionCache() *EcDataPartitionCache {
	return &EcDataPartitionCache{partitions: make(map[uint64]*EcDataPartition)}
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
		return nil, proto.ErrDataPartitionNotExists
	}
	return
}

func newEcDataReplica(ecNode *ECNode) (replica *EcDataReplica) {
	replica = new(EcDataReplica)
	replica.ecNode = ecNode
	replica.Addr = ecNode.Addr
	replica.ReportTime = time.Now().Unix()
	return
}

func newEcDataPartition(ID, volID uint64, volName string, dataUnitsNum, parityUnitsNum uint8) (ecdp *EcDataPartition) {
	partition := newDataPartition(ID, dataUnitsNum+parityUnitsNum, volName, volID)
	ecdp = &EcDataPartition{DataPartition: partition}
	ecdp.DataUnitsNum = dataUnitsNum
	ecdp.ParityUnitsNum = parityUnitsNum
	return
}

func (ecdp *EcDataPartition) getLeaderAddr() (leaderAddr string) {
	return
}

func (ecdp *EcDataPartition) ecHostsToString() string {
	return strings.Join(ecdp.EcHosts, underlineSeparator)
}

func (ecdp *EcDataPartition) getReplica(addr string) (replica *EcDataReplica, err error) {
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

func (ecdp *EcDataPartition) addReplica(replica *EcDataReplica) {
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
		replica = newEcDataReplica(ecNode)
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

func (ecdp *EcDataPartition) addEcReplica(replica *EcDataReplica) {
	for _, r := range ecdp.ecReplicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	ecdp.ecReplicas = append(ecdp.ecReplicas, replica)
}

func (ecdp *EcDataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	ecNode, err := c.ecNode(nodeAddr)
	if err != nil {
		return err
	}
	replica := newEcDataReplica(ecNode)
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
		Hosts:       ecdp.EcHosts,
	})
	ecdp.resetTaskID(task)
	return
}

func (c *Cluster) createEcDataPartition(partitionID uint64, vol *Vol) (ecdp *EcDataPartition, err error) {
	var (
		targetHosts []string
		wg          sync.WaitGroup
	)
	replicaNum := vol.EcDataBlockNum + vol.EcParityBlockNum
	vol.createEcDpMutex.Lock()
	defer vol.createEcDpMutex.Unlock()
	errChannel := make(chan error, replicaNum)
	if targetHosts, err = c.chooseTargetEcNodes("", nil, int(replicaNum)); err != nil {
		goto errHandler
	}
	ecdp = newEcDataPartition(partitionID, vol.ID, vol.Name, vol.EcDataBlockNum, vol.EcParityBlockNum)
	ecdp.EcHosts = targetHosts
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateEcDataPartitionToEcNode(host, vol.dataPartitionSize, ecdp, ecdp.EcHosts); err != nil {
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
	log.LogInfof("action[createEcDataPartition] success,volName[%v],partitionId[%v],ecHosts[%v]", vol.Name, partitionID, ecdp.Hosts)
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
