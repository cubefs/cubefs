package master

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	UpdateVolScrub = iota
	UpdateVolCheckSum
)

func newCreateEcPartitionRequest(volName string, ID uint64, partitionSize uint64, dataNodeNum, parityNodeNum, nodeIndex uint32, hosts []string, maxStipeUnit uint64) (req *proto.CreateEcPartitionRequest) {
	req = &proto.CreateEcPartitionRequest{
		PartitionID:   ID,
		PartitionSize: partitionSize,
		VolumeID:      volName,
		DataNodeNum:   dataNodeNum,
		ParityNodeNum: parityNodeNum,
		NodeIndex:     nodeIndex,
		Hosts:         hosts,
		EcMaxUnitSize: maxStipeUnit,
	}
	return
}

func newChangeEcPartitionMembersRequest(ID uint64, newHosts []string) (req *proto.ChangeEcPartitionMembersRequest) {
	req = &proto.ChangeEcPartitionMembersRequest{
		PartitionId: ID,
		Hosts:       newHosts,
	}
	return
}

func newDeleteEcPartitionRequest(ID uint64) (req *proto.DeleteEcPartitionRequest) {
	req = &proto.DeleteEcPartitionRequest{
		PartitionId: ID,
	}
	return
}

type EcDataPartition struct {
	*DataPartition
	DataUnitsNum     uint8
	ParityUnitsNum   uint8
	MaxSripeUintSize uint64
	NeededSpace      uint64
	LastParityTime   string
	FinishEcTime     int64
	LastUpdateTime   int64
	ecReplicas       []*EcReplica
	hostsIdx         []bool
	logicUsed        uint64
}

type EcDataPartitionCache struct {
	partitions             map[uint64]*EcDataPartition
	volName                string
	vol                    *Vol
	responseCache          []byte
	readableAndWritableCnt int // number of readable and writable partitionMap
	sync.RWMutex
}

type ecDataPartitionValue struct {
	DataUnitsNum     uint8
	ParityUnitsNum   uint8
	ReplicaNum       uint8
	EcMigrateStatus  uint8
	IsRecover        bool
	Status           int8
	PartitionID      uint64
	VolID            uint64
	MaxSripeUintSize uint64
	NeededSpace      uint64
	Hosts            string
	VolName          string
	Replicas         []*replicaValue
	HostsIdx         []bool
	FinishEcTime     int64
	LastUpdateTime   int64
}

type EcReplica struct {
	proto.EcReplica
	ecNode *ECNode
}

func extractPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractNeedDelEc(r *http.Request) (needDelEc bool, err error) {
	var value string
	if value = r.FormValue(needDelEcKey); value == "" {
		needDelEc = false
		return
	}
	if needDelEc, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func getIndex(addr string, hosts []string) (index int) {
	index = -1
	if len(hosts) == 0 {
		return
	}
	if "" == addr {
		return
	}
	for i, host := range hosts {
		if addr == host {
			index = i
			return
		}
	}
	return
}

func newEcDataPartition(ecDataBlockNum, ecParityBlockNum uint8, maxStripeUnitSize, ID, volID uint64, volName string) (ecdp *EcDataPartition) {
	partition := newDataPartition(ID, ecDataBlockNum+ecParityBlockNum, volName, volID)
	ecdp = &EcDataPartition{DataPartition: partition}
	ecdp.ecReplicas = make([]*EcReplica, 0)
	ecdp.DataUnitsNum = ecDataBlockNum
	ecdp.ParityUnitsNum = ecParityBlockNum
	ecdp.Status = proto.ReadOnly
	ecdp.MaxSripeUintSize = maxStripeUnitSize
	ecdp.hostsIdx = make([]bool, ecDataBlockNum+ecParityBlockNum)
	return
}

func newEcDataPartitionCache(vol *Vol) (ecdpCache *EcDataPartitionCache) {
	ecdpCache = new(EcDataPartitionCache)
	ecdpCache.partitions = make(map[uint64]*EcDataPartition)
	ecdpCache.volName = vol.Name
	ecdpCache.vol = vol
	return
}

func newEcDataPartitionValue(ep *EcDataPartition) (edpv *ecDataPartitionValue) {
	edpv = &ecDataPartitionValue{

		DataUnitsNum:     ep.DataUnitsNum,
		ParityUnitsNum:   ep.ParityUnitsNum,
		ReplicaNum:       ep.ReplicaNum,
		Status:           ep.Status,
		EcMigrateStatus:  ep.EcMigrateStatus,
		IsRecover:        ep.isRecover,
		PartitionID:      ep.PartitionID,
		VolID:            ep.VolID,
		MaxSripeUintSize: ep.MaxSripeUintSize,
		Hosts:            ep.ecHostsToString(),
		VolName:          ep.VolName,
		Replicas:         make([]*replicaValue, 0),
		HostsIdx:         ep.hostsIdx,
		NeededSpace:      ep.NeededSpace,
		FinishEcTime:     ep.FinishEcTime,
		LastUpdateTime:   ep.LastUpdateTime,
	}
	for _, replica := range ep.ecReplicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		edpv.Replicas = append(edpv.Replicas, rv)
	}
	return
}

func newEcReplica(ecNode *ECNode) (replica *EcReplica) {
	replica = new(EcReplica)
	replica.Addr = ecNode.Addr
	replica.ecNode = ecNode
	replica.HttpPort = ecNode.HttpPort
	replica.ReportTime = time.Now().Unix()
	return
}

func parseRequestToDecommissionEcPartition(r *http.Request) (partitionID uint64, nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractPartitionID(r); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func parseRequestToGetEcPartition(r *http.Request) (ID uint64, volName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractPartitionID(r); err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	return
}

func (m *Server) decommissionEcPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		partitionID uint64
		addr        string
		ep          *EcDataPartition
		err         error
	)

	if partitionID, addr, err = parseRequestToDecommissionEcPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if ep, err = m.cluster.getEcPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeEcPartitionNotExists, Msg: err.Error()})
		return
	}
	if err = m.cluster.decommissionEcDataPartition(addr, ep, ""); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommissionEcPartition succeeds.")
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) getEcPartition(w http.ResponseWriter, r *http.Request) {
	var (
		ep          *EcDataPartition
		partitionID uint64
		volName     string
		vol         *Vol
		err         error
	)
	if partitionID, volName, err = parseRequestToGetEcPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if volName != "" {
		if vol, err = m.cluster.getVol(volName); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrEcPartitionNotExists))
			return
		}
		if ep, err = vol.getEcPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrEcPartitionNotExists))
			return
		}
	} else {
		if ep, err = m.cluster.getEcPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrEcPartitionNotExists))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(ep.ToProto(m.cluster)))
}

func (m *Server) diagnoseEcPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err              error
		rstMsg           *proto.EcPartitionDiagnosis
		inactiveNodes    []string
		corruptEps       []*EcDataPartition
		lackReplicaEps   []*EcDataPartition
		corruptEpIDs     []uint64
		lackReplicaEpIDs []uint64
	)
	if inactiveNodes, corruptEps, err = m.cluster.checkCorruptEcPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}

	if lackReplicaEps, err = m.cluster.checkLackReplicaEcPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for _, ep := range corruptEps {
		corruptEpIDs = append(corruptEpIDs, ep.PartitionID)
	}
	for _, ep := range lackReplicaEps {
		lackReplicaEpIDs = append(lackReplicaEpIDs, ep.PartitionID)
	}
	rstMsg = &proto.EcPartitionDiagnosis{
		InactiveEcNodes:           inactiveNodes,
		CorruptEcPartitionIDs:     corruptEpIDs,
		LackReplicaEcPartitionIDs: lackReplicaEpIDs,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], lackReplicaDpIDs:[%v]",
		m.cluster.Name, inactiveNodes, corruptEpIDs, lackReplicaEpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (ecdp *EcDataPartition) getFileMinus() (minus float64) {
	if len(ecdp.ecReplicas) == 0 {
		return
	}
	fileCount := ecdp.ecReplicas[0].FileCount
	for _, replica := range ecdp.ecReplicas {
		if math.Abs(float64(replica.FileCount)-float64(fileCount)) > minus {
			minus = math.Abs(float64(replica.FileCount) - float64(fileCount))
		}
	}
	return minus
}

func (ecdp *EcDataPartition) isEcFilesCatchUp() (ok bool) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	fileMinus := ecdp.getFileMinus()
	return fileMinus < 2
}

// Obtain all the ec partitions in a volume.
func (m *Server) getEcPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		name string
		vol  *Vol
		err  error
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if body, err = vol.getEcPartitionsView(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	send(w, r, body)
}

func (c *Cluster) addEcReplica(ecdp *EcDataPartition, index int, hosts []string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addEcReplica],vol[%v], ec partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
		}
	}()
	addr := hosts[index]
	ecNode, err := c.ecNode(addr)
	if err != nil {
		return
	}

	if err = c.doCreateEcReplica(ecdp, ecNode, hosts); err != nil {
		return
	}
	ecdp.hostsIdx[index] =  true
	return
}

func (c *Cluster) getNeedReservedSize(dp *DataPartition, ecDataNum uint8) (size uint64) {
	maxUsed := uint64(0)
	maxFileCount := uint32(0)
	for _, host := range dp.Replicas {
		if host.Used > maxUsed {
			maxUsed = host.Used
		}

		if host.FileCount > maxFileCount {
			maxFileCount = host.FileCount
		}

	}
	if maxUsed == 0 {
		maxUsed = dp.total
	}
	unitSize := float64(maxUsed) / float64(ecDataNum)
	size = uint64(math.Ceil(unitSize))
	if size%proto.PageSize != 0 {
		size = (size/proto.PageSize + 1) * proto.PageSize
	}

	size += uint64(maxFileCount * proto.PageSize)
	log.LogDebugf("vol(%v) maxUsed(%v) maxFileCount(%v)\n", dp.VolID, maxUsed, maxFileCount)
	return
}

func (c *Cluster) createEcDataPartition(vol *Vol, dp *DataPartition) (ecdp *EcDataPartition, err error) {
	if ecdp, err = vol.getEcPartitionByID(dp.PartitionID); err == nil {
		return
	}

	var (
		targetHosts []string
		wg          sync.WaitGroup
		partitionID uint64
		neededSpace uint64
		ecNode      *ECNode
	)
	replicaNum := vol.EcDataNum + vol.EcParityNum
	neededSpace = c.getNeedReservedSize(dp, vol.EcDataNum)
	// ec partition and data partition using the same id allocator
	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()
	errChannel := make(chan error, replicaNum)
	zoneNum := int(math.Ceil(float64(replicaNum) / float64(vol.EcParityNum)))
	if targetHosts, err = c.chooseTargetEcNodes("", nil, int(replicaNum), zoneNum); err != nil {
		goto errHandler
	}

	for _, host := range targetHosts {
		if ecNode, err = c.ecNode(host); err == nil && (ecNode.RemainCapacity < neededSpace+util.MB) {
			err = errors.New(fmt.Sprintf("select ecnode:%v free space is less than needed:%v", host, neededSpace))
			return
		}
		ecNode.RemainCapacity -= neededSpace
	}

	ecdp = newEcDataPartition(vol.EcDataNum, vol.EcParityNum, vol.EcMaxUnitSize, dp.PartitionID, vol.ID, vol.Name)
	ecdp.Hosts = targetHosts
	for i, host := range targetHosts {
		ecdp.hostsIdx[i] = true
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateEcDataPartitionToEcNode(host, neededSpace, ecdp, ecdp.Hosts); err != nil {
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
		for i, host := range targetHosts {
			ecdp.hostsIdx[i] = false
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				_, err := ecdp.getEcReplica(host)
				if err != nil {
					return
				}
				task := ecdp.createTaskToDeleteEcPartition(host)
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
		ecdp.NeededSpace = neededSpace
		ecdp.LastUpdateTime = getDpLastUpdateTime(dp)
	}
	if err = c.syncAddEcDataPartition(ecdp); err != nil {
		goto errHandler
	}
	vol.ecDataPartitions.put(ecdp)
	log.LogInfof("action[createEcDataPartition] success,volName[%v],partitionId[%v],Hosts[%v] status[%v]", vol.Name, partitionID, ecdp.Hosts, ecdp.Status)
	return
errHandler:
	err = fmt.Errorf("action[createEcDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, vol.Name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) decommissionEcDataPartition(offlineAddr string, ecdp *EcDataPartition, errMsg string) (err error) {
	var (
		targetHosts []string
		newAddr     string
		msg         string
		ecNode      *ECNode
		zone        *Zone
		newHosts    []string
		ecReplica   *EcReplica
		zones       []string
		excludeZone string
		index       int
	)
	ecdp.RLock()
	if ok := ecdp.hasHost(offlineAddr); !ok {
		log.LogErrorf("decommissionEcDataPartition no this host")
		ecdp.RUnlock()
		return
	}
	ecdp.RUnlock()

	ecReplica, err = ecdp.getEcReplica(offlineAddr)
	if err != nil {
		return
	}
	if err = c.validateDecommissionEcDataPartition(ecdp, offlineAddr); err != nil {
		goto errHandler
	}

	if ecNode, err = c.ecNode(offlineAddr); err != nil {
		goto errHandler
	}

	if ecNode.ZoneName == "" {
		err = fmt.Errorf("ecNode[%v] zone is nil", ecNode.Addr)
		goto errHandler
	}
	if zone, err = c.t.getZone(ecNode.ZoneName); err != nil {
		goto errHandler
	}

	if targetHosts, _, err = zone.getAvailEcNodeHosts(ecdp.Hosts, 1); err != nil {
		// select ec nodes from the other zone
		zones = ecdp.getLiveZones(offlineAddr)
		if len(zones) == 0 {
			excludeZone = zone.name
		} else {
			excludeZone = zones[0]
		}
		if targetHosts, err = c.chooseTargetEcNodes(excludeZone, ecdp.Hosts, 1, 1); err != nil {
			goto errHandler
		}
	}

	newAddr = targetHosts[0]
	newHosts = ecdp.Hosts
	if err = c.removeEcDataReplica(ecdp, offlineAddr, false); err != nil {
		goto errHandler
	}

	for i, host := range newHosts {
		if host == offlineAddr {
			newHosts[i] = newAddr
			index = i
		}
	}
	if err = c.addEcReplica(ecdp, index, newHosts); err != nil {
		goto errHandler
	}

	log.LogDebugf("action[decommissionEcDataPartition] target Hosts:%v newHosts[%v]", targetHosts, ecdp.Hosts)
	if _, err = c.syncChangeEcPartitionMembers(ecdp, newHosts[0]); err != nil {
		goto errHandler
	}

	ecdp.Lock()
	ecdp.Status = proto.ReadOnly
	ecdp.isRecover = true
	c.syncUpdateEcDataPartition(ecdp)
	ecdp.Unlock()
	c.putBadEcPartitionIDs(ecReplica, offlineAddr, ecdp.PartitionID)
	log.LogWarnf("clusterID[%v] partitionID:%v  on Node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, ecdp.PartitionID, offlineAddr, newAddr, ecdp.Hosts)
	return
errHandler:
	log.LogErrorf("decommissionEcDataPartition [%v] err[%v]", ecdp.PartitionID, err)
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, ecdp.PartitionID, offlineAddr, newAddr, err, ecdp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
	}
	return
}

func (c *Cluster) deleteEcReplicaFromEcNodeOptimistic(ecdp *EcDataPartition, ecNode *ECNode) (err error) {
	ecdp.Lock()
	// in case ecNode is unreachable, remove replica first.
	ecdp.removeReplicaByAddr(ecNode.Addr)
	ecdp.checkAndRemoveMissReplica(ecNode.Addr)
	newPeers, newHosts := ecdp.removePeerAndHost(ecNode)
	if err = ecdp.update("deleteDataReplica", ecdp.VolName, newPeers, newHosts, c); err != nil {
		ecdp.Unlock()
		return
	}
	task := ecdp.createTaskToDeleteEcPartition(ecNode.Addr)
	ecdp.Unlock()
	_, err = ecNode.TaskManager.syncSendAdminTask(task)
	return nil
}

func (c *Cluster) doCreateEcReplica(ecdp *EcDataPartition, addNode *ECNode, hosts []string) (err error) {
	diskPath, err := c.syncCreateEcDataPartitionToEcNode(addNode.Addr, ecdp.NeededSpace, ecdp, hosts)
	if err != nil {
		return
	}

	ecdp.Lock()
	defer ecdp.Unlock()
	if err = ecdp.afterCreation(addNode.Addr, diskPath, c); err != nil {
		return
	}
	if err = ecdp.update("doCreateEcReplica", ecdp.VolName, ecdp.Peers, hosts, c); err != nil {
		return
	}
	return
}

func ecPartitionNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("ec partition[%v]", id))
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

func (c *Cluster) loadEcPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ecPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadEcPartitions],err:%v", err.Error())
		return err
	}
	for _, value := range result {

		edpv := &ecDataPartitionValue{}
		if err = json.Unmarshal(value, edpv); err != nil {
			err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(edpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadEcPartitions] err:%v", err1.Error())
			continue
		}
		if vol.ID != edpv.VolID {
			Warn(c.Name, fmt.Sprintf("action[loadEcPartitions] has duplicate vol[%v],vol.ID[%v],edpv.VolID[%v]", edpv.VolName, vol.ID, edpv.VolID))
			continue
		}
		ep := newEcDataPartition(edpv.DataUnitsNum, edpv.ParityUnitsNum, edpv.MaxSripeUintSize, edpv.PartitionID, edpv.VolID, edpv.VolName)
		ep.Hosts = strings.Split(edpv.Hosts, underlineSeparator)
		ep.hostsIdx = edpv.HostsIdx
		ep.EcMigrateStatus = edpv.EcMigrateStatus
		ep.isRecover = edpv.IsRecover
		ep.Status = edpv.Status
		ep.NeededSpace = edpv.NeededSpace
		ep.FinishEcTime = edpv.FinishEcTime
		ep.LastUpdateTime = edpv.LastUpdateTime
		for _, rv := range edpv.Replicas {
			ep.afterCreation(rv.Addr, rv.DiskPath, c)
		}
		vol.ecDataPartitions.put(ep)
		log.LogInfof("action[loadEcPartitions],vol[%v],ep[%v]", vol.Name, ep.PartitionID)
	}
	return
}

func (c *Cluster) putEcDataPartitionInfo(opType uint32, partition *EcDataPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ecPartitionPrefix + strconv.FormatUint(partition.VolID, 10) + keySeparator + strconv.FormatUint(partition.PartitionID, 10)
	ecdpv := newEcDataPartitionValue(partition)
	metadata.V, err = json.Marshal(ecdpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

// remove ec replica on ec node
func (c *Cluster) removeEcDataReplica(ecdp *EcDataPartition, addr string, validate bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[removeEcDataReplica],vol[%v],ec partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
		}
	}()
	if validate == true {
		if err = c.validateDecommissionEcDataPartition(ecdp, addr); err != nil {
			return
		}
	}
	if c.isEcRecovering(ecdp, addr) {
		err = fmt.Errorf("vol[%v],ec partition[%v] can't decommision until it has recovered", ecdp.VolName, ecdp.PartitionID)
		return
	}
	ecNode, err := c.ecNode(addr)
	if err != nil {
		return
	}
	//todo delete peer?
	if err = c.deleteEcReplicaFromEcNodeOptimistic(ecdp, ecNode); err != nil {
		return
	}
	return
}

func (ecdp *EcDataPartition) removeOneEcReplicaByHost(c *Cluster, host string) (err error) {
	ecdp.offlineMutex.Lock()
	defer ecdp.offlineMutex.Unlock()
	oldReplicaNum := len(ecdp.ecReplicas)
	if err = c.removeEcDataReplica(ecdp, host, false); err != nil {
		return
	}
	ecdp.RLock()
	defer ecdp.RUnlock()
	ecdp.ReplicaNum = uint8(oldReplicaNum) - 1
	if err = c.syncUpdateEcDataPartition(ecdp); err != nil {
		ecdp.ReplicaNum = uint8(oldReplicaNum)
	}
	return
}

func (c *Cluster) syncAddEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncAddEcPartition, partition)
}

func (c *Cluster) syncDelEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncDelEcPartition, partition)
}

func (c *Cluster) syncChangeEcPartitionMembers(ecdp *EcDataPartition, recoverAddr string) (resp *proto.Packet, err error) {
	hosts := ecdp.Hosts
	task := ecdp.createTaskToChangeEcPartitionMembers(hosts, recoverAddr)
	ecNode, err := c.ecNode(recoverAddr)
	if err != nil {
		return
	}
	if resp, err = ecNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return resp, nil
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

func (c *Cluster) syncPutEcNodeInfo(opType uint32, ecNode *ECNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ecNodePrefix + strconv.FormatUint(ecNode.ID, 10) + keySeparator + ecNode.Addr
	dnv := newEcNodeValue(ecNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncUpdateEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncUpdateEcPartition, partition)
}

func (c *Cluster) validateDecommissionEcDataPartition(ecdp *EcDataPartition, offlineAddr string) (err error) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	var vol *Vol
	if vol, err = c.getVol(ecdp.VolName); err != nil {
		return
	}
	replicaNum := int(ecdp.ParityUnitsNum + ecdp.DataUnitsNum)
	if err = ecdp.hasMissingOneEcReplica(offlineAddr, replicaNum); err != nil {
		return
	}

	// if the partition can be offline or not
	if err = ecdp.canBeOffLine(offlineAddr); err != nil {
		return
	}

	if c.isEcRecovering(ecdp, offlineAddr) {
		log.LogErrorf("partition [%v] is recovering", ecdp.PartitionID)
		err = fmt.Errorf("vol[%v],ec partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, ecdp.PartitionID, offlineAddr)
		return
	}
	return
}

func (ecdp *EcDataPartition) addEcReplica(replica *EcReplica) {
	for _, r := range ecdp.ecReplicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	ecdp.ecReplicas = append(ecdp.ecReplicas, replica)
}

func (ecdp *EcDataPartition) addEcPeer(ecNode *ECNode) {
	peer := proto.Peer{ID: ecNode.ID, Addr: ecNode.Addr}
	ecdp.Peers = append(ecdp.Peers, peer)
}

func (ecdp *EcDataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	ecNode, err := c.ecNode(nodeAddr)
	if err != nil || ecNode == nil {
		return err
	}
	replica := newEcReplica(ecNode)
	replica.Status = proto.ReadOnly
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	ecdp.addEcReplica(replica)
	ecdp.checkAndRemoveMissReplica(replica.Addr)
	ecdp.addEcPeer(ecNode)

	return
}
func (partition *EcDataPartition) containsBadDisk(diskPath string, nodeAddr string) bool {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.ecReplicas {
		if nodeAddr == replica.Addr && diskPath == replica.DiskPath {
			return true
		}
	}
	return false
}
func (ecdp *EcDataPartition) canBeOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		ecdp.PartitionID, ecdp.Hosts, offlineAddr)
	liveReplicas := ecdp.liveReplicas(defaultDataPartitionTimeOutSec)

	otherLiveReplicas := make([]*EcReplica, 0)
	for i := 0; i < len(liveReplicas); i++ {
		replica := liveReplicas[i]
		if replica.Addr != offlineAddr {
			otherLiveReplicas = append(otherLiveReplicas, replica)
		}
	}

	if len(otherLiveReplicas) < int(ecdp.DataUnitsNum) {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v, ecDataUnitsNum:%v", proto.ErrCannotBeOffLine, len(liveReplicas), ecdp.DataUnitsNum)
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

func (ecdp *EcDataPartition) checkAndRemoveMissReplica(addr string) {
	if _, ok := ecdp.MissingNodes[addr]; ok {
		delete(ecdp.MissingNodes, addr)
	}
}

func (ecdp *EcDataPartition) convertToEcPartitionResponse() (ecdpr *proto.EcPartitionResponse) {
	ecdpr = new(proto.EcPartitionResponse)
	ecdp.Lock()
	defer ecdp.Unlock()
	ecdpr.PartitionID = ecdp.PartitionID
	ecdpr.Status = ecdp.Status
	ecdpr.Hosts = make([]string, len(ecdp.Hosts))
	copy(ecdpr.Hosts, ecdp.Hosts)
	ecdpr.LeaderAddr = ecdp.Hosts[0]
	ecdpr.DataUnitsNum = ecdp.DataUnitsNum
	ecdpr.ParityUnitsNum = ecdp.ParityUnitsNum
	ecdpr.ReplicaNum = ecdp.ReplicaNum
	return
}

func (ecdp *EcDataPartition) createTaskToCreateEcDataPartition(addr string, ecPartitionSize uint64, hosts []string) (task *proto.AdminTask) {
	var nodeIndex uint32
	if index := getIndex(addr, hosts); index > -1 {
		nodeIndex = uint32(index)
	}
	task = proto.NewAdminTask(proto.OpCreateEcDataPartition, addr, newCreateEcPartitionRequest(
		ecdp.VolName, ecdp.PartitionID, ecPartitionSize, uint32(ecdp.DataUnitsNum), uint32(ecdp.ParityUnitsNum), nodeIndex, hosts, ecdp.MaxSripeUintSize))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) createTaskToChangeEcPartitionMembers(newHosts []string, recoverAddr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpChangeEcPartitionMembers, recoverAddr, newChangeEcPartitionMembersRequest(ecdp.PartitionID, newHosts))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) createTaskToDeleteEcPartition(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteEcDataPartition, addr, newDeleteEcPartitionRequest(ecdp.PartitionID))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) deleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range ecdp.ecReplicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("deleteReplicaByIndex replica:%v  index:%v  locations :%v ", ecdp.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := ecdp.ecReplicas[index+1:]
	ecdp.ecReplicas = ecdp.ecReplicas[:index]
	ecdp.ecReplicas = append(ecdp.ecReplicas, replicasAfter...)
}

func (ecdp *EcDataPartition) ecHostsToString() string {
	return strings.Join(ecdp.Hosts, underlineSeparator)
}

func (ecdp *EcDataPartition) getLiveZones(offlineAddr string) (zones []string) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	for _, replica := range ecdp.ecReplicas {
		if replica.ecNode == nil {
			continue
		}
		if replica.ecNode.Addr == offlineAddr {
			continue
		}
		zones = append(zones, replica.ecNode.ZoneName)
	}
	return
}

func (ecdp *EcDataPartition) getEcReplica(addr string) (replica *EcReplica, err error) {
	for index := 0; index < len(ecdp.ecReplicas); index++ {
		replica = ecdp.ecReplicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogErrorf("action[getEcReplica],partitionID:%v,locations:%v,err:%v",
		ecdp.PartitionID, addr, dataReplicaNotFound(addr))
	return nil, errors.Trace(dataReplicaNotFound(addr), "%v not found", addr)
}

// Check if there is a replica missing or not.
func (ecdp *EcDataPartition) hasMissingOneEcReplica(offlineAddr string, replicaNum int) (err error) {
	curHostCount := len(ecdp.Hosts)
	for _, host := range ecdp.Hosts {
		if host == offlineAddr {
			curHostCount = curHostCount - 1
		}
	}
	curReplicaCount := len(ecdp.ecReplicas)
	for _, r := range ecdp.ecReplicas {
		if r.Addr == offlineAddr {
			curReplicaCount = curReplicaCount - 1
		}
	}
	if curReplicaCount < replicaNum-1 || curHostCount < replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneEcReplica", ecdp.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (ecdp *EcDataPartition) liveReplicas(timeOutSec int64) (replicas []*EcReplica) {
	replicas = make([]*EcReplica, 0)
	for i := 0; i < len(ecdp.ecReplicas); i++ {
		replica := ecdp.ecReplicas[i]
		if replica.isLive(timeOutSec) == true && ecdp.hasHost(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}
	return
}

// Remove the replica address from the memory.
func (ecdp *EcDataPartition) removeReplicaByAddr(addr string) {
	delIndex := -1
	var replica *EcReplica
	for i := 0; i < len(ecdp.ecReplicas); i++ {
		replica = ecdp.ecReplicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[removeReplicaByAddr], ec partition:%v  on Node:%v  OffLine,the node is in replicas:%v", ecdp.PartitionID, addr, replica != nil)
	log.LogDebug(msg)
	if delIndex == -1 {
		return
	}
	ecdp.FileInCoreMap = make(map[string]*FileInCore, 0)
	ecdp.deleteReplicaByIndex(delIndex)
	ecdp.modifyTime = time.Now().Unix()

	return
}

func (ecdp *EcDataPartition) removePeerAndHost(rp *ECNode) (newPeers []proto.Peer, newHosts []string) {
	newPeers = make([]proto.Peer, 0, len(ecdp.Peers)-1)
	newHosts = make([]string, 0, len(ecdp.Hosts)-1)
	for _, peer := range ecdp.Peers {
		if peer.ID == rp.ID && peer.Addr == rp.Addr {
			continue
		}
		newPeers = append(newPeers, peer)
	}
	for _, host := range ecdp.Hosts {
		if host == rp.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	return
}
func (ecdp *EcDataPartition) ToProto(c *Cluster) *proto.EcPartitionInfo {
	ecdp.RLock()
	defer ecdp.RUnlock()
	var replicas = make([]*proto.EcReplica, len(ecdp.ecReplicas))
	for i, replica := range ecdp.ecReplicas {
		replicas[i] = &replica.EcReplica
		replicas[i].HttpPort = replica.HttpPort
	}
	zones := make([]string, len(ecdp.Hosts))
	for idx, host := range ecdp.Hosts {
		ecNode, err := c.ecNode(host)
		if err == nil {
			zones[idx] = ecNode.ZoneName
		}
	}
	partition := &proto.DataPartitionInfo{
		PartitionID:     ecdp.PartitionID,
		LastLoadedTime:  ecdp.FinishEcTime,
		Status:          ecdp.Status,
		Hosts:           ecdp.Hosts,
		EcMigrateStatus: ecdp.EcMigrateStatus,
		ReplicaNum:      ecdp.ParityUnitsNum + ecdp.DataUnitsNum,
		Peers:           ecdp.Peers,
		Zones:           zones,
		MissingNodes:    ecdp.MissingNodes,
		VolName:         ecdp.VolName,
		VolID:           ecdp.VolID,
	}
	return &proto.EcPartitionInfo{
		DataPartitionInfo: partition,
		EcReplicas:        replicas,
		ParityUnitsNum:    ecdp.ParityUnitsNum,
		DataUnitsNum:      ecdp.DataUnitsNum,
	}
}

func (ecdp *EcDataPartition) update(action, volName string, newPeers []proto.Peer, newHosts []string, c *Cluster) (err error) {
	orgHosts := make([]string, len(ecdp.Hosts))
	copy(orgHosts, ecdp.Hosts)
	oldPeers := make([]proto.Peer, len(ecdp.Peers))
	copy(oldPeers, ecdp.Peers)
	orgHostsIdx := make([]bool, len(ecdp.hostsIdx))
	copy(orgHostsIdx, ecdp.hostsIdx)
	for i, host := range ecdp.Hosts {
		if contains(newHosts, host) {
			continue
		}
		ecdp.hostsIdx[i] = false
	}
	ecdp.Hosts = newHosts
	ecdp.Peers = newPeers
	if err = c.syncUpdateEcDataPartition(ecdp); err != nil {
		ecdp.Hosts = orgHosts
		ecdp.Peers = oldPeers
		ecdp.hostsIdx = orgHostsIdx
		return errors.Trace(err, "action[%v] update partition[%v] vol[%v] failed", action, ecdp.PartitionID, volName)
	}

	msg := fmt.Sprintf("action[%v] success,vol[%v] partitionID:%v "+
		"oldHosts[%v], newHosts[%v], oldPeers[%v], newPeers[%v]",
		action, volName, ecdp.PartitionID, orgHosts, ecdp.Hosts, oldPeers, ecdp.Peers)
	log.LogInfo(msg)
	return
}

func (ep *EcDataPartition) setEcPartitionUsed(c *Cluster) {
	var (
		used    uint64
		maxUsed uint64
	)
	for _, r := range ep.ecReplicas {
		used += r.Used
		if r.Used > maxUsed {
			maxUsed = r.Used
		}
	}
	replicaNum := ep.ParityUnitsNum + 1
	vol, err := c.getVol(ep.VolName)
	if err == nil {
		replicaNum = vol.dpReplicaNum
	}
	ep.logicUsed = uint64(float64(used) / float64(replicaNum))
	ep.used = maxUsed
}

func (ecdp *EcDataPartition) updateMetric(vr *proto.EcPartitionReport, ecNode *ECNode, c *Cluster) {
	if !ecdp.hasHost(ecNode.Addr) {
		return
	}
	ecdp.Lock()
	defer ecdp.Unlock()
	replica, err := ecdp.getEcReplica(ecNode.Addr)
	if err != nil {
		replica = newEcReplica(ecNode)
		ecdp.addEcReplica(replica)
	}
	ecdp.total = vr.Total
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	replica.NodeIndex = vr.NodeIndex
	ecdp.setEcPartitionUsed(c)
	replica.FileCount = uint32(vr.ExtentCount)
	replica.ReportTime = time.Now().Unix()
	replica.IsLeader = vr.IsLeader
	if ecNode.Addr == ecdp.Hosts[0] {
		ecdp.isRecover = vr.IsRecover
	}
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

func (ecdpCache *EcDataPartitionCache) get(partitionID uint64) (ecdp *EcDataPartition, err error) {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	ecdp, ok := ecdpCache.partitions[partitionID]
	if !ok {
		return nil, proto.ErrEcPartitionNotExists
	}
	return
}

func (ecdpCache *EcDataPartitionCache) delete(partitionID uint64) (err error) {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	_, ok := ecdpCache.partitions[partitionID]
	if !ok {
		return proto.ErrEcPartitionNotExists
	}
	delete(ecdpCache.partitions, partitionID)
	return
}

func (ecdpCache *EcDataPartitionCache) getEcPartitionResponseCache() []byte {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	return ecdpCache.responseCache
}

func (ecdpCache *EcDataPartitionCache) getEcPartitionsView(minPartitionID uint64) (epResps []*proto.EcPartitionResponse) {
	epResps = make([]*proto.EcPartitionResponse, 0)
	log.LogDebugf("volName[%v] EcPartitionMapLen[%v], minPartitionID[%v]",
		ecdpCache.volName, len(ecdpCache.partitions), minPartitionID)
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	for _, ep := range ecdpCache.partitions {
		if ep.PartitionID <= minPartitionID || !proto.IsEcFinished(ep.EcMigrateStatus) {
			continue
		}
		epResp := ep.convertToEcPartitionResponse()
		epResps = append(epResps, epResp)
	}
	return
}

func (ecdpCache *EcDataPartitionCache) put(ecdp *EcDataPartition) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.partitions[ecdp.PartitionID] = ecdp
}

func (ecdpCache *EcDataPartitionCache) setEcPartitionResponseCache(responseCache []byte) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	if responseCache != nil {
		ecdpCache.responseCache = responseCache
	}
}

func (ecdpCache *EcDataPartitionCache) setReadWriteDataPartitions(readWrites int, clusterName string) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.readableAndWritableCnt = readWrites
}

func (ecdpCache *EcDataPartitionCache) updateResponseCache(needsUpdate bool, minPartitionID uint64) (body []byte, err error) {
	responseCache := ecdpCache.getEcPartitionResponseCache()
	if responseCache == nil || needsUpdate || len(responseCache) == 0 {
		epResps := ecdpCache.getEcPartitionsView(minPartitionID)
		if len(epResps) == 0 {
			log.LogDebugf(fmt.Sprintf("action[updateEpResponseCache],volName[%v] minPartitionID:%v,err:%v",
				ecdpCache.volName, minPartitionID, proto.ErrNoAvailEcPartition))
			return nil, proto.ErrNoAvailEcPartition
		}
		cv := proto.NewEcPartitionsView()
		cv.EcPartitions = epResps
		reply := newSuccessHTTPReply(cv)
		if body, err = json.Marshal(reply); err != nil {
			log.LogError(fmt.Sprintf("action[updateEpResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, err.Error()))
			return nil, proto.ErrMarshalData
		}
		ecdpCache.setEcPartitionResponseCache(body)
		return
	}
	body = make([]byte, len(responseCache))
	copy(body, responseCache)

	return
}

func (replica *EcReplica) isActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *EcReplica) isLive(timeOutSec int64) (isAvailable bool) {
	if replica.ecNode.isActive == true && replica.Status != proto.Unavailable &&
		replica.isActive(timeOutSec) == true {
		isAvailable = true
	}

	return
}

func (dpMap *EcDataPartitionCache) checkBadDiskEcDataPartitions(diskPath, nodeAddr string) (partitions []*EcDataPartition) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	partitions = make([]*EcDataPartition, 0)
	for _, ecdp := range dpMap.partitions {
		if !proto.IsEcFinished(ecdp.EcMigrateStatus) {
			continue
		}
		if ecdp.containsBadDisk(diskPath, nodeAddr) {
			partitions = append(partitions, ecdp)
		}
	}
	return
}

func (ecdp *EcDataPartition) appendEcInfoToDataPartitionResponse(dpr *proto.DataPartitionResponse) {
	dpr.EcDataNum = ecdp.DataUnitsNum
	dpr.EcMaxUnitSize = ecdp.MaxSripeUintSize
	dpr.EcMigrateStatus = ecdp.EcMigrateStatus
	dpr.EcHosts = make([]string, len(ecdp.Hosts))
	copy(dpr.EcHosts, ecdp.Hosts)
	dpr.Status = proto.ReadOnly
	dpr.IsRecover = ecdp.isRecover
}

func (ecMap *EcDataPartitionCache) totalUsedSpace() (totalUsed uint64) {
	ecMap.RLock()
	defer ecMap.RUnlock()
	for _, ep := range ecMap.partitions {
		if !proto.IsEcFinished(ep.EcMigrateStatus) {
			continue
		}
		totalUsed = totalUsed + ep.logicUsed
	}
	return
}

func (m *Server) setEcPartitionRollBack(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		ID        uint64
		needDelEc bool
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if ID, err = extractPartitionID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if needDelEc, err = extractNeedDelEc(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.ecRollBack(ID, needDelEc); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("ec partitionID :%v  rollback successfully", ID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (ecdp *EcDataPartition) canDelDp(doDelDpAlreadyEcTime, ecMigrationSaveTime int64) bool {
	curTime := time.Now().Unix()
	if doDelDpAlreadyEcTime != 0 && curTime-doDelDpAlreadyEcTime >= 5*EcTimeMinute {
		return true
	}

	if curTime-ecdp.FinishEcTime >= ecMigrationSaveTime*EcTimeMinute {
		return true
	}
	return false
}
