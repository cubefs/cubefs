// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	authSDK "github.com/cubefs/cubefs/sdk/auth"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

var (
	clusterDpCntLimit uint64
	clusterMpCntLimit uint64
)

// nolint: structcheck
type ClusterVolSubItem struct {
	vols                map[string]*Vol
	delayDeleteVolsInfo []*delayDeleteVolInfo
	volMutex            sync.RWMutex // volume mutex
	createVolMutex      sync.RWMutex // create volume mutex
	deleteVolMutex      sync.RWMutex // delete volume mutex
}

// nolint: structcheck
type ClusterTopoSubItem struct {
	dataNodes sync.Map
	metaNodes sync.Map
	lcNodes   sync.Map

	idAlloc            *IDAllocator
	t                  *topology
	dataNodeStatInfo   *nodeStatInfo
	dataStatsByMedia   map[string]*nodeStatInfo
	metaNodeStatInfo   *nodeStatInfo
	zoneStatInfos      map[string]*proto.ZoneStat
	volStatInfo        sync.Map
	zoneIdxMux         sync.Mutex //
	lastZoneIdxForNode int

	checkAutoCreateDataPartition bool
	FaultDomain                  bool
	needFaultDomain              bool // FaultDomain is true and normal zone already used up
	domainManager                *DomainManager

	inodeCountNotEqualMP  *sync.Map
	maxInodeNotEqualMP    *sync.Map
	dentryCountNotEqualMP *sync.Map

	mnMutex sync.RWMutex // meta node mutex
	dnMutex sync.RWMutex // data node mutex
	nsMutex sync.RWMutex // nodeset mutex
}

// nolint: structcheck
type ClusterDecommission struct {
	BadDataPartitionIds     *sync.Map
	BadMetaPartitionIds     *sync.Map
	DecommissionDisks       sync.Map
	DecommissionLimit       uint64
	AutoDecommissionDiskMux sync.Mutex
	DecommissionDiskLimit   uint32
	MarkDiskBrokenThreshold atomicutil.Float64
	badPartitionMutex       sync.RWMutex // BadDataPartitionIds and BadMetaPartitionIds operate mutex

	ForbidMpDecommission        bool
	EnableAutoDpMetaRepair      atomicutil.Bool
	EnableAutoDecommissionDisk  atomicutil.Bool
	AutoDecommissionInterval    atomicutil.Int64
	AutoDpMetaRepairParallelCnt atomicutil.Uint32
	server                      *Server
}

type CleanTask struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	TaskCnt   int       `json:"taskCount"`
	FreezeCnt int       `json:"freezeCount"`
	CleanCnt  int       `json:"cleanCount"`
	ResetCnt  int       `json:"resetCount"`
	Timeout   time.Time `json:"-"`
}

// Cluster stores all the cluster-level information.
type Cluster struct {
	Name            string
	CreateTime      int64
	clusterUuid     string
	leaderInfo      *LeaderInfo
	cfg             *clusterConfig
	fsm             *MetadataFsm
	partition       raftstore.Partition
	MasterSecretKey []byte
	retainLogs      uint64
	stopc           chan bool
	stopFlag        int32
	wg              sync.WaitGroup

	ClusterVolSubItem
	ClusterTopoSubItem
	ClusterDecommission

	metaReady               bool
	DisableAutoAllocate     bool
	diskQosEnable           bool
	checkDataReplicasEnable bool
	fileStatsEnable         bool
	clusterUuidEnable       bool
	authenticate            bool
	legacyDataMediaType     uint32
	dataMediaTypeVaild      bool

	S3ApiQosQuota  *sync.Map // (api,uid,limtType) -> limitQuota
	QosAcceptLimit *rate.Limiter
	apiLimiter     *ApiLimiter

	followerReadManager *followerReadManager
	lcMgr               *lifecycleManager
	snapshotMgr         *snapshotDelManager

	ac           *authSDK.AuthClient
	masterClient *masterSDK.MasterClient

	flashNodeTopo *flashNodeTopology

	cleanTask map[string]*CleanTask
	Cleaning  bool
	mu        sync.Mutex
	PlanRun   bool
}

type cTask struct {
	name     string
	tickTime time.Duration
	function func() bool
	noWait   bool
}

type delayDeleteVolInfo struct {
	volName  string
	authKey  string
	execTime time.Time
	user     *User
}

type followerReadManager struct {
	volDataPartitionsView     map[string][]byte
	volDataPartitionsCompress map[string][]byte
	status                    map[string]bool
	lastUpdateTick            map[string]time.Time
	needCheck                 bool
	c                         *Cluster
	volViewMap                map[string]*volValue
	rwMutex                   sync.RWMutex
}

func newFollowerReadManager(c *Cluster) (mgr *followerReadManager) {
	mgr = new(followerReadManager)
	mgr.volDataPartitionsView = make(map[string][]byte)
	mgr.volDataPartitionsCompress = make(map[string][]byte)
	mgr.status = make(map[string]bool)
	mgr.lastUpdateTick = make(map[string]time.Time)
	mgr.c = c
	return
}

func (mgr *followerReadManager) reSet() {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	mgr.volDataPartitionsView = make(map[string][]byte)
	mgr.volDataPartitionsCompress = make(map[string][]byte)
	mgr.status = make(map[string]bool)
	mgr.lastUpdateTick = make(map[string]time.Time)
}

func (mgr *followerReadManager) getVolumeDpView() {
	var (
		err      error
		volViews []*volValue
		view     *proto.DataPartitionsView
	)
	if err, volViews = mgr.c.loadVolsViews(); err != nil {
		panic(err)
	}

	if len(volViews) == 0 {
		return
	}

	mgr.rwMutex.Lock()
	mgr.volViewMap = make(map[string]*volValue)
	for _, vv := range volViews {
		mgr.volViewMap[vv.Name] = vv

		if _, ok := mgr.lastUpdateTick[vv.Name]; !ok {
			// record when first discovery the volume
			mgr.lastUpdateTick[vv.Name] = time.Now()
			mgr.status[vv.Name] = false
		}
	}
	mgr.rwMutex.Unlock()

	if mgr.c.leaderInfo.id == 0 {
		log.LogErrorf("followerReadManager.getVolumeDpView but master leader not ready")
		return
	}

	avgSleepTime := time.Second * 5 / time.Duration(len(volViews))

	for _, vv := range volViews {
		if (vv.Status == proto.VolStatusMarkDelete && !vv.Forbidden) || (vv.Status == proto.VolStatusMarkDelete && vv.Forbidden && time.Since(vv.DeleteExecTime) <= 0) {
			mgr.rwMutex.Lock()
			mgr.lastUpdateTick[vv.Name] = time.Now()
			mgr.status[vv.Name] = false
			mgr.rwMutex.Unlock()
			continue
		}

		mgr.c.masterClient.SetLeader(mgr.c.leaderInfo.addr)
		log.LogDebugf("followerReadManager.getVolumeDpView %v leader(%v)", vv.Name, mgr.c.masterClient.Leader())
		if view, err = mgr.c.masterClient.ClientAPI().GetDataPartitionsFromLeader(vv.Name); err != nil {
			log.LogErrorf("followerReadManager.getVolumeDpView %v GetDataPartitions err %v leader(%v)", vv.Name, err, mgr.c.masterClient.Leader())
			continue
		}
		time.Sleep(avgSleepTime)
		mgr.updateVolViewFromLeader(vv.Name, view)
	}
}

func (mgr *followerReadManager) sendFollowerVolumeDpView() {
	var err error
	vols := mgr.c.copyVols()

	if len(vols) == 0 {
		return
	}
	avgSleepTime := time.Second * 5 / time.Duration(len(vols))
	for _, vol := range vols {
		log.LogDebugf("followerReadManager.sendFollowerVolumeDpView %v", vol.Name)
		if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && time.Until(vol.DeleteExecTime) <= 0) {
			continue
		}
		time.Sleep(avgSleepTime)

		var body []byte
		if body, err = vol.getDataPartitionsView(); err != nil {
			log.LogErrorf("followerReadManager.sendFollowerVolumeDpView err %v", err)
			continue
		}
		for _, addr := range AddrDatabase {
			if addr == mgr.c.leaderInfo.addr {
				continue
			}

			mgr.c.masterClient.SetLeader(addr)
			if err = mgr.c.masterClient.AdminAPI().PutDataPartitions(vol.Name, body); err != nil {
				mgr.c.masterClient.SetLeader("")
				log.LogErrorf("followerReadManager.sendFollowerVolumeDpView PutDataPartitions name %v addr %v err %v", vol.Name, addr, err)
				continue
			}
			mgr.c.masterClient.SetLeader("")
			log.LogDebugf("followerReadManager.sendFollowerVolumeDpView PutDataPartitions name %v addr %v err %v", vol.Name, addr, err)
		}
	}
}

// NOTICE: caller must correctly use mgr.rwMutex
func (mgr *followerReadManager) isVolRecordObsolete(volName string) bool {
	volView, ok := mgr.volViewMap[volName]
	if !ok {
		// vol has been completely deleted
		return true
	}
	return (volView.Status == proto.VolStatusMarkDelete && !volView.Forbidden) ||
		(volView.Status == proto.VolStatusMarkDelete && volView.Forbidden && time.Until(volView.DeleteExecTime) <= 0)
}

func (mgr *followerReadManager) DelObsoleteVolRecord(obsoleteVolNames map[string]struct{}) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	for volName := range obsoleteVolNames {
		log.LogDebugf("followerReadManager.DelObsoleteVolRecord, delete obsolete vol: %v", volName)
		delete(mgr.volDataPartitionsView, volName)
		delete(mgr.volDataPartitionsCompress, volName)
		delete(mgr.status, volName)
		delete(mgr.lastUpdateTick, volName)
	}
}

func (mgr *followerReadManager) checkStatus() {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	timeNow := time.Now()
	for volNm, lastTime := range mgr.lastUpdateTick {
		if mgr.isVolRecordObsolete(volNm) {
			log.LogDebugf("action[checkStatus] volume %v is obsolete, skip it", volNm)
			continue
		}

		if lastTime.Before(timeNow.Add(-5 * time.Minute)) {
			mgr.status[volNm] = false
			log.LogWarnf("action[checkStatus] volume %v expired last time %v, now %v", volNm, lastTime, timeNow)
		}
	}
}

func (mgr *followerReadManager) updateVolViewFromLeader(key string, view *proto.DataPartitionsView) {
	if !mgr.checkViewContent(key, view, true) {
		log.LogErrorf("updateVolViewFromLeader. key %v checkViewContent failed status %v", key, mgr.status[key])
		return
	}

	reply := newSuccessHTTPReply(view)
	if body, err := json.Marshal(reply); err != nil {
		log.LogErrorf("action[updateDpResponseCache] marshal error %v", err)
		return
	} else {
		mgr.rwMutex.Lock()
		defer mgr.rwMutex.Unlock()
		mgr.volDataPartitionsView[key] = body
		gzipData, err := compressor.New(compressor.EncodingGzip).Compress(body)
		if err != nil {
			log.LogErrorf("action[updateDpResponseCache] compress error:%+v", err)
			return
		}
		mgr.volDataPartitionsCompress[key] = gzipData
	}
	mgr.status[key] = true
	mgr.lastUpdateTick[key] = time.Now()
}

func (mgr *followerReadManager) checkViewContent(volName string, view *proto.DataPartitionsView, isUpdate bool) (ok bool) {
	if !isUpdate && !mgr.needCheck {
		return true
	}

	if len(view.DataPartitions) == 0 {
		return true
	}
	for i := 0; i < len(view.DataPartitions); i++ {
		dp := view.DataPartitions[i]
		if len(dp.Hosts) == 0 {
			log.LogErrorf("checkViewContent. vol %v, dp id %v, leader %v, status %v",
				volName, dp.PartitionID, dp.LeaderAddr, dp.Status)
		}
	}
	return true
}

func (mgr *followerReadManager) getVolViewAsFollower(key string, compress bool) (value []byte, ok bool) {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()
	ok = true
	if compress {
		value = mgr.volDataPartitionsCompress[key]
	} else {
		value = mgr.volDataPartitionsView[key]
	}
	log.LogDebugf("getVolViewAsFollower. volume %v return!", key)
	return
}

func (mgr *followerReadManager) IsVolViewReady(volName string) bool {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()
	if status, ok := mgr.status[volName]; ok {
		return status
	}
	return false
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition,
	cfg *clusterConfig, server *Server,
) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol)
	c.delayDeleteVolsInfo = make([]*delayDeleteVolInfo, 0)
	c.stopc = make(chan bool)
	c.cfg = cfg
	if clusterDpCntLimit == 0 {
		atomic.StoreUint64(&clusterDpCntLimit, defaultMaxDpCntLimit)
	}
	if clusterMpCntLimit == 0 {
		atomic.StoreUint64(&clusterMpCntLimit, defaultMaxMpCntLimit)
	}
	c.t = newTopology()
	c.BadDataPartitionIds = new(sync.Map)
	c.BadMetaPartitionIds = new(sync.Map)
	c.dataNodeStatInfo = new(nodeStatInfo)
	c.dataStatsByMedia = make(map[string]*proto.NodeStatInfo)
	c.metaNodeStatInfo = new(nodeStatInfo)
	c.FaultDomain = cfg.faultDomain
	c.zoneStatInfos = make(map[string]*proto.ZoneStat)
	c.followerReadManager = newFollowerReadManager(c)
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.domainManager = newDomainManager(c)
	c.QosAcceptLimit = rate.NewLimiter(rate.Limit(c.cfg.QosMasterAcceptLimit), proto.QosDefaultBurst)
	c.apiLimiter = newApiLimiter()
	c.DecommissionLimit = defaultDecommissionParallelLimit
	c.checkAutoCreateDataPartition = false
	c.masterClient = masterSDK.NewMasterClient(nil, false)
	c.masterClient.SetTransport(proto.GetHttpTransporter(&proto.HttpCfg{
		PoolSize: int(cfg.httpPoolSize),
	}))
	c.inodeCountNotEqualMP = new(sync.Map)
	c.maxInodeNotEqualMP = new(sync.Map)
	c.dentryCountNotEqualMP = new(sync.Map)
	c.lcMgr = newLifecycleManager()
	c.lcMgr.cluster = c
	c.snapshotMgr = newSnapshotManager()
	c.snapshotMgr.cluster = c
	c.S3ApiQosQuota = new(sync.Map)
	c.MarkDiskBrokenThreshold.Store(defaultMarkDiskBrokenThreshold)
	c.EnableAutoDpMetaRepair.Store(defaultEnableDpMetaRepair)
	c.AutoDecommissionInterval.Store(int64(defaultAutoDecommissionDiskInterval))
	c.server = server
	c.flashNodeTopo = newFlashNodeTopology()
	c.cleanTask = make(map[string]*CleanTask)
	c.PlanRun = false
	return
}

func (c *Cluster) scheduleTask() {
	c.scheduleToCheckDelayDeleteVols()
	c.scheduleToCheckDataPartitions()
	c.scheduleToLoadDataPartitions()
	c.scheduleToCheckReleaseDataPartitions()
	c.scheduleToCheckHeartbeat()
	c.scheduleToCheckMetaPartitions()
	c.scheduleToUpdateStatInfo()
	c.scheduleToManageDp()
	c.scheduleToCheckVolStatus()
	c.scheduleToCheckVolQos()
	c.scheduleToCheckDiskRecoveryProgress()
	c.scheduleToCheckMetaPartitionRecoveryProgress()
	c.scheduleToLoadMetaPartitions()
	c.scheduleToCheckNodeSetGrpManagerStatus()
	c.scheduleToCheckFollowerReadCache()
	c.scheduleToCheckDecommissionDataNode()
	c.scheduleToCheckDecommissionDisk()
	c.scheduleToLcScan()
	c.scheduleToSnapshotDelVerScan()
	c.scheduleToBadDisk()
	c.scheduleToCheckVolUid()
	c.scheduleToCheckDataReplicaMeta()
	c.scheduleToUpdateFlashGroupRespCache()
	c.scheduleStartBalanceTask()
	c.scheduleToUpdateFlashGroupSlots()
}

func (c *Cluster) masterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) tryToChangeLeaderByHost() error {
	return c.partition.TryToLeader(1)
}

func (c *Cluster) scheduleToUpdateStatInfo() {
	c.runTask(
		&cTask{
			tickTime: 2 * time.Minute,
			name:     "scheduleToUpdateStatInfo",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.updateStatInfo()
				}
				return
			},
		})
}

func (c *Cluster) addNodeSetGrp(ns *nodeSet, load bool) (err error) {
	log.LogWarnf("addNodeSetGrp nodeSet id[%v] zonename[%v] load[%v] grpManager init[%v]",
		ns.ID, ns.zoneName, load, c.domainManager.init)

	if c.domainManager.init {
		err = c.domainManager.putNodeSet(ns, load)
		c.putZoneDomain(false)
	}
	return
}

const (
	TypeMetaPartition uint32 = 0x01
	TypeDataPartition uint32 = 0x02
)

func (c *Cluster) getHostFromDomainZone(domainId uint64, createType uint32, replicaNum uint8, mediaType uint32) (hosts []string, peers []proto.Peer, err error) {
	hosts, peers, err = c.domainManager.getHostFromNodeSetGrp(domainId, replicaNum, createType, mediaType)
	return
}

func (c *Cluster) IsLeader() bool {
	if c.partition != nil {
		return c.partition.IsRaftLeader()
	}
	return false
}

func (c *Cluster) scheduleToManageDp() {
	go func() {
		// check volumes after switching leader two minutes
		time.Sleep(2 * time.Minute)
		c.checkAutoCreateDataPartition = true
	}()

	// schedule delete dataPartition
	c.runTask(
		&cTask{
			tickTime: 2 * time.Minute,
			name:     "scheduleToManageDp",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					vols := c.copyVols()
					for _, vol := range vols {
						if proto.IsHot(vol.VolType) {
							continue
						}
						vol.autoDeleteDp(c)
					}
				}
				return
			},
		})
}

func (c *Cluster) runTask(task *cTask) {
	if !task.noWait {
		c.wg.Add(1)
	}
	go func() {
		if !task.noWait {
			defer c.wg.Done()
		}
		log.LogWarnf("runTask %v start!", task.name)
		currTickTm := task.tickTime
		ticker := time.NewTicker(currTickTm)
		for {
			select {
			case <-ticker.C:
				if task.function() {
					log.LogWarnf("runTask %v exit!", task.name)
					ticker.Stop()
					return
				}
				if currTickTm != task.tickTime { // there's no conflict, thus no need consider consistency between tickTime and currTickTm
					ticker.Reset(task.tickTime)
					currTickTm = task.tickTime
				}
			case <-c.stopc:
				log.LogWarnf("runTask %v exit!", task.name)
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cluster) scheduleToCheckDelayDeleteVols() {
	c.runTask(
		&cTask{
			tickTime: 5 * time.Second,
			name:     "scheduleToCheckDelayDeleteVols",
			function: func() (fin bool) {
				if len(c.delayDeleteVolsInfo) == 0 {
					return
				}
				c.deleteVolMutex.Lock()
				for index := 0; index < len(c.delayDeleteVolsInfo); index++ {
					currentDeleteVol := c.delayDeleteVolsInfo[index]
					log.LogDebugf("action[scheduleToCheckDelayDeleteVols] currentDeleteVol[%v]", currentDeleteVol)
					if time.Until(currentDeleteVol.execTime) > 0 {
						continue
					}
					go func() {
						if err := currentDeleteVol.user.deleteVolPolicy(currentDeleteVol.volName); err != nil {
							msg := fmt.Sprintf("delete vol[%v] failed: err:[%v]", currentDeleteVol.volName, err)
							log.LogError(msg)
							return
						}
						msg := fmt.Sprintf("delete vol[%v] successfully", currentDeleteVol.volName)
						log.LogWarn(msg)
					}()
					if len(c.delayDeleteVolsInfo) == 1 {
						c.delayDeleteVolsInfo = make([]*delayDeleteVolInfo, 0)
						continue
					}
					if index == 0 {
						c.delayDeleteVolsInfo = c.delayDeleteVolsInfo[index+1:]
					} else if index == len(c.delayDeleteVolsInfo)-1 {
						c.delayDeleteVolsInfo = c.delayDeleteVolsInfo[:index]
					} else {
						c.delayDeleteVolsInfo = append(c.delayDeleteVolsInfo[:index], c.delayDeleteVolsInfo[index+1:]...)
					}
				}
				c.deleteVolMutex.Unlock()
				return
			},
		})
}

func (c *Cluster) scheduleToCheckDataPartitions() {
	c.runTask(&cTask{
		tickTime: time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition),
		name:     "scheduleToCheckDataPartitions",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkDataPartitions()
			}
			return
		},
	})
}

func (c *Cluster) scheduleToCheckVolStatus() {
	c.runTask(&cTask{
		tickTime: time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition),
		name:     "scheduleToCheckVolStatus",
		function: func() (fin bool) {
			if c.partition.IsRaftLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkStatus(c)
					vol.CheckStrategy(c)
				}
			}
			return
		},
	})
}

func (c *Cluster) scheduleToCheckFollowerReadCache() {
	task := &cTask{tickTime: time.Second, name: "scheduleToCheckFollowerReadCache"}
	task.function = func() (fin bool) {
		if !c.cfg.EnableFollowerCache {
			return true
		}
		begin := time.Now()
		if !c.partition.IsRaftLeader() {
			c.followerReadManager.getVolumeDpView()
			c.followerReadManager.checkStatus()
		} else {
			c.followerReadManager.sendFollowerVolumeDpView()
		}
		end := time.Now()
		if end.Sub(begin).Seconds() > 5 {
			return
		}
		task.tickTime = time.Second*5 - end.Sub(begin)
		return
	}
	c.runTask(task)
}

func (c *Cluster) scheduleToCheckVolQos() {
	c.runTask(
		&cTask{
			tickTime: time.Duration(float32(time.Second) * 0.5),
			name:     "scheduleToCheckVolQos",
			function: func() (fin bool) {
				if c.partition.IsRaftLeader() {
					vols := c.copyVols()
					for _, vol := range vols {
						vol.checkQos()
					}
				}
				return
			},
		})
}

func (c *Cluster) scheduleToCheckVolUid() {
	c.runTask(
		&cTask{
			tickTime: time.Duration(float32(time.Second) * 0.5),
			name:     "scheduleToCheckVolUid",
			function: func() (fin bool) {
				if c.partition.IsRaftLeader() {
					vols := c.copyVols()
					for _, vol := range vols {
						vol.uidSpaceManager.scheduleUidUpdate()
						vol.uidSpaceManager.reCalculate()
					}
				}
				return
			},
		})
}

func (c *Cluster) scheduleToCheckNodeSetGrpManagerStatus() {
	task := &cTask{tickTime: time.Second, name: "scheduleToCheckNodeSetGrpManagerStatus"}
	task.function = func() (fin bool) {
		if !c.FaultDomain || !c.partition.IsRaftLeader() {
			task.tickTime = time.Minute
			return
		}
		c.domainManager.checkAllGrpState()
		c.domainManager.checkExcludeZoneState()
		task.tickTime = 5 * time.Second
		return
	}
	c.runTask(task)
}

func (c *Cluster) scheduleToLoadDataPartitions() {
	c.runTask(
		&cTask{
			tickTime: 5 * time.Second,
			name:     "scheduleToLoadDataPartitions",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.doLoadDataPartitions()
				}
				return
			},
			noWait: true,
		})
}

// Check the replica status of each data partition.
func (c *Cluster) checkDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDataPartitions occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		vol.checkDataPartitions(c)
		if c.metaReady {
			vol.dataPartitions.updateResponseCache(true, 0, vol)
			vol.dataPartitions.updateCompressCache(true, 0, vol)
		}
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite partitions:%v  ",
			vol.Name, vol.dataPartitions.readableAndWritableCnt)
		log.LogInfo(msg)

		if c.checkAutoCreateDataPartition {
			vol.checkAutoDataPartitionCreation(c)
		}
	}
}

func (c *Cluster) doLoadDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doLoadDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doLoadDataPartitions occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) ||
			(vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && time.Until(vol.DeleteExecTime) <= 0) {
			continue
		}
		vol.loadDataPartition(c)
	}
}

func (c *Cluster) scheduleToCheckReleaseDataPartitions() {
	c.runTask(
		&cTask{
			tickTime: time.Second * defaultIntervalToFreeDataPartition,
			name:     "scheduleToCheckReleaseDataPartitions",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.releaseDataPartitionAfterLoad()
				}
				return
			},
		})
}

// Release the memory used for loading the data partition.
func (c *Cluster) releaseDataPartitionAfterLoad() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("releaseDataPartitionAfterLoad occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"releaseDataPartitionAfterLoad occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		vol.releaseDataPartitions(c.cfg.numberOfDataPartitionsToFree, c.cfg.secondsToFreeDataPartitionAfterLoad)
	}
}

func (c *Cluster) scheduleToCheckHeartbeat() {
	c.runTask(
		&cTask{
			tickTime: time.Second * defaultIntervalToCheckHeartbeat,
			name:     "scheduleToCheckHeartbeat_checkDataNodeHeartbeat",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkDataNodeHeartbeat()
					// update load factor
					setOverSoldFactor(c.cfg.ClusterLoadFactor)
				}
				return
			},
		})

	c.runTask(
		&cTask{
			tickTime: time.Second * defaultIntervalToCheckHeartbeat,
			name:     "scheduleToCheckHeartbeat_checkMetaNodeHeartbeat",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkMetaNodeHeartbeat()
				}
				return
			},
		})

	c.runTask(
		&cTask{
			tickTime: time.Second * defaultIntervalToCheckHeartbeat,
			name:     "scheduleToCheckHeartbeat_checkLcNodeHeartbeat",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkLcNodeHeartbeat()
				}
				return
			},
		})

	go func() {
		ticker := time.NewTicker(time.Second * defaultIntervalToCheckHeartbeat)
		defer ticker.Stop()
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkFlashNodeHeartbeat()
			}
			<-ticker.C
		}
	}()
}

func (c *Cluster) checkDataNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	id := uuid.New()
	log.LogDebugf("checkDataNodeHeartbeat start %v", id.String())
	c.dataNodes.Range(func(addr, dataNode interface{}) bool {
		node := dataNode.(*DataNode)
		node.checkLiveness()
		log.LogDebugf("checkDataNodeHeartbeat checkLiveness for data node %v  %v", node.Addr, id.String())
		task := node.createHeartbeatTask(c.masterAddr(), c.diskQosEnable, c.GetDecommissionDataPartitionBackupTimeOut().String(),
			c.cfg.forbidWriteOpOfProtoVer0, c.RaftPartitionCanUsingDifferentPortEnabled())
		log.LogDebugf("checkDataNodeHeartbeat createHeartbeatTask for data node %v task %v %v", node.Addr,
			task.RequestID, id.String())
		hbReq := task.Request.(*proto.HeartBeatRequest)
		c.volMutex.RLock()
		defer c.volMutex.RUnlock()
		for _, vol := range c.vols {
			if vol.Forbidden {
				hbReq.ForbiddenVols = append(hbReq.ForbiddenVols, vol.Name)
			}
			if vol.dpRepairBlockSize != proto.DefaultDpRepairBlockSize {
				hbReq.VolDpRepairBlockSize[vol.Name] = vol.dpRepairBlockSize
			}

			if vol.DirectRead {
				hbReq.DirectReadVols = append(hbReq.DirectReadVols, vol.Name)
			}

			if vol.ForbidWriteOpOfProtoVer0.Load() {
				hbReq.VolsForbidWriteOpOfProtoVer0 = append(hbReq.VolsForbidWriteOpOfProtoVer0, vol.Name)
			}
		}
		tasks = append(tasks, task)
		return true
	})
	log.LogDebugf("checkDataNodeHeartbeat add task %v", id.String())
	c.addDataNodeTasks(tasks)
	log.LogDebugf("checkDataNodeHeartbeat end %v", id.String())
}

func (c *Cluster) checkMetaNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)

	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.createHeartbeatTask(c.masterAddr(), c.fileStatsEnable, c.cfg.forbidWriteOpOfProtoVer0, c.RaftPartitionCanUsingDifferentPortEnabled())
		hbReq := task.Request.(*proto.HeartBeatRequest)

		c.volMutex.RLock()
		defer c.volMutex.RUnlock()

		for _, vol := range c.vols {
			if vol.FollowerRead {
				hbReq.FLReadVols = append(hbReq.FLReadVols, vol.Name)
			}
			if vol.DisableAuditLog {
				hbReq.DisableAuditVols = append(hbReq.DisableAuditVols, vol.Name)
			}
			if vol.Forbidden {
				hbReq.ForbiddenVols = append(hbReq.ForbiddenVols, vol.Name)
			}
			if vol.ForbidWriteOpOfProtoVer0.Load() {
				hbReq.VolsForbidWriteOpOfProtoVer0 = append(hbReq.VolsForbidWriteOpOfProtoVer0, vol.Name)
			}

			spaceInfo := vol.uidSpaceManager.getSpaceOp()
			hbReq.UidLimitInfo = append(hbReq.UidLimitInfo, spaceInfo...)

			if vol.quotaManager != nil {
				quotaHbInfos := vol.quotaManager.getQuotaHbInfos()
				if len(quotaHbInfos) != 0 {
					hbReq.QuotaHbInfos = append(hbReq.QuotaHbInfos, quotaHbInfos...)
				}
			}

			hbReq.TxInfo = append(hbReq.TxInfo, &proto.TxInfo{
				Volume:     vol.Name,
				Mask:       vol.enableTransaction,
				OpLimitVal: vol.txOpLimit,
			})
		}
		log.LogDebugf("checkMetaNodeHeartbeat start")
		for _, info := range hbReq.QuotaHbInfos {
			log.LogDebugf("checkMetaNodeHeartbeat info [%v]", info)
		}
		tasks = append(tasks, task)
		return true
	})

	c.addMetaNodeTasks(tasks)
}

func (c *Cluster) checkLcNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	diedNodes := make([]string, 0)
	c.lcNodes.Range(func(addr, lcNode interface{}) bool {
		node := lcNode.(*LcNode)
		node.checkLiveness()
		if !node.IsActive {
			log.LogInfof("checkLcNodeHeartbeat: lcnode(%v) is inactive", node.Addr)
			diedNodes = append(diedNodes, node.Addr)
			return true
		}
		task := node.createHeartbeatTask(c.masterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.addLcNodeTasks(tasks)
	for _, node := range diedNodes {
		log.LogInfof("checkLcNodeHeartbeat: deregister node(%v)", node)
		_ = c.delLcNode(node)
	}
}

func (c *Cluster) scheduleToCheckMetaPartitions() {
	c.runTask(
		&cTask{
			tickTime: time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition),
			name:     "scheduleToCheckMetaPartitions",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkMetaPartitions()
				}
				return
			},
		})
}

func (c *Cluster) checkMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaPartitions occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		vol.checkMetaPartitions(c)
	}
}

func (c *Cluster) getInvalidIDNodes() (nodes []*InvalidNodeView) {
	metaNodes := c.getNotConsistentIDMetaNodes()
	nodes = append(nodes, metaNodes...)
	dataNodes := c.getNotConsistentIDDataNodes()
	nodes = append(nodes, dataNodes...)
	return
}

func (c *Cluster) getNotConsistentIDMetaNodes() (metaNodes []*InvalidNodeView) {
	metaNodes = make([]*InvalidNodeView, 0)
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		notConsistent, oldID := c.hasNotConsistentIDMetaPartitions(metanode)
		if notConsistent {
			metaNodes = append(metaNodes, &InvalidNodeView{Addr: metanode.Addr, ID: metanode.ID, OldID: oldID, NodeType: "meta"})
		}
		return true
	})
	return
}

func (c *Cluster) hasNotConsistentIDMetaPartitions(metanode *MetaNode) (notConsistent bool, oldID uint64) {
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			for _, peer := range mp.Peers {
				if peer.Addr == metanode.Addr && peer.ID != metanode.ID {
					vol.mpsLock.RUnlock()
					return true, peer.ID
				}
			}
		}
		vol.mpsLock.RUnlock()
	}
	return
}

func (c *Cluster) getNotConsistentIDDataNodes() (dataNodes []*InvalidNodeView) {
	dataNodes = make([]*InvalidNodeView, 0)
	c.dataNodes.Range(func(key, value interface{}) bool {
		datanode, ok := value.(*DataNode)
		if !ok {
			return true
		}
		notConsistent, oldID := c.hasNotConsistentIDDataPartitions(datanode)
		if notConsistent {
			dataNodes = append(dataNodes, &InvalidNodeView{Addr: datanode.Addr, ID: datanode.ID, OldID: oldID, NodeType: "data"})
		}
		return true
	})
	return
}

func (c *Cluster) hasNotConsistentIDDataPartitions(datanode *DataNode) (notConsistent bool, oldID uint64) {
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.dataPartitions.partitions {
			for _, peer := range mp.Peers {
				if peer.Addr == datanode.Addr && peer.ID != datanode.ID {
					return true, peer.ID
				}
			}
		}
	}
	return
}

func (c *Cluster) updateDataNodeBaseInfo(nodeAddr string, id uint64) (err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	value, ok := c.dataNodes.Load(nodeAddr)
	if !ok {
		err = fmt.Errorf("node %v is not exist", nodeAddr)
		return
	}
	dataNode := value.(*DataNode)
	if dataNode.ID == id {
		return
	}
	cmds := make(map[string]*RaftCmd)
	metadata, err := c.buildDeleteDataNodeCmd(dataNode)
	if err != nil {
		return
	}
	cmds[metadata.K] = metadata
	dataNode.ID = id
	metadata, err = c.buildUpdateDataNodeCmd(dataNode)
	if err != nil {
		return
	}
	cmds[metadata.K] = metadata
	if err = c.syncBatchCommitCmd(cmds); err != nil {
		return
	}
	// partitions := c.getAllMetaPartitionsByMetaNode(nodeAddr)
	return
}

func (c *Cluster) updateMetaNodeBaseInfo(nodeAddr string, id uint64) (err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	value, ok := c.metaNodes.Load(nodeAddr)
	if !ok {
		err = fmt.Errorf("node %v is not exist", nodeAddr)
		return
	}
	metaNode := value.(*MetaNode)
	if metaNode.ID == id {
		return
	}
	cmds := make(map[string]*RaftCmd)
	metadata, err := c.buildDeleteMetaNodeCmd(metaNode)
	if err != nil {
		return
	}
	cmds[metadata.K] = metadata
	metaNode.ID = id
	metadata, err = c.buildUpdateMetaNodeCmd(metaNode)
	if err != nil {
		return
	}
	cmds[metadata.K] = metadata
	if err = c.syncBatchCommitCmd(cmds); err != nil {
		return
	}
	// partitions := c.getAllMetaPartitionsByMetaNode(nodeAddr)
	return
}

// RaftPartitionCanUsingDifferentPortEnabled check whether raft partition can use different port or not
func (c *Cluster) RaftPartitionCanUsingDifferentPortEnabled() bool {
	if c.cfg.raftPartitionAlreadyUseDifferentPort.Load() {
		// this cluster has already enabled this feature
		return true
	}

	if !c.cfg.raftPartitionCanUseDifferentPort.Load() {
		// user currently don't  enable this feature
		return false
	}

	// user currently try to enable this feature
	enabled := true
	c.mnMutex.RLock()
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		if len(metaNode.HeartbeatPort) == 0 || len(metaNode.ReplicaPort) == 0 {
			enabled = false
			return false
		}
		return true
	})
	c.mnMutex.RUnlock()

	if enabled {
		c.dnMutex.RLock()
		c.dataNodes.Range(func(addr, node interface{}) bool {
			dataNode := node.(*DataNode)
			if len(dataNode.HeartbeatPort) == 0 || len(dataNode.ReplicaPort) == 0 {
				enabled = false
				return false
			}
			return true
		})

		c.dnMutex.RUnlock()
	}

	if enabled && !c.cfg.raftPartitionAlreadyUseDifferentPort.Load() {
		// all data nodes and meta nodes are registered with HeartbeatPort and ReplicaPort
		// this feature now is enabled, we update cluster cfg and store
		c.cfg.raftPartitionAlreadyUseDifferentPort.Store(true)
		if err := c.syncPutCluster(); err != nil {
			log.LogErrorf("error syncPutCluster when set raftPartitionAlreadyUseDifferentPort to true, err:%v", err)
			c.cfg.raftPartitionAlreadyUseDifferentPort.Store(false) // set back to false, let syncPutCluster try again in future
			return false
		}
		log.LogInfof("all data nodes and meta nodes are registered with HeartbeatPort and ReplicaPort, " +
			"raft partition use different port feature now is enabled")
	}

	return enabled
}

func (c *Cluster) addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName string, nodesetId uint64) (id uint64, err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()

	var metaNode *MetaNode
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		if nodesetId > 0 && nodesetId != metaNode.ID {
			return metaNode.ID, fmt.Errorf("addr already in nodeset [%v]", nodeAddr)
		}

		if len(heartbeatPort) > 0 && len(replicaPort) > 0 {
			metaNode.Lock()
			defer metaNode.Unlock()
			if len(metaNode.HeartbeatPort) == 0 || len(metaNode.ReplicaPort) == 0 {
				// compatible with old version in which raft heartbeat port and replica port did not persist
				metaNode.HeartbeatPort = heartbeatPort
				metaNode.ReplicaPort = replicaPort
				if err = c.syncUpdateMetaNode(metaNode); err != nil {
					return metaNode.ID, err
				}
			}
		}

		return metaNode.ID, nil
	}
	if c.cfg.raftPartitionCanUseDifferentPort.Load() {
		if len(heartbeatPort) == 0 || len(replicaPort) == 0 {
			err = fmt.Errorf("when master enable raftPartitionCanUseDifferentPort, only allow new metanode with valid heartbeatPort and replicaPort to register. "+
				"metanode(%v, heartbeatPort:%v, replicaPort:%v) may need to upgrade", nodeAddr, heartbeatPort, replicaPort)
			return
		}
	}

	metaNode = newMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, c.Name)
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		log.LogInfof("[addMetaNode] create zone(%v) by metanode(%v)", zoneName, nodeAddr)
		zone = c.t.putZoneIfAbsent(newZone(zoneName, proto.MediaType_Unspecified))
	}

	var ns *nodeSet
	if nodesetId > 0 {
		if ns, err = zone.getNodeSet(nodesetId); err != nil {
			return nodesetId, err
		}
	} else {
		c.nsMutex.Lock()
		ns = zone.getAvailNodeSetForMetaNode()
		if ns == nil {
			if ns, err = zone.createNodeSet(c); err != nil {
				c.nsMutex.Unlock()
				goto errHandler
			}
		}
		c.nsMutex.Unlock()
	}

	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	metaNode.ID = id
	metaNode.NodeSetID = ns.ID
	log.LogInfof("action[addMetaNode] metanode id[%v] zonename [%v] add meta node to nodesetid[%v]", id, zoneName, ns.ID)
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putMetaNode(metaNode)
	// nodeset be avaliable first time can be put into nodesetGrp

	c.addNodeSetGrp(ns, false)
	c.metaNodes.Store(nodeAddr, metaNode)
	log.LogInfof("action[addMetaNode],clusterID[%v] metaNodeAddr:%v,nodeSetId[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) checkSetZoneMediaType(zone *Zone, mediaType uint32) (changed bool, err error) {
	zoneMediaType := zone.GetDataMediaType()
	if zoneMediaType == mediaType {
		log.LogInfof("[checkSetZoneMediaType] zone(%v) mediaType is same with %v",
			zone.name, proto.MediaTypeString(mediaType))
		return false, nil
	}

	if zoneMediaType != proto.MediaType_Unspecified {
		// there has datanode added into the zone
		err = fmt.Errorf("zone(%v) mediaType(%v) already set, can not set as mediaType(%v)",
			zone.name, proto.MediaTypeString(zoneMediaType), proto.MediaTypeString(mediaType))
		log.LogErrorf("[checkSetZoneMediaType] %v", err.Error())
		return false, err
	}

	// there has no datanode added into the zone yet
	zone.SetDataMediaType(mediaType)
	log.LogInfof("[checkSetZoneMediaType] zone(%v) set mediaType(%v)", zone.name, proto.MediaTypeString(mediaType))

	return true, nil
}

func (c *Cluster) checkSetZoneMediaTypePersist(zone *Zone, mediaType uint32) (changed bool, err error) {
	oldMediaType := zone.dataMediaType

	var needPersistZone bool
	needPersistZone, err = c.checkSetZoneMediaType(zone, mediaType)
	if err != nil {
		log.LogErrorf("[checkSetZoneMediaTypePersist] zone(%v) exists, but checkSetZoneMediaType err: %v",
			zone.name, err.Error())
		return needPersistZone, err
	}

	if !needPersistZone {
		return false, nil
	}

	log.LogInfof("[checkSetZoneMediaTypePersist] zone(%v) old mediaType(%v), new mediaType(%v), persist",
		zone.name, proto.MediaTypeString(oldMediaType), proto.MediaTypeString(mediaType))
	persistErr := c.sycnPutZoneInfo(zone)
	if persistErr != nil {
		err = fmt.Errorf("persist zone(%v) failed: %v", zone.name, persistErr.Error())
		log.LogErrorf("[checkSetZoneMediaTypePersist] %v", err.Error())
		return false, err
	}

	return true, nil
}

func (c *Cluster) addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName string, nodesetId uint64, mediaType uint32) (id uint64, err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var dataNode *DataNode
	var zone *Zone

	if zoneName == "" {
		zoneName = DefaultZoneName
	}

	log.LogInfof("[addDataNode] to add: datanode(%v) zone(%v) nodesetId(%v) mediaType(%v)",
		nodeAddr, zoneName, nodesetId, mediaType)

	if !proto.IsValidMediaType(mediaType) {
		if !proto.IsValidMediaType(c.legacyDataMediaType) {
			err = fmt.Errorf("invalid mediaType(%v) in req when adding datanode(%v), and cluster LegacyDataMediaType not set",
				mediaType, nodeAddr)
			return
		}

		mediaType = c.legacyDataMediaType
		log.LogWarnf("[addDataNode] adding datanode(%v), set mediaType as cluster LegacyDataMediaType(%v)",
			nodeAddr, proto.MediaTypeString(c.legacyDataMediaType))
	}

	// datanode existed
	if node, ok := c.dataNodes.Load(nodeAddr); ok {
		log.LogInfof("[addDataNode] addr(%v) exists, will check if its info is consistent with the exist one", nodeAddr)
		dataNode = node.(*DataNode)
		if nodesetId > 0 && nodesetId != dataNode.NodeSetID {
			return dataNode.ID, fmt.Errorf("addr already in nodeset [%v]", nodeAddr)
		}
		if zoneName != dataNode.ZoneName {
			return dataNode.ID, fmt.Errorf("zoneName not equalt old, new %s, old %s", zoneName, dataNode.ZoneName)
		}
		if mediaType != dataNode.MediaType {
			return dataNode.ID, fmt.Errorf("mediaType not equalt old, new %v, old %v", mediaType, dataNode.MediaType)
		}

		if len(raftHeartbeatPort) > 0 && len(raftReplicaPort) > 0 {
			dataNode.Lock()
			defer dataNode.Unlock()
			if len(dataNode.HeartbeatPort) == 0 || len(dataNode.ReplicaPort) == 0 {
				// compatible with old version in which raft heartbeat port and replica port did not persist
				dataNode.HeartbeatPort = raftHeartbeatPort
				dataNode.ReplicaPort = raftReplicaPort
				if err = c.syncUpdateDataNode(dataNode); err != nil {
					return dataNode.ID, err
				}
			}
		}

		return dataNode.ID, nil
	}
	if c.cfg.raftPartitionCanUseDifferentPort.Load() {
		if len(raftHeartbeatPort) == 0 || len(raftReplicaPort) == 0 {
			err = fmt.Errorf("when master enable raftPartitionCanUseDifferentPort, only allow new datanode with valid heartbeatPort and replicaPort to register. "+
				"datanode(%v, heartbeatPort:%v, replicaPort:%v) may need to upgrade", nodeAddr, raftHeartbeatPort, raftReplicaPort)
			return
		}
	}

	needPersistZone := false
	dataNode = newDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, c.Name, mediaType)
	if zone, _ = c.t.getZone(zoneName); zone == nil {
		log.LogInfof("[addDataNode] create zone(%v) by datanode(%v), mediaType(%v)",
			zoneName, nodeAddr, proto.MediaTypeString(mediaType))
		zone = newZone(zoneName, mediaType)
		needPersistZone = true
	}

	if !proto.IsValidMediaType(zone.dataMediaType) {
		zone.SetDataMediaType(mediaType)
		needPersistZone = true
	}

	if mediaType != zone.dataMediaType {
		return dataNode.ID, fmt.Errorf("zone mediaType not equalt old, new %v, old %v", mediaType, zone.dataMediaType)
	}

	if needPersistZone {
		persistErr := c.sycnPutZoneInfo(zone)
		if persistErr != nil {
			err = fmt.Errorf("persist zone(%v) failed when adding datanode(%v)", zoneName, nodeAddr)
			log.LogErrorf("[addDataNode] %v", err.Error())
			return
		}
	}

	c.t.putZoneIfAbsent(zone) // put if the above code creates zone

	var ns *nodeSet
	if nodesetId > 0 {
		if ns, err = zone.getNodeSet(nodesetId); err != nil {
			log.LogErrorf("[addDataNode] %v", err.Error())
			return nodesetId, err
		}
	} else {
		c.nsMutex.Lock()
		ns = zone.getAvailNodeSetForDataNode()
		if ns == nil {
			if ns, err = zone.createNodeSet(c); err != nil {
				c.nsMutex.Unlock()
				goto errHandler
			}
		}
		c.nsMutex.Unlock()
	}
	// allocate dataNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	dataNode.ID = id
	dataNode.NodeSetID = ns.ID
	log.LogInfof("action[addDataNode] datanode id[%v] zonename[%v] MediaType[%v] add node to nodesetid[%v]",
		id, zoneName, dataNode.MediaType, ns.ID)
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putDataNode(dataNode)

	// nodeset be available first time can be put into nodesetGrp
	c.addNodeSetGrp(ns, false)

	c.dataNodes.Store(nodeAddr, dataNode)
	log.LogInfof("action[addDataNode] clusterID[%v] dataNodeAddr:%v, nodeSetId[%v], capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addDataNode] clusterID[%v] dataNodeAddr:%v err:%v", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) checkInactiveDataNodes() (inactiveDataNodes []string, err error) {
	inactiveDataNodes = make([]string, 0)

	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		if !dataNode.isActive {
			inactiveDataNodes = append(inactiveDataNodes, dataNode.Addr)
		}
		return true
	})

	log.LogInfof("clusterID[%v] inactiveDataNodes:%v", c.Name, inactiveDataNodes)
	return
}

// func (c *Cluster) checkLackReplicaAndHostDataPartitions() (lackReplicaDataPartitions []*DataPartition, err error) {
// 	lackReplicaDataPartitions = make([]*DataPartition, 0)
// 	vols := c.copyVols()
// 	var ids []uint64
// 	for _, vol := range vols {
// 		var dps *DataPartitionMap
// 		dps = vol.dataPartitions
// 		for _, dp := range dps.partitions {
// 			if dp.ReplicaNum > uint8(len(dp.Hosts)) && len(dp.Hosts) == len(dp.Replicas) && (dp.IsDecommissionInitial() || dp.IsRollbackFailed()) {
// 				lackReplicaDataPartitions = append(lackReplicaDataPartitions, dp)
// 				ids = append(ids, dp.PartitionID)
// 			}
// 		}
// 	}
// 	log.LogInfof("clusterID[%v] checkLackReplicaAndHostDataPartitions count:[%v] ids[%v]", c.Name,
// 		len(lackReplicaDataPartitions), ids)
// 	return
// }

func (c *Cluster) checkReplicaOfDataPartitions(ignoreDiscardDp bool) (
	lackReplicaDPs []*DataPartition, unavailableReplicaDPs []*DataPartition, repFileCountDifferDps []*DataPartition,
	repUsedSizeDifferDps []*DataPartition, excessReplicaDPs []*DataPartition, noLeaderDPs []*DataPartition, err error,
) {
	noLeaderDPs = make([]*DataPartition, 0)
	lackReplicaDPs = make([]*DataPartition, 0)
	unavailableReplicaDPs = make([]*DataPartition, 0)
	excessReplicaDPs = make([]*DataPartition, 0)

	vols := c.copyVols()
	for _, vol := range vols {
		dps := vol.dataPartitions
		for _, dp := range dps.partitions {
			if ignoreDiscardDp && dp.IsDiscard {
				continue
			}

			if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) ||
				(vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && time.Until(vol.DeleteExecTime) <= 0) {
				continue
			}

			if proto.IsHot(vol.VolType) {
				if dp.lostLeader(c) {
					noLeaderDPs = append(noLeaderDPs, dp)
				}
			}

			if vol.dpReplicaNum > uint8(len(dp.Hosts)) || int(vol.dpReplicaNum) > len(dp.liveReplicas(defaultDataPartitionTimeOutSec)) {
				lackReplicaDPs = append(lackReplicaDPs, dp)
			}

			if (dp.GetDecommissionStatus() == DecommissionInitial || dp.GetDecommissionStatus() == DecommissionFail) &&
				(uint8(len(dp.Hosts)) > dp.ReplicaNum || uint8(len(dp.Replicas)) > dp.ReplicaNum) {
				excessReplicaDPs = append(excessReplicaDPs, dp)
			}

			repSizeDiff := 0.0
			repSizeSentry := 0.0
			repFileCountDiff := uint32(0)
			repFileCountSentry := uint32(0)
			if len(dp.Replicas) != 0 {
				repSizeSentry = float64(dp.Replicas[0].Used)
				repFileCountSentry = dp.Replicas[0].FileCount
			}

			recordReplicaUnavailable := false
			for _, replica := range dp.Replicas {
				if !recordReplicaUnavailable && replica.Status == proto.Unavailable {
					unavailableReplicaDPs = append(unavailableReplicaDPs, dp)
					recordReplicaUnavailable = true
				}
				if dp.IsDoingDecommission() {
					continue
				}

				if dp.IsDoingDecommission() {
					continue
				}

				tempSizeDiff := math.Abs(float64(replica.Used) - repSizeSentry)
				if tempSizeDiff > repSizeDiff {
					repSizeDiff = tempSizeDiff
				}

				tempFileCountDiff := replica.FileCount - repFileCountSentry
				if tempFileCountDiff > repFileCountDiff {
					repFileCountDiff = tempFileCountDiff
				}
			}

			if repSizeDiff > float64(c.cfg.diffReplicaSpaceUsage) {
				repUsedSizeDifferDps = append(repUsedSizeDifferDps, dp)
			}

			if repFileCountDiff > c.cfg.diffReplicaFileCount {
				repFileCountDifferDps = append(repFileCountDifferDps, dp)
			}
		}
	}

	log.LogInfof("clusterID[%v] lackReplicaDp count:[%v], unavailableReplicaDp count:[%v], "+
		"repFileCountDifferDps count[%v], repUsedSizeDifferDps count[%v], "+
		"excessReplicaDPs count[%v], noLeaderDPs count[%v] ",
		c.Name, len(lackReplicaDPs), len(unavailableReplicaDPs),
		len(repFileCountDifferDps), len(repUsedSizeDifferDps),
		len(excessReplicaDPs), len(noLeaderDPs))
	return
}

func (c *Cluster) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	vols := c.copyVols()

	for _, vol := range vols {
		if dp, err = vol.getDataPartitionByID(partitionID); err == nil {
			return
		}
	}
	err = dataPartitionNotFound(partitionID)
	return
}

func (c *Cluster) getMetaPartitionByID(id uint64) (mp *MetaPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if mp, err = vol.metaPartition(id); err == nil {
			return
		}
	}
	err = metaPartitionNotFound(id)
	return
}

func (c *Cluster) checkVol(vol *Vol) (err error) {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	if v, ok := c.vols[vol.Name]; ok && v.ID > vol.ID {
		err = fmt.Errorf("volume [%v] already exist [%v], cann't set new vol [%v]", vol.Name, v, vol)
		log.LogErrorf("action[checkVol] %v", err)
		return
	}
	return
}

func (c *Cluster) putVol(vol *Vol) (err error) {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	if v, ok := c.vols[vol.Name]; ok {
		if v.ID > vol.ID {
			err = fmt.Errorf("volume [%v] already exist [%v], cann't set new vol [%v]", vol.Name, v, vol)
			log.LogErrorf("action[putVol] %v", err)
			return
		}
		log.LogWarnf("volume [%v] already exist [%v] not deleted well. new vol [%v]", vol.Name, v, vol)
	}
	c.vols[vol.Name] = vol
	return
}

func (c *Cluster) SetVerStrategy(volName string, strategy proto.VolumeVerStrategy, isForce bool) (err error) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()

	vol, ok := c.vols[volName]
	if !ok {
		err = proto.ErrVolNotExists
		return
	}

	if !proto.IsHot(vol.VolType) {
		err = fmt.Errorf("vol need be hot one")
		return
	}
	return vol.VersionMgr.SetVerStrategy(strategy, isForce)
}

func (c *Cluster) getVolVer(volName string) (info *proto.VolumeVerInfo, err error) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()

	var verSeqPrepare uint64

	vol, ok := c.vols[volName]
	if !ok {
		err = proto.ErrVolNotExists
		return
	}

	if !proto.IsHot(vol.VolType) {
		err = fmt.Errorf("vol need be hot one")
		return
	}

	if vol.VersionMgr.enabled {
		verSeqPrepare = vol.VersionMgr.prepareCommit.prepareInfo.Ver
	}
	var pStatus uint8
	if vol.VersionMgr.prepareCommit.prepareInfo != nil {
		pStatus = vol.VersionMgr.prepareCommit.prepareInfo.Status
	}
	info = &proto.VolumeVerInfo{
		Name:             volName,
		VerSeq:           vol.VersionMgr.verSeq,
		VerSeqPrepare:    verSeqPrepare,
		VerPrepareStatus: pStatus,
		Enabled:          vol.VersionMgr.enabled,
	}
	return
}

func (c *Cluster) getVol(volName string) (vol *Vol, err error) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	vol, ok := c.vols[volName]
	if !ok {
		err = proto.ErrVolNotExists
	}

	return
}

func (c *Cluster) volDelete(volName string) bool {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	vol, ok := c.vols[volName]
	if !ok {
		return true
	}
	if vol.Status == proto.VolStatusMarkDelete {
		return true
	}
	return false
}

func (c *Cluster) deleteVol(name string) {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	delete(c.vols, name)
}

func (c *Cluster) markDeleteVol(name, authKey string, force bool, isNotCancel bool) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)

	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[markDeleteVol] err[%v]", err)
		return proto.ErrVolNotExists
	}

	if !isNotCancel {
		serverAuthKey = vol.Owner
		if !matchKey(serverAuthKey, authKey) {
			return proto.ErrVolAuthKeyNotMatch
		}

		vol.Status = proto.VolStatusNormal
		if err = c.syncUpdateVol(vol); err != nil {
			vol.Status = proto.VolStatusMarkDelete
			return proto.ErrPersistenceByRaft
		}
		return
	}

	if !c.cfg.volForceDeletion {
		volDentryCount := uint64(0)
		mpsCopy := vol.cloneMetaPartitionMap()
		for _, mp := range mpsCopy {
			// to avoid latency, fetch latest mp dentry count from metanode
			c.doLoadMetaPartition(mp)
			mpDentryCount := uint64(0)
			for _, response := range mp.LoadResponse {
				if response.DentryCount > mpDentryCount {
					mpDentryCount = response.DentryCount
				}
			}
			volDentryCount += mpDentryCount
		}

		if volDentryCount > c.cfg.volDeletionDentryThreshold {
			return fmt.Errorf("vol %s is not empty ! it's dentry count %d > dentry count deletion threshold %d, deletion not permitted ! ",
				vol.Name, volDentryCount, c.cfg.volDeletionDentryThreshold)
		}
	}

	if proto.IsCold(vol.VolType) && vol.totalUsedSpace() > 0 && !force {
		return fmt.Errorf("ec-vol can't be deleted if ec used size not equal 0, now(%d)", vol.totalUsedSpace())
	}

	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}

	vol.Status = proto.VolStatusMarkDelete
	if err = c.syncUpdateVol(vol); err != nil {
		vol.Status = proto.VolStatusNormal
		return proto.ErrPersistenceByRaft
	}
	return
}

func (c *Cluster) batchCreatePreLoadDataPartition(vol *Vol, preload *DataPartitionPreLoad) (err error, dps []*DataPartition) {
	if proto.IsHot(vol.VolType) {
		return fmt.Errorf("vol type is not warm"), nil
	}

	if vol.cacheDpStorageClass == proto.StorageClass_Unspecified {
		err = fmt.Errorf(" has no resource to create preload data partition")
		log.LogErrorf("[batchCreatePreLoadDataPartition] vol(%v) err: %v", vol.Name, err.Error())
		return err, nil
	}

	total := overSoldCap(uint64(preload.preloadCacheCapacity))
	reqCreateCount := (total-1)/(util.DefaultDataPartitionSize/util.GB) + 1

	for i := 0; i < int(reqCreateCount); i++ {
		log.LogInfof("create preload data partition (%v) total (%v)", i, reqCreateCount)
		var dp *DataPartition
		if dp, err = c.createDataPartition(vol.Name, preload, vol.cacheDpStorageClass); err != nil {
			log.LogErrorf("create preload data partition fail: volume(%v) err(%v)", vol.Name, err)
			return err, nil
		}

		dps = append(dps, dp)
	}

	return
}

func (c *Cluster) batchCreateDataPartition(vol *Vol, reqCount int, init bool, mediaType uint32) (err error) {
	log.LogInfof("[batchCreateDataPartition] vol(%v) mediaType(%v) reqCount(%v) init(%v)",
		vol.Name, proto.MediaTypeString(mediaType), reqCount, init)

	if !init {
		if _, err = vol.needCreateDataPartition(); err != nil {
			log.LogWarnf("action[batchCreateDataPartition] create data partition failed, err[%v]", err)
			return
		}
	}

	var createdCnt int
	for i := 0; i < reqCount; i++ {
		if c.DisableAutoAllocate && !init {
			log.LogWarn("disable auto allocate dataPartition")
			return fmt.Errorf("cluster is disable auto allocate dataPartition")
		}

		if vol.Forbidden {
			log.LogWarn("disable auto allocate dataPartition by forbidden volume")
			return fmt.Errorf("volume is forbidden")
		}

		if _, err = c.createDataPartition(vol.Name, nil, mediaType); err != nil {
			log.LogErrorf("action[batchCreateDataPartition] after create [%v] data partition, occurred error,err[%v]", i, err)
			break
		}
		createdCnt++
	}

	log.LogInfof("action[batchCreateDataPartition] vol(%v) mediaType(%v) created data partition count: %v",
		vol.Name, proto.MediaTypeString(mediaType), createdCnt)
	vol.dataPartitions.IncReadWriteDataPartitionCntByMediaType(createdCnt, mediaType)
	return
}

func (c *Cluster) isFaultDomain(vol *Vol) bool {
	var specifyZoneNeedDomain bool
	if c.FaultDomain && !vol.crossZone && !c.needFaultDomain {
		if value, ok := c.t.zoneMap.Load(vol.zoneName); ok {
			if value.(*Zone).status == unavailableZone {
				specifyZoneNeedDomain = true
			}
		}
	}
	log.LogInfof("action[isFaultDomain] vol [%v] zoname [%v] FaultDomain[%v] need fault domain[%v] vol crosszone[%v] default[%v] specifyZoneNeedDomain[%v] domainOn[%v]",
		vol.Name, vol.zoneName, c.FaultDomain, c.needFaultDomain, vol.crossZone, vol.defaultPriority, specifyZoneNeedDomain, vol.domainOn)
	domainOn := c.FaultDomain &&
		(vol.domainOn ||
			(!vol.crossZone && c.needFaultDomain) || specifyZoneNeedDomain ||
			(vol.crossZone && (!vol.defaultPriority ||
				(vol.defaultPriority && (c.needFaultDomain || len(c.t.domainExcludeZones) <= 1)))))
	if !vol.domainOn && domainOn {
		vol.domainOn = domainOn
		// todo:(leonchang). updateView used to update domainOn status in viewCache, use channel may be better or else lock may happend
		// vol.updateViewCache(c)
		c.syncUpdateVol(vol)
		log.LogInfof("action[isFaultDomain] vol [%v] set domainOn", vol.Name)
	}
	return vol.domainOn
}

// Synchronously create a data partition.
// 1. Choose one of the available data nodes.
// 2. Assign it a partition ID.
// 3. Communicate with the data node to synchronously create a data partition.
// - If succeeded, replicate the data through raft and persist it to RocksDB.
// - Otherwise, throw errors
func (c *Cluster) createDataPartition(volName string, preload *DataPartitionPreLoad, mediaType uint32) (dp *DataPartition, err error) {
	var (
		vol          *Vol
		partitionID  uint64
		targetHosts  []string
		targetPeers  []proto.Peer
		wg           sync.WaitGroup
		isPreload    bool
		partitionTTL int64
		ok           bool
	)

	log.LogInfof("action[createDataPartition] vol(%v) preload(%v) mediType(%v)",
		volName, preload, proto.MediaTypeString(mediaType))

	c.volMutex.RLock()
	if vol, ok = c.vols[volName]; !ok {
		err = fmt.Errorf("vol %v not exist", volName)
		log.LogWarnf("createDataPartition volName %v not found", volName)
		c.volMutex.RUnlock()
		return
	}
	c.volMutex.RUnlock()

	dpReplicaNum := vol.dpReplicaNum
	zoneName := vol.zoneName

	if preload != nil {
		dpReplicaNum = uint8(preload.preloadReplicaNum)
		zoneName = preload.preloadZoneName
		isPreload = true
		partitionTTL = int64(preload.PreloadCacheTTL)*util.OneDaySec() + time.Now().Unix()
	}

	if vol, err = c.getVol(volName); err != nil {
		return
	}

	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()

	errChannel := make(chan error, dpReplicaNum)

	if c.isFaultDomain(vol) {
		if targetHosts, targetPeers, err = c.getHostFromDomainZone(vol.domainId, TypeDataPartition, dpReplicaNum, mediaType); err != nil {
			goto errHandler
		}
	} else {
		zoneNum := c.decideZoneNum(vol, mediaType) // zoneNum scope [1,3]
		if targetHosts, targetPeers, err = c.getHostFromNormalZone(TypeDataPartition, nil, nil, nil,
			int(dpReplicaNum), zoneNum, zoneName, mediaType); err != nil {
			goto errHandler
		}
	}
	if err = c.checkMultipleReplicasOnSameMachine(targetHosts); err != nil {
		goto errHandler
	}

	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errHandler
	}

	dp = newDataPartition(partitionID, dpReplicaNum, volName, vol.ID, proto.GetDpType(vol.VolType, isPreload), partitionTTL, mediaType)
	dp.Hosts = targetHosts
	dp.Peers = targetPeers

	log.LogInfof("action[createDataPartition] partitionID [%v] get host [%v]", partitionID, targetHosts)

	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()

			var diskPath string

			if diskPath, err = c.syncCreateDataPartitionToDataNode(host, vol.dataPartitionSize,
				dp, dp.Peers, dp.Hosts, proto.NormalCreateDataPartition, dp.PartitionType, false, false); err != nil {
				log.LogErrorf("[createDataPartition] %v", err)
				errChannel <- err
				return
			}

			dp.Lock()
			defer dp.Unlock()
			if err = dp.afterCreation(host, diskPath, c); err != nil {
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
				_, err := dp.getReplica(host)
				if err != nil {
					return
				}
				task := dp.createTaskToDeleteDataPartition(host, false)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addDataNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		goto errHandler
	default:
		dp.total = vol.dataPartitionSize
		dp.setReadWrite()
	}

	if err = c.syncAddDataPartition(dp); err != nil {
		goto errHandler
	}

	vol.dataPartitions.put(dp)
	log.LogInfof("action[createDataPartition] success,volName[%v],partitionId[%v], count[%d]", volName, partitionID, len(vol.dataPartitions.partitions))
	return

errHandler:
	err = fmt.Errorf("action[createDataPartition],clusterID[%v] vol[%v] mediaType(%v) Err:%v ",
		c.Name, volName, proto.MediaTypeString(mediaType), err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, size uint64, dp *DataPartition,
	peers []proto.Peer, hosts []string, createType int, partitionType int, needRollBack, ignoreDecommissionDisk bool,
) (diskPath string, err error) {
	log.LogInfof("action[syncCreateDataPartitionToDataNode] dp [%v] createType[%v], partitionType[%v] ignoreDecommissionDisk[%v]",
		dp.PartitionID, createType, partitionType, ignoreDecommissionDisk)
	dataNode, err := c.dataNode(host)
	if err != nil {
		return
	}
	var task *proto.AdminTask
	if ignoreDecommissionDisk {
		task = dp.createTaskToCreateDataPartition(host, size, peers, hosts, createType, partitionType, []string{})
	} else {
		task = dp.createTaskToCreateDataPartition(host, size, peers, hosts, createType, partitionType, dataNode.getDecommissionedDisks())
	}
	if task == nil {
		err = errors.NewErrorf("action[syncCreateDataPartitionToDataNode] dp[%v] meditType(%v) create task for creating data partition failed",
			dp.decommissionInfo(), proto.MediaTypeString(dp.MediaType))
		return
	}

	var resp *proto.Packet
	if resp, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		// data node is not alive or other process error
		if needRollBack {
			dp.DecommissionNeedRollback = true
			c.syncUpdateDataPartition(dp)
		}
		return
	}
	return string(resp.Data), nil
}

func (c *Cluster) syncCreateMetaPartitionToMetaNode(host string, mp *MetaPartition) (err error) {
	hosts := make([]string, 0)
	hosts = append(hosts, host)
	tasks := mp.buildNewMetaPartitionTasks(hosts, mp.Peers, mp.volName)
	metaNode, err := c.metaNode(host)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(tasks[0]); err != nil {
		return
	}
	return
}

func (c *Cluster) getZoneListFromVolZoneName(vol *Vol, mediaType uint32) (zoneListOfMediaType []*Zone) {
	zoneListOfMediaType = make([]*Zone, 0)

	specificZoneList := strings.Split(vol.zoneName, ",")
	for _, zoneName := range specificZoneList {
		zone, err := c.t.getZone(zoneName)
		if err != nil {
			continue
		}

		if mediaType == proto.MediaType_Unspecified {
			log.LogDebugf("[getZoneListFromVolZoneName] vol(%v) pick up zone(%v), mediaType(%v)",
				vol.Name, zoneName, proto.MediaTypeString(mediaType))
			zoneListOfMediaType = append(zoneListOfMediaType, zone)
			continue
		}

		if zone.dataMediaType != mediaType {
			log.LogDebugf("[getZoneListFromVolZoneName] vol(%v) skip zone(%v), zone mediaType(%v), require mediaType(%v)",
				vol.Name, zoneName, proto.MediaTypeString(zone.dataMediaType), proto.MediaTypeString(mediaType))
			continue
		}

		zoneListOfMediaType = append(zoneListOfMediaType, zone)
		log.LogDebugf("[getZoneListFromVolZoneName] vol(%v) pick up zone(%v) of mediaType(%v)",
			vol.Name, zoneName, proto.MediaTypeString(mediaType))
	}

	return
}

// decideZoneNum
// if vol is not cross zone, return 1
// if vol enable cross zone and the zone number of cluster less than defaultReplicaNum return 2
// otherwise, return defaultReplicaNum
func (c *Cluster) decideZoneNum(vol *Vol, mediaType uint32) (zoneNum int) {
	if !vol.crossZone {
		zoneNum = 1
		log.LogInfof("[decideZoneNum] to create vol(%v), zoneName(%v) mediaType(%v), crossZone is not set, decide zoneNum: %v",
			vol.Name, vol.zoneName, proto.MediaTypeString(mediaType), zoneNum)
		return
	}

	specificZoneListOfMediaType := c.getZoneListFromVolZoneName(vol, mediaType)
	log.LogInfof("[decideZoneNum] to create vol(%v), zoneName(%v) crossZone(%v), zoneCount of mediaType(%v): %v",
		vol.Name, vol.zoneName, vol.crossZone, proto.MediaTypeString(mediaType), len(specificZoneListOfMediaType))

	var zoneLen int
	if c.FaultDomain {
		zoneLen = len(c.t.domainExcludeZones)
	} else {
		if len(specificZoneListOfMediaType) >= 1 {
			zoneLen = len(specificZoneListOfMediaType)
		} else {
			zoneLen = 2
		}
	}

	if zoneLen < defaultReplicaNum {
		zoneNum = zoneLen
		if zoneNum == 1 {
			log.LogWarnf("[decideZoneNum] to create vol(%v), zoneName(%v) mediaType(%v), crossZone is true, but only one zone qualified",
				vol.Name, vol.zoneName, proto.MediaTypeString(mediaType))
		}
	}
	if zoneLen > defaultReplicaNum {
		zoneNum = defaultReplicaNum
	}

	log.LogInfof("[decideZoneNum] to create vol(%v), zoneName(%v) mediaType(%v), crossZone(%v), decide zoneNum: %v",
		vol.Name, vol.zoneName, proto.MediaTypeString(mediaType), vol.crossZone, zoneNum)
	return zoneNum
}

func (c *Cluster) chooseZone2Plus1(rsMgr *rsManager, zones []*Zone, excludeNodeSets []uint64, excludeHosts []string,
	nodeType uint32, replicaNum int) (hosts []string, peers []proto.Peer, err error,
) {
	if replicaNum < 2 || replicaNum > defaultReplicaNum {
		return nil, nil, fmt.Errorf("action[chooseZone2Plus1] replicaNum [%v]", replicaNum)
	}
	zoneList := make([]*Zone, 2)
	for i := range []int{1, 2} {
		if rsMgr.zoneIndexForNode >= len(zones) {
			rsMgr.zoneIndexForNode = 0
		}
		zoneList[i] = zones[rsMgr.zoneIndexForNode]
		rsMgr.zoneIndexForNode++
	}
	sort.Slice(zoneList, func(i, j int) bool {
		return zoneList[i].getSpaceLeft(nodeType) < zoneList[j].getSpaceLeft(nodeType)
	})

	log.LogInfof("action[chooseZone2Plus1] type [%v] after check,zone0 [%v] left [%v] zone1 [%v] left [%v]",
		nodeType, zoneList[0].name, zoneList[0].getSpaceLeft(nodeType), zoneList[1].name, zoneList[1].getSpaceLeft(nodeType))

	num := 1
	for _, zone := range zoneList {
		selectedHosts, selectedPeers, e := zone.getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, num)
		if e != nil {
			log.LogErrorf("action[chooseZone2Plus1] getAvailNodeHosts error: [%v]", e)
			return nil, nil, e
		}

		hosts = append(hosts, selectedHosts...)
		peers = append(peers, selectedPeers...)
		log.LogInfof("action[chooseZone2Plus1] zone [%v] left [%v] get hosts[%v]",
			zone.name, zone.getSpaceLeft(nodeType), selectedHosts)

		num = replicaNum - num
	}
	log.LogInfof("action[chooseZone2Plus1] finally get hosts[%v]", hosts)

	return hosts, peers, nil
}

func (c *Cluster) chooseZoneNormal(zones []*Zone, excludeNodeSets []uint64, excludeHosts []string,
	nodeType uint32, replicaNum int,
) (hosts []string, peers []proto.Peer, err error) {
	log.LogInfof("action[chooseZoneNormal] zones[%s] nodeType[%d] replicaNum[%d]", printZonesName(zones), nodeType, replicaNum)

	c.zoneIdxMux.Lock()
	defer c.zoneIdxMux.Unlock()

	for i := 0; i < replicaNum; i++ {
		// try all zone from zones list to get the available hosts
		for j := 0; j < len(zones); j++ {
			zone := zones[c.lastZoneIdxForNode]
			c.lastZoneIdxForNode = (c.lastZoneIdxForNode + 1) % len(zones)
			selectedHosts, selectedPeers, err := zone.getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, 1)
			if err != nil {
				// no zone available
				if j == len(zones)-1 {
					log.LogErrorf("action[chooseZoneNormal] error [%v]", err)
					return nil, nil, err
				}
				continue
			}

			hosts = append(hosts, selectedHosts...)
			peers = append(peers, selectedPeers...)
			// if get the available hosts, choose zone for next replica
			break
		}
	}

	return
}

func (c *Cluster) getSpecificZoneList(specifiedZone string) (zones []*Zone, err error) {
	// when creating vol,user specified a zone,we reset zoneNum to 1,to be created partition with specified zone,
	// if specified zone is not writable,we choose a zone randomly

	if err = c.checkNormalZoneName(specifiedZone); err != nil {
		Warn(c.Name, fmt.Sprintf("cluster[%v],specified zone[%v]is found", c.Name, specifiedZone))
		return
	}
	zoneList := strings.Split(specifiedZone, ",")
	for i := 0; i < len(zoneList); i++ {
		var zone *Zone
		if zone, err = c.t.getZone(zoneList[i]); err != nil {
			Warn(c.Name, fmt.Sprintf("cluster[%v],specified zone[%v]is found", c.Name, specifiedZone))
			return
		}
		zones = append(zones, zone)
	}
	return
}

func (c *Cluster) getHostFromNormalZone(nodeType uint32, excludeZones []string, excludeNodeSets []uint64,
	excludeHosts []string, replicaNum int, zoneNumNeed int,
	specifiedZoneName string, dataMediaType uint32) (hosts []string, peers []proto.Peer, err error,
) {
	log.LogInfof("[getHostFromNormalZone] dataMediaType(%v) nodeType(%v) replicaNum(%v) zoneNumNeed(%v) specifiedZoneName(%v)",
		proto.MediaTypeString(nodeType), nodeType, replicaNum, zoneNumNeed, specifiedZoneName)

	var zonesQualified []*Zone
	if replicaNum <= zoneNumNeed {
		zoneNumNeed = replicaNum
	}

	var specifiedZones []*Zone
	var rsMgr *rsManager
	if specifiedZoneName != "" {
		if specifiedZones, err = c.getSpecificZoneList(specifiedZoneName); err != nil {
			return
		}
	}
	if nodeType == TypeDataPartition {
		rsMgr = &c.t.dataTopology
	} else {
		rsMgr = &c.t.metaTopology
	}

	// get all zones that qualified
	if zonesQualified, err = c.t.allocZonesForNode(rsMgr, zoneNumNeed, replicaNum, excludeZones, specifiedZones, dataMediaType); err != nil {
		return
	}

	if len(zonesQualified) == 1 {
		log.LogInfof("action[getHostFromNormalZone] zones [%v]", zonesQualified[0].name)
		if hosts, peers, err = zonesQualified[0].getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, replicaNum); err != nil {
			log.LogErrorf("action[getHostFromNormalZone] err[%v]", err)
			return
		}
		goto result
	}

	if excludeHosts == nil {
		excludeHosts = make([]string, 0)
	}
	// The upper process tries to go and get the dedicated zones, and the latter tries to choose the right zones as possible.
	if c.cfg.DefaultNormalZoneCnt == defaultNormalCrossZoneCnt && len(zonesQualified) >= defaultNormalCrossZoneCnt || replicaNum == 1 {
		if hosts, peers, err = c.chooseZoneNormal(zonesQualified, excludeNodeSets, excludeHosts, nodeType, replicaNum); err != nil {
			return
		}
	} else {
		if hosts, peers, err = c.chooseZone2Plus1(rsMgr, zonesQualified, excludeNodeSets, excludeHosts, nodeType, replicaNum); err != nil {
			return
		}
	}

result:
	log.LogInfof("action[getHostFromNormalZone] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNumNeed, len(zonesQualified), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[getHostFromNormalZone] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNumNeed, len(zonesQualified), hosts)
		return nil, nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneNum[%v],selectedZones[%v]",
			len(hosts), replicaNum, zoneNumNeed, len(zonesQualified))
	}

	return
}

func (c *Cluster) dataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		if !c.IsLeader() {
			err = errors.New("meta data for data nodes is cleared due to leader change!")
		} else {
			err = errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
		}
		return
	}

	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) metaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		if !c.IsLeader() {
			err = errors.New("meta data for meta nodes is cleared due to leader change!")
		} else {
			err = errors.Trace(metaNodeNotFound(addr), "%v not found", addr)
		}
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) lcNode(addr string) (lcNode *LcNode, err error) {
	value, ok := c.lcNodes.Load(addr)
	if !ok {
		err = errors.Trace(lcNodeNotFound(addr), "%v not found", addr)
		return
	}
	lcNode = value.(*LcNode)
	return
}

func (c *Cluster) getAllDataPartitionByDataNode(addr string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.IsDiscard {
				continue
			}
			for _, host := range dp.Hosts {
				if host == addr {
					partitions = append(partitions, dp)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionByMetaNode(addr string) (partitions []*MetaPartition) {
	partitions = make([]*MetaPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}

	return
}

func (c *Cluster) getAllDataPartitionIDByDatanode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			for _, host := range dp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, dp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionIDByMetaNode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, mp.PartitionID)
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}

	return
}

func (c *Cluster) getAllMetaPartitionsByMetaNode(addr string) (partitions []*MetaPartition) {
	partitions = make([]*MetaPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			vol.mpsLock.RLock()
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
			vol.mpsLock.RUnlock()
		}
	}
	return
}

func (c *Cluster) decommissionDataNodePause(dataNode *DataNode) (err error, failed []uint64) {
	if !dataNode.CanBePaused() {
		err = fmt.Errorf("action[decommissionDataNodePause] dataNode[%v] status[%v] donot support cancel",
			dataNode.Addr, dataNode.GetDecommissionStatus())
		return
	}
	dataNode.SetDecommissionStatus(DecommissionPause)
	// may cause progress confused for new allocated dp
	dataNode.ToBeOffline = false
	dataNode.DecommissionCompleteTime = time.Now().Unix()
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		log.LogErrorf("action[decommissionDataNodePause] dataNode[%v] sync update failed[ %v]",
			dataNode.Addr, err.Error())
		return
	}
	for _, disk := range dataNode.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
		if value, ok := c.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			_, dps := c.decommissionDiskPause(dd)
			log.LogInfof("action[decommissionDataNodePause] dataNode [%s] pause disk %v with failed dp[%v]",
				dataNode.Addr, dd.GenerateKey(), dps)
			failed = append(failed, dps...)
		}
	}
	log.LogDebugf("action[decommissionDataNodePause] dataNode[%v] cancel decommission, offline %v with failed dp[%v]",
		dataNode.Addr, dataNode.ToBeOffline, failed)
	return
}

func (c *Cluster) decommissionDiskPause(disk *DecommissionDisk) (err error, failed []uint64) {
	if !disk.CanBePaused() {
		err = fmt.Errorf("action[decommissionDiskPause] dataNode[%v] disk[%s] status[%v] donot support cancel",
			disk.SrcAddr, disk.SrcAddr, disk.GetDecommissionStatus())
		return
	}
	disk.SetDecommissionStatus(DecommissionPause)
	// disk.DecommissionDpTotal = 0
	if err = c.syncUpdateDecommissionDisk(disk); err != nil {
		log.LogErrorf("action[decommissionDiskPause] dataNode[%v] disk[%s] sync update failed[ %v]",
			disk.SrcAddr, disk.SrcAddr, err.Error())
		return
	}
	partitions := disk.GetLatestDecommissionDP(c)
	dpIds := make([]uint64, 0)
	for _, dp := range partitions {
		if !dp.PauseDecommission(c) {
			failed = append(failed, dp.PartitionID)
		}
		dpIds = append(dpIds, dp.PartitionID)
	}
	log.LogDebugf("action[decommissionDiskPause] dataNode[%v] disk[%s] cancel decommission dps[%v] with failed [%v]",
		disk.SrcAddr, disk.SrcAddr, dpIds, failed)
	return
}

func (c *Cluster) migrateDataNode(srcAddr, targetAddr string, raftForce bool, limit int) (err error) {
	msg := fmt.Sprintf("action[migrateDataNode], src(%s) migrate to target(%s) raftForcs(%v) limit(%v)",
		srcAddr, targetAddr, raftForce, limit)
	log.LogWarn(msg)

	srcNode, err := c.dataNode(srcAddr)
	if err != nil {
		return
	}

	if targetAddr != "" {
		var targetNode *DataNode
		targetNode, err = c.dataNode(targetAddr)
		if err != nil {
			return
		}

		if targetNode.MediaType != srcNode.MediaType {
			err = fmt.Errorf("targetNode mediaType(%v) not match srcNode mediaType(%v)",
				proto.MediaTypeString(targetNode.MediaType), proto.MediaTypeString(srcNode.MediaType))
			log.LogErrorf("[migrateDataNode] %v", err.Error())
			return
		}
	}

	status := srcNode.GetDecommissionStatus()

	if status == markDecommission || status == DecommissionRunning {
		err = fmt.Errorf("migrate src(%v) is still on working, please wait,check or cancel if abnormal:%v",
			srcAddr, srcNode.GetDecommissionStatus())
		log.LogWarnf("action[migrateDataNode] %v", err)
		return
	}
	srcNode.markDecommission(targetAddr, raftForce, limit)
	c.syncUpdateDataNode(srcNode)
	log.LogInfof("action[migrateDataNode] %v return now", srcAddr)
	return
}

func (c *Cluster) decommissionDataNode(dataNode *DataNode, force bool) (err error) {
	return c.migrateDataNode(dataNode.Addr, "", false, 0)
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	c.t.deleteDataNode(dataNode)
	go dataNode.clean()
}

func (c *Cluster) delDecommissionDiskFromCache(dd *DecommissionDisk) {
	c.DecommissionDisks.Delete(dd.GenerateKey())
}

func (c *Cluster) decommissionSingleDp(dp *DataPartition, newAddr, offlineAddr string) (err error) {
	var (
		dataNode       *DataNode
		decommContinue bool
		newReplica     *DataReplica
	)

	ticker := time.NewTicker(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
	defer func() {
		ticker.Stop()
	}()
	// 1. add new replica first
	if dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionEnter {
		if err = c.addDataReplica(dp, newAddr, false); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v addDataReplica %v fail err %v", dp.PartitionID, newAddr, err)
			goto ERR
		}
		// if addDataReplica is success, can add to BadDataPartitionIds
		dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionWaitAddRes)
		dp.SetDecommissionStatus(DecommissionRunning)
		dp.isRecover = true
		dp.Status = proto.ReadOnly
		dp.RecoverStartTime = time.Now()
		c.syncUpdateDataPartition(dp)
		c.putBadDataPartitionIDsByDiskPath(dp.DecommissionSrcDiskPath, dp.DecommissionSrcAddr, dp.PartitionID)
		log.LogWarnf("action[decommissionSingleDp] dp %v start wait add replica %v", dp.PartitionID, newAddr)
	}
	// 2. wait for repair
	if dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes {
		const dataNodeRebootMaxTimes = 24 // 2 minutes for dataNode to reboot, total 10 miniutes
		dataNodeRebootRetryTimes := 0
		for {
			select {
			case decommContinue = <-dp.SpecialReplicaDecommissionStop: //
				if !decommContinue {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica is stopped", dp.PartitionID)
					dp.SetDecommissionStatus(DecommissionPause)
					log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
					goto ERR
				}
			case <-ticker.C:
				if !c.partition.IsRaftLeader() {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica result addr %v master leader changed", dp.PartitionID, newAddr)
					log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
					goto ERR
				}
			}
			// check new replica status
			liveReplicas := dp.getLiveReplicasFromHosts(c.getDataPartitionTimeoutSec())
			newReplica, err = dp.getReplica(newAddr)
			if err != nil {
				err = fmt.Errorf("action[decommissionSingleDp] dp %v replica %v not found",
					dp.PartitionID, newAddr)
				log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
				dp.DecommissionNeedRollback = false
				goto ERR
			}
			log.LogInfof("action[decommissionSingleDp] dp %v liveReplicas num[%v]",
				dp.PartitionID, len(liveReplicas))
			// for operation of auto add replica, liveReplicas should equal to dp.ReplicaNum
			if (len(liveReplicas) >= int(dp.ReplicaNum+1) && dp.DecommissionType != AutoAddReplica) ||
				(len(liveReplicas) == int(dp.ReplicaNum) && dp.DecommissionType == AutoAddReplica) {
				log.LogInfof("action[decommissionSingleDp] dp %v replica[%v] status %v",
					dp.PartitionID, newReplica.Addr, newReplica.Status)
				dataNodeRebootRetryTimes = 0 // reset dataNodeRebootRetryTimes
				if len(liveReplicas) > int(dp.ReplicaNum+1) {
					log.LogInfof("action[decommissionSingleDp] dp %v replica[%v] new replica status[%v] has excess replicas",
						dp.PartitionID, newReplica.Addr, newReplica.Status)
				}
				if newReplica.isRepairing() { // wait for repair
					masterNode, _ := dp.getReplica(dp.Hosts[0])
					duration := time.Unix(masterNode.ReportTime, 0).Sub(time.Unix(newReplica.ReportTime, 0))
					if math.Abs(duration.Minutes()) > 10 {
						err = fmt.Errorf("action[decommissionSingleDp] dp %v host[0] %v is down",
							dp.PartitionID, masterNode.Addr)
						dp.DecommissionNeedRollback = false
						newReplica.Status = proto.Unavailable // remove from data partition check
						log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
						goto ERR
					}
					if time.Since(dp.RecoverStartTime) > c.GetDecommissionDataPartitionRecoverTimeOut() {
						err = fmt.Errorf("action[decommissionSingleDp] dp %v new replica %v repair time out:%v",
							dp.PartitionID, newAddr, time.Since(dp.RecoverStartTime))
						dp.DecommissionNeedRollback = true
						newReplica.Status = proto.Unavailable // remove from data partition check
						log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
						goto ERR
					}
					continue
				} else {
					dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionWaitAddResFin)
					c.syncUpdateDataPartition(dp)
					log.LogInfof("action[decommissionSingleDp] dp %v add replica success", dp.PartitionID)
					break
				}
			} else {
				// newReplica repair failed or encounter bad disk ,need rollback
				if newReplica.isUnavailable() {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v new replica %v is Unavailable",
						dp.PartitionID, newAddr)
					dp.DecommissionNeedRollback = true
					log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
					goto ERR
				}
				if dataNodeRebootRetryTimes >= dataNodeRebootMaxTimes {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v old replica unavailable",
						dp.PartitionID)
					log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
					goto ERR
				}
				dataNodeRebootRetryTimes++
			}
		}
	}
	// 2. wait for leader
	if dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddResFin {
		if !c.partition.IsRaftLeader() {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica result addr %v master leader changed", dp.PartitionID, newAddr)
			goto ERR
		}
		if dataNode, err = c.dataNode(newAddr); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v get offlineAddr %v err %v", dp.PartitionID, newAddr, err)
			goto ERR
		}
		times := 0
		for {
			// if leader is selected
			if dp.getLeaderAddr() != "" {
				break
			}
			log.LogInfof("action[decommissionSingleDp] dp %v try tryToChangeLeader addr %v", dp.PartitionID, newAddr)
			if err = dp.tryToChangeLeader(c, dataNode); err != nil {
				log.LogWarnf("action[decommissionSingleDp] dp %v ChangeLeader to addr %v err %v", dp.PartitionID, newAddr, err)
			}

			select {
			case <-ticker.C:
				if !c.partition.IsRaftLeader() {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait tryToChangeLeader  addr %v master leader changed", dp.PartitionID, newAddr)
					goto ERR
				}
				times++
				if times == 60 {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait leader selection  new addr %v  timeout", dp.PartitionID, newAddr)
					goto ERR
				}
			case decommContinue = <-dp.SpecialReplicaDecommissionStop:
				if !decommContinue {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait for leader selection is stopped", dp.PartitionID)
					dp.SetDecommissionStatus(DecommissionPause)
					goto ERR
				}
			}
		}
		log.LogInfof("action[decommissionSingleDp] dp %v try removeDataReplica %v", dp.PartitionID, offlineAddr)
		dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionRemoveOld)
		c.syncUpdateDataPartition(dp)
	}
	// 3.delete offline replica
	if dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionRemoveOld {
		if err = c.removeDataReplica(dp, offlineAddr, false, false); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v err %v", dp.PartitionID, err)
			goto ERR
		}
		dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
		dp.SetDecommissionStatus(DecommissionSuccess)
		// dp may not add into decommission list when master restart or leader change
		dp.setRestoreReplicaStop()
		c.syncUpdateDataPartition(dp)
		log.LogInfof("action[decommissionSingleDp] dp %v success", dp.PartitionID)
		return
	}
	log.LogWarnf("action[decommissionSingleDp] dp %v unexpect end: %v", dp.PartitionID, dp.GetSpecialReplicaDecommissionStep())
	return nil
ERR:
	log.LogWarnf("action[decommissionSingleDp] dp %v err:%v", dp.PartitionID, err)
	return err
}

// func (c *Cluster) autoAddDataReplica(dp *DataPartition) (success bool, err error) {
// 	var (
// 		targetHosts []string
// 		newAddr     string
// 		vol         *Vol
// 		zone        *Zone
// 		ns          *nodeSet
// 	)
// 	success = false

// 	dp.RLock()

// 	// not support
// 	if dp.isSpecialReplicaCnt() {
// 		dp.RUnlock()
// 		return
// 	}

// 	dp.RUnlock()

// 	// not support
// 	if !proto.IsNormalDp(dp.PartitionType) {
// 		return
// 	}

// 	var ok bool
// 	if vol, ok = c.vols[dp.VolName]; !ok {
// 		log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] vol not exist, PersistenceHosts:[%v]",
// 			c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
// 		return
// 	}

// 	// not support
// 	if c.isFaultDomain(vol) {
// 		return
// 	}

// 	if vol.crossZone {
// 		zones := dp.getZones()
// 		if targetHosts, _, err = c.getHostFromNormalZone(TypeDataPartition, zones, nil, dp.Hosts, 1, 1, "", dp.MediaType); err != nil {
// 			goto errHandler
// 		}
// 	} else {
// 		if zone, err = c.t.getZone(vol.zoneName); err != nil {
// 			log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] zone not exist, PersistenceHosts:[%v]",
// 				c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
// 			return
// 		}
// 		nodeSets := dp.getNodeSets()
// 		if len(nodeSets) != 1 {
// 			log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] the number of nodeSets is not one, PersistenceHosts:[%v]",
// 				c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
// 			return
// 		}
// 		if ns, err = zone.getNodeSet(nodeSets[0]); err != nil {
// 			goto errHandler
// 		}
// 		if targetHosts, _, err = ns.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
// 			goto errHandler
// 		}
// 	}

// 	newAddr = targetHosts[0]
// 	if err = c.addDataReplica(dp, newAddr, false); err != nil {
// 		goto errHandler
// 	}

// 	dp.Status = proto.ReadOnly
// 	dp.isRecover = true
// 	c.putBadDataPartitionIDs(nil, newAddr, dp.PartitionID)

// 	dp.RLock()
// 	c.syncUpdateDataPartition(dp)
// 	dp.RUnlock()

// 	log.LogInfof("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] auto add data replica success, newReplicaHost[%v], PersistenceHosts:[%v]",
// 		c.Name, dp.VolName, dp.PartitionID, newAddr, dp.Hosts)
// 	success = true
// 	return

// errHandler:
// 	if err != nil {
// 		err = fmt.Errorf("clusterID[%v] vol[%v] partitionID[%v], err[%v]", c.Name, dp.VolName, dp.PartitionID, err)
// 		log.LogErrorf("action[autoAddDataReplica] err %v", err)
// 	}
// 	return
// }

// Decommission a data partition.
// 1. Check if we can decommission a data partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas is less than the majority
// 2. Choose a new data node.
// 3. synchronized decommission data partition
// 4. synchronized create a new data partition
// 5. Set the data partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) migrateDataPartition(srcAddr, targetAddr string, dp *DataPartition, raftForce bool, errMsg string) (err error) {
	var (
		targetHosts     []string
		finalHosts      []string
		newAddr         string
		msg             string
		dataNode        *DataNode
		zone            *Zone
		replica         *DataReplica
		ns              *nodeSet
		excludeNodeSets []uint64
		zones           []string
	)
	log.LogDebugf("[migrateDataPartition] src %v target %v raftForce %v", srcAddr, targetAddr, raftForce)
	dp.RLock()
	if ok := dp.hasHost(srcAddr); !ok {
		dp.RUnlock()
		return
	}
	if dp.isSpecialReplicaCnt() {
		if dp.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionInitial {
			err = fmt.Errorf("volume [%v] dp [%v] is on decommission", dp.VolName, dp.PartitionID)
			log.LogErrorf("action[decommissionDataPartition][%v] ", err)
			dp.RUnlock()
			return
		}
		dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	}

	replica, _ = dp.getReplica(srcAddr)
	dp.RUnlock()

	// delete if not normal data partition
	if !proto.IsNormalDp(dp.PartitionType) {
		c.vols[dp.VolName].deleteDataPartition(c, dp)
		return
	}

	if err = c.validateDecommissionDataPartition(dp, srcAddr); err != nil {
		goto errHandler
	}

	if dataNode, err = c.dataNode(srcAddr); err != nil {
		goto errHandler
	}

	if dataNode.ZoneName == "" {
		err = fmt.Errorf("dataNode[%v] zone is nil", dataNode.Addr)
		goto errHandler
	}

	if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
		goto errHandler
	}

	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		goto errHandler
	}

	dp.RLock()
	finalHosts = append(dp.Hosts, newAddr) // add new one
	dp.RUnlock()
	for i, host := range finalHosts {
		if host == srcAddr {
			finalHosts = append(finalHosts[:i], finalHosts[i+1:]...) // remove old one
			break
		}
	}
	if err = c.checkMultipleReplicasOnSameMachine(finalHosts); err != nil {
		goto errHandler
	}

	if targetAddr != "" {
		targetHosts = []string{targetAddr}
		if err = c.checkDataNodesMediaTypeForMigrate(dataNode, targetAddr); err != nil {
			log.LogErrorf("[migrateDataPartition] check mediaType err: %v", err.Error())
			goto errHandler
		}
	} else if targetHosts, _, err = ns.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
		if _, ok := c.vols[dp.VolName]; !ok {
			log.LogWarnf("clusterID[%v] partitionID:%v  on node:%v offline failed,PersistenceHosts:[%v]",
				c.Name, dp.PartitionID, srcAddr, dp.Hosts)
			goto errHandler
		}
		if c.isFaultDomain(c.vols[dp.VolName]) {
			log.LogErrorf("clusterID[%v] partitionID:%v  on node:%v is banlance zone,PersistenceHosts:[%v]",
				c.Name, dp.PartitionID, srcAddr, dp.Hosts)
			goto errHandler
		}
		// select data nodes from the other node set in same zone
		excludeNodeSets = append(excludeNodeSets, ns.ID)
		if targetHosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, excludeNodeSets, dp.Hosts, 1); err != nil {
			// select data nodes from the other zone
			zones = dp.getLiveZones(srcAddr)
			if targetHosts, _, err = c.getHostFromNormalZone(TypeDataPartition, zones, excludeNodeSets, dp.Hosts, 1, 1, "", dp.MediaType); err != nil {
				goto errHandler
			}
		}
	}

	newAddr = targetHosts[0]
	err = c.updateDataNodeSize(newAddr, dp)
	if err != nil {
		log.LogErrorf("action[migrateDataPartition] target addr can't be writable, add %s %s", newAddr, err.Error())
		return
	}

	defer func() {
		if err != nil {
			c.returnDataSize(newAddr, dp)
		}
	}()

	// if special replica wait for
	if dp.ReplicaNum == 1 || (dp.ReplicaNum == 2 && (dp.ReplicaNum == c.vols[dp.VolName].dpReplicaNum) && !raftForce) {
		dp.Status = proto.ReadOnly
		dp.isRecover = true
		c.putBadDataPartitionIDs(replica, srcAddr, dp.PartitionID)

		if err = c.decommissionSingleDp(dp, newAddr, srcAddr); err != nil {
			goto errHandler
		}
	} else {
		if err = c.removeDataReplica(dp, srcAddr, false, raftForce); err != nil {
			goto errHandler
		}
		if err = c.addDataReplica(dp, newAddr, false); err != nil {
			goto errHandler
		}

		dp.Status = proto.ReadOnly
		dp.isRecover = true
		c.putBadDataPartitionIDs(replica, srcAddr, dp.PartitionID)
	}
	log.LogDebugf("[migrateDataPartition] src %v target %v raftForce %v", srcAddr, targetAddr, raftForce)
	dp.RLock()
	c.syncUpdateDataPartition(dp)
	dp.RUnlock()

	log.LogWarnf("[migrateDataPartition] clusterID[%v] partitionID:%v  on node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, srcAddr, newAddr, dp.Hosts)
	dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	return

errHandler:
	if dp.isSpecialReplicaCnt() {
		if dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionEnter {
			dp.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
		}
	}
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, srcAddr, newAddr, err, dp.Hosts)

	if err != nil {
		Warn(c.Name, msg)
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		log.LogErrorf("actin[decommissionDataPartition] err %v", err)
	}

	return
}

// Decommission a data partition.
// 1. Check if we can decommission a data partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas is less than the majority
// 2. Choose a new data node.
// 3. synchronized decommission data partition
// 4. synchronized create a new data partition
// 5. Set the data partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) decommissionDataPartition(offlineAddr string, dp *DataPartition, raftForce bool, errMsg string) (err error) {
	return c.migrateDataPartition(offlineAddr, "", dp, raftForce, errMsg)
}

func (c *Cluster) validateDecommissionDataPartition(dp *DataPartition, offlineAddr string) (err error) {
	dp.RLock()
	defer dp.RUnlock()
	var vol *Vol
	if vol, err = c.getVol(dp.VolName); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	if err = dp.hasMissingOneReplica(offlineAddr, int(vol.dpReplicaNum)); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	// if the partition can be offline or not
	if err = dp.canBeOffLine(offlineAddr); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	// for example, new replica is added,  but remove src replica is failed. Then retry decommission should not check isRecover
	// leader change also do not check isRecover for isRecover is not reset
	if dp.DecommissionRetry >= 1 && dp.isSpecialReplicaCnt() {
		log.LogInfof("action[validateDecommissionDataPartition] vol %v dp %v decommission retry,do not check isRecover",
			dp.VolName, dp.PartitionID)
		return
	}
	if dp.isRecover && !dp.activeUsedSimilar() {
		err = fmt.Errorf("vol[%v],data partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, dp.PartitionID, offlineAddr)
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}
	log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v looks fine!", dp.VolName, dp.PartitionID)
	return
}

func (c *Cluster) addDataReplica(dp *DataPartition, addr string, ignoreDecommissionDisk bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addDataReplica],vol[%v],dp %v ,err[%v]", dp.VolName, dp.PartitionID, err)
		} else {
			log.LogInfof("action[addDataReplica]  dp %v add replica dst addr %v success!", dp.PartitionID, addr)
		}
	}()

	log.LogInfof("action[addDataReplica]  dp %v enter %v add replica %v", dp.PartitionID, ignoreDecommissionDisk, addr)

	dp.addReplicaMutex.Lock()
	defer dp.addReplicaMutex.Unlock()

	targetDataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	if targetDataNode.MediaType != dp.MediaType {
		err = fmt.Errorf("target datanode mediaType(%v) not match datapartition mediaType(%v)",
			proto.MediaTypeString(targetDataNode.MediaType), proto.MediaTypeString(dp.MediaType))
		log.LogErrorf("[addDataReplica] dpId(%v), err: %v", dp.PartitionID, err.Error())
		return
	}

	addPeer := proto.Peer{ID: targetDataNode.ID, Addr: addr, HeartbeatPort: targetDataNode.HeartbeatPort, ReplicaPort: targetDataNode.ReplicaPort}

	if !proto.IsNormalDp(dp.PartitionType) {
		return fmt.Errorf("action[addDataReplica] [%d] is not normal dp, not support add or delete replica", dp.PartitionID)
	}

	log.LogInfof("action[addDataReplica] dp %v dst addr %v try add raft member, node id %v", dp.PartitionID, addr, targetDataNode.ID)
	if err = c.addDataPartitionRaftMember(dp, addPeer); err != nil {
		log.LogWarnf("action[addDataReplica] dp %v addr %v try add raft member err [%v]", dp.PartitionID, addr, err)
		return
	}

	log.LogInfof("action[addDataReplica] dp %v addr %v try create data replica ignoreDecommissionDisk %v",
		dp.PartitionID, addr, ignoreDecommissionDisk)
	if err = c.createDataReplica(dp, addPeer, ignoreDecommissionDisk); err != nil {
		c.removeHostMember(dp, addPeer)
		log.LogWarnf("action[addDataReplica] dp %v addr %v createDataReplica err [%v]", dp.PartitionID, addr, err)
		return
	}
	return
}

// update datanode size with to replica size
func (c *Cluster) updateDataNodeSize(addr string, dp *DataPartition) error {
	if len(dp.Replicas) == 0 {
		return errors.NewErrorf("dp %v has empty replica", dp.decommissionInfo())
	}
	leaderSize := dp.Replicas[0].Used
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return err
	}

	dataNode.Lock()
	defer dataNode.Unlock()

	if !dataNode.isWriteAbleWithSizeNoLock(10 * util.GB) {
		return fmt.Errorf("new datanode %s is not writable AvailableSpace(%v) isActive(%v) RdOnly(%v) Total(%v) Used(%v)",
			addr, dataNode.AvailableSpace, dataNode.isActive, dataNode.RdOnly, dataNode.Total, dataNode.Used)
	}

	dataNode.LastUpdateTime = time.Now()
	if dataNode.AvailableSpace < leaderSize {
		dataNode.AvailableSpace = 0
		return nil
	}

	dataNode.AvailableSpace -= leaderSize
	return nil
}

func (c *Cluster) returnDataSize(addr string, dp *DataPartition) {
	if len(dp.Replicas) == 0 {
		log.LogErrorf("returnDataSize dp(%v) has no replicas", dp.PartitionID)
		return
	}
	leaderSize := dp.Replicas[0].Used
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	dataNode.Lock()
	defer dataNode.Unlock()
	log.LogWarnf("returnDataSize after error, addr %s, ava %d, leader %d", addr, dataNode.AvailableSpace, leaderSize)

	dataNode.LastUpdateTime = time.Now()
	dataNode.AvailableSpace += leaderSize
}

func (c *Cluster) buildAddDataPartitionRaftMemberTaskAndSyncSendTask(dp *DataPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	log.LogInfof("action[buildAddDataPartitionRaftMemberTaskAndSyncSendTask] add peer [%v] start", addPeer)
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}
		if err != nil {
			log.LogErrorf("vol[%v],data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		} else {
			log.LogWarnf("vol[%v],data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		}
	}()
	task, err := dp.createTaskToAddRaftMember(addPeer, leaderAddr)
	if err != nil {
		return
	}
	leaderDataNode, err := c.dataNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderDataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	log.LogInfof("action[buildAddDataPartitionRaftMemberTaskAndSyncSendTask] add peer [%v] finished", addPeer)
	return
}

func (c *Cluster) addDataPartitionRaftMember(dp *DataPartition, addPeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)

	if leaderAddr, candidateAddrs, err = dp.prepareAddRaftMember(addPeer); err != nil {
		// maybe already add success before(master has updated hosts)
		return nil
	}

	dp.Lock()

	oldHosts := make([]string, len(dp.Hosts))
	copy(oldHosts, dp.Hosts)
	oldPeers := make([]proto.Peer, len(dp.Peers))
	copy(oldPeers, dp.Peers)

	dp.Hosts = append(dp.Hosts, addPeer.Addr)
	dp.Peers = append(dp.Peers, addPeer)

	dp.Unlock()

	// send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		if leaderAddr == "" && len(candidateAddrs) < int(dp.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildAddDataPartitionRaftMemberTaskAndSyncSendTask(dp, addPeer, host)
		if err == nil {
			break
		} else {
			// if send to leader raise err, it may send to follower ,then follower forward
			// this request to leader, return nil. so when leader encounter en error, should
			//  return err
			if leaderAddr != "" && leaderAddr == host {
				dp.Hosts = oldHosts
				dp.Peers = oldPeers
				return err
			}
		}

		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}

	dp.Lock()
	defer dp.Unlock()
	if err != nil {
		dp.Hosts = oldHosts
		dp.Peers = oldPeers
		return
	}

	log.LogInfof("action[addDataPartitionRaftMember] try host [%v] to [%v] peers [%v] to [%v]",
		dp.Hosts, dp.Hosts, dp.Peers, dp.Peers)

	if err = dp.update("addDataPartitionRaftMember", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Hosts = oldHosts
		dp.Peers = oldPeers
		return
	}

	return
}

func (c *Cluster) createDataReplica(dp *DataPartition, addPeer proto.Peer, ignoreDecommissionDisk bool) (err error) {
	vol, err := c.getVol(dp.VolName)
	if err != nil {
		return
	}

	dp.RLock()
	hosts := make([]string, len(dp.Hosts))
	copy(hosts, dp.Hosts)
	peers := make([]proto.Peer, len(dp.Peers))
	copy(peers, dp.Peers)
	dp.RUnlock()

	diskPath, err := c.syncCreateDataPartitionToDataNode(addPeer.Addr, vol.dataPartitionSize,
		dp, peers, hosts, proto.DecommissionedCreateDataPartition, dp.PartitionType, true, ignoreDecommissionDisk)
	if err != nil {
		log.LogErrorf("[createDataReplica] %v", err)
		return
	}

	dp.Lock()
	defer dp.Unlock()

	if err = dp.afterCreation(addPeer.Addr, diskPath, c); err != nil {
		return
	}
	if err = dp.update("createDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		return
	}

	return
}

func (c *Cluster) removeDataReplica(dp *DataPartition, addr string, validate bool, raftForceDel bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[removeDataReplica],vol[%v],data partition[%v] remove %v,err[%v]",
				dp.VolName, dp.PartitionID, addr, err)
		}
	}()
	// skip removeDataReplica when decommission AutoAddReplica mode, but when execute rollback operation,
	// can not skip it to delete decommission dst replica
	if dp.DecommissionType == AutoAddReplica && addr == dp.DecommissionSrcAddr {
		log.LogDebugf("action[removeDataReplica]auto add dp %v skip  removeDataReplica %v", dp.PartitionID, addr)
		return
	}
	log.LogInfof("action[removeDataReplica]  dp %v try remove replica  addr [%v]", dp.PartitionID, addr)
	// validate be set true only in api call
	if validate && !raftForceDel {
		if err = c.validateDecommissionDataPartition(dp, addr); err != nil {
			return
		}
	}

	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	if !proto.IsNormalDp(dp.PartitionType) {
		return fmt.Errorf("[%d] is not normal dp, not support add or delete replica", dp.PartitionID)
	}
	removePeer := proto.Peer{ID: dataNode.ID, Addr: addr, HeartbeatPort: dataNode.HeartbeatPort, ReplicaPort: dataNode.ReplicaPort}
	if err = c.removeDataPartitionRaftMember(dp, removePeer, raftForceDel); err != nil {
		return
	}

	if err = c.removeHostMember(dp, removePeer); err != nil {
		return
	}

	if err = c.deleteDataReplica(dp, dataNode, raftForceDel); err != nil {
		return
	}
	// may already change leader during last decommission
	leaderAddr := dp.getLeaderAddrWithLock()
	if leaderAddr != addr {
		return
	}

	if dataNode, err = c.dataNode(dp.Hosts[0]); err != nil {
		return
	}

	if err = dp.tryToChangeLeader(c, dataNode); err != nil {
		return
	}

	return
}

func (c *Cluster) removeHostMember(dp *DataPartition, removePeer proto.Peer) (err error) {
	newHosts := make([]string, 0, len(dp.Hosts)-1)
	for _, host := range dp.Hosts {
		if host == removePeer.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	newPeers := make([]proto.Peer, 0, len(dp.Peers)-1)
	for _, peer := range dp.Peers {
		if peer.ID == removePeer.ID && peer.Addr == removePeer.Addr {
			continue
		}
		newPeers = append(newPeers, peer)
	}

	dp.Lock()
	defer dp.Unlock()
	if err = dp.update("removeDataPartitionRaftMember", dp.VolName, newPeers, newHosts, c); err != nil {
		return
	}
	return
}

func (c *Cluster) removeDataPartitionRaftMember(dp *DataPartition, removePeer proto.Peer, force bool) (err error) {
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()
	defer func() {
		if err1 := c.updateDataPartitionOfflinePeerIDWithLock(dp, 0); err1 != nil {
			err = errors.Trace(err, "updateDataPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()
	if err = c.updateDataPartitionOfflinePeerIDWithLock(dp, removePeer.ID); err != nil {
		log.LogErrorf("action[removeDataPartitionRaftMember] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}
	return dp.createTaskToRemoveRaftMember(c, removePeer, force, false)
}

// call from remove raft member
func (c *Cluster) updateDataPartitionOfflinePeerIDWithLock(dp *DataPartition, peerID uint64) (err error) {
	dp.Lock()
	defer dp.Unlock()
	dp.OfflinePeerID = peerID
	if err = dp.update("updateDataPartitionOfflinePeerIDWithLock", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		return
	}
	return
}

func (c *Cluster) deleteDataReplica(dp *DataPartition, dataNode *DataNode, raftForceDel bool) (err error) {
	dp.Lock()
	// in case dataNode is unreachable,update meta first.
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	log.LogDebugf("action[deleteDataReplica] vol[%v],data partition[%v] remove replica[%v] force(%v)",
		dp.VolName, dp.decommissionInfo(), dataNode.Addr, raftForceDel)
	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Unlock()
		return
	}
	task := dp.createTaskToDeleteDataPartition(dataNode.Addr, raftForceDel)
	dp.Unlock()

	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
	}

	return nil
}

func (c *Cluster) putBadMetaPartitions(addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	newBadPartitionIDs := make([]uint64, 0)
	badPartitionIDs, ok := c.BadMetaPartitionIds.Load(addr)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadMetaPartitionIds.Store(addr, newBadPartitionIDs)
}

func (c *Cluster) getBadMetaPartitionsView() (bmpvs []badPartitionView) {
	c.badPartitionMutex.RLock()
	defer c.badPartitionMutex.RUnlock()

	bmpvs = make([]badPartitionView, 0)
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		bmpvs = append(bmpvs, bpv)
		return true
	})
	return
}

func (c *Cluster) putBadDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	var key string
	newBadPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func (c *Cluster) clearBadDataPartitionIDS() {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()
	keysToDelete := make([]string, 0)
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		keysToDelete = append(keysToDelete, key.(string))
		return true
	})
	for _, key := range keysToDelete {
		c.BadDataPartitionIds.Delete(key)
	}
}

func (c *Cluster) putBadDataPartitionIDsByDiskPath(disk, addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	var key string
	newBadPartitionIDs := make([]uint64, 0)
	key = fmt.Sprintf("%s:%s", addr, disk)

	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	if in(partitionID, newBadPartitionIDs) {
		return
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func in(target uint64, strArray []uint64) bool {
	for _, element := range strArray {
		if target == element {
			return true
		}
	}
	return false
}

func (c *Cluster) getBadDataPartitionsView() (bpvs []badPartitionView) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	bpvs = make([]badPartitionView, 0)
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badDataPartitionIds}
		bpvs = append(bpvs, bpv)
		return true
	})
	return
}

func (c *Cluster) getBadDataPartitionsRepairView() (bprvs []proto.BadPartitionRepairView) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	bprvs = make([]proto.BadPartitionRepairView, 0)
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		dpRepairInfos := make([]proto.DpRepairInfo, 0)
		path := key.(string)

		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				continue
			}
			replica, err := partition.getReplica(partition.DecommissionDstAddr)
			if err != nil {
				log.LogDebugf("getBadDataPartitionsRepairView: replica for partitionID[%v] addr[%v] is empty", partitionID, partition.DecommissionDstAddr)
				continue
			}
			dpRepairInfo := proto.DpRepairInfo{PartitionID: partitionID, DecommissionRepairProgress: replica.DecommissionRepairProgress}
			dpRepairInfos = append(dpRepairInfos, dpRepairInfo)
			log.LogDebugf("getBadDataPartitionsRepairView: partitionID[%v], addr[%v], dpRepairInfo[%v]",
				partitionID, partition.DecommissionDstAddr, dpRepairInfo)
		}

		bprv := proto.BadPartitionRepairView{Path: path, PartitionInfos: dpRepairInfos}
		bprvs = append(bprvs, bprv)
		return true
	})
	return
}

func (c *Cluster) migrateMetaNode(srcAddr, targetAddr string, limit int) (err error) {
	var toBeOfflineMps []*MetaPartition

	if c.ForbidMpDecommission {
		err = fmt.Errorf("cluster mataPartition decommission switch is disabled")
		return
	}

	msg := fmt.Sprintf("action[migrateMetaNode],clusterID[%v] migrate from node[%v] to [%s] begin", c.Name, srcAddr, targetAddr)
	log.LogWarn(msg)

	metaNode, err := c.metaNode(srcAddr)
	if err != nil {
		return err
	}

	metaNode.MigrateLock.Lock()
	defer metaNode.MigrateLock.Unlock()

	partitions := c.getAllMetaPartitionByMetaNode(srcAddr)
	if targetAddr != "" {
		toBeOfflineMps = make([]*MetaPartition, 0)
		for _, mp := range partitions {
			if contains(mp.Hosts, targetAddr) {
				continue
			}

			toBeOfflineMps = append(toBeOfflineMps, mp)
		}
	} else {
		toBeOfflineMps = partitions
	}

	if len(toBeOfflineMps) <= 0 && len(partitions) != 0 {
		return fmt.Errorf("migrateMataNode no partition can migrate from [%s] to [%s] limit [%v]", srcAddr, targetAddr, limit)
	}

	if limit <= 0 {
		limit = util.DefaultMigrateMpCnt
	}

	if limit > len(toBeOfflineMps) {
		limit = len(toBeOfflineMps)
	}

	var wg sync.WaitGroup
	metaNode.ToBeOffline = true
	metaNode.MaxMemAvailWeight = 1
	errChannel := make(chan error, limit)

	defer func() {
		metaNode.ToBeOffline = false
		close(errChannel)
	}()

	for idx := 0; idx < limit; idx++ {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			if err1 := c.migrateMetaPartition(srcAddr, targetAddr, mp); err1 != nil {
				errChannel <- err1
			}
		}(toBeOfflineMps[idx])
	}

	wg.Wait()
	select {
	case err = <-errChannel:
		log.LogErrorf("action[migrateMetaNode] clusterID[%v] migrate node[%s] to [%s] faild, err(%s)",
			c.Name, srcAddr, targetAddr, err.Error())
		return
	default:
	}

	if limit < len(partitions) {
		log.LogWarnf("action[migrateMetaNode] clusterID[%v] migrate from [%s] to [%s] cnt[%d] success",
			c.Name, srcAddr, targetAddr, limit)
		return
	}

	if err = c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[migrateMetaNode], clusterID[%v] node[%v] synDelMetaNode failed,err[%s]",
			c.Name, srcAddr, err.Error())
		Warn(c.Name, msg)
		return
	}

	c.deleteMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[migrateMetaNode],clusterID[%v] migrate from node[%v] to node(%s) success", c.Name, srcAddr, targetAddr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) decommissionMetaNode(metaNode *MetaNode) (err error) {
	return c.migrateMetaNode(metaNode.Addr, "", 0)
}

func (c *Cluster) deleteMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	c.t.deleteMetaNode(metaNode)
	go metaNode.clean()
}

func (c *Cluster) updateVol(name, authKey string, newArgs *VolVarargs) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
		volUsedSpace  uint64
		oldArgs       *VolVarargs
	)

	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[updateVol] err[%v]", err)
		err = proto.ErrVolNotExists
		goto errHandler
	}

	if vol.status() == proto.VolStatusMarkDelete {
		log.LogErrorf("action[updateVol] vol is already deleted, name(%s)", name)
		err = proto.ErrVolNotExists
		goto errHandler
	}

	vol.volLock.Lock()
	defer vol.volLock.Unlock()

	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}

	volUsedSpace = vol.totalUsedSpace()
	if float64(newArgs.capacity*util.GB) < float64(volUsedSpace)*1.01 && newArgs.capacity != vol.Capacity {
		err = fmt.Errorf("capacity[%v] has to be 1 percent larger than the used space[%v]", newArgs.capacity,
			volUsedSpace/util.GB)
		goto errHandler
	}

	log.LogInfof("[checkZoneName] name [%s], zone [%s]", name, newArgs.zoneName)
	if newArgs.zoneName, err = c.checkZoneName(name, newArgs.crossZone, vol.defaultPriority, newArgs.zoneName, vol.domainId); err != nil {
		goto errHandler
	}

	if newArgs.coldArgs.cacheCap >= newArgs.capacity {
		err = fmt.Errorf("capacity must be large than cache capacity, newCap(%d), newCacheCap(%d)", newArgs.capacity, newArgs.coldArgs.cacheCap)
		goto errHandler
	}

	oldArgs = getVolVarargs(vol)
	setVolFromArgs(newArgs, vol)
	if err = c.syncUpdateVol(vol); err != nil {
		setVolFromArgs(oldArgs, vol)
		log.LogErrorf("action[updateVol] vol[%v] err[%v]", name, err)
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}

	return

errHandler:
	err = fmt.Errorf("action[updateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) checkNormalZoneName(zoneName string) (err error) {
	var zones []string
	if c.needFaultDomain {
		zones = c.t.domainExcludeZones
	} else {
		zones = c.t.getZoneNameList()
	}

	zoneList := strings.Split(zoneName, ",")
	for i := 0; i < len(zoneList); i++ {
		var isZone bool
		for j := 0; j < len(zones); j++ {
			if zoneList[i] == zones[j] {
				isZone = true
				break
			}
		}

		if !isZone {
			return fmt.Errorf("action[checkZoneName] the zonename[%s] not found", zoneList[i])
		}
	}
	return
}

func (c *Cluster) checkZoneName(name string,
	crossZone bool,
	defaultPriority bool,
	zoneName string,
	domainId uint64) (newZoneName string, err error,
) {
	zoneList := strings.Split(zoneName, ",")
	newZoneName = zoneName

	if crossZone {
		if newZoneName != "" {
			if len(zoneList) == 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] vol use specified single zoneName conflit with cross zone flag")
			} else {
				if err = c.checkNormalZoneName(newZoneName); err != nil {
					return newZoneName, err
				}
			}
		}
		if c.FaultDomain {
			if newZoneName != "" {
				if !defaultPriority || domainId > 0 {
					return newZoneName, fmt.Errorf("action[checkZoneName] vol need FaultDomain but set zone name")
				}
			} else {
				if domainId > 0 {
					if _, ok := c.domainManager.domainId2IndexMap[domainId]; !ok {
						return newZoneName, fmt.Errorf("action[checkZoneName] cluster can't find oomainId [%v]", domainId)
					}
				}
			}
		} else {
			if c.t.zoneLen() <= 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] cluster has one zone,can't cross zone")
			}
		}
	} else { // cross zone disable means not use domain at the time vol be created
		if newZoneName == "" {
			if !c.needFaultDomain {
				if _, err = c.t.getZone(DefaultZoneName); err != nil {
					return newZoneName, fmt.Errorf("action[checkZoneName] the vol is not cross zone and didn't set zone name,but there's no default zone")
				}
			}
			log.LogInfof("action[checkZoneName] vol [%v] use default zone", name)
			newZoneName = DefaultZoneName
		} else {
			if len(zoneList) > 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] vol specified zoneName need cross zone")
			}

			if err = c.checkNormalZoneName(newZoneName); err != nil {
				return newZoneName, err
			}
		}
	}
	return
}

func (c *Cluster) HasResourceOfStorageBlobStore() (has bool) {
	has = true
	if c.server.bStoreAddr == "" || c.server.servicePath == "" {
		has = false
	}

	return has
}

// StorageClassResourceChecker : check if the cluster has resource to support the specified storage class
type StorageClassResourceChecker struct {
	StorageClassResourceSet map[uint32]struct{}
}

func (checker *StorageClassResourceChecker) HasResourceOfStorageClass(storageClass uint32) (has bool) {
	_, has = checker.StorageClassResourceSet[storageClass]
	return
}

func NewStorageClassResourceChecker(c *Cluster, zoneNameList string) (checker *StorageClassResourceChecker) {
	checker = &StorageClassResourceChecker{
		StorageClassResourceSet: make(map[uint32]struct{}),
	}

	dataNodeMediaTypeMap := c.t.getDataMediaTypeCanUse(zoneNameList)
	for storageClass := range dataNodeMediaTypeMap {
		checker.StorageClassResourceSet[storageClass] = struct{}{}
	}

	if c.HasResourceOfStorageBlobStore() {
		checker.StorageClassResourceSet[proto.StorageClass_BlobStore] = struct{}{}
	}

	return
}

func (c *Cluster) GetFastestReplicaStorageClassInCluster(resourceChecker *StorageClassResourceChecker,
	zoneNameList string,
) (chosenStorageClass uint32) {
	chosenStorageClass = proto.StorageClass_Unspecified

	if resourceChecker == nil {
		resourceChecker = NewStorageClassResourceChecker(c, zoneNameList)
	}

	if resourceChecker.HasResourceOfStorageClass(proto.StorageClass_Replica_SSD) {
		chosenStorageClass = proto.StorageClass_Replica_SSD
	} else if resourceChecker.HasResourceOfStorageClass(proto.StorageClass_Replica_HDD) {
		chosenStorageClass = proto.StorageClass_Replica_HDD
	}

	return
}

func (c *Cluster) initDataPartitionsForCreateVol(vol *Vol, targetDpCount int, mediaType uint32) (dpCountOfMediaType int, err error) {
	if targetDpCount > maxInitDataPartitionCnt {
		err = fmt.Errorf("[initDataPartitionsForCreateVol] initDataPartitions failed, vol[%v], targetDpCount[%d] exceeds maximum limit[%d]",
			vol.Name, targetDpCount, maxInitDataPartitionCnt)
		return 0, err
	}

	for retryCount := 0; dpCountOfMediaType < defaultInitDataPartitionCnt && retryCount < 3; retryCount++ {
		oldDpCountOfMediaType := dpCountOfMediaType

		toCreateCount := targetDpCount - dpCountOfMediaType
		err = vol.initDataPartitions(c, toCreateCount, mediaType)
		if err != nil {
			log.LogErrorf("action[initDataPartitionsForCreateVol] vol(%v) mediaType(%v) retryCount(%v), init dataPartition error: %v",
				vol.Name, proto.MediaTypeString(mediaType), retryCount, err.Error())
		}

		dpCountOfMediaType = vol.dataPartitions.getDataPartitionsCountOfMediaType(mediaType)
		log.LogInfof("[initDataPartitionsForCreateVol] vol(%v) mediaType(%v) retryCount(%v), this round created dp count: %v, total: %v",
			vol.Name, proto.MediaTypeString(mediaType), retryCount, dpCountOfMediaType-oldDpCountOfMediaType, dpCountOfMediaType)
	}

	if dpCountOfMediaType < defaultInitDataPartitionCnt {
		err = fmt.Errorf("action[initDataPartitionsForCreateVol] vol[%v] mediaType[%v] initDataPartitions failed, createdCount(%v), less than minLimit(%d)",
			vol.Name, proto.MediaTypeString(mediaType), dpCountOfMediaType, defaultInitDataPartitionCnt)

		oldVolStatus := vol.Status
		vol.Status = proto.VolStatusMarkDelete
		if errSync := c.syncUpdateVol(vol); errSync != nil {
			log.LogErrorf("action[initDataPartitionsForCreateVol] vol[%v] mediaType[%v] after init dataPartition error, mark vol delete persist failed",
				vol.Name, proto.MediaTypeString(mediaType))
			vol.Status = oldVolStatus
		} else {
			log.LogErrorf("action[initDataPartitionsForCreateVol] vol[%v] mediaType[%v] mark vol delete after init dataPartition error",
				vol.Name, proto.MediaTypeString(mediaType))
		}

		return dpCountOfMediaType, err
	}

	return dpCountOfMediaType, nil
}

// Create a new volume.
// By default, we create 3 meta partitions and 10 data partitions during initialization.
func (c *Cluster) createVol(req *createVolReq) (vol *Vol, err error) {
	if c.DisableAutoAllocate {
		log.LogWarn("the cluster is frozen")
		return nil, fmt.Errorf("the cluster is frozen, can not create volume")
	}

	var readWriteDataPartitions int

	if req.zoneName, err = c.checkZoneName(req.name, req.crossZone, req.normalZonesFirst, req.zoneName, req.domainId); err != nil {
		return
	}

	if vol, err = c.doCreateVol(req); err != nil {
		goto errHandler
	}

	vol.aclMgr.init(c, vol)
	vol.initUidSpaceManager(c)
	vol.initQuotaManager(c)
	if err = vol.VersionMgr.init(c); err != nil {
		log.LogError("init dataPartition error in verMgr init", err.Error())
	}

	if err = vol.initMetaPartitions(c, req.mpCount); err != nil {
		vol.Status = proto.VolStatusMarkDelete

		if e := vol.deleteVolFromStore(c); e != nil {
			log.LogErrorf("action[createVol] deleteVolFromStore failed, vol[%v] err[%v]", vol.Name, e)
		}

		c.deleteVol(req.name)

		err = fmt.Errorf("action[createVol] initMetaPartitions failed, vol[%v] err[%v]", vol.Name, err)
		goto errHandler
	}

	// NOTE: init data partitions
	if proto.IsStorageClassReplica(vol.volStorageClass) && vol.Capacity > 0 {
		for _, acs := range req.allowedStorageClass {
			if !proto.IsStorageClassReplica(acs) {
				continue
			}

			chosenMediaType := proto.GetMediaTypeByStorageClass(acs)
			if readWriteDataPartitions, err = c.initDataPartitionsForCreateVol(vol, req.dpCount, chosenMediaType); err != nil {
				goto errHandler
			}
			log.LogInfof("action[createVol] vol[%v] created dp cnt[%v] mediaType(%v) for replica",
				req.name, readWriteDataPartitions, proto.MediaTypeString(chosenMediaType))
		}
	} else if proto.IsStorageClassBlobStore(vol.volStorageClass) && vol.CacheCapacity > 0 {
		chosenMediaType := proto.GetMediaTypeByStorageClass(vol.cacheDpStorageClass)
		log.LogInfof("action[createVol] vol[%v] to create cache dp with storageClass(%v) for blobStore",
			req.name, proto.StorageClassString(vol.cacheDpStorageClass))

		if readWriteDataPartitions, err = c.initDataPartitionsForCreateVol(vol, req.dpCount, chosenMediaType); err != nil {
			goto errHandler
		}
		log.LogInfof("action[createVol] vol[%v] created dp cnt[%v] mediaType(%v) for blobStore",
			req.name, readWriteDataPartitions, proto.MediaTypeString(chosenMediaType))
	}

	vol.updateViewCache(c)
	// NOTE: update dp view cache
	vol.dataPartitions.updateResponseCache(true, 0, vol)
	vol.dataPartitions.updateCompressCache(true, 0, vol)

	log.LogInfof("action[createVol] vol[%v], readableAndWritableCnt[%v]",
		req.name, vol.dataPartitions.readableAndWritableCnt)
	return

errHandler:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, req.name, err)
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) doCreateVol(req *createVolReq) (vol *Vol, err error) {
	c.createVolMutex.Lock()
	defer c.createVolMutex.Unlock()

	createTime := time.Now().Unix() // record unix seconds of volume create time
	var dataPartitionSize uint64

	if req.dpSize*util.GB == 0 {
		dataPartitionSize = util.DefaultDataPartitionSize
	} else {
		dataPartitionSize = uint64(req.dpSize) * util.GB
	}

	vv := volValue{
		Name:                    req.name,
		Owner:                   req.owner,
		ZoneName:                req.zoneName,
		DataPartitionSize:       dataPartitionSize,
		Capacity:                uint64(req.capacity),
		DpReplicaNum:            req.dpReplicaNum,
		ReplicaNum:              defaultReplicaNum,
		FollowerRead:            req.followerRead,
		MetaFollowerRead:        req.metaFollowerRead,
		MaximallyRead:           req.maximallyRead,
		Authenticate:            req.authenticate,
		CrossZone:               req.crossZone,
		DefaultPriority:         req.normalZonesFirst,
		DomainId:                req.domainId,
		CreateTime:              createTime,
		DeleteLockTime:          req.deleteLockTime,
		Description:             req.description,
		EnablePosixAcl:          req.enablePosixAcl,
		EnableQuota:             req.enableQuota,
		EnableTransaction:       req.enableTransaction,
		TxTimeout:               req.txTimeout,
		TxConflictRetryNum:      req.txConflictRetryNum,
		TxConflictRetryInterval: req.txConflictRetryInterval,

		VolType:          req.volType,
		EbsBlkSize:       req.coldArgs.objBlockSize,
		CacheCapacity:    req.coldArgs.cacheCap,
		CacheAction:      req.coldArgs.cacheAction,
		CacheThreshold:   req.coldArgs.cacheThreshold,
		CacheTTL:         req.coldArgs.cacheTtl,
		CacheHighWater:   req.coldArgs.cacheHighWater,
		CacheLowWater:    req.coldArgs.cacheLowWater,
		CacheLRUInterval: req.coldArgs.cacheLRUInterval,
		CacheRule:        req.coldArgs.cacheRule,

		VolQosEnable: req.qosLimitArgs.qosEnable,
		IopsRLimit:   req.qosLimitArgs.iopsRVal,
		IopsWLimit:   req.qosLimitArgs.iopsWVal,
		FlowRlimit:   req.qosLimitArgs.flowRVal,
		FlowWlimit:   req.qosLimitArgs.flowWVal,

		DpReadOnlyWhenVolFull:   req.DpReadOnlyWhenVolFull,
		EnableAutoMetaRepair:    false,
		TrashInterval:           req.trashInterval,
		AccessTimeInterval:      req.accessTimeValidInterval,
		EnablePersistAccessTime: req.enablePersistAccessTime,

		VolStorageClass:     req.volStorageClass,
		AllowedStorageClass: req.allowedStorageClass,
		CacheDpStorageClass: req.cacheDpStorageClass,

		RemoteCacheEnable:         req.remoteCacheEnable,
		RemoteCacheAutoPrepare:    req.remoteCacheAutoPrepare,
		RemoteCacheTTL:            req.remoteCacheTTL,
		RemoteCachePath:           req.remoteCachePath,
		RemoteCacheReadTimeoutSec: req.remoteCacheReadTimeout,
		RemoteCacheMaxFileSizeGB:  req.remoteCacheMaxFileSizeGB,
		RemoteCacheOnlyForNotSSD:  req.remoteCacheOnlyForNotSSD,
		RemoteCacheMultiRead:      req.remoteCacheMultiRead,
	}

	vv.QuotaOfClass = make([]*proto.StatOfStorageClass, 0)
	for _, c := range vv.AllowedStorageClass {
		vv.QuotaOfClass = append(vv.QuotaOfClass, proto.NewStatOfStorageClass(c))
	}

	log.LogInfof("[doCreateVol] volView, %v", vv.String())

	if vv.EnableTransaction == 0 {
		vv.EnableTransaction = proto.TxOpMask(proto.TxOpMaskRename)
		log.LogWarnf("[doCreateVol] volView, name %s, set rename default", vv.Name)
	}

	if _, err = c.getVol(req.name); err == nil {
		err = proto.ErrDuplicateVol
		goto errHandler
	}

	vv.ID, err = c.idAlloc.allocateCommonID()
	if err != nil {
		goto errHandler
	}

	vol = newVol(vv)
	log.LogInfof("[doCreateVol] vol, %v", vol)

	// refresh oss secure
	vol.refreshOSSSecure()

	if err = c.syncAddVol(vol); err != nil {
		goto errHandler
	}

	if err = c.putVol(vol); err != nil {
		goto errHandler
	}

	return

errHandler:
	err = fmt.Errorf("action[doCreateVol], clusterID[%v] name:%v, err:%v ", c.Name, req.name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) dataNodeCount() (len int) {
	c.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) metaNodeCount() (len int) {
	c.metaNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) allMasterNodes() (masterNodes []proto.NodeView) {
	masterNodes = make([]proto.NodeView, 0)

	for _, addr := range c.cfg.peerAddrs {
		split := strings.Split(addr, colonSplit)
		id, _ := strconv.ParseUint(split[0], 10, 64)
		masterNode := proto.NodeView{ID: id, Addr: split[1] + ":" + split[2], Status: true}
		masterNodes = append(masterNodes, masterNode)
	}
	return masterNodes
}

func (c *Cluster) lcNodeCount() (len int) {
	c.lcNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) allDataNodes() (dataNodes []proto.NodeView) {
	dataNodes = make([]proto.NodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, proto.NodeView{
			Addr: dataNode.Addr, DomainAddr: dataNode.DomainAddr,
			Status: dataNode.isActive, ID: dataNode.ID, IsWritable: dataNode.IsWriteAble(), MediaType: dataNode.MediaType,
			ForbidWriteOpOfProtoVer0: dataNode.ReceivedForbidWriteOpOfProtoVer0,
		})
		return true
	})
	return
}

func (c *Cluster) allMetaNodes() (metaNodes []proto.NodeView) {
	metaNodes = make([]proto.NodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, proto.NodeView{
			ID: metaNode.ID, Addr: metaNode.Addr, DomainAddr: metaNode.DomainAddr,
			Status: metaNode.IsActive, IsWritable: metaNode.IsWriteAble(), MediaType: proto.MediaType_Unspecified,
			ForbidWriteOpOfProtoVer0: metaNode.ReceivedForbidWriteOpOfProtoVer0,
		})
		return true
	})
	return
}

func (c *Cluster) allFlashNodes() (flashNodes []proto.NodeView) {
	flashNodes = make([]proto.NodeView, 0)
	c.flashNodeTopo.flashNodeMap.Range(func(addr, node interface{}) bool {
		flashNode := node.(*FlashNode)
		isWritable := flashNode.isWriteable()
		flashNode.RLock()
		flashNodes = append(flashNodes, proto.NodeView{
			ID:         flashNode.ID,
			Addr:       flashNode.Addr,
			Status:     flashNode.IsActive,
			IsWritable: isWritable,
		})
		flashNode.RUnlock()
		return true
	})
	return
}

// get metaNode with specified condition
func (c *Cluster) getSpecifiedMetaNodes(zones map[string]struct{}, nodeSetIds map[uint64]struct{}) (metaNodes []*MetaNode) {
	log.LogInfof("cluster metaNode length:%v", c.allMetaNodes())
	// if nodeSetId is set,choose metaNode which in nodesetId and ignore zones
	if len(nodeSetIds) != 0 {
		log.LogInfof("select from nodeSet")
		c.metaNodes.Range(func(addr, node interface{}) bool {
			metaNode := node.(*MetaNode)
			if _, ok := nodeSetIds[metaNode.NodeSetID]; ok {
				metaNodes = append(metaNodes, metaNode)
			}
			return true
		})
		return
	}

	// if zones is set, choose metaNodes which in zones
	if len(zones) != 0 {
		log.LogInfof("select from zone")
		c.metaNodes.Range(func(addr, node interface{}) bool {
			metaNode := node.(*MetaNode)
			if _, ok := zones[metaNode.ZoneName]; ok {
				metaNodes = append(metaNodes, metaNode)
			}
			return true
		})
		return
	}

	log.LogInfof("select all cluster metaNode")
	// get all metaNodes in cluster
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, metaNode)
		return true
	})

	return
}

func (c *Cluster) balanceMetaPartitionLeader(zones map[string]struct{}, nodeSetIds map[uint64]struct{}) error {
	sortedNodes := c.getSortLeaderMetaNodes(zones, nodeSetIds)
	if sortedNodes == nil || len(sortedNodes.nodes) == 0 {
		return errors.New("no metaNode be selected")
	}

	sortedNodes.balanceLeader()

	return nil
}

func (c *Cluster) getSortLeaderMetaNodes(zones map[string]struct{}, nodeSetIds map[uint64]struct{}) *sortLeaderMetaNode {
	metaNodes := c.getSpecifiedMetaNodes(zones, nodeSetIds)
	log.LogInfof("metaNode length:%d", len(metaNodes))
	if len(metaNodes) == 0 {
		return nil
	}

	leaderNodes := make([]*LeaderMetaNode, 0)
	countM := make(map[string]int)
	totalCount := 0
	average := 0
	for _, node := range metaNodes {
		metaPartitions := make([]*MetaPartition, 0)
		for _, mp := range node.metaPartitionInfos {
			if mp.IsLeader {
				metaPartition, err := c.getMetaPartitionByID(mp.PartitionID)
				if err != nil {
					continue
				}
				metaPartitions = append(metaPartitions, metaPartition)
			}
		}

		// some metaNode's mps length could be 0
		leaderNodes = append(leaderNodes, &LeaderMetaNode{
			metaPartitions: metaPartitions,
			addr:           node.Addr,
		})
		countM[node.Addr] = len(metaPartitions)
		totalCount += len(metaPartitions)
	}
	if len(leaderNodes) != 0 {
		average = totalCount / len(leaderNodes)
	}
	s := &sortLeaderMetaNode{
		nodes:        leaderNodes,
		leaderCountM: countM,
		average:      average,
	}
	sort.Sort(s)
	return s
}

func (c *Cluster) allVolNames() (vols []string) {
	vols = make([]string, 0)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for name := range c.vols {
		vols = append(vols, name)
	}
	return
}

func (c *Cluster) copyVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()

	for name, vol := range c.vols {
		vols[name] = vol
	}

	return
}

// Return all the volumes except the ones that have been marked to be deleted.
func (c *Cluster) allVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for name, vol := range c.vols {
		if vol.Status == proto.VolStatusNormal || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden) {
			vols[name] = vol
		}
	}
	return
}

func (c *Cluster) getDataPartitionCount() (count int) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for _, vol := range c.vols {
		count = count + len(vol.dataPartitions.partitions)
	}
	return
}

func (c *Cluster) getMetaPartitionCount() (count int) {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.mpsLock.RLock()
		count = count + len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
	}
	return count
}

func (c *Cluster) setClusterInfo(dirLimit uint32) (err error) {
	oldLimit := c.cfg.DirChildrenNumLimit
	atomic.StoreUint32(&c.cfg.DirChildrenNumLimit, dirLimit)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterInfo] err[%v]", err)
		atomic.StoreUint32(&c.cfg.DirChildrenNumLimit, oldLimit)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) getMonitorPushAddr() (addr string) {
	addr = c.cfg.MonitorPushAddr
	return
}

func (c *Cluster) setMetaNodeThreshold(threshold float32) (err error) {
	if threshold > 1.0 || threshold < 0.0 {
		err = fmt.Errorf("set threshold failed: threshold (%v) should between 0.0 and 1.0", threshold)
		return
	}
	oldThreshold := c.cfg.MetaNodeThreshold
	c.cfg.MetaNodeThreshold = threshold
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeThreshold] err[%v]", err)
		c.cfg.MetaNodeThreshold = oldThreshold
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMasterVolDeletionDelayTime(volDeletionDelayTimeHour int) (err error) {
	oldVolDeletionDelayTimeHour := c.cfg.volDelayDeleteTimeHour
	c.cfg.volDelayDeleteTimeHour = int64(volDeletionDelayTimeHour)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMasterVolDeletionDelayTime] err[%v]", err)
		c.cfg.volDelayDeleteTimeHour = oldVolDeletionDelayTimeHour
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeDeleteBatchCount(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.MetaNodeDeleteBatchCount)
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeDeleteBatchCount] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClusterLoadFactor(factor float32) (err error) {
	oldVal := c.cfg.ClusterLoadFactor
	c.cfg.ClusterLoadFactor = factor
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterLoadFactorErr] err[%v]", err)
		c.cfg.ClusterLoadFactor = oldVal
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeDeleteLimitRate(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DataNodeDeleteLimitRate)
	atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeDeleteLimitRate] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataPartitionMaxRepairErrCnt(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DpMaxRepairErrCnt)
	atomic.StoreUint64(&c.cfg.DpMaxRepairErrCnt, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataPartitionMaxRepairErrCnt] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DpMaxRepairErrCnt, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataPartitionRepairTimeOut(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DpRepairTimeOut)
	atomic.StoreUint64(&c.cfg.DpRepairTimeOut, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataPartitionRepairTimeOut] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DpRepairTimeOut, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataPartitionBackupTimeOut(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DpBackupTimeOut)
	if val < uint64(proto.DefaultDataPartitionBackupTimeOut/time.Second) {
		val = uint64(proto.DefaultDataPartitionBackupTimeOut / time.Second)
	}
	atomic.StoreUint64(&c.cfg.DpBackupTimeOut, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataPartitionBackupTimeOut] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DpBackupTimeOut, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeAutoRepairLimitRate(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DataNodeAutoRepairLimitRate)
	atomic.StoreUint64(&c.cfg.DataNodeAutoRepairLimitRate, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeAutoRepairLimitRate] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DataNodeAutoRepairLimitRate, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataPartitionTimeout(val int64) (err error) {
	oldVal := atomic.LoadInt64(&c.cfg.DataPartitionTimeOutSec)
	atomic.StoreInt64(&c.cfg.DataPartitionTimeOutSec, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setDataPartitionTimeout] failed to set dp timeout, err(%v)", err)
		atomic.StoreInt64(&c.cfg.DataPartitionTimeOutSec, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeDeleteWorkerSleepMs(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs)
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeDeleteWorkerSleepMs] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) getMaxDpCntLimit() (dpCntInLimit uint64) {
	dpCntInLimit = atomic.LoadUint64(&clusterDpCntLimit)
	return
}

func (c *Cluster) setMaxDpCntLimit(val uint64) (err error) {
	if val == 0 {
		val = defaultMaxDpCntLimit
	}
	oldVal := c.getMaxDpCntLimit()
	// atomic.StoreUint64(&c.cfg.MaxDpCntLimit, val)
	atomic.StoreUint64(&clusterDpCntLimit, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[MaxDpCntLimit] err[%v]", err)
		atomic.StoreUint64(&clusterDpCntLimit, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) getMaxMpCntLimit() (mpCntLimit uint64) {
	mpCntLimit = atomic.LoadUint64(&clusterMpCntLimit)
	return
}

func (c *Cluster) setMaxMpCntLimit(val uint64) (err error) {
	if val == 0 {
		val = defaultMaxMpCntLimit
	}
	oldVal := c.getMaxMpCntLimit()
	// atomic.StoreUint64(&c.cfg.MaxMpCntLimit, val)
	atomic.StoreUint64(&clusterMpCntLimit, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setMaxMpCntLimit] failed to set mp limit to value(%v), err(%v)", val, err)
		atomic.StoreUint64(&clusterMpCntLimit, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMarkDiskBrokenThreshold(val float64) (err error) {
	if val <= 0 || val > 1 {
		val = defaultMarkDiskBrokenThreshold
	}
	oldVal := c.MarkDiskBrokenThreshold.Load()
	c.MarkDiskBrokenThreshold.Store(val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setMarkDiskBrokenThreshold] failed to set mark disk broken threshold, err(%v)", err)
		c.MarkDiskBrokenThreshold.Store(oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) getMarkDiskBrokenThreshold() (v float64) {
	v = c.MarkDiskBrokenThreshold.Load()
	if v < 0 || v > 1 {
		v = defaultMarkDiskBrokenThreshold
	}
	return
}

func (c *Cluster) setEnableAutoDpMetaRepair(val bool) (err error) {
	oldVal := c.EnableAutoDpMetaRepair.Load()
	c.EnableAutoDpMetaRepair.Store(val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setEnableAutoDpMetaRepair] failed to set enable auto dp meta, err(%v)", err)
		c.EnableAutoDpMetaRepair.Store(oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) getDataPartitionTimeoutSec() (val int64) {
	val = atomic.LoadInt64(&c.cfg.DataPartitionTimeOutSec)
	if val == 0 {
		val = defaultDataPartitionTimeOutSec
	}
	return
}

func (c *Cluster) getEnableAutoDpMetaRepair() (v bool) {
	v = c.EnableAutoDpMetaRepair.Load()
	return
}

func (c *Cluster) setClusterCreateTime(createTime int64) (err error) {
	oldVal := c.CreateTime
	c.CreateTime = createTime
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterCreateTime] err[%v]", err)
		c.CreateTime = oldVal
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDisableAutoAllocate(disableAutoAllocate bool) (err error) {
	oldFlag := c.DisableAutoAllocate
	c.DisableAutoAllocate = disableAutoAllocate
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDisableAutoAllocate] err[%v]", err)
		c.DisableAutoAllocate = oldFlag
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setForbidMpDecommission(isForbid bool) (err error) {
	oldFlag := c.ForbidMpDecommission
	c.ForbidMpDecommission = isForbid
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setForbidMpDecommission] err[%v]", err)
		c.ForbidMpDecommission = oldFlag
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setForbidWriteOpOfProtoVersion0(forbid bool) (err error) {
	oldVal := c.cfg.forbidWriteOpOfProtoVer0

	if forbid == oldVal {
		log.LogInfof("[setForbidWriteOpOfProtoVersion0] value not change: %v", forbid)
		return
	}

	c.cfg.forbidWriteOpOfProtoVer0 = forbid
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setForbidWriteOpOfProtoVersion0] persist err: %v", err)
		c.cfg.forbidWriteOpOfProtoVer0 = oldVal
		err = proto.ErrPersistenceByRaft
		return
	}
	log.LogInfof("[setForbidWriteOpOfProtoVersion0] changed to: %v", forbid)
	return
}

func (c *Cluster) clearVols() {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	vols := c.vols
	go func() {
		for _, vol := range vols {
			vol.qosManager.stop()
		}
	}()
	c.vols = make(map[string]*Vol)
}

func (c *Cluster) clearTopology() {
	c.t.clear()
}

func (c *Cluster) clearDataNodes() {
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		c.dataNodes.Delete(key)
		dataNode.clean()
		return true
	})
}

func (c *Cluster) clearMetaNodes() {
	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		c.metaNodes.Delete(key)
		metaNode.clean()
		return true
	})
}

func (c *Cluster) scheduleToCheckDecommissionDataNode() {
	c.runTask(&cTask{
		tickTime: 10 * time.Second,
		name:     "scheduleToCheckDecommissionDataNode",
		function: func() (fin bool) {
			if c.partition.IsRaftLeader() && c.metaReady {
				c.checkDecommissionDataNode()
			}
			return
		},
	})
}

func (c *Cluster) checkDecommissionDataNode() {
	// decommission datanode mark
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNode.updateDecommissionStatus(c, false, true)
		if dataNode.GetDecommissionStatus() == markDecommission {
			c.TryDecommissionDataNode(dataNode)
		} else if dataNode.GetDecommissionStatus() == DecommissionSuccess {
			partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
			// if only decommission part of data partitions, do not remove the data node
			if len(partitions) != 0 {
				// if time.Now().Sub(time.Unix(dataNode.DecommissionCompleteTime, 0)) > (20 * time.Minute) {
				//	log.LogWarnf("action[checkDecommissionDataNode] dataNode %v decommission completed, "+
				//		"but has dp left, so only reset decommission status", dataNode.Addr)
				//	dataNode.resetDecommissionStatus()
				//	c.syncUpdateDataNode(dataNode)
				// }
				return true
			}
			// maybe has decommission failed dp
			failedPartitions := c.getAllDecommissionDataPartitionByDataNode(dataNode.Addr)
			if len(failedPartitions) != 0 {
				return true
			}
			if err := c.syncDeleteDataNode(dataNode); err != nil {
				msg := fmt.Sprintf("action[checkDecommissionDataNode],clusterID[%v] Node[%v] syncDeleteDataNode failed,err[%v]",
					c.Name, dataNode.Addr, err)
				log.LogWarnf("%s", msg)
			} else {
				msg := fmt.Sprintf("del dataNode %v", dataNode.Addr)
				log.LogWarnf("action[checkDecommissionDataNode] %v", msg)
				dataNode.delDecommissionDiskFromCache(c)
				c.delDataNodeFromCache(dataNode)
				auditlog.LogMasterOp("DataNodeDecommission", msg, nil)
			}
		}
		return true
	})
}

func (c *Cluster) TryDecommissionDataNode(dataNode *DataNode) {
	var (
		toBeOffLinePartitions []*DataPartition
		err                   error
	)
	log.LogDebugf("action[TryDecommissionDataNode] dataNode [%s] limit[%v]", dataNode.Addr, dataNode.DecommissionLimit)
	dataNode.MigrateLock.Lock()
	defer func() {
		dataNode.MigrateLock.Unlock()
		if err != nil {
			log.LogErrorf("action[TryDecommissionDataNode] dataNode [%s] failed:err %v", dataNode.Addr, err)
			dataNode.SetDecommissionStatus(DecommissionFail)
		}
		c.syncUpdateDataNode(dataNode)
	}()
	// recover from stop
	if len(dataNode.DecommissionDiskList) != 0 {
		for _, disk := range dataNode.DecommissionDiskList {
			key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
			// if not found, may already success, so only care running disk
			if value, ok := c.DecommissionDisks.Load(key); ok {
				dd := value.(*DecommissionDisk)
				if dd.GetDecommissionStatus() == DecommissionPause {
					dd.SetDecommissionStatus(markDecommission)
					log.LogInfof("action[TryDecommissionDataNode] dataNode [%s] restore %v from stop",
						dataNode.Addr, dd.GenerateKey())
				}
			}
		}
		dataNode.SetDecommissionStatus(DecommissionRunning)
		dataNode.ToBeOffline = true
		log.LogDebugf("action[TryDecommissionDataNode] dataNode [%s] recover from DecommissionDiskList", dataNode.Addr)
		return
	}
	log.LogDebugf("action[TryDecommissionDataNode] dataNode [%s]  prepare to decommission", dataNode.Addr)
	var partitions []*DataPartition
	disks := dataNode.getDisks(c)
	for _, disk := range disks {
		partitionsFromDisk := dataNode.badPartitions(disk, c)
		partitions = append(partitions, partitionsFromDisk...)
	}
	// may allocate new dp when dataNode cancel decommission before
	// partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
	if dataNode.DecommissionDstAddr != "" {
		for _, dp := range partitions {
			// two replica can't exist on same node
			if dp.hasHost(dataNode.DecommissionDstAddr) {
				log.LogWarnf("action[TryDecommissionDataNode] skip dp [%v] on both data node", dp.PartitionID)
				continue
			}
			toBeOffLinePartitions = append(toBeOffLinePartitions, dp)
		}
	} else {
		toBeOffLinePartitions = partitions
	}

	if len(toBeOffLinePartitions) <= 0 && len(partitions) != 0 {
		err = fmt.Errorf("DecommissionDataNode no partition can migrate from [%s] to [%s] for replica address conflict",
			dataNode.Addr, dataNode.DecommissionDstAddr)
		log.LogWarnf("action[TryDecommissionDataNode] %v", err.Error())
		dataNode.markDecommissionFail()
		return
	}

	// check decommission dp last time
	oldPartitions := c.getAllDecommissionDataPartitionByDataNode(dataNode.Addr)

	if len(oldPartitions) != 0 {
		toBeOffLinePartitions = mergeDataPartitionArr(toBeOffLinePartitions, oldPartitions)
	}

	if !(dataNode.DecommissionLimit == 0 || dataNode.DecommissionLimit > len(toBeOffLinePartitions)) {
		toBeOffLinePartitions = toBeOffLinePartitions[:dataNode.DecommissionLimit]
	}
	if len(toBeOffLinePartitions) == 0 {
		log.LogWarnf("action[TryDecommissionDataNode]no dp left on dataNode %v, mark decommission success", dataNode.Addr)
		dataNode.markDecommissionSuccess(c)
		return
	}

	// recode dp count in each disk
	dpToDecommissionByDisk := make(map[string]int)
	var (
		toBeOffLinePartitionsFinal    []*DataPartition
		toBeOffLinePartitionsFinalIds []uint64
	)

	// find respond disk
	for _, dp := range toBeOffLinePartitions {
		disk := dp.getReplicaDisk(dataNode.Addr)
		if disk == "" {
			log.LogWarnf("action[TryDecommissionDataNode] cannot find dp replica [%v] on dataNode[%v]",
				dp.PartitionID, dataNode.Addr)
			// master change leader or restart, operation for decommission success dp is not triggered
			if dp.IsDecommissionSuccess() {
				dp.ResetDecommissionStatus()
				dp.setRestoreReplicaStop()
				c.syncUpdateDataPartition(dp)
				continue
			}
			if dp.DecommissionSrcDiskPath == "" {
				dp.ResetDecommissionStatus()
				dp.setRestoreReplicaStop()
				c.syncUpdateDataPartition(dp)
				log.LogWarnf("action[TryDecommissionDataNode] cannot find DecommissionSrcDiskPath for "+
					"dp replica [%v] on dataNode[%v],reset decommission status",
					dp.PartitionID, dataNode.Addr)
				continue
			}
			// dp decommission failed with decommission src replica is deleted
			toBeOffLinePartitionsFinal = append(toBeOffLinePartitionsFinal, dp)
			toBeOffLinePartitionsFinalIds = append(toBeOffLinePartitionsFinalIds, dp.PartitionID)
			dpToDecommissionByDisk[dp.DecommissionSrcDiskPath]++
		} else {
			toBeOffLinePartitionsFinal = append(toBeOffLinePartitionsFinal, dp)
			toBeOffLinePartitionsFinalIds = append(toBeOffLinePartitionsFinalIds, dp.PartitionID)
			dpToDecommissionByDisk[disk]++
		}
	}
	if len(toBeOffLinePartitionsFinal) == 0 {
		dataNode.markDecommissionSuccess(c)
		return
	}
	if len(dpToDecommissionByDisk) == 0 {
		err = fmt.Errorf("no dp replica can be found on %v, partitions %v",
			dataNode.Addr, toBeOffLinePartitionsFinalIds)
		log.LogWarnf("action[TryDecommissionDataNode] %v", err.Error())
		dataNode.markDecommissionFail()
		return
	}

	decommissionDpTotal := 0
	left := len(toBeOffLinePartitionsFinal)
	decommissionDiskList := make([]string, 0)
	log.LogInfof("action[TryDecommissionDataNode] try decommission dp[%v] %v from dataNode[%s] ",
		len(toBeOffLinePartitionsFinalIds), toBeOffLinePartitionsFinalIds, dataNode.Addr)
	for _, persistDisk := range dataNode.AllDisks {
		if _, ok := dpToDecommissionByDisk[persistDisk]; !ok {
			c.addAndSyncDecommissionedDisk(dataNode, persistDisk)
			msg := fmt.Sprintf("no dp left on %v_%v, disable it directly", dataNode.Addr, persistDisk)
			auditlog.LogMasterOp("DiskDecommission", msg, nil)
			log.LogInfof("action[TryDecommissionDataNode] %v ", msg)
		}
	}
	for disk, dpCnt := range dpToDecommissionByDisk {
		//
		if left == 0 {
			break
		}
		if left-dpCnt >= 0 {
			err = c.migrateDisk(dataNode, disk, dataNode.DecommissionDstAddr, dataNode.DecommissionRaftForce, dpCnt, true, ManualDecommission)
			if err != nil {
				if strings.Contains(err.Error(), "still on working") {
					decommissionDiskList = append(decommissionDiskList, disk)
					log.LogWarnf("action[TryDecommissionDataNode] disk(%v_%v) is decommissioning,add it to "+
						"decommissionDiskList", dataNode.Addr, disk)
				}
				msg := fmt.Sprintf("disk(%v_%v)failed to mark decommission", dataNode.Addr, disk)
				log.LogWarnf("action[TryDecommissionDataNode] %v failed %v", msg, err)
				auditlog.LogMasterOp("DiskDecommission", msg, err)
				continue
			}
			decommissionDpTotal += dpCnt
			left = left - dpCnt
		} else {
			err = c.migrateDisk(dataNode, disk, dataNode.DecommissionDstAddr, dataNode.DecommissionRaftForce, left, true, ManualDecommission)
			if err != nil {
				if strings.Contains(err.Error(), "still on working") {
					decommissionDiskList = append(decommissionDiskList, disk)
					log.LogWarnf("action[TryDecommissionDataNode] disk(%v_%v) is decommissioning,add it to "+
						"decommissionDiskList", dataNode.Addr, disk)
				}
				msg := fmt.Sprintf("disk(%v_%v)failed to mark decommission", dataNode.Addr, disk)
				log.LogWarnf("action[TryDecommissionDataNode] %v failed %v", msg, err)
				auditlog.LogMasterOp("DiskDecommission", msg, err)
				continue
			}
			decommissionDpTotal += left
			left = 0
		}
		decommissionDiskList = append(decommissionDiskList, disk)
	}
	// put all dp to nodeset's decommission list
	// for _, dp := range toBeOffLinePartitions {
	//	dp.MarkDecommissionStatus(dataNode.Addr, dataNode.DecommissionDstAddr, "",
	//		dataNode.DecommissionRaftForce, dataNode.DecommissionTerm, c)
	//	c.syncUpdateDataPartition(dp)
	//	ns.AddToDecommissionDataPartitionList(dp)
	//	toBeOffLinePartitionIds = append(toBeOffLinePartitionIds, dp.PartitionID)
	// }
	// disk wait for decommission
	dataNode.SetDecommissionStatus(DecommissionRunning)
	// avoid alloc dp on this node
	dataNode.ToBeOffline = true
	dataNode.DecommissionDiskList = decommissionDiskList
	dataNode.DecommissionDpTotal = decommissionDpTotal
	msg := fmt.Sprintf(" try decommission disk[%v] from dataNode[%s] raftForce [%v] to dst [%v] DecommissionDpTotal[%v]",
		decommissionDiskList, dataNode.Addr, dataNode.DecommissionRaftForce, dataNode.DecommissionDstAddr, dataNode.DecommissionDpTotal)
	log.LogInfof("action[TryDecommissionDataNode] %v", msg)
	auditlog.LogMasterOp("DataNodeDecommission", msg, nil)
}

func (c *Cluster) checkDataNodesMediaTypeForMigrate(srcNode *DataNode, dstAddr string) (err error) {
	var dstNode *DataNode

	if srcNode == nil {
		log.LogErrorf("[checkDataNodesMediaTypeForMigrate] srcNode is nil")
		return
	}

	if dstAddr != "" {
		dstNode, err = c.dataNode(dstAddr)
		if err != nil {
			log.LogErrorf("[CheckDataNodesMediaTypeForMigrate] get dstNode(%v) failed: %v", dstAddr, err.Error())
			return
		}

		if dstNode.MediaType != srcNode.MediaType {
			err = fmt.Errorf("dstNode mediaType(%v) not match srcNode mediaType(%v)",
				proto.MediaTypeString(dstNode.MediaType), proto.MediaTypeString(srcNode.MediaType))
			log.LogErrorf("[CheckDataNodesMediaTypeForMigrate] %v", err.Error())
			return
		}
	}

	return nil
}

func (c *Cluster) checkDataNodeAddrMediaTypeForMigrate(srcAddr, dstAddr string) (err error) {
	var srcNode *DataNode

	srcNode, err = c.dataNode(srcAddr)
	if err != nil {
		log.LogErrorf("[CheckDataNodesMediaTypeForMigrate] get srcNode(%v) failed: %v", srcAddr, err.Error())
		return
	}

	return c.checkDataNodesMediaTypeForMigrate(srcNode, dstAddr)
}

func (c *Cluster) migrateDisk(dataNode *DataNode, diskPath, dstAddr string, raftForce bool, limit int, diskDisable bool, migrateType uint32) (err error) {
	var disk *DecommissionDisk
	nodeAddr := dataNode.Addr
	if dstAddr != "" {
		if err = c.checkDataNodeAddrMediaTypeForMigrate(nodeAddr, dstAddr); err != nil {
			log.LogErrorf("[migrateDisk] check mediaType err: %v", err.Error())
			return
		}
	}

	key := fmt.Sprintf("%s_%s", nodeAddr, diskPath)
	if value, ok := c.DecommissionDisks.Load(key); ok {
		disk = value.(*DecommissionDisk)
		status := disk.GetDecommissionStatus()
		if status == markDecommission || status == DecommissionRunning {
			err = fmt.Errorf("migrate src(%v) diskPath(%v)s still on working, please wait,check or cancel if abnormal",
				nodeAddr, diskPath)
			log.LogWarnf("action[addDecommissionDisk] %v", err)
			return
		}
	} else {
		disk = &DecommissionDisk{
			SrcAddr:                 nodeAddr,
			DiskPath:                diskPath,
			DiskDisable:             diskDisable,
			IgnoreDecommissionDps:   make([]proto.IgnoreDecommissionDP, 0),
			ResidualDecommissionDps: make([]proto.IgnoreDecommissionDP, 0),
		}
		c.DecommissionDisks.Store(disk.GenerateKey(), disk)
	}
	disk.Type = migrateType
	disk.DiskDisable = diskDisable
	disk.ResidualDecommissionDps = make([]proto.IgnoreDecommissionDP, 0)
	disk.IgnoreDecommissionDps = make([]proto.IgnoreDecommissionDP, 0)
	// disk should be decommission all the dp
	disk.markDecommission(dstAddr, raftForce, limit)
	if err = c.syncAddDecommissionDisk(disk); err != nil {
		err = fmt.Errorf("action[addDecommissionDisk],clusterID[%v] dataNodeAddr:%v diskPath:%v err:%v ",
			c.Name, nodeAddr, diskPath, err.Error())
		Warn(c.Name, err.Error())
		c.delDecommissionDiskFromCache(disk)
		return
	}
	if disk.DiskDisable {
		c.addAndSyncDecommissionedDisk(dataNode, disk.DiskPath)
	}
	// add to the nodeset decommission list
	c.addDecommissionDiskToNodeset(disk)
	log.LogInfof("action[addDecommissionDisk],clusterID[%v] add disk[%v]", c.Name, disk.decommissionInfo())
	return
}

func (c *Cluster) restoreStoppedAutoDecommissionDisk(nodeAddr, diskPath string) (err error) {
	var disk *DecommissionDisk
	key := fmt.Sprintf("%s_%s", nodeAddr, diskPath)

	if value, ok := c.DecommissionDisks.Load(key); !ok {
		disk = value.(*DecommissionDisk)
	} else {
		return errors.NewErrorf("cannot find auto decommission disk %v", key)
	}
	if disk.GetDecommissionStatus() != DecommissionPause {
		err = fmt.Errorf("decommission disk [%v]is not stopped: %v", key, disk.GetDecommissionStatus())
		log.LogWarnf("action[restoreStoppedAutoDecommissionDisk] %v", err)
		return
	}
	if disk.IsManualDecommissionDisk() {
		err = fmt.Errorf("decommission disk [%v]is not manual decommission type: %v", key, disk.Type)
		log.LogWarnf("action[restoreStoppedAutoDecommissionDisk] %v", err)
		return
	}

	disk.SetDecommissionStatus(markDecommission)
	c.syncAddDecommissionDisk(disk)
	log.LogInfof("action[restoreStoppedAutoDecommissionDisk],clusterID[%v] dataNodeAddr:%v,diskPath[%v] ",
		c.Name, nodeAddr, diskPath)
	return
}

func (c *Cluster) scheduleToCheckDecommissionDisk() {
	c.runTask(&cTask{
		tickTime: 10 * time.Second,
		name:     "scheduleToCheckDecommissionDisk",
		function: func() (fin bool) {
			if c.partition.IsRaftLeader() && c.metaReady {
				c.checkDecommissionDisk()
			}
			return
		},
	})
}

func (c *Cluster) checkDecommissionDisk() {
	// decommission disk mark
	c.DecommissionDisks.Range(func(key, value interface{}) bool {
		disk := value.(*DecommissionDisk)
		status := disk.GetDecommissionStatus()
		// keep failed decommission disk in list for preventing the reuse of a
		// term in future decommissioning operations
		if status == DecommissionSuccess {
			if time.Since(time.Unix(disk.DecommissionCompleteTime, 0)) > (120 * time.Hour) {
				if err := c.syncDeleteDecommissionDisk(disk); err != nil {
					msg := fmt.Sprintf("action[checkDecommissionDisk],clusterID[%v] node[%v] disk[%v],"+
						"syncDeleteDecommissionDisk failed,err[%v]",
						c.Name, disk.SrcAddr, disk.DiskPath, err)
					log.LogWarnf("%s", msg)
				} else {
					c.delDecommissionDiskFromCache(disk)
					log.LogDebugf("action[checkDecommissionDisk] delete DecommissionDisk[%s] status(%v)",
						disk.GenerateKey(), status)
				}
			}
		}
		return true
	})
}

func (c *Cluster) scheduleToBadDisk() {
	task := &cTask{tickTime: 5 * time.Second, name: "scheduleToCheckDelayDeleteVols"}
	task.function = func() (fin bool) {
		if c.partition.IsRaftLeader() && c.AutoDecommissionDiskIsEnabled() && c.metaReady {
			c.checkBadDisk()
		}
		task.tickTime = c.GetAutoDecommissionDiskInterval()
		return
	}
	c.runTask(task)
}

func (c *Cluster) canAutoDecommissionDisk(addr string, diskPath string) (yes bool, status uint32) {
	key := fmt.Sprintf("%s_%s", addr, diskPath)
	if value, ok := c.DecommissionDisks.Load(key); ok {
		d := value.(*DecommissionDisk)
		status = d.GetDecommissionStatus()
		yes = status != markDecommission && status != DecommissionRunning && status != DecommissionPause && status != DecommissionCancel
		return
	}
	yes = true
	return
}

func (c *Cluster) handleDataNodeBadDisk(dataNode *DataNode) {
	badDisks := make([]proto.BadDiskStat, 0)
	dataNode.RLock()
	badDisks = append(badDisks, dataNode.BadDiskStats...)
	dataNode.RUnlock()

	for _, disk := range badDisks {
		// TODO:no dp left on bad disk, notify sre to remove this disk
		// decommission failed, but lack replica for disk err dp is already removed
		retry := c.RetryDecommissionDisk(dataNode.Addr, disk.DiskPath)
		if disk.TotalPartitionCnt == 0 && !retry {
			// msg := fmt.Sprintf("disk(%v_%v) can be removed", dataNode.Addr, disk.DiskPath)
			// auditlog.LogMasterOp("DiskDecommission", msg, nil)
			continue
		}
		var ratio float64
		if disk.TotalPartitionCnt != 0 {
			ratio = float64(len(disk.DiskErrPartitionList)) / float64(disk.TotalPartitionCnt)
		} else {
			ratio = 0
		}
		log.LogDebugf("[handleDataNodeBadDisk] data node(%v) bad disk(%v), bad dp cnt (%v) total dp cnt(%v) ratio(%v) retry(%v)",
			dataNode.Addr, disk.DiskPath, len(disk.DiskErrPartitionList), disk.TotalPartitionCnt, ratio, retry)
		// decommission dp form bad disk
		threshold := c.getMarkDiskBrokenThreshold()
		if threshold == defaultMarkDiskBrokenThreshold || ratio >= threshold || retry {
			log.LogInfof("[handleDataNodeBadDisk] try to decommission disk(%v) on %v", disk.DiskPath, dataNode.Addr)
			// NOTE: decommission all dps and disable disk
			ok, status := c.canAutoDecommissionDisk(dataNode.Addr, disk.DiskPath)
			if !ok {
				log.LogWarnf("[handleDataNodeBadDisk] cannnot auto decommission dp on data node(%v) disk(%v) status(%v), skip",
					dataNode.Addr, disk.DiskPath, GetDecommissionStatusMessage(status))
				continue
			}
			err := c.migrateDisk(dataNode, disk.DiskPath, "", false, 0, true, AutoDecommission)
			if err != nil {
				msg := fmt.Sprintf("disk(%v_%v)failed to mark decommission", dataNode.Addr, disk.DiskPath)
				auditlog.LogMasterOp("DiskDecommission", msg, err)
				log.LogErrorf("[handleDataNodeBadDisk]%v, err(%v)", msg, err)
			}
		} else {
			for _, dpId := range disk.DiskErrPartitionList {
				log.LogDebugf("[handleDataNodeBadDisk] try to decommission dp(%v)", dpId)
				dp, err := c.getDataPartitionByID(dpId)
				if err != nil {
					log.LogErrorf("[handleDataNodeBadDisk] failed to get data node(%v) dp(%v), err(%v)", dataNode.Addr, dpId, err)
					continue
				}
				// NOTE: replica not found, maybe decommissioned
				if _, err = dp.getReplica(dataNode.Addr); err != nil {
					log.LogInfof("[handleDataNodeBadDisk] data node(%v) not found in dp(%v) maybe decommissioned?", dataNode.Addr, dpId)
					continue
				}
				err = c.markDecommissionDataPartition(dp, dataNode, false, AutoDecommission)
				if err != nil {
					log.LogErrorf("[handleDataNodeBadDisk] failed to decommssion dp(%v) on data node(%v) disk(%v), err(%v)", dataNode.Addr, disk.DiskPath, dp.PartitionID, err)
					continue
				}
			}
		}
	}
}

func (c *Cluster) checkBadDisk() {
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		c.handleDataNodeBadDisk(dataNode)
		return true
	})
}

func (c *Cluster) TryDecommissionDisk(disk *DecommissionDisk) {
	var (
		node                *DataNode
		err                 error
		badPartitionIds     []uint64
		lastBadPartitionIds []uint64
		// tmpIds              []uint64
		badPartitions []*DataPartition
		rstMsg        string
		zone          *Zone
		ns            *nodeSet
	)
	defer func() {
		if err != nil {
			disk.DecommissionTimes++
		}
		auditlog.LogMasterOp("DiskDecommission", rstMsg, err)
		c.syncUpdateDecommissionDisk(disk)
	}()
	if node, err = c.dataNode(disk.SrcAddr); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] cannot find dataNode[%s]", disk.SrcAddr)
		disk.markDecommissionFailed()
		return
	}
	badPartitions = node.badPartitions(disk.DiskPath, c)
	for _, dp := range badPartitions {
		badPartitionIds = append(badPartitionIds, dp.PartitionID)
	}
	log.LogInfof("action[TryDecommissionDisk] disk[%v_%v] len(%v) badPartitionIds %v",
		node.Addr, disk.DiskPath, len(badPartitionIds), badPartitionIds)
	badPartitionIds = badPartitionIds[:0]
	// check decommission dp last time
	lastBadPartitions := c.getAllDecommissionDataPartitionByDisk(disk.SrcAddr, disk.DiskPath)
	for _, dp := range lastBadPartitions {
		lastBadPartitionIds = append(lastBadPartitionIds, dp.PartitionID)
	}
	log.LogInfof("action[TryDecommissionDisk] disk[%v_%v] len(%v) lastBadPartitionIds %v",
		node.Addr, disk.DiskPath, len(lastBadPartitionIds), lastBadPartitionIds)

	badPartitions = mergeDataPartitionArr(badPartitions, lastBadPartitions)
	log.LogDebugf("[TryDecommissionDisk] data node(%v) disk(%v) bad dps(%v)", node.Addr, disk.DiskPath, len(badPartitions))
	if len(badPartitions) == 0 {
		log.LogInfof("action[TryDecommissionDisk] receive decommissionDisk node[%v] "+
			"no any partitions on disk[%v],offline successfully",
			node.Addr, disk.DiskPath)
		rstMsg = fmt.Sprintf("no any partitions on disk[%v],offline successfully", disk.decommissionInfo())
		disk.markDecommissionSuccess()
		disk.DecommissionDpTotal = 0
		if disk.DiskDisable {
			c.addAndSyncDecommissionedDisk(node, disk.DiskPath)
		}
		return
	}
	// tmpIds = tmpIds[:0]
	// for _, dp := range badPartitions {
	//	tmpIds = append(tmpIds, dp.PartitionID)
	// }
	// log.LogInfof("action[TryDecommissionDisk] disk[%v_%v] tmpIds %v",
	//	node.Addr, disk.DiskPath, tmpIds)
	// log.LogInfof("action[TryDecommissionDisk] disk[%v_%v] DecommissionDpCount %v",
	//	node.Addr, disk.DiskPath, disk.DecommissionDpCount)
	// recover from pause
	if disk.DecommissionDpTotal != InvalidDecommissionDpCnt {
		badPartitions = lastBadPartitions
	} else { // the first time for decommission
		if disk.DecommissionDpCount == 0 || disk.DecommissionDpCount > len(badPartitions) {
			disk.DecommissionDpTotal = len(badPartitions)
		} else {
			disk.DecommissionDpTotal = disk.DecommissionDpCount
			badPartitions = badPartitions[:disk.DecommissionDpCount]
		}
	}

	if zone, err = c.t.getZone(node.ZoneName); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] zone failed[%v]",
			node.Addr, err.Error())
		disk.markDecommissionFailed()
		return
	}
	if ns, err = zone.getNodeSet(node.NodeSetID); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] nodeset[%v] failed[%v]",
			node.Addr, node.NodeSetID, err.Error())
		disk.markDecommissionFailed()
		return
	}
	var ignoreIDs []uint64
	IgnoreDecommissionDps := make([]proto.IgnoreDecommissionDP, 0)
	for _, dp := range badPartitions {
		// dp with decommission success cannot be reset during master load metadata
		if dp.IsDecommissionSuccess() && dp.DecommissionTerm == disk.DecommissionTerm {
			log.LogInfof("action[TryDecommissionDisk] reset dp [%v] decommission status for disk %v:%v",
				dp.PartitionID, disk.SrcAddr, disk.DiskPath)
			dp.ResetDecommissionStatus()
			dp.setRestoreReplicaStop()
			c.syncUpdateDataPartition(dp)
			disk.DecommissionDpTotal -= 1
			ignoreIDs = append(ignoreIDs, dp.PartitionID)
			continue
		}
		if err = dp.MarkDecommissionStatus(node.Addr, disk.DstAddr, disk.DiskPath, disk.DecommissionRaftForce,
			disk.DecommissionTerm, disk.Type, c, ns); err != nil {
			if strings.Contains(err.Error(), proto.ErrDecommissionDiskErrDPFirst.Error()) {
				c.syncUpdateDataPartition(dp)
				// still decommission dp but not involved in the calculation of the decommission progress.
				// disk.DecommissionDpTotal -= 1
				ns.AddToDecommissionDataPartitionList(dp, c)
				ignoreIDs = append(ignoreIDs, dp.PartitionID)
				IgnoreDecommissionDps = append(IgnoreDecommissionDps, proto.IgnoreDecommissionDP{
					PartitionID: dp.PartitionID,
					ErrMsg:      proto.ErrDecommissionDiskErrDPFirst.Error(),
				})
				continue
			} else if strings.Contains(err.Error(), proto.ErrPerformingDecommission.Error()) {
				if dp.DecommissionSrcAddr != node.Addr {
					// disk.DecommissionDpTotal -= 1
					ignoreIDs = append(ignoreIDs, dp.PartitionID)
					log.LogWarnf("action[TryDecommissionDisk] disk(%v) dp(%v) is decommissioning",
						disk.decommissionInfo(), dp.PartitionID)
					IgnoreDecommissionDps = append(IgnoreDecommissionDps, proto.IgnoreDecommissionDP{
						PartitionID: dp.PartitionID,
						ErrMsg:      proto.ErrPerformingDecommission.Error(),
					})
					continue
				} else {
					log.LogDebugf("action[TryDecommissionDisk] disk(%v) dp(%v) may be mark decommission before leader change",
						disk.decommissionInfo(), dp.PartitionID)
					ns.AddToDecommissionDataPartitionList(dp, c)
				}
			} else if strings.Contains(err.Error(), proto.ErrWaitForAutoAddReplica.Error()) {
				ignoreIDs = append(ignoreIDs, dp.PartitionID)
				log.LogWarnf("action[TryDecommissionDisk] disk(%v) dp(%v) is auto add replica",
					disk.decommissionInfo(), dp.PartitionID)
				IgnoreDecommissionDps = append(IgnoreDecommissionDps, proto.IgnoreDecommissionDP{
					PartitionID: dp.PartitionID,
					ErrMsg:      proto.ErrPerformingDecommission.Error(),
				})
				continue
			} else {
				// mark as failed and set decommission src, make sure it can be included in the calculation of progress
				dp.DecommissionSrcAddr = node.Addr
				dp.DecommissionSrcDiskPath = disk.DiskPath
				dp.markRollbackFailed(false)
				dp.DecommissionErrorMessage = err.Error()
				dp.DecommissionTerm = disk.DecommissionTerm
				log.LogWarnf("action[TryDecommissionDisk] disk(%v) set dp(%v) DecommissionTerm %v",
					disk.decommissionInfo(), dp.PartitionID, disk.DecommissionTerm)
			}
		} else {
			ns.AddToDecommissionDataPartitionList(dp, c)
		}
		c.syncUpdateDataPartition(dp)
		badPartitionIds = append(badPartitionIds, dp.PartitionID)
	}
	disk.SetDecommissionStatus(DecommissionRunning)
	disk.IgnoreDecommissionDps = IgnoreDecommissionDps
	rstMsg = fmt.Sprintf("disk[%v] badPartitionIds %v offline successfully, ignore (%v) %v",
		disk.decommissionInfo(), badPartitionIds, len(ignoreIDs), ignoreIDs)
	log.LogInfof("action[TryDecommissionDisk] %s", rstMsg)
}

func (c *Cluster) getAllDecommissionDataPartitionByDataNode(addr string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) getAllDecommissionDataPartitionByDiskAndTerm(addr, disk string, term uint64) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr && dp.DecommissionSrcDiskPath == disk && dp.DecommissionTerm == term {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) getAllDecommissionDataPartitionByDisk(addr, disk string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr && dp.DecommissionSrcDiskPath == disk {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) listQuotaAll() (volsInfo []*proto.VolInfo) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for _, vol := range c.vols {
		if vol.quotaManager.HasQuota() {
			stat := volStat(vol, false)
			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize,
				stat.UsedSize, stat.DpReadOnlyWhenVolFull)
			volsInfo = append(volsInfo, volInfo)
		}
	}

	return
}

func mergeDataPartitionArr(newDps, oldDps []*DataPartition) []*DataPartition {
	ret := make([]*DataPartition, 0)
	tempMap := make(map[uint64]bool)
	for _, v := range newDps {
		ret = append(ret, v)
		tempMap[v.PartitionID] = true
	}
	for _, v := range oldDps {
		if !tempMap[v.PartitionID] {
			ret = append(ret, v)
			tempMap[v.PartitionID] = true
		}
	}
	return ret
}

func (c *Cluster) generateClusterUuid() (err error) {
	cid := "CID-" + uuid.NewString()
	c.clusterUuid = cid
	if err := c.syncPutCluster(); err != nil {
		c.clusterUuid = ""
		return errors.NewErrorf(fmt.Sprintf("syncPutCluster failed %v", err.Error()))

	}
	return
}

func (c *Cluster) initAuthentication(cfg *config.Config) {
	var (
		authnodes   []string
		enableHTTPS bool
		certFile    string
	)
	authNodeHostConfig := cfg.GetString(AuthNodeHost)
	authnodes = strings.Split(authNodeHostConfig, ",")

	enableHTTPS = cfg.GetBool(AuthNodeEnableHTTPS)
	if enableHTTPS {
		certFile = cfg.GetString(AuthNodeCertFile)
	}

	c.ac = authSDK.NewAuthClient(authnodes, enableHTTPS, certFile)
}

func (c *Cluster) parseAndCheckClientIDKey(r *http.Request, Type proto.MsgType) (err error) {
	var (
		clientIDKey string
		clientID    string
		clientKey   []byte
	)

	if err = r.ParseForm(); err != nil {
		return
	}

	if clientIDKey, err = extractClientIDKey(r); err != nil {
		return
	}

	if clientID, clientKey, err = proto.ExtractIDAndAuthKey(clientIDKey); err != nil {
		return
	}

	if err = proto.IsValidClientID(clientID); err != nil {
		return
	}

	ticket, err := c.ac.API().GetTicket(clientID, string(clientKey), proto.MasterServiceID)
	if err != nil {
		err = fmt.Errorf("get ticket from auth node failed, clientIDKey[%v], err[%v]", clientIDKey, err.Error())
		return
	}

	_, err = checkTicket(ticket.Ticket, c.MasterSecretKey, Type)
	if err != nil {
		err = fmt.Errorf("check ticket failed, clientIDKey[%v], err[%v]", clientIDKey, err.Error())
		return
	}
	return
}

func (c *Cluster) addLcNode(nodeAddr string) (id uint64, err error) {
	var ln *LcNode
	if value, ok := c.lcNodes.Load(nodeAddr); ok {
		ln = value.(*LcNode)
		ln.ReportTime = time.Now()
		ln.clean()
		ln.TaskManager = newAdminTaskManager(ln.Addr, c.Name)
		log.LogInfof("action[addLcNode] already add nodeAddr: %v, id: %v", nodeAddr, ln.ID)
	} else {
		// allocate LcNode id
		if id, err = c.idAlloc.allocateCommonID(); err != nil {
			goto errHandler
		}
		// allocate id first and then set report time, avoid allocate id taking a long time and check heartbeat timeout
		ln = newLcNode(nodeAddr, c.Name)
		ln.ID = id
		log.LogInfof("action[addLcNode] add nodeAddr: %v, allocateCommonID: %v", nodeAddr, id)
	}

	if err = c.syncAddLcNode(ln); err != nil {
		goto errHandler
	}
	c.lcNodes.Store(nodeAddr, ln)

	c.lcMgr.lcNodeStatus.Lock()
	c.lcMgr.lcNodeStatus.WorkingCount[nodeAddr] = 0
	c.lcMgr.lcNodeStatus.Unlock()

	c.snapshotMgr.lcNodeStatus.Lock()
	c.snapshotMgr.lcNodeStatus.WorkingCount[nodeAddr] = 0
	c.snapshotMgr.lcNodeStatus.Unlock()
	log.LogInfof("action[addLcNode], clusterID[%v], lcNodeAddr: %v, id: %v, success", c.Name, nodeAddr, ln.ID)
	return ln.ID, nil

errHandler:
	err = fmt.Errorf("action[addLcNode], clusterID[%v], lcNodeAddr: %v, err: %v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

type LcNodeStatInfo struct {
	Addr string
}

type LcNodeInfoResponse struct {
	RegisterInfos      []*LcNodeStatInfo
	LcConfigurations   map[string]*proto.LcConfiguration
	LcRuleTaskStatus   lcRuleTaskStatus
	LcNodeStatus       lcNodeStatus
	SnapshotVerStatus  lcSnapshotVerStatus
	SnapshotNodeStatus lcNodeStatus
}

func (c *Cluster) adminLcNodeInfo(vol, rid, done string) (rsp *LcNodeInfoResponse, err error) {
	if vol == "" && rid != "" {
		err = errors.New("err: ruleid must be used with vol")
		return
	}
	if done != "" && done != "true" && done != "false" {
		err = errors.New("err: invalid done")
		return
	}

	rsp = &LcNodeInfoResponse{
		LcRuleTaskStatus: lcRuleTaskStatus{
			ToBeScanned: make(map[string]*proto.RuleTask),
			Results:     make(map[string]*proto.LcNodeRuleTaskResponse),
		},
	}
	var b []byte

	var tid string
	if rid != "" {
		tid = fmt.Sprintf("%s:%s", vol, rid)
	}
	if vol != "" || done != "" {
		tmpLcRuleTaskStatus := lcRuleTaskStatus{}
		c.lcMgr.lcRuleTaskStatus.RLock()
		if b, err = json.Marshal(c.lcMgr.lcRuleTaskStatus); err != nil {
			c.lcMgr.lcRuleTaskStatus.RUnlock()
			return
		}
		c.lcMgr.lcRuleTaskStatus.RUnlock()
		if err = json.Unmarshal(b, &tmpLcRuleTaskStatus); err != nil {
			return
		}

		for k, v := range tmpLcRuleTaskStatus.Results {
			if vol == "" || (vol == v.Volume && (tid == "" || tid == k)) {
				if done == "true" && v.Done {
					rsp.LcRuleTaskStatus.Results[k] = v
					continue
				}
				if done == "false" && !v.Done {
					rsp.LcRuleTaskStatus.Results[k] = v
					continue
				}
				if done == "" {
					rsp.LcRuleTaskStatus.Results[k] = v
				}
			}
		}

		for k, v := range tmpLcRuleTaskStatus.ToBeScanned {
			if vol == "" || (vol == v.VolName && (tid == "" || tid == k)) {
				if done == "" || done == "false" {
					rsp.LcRuleTaskStatus.ToBeScanned[k] = v
				}
			}
		}

		rsp.LcRuleTaskStatus.StartTime = tmpLcRuleTaskStatus.StartTime
		rsp.LcRuleTaskStatus.EndTime = tmpLcRuleTaskStatus.EndTime
		return
	}

	c.lcNodes.Range(func(addr, value interface{}) bool {
		rsp.RegisterInfos = append(rsp.RegisterInfos, &LcNodeStatInfo{
			Addr: addr.(string),
		})
		return true
	})

	log.LogDebug("start get lcConfigurations")
	c.lcMgr.RLock()
	if b, err = json.Marshal(c.lcMgr.lcConfigurations); err != nil {
		c.lcMgr.RUnlock()
		return
	}
	c.lcMgr.RUnlock()
	log.LogDebug("finish get lcConfigurations")
	if err = json.Unmarshal(b, &rsp.LcConfigurations); err != nil {
		return
	}

	log.LogDebug("start get lcRuleTaskStatus")
	c.lcMgr.lcRuleTaskStatus.RLock()
	if b, err = json.Marshal(c.lcMgr.lcRuleTaskStatus); err != nil {
		c.lcMgr.lcRuleTaskStatus.RUnlock()
		return
	}
	c.lcMgr.lcRuleTaskStatus.RUnlock()
	log.LogDebug("finish get lcRuleTaskStatus")
	if err = json.Unmarshal(b, &rsp.LcRuleTaskStatus); err != nil {
		return
	}

	c.lcMgr.lcNodeStatus.RLock()
	if b, err = json.Marshal(c.lcMgr.lcNodeStatus); err != nil {
		c.lcMgr.lcNodeStatus.RUnlock()
		return
	}
	c.lcMgr.lcNodeStatus.RUnlock()
	if err = json.Unmarshal(b, &rsp.LcNodeStatus); err != nil {
		return
	}

	c.snapshotMgr.lcSnapshotTaskStatus.RLock()
	if b, err = json.Marshal(c.snapshotMgr.lcSnapshotTaskStatus); err != nil {
		c.snapshotMgr.lcSnapshotTaskStatus.RUnlock()
		return
	}
	c.snapshotMgr.lcSnapshotTaskStatus.RUnlock()
	if err = json.Unmarshal(b, &rsp.SnapshotVerStatus); err != nil {
		return
	}

	c.snapshotMgr.lcNodeStatus.RLock()
	if b, err = json.Marshal(c.snapshotMgr.lcNodeStatus); err != nil {
		c.snapshotMgr.lcNodeStatus.RUnlock()
		return
	}
	c.snapshotMgr.lcNodeStatus.RUnlock()
	if err = json.Unmarshal(b, &rsp.SnapshotNodeStatus); err != nil {
		return
	}
	return
}

func (c *Cluster) clearLcNodes() {
	c.lcNodes.Range(func(key, value interface{}) bool {
		lcNode := value.(*LcNode)
		c.lcNodes.Delete(key)
		lcNode.clean()
		return true
	})
}

func (c *Cluster) delLcNode(nodeAddr string) (err error) {
	c.lcMgr.lcNodeStatus.RemoveNode(nodeAddr)
	c.snapshotMgr.lcNodeStatus.RemoveNode(nodeAddr)

	lcNode, err := c.lcNode(nodeAddr)
	if err != nil {
		log.LogErrorf("action[delLcNode], clusterID:%v, lcNodeAddr:%v, load err:%v ", c.Name, nodeAddr, err)
		return
	}
	if err = c.syncDeleteLcNode(lcNode); err != nil {
		log.LogErrorf("action[delLcNode], clusterID:%v, lcNodeAddr:%v syncDeleteLcNode err:%v ", c.Name, nodeAddr, err)
		return
	}
	val, loaded := c.lcNodes.LoadAndDelete(nodeAddr)
	log.LogInfof("action[delLcNode], clusterID:%v, lcNodeAddr:%v, LoadAndDelete result val:%v, loaded:%v", c.Name, nodeAddr, val, loaded)
	return
}

func (c *Cluster) scheduleToLcScan() {
	go func() {
		for {
			now := time.Now()
			next := now.Add(time.Hour * 24)
			next = time.Date(next.Year(), next.Month(), next.Day(), c.cfg.StartLcScanTime, 0, 0, 0, next.Location())
			log.LogInfof("scheduleToLcScan: will start at %v ", next)
			t := time.NewTimer(next.Sub(now))
			<-t.C
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.startLcScan()
			}
			t.Stop()
		}
	}()
}

func (c *Cluster) startLcScan() {
	for c.partition != nil && c.partition.IsRaftLeader() {
		success, msg := c.lcMgr.startLcScan("", "")
		if !success {
			log.LogErrorf("%v, retry after 1min", msg)
			time.Sleep(time.Minute)
			continue
		}
		log.LogInfo(msg)
		return
	}
}

func (c *Cluster) scheduleToSnapshotDelVerScan() {
	go c.snapshotMgr.process()
	// make sure resume all the processing ver deleting tasks before checking
	waitTime := time.Second * defaultIntervalToCheck
	waited := false
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if !waited {
					log.LogInfof("wait for %v seconds once after becoming leader to make sure all the ver deleting tasks are resumed",
						waitTime)
					time.Sleep(waitTime)
					waited = true
				}
				c.getSnapshotDelVer()
			}
			time.Sleep(waitTime)
		}
	}()
}

func (c *Cluster) getSnapshotDelVer() {
	if c.partition == nil || !c.partition.IsRaftLeader() {
		log.LogWarn("getSnapshotDelVer: master is not leader")
		return
	}
	c.snapshotMgr.lcSnapshotTaskStatus.ResetVerInfos()

	vols := c.allVols()
	for volName, vol := range vols {
		volVerInfoList := vol.VersionMgr.getVersionList()
		for _, volVerInfo := range volVerInfoList.VerList {
			if volVerInfo.Status == proto.VersionDeleting {
				task := &proto.SnapshotVerDelTask{
					Id:             fmt.Sprintf("%s:%d", volName, volVerInfo.Ver),
					VolName:        volName,
					VolVersionInfo: volVerInfo,
				}
				c.snapshotMgr.lcSnapshotTaskStatus.AddVerInfo(task)
			}
		}
	}
	log.LogDebug("getSnapshotDelVer AddVerInfo finish")
	c.snapshotMgr.lcSnapshotTaskStatus.DeleteOldResult()
	log.LogDebug("getSnapshotDelVer DeleteOldResult finish")
}

func (c *Cluster) SetBucketLifecycle(req *proto.LcConfiguration) error {
	lcConf := &proto.LcConfiguration{
		VolName: req.VolName,
		Rules:   req.Rules,
	}
	if c.lcMgr.GetS3BucketLifecycle(req.VolName) != nil {
		if err := c.syncUpdateLcConf(lcConf); err != nil {
			err = fmt.Errorf("action[SetS3BucketLifecycle],clusterID[%v] vol:%v err:%v ", c.Name, lcConf.VolName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
			return err
		}
	} else {
		if err := c.syncAddLcConf(lcConf); err != nil {
			err = fmt.Errorf("action[SetS3BucketLifecycle],clusterID[%v] vol:%v err:%v ", c.Name, lcConf.VolName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
			return err
		}
	}
	_ = c.lcMgr.SetS3BucketLifecycle(lcConf)
	log.LogInfof("action[SetS3BucketLifecycle],clusterID[%v] vol:%v", c.Name, lcConf.VolName)
	return nil
}

func (c *Cluster) GetBucketLifecycle(VolName string) (lcConf *proto.LcConfiguration) {
	lcConf = c.lcMgr.GetS3BucketLifecycle(VolName)
	log.LogInfof("action[GetS3BucketLifecycle],clusterID[%v] vol:%v", c.Name, VolName)
	return
}

func (c *Cluster) DelBucketLifecycle(VolName string) error {
	lcConf := &proto.LcConfiguration{
		VolName: VolName,
	}
	if err := c.syncDeleteLcConf(lcConf); err != nil {
		err = fmt.Errorf("action[DelS3BucketLifecycle],clusterID[%v] vol:%v err:%v ", c.Name, VolName, err.Error())
		log.LogError(errors.Stack(err))
		Warn(c.Name, err.Error())
		return err
	}
	c.lcMgr.DelS3BucketLifecycle(VolName)
	log.LogInfof("action[DelS3BucketLifecycle],clusterID[%v] vol:%v", c.Name, VolName)
	return nil
}

func (c *Cluster) addDecommissionDiskToNodeset(dd *DecommissionDisk) (err error) {
	var (
		node *DataNode
		zone *Zone
		ns   *nodeSet
	)
	if node, err = c.dataNode(dd.SrcAddr); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] cannot find dataNode[%s]", dd.SrcAddr)
		return
	}
	if zone, err = c.t.getZone(node.ZoneName); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] zone failed[%v]",
			node.Addr, err.Error())
		return
	}
	if ns, err = zone.getNodeSet(node.NodeSetID); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] nodeset[%v] failed[%v]",
			node.Addr, node.NodeSetID, err.Error())
		return
	}
	ns.AddDecommissionDisk(dd)
	return nil
}

func (c *Cluster) AutoDecommissionDiskIsEnabled() bool {
	c.AutoDecommissionDiskMux.Lock()
	defer c.AutoDecommissionDiskMux.Unlock()
	return c.EnableAutoDecommissionDisk.Load()
}

func (c *Cluster) SetAutoDecommissionDisk(flag bool) {
	c.AutoDecommissionDiskMux.Lock()
	defer c.AutoDecommissionDiskMux.Unlock()
	c.EnableAutoDecommissionDisk.Store(flag)
}

func (c *Cluster) GetAutoDecommissionDiskInterval() (interval time.Duration) {
	tmp := c.AutoDecommissionInterval.Load()
	if tmp == 0 {
		tmp = int64(defaultAutoDecommissionDiskInterval)
	}
	interval = time.Duration(tmp)
	return
}

func (c *Cluster) setAutoDecommissionDiskInterval(interval time.Duration) (err error) {
	old := c.AutoDecommissionInterval.Load()
	c.AutoDecommissionInterval.Store(int64(interval))
	if err = c.syncPutCluster(); err != nil {
		c.AutoDecommissionInterval.Store(old)
		return
	}
	return
}

func (c *Cluster) GetAutoDpMetaRepairParallelCnt() (cnt int) {
	cnt = int(c.AutoDpMetaRepairParallelCnt.Load())
	if cnt == 0 {
		cnt = defaultAutoDpMetaRepairPallarelCnt
	}
	return
}

func (c *Cluster) setAutoDpMetaRepairParallelCnt(cnt int) (err error) {
	old := c.AutoDpMetaRepairParallelCnt.Load()
	c.AutoDpMetaRepairParallelCnt.Store(uint32(cnt))
	if err = c.syncPutCluster(); err != nil {
		c.AutoDpMetaRepairParallelCnt.Store(old)
		return
	}
	return
}

func (c *Cluster) GetDecommissionDataPartitionRecoverTimeOut() time.Duration {
	if c.cfg.DpRepairTimeOut == 0 {
		return time.Hour * 2
	}
	return time.Duration(c.cfg.DpRepairTimeOut)
}

func (c *Cluster) GetDecommissionDataPartitionBackupTimeOut() time.Duration {
	if c.cfg.DpBackupTimeOut == 0 {
		return proto.DefaultDataPartitionBackupTimeOut
	}
	return time.Duration(c.cfg.DpBackupTimeOut)
}

func (c *Cluster) GetDecommissionDiskLimit() (limit uint32) {
	limit = atomic.LoadUint32(&c.DecommissionDiskLimit)
	if limit == 0 {
		limit = 1
	}
	return
}

func (c *Cluster) setDecommissionDiskLimit(limit uint32) (err error) {
	oldVal := c.GetDecommissionDiskLimit()
	atomic.StoreUint32(&c.DecommissionDiskLimit, limit)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setDataPartitionTimeout] failed to set DecommissionDiskLimit , err(%v)", err)
		atomic.StoreUint32(&c.DecommissionDiskLimit, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDecommissionDpLimit(limit uint64) (err error) {
	zones := c.t.getAllZones()
	for _, zone := range zones {
		err = zone.updateDecommissionLimit(int32(limit), c)
		if err != nil {
			return
		}
	}
	atomic.StoreUint64(&c.DecommissionLimit, limit)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("[setDataPartitionTimeout] failed to set DecommissionDiskLimit , err(%v)", err)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClusterMediaType(mediaType uint32) (err error) {
	log.LogWarnf("setClusterMediaType: try to update mediaType %d", mediaType)

	if !proto.IsValidMediaType(mediaType) {
		return fmt.Errorf("setClusterMediaType: mediaType is not vailid, type %d", mediaType)
	}

	if mediaType != c.cfg.cfgDataMediaType {
		return fmt.Errorf("setClusterMediaType: mediaType should equal to cfg media type, req %d, cfg %d",
			mediaType, c.cfg.cfgDataMediaType)
	}

	oldType := c.legacyDataMediaType
	if oldType == mediaType {
		log.LogWarnf("setClusterMediaType: mediaType is already update.")
		return nil
	}

	if oldType != proto.MediaType_Unspecified {
		return fmt.Errorf("setClusterMediaType: cant't update mediaType old %d, new %d", oldType, mediaType)
	}

	c.legacyDataMediaType = mediaType
	if err = c.syncPutCluster(); err != nil {
		c.legacyDataMediaType = oldType
		log.LogErrorf("[setClusterMediaType] failed to set cluster err(%v)", err)
		err = proto.ErrPersistenceByRaft
		return
	}

	// update datanodes
	c.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		if !proto.IsValidMediaType(node.MediaType) {
			node.MediaType = mediaType
		}
		return true
	})

	// update vols
	c.volMutex.RLock()
	for _, v := range c.vols {
		c.setStorageClassForLegacyVol(v)
	}
	c.volMutex.RUnlock()

	// update zones
	c.t.zoneLock.RLock()
	for _, z := range c.t.zones {
		if !proto.IsValidMediaType(z.dataMediaType) {
			z.SetDataMediaType(mediaType)
		}
	}
	c.t.zoneLock.RUnlock()

	// update dps
	c.rangeAllParitions(func(d *DataPartition) bool {
		if !proto.IsValidMediaType(d.MediaType) {
			d.MediaType = mediaType
		}
		return true
	})

	c.dataMediaTypeVaild = true
	log.LogWarnf("setClusterMediaType: update mediaType success, old %d, new %d", oldType, mediaType)
	return
}

func (c *Cluster) rangeAllParitions(f func(d *DataPartition) bool) {
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.dataPartitions.RLock()
		for _, dp := range vol.dataPartitions.partitions {
			if !f(dp) {
				return
			}
		}
		vol.dataPartitions.RUnlock()
	}
}

func (c *Cluster) markDecommissionDataPartition(dp *DataPartition, src *DataNode, raftForce bool, migrateType uint32) (err error) {
	addr := src.Addr
	replica, err := dp.getReplica(addr)
	if err != nil {
		err = errors.NewErrorf(" dataPartitionID :%v not find replica for addr %v", dp.PartitionID, addr)
		return
	}
	zone, err := c.t.getZone(src.ZoneName)
	if err != nil {
		err = errors.NewErrorf(" dataPartitionID :%v not find zone for addr %v", dp.PartitionID, addr)
		return
	}
	ns, err := zone.getNodeSet(src.NodeSetID)
	if err != nil {
		err = errors.NewErrorf(" dataPartitionID :%v not find nodeset for addr %v", dp.PartitionID, addr)
		return
	}

	if err = dp.MarkDecommissionStatus(addr, "", replica.DiskPath, raftForce, uint64(time.Now().Unix()), migrateType, c, ns); err != nil {
		if !strings.Contains(err.Error(), proto.ErrDecommissionDiskErrDPFirst.Error()) {
			dp.markRollbackFailed(false)
			dp.DecommissionErrorMessage = err.Error()
			c.syncUpdateDataPartition(dp)
			return
		}
	}
	// TODO: handle error
	err = c.syncUpdateDataPartition(dp)
	if err != nil {
		return
	}
	ns.AddToDecommissionDataPartitionList(dp, c)
	return
}

func (c *Cluster) removeDPFromBadDataPartitionIDs(addr, diskPath string, partitionID uint64) error {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	key := fmt.Sprintf("%s:%s", addr, diskPath)
	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if !ok {
		return errors.NewErrorf("action[TryDecommissionDisk] cannot find %v in BadDataPartitionIds", key)
	}
	newBadPartitionIDs := make([]uint64, 0)
	for _, dp := range badPartitionIDs.([]uint64) {
		if dp != partitionID {
			newBadPartitionIDs = append(newBadPartitionIDs, dp)
		}
	}
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
	return nil
}

func (c *Cluster) getDiskErrDataPartitionsView() (dps proto.DiskErrPartitionView) {
	dps = proto.DiskErrPartitionView{
		DiskErrReplicas: make(map[uint64][]proto.DiskErrReplicaInfo),
	}
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		dataNode.RLock()
		for _, disk := range dataNode.BadDiskStats {
			for _, dpId := range disk.DiskErrPartitionList {
				dps.DiskErrReplicas[dpId] = append(dps.DiskErrReplicas[dpId],
					proto.DiskErrReplicaInfo{Addr: dataNode.Addr, Disk: disk.DiskPath})
			}
		}
		dataNode.RUnlock()
		return true
	})

	return
}

func (c *Cluster) RetryDecommissionDisk(addr string, diskPath string) bool {
	key := fmt.Sprintf("%s_%s", addr, diskPath)
	if value, ok := c.DecommissionDisks.Load(key); ok {
		d := value.(*DecommissionDisk)
		status := d.GetDecommissionStatus()
		return status == DecommissionFail
	}
	return false
}

func (c *Cluster) syncRecoverBackupDataPartitionReplica(host, disk string, dp *DataPartition) (err error) {
	log.LogInfof("action[syncRecoverBackupDataPartitionReplica] dp [%v] to recover replica on %v_%v", dp.PartitionID, host, disk)
	var dataNode *DataNode
	dataNode, err = c.dataNode(host)
	if err != nil {
		return
	}
	task := dp.createTaskToRecoverBackupDataPartitionReplica(host, disk)
	if _, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (c *Cluster) scheduleToCheckDataReplicaMeta() {
	c.runTask(&cTask{
		tickTime: time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition),
		name:     "scheduleToCheckDataReplicaMeta",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkDataReplicaMeta()
			}
			return
		},
	})
}

func (c *Cluster) checkDataReplicaMeta() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataReplicaMeta occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDataReplicaMeta occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		vol.checkDataReplicaMeta(c)
	}
}

func (c *Cluster) getAllDataPartitionWithDiskPathByDataNode(addr string) (infos []proto.DataPartitionDiskInfo) {
	infos = make([]proto.DataPartitionDiskInfo, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			for _, replica := range dp.Replicas {
				if replica.Addr == addr {
					infos = append(infos, proto.DataPartitionDiskInfo{PartitionId: dp.PartitionID, Disk: replica.DiskPath})
					break
				}
			}
		}
	}
	return
}

func (c *Cluster) processDataPartitionDecommission(id uint64) bool {
	zones := c.t.getAllZones()
	for _, zone := range zones {
		nodeSets := zone.getAllNodeSet()
		for _, ns := range nodeSets {
			if ns.processDataPartitionDecommission(id) {
				return true
			}
		}
	}
	return false
}

func (c *Cluster) getDpOpLog(addr string, dpId string) proto.OpLogView {
	var opv proto.OpLogView
	opCounts := make(map[string]int32)
	if addr != "" {
		dataNode, err := c.dataNode(addr)
		if err != nil {
			log.LogErrorf("get dataNode failed, err(%v)", err.Error())
			return opv
		}
		for _, opLog := range dataNode.DpOpLogs {
			parts := strings.Split(opLog.Name, "_")
			if len(parts) < 3 {
				log.LogErrorf("Invalid opLog name format: %s", opLog.Name)
				continue
			}
			opv.DpOpLogs = append(opv.DpOpLogs, proto.OpLog{
				Name:  parts[0] + "_" + parts[1],
				Op:    parts[2],
				Count: opLog.Count,
			})
		}
		return opv
	}
	if dpId != "" {
		id, err := strconv.ParseUint(dpId, 10, 64)
		if err != nil {
			log.LogErrorf("failed to transform dpId, err(%v)", err)
			return opv
		}
		dp, err := c.getDataPartitionByID(id)
		if err != nil {
			log.LogErrorf("failed to get dp(%v), err(%v)", dpId, err)
			return opv
		}
		for _, host := range dp.Hosts {
			dataNode, err := c.dataNode(host)
			if err != nil {
				log.LogErrorf("get dataNode failed, err(%v)", err.Error())
				return opv
			}
			for _, opLog := range dataNode.DpOpLogs {
				parts := strings.Split(opLog.Name, "_")
				if len(parts) < 3 {
					log.LogErrorf("Invalid opLog name format: %s", opLog.Name)
					continue
				}
				if parts[1] == dpId {
					opv.DpOpLogs = append(opv.DpOpLogs, proto.OpLog{
						Name:  fmt.Sprintf("%s [%s]", parts[0]+"_"+parts[1], dataNode.Addr),
						Op:    parts[2],
						Count: opLog.Count,
					})
				}
			}
		}
		return opv
	}
	c.dataNodes.Range(func(key, node interface{}) bool {
		dataNode := node.(*DataNode)
		for _, opLog := range dataNode.DpOpLogs {
			if curCount, exists := opCounts[opLog.Name]; !exists || curCount < opLog.Count {
				opCounts[opLog.Name] = opLog.Count
			}
		}
		return true
	})
	for key, count := range opCounts {
		parts := strings.Split(key, "_")
		if len(parts) < 3 {
			log.LogErrorf("Invalid opLog name format: %s", key)
			continue
		}
		opv.DpOpLogs = append(opv.DpOpLogs, proto.OpLog{
			Name:  parts[0] + "_" + parts[1],
			Op:    parts[2],
			Count: count,
		})
	}
	return opv
}

func (c *Cluster) getDiskOpLog(addr string, diskName string) proto.OpLogView {
	var opv proto.OpLogView
	if addr != "" && diskName != "" {
		dataNode, err := c.dataNode(addr)
		if err != nil {
			log.LogErrorf("get dataNode failed, err(%v)", err.Error())
			return opv
		}
		for _, opLog := range dataNode.DpOpLogs {
			parts := strings.Split(opLog.Name, "_")
			if len(parts) < 3 {
				log.LogErrorf("Invalid opLog name format: %s", opLog.Name)
				continue
			}
			dpId, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				log.LogErrorf("failed to transform dpId, err(%v)", err)
				return opv
			}
			dp, err := c.getDataPartitionByID(dpId)
			if err != nil {
				log.LogErrorf("failed to get dp(%v), err(%v)", dpId, err)
				return opv
			}
			for _, replica := range dp.Replicas {
				if replica.Addr != addr || replica.DiskPath != diskName {
					continue
				}
				opv.DiskOpLogs = append(opv.DiskOpLogs, proto.OpLog{
					Name:  opLog.Name,
					Op:    opLog.Op,
					Count: opLog.Count,
				})
			}
		}
		return opv
	}
	if addr != "" {
		dataNode, err := c.dataNode(addr)
		if err != nil {
			log.LogErrorf("get dataNode failed, err(%v)", err.Error())
			return opv
		}
		opv.DiskOpLogs = append(opv.DiskOpLogs, dataNode.DiskOpLogs...)
		return opv
	}
	c.dataNodes.Range(func(key, node interface{}) bool {
		dataNode := node.(*DataNode)
		for _, opLog := range dataNode.DiskOpLogs {
			opv.DiskOpLogs = append(opv.DiskOpLogs, proto.OpLog{
				Name:  fmt.Sprintf("%s [%s]", opLog.Name, dataNode.Addr),
				Op:    opLog.Op,
				Count: opLog.Count,
			})
		}
		return true
	})
	return opv
}

func (c *Cluster) getClusterOpLog() proto.OpLogView {
	var opv proto.OpLogView
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodeOpLogs := dataNode.getDataNodeOpLog()
		opv.ClusterOpLogs = append(opv.ClusterOpLogs, dataNodeOpLogs...)
		return true
	})
	return opv
}

func (c *Cluster) getVolOpLog(volName string) proto.OpLogView {
	var opv proto.OpLogView
	opCounts := make(map[string]int32)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		volOpLogs := dataNode.getVolOpLog(c, volName)
		for _, opLog := range volOpLogs {
			newName := opLog.Name + "_" + opLog.Op
			if curCount, exists := opCounts[newName]; !exists || curCount < opLog.Count {
				opCounts[newName] = opLog.Count
			}
		}
		return true
	})
	for key, count := range opCounts {
		parts := strings.Split(key, "_")
		if len(parts) < 3 {
			log.LogErrorf("Invalid opLog name format: %s", key)
			continue
		}
		opv.VolOpLogs = append(opv.VolOpLogs, proto.OpLog{
			Name:  parts[1],
			Op:    parts[2],
			Count: count,
		})
	}
	return opv
}

func (c *Cluster) checkMultipleReplicasOnSameMachine(hosts []string) (err error) {
	if !c.cfg.AllowMultipleReplicasOnSameMachine {
		distinctIp := map[string]struct{}{}
		for _, hostStr := range hosts {
			ip, _, _ := net.SplitHostPort(hostStr)
			if _, exist := distinctIp[ip]; exist {
				return fmt.Errorf("Don't allow multiple replicas on same machine while create dp/mp. Multiple replicas locate on [%v] ", ip)
			}
			distinctIp[ip] = struct{}{}
		}
	}
	return nil
}
