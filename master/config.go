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
	syslog "log"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	pt "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
)

// config key
const (
	colonSplit = ":"
	commaSplit = ","
	cfgPeers   = "peers"
	// if the data partition has not been reported within this interval  (in terms of seconds), it will be considered as missing.
	missingDataPartitionInterval        = "missingDataPartitionInterval"
	cfgDpNoLeaderReportIntervalSec      = "dpNoLeaderReportIntervalSec"
	cfgMpNoLeaderReportIntervalSec      = "mpNoLeaderReportIntervalSec"
	dataPartitionTimeOutSec             = "dataPartitionTimeOutSec"
	NumberOfDataPartitionsToLoad        = "numberOfDataPartitionsToLoad"
	secondsToFreeDataPartitionAfterLoad = "secondsToFreeDataPartitionAfterLoad"
	nodeSetCapacity                     = "nodeSetCap"
	cfgMetaNodeReservedMem              = "metaNodeReservedMem"
	heartbeatPortKey                    = "heartbeatPort"
	replicaPortKey                      = "replicaPort"
	faultDomain                         = "faultDomain"
	cfgDomainBatchGrpCnt                = "faultDomainGrpBatchCnt"
	cfgDomainBuildAsPossible            = "faultDomainBuildAsPossible"
	cfgmetaPartitionInodeIdStep         = "metaPartitionInodeIdStep"
	cfgMaxQuotaNumPerVol                = "maxQuotaNumPerVol"
	disableAutoCreate                   = "disableAutoCreate"

	enableFollowerCache = "enableFollowerCache"
	enableSnapshot      = "enableSnapshot"
	cfgMonitorPushAddr  = "monitorPushAddr"
	cfgStartLcScanTime  = "startLcScanTime"

	cfgVolForceDeletion           = "volForceDeletion"
	cfgVolDeletionDentryThreshold = "volDeletionDentryThreshold"

	cfgHttpReversePoolSize = "httpReversePoolSize"

	cfgLegacyDataMediaType = "legacyDataMediaType" // for hybrid cloud upgrade

	cfgRaftPartitionCanUseDifferentPort   = "raftPartitionCanUseDifferentPort"
	cfgAllowMultipleReplicasOnSameMachine = "allowMultipleReplicasOnSameMachine"
)

// default value
const (
	defaultTobeFreedDataPartitionCount         = 1000
	defaultSecondsToFreeDataPartitionAfterLoad = 5 * 60 // a data partition can only be freed after loading 5 mins
	defaultIntervalToFreeDataPartition         = 10     // in terms of seconds
	defaultIntervalToCheck                     = 60
	defaultIntervalToCheckHeartbeat            = 6
	defaultIntervalToCheckDataPartition        = 5
	defaultIntervalToCheckQos                  = 1
	defaultIntervalToCheckCrc                  = 20 * defaultIntervalToCheck // in terms of seconds
	noHeartBeatTimes                           = 3                           // number of times that no heartbeat reported
	defaultNodeTimeOutSec                      = noHeartBeatTimes * defaultIntervalToCheckHeartbeat
	defaultDataPartitionTimeOutSec             = 200 * defaultIntervalToCheckHeartbeat // datanode with massive amount of dp may cost 10-min
	defaultMissingDataPartitionInterval        = 24 * 3600
	// defaultDpNoLeaderReportIntervalSec         = 10 * 60
	// TODO-chi: for test
	defaultDpNoLeaderReportIntervalSec         = 10
	defaultMpNoLeaderReportIntervalSec         = 5
	defaultIntervalToAlarmMissingDataPartition = 60 * 60
	timeToWaitForResponse                      = 120         // time to wait for response by the master during loading partition
	defaultPeriodToLoadAllDataPartitions       = 60 * 60 * 4 // how long we need to load all the data partitions on the master every time
	defaultNumberOfDataPartitionsToLoad        = 50          // how many data partitions to load every time
	defaultMetaPartitionTimeOutSec             = 10 * defaultIntervalToCheckHeartbeat
	// DefaultMetaPartitionMissSec                         = 3600

	defaultIntervalToAlarmMissingMetaPartition         = 10 * 60 // interval of checking if a replica is missing
	defaultMetaPartitionMemUsageThreshold      float32 = 0.75    // memory usage threshold on a meta partition
	defaultDomainUsageThreshold                float64 = 0.90    // storage usage threshold on a data partition
	defaultOverSoldFactor                      float32 = 0       // 0 means no oversold limit
	defaultMaxMetaPartitionCountOnEachNode             = 10000
	defaultReplicaNum                                  = 3
	defaultDiffSpaceUsage                              = 1024 * 1024 * 1024
	defaultDiffReplicaFileCount                        = 20
	defaultNodeSetGrpStep                              = 1
	defaultMasterMinQosAccept                          = 20000
	defaultMaxDpCntLimit                               = 3000
	defaultMaxMpCntLimit                               = 300
	defaultStartLcScanTime                             = 1
	defaultMaxConcurrentLcNodes                        = 3
	metaPartitionInodeUsageThreshold           float64 = 0.75 // inode usage threshold on a meta partition
	lowerLimitRWMetaPartition                          = 3    // lower limit of RW meta partition, equal defaultReplicaNum
	defaultHttpReversePoolSize                         = 1024

	defaultFlashNodeHandleReadTimeout   = 3
	defaultFlashNodeReadDataNodeTimeout = 3
)

// AddrDatabase is a map that stores the address of a given host (e.g., the leader)
var AddrDatabase = make(map[uint64]string)

type clusterConfig struct {
	secondsToFreeDataPartitionAfterLoad int64
	NodeTimeOutSec                      int64
	MissingDataPartitionInterval        int64
	DpNoLeaderReportIntervalSec         int64
	MpNoLeaderReportIntervalSec         int64
	DataPartitionTimeOutSec             int64
	IntervalToAlarmMissingDataPartition int64
	PeriodToLoadALLDataPartitions       int64
	metaNodeReservedMem                 uint64
	httpProxyPoolSize                   uint64
	httpPoolSize                        uint64
	IntervalToCheckDataPartition        int // seconds
	IntervalToCheckQos                  int // seconds
	numberOfDataPartitionsToFree        int
	numberOfDataPartitionsToLoad        int
	nodeSetCapacity                     int
	MetaNodeThreshold                   float32
	ClusterLoadFactor                   float32
	MetaNodeDeleteBatchCount            uint64 // metanode delete batch count
	DataNodeDeleteLimitRate             uint64 // datanode delete limit rate
	MetaNodeDeleteWorkerSleepMs         uint64 // metaNode delete worker sleep time with millisecond. if 0 for no sleep
	// MaxDpCntLimit                       uint64 // datanode data partition limit
	// MaxMpCntLimit                       uint64 // metanode meta partition limit
	DataNodeAutoRepairLimitRate uint64 // datanode autorepair limit rate
	DpMaxRepairErrCnt           uint64
	DpRepairTimeOut             uint64
	DpBackupTimeOut             uint64
	peers                       []raftstore.PeerAddress
	peerAddrs                   []string
	heartbeatPort               int64
	replicaPort                 int64
	diffReplicaSpaceUsage       uint64
	diffReplicaFileCount        uint32
	faultDomain                 bool
	DefaultNormalZoneCnt        int
	DomainBuildAsPossible       bool
	DataPartitionUsageThreshold float64
	QosMasterAcceptLimit        uint64
	DirChildrenNumLimit         uint32
	MetaPartitionInodeIdStep    uint64
	MaxQuotaNumPerVol           int
	DisableAutoCreate           bool
	EnableFollowerCache         bool
	EnableSnapshot              bool
	MonitorPushAddr             string
	StartLcScanTime             int
	MaxConcurrentLcNodes        uint64

	volForceDeletion           bool   // when delete a volume, ignore it's dentry count or not
	volDeletionDentryThreshold uint64 // in case of volForceDeletion is set to false, define the dentry count threshold to allow volume deletion
	volDelayDeleteTimeHour     int64

	cfgDataMediaType uint32 // used to control update mediaType, pay attention to use.

	// configuring datanode and metanode to forbidden write operate codes of packet protocol version-0
	// PacketProtoVersion-0: before hybrid cloud version
	// PacketProtoVersion-1: from hybrid cloud version
	forbidWriteOpOfProtoVer0 bool

	// whether data partition/meta partition can use different raft heartbeat port and replicate port.
	// if so we can deploy multiple datanode/metanode on single machine
	raftPartitionCanUseDifferentPort     atomicutil.Bool
	raftPartitionAlreadyUseDifferentPort atomicutil.Bool

	AllowMultipleReplicasOnSameMachine bool // whether dp/mp replicas can locate on same machine, default true

	flashNodeHandleReadTimeout   int
	flashNodeReadDataNodeTimeout int
}

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)
	cfg.numberOfDataPartitionsToFree = defaultTobeFreedDataPartitionCount
	cfg.secondsToFreeDataPartitionAfterLoad = defaultSecondsToFreeDataPartitionAfterLoad
	cfg.NodeTimeOutSec = defaultNodeTimeOutSec
	cfg.MissingDataPartitionInterval = defaultMissingDataPartitionInterval
	cfg.DpNoLeaderReportIntervalSec = defaultDpNoLeaderReportIntervalSec
	cfg.MpNoLeaderReportIntervalSec = defaultMpNoLeaderReportIntervalSec
	cfg.DataPartitionTimeOutSec = defaultDataPartitionTimeOutSec
	cfg.IntervalToCheckDataPartition = defaultIntervalToCheckDataPartition
	cfg.IntervalToCheckQos = defaultIntervalToCheckQos
	cfg.IntervalToAlarmMissingDataPartition = defaultIntervalToAlarmMissingDataPartition
	cfg.numberOfDataPartitionsToLoad = defaultNumberOfDataPartitionsToLoad
	cfg.PeriodToLoadALLDataPartitions = defaultPeriodToLoadAllDataPartitions
	cfg.MetaNodeThreshold = defaultMetaPartitionMemUsageThreshold
	cfg.ClusterLoadFactor = defaultOverSoldFactor
	// cfg.MaxDpCntLimit = defaultMaxDpCntLimit
	// cfg.MaxMpCntLimit = defaultMaxMpCntLimit
	cfg.metaNodeReservedMem = defaultMetaNodeReservedMem
	cfg.diffReplicaSpaceUsage = defaultDiffSpaceUsage
	cfg.diffReplicaFileCount = defaultDiffReplicaFileCount
	cfg.QosMasterAcceptLimit = defaultMasterMinQosAccept
	cfg.DirChildrenNumLimit = pt.DefaultDirChildrenNumLimit
	cfg.MetaPartitionInodeIdStep = defaultMetaPartitionInodeIDStep
	cfg.MaxQuotaNumPerVol = defaultMaxQuotaNumPerVol
	cfg.StartLcScanTime = defaultStartLcScanTime
	cfg.MaxConcurrentLcNodes = defaultMaxConcurrentLcNodes
	cfg.volDelayDeleteTimeHour = defaultVolDelayDeleteTimeHour
	cfg.flashNodeHandleReadTimeout = defaultFlashNodeHandleReadTimeout
	cfg.flashNodeReadDataNodeTimeout = defaultFlashNodeReadDataNodeTimeout
	return
}

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, colonSplit)
	id, err = strconv.ParseUint(peerStr[0], 10, 64)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(peerStr[2], 10, 64)
	if err != nil {
		return
	}
	ip = peerStr[1]
	return
}

func (cfg *clusterConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, commaSplit)
	cfg.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, raftstore.PeerAddress{Peer: proto.Peer{ID: id}, Address: ip, HeartbeatPort: int(cfg.heartbeatPort), ReplicaPort: int(cfg.replicaPort)})
		address := fmt.Sprintf("%v:%v", ip, port)
		syslog.Println(address)
		AddrDatabase[id] = address
	}
	return nil
}

func (cfg *clusterConfig) checkRaftPartitionCanUseDifferentPort(m *Server, optVal bool) (err error) {
	cfg.raftPartitionCanUseDifferentPort.Store(optVal)
	clusterCfg, err := m.rocksDBStore.SeekForPrefix([]byte(clusterPrefix))
	if err != nil {
		err = fmt.Errorf("action[checkConfig] error when load cluster config form rocksdb,err:%v", err.Error())
		return err
	}
	for _, c := range clusterCfg {
		cv := &clusterValue{}
		if err = json.Unmarshal(c, cv); err != nil {
			log.LogErrorf("action[checkRaftPartitionCanUseDifferentPort], unmarshal err:%v", err.Error())
			return err
		}

		if cv.Name != m.clusterName {
			log.LogErrorf("action[checkRaftPartitionCanUseDifferentPort] loaded cluster value: %+v", cv)
			continue
		}

		if cv.RaftPartitionAlreadyUseDifferentPort && !cfg.raftPartitionCanUseDifferentPort.Load() {
			// raft partition has already use different port, but user want to disable this param again
			return fmt.Errorf("raft partition has already use different port, can not try to disable this param again")
		}
		cfg.raftPartitionAlreadyUseDifferentPort.Store(cv.RaftPartitionAlreadyUseDifferentPort)
	}

	return nil
}
