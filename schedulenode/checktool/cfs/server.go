package cfs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cfgKeyUsedRatio                      = "usedRatio"
	cfgKeyAvailSpaceRatio                = "availSpaceRatio"
	cfgKeyReadWriteDpRatio               = "readWriteDpRatio"
	cfgKeyClusterUsedRatio               = "clusterUsedRatio"
	cfgKeyNlClusterUsedRatio             = "nlClusterUsedRatio"
	cfgKeyMinRWCnt                       = "minRWCnt"
	cfgKeyDomains                        = "cfsDomains"
	cfgKeyInterval                       = "interval"
	cfgKeyDpCheckInterval                = "dpCheckInterval"
	cfgKeyMpCheckInterval                = "mpCheckInterval"
	cfgKeyMaxOfflineDataNodes            = "maxOfflineDataNodes"
	cfgKeyMaxOfflineDisks                = "maxOfflineDisks"
	cfgKeyMinOfflineDiskMinute           = "minOfflineDiskMinute"
	domainSeparator                      = ","
	UMPCFSNormalWarnKey                  = checktool.UmpKeyStorageBotPrefix + "cfs"
	UMPCFSZoneUsedRatioWarnKey           = checktool.UmpKeyStorageBotPrefix + "cfs.zone.used.ratio"
	UMPCFSZoneUsedRatioOPWarnKey         = checktool.UmpKeyStorageBotPrefix + "cfs.zone.used.ratio.op"
	UMPCFSRaftlogBackWarnKey             = checktool.UmpKeyStorageBotPrefix + "chubaofs.raft.log.backup"
	UMPCFSClusterUsedRatio               = checktool.UmpKeyStorageBotPrefix + "chubaofs.cluster.used.ratio"
	UMPCFSClusterConnRefused             = checktool.UmpKeyStorageBotPrefix + "chubaofs.cluster.connection.refused"
	UMPKeyInactiveNodes                  = checktool.UmpKeyStorageBotPrefix + "chubaofs.inactive.nodes"
	UMPKeyMetaPartitionNoLeader          = checktool.UmpKeyStorageBotPrefix + "chubaofs.meta.partition.no.leader"
	UMPKeyDataPartitionLoadFailed        = checktool.UmpKeyStorageBotPrefix + "chubaofs.data.partition.load.failed"
	UMPKeyMetaPartitionPeerInconsistency = checktool.UmpKeyStorageBotPrefix + "chubaofs.meta.partition.peer.inconsistency"
	UMPKeyDataPartitionPeerInconsistency = checktool.UmpKeyStorageBotPrefix + "chubaofs.data.partition.peer.inconsistency"
	UMPKeyMetaNodeDiskSpace              = checktool.UmpKeyStorageBotPrefix + "chubaofs.meta.node.disk.space"
	UMPKeyMetaNodeDiskRatio              = checktool.UmpKeyStorageBotPrefix + "chubaofs.meta.node.disk.ratio"
	UMPKeyMasterLbPodStatus              = checktool.UmpKeyStorageBotPrefix + "chubaofs.master.lb.pod.status"
	UMPCFSNodeRestartWarnKey             = checktool.UmpKeyStorageBotPrefix + "cfs.restart.node"
	UMPCFSInactiveNodeWarnKey            = checktool.UmpKeyStorageBotPrefix + "cfs.inactive.node"
	UMPCFSZoneWriteAbilityWarnKey        = checktool.UmpKeyStorageBotPrefix + "cfs.zone.writeability.ratio"
	UMPCFSInodeCountDiffWarnKey          = checktool.UmpKeyStorageBotPrefix + "cfs.inode.count.diff"
	UMPCFSRapidMemIncreaseWarnKey        = checktool.UmpKeyStorageBotPrefix + "cfs.rapid.mem.increase"
	UMPCFSMySqlMemWarnKey                = checktool.UmpKeyStorageBotPrefix + "cfs.mysql.mem"
	TB                                   = 1024 * 1024 * 1024 * 1024
	GB                                   = 1024 * 1024 * 1024
	defaultMpNoLeaderWarnInternal        = 10 * 60
	defaultMpNoLeaderMinCount            = 3
	keyWordReleaseCluster                = "seqwrite"
	defaultMaxInactiveNodes              = 2
	defaultMaxOfflineDataNodes           = 3
	defaultMaxOfflineDisks               = 10
	defaultMinOfflineDiskDuration        = time.Minute * 30
	defaultMNDiskMinWarnSize             = GB * 20
	defaultMNDiskMinWarnRatio            = 0.7
	cfsKeymasterJsonPath                 = "cfsmasterJsonPath"
	minRWDPAndMPVolsJsonPath             = "minRWDPAndMPVolsJsonPath"
	cfsKeyWarnFaultToUsersJsonPath       = "cfsWarnFaultToUsersJsonPath"
	cfgKeyDPMaxPendQueueCount            = "dpMaxPendQueueCount"
	cfgKeyDPMaxAppliedIDDiffCount        = "dpMaxAppliedIDDiffCount"
	cfgKeyMPMaxPendQueueCount            = "mpMaxPendQueueCount"
	cfgKeyMPMaxAppliedIDDiffCount        = "mpMaxAppliedIDDiffCount"
	cfgKeyDPPendQueueAlarmThreshold      = "dpPendQueueAlarmThreshold"
	cfgKeyMPPendQueueAlarmThreshold      = "mpPendQueueAlarmThreshold"
	cfgKeySreDbConfigDSNPort             = "sreDbConfig"
	cfgKeyMetaNodeExportDiskUsedRatio    = "metaNodeExportDiskUsedRatio"
	cfgKeyIgnoreCheckMP                  = "ignoreCheckMP"
	cfgKeyNodeRapidMemIncWarnThreshold   = "nodeRapidMemIncWarnThreshold"
	cfgKeyNodeRapidMemIncreaseWarnRatio  = "nodeRapidMemIncreaseWarnRatio"
	defaultNodeRapidMemIncWarnThreshold  = 20 //内存使用率(%)
	defaultNodeRapidMemIncreaseWarnRatio = 0.05
	minMetaNodeExportDiskUsedRatio       = 70
	defaultRestartNodeMaxCountIn24Hour   = 3
	maxBadDataPartitionsCount            = 200
	minWarnFaultToUsersCheckInterval     = 60 * 5
	defaultMaxPendQueueCount             = 0
	defaultMaxAppliedIDDiffCount         = 100
)

const (
	errorConnRefused = "connection refused"
)

const (
	dataNodeType  = 0
	metaNodeType  = 1
	flashNodeType = 2
)

var (
	checkVolWg         sync.WaitGroup
	checkNodeWg        sync.WaitGroup
	checkMetaDataWg    sync.WaitGroup
	noLeaderMps        *sync.Map
	checkMasterNodesWg sync.WaitGroup
	masterNodesMutex   sync.Mutex
	checkDpCorruptWg   sync.WaitGroup
	checkMpCorruptWg   sync.WaitGroup
)

type ChubaoFSMonitor struct {
	usedRatio                       float64
	availSpaceRatio                 float64
	readWriteDpRatio                float64
	hosts                           []*ClusterHost
	idHosts                         []*ClusterHost
	minReadWriteCount               int64
	lastWarnTime                    int64
	scheduleInterval                int
	clusterUsedRatio                float64
	nlClusterUsedRatio              float64
	metrics                         map[string]*AlarmMetric
	chubaoFSMasterNodes             map[string][]string
	badDiskXBPTickets               *sync.Map            //map[string]XBPTicketInfo
	markDeleteVols                  map[string]time.Time // host#volName:lastWarnTime
	offlineDataNodeMaxCountIn24Hour int
	offlineDiskMaxCountIn24Hour     int
	offlineDiskMinDuration          time.Duration
	masterLbLastWarnInfo            map[string]*MasterLBWarnInfo
	scheduleDpCheckInterval         int
	scheduleMpCheckInterval         int
	MinRWDPAndMPVols                []MinRWDPAndMPVolInfo
	lastZoneDiskUsedRatioAlarmTime  time.Time
	lastZoneDiskUsedRatioTelAlarm   time.Time
	lastZoneDiskUsedRatioTelOpAlarm time.Time
	lastRestartNodeTime             time.Time
	RestartNodeMaxCountIn24Hour     int
	highLoadNodeSolver              *ChubaoFSHighLoadNodeSolver
	volNeedAllocateDPContinuedTimes map[string]int
	WarnFaultToUsers                []*WarnFaultToTargetUsers
	WarnFaultToUsersCheckInterval   int
	sreDB                           *gorm.DB
	metaNodeExportDiskUsedRatio     float64
	ignoreCheckMp                   bool
	nodeRapidMemIncWarnThreshold    float64
	nodeRapidMemIncreaseWarnRatio   float64
	ctx                             context.Context
}

func NewChubaoFSMonitor(ctx context.Context) *ChubaoFSMonitor {
	return &ChubaoFSMonitor{
		metrics:                         make(map[string]*AlarmMetric, 0),
		chubaoFSMasterNodes:             make(map[string][]string),
		badDiskXBPTickets:               new(sync.Map),
		markDeleteVols:                  make(map[string]time.Time),
		masterLbLastWarnInfo:            make(map[string]*MasterLBWarnInfo),
		volNeedAllocateDPContinuedTimes: make(map[string]int),
		WarnFaultToUsers:                make([]*WarnFaultToTargetUsers, 0),
		ctx:                             ctx,
	}
}

func (s *ChubaoFSMonitor) Start(cfg *config.Config) (err error) {
	err = s.parseConfig(cfg)
	if err != nil {
		return
	}
	noLeaderMps = new(sync.Map)
	s.scheduleTask(cfg)
	StartChubaoFSDPReleaser(cfg)
	highLoadNodeSolver := StartChubaoFSHighLoadNodeSolver(cfg)
	if highLoadNodeSolver != nil {
		s.RestartNodeMaxCountIn24Hour = defaultRestartNodeMaxCountIn24Hour
		s.highLoadNodeSolver = highLoadNodeSolver
	}
	fmt.Println("starting ChubaoFSMonitor finished")
	return
}

func (s *ChubaoFSMonitor) extractChubaoFSInfo(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	if err = json.Unmarshal(cfg.Raw, &s.chubaoFSMasterNodes); err != nil {
		return
	}
	fmt.Println("chubaoFSMasterNodes:", s.chubaoFSMasterNodes)
	return
}

func (s *ChubaoFSMonitor) extractMinRWDPAndMPVols(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	volInfos := struct {
		MinRWDPAndMPVols []MinRWDPAndMPVolInfo
	}{}
	if err = json.Unmarshal(cfg.Raw, &volInfos); err != nil {
		return
	}
	s.MinRWDPAndMPVols = volInfos.MinRWDPAndMPVols
	fmt.Println("MinRWDPAndMPVols:", s.MinRWDPAndMPVols)
	return
}

func (s *ChubaoFSMonitor) extractWarnFaultToUsers(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	userInfos := struct {
		WarnFaultToUsersCheckInterval int
		WarnFaultToUsers              []*WarnFaultToTargetUsers
	}{}
	if err = json.Unmarshal(cfg.Raw, &userInfos); err != nil {
		return
	}
	s.WarnFaultToUsers = userInfos.WarnFaultToUsers
	s.WarnFaultToUsersCheckInterval = userInfos.WarnFaultToUsersCheckInterval
	if s.WarnFaultToUsersCheckInterval < minWarnFaultToUsersCheckInterval {
		s.WarnFaultToUsersCheckInterval = minWarnFaultToUsersCheckInterval
	}
	fmt.Println("WarnFaultToUsersCheckInterval:", s.WarnFaultToUsersCheckInterval)
	if marshal, err1 := json.Marshal(s.WarnFaultToUsers); err1 == nil {
		fmt.Println("WarnFaultToUsers:", string(marshal))
	}
	return
}

func (s *ChubaoFSMonitor) scheduleTask(cfg *config.Config) {
	go s.scheduleToCheckVol()
	go s.scheduleToCheckSpecificVol()
	go s.scheduleToCheckNodesAlive()
	//go s.scheduleToCheckIDMetaNodeDiskStat()
	go s.scheduleToCheckClusterUsedRatio()
	go s.scheduleToCheckMasterLbPodStatus(cfg)
	//go s.scheduleToCompareMasterMetaDataDiff()
	go s.scheduleToCheckMasterNodesAlive()
	go s.scheduleToCheckDiskError()
	go s.scheduleToCheckXBPTicket()
	go s.scheduleToCheckZoneDiskUsedRatio()
	go s.scheduleToCheckObjectNodeAlive(cfg)
	go s.scheduleToCheckMpPeerCorrupt()
	go s.scheduleToCheckDpPeerCorrupt()
	go s.scheduleToCheckAndWarnFaultToUsers()
	go s.scheduleToCheckMetaPartitionSplit()
	go s.scheduleToCheckZoneMnDnWriteAbilityRate()
	go s.scheduleToCheckCFSHighIncreaseMemNodes()
}

func (s *ChubaoFSMonitor) scheduleToCheckVol() {
	s.checkAvailSpaceAndVolsStatus()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkAvailSpaceAndVolsStatus()
		}
	}
}

func (s *ChubaoFSMonitor) parseConfig(cfg *config.Config) (err error) {
	cfsMasterJsonPath := cfg.GetString(cfsKeymasterJsonPath)
	if cfsMasterJsonPath == "" {
		return fmt.Errorf("cfsMasterJsonPath is empty")
	}
	if err = s.extractChubaoFSInfo(cfsMasterJsonPath); err != nil {
		return fmt.Errorf("parse cfsmasterJsonPath failed, cfsmasterJsonPath can't be nil err:%v", err)
	}
	minRWDPAndMPVolsJson := cfg.GetString(minRWDPAndMPVolsJsonPath)
	if minRWDPAndMPVolsJson == "" {
		return fmt.Errorf("minRWDPAndMPVolsJsonPath is empty")
	}
	if err = s.extractMinRWDPAndMPVols(minRWDPAndMPVolsJson); err != nil {
		return fmt.Errorf("parse minRWDPAndMPVolsJsonPath failed, cfsmasterJsonPath can't be nil err:%v", err)
	}
	useRatio := cfg.GetFloat(cfgKeyUsedRatio)
	if useRatio <= 0 {
		return fmt.Errorf("parse usedRatio failed")
	}
	s.usedRatio = useRatio
	availSpaceRatio := cfg.GetFloat(cfgKeyAvailSpaceRatio)
	if availSpaceRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.availSpaceRatio = availSpaceRatio
	readWriteDpRatio := cfg.GetFloat(cfgKeyReadWriteDpRatio)
	if readWriteDpRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.readWriteDpRatio = readWriteDpRatio

	minRWCnt := cfg.GetFloat(cfgKeyMinRWCnt)
	if minRWCnt <= 0 {
		return fmt.Errorf("parse minRWCnt failed")
	}
	s.minReadWriteCount = int64(minRWCnt)
	domains := cfg.GetString(cfgKeyDomains)
	if domains == "" {
		return fmt.Errorf("parse cfsDomains failed,cfsDomains can't be nil")
	}
	hosts := strings.Split(domains, domainSeparator)

	clusterHosts := make([]*ClusterHost, 0)
	for _, host := range hosts {
		if cfg.GetString(config.CfgRegion) == config.IDRegion && !strings.Contains(host, config.IDRegion) {
			continue
		}
		if cfg.GetString(config.CfgRegion) != config.IDRegion && strings.Contains(host, config.IDRegion) {
			continue
		}
		clusterHosts = append(clusterHosts, newClusterHost(host))
	}
	s.hosts = clusterHosts
	s.updateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(cfg)
	interval := cfg.GetString(cfgKeyInterval)
	if interval == "" {
		return fmt.Errorf("parse interval failed,interval can't be nil")
	}

	if s.scheduleInterval, err = strconv.Atoi(interval); err != nil {
		return err
	}
	// dp corrupt check
	dpCheckInterval := cfg.GetString(cfgKeyDpCheckInterval)
	if dpCheckInterval == "" {
		return fmt.Errorf("parse dpCheckInterval failed,dpCheckInterval can't be nil")
	}
	if s.scheduleDpCheckInterval, err = strconv.Atoi(dpCheckInterval); err != nil {
		return err
	}
	// mp corrupt check
	mpCheckInterval := cfg.GetString(cfgKeyMpCheckInterval)
	if mpCheckInterval == "" {
		return fmt.Errorf("parse mpCheckInterval failed,mpCheckInterval can't be nil")
	}
	if s.scheduleMpCheckInterval, err = strconv.Atoi(mpCheckInterval); err != nil {
		return err
	}

	s.offlineDataNodeMaxCountIn24Hour, _ = strconv.Atoi(cfg.GetString(cfgKeyMaxOfflineDataNodes))
	if s.offlineDataNodeMaxCountIn24Hour <= 0 {
		s.offlineDataNodeMaxCountIn24Hour = 1
	}
	if s.offlineDataNodeMaxCountIn24Hour > defaultMaxOfflineDataNodes {
		s.offlineDataNodeMaxCountIn24Hour = defaultMaxOfflineDataNodes
	}

	s.offlineDiskMaxCountIn24Hour, _ = strconv.Atoi(cfg.GetString(cfgKeyMaxOfflineDisks))
	if s.offlineDiskMaxCountIn24Hour <= 0 {
		s.offlineDiskMaxCountIn24Hour = 1
	}
	if s.offlineDiskMaxCountIn24Hour > defaultMaxOfflineDisks {
		s.offlineDiskMaxCountIn24Hour = defaultMaxOfflineDisks
	}
	s.clusterUsedRatio = cfg.GetFloat(cfgKeyClusterUsedRatio)
	if s.clusterUsedRatio <= 0 {
		return fmt.Errorf("parse clusterUsedRatio failed")
	}
	s.nlClusterUsedRatio = cfg.GetFloat(cfgKeyNlClusterUsedRatio)
	if s.nlClusterUsedRatio <= 0 {
		return fmt.Errorf("parse nlClusterUsedRatio failed")
	}
	offlineDiskMinMinute, _ := strconv.Atoi(cfg.GetString(cfgKeyMinOfflineDiskMinute))
	s.offlineDiskMinDuration = time.Minute * time.Duration(offlineDiskMinMinute)
	if s.offlineDiskMinDuration < defaultMinOfflineDiskDuration {
		s.offlineDiskMinDuration = defaultMinOfflineDiskDuration
	}
	if cfsWarnFaultToUsersJsonPath := cfg.GetString(cfsKeyWarnFaultToUsersJsonPath); cfsWarnFaultToUsersJsonPath != "" {
		if err = s.extractWarnFaultToUsers(cfsWarnFaultToUsersJsonPath); err != nil {
			return fmt.Errorf("parse cfsWarnFaultToUsersJsonPath failed,detail:%v err:%v", cfsWarnFaultToUsersJsonPath, err)
		}
	}
	s.parseSreDBConfig(cfg)
	s.parseHighMemNodeWarnConfig(cfg)
	s.metaNodeExportDiskUsedRatio = cfg.GetFloat(cfgKeyMetaNodeExportDiskUsedRatio)
	if s.metaNodeExportDiskUsedRatio <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyMetaNodeExportDiskUsedRatio)
	}
	if s.metaNodeExportDiskUsedRatio < minMetaNodeExportDiskUsedRatio {
		s.metaNodeExportDiskUsedRatio = minMetaNodeExportDiskUsedRatio
	}
	s.ignoreCheckMp = cfg.GetBool(cfgKeyIgnoreCheckMP)
	fmt.Printf("usedRatio[%v],availSpaceRatio[%v],readWriteDpRatio[%v],minRWCnt[%v],domains[%v],scheduleInterval[%v],clusterUsedRatio[%v]"+
		",offlineDataNodeMaxCountIn24Hour[%v],offlineDiskMaxCountIn24Hour[%v],offlineDiskMinDuration[%v],  mpCheckInterval[%v], dpCheckInterval[%v],metaNodeExportDiskUsedRatio[%v],ignoreCheckMp[%v]\n",
		s.usedRatio, s.availSpaceRatio, s.readWriteDpRatio, s.minReadWriteCount, s.hosts, s.scheduleInterval, s.clusterUsedRatio, s.offlineDataNodeMaxCountIn24Hour,
		s.offlineDiskMaxCountIn24Hour, s.offlineDiskMinDuration, s.scheduleMpCheckInterval, s.scheduleDpCheckInterval, s.metaNodeExportDiskUsedRatio, s.ignoreCheckMp)
	return
}

func (s *ChubaoFSMonitor) updateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(cfg *config.Config) {
	dpMaxPendQueueCount, _ := strconv.Atoi(cfg.GetString(cfgKeyDPMaxPendQueueCount))
	dpMaxAppliedIDDiffCount, _ := strconv.Atoi(cfg.GetString(cfgKeyDPMaxAppliedIDDiffCount))
	mpMaxPendQueueCount, _ := strconv.Atoi(cfg.GetString(cfgKeyMPMaxPendQueueCount))
	mpMaxAppliedIDDiffCount, _ := strconv.Atoi(cfg.GetString(cfgKeyMPMaxAppliedIDDiffCount))

	dpPendQueueAlarmThreshold, _ := strconv.Atoi(cfg.GetString(cfgKeyDPPendQueueAlarmThreshold))
	mpPendQueueAlarmThreshold, _ := strconv.Atoi(cfg.GetString(cfgKeyMPPendQueueAlarmThreshold))
	if dpMaxPendQueueCount < defaultMaxPendQueueCount {
		dpMaxPendQueueCount = defaultMaxPendQueueCount
	}
	if mpMaxPendQueueCount < defaultMaxPendQueueCount {
		mpMaxPendQueueCount = defaultMaxPendQueueCount
	}
	if dpMaxAppliedIDDiffCount < defaultMaxAppliedIDDiffCount {
		dpMaxAppliedIDDiffCount = defaultMaxAppliedIDDiffCount
	}
	if mpMaxAppliedIDDiffCount < defaultMaxAppliedIDDiffCount {
		mpMaxAppliedIDDiffCount = defaultMaxAppliedIDDiffCount
	}
	if dpPendQueueAlarmThreshold < 2 {
		dpPendQueueAlarmThreshold = 2
	}
	if mpPendQueueAlarmThreshold < 2 {
		mpPendQueueAlarmThreshold = 2
	}
	for _, clusterHost := range s.hosts {
		clusterHost.DPMaxPendQueueCount = dpMaxPendQueueCount
		clusterHost.DPMaxAppliedIDDiffCount = dpMaxAppliedIDDiffCount
		clusterHost.MPMaxPendQueueCount = mpMaxPendQueueCount
		clusterHost.MPMaxAppliedIDDiffCount = mpMaxAppliedIDDiffCount
		clusterHost.DPPendQueueAlarmThreshold = dpPendQueueAlarmThreshold
		clusterHost.MPPendQueueAlarmThreshold = mpPendQueueAlarmThreshold
	}
	fmt.Printf("hosts:%v dpMaxPendQueueCount:%v dpMaxAppliedIDDiffCount:%v mpMaxPendQueueCount:%v mpMaxAppliedIDDiffCount:%v dpPendQueueAlarmThreshold:%v mpPendQueueAlarmThreshold:%v\n",
		s.hosts, dpMaxPendQueueCount, dpMaxAppliedIDDiffCount, mpMaxPendQueueCount, mpMaxAppliedIDDiffCount, dpPendQueueAlarmThreshold, mpPendQueueAlarmThreshold)
}

func (s *ChubaoFSMonitor) parseSreDBConfig(cfg *config.Config) {
	var err error
	dBConfigDSN := cfg.GetString(cfgKeySreDbConfigDSNPort)
	if dBConfigDSN == "" {
		fmt.Println("sre DBConfigDSN is empty")
		return
	}
	fmt.Println("cfgKeySreDbConfigDSNPort:", dBConfigDSN)
	s.sreDB, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                       dBConfigDSN,
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}), &gorm.Config{})
	if err != nil {
		fmt.Println("init sreDB failed err:", err)
		s.sreDB = nil
		return
	}
	return
}

func (s *ChubaoFSMonitor) parseHighMemNodeWarnConfig(cfg *config.Config) {
	s.nodeRapidMemIncWarnThreshold = cfg.GetFloat(cfgKeyNodeRapidMemIncWarnThreshold)
	if s.nodeRapidMemIncWarnThreshold <= 0 {
		s.nodeRapidMemIncWarnThreshold = defaultNodeRapidMemIncWarnThreshold
		fmt.Printf("parse %v failed use default value\n", cfgKeyNodeRapidMemIncreaseWarnRatio)
	}
	s.nodeRapidMemIncreaseWarnRatio = cfg.GetFloat(cfgKeyNodeRapidMemIncreaseWarnRatio)
	if s.nodeRapidMemIncreaseWarnRatio <= 0 {
		s.nodeRapidMemIncreaseWarnRatio = defaultNodeRapidMemIncreaseWarnRatio
		fmt.Printf("parse %v failed use default value\n", cfgKeyNodeRapidMemIncWarnThreshold)
	}
	fmt.Printf("parseHighMemNodeWarnConfig nodeRapidMemIncWarnThreshold:%v ,nodeRapidMemIncreaseWarnRatio:%v\n", s.nodeRapidMemIncWarnThreshold, s.nodeRapidMemIncreaseWarnRatio)
	return
}
