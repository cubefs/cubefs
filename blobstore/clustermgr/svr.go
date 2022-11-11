// Copyright 2022 The CubeFS Authors.
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

package clustermgr

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	httpproxy "net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/configmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/servicemgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/volumemgr"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/config"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/hashicorp/consul/api"
)

const (
	ServiceStatusNormal   = 1
	ServiceStatusSnapshot = 2
)

const (
	NeedReadIndex   = 1
	NoNeedReadIndex = 2
)

const (
	BidScopeName             = "bid"
	MaxBidCount              = 100000
	DefaultChunkSize         = 17179869184
	DefaultVolumeReserveSize = 10485760
)

const (
	defaultClusterReportIntervalS   = 60
	defaultHeartbeatNotifyIntervalS = 10
	defaultMaxHeartbeatNotifyNum    = 2000
	defaultMetricReportIntervalM    = 2
	defaultCheckConsistentIntervalM = 360
)

var (
	service *Service
	conf    Config
)

type Config struct {
	Region                   string                    `json:"region"`
	IDC                      []string                  `json:"idc"`
	UnavailableIDC           string                    `json:"unavailable_idc"`
	ClusterID                proto.ClusterID           `json:"cluster_id"`
	Readonly                 bool                      `json:"readonly"`
	VolumeMgrConfig          volumemgr.VolumeMgrConfig `json:"volume_mgr_config"`
	DBPath                   string                    `json:"db_path"`
	NormalDBPath             string                    `json:"normal_db_path"`
	NormalDBOption           kvstore.RocksDBOption     `json:"normal_db_option"`
	KvDBPath                 string                    `json:"kv_db_path"`
	KvDBOption               kvstore.RocksDBOption     `json:"kv_db_option"`
	CodeModePolicies         []codemode.Policy         `json:"code_mode_policies"`
	ClusterCfg               map[string]interface{}    `json:"cluster_config"`
	RaftConfig               RaftConfig                `json:"raft_config"`
	DiskMgrConfig            diskmgr.DiskMgrConfig     `json:"disk_mgr_config"`
	ClusterReportIntervalS   int                       `json:"cluster_report_interval_s"`
	ConsulAgentAddr          string                    `json:"consul_agent_addr"`
	HeartbeatNotifyIntervalS int                       `json:"heartbeat_notify_interval_s"`
	MaxHeartbeatNotifyNum    int                       `json:"max_heartbeat_notify_num"`
	ChunkSize                uint64                    `json:"chunk_size"`
	MetricReportIntervalM    int                       `json:"metric_report_interval_m"`
	ConsistentCheckIntervalM int                       `json:"consistent_check_interval_m"`

	cmd.Config
}

type RaftConfig struct {
	RaftDBPath       string                `json:"raft_db_path"`
	RaftDBOption     kvstore.RocksDBOption `json:"raft_db_option"`
	SnapshotPatchNum int                   `json:"snapshot_patch_num"`
	ServerConfig     raftserver.Config     `json:"server_config"`
	RaftNodeConfig   base.RaftNodeConfig   `json:"raft_node_config"`
}

type Service struct {
	ConfigMgr  *configmgr.ConfigMgr
	ScopeMgr   *scopemgr.ScopeMgr
	ServiceMgr *servicemgr.ServiceMgr
	// Note: DiskMgr should always list before volumeMgr
	// cause DiskMgr applier LoadData should be call first, or VolumeMgr LoadData may return error with disk not found
	DiskMgr   *diskmgr.DiskMgr
	VolumeMgr *volumemgr.VolumeMgr
	KvMgr     *kvmgr.KvMgr

	dbs map[string]base.SnapshotDB
	// status indicate service's current state, like normal/snapshot
	status uint32
	// electedLeaderReadIndex indicate that service(elected leader) should execute ReadIndex or not before accept incoming request
	electedLeaderReadIndex uint32
	raftNode               *base.RaftNode
	raftStartOnce          sync.Once
	raftStartCh            chan interface{}
	closeCh                chan interface{}
	consulClient           *api.Client
	*Config
}

func init() {
	mod := &cmd.Module{
		Name:       "CLUSTERMGR",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	var err error
	config.Init("f", "", "clustermgr.conf")
	if err = config.Load(&conf); err != nil {
		return nil, err
	}
	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	var err error
	service, err = New(&conf)
	if err != nil {
		log.Fatalf("Failed to new clustermgr service, err: %v", err)
	}
	return NewHandler(service), []rpc.ProgressHandler{service}
}

func tearDown() {
}

func New(cfg *Config) (*Service, error) {
	if err := cfg.checkAndFix(); err != nil {
		log.Fatalf(fmt.Sprint("clusterMgr service config check failed => ", errors.Detail(err)))
	}

	// db initial: normal/volume/raft
	normalDB, err := normaldb.OpenNormalDB(cfg.NormalDBPath, false, func(option *kvstore.RocksDBOption) {
		*option = cfg.NormalDBOption
	})
	if err != nil {
		log.Fatalf("open normal database failed, err: %v", err)
	}
	volumeDB, err := volumedb.Open(cfg.VolumeMgrConfig.VolumeDBPath, false, func(option *kvstore.RocksDBOption) {
		*option = cfg.VolumeMgrConfig.VolumeDBOption
	})
	if err != nil {
		log.Fatalf("open volume database failed, err: %v", err)
	}
	raftDB, err := raftdb.OpenRaftDB(cfg.RaftConfig.RaftDBPath, false, func(option *kvstore.RocksDBOption) {
		*option = cfg.RaftConfig.RaftDBOption
	})
	if err != nil {
		log.Fatalf("open raft database failed, err: %v", err)
	}
	kvDB, err := kvdb.Open(cfg.KvDBPath, false, func(option *kvstore.RocksDBOption) {
		*option = cfg.KvDBOption
	})
	if err != nil {
		log.Fatal("open kv database failed,err:%v", err)
	}

	// consul client initial
	consulConf := api.DefaultConfig()
	consulConf.Address = cfg.ConsulAgentAddr
	consulClient, err := api.NewClient(consulConf)
	if err != nil {
		log.Fatalf("new consul client failed, err: %v", err)
	}

	service := &Service{
		dbs:          map[string]base.SnapshotDB{"volume": volumeDB, "normal": normalDB, "keyValue": kvDB},
		Config:       cfg,
		raftStartCh:  make(chan interface{}),
		status:       ServiceStatusNormal,
		consulClient: consulClient,
		closeCh:      make(chan interface{}),
	}

	// module manager initial
	scopeMgr, err := scopemgr.NewScopeMgr(normalDB)
	if err != nil {
		log.Fatalf("new scopeMgr failed, err: %v", err)
	}
	diskMgr, err := diskmgr.New(scopeMgr, normalDB, cfg.DiskMgrConfig)
	if err != nil {
		log.Fatalf("new diskMgr failed, err: %v", err)
	}

	kvMgr, err := kvmgr.NewKvMgr(kvDB)
	if err != nil {
		log.Fatalf("new kvMgr failed, error: %v", errors.Detail(err))
	}

	configMgr, err := configmgr.New(kvMgr, cfg.ClusterCfg)
	if err != nil {
		log.Fatalf("new configMg failed, error: %v", err)
	}

	serviceMgr := servicemgr.NewServiceMgr(normaldb.OpenServiceTable(normalDB))

	volumeMgr, err := volumemgr.NewVolumeMgr(cfg.VolumeMgrConfig, diskMgr, scopeMgr, configMgr, volumeDB)
	if err != nil {
		log.Fatalf("new volumeMgr failed, error: %v", errors.Detail(err))
	}

	service.KvMgr = kvMgr
	service.VolumeMgr = volumeMgr
	service.ConfigMgr = configMgr
	service.DiskMgr = diskMgr
	service.ServiceMgr = serviceMgr
	service.ScopeMgr = scopeMgr

	// raft server initial
	applyIndex := uint64(0)
	rawApplyIndex, err := raftDB.Get(base.ApplyIndexKey)
	if err != nil {
		log.Fatalf("get raft apply index from kv store failed, err: %v", err)
	}
	if len(rawApplyIndex) > 0 {
		applyIndex = binary.BigEndian.Uint64(rawApplyIndex)
	}

	// raft node initial
	cfg.RaftConfig.RaftNodeConfig.ApplyIndex = applyIndex
	raftNode, err := base.NewRaftNode(&cfg.RaftConfig.RaftNodeConfig, raftDB, service.dbs)
	if err != nil {
		log.Fatalf("new raft node failed, err: %v", err)
	}
	// register all mgr's apply method
	raftNode.RegistRaftApplier(service)
	service.raftNode = raftNode

	cfg.RaftConfig.ServerConfig.SM = service
	cfg.RaftConfig.ServerConfig.Applied = applyIndex
	members, err := raftNode.GetRaftMembers(context.Background())
	if err != nil {
		log.Fatalf("get raft members failed, err: %v", err.Error())
	}

	log.Infof("config members: %+v, raftdb members: %+v", cfg.RaftConfig.RaftNodeConfig.Members, members)

	for _, member := range members {
		m := raftserver.Member{NodeID: member.ID, Host: member.Host, Learner: member.Learner, Context: []byte(member.NodeHost)}
		cfg.RaftConfig.ServerConfig.Members = append(cfg.RaftConfig.ServerConfig.Members, m)
	}
	raftServer, err := raftserver.NewRaftServer(&cfg.RaftConfig.ServerConfig)
	if err != nil {
		log.Fatalf("new raft server failed, err: %v", err)
	}

	// set raftServer
	service.raftNode.SetRaftServer(raftServer)
	scopeMgr.SetRaftServer(raftServer)
	volumeMgr.SetRaftServer(raftServer)
	configMgr.SetRaftServer(raftServer)

	// wait for raft start
	service.waitForRaftStart()

	volumeMgr.Start()
	// refresh disk expire time after all ready
	diskMgr.RefreshExpireTime()
	// start raft node background progress
	go raftNode.Start()

	// start service background loop
	go service.loop()

	return service, nil
}

func (s *Service) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	status := atomic.LoadUint32(&s.status)

	// forward to leader if current service's status is not normal or method is not GET
	if status != ServiceStatusNormal || (req.Method != http.MethodGet && !s.raftNode.IsLeader()) {
		s.forwardToLeader(w, req)
		return
	}
	// service status is normal, then we should just execute f
	if atomic.LoadUint32(&s.electedLeaderReadIndex) == NeedReadIndex {
		span, ctx := trace.StartSpanFromHTTPHeaderSafe(req, "")
		if err := s.raftNode.ReadIndex(ctx); err != nil {
			span.Errorf("leader read index failed, err: %s", err.Error())
			rpc.ReplyErr(w, apierrors.CodeRaftReadIndex, apierrors.ErrRaftReadIndex.Error())
			return
		}
		atomic.StoreUint32(&s.electedLeaderReadIndex, NoNeedReadIndex)
	}
	f(w, req)
}

func (s *Service) Close() {
	// 1. close service loop
	close(s.closeCh)

	// 2. stop raft server
	s.raftNode.Stop()

	// 3. close module manager
	s.VolumeMgr.Close()
	s.DiskMgr.Close()
	time.Sleep(1 * time.Second)

	// 4. close all database
	for i := range s.dbs {
		s.dbs[i].Close()
	}
}

func (s *Service) BidAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.BidScopeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept BidAlloc request, args: %v", args)

	if args.Count > MaxBidCount {
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}

	base, end, err := s.ScopeMgr.Alloc(ctx, BidScopeName, int(args.Count))
	if err != nil {
		span.Error("alloc scope failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(&clustermgr.BidScopeRet{
		StartBid: proto.BlobID(base),
		EndBid:   proto.BlobID(end),
	})
}

func (c *Config) checkAndFix() (err error) {
	if len(c.IDC) == 0 {
		return errors.New("IDC is nil")
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = DefaultChunkSize
	}
	if c.ClusterCfg == nil {
		c.ClusterCfg = make(map[string]interface{})
	}
	if c.ClusterCfg[proto.VolumeReserveSizeKey] == nil {
		c.ClusterCfg[proto.VolumeReserveSizeKey] = DefaultVolumeReserveSize
	}
	c.VolumeMgrConfig.ChunkSize = c.ChunkSize
	c.DiskMgrConfig.ChunkSize = int64(c.ChunkSize)
	c.ClusterCfg[proto.VolumeChunkSizeKey] = c.ChunkSize
	c.ClusterCfg[proto.CodeModeConfigKey] = c.CodeModePolicies

	if len(c.CodeModePolicies) == 0 {
		return errors.New("invalid code mode config")
	}
	sort.Slice(c.CodeModePolicies, func(i, j int) bool {
		return c.CodeModePolicies[i].MinSize < c.CodeModePolicies[j].MinSize
	})
	sortedPolicies := make([]codemode.Policy, 0)
	for i := range c.CodeModePolicies {
		if c.CodeModePolicies[i].Enable {
			sortedPolicies = append(sortedPolicies, c.CodeModePolicies[i])
		}
	}
	if len(sortedPolicies) > 0 {
		if sortedPolicies[0].MinSize != 0 {
			return errors.New("min size range must be started with 0")
		}
	} else {
		for _, modePolicy := range c.CodeModePolicies {
			codeMode := modePolicy.ModeName.GetCodeMode()
			c.DiskMgrConfig.CodeModes = append(c.DiskMgrConfig.CodeModes, codeMode)
		}
	}
	for i := 0; i < len(sortedPolicies)-1; i++ {
		if sortedPolicies[i+1].MinSize != sortedPolicies[i].MaxSize+1 {
			return errors.New("size range must be serially")
		}
	}

	m := make(map[codemode.CodeModeName]struct{})
	for _, modePolicy := range sortedPolicies {
		if _, ok := m[modePolicy.ModeName]; !ok {
			m[modePolicy.ModeName] = struct{}{}
		} else {
			return errors.New(" code mode repeat")
		}
		codeMode := modePolicy.ModeName.GetCodeMode()
		if c.UnavailableIDC == "" && codeMode.Tactic().AZCount != len(c.IDC) {
			return errors.New("idc count not match modeTactic AZCount")
		}
		c.DiskMgrConfig.CodeModes = append(c.DiskMgrConfig.CodeModes, codeMode)
	}
	c.VolumeMgrConfig.CodeModePolicies = c.CodeModePolicies

	c.DiskMgrConfig.IDC = c.IDC
	c.VolumeMgrConfig.IDC = c.IDC
	c.VolumeMgrConfig.UnavailableIDC = c.UnavailableIDC
	c.VolumeMgrConfig.Region = c.Region
	c.VolumeMgrConfig.ClusterID = c.ClusterID

	if c.RaftConfig.SnapshotPatchNum == 0 {
		c.RaftConfig.SnapshotPatchNum = 64
	}

	c.NormalDBOption.CreateIfMissing = true
	c.VolumeMgrConfig.VolumeDBOption.CreateIfMissing = true
	c.RaftConfig.RaftDBOption.CreateIfMissing = true
	c.KvDBOption.CreateIfMissing = true

	if c.NormalDBPath == "" {
		c.NormalDBPath = c.DBPath + "/normaldb"
	}
	if c.VolumeMgrConfig.VolumeDBPath == "" {
		c.VolumeMgrConfig.VolumeDBPath = c.DBPath + "/volumedb"
	}
	if c.RaftConfig.RaftDBPath == "" {
		c.RaftConfig.RaftDBPath = c.DBPath + "/raftdb"
	}
	if c.KvDBPath == "" {
		c.KvDBPath = c.DBPath + "/kvdb"
	}

	return
}

func (s *Service) waitForRaftStart() {
	// wait for election
	<-s.raftStartCh
	log.Info("receive leader change success")

	// wait for wal log replay
	for {
		err := s.raftNode.ReadIndex(context.Background())
		if err == nil {
			break
		}
		log.Error("raftNode read index failed: ", err)
	}

	log.Info("raft start success")
}

// forwardToLeader will forward http request to raft leader
func (s *Service) forwardToLeader(w http.ResponseWriter, req *http.Request) {
	url, err := url.Parse(s.RaftConfig.RaftNodeConfig.NodeProtocol + req.RequestURI)
	if err != nil {
		panic("parse leader host url failed: " + err.Error())
	}
	url.Host = s.raftNode.GetLeaderHost()

	// without leader, then return special error
	if url.Host == "" {
		rpc.ReplyErr(w, apierrors.CodeNoLeader, apierrors.ErrNoLeader.Error())
		return
	}

	log.Infof("forward url: %v", url)

	proxy := httpproxy.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL = url
		},
	}

	proxy.ServeHTTP(w, req)
}

// service loop use for updating clusterInfo in consul timely
// also, it will trigger heartbeat change callback to volumeMgr
func (s *Service) loop() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "service-loop")

	if s.ClusterReportIntervalS == 0 {
		s.ClusterReportIntervalS = defaultClusterReportIntervalS
	}
	if s.HeartbeatNotifyIntervalS == 0 {
		s.HeartbeatNotifyIntervalS = defaultHeartbeatNotifyIntervalS
	}
	if s.MaxHeartbeatNotifyNum <= 0 {
		s.MaxHeartbeatNotifyNum = defaultMaxHeartbeatNotifyNum
	}
	if s.MetricReportIntervalM <= 0 {
		s.MetricReportIntervalM = defaultMetricReportIntervalM
	}
	if s.ConsistentCheckIntervalM <= 0 {
		s.ConsistentCheckIntervalM = defaultCheckConsistentIntervalM
	}

	reportTicker := time.NewTicker(time.Duration(s.ClusterReportIntervalS) * time.Second)
	defer reportTicker.Stop()
	heartbeatNotifyTicker := time.NewTicker(time.Duration(s.HeartbeatNotifyIntervalS) * time.Second)
	defer heartbeatNotifyTicker.Stop()

	metricReportTicker := time.NewTicker(time.Duration(s.MetricReportIntervalM) * time.Minute)
	defer metricReportTicker.Stop()

	checkTicker := time.NewTicker(time.Duration(s.ConsistentCheckIntervalM) * time.Minute)
	defer checkTicker.Stop()

	for {
		select {
		case <-reportTicker.C:
			if !s.raftNode.IsLeader() || s.ConsulAgentAddr == "" {
				continue
			}
			clusterInfo := clustermgr.ClusterInfo{
				Region:    s.Region,
				ClusterID: s.ClusterID,
				Readonly:  s.Readonly,
				Nodes:     make([]string, 0),
			}
			spaceStatInfo := s.DiskMgr.Stat(ctx)
			clusterInfo.Capacity = spaceStatInfo.TotalSpace
			clusterInfo.Available = spaceStatInfo.WritableSpace
			// filter learner node
			peers := s.raftNode.Status().Peers
			peersM := make(map[uint64]raftserver.Peer)
			for i := range peers {
				peersM[peers[i].Id] = peers[i]
			}
			for id, node := range s.raftNode.GetNodes() {
				if peersM[id].IsLearner {
					continue
				}
				clusterInfo.Nodes = append(clusterInfo.Nodes, s.RaftConfig.RaftNodeConfig.NodeProtocol+node)
			}

			val, err := json.Marshal(clusterInfo)
			if err != nil {
				span.Error("json marshal clusterInfo failed, err: ", err)
				break
			}

			clusterKey := clustermgr.GetConsulClusterPath(s.Region) + s.ClusterID.ToString()
			_, err = s.consulClient.KV().Put(&api.KVPair{Key: clusterKey, Value: val}, nil)
			if err != nil {
				span.Error("update clusterInfo into consul failed, err: ", err)
			}
		case <-heartbeatNotifyTicker.C:
			if !s.raftNode.IsLeader() {
				continue
			}
			changes := s.DiskMgr.GetHeartbeatChangeDisks()
			// report heartbeat change metric
			s.reportHeartbeatChange(float64(len(changes)))
			// in some case, like cm's network problem, it may trigger a mounts of disk heartbeat change
			// in this situation, we need to ignore it and do some alert
			if len(changes) > s.MaxHeartbeatNotifyNum {
				span.Error("a lots of disk heartbeat change happen: ", changes)
				continue
			}
			for i := range changes {
				span.Debugf("notify disk heartbeat change, change info: %v", changes[i])
				err := s.VolumeMgr.DiskWritableChange(ctx, changes[i].DiskID)
				if err != nil {
					span.Error("notify disk heartbeat change failed, err: ", err)
				}
			}
		case <-metricReportTicker.C:
			s.metricReport(ctx)
		case <-checkTicker.C:
			if !s.raftNode.IsLeader() {
				continue
			}
			go func() {
				clis := make([]*clustermgr.Client, 0)
				peers := s.raftNode.Status().Peers
				peersM := make(map[uint64]raftserver.Peer)
				for i := range peers {
					peersM[peers[i].Id] = peers[i]
				}
				for id, node := range s.raftNode.GetNodes() {
					if peersM[id].IsLearner {
						continue
					}
					host := s.RaftConfig.RaftNodeConfig.NodeProtocol + node
					cli := clustermgr.New(&clustermgr.Config{LbConfig: rpc.LbConfig{Hosts: []string{host}}})
					clis = append(clis, cli)
				}
				if len(clis) <= 1 {
					return
				}

				iVids, err := s.checkVolInfos(ctx, clis)
				if err != nil {
					span.Errorf("get checkVolInfos failed:%v", err)
					return
				}

				if len(iVids) != 0 {
					// readIndex request may be aggregated,which could temporarily lead to each nodes volume info not equal
					// so use get volume do double check
					actualIVids, err := s.doubleCheckVolInfos(ctx, clis, iVids)
					if err != nil {
						span.Errorf("double check vids:%v volume info failed:%v", iVids, err)
						return
					}
					if len(actualIVids) != 0 {
						s.reportInConsistentVols(actualIVids)
					}
				}
			}()

		case <-s.closeCh:
			return
		}
	}
}

func (s *Service) metricReport(ctx context.Context) {
	isLeader := strconv.FormatBool(s.raftNode.IsLeader())
	s.report(ctx)
	s.VolumeMgr.Report(ctx, s.Region, s.ClusterID)
	s.DiskMgr.Report(ctx, s.Region, s.ClusterID, isLeader)
}

func (s *Service) checkVolInfos(ctx context.Context, clis []*clustermgr.Client) ([]proto.Vid, error) {
	span := trace.SpanFromContextSafe(ctx)
	inconsistentVids := make([]proto.Vid, 0)
	marker := proto.Vid(0)
	listCnt := 2000
	volInfos := make([]clustermgr.ListVolumes, len(clis))
	for {
		var (
			nextMarker proto.Vid
			lastCnt    int
			err        error
		)
		for i, cli := range clis {
			if err = retry.Timed(3, 200).On(func() error {
				volInfos[i], err = cli.ListVolume(ctx, &clustermgr.ListVolumeArgs{Marker: marker, Count: listCnt})
				return err
			}); err != nil {
				span.Errorf("list volume: marker[%d], listCnt[%d], code[%d], error[%v]",
					marker, listCnt, rpc.DetectStatusCode(err), err)
				return nil, err
			}
			if len(volInfos[i].Volumes) < listCnt {
				lastCnt = len(volInfos[i].Volumes)
			}
			nextMarker = volInfos[i].Marker
		}

		inconsistentVids = append(inconsistentVids, getInconsistent(ctx, volInfos)...)

		if lastCnt < listCnt || nextMarker == proto.Vid(0) {
			span.Debugf("list volume finished, last marker vid is:%d,last list cnt:%d", marker, lastCnt)
			break
		}
		marker = nextMarker
	}

	return inconsistentVids, nil
}

func (s *Service) doubleCheckVolInfos(ctx context.Context, clis []*clustermgr.Client, vids []proto.Vid) (iVids []proto.Vid, err error) {
	span := trace.SpanFromContextSafe(ctx)
	for _, vid := range vids {
		vidInfo := make([]*clustermgr.VolumeInfo, len(clis))
		for i, cli := range clis {
			vidInfo[i], err = cli.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
			if err != nil {
				span.Errorf("get vid:%d info failed:%v", vid, err)
				return
			}
		}
		for i := 1; i < len(vidInfo); i++ {
			if !vidInfo[0].Equal(vidInfo[i]) {
				iVids = append(iVids, vidInfo[0].Vid)
			}
		}
	}
	return
}

func getInconsistent(ctx context.Context, allVols []clustermgr.ListVolumes) []proto.Vid {
	span := trace.SpanFromContextSafe(ctx)
	inConsistentVids := make([]proto.Vid, 0)
	if len(allVols) <= 1 {
		return nil
	}

	// if allVols's volumes length not match, add all volumes to inconsistenVids
	volLen := len(allVols[0].Volumes)
	for i := 1; i < len(allVols); i++ {
		if len(allVols[i].Volumes) != volLen {
			for _, vol := range allVols[0].Volumes {
				inConsistentVids = append(inConsistentVids, vol.Vid)
			}
			span.Error("list volume length not match")
			return inConsistentVids
		}
	}

	for i := 0; i < len(allVols[0].Volumes); i++ {
		for j := 1; j < len(allVols); j++ {
			if !allVols[0].Volumes[i].Equal(allVols[j].Volumes[i]) {
				span.Errorf("volume not match,src:%v, dst:%v", allVols[j].Volumes[i], allVols[0].Volumes[i])
				inConsistentVids = append(inConsistentVids, allVols[0].Volumes[i].Vid)
			}
		}
	}
	return inConsistentVids
}
