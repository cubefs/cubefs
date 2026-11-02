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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// just for cli write to readonly initialised cluster.
var ForceWriteReadonlyCluster = false

// codeModePolicyReloadMinInterval and codeModePolicyReloadMaxInterval bound the
// background reload interval of codemode policies.
const (
	codeModePolicyReloadMinInterval = 2 * time.Hour
	codeModePolicyReloadMaxInterval = 4 * time.Hour
)

// codeModePolicyReloadInterval picks a random reload interval in
// [codeModePolicyReloadMinInterval, codeModePolicyReloadMaxInterval), so that
// access instances do not reload codemode policies simultaneously.
func codeModePolicyReloadInterval() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	delta := int64(codeModePolicyReloadMaxInterval - codeModePolicyReloadMinInterval)
	return codeModePolicyReloadMinInterval + time.Duration(r.Int63n(delta))
}

// AlgChoose algorithm of choose cluster
type AlgChoose uint32

const (
	minAlg AlgChoose = iota
	// AlgAvailable available capacity and some random alloc
	AlgAvailable
	// AlgRoundRobin alloc cluster round robin
	AlgRoundRobin
	// AlgRandom completely random alloc
	AlgRandom
	maxAlg
)

type keyClient struct {
	key string
	cli *cmapi.Client
}

var cachedClient = struct {
	mu    sync.Mutex
	cache map[proto.ClusterID]*keyClient
}{cache: make(map[proto.ClusterID]*keyClient)}

func getClusterClient(ctx context.Context, clusterID proto.ClusterID, conf cmapi.Config) *cmapi.Client {
	hosts := make([]string, len(conf.Hosts))
	copy(hosts, conf.Hosts[:])
	sort.Strings(hosts)
	key := strings.Join(hosts, "-")

	cachedClient.mu.Lock()
	defer cachedClient.mu.Unlock()
	keyCli, ok := cachedClient.cache[clusterID]
	if !ok {
		cli := cmapi.New(&conf)
		cachedClient.cache[clusterID] = &keyClient{key: key, cli: cli}
		return cli
	}
	if keyCli.key != key {
		span := trace.SpanFromContextSafe(ctx)
		span.Warnf("change cluster(%d) clustermgr hosts %s -> %s", clusterID, keyCli.key, key)
		cli := cmapi.New(&conf)
		keyCli.key = key
		*keyCli.cli = *cli
	}
	return keyCli.cli
}

// IsValid returns valid algorithm or not.
func (alg AlgChoose) IsValid() bool {
	return alg > minAlg && alg < maxAlg
}

func (alg AlgChoose) String() string {
	switch alg {
	case AlgAvailable:
		return "Available"
	case AlgRoundRobin:
		return "RoundRobin"
	case AlgRandom:
		return "Random"
	default:
		return "Unknow"
	}
}

// errors
var (
	ErrNoSuchCluster       = errors.New("controller: no such cluster")
	ErrNoClusterAvailable  = errors.New("controller: no cluster available")
	ErrNoCodeModeAvailable = errors.New("controller: no enabled codemode policy loaded from any cluster")
)

// ParseClusterChooseAlg parses cluster_choose_alg from access JSON config.
// Unrecognized values default to AlgAvailable.
func ParseClusterChooseAlg(s string) AlgChoose {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "roundrobin":
		return AlgRoundRobin
	case "random":
		return AlgRandom
	default:
		return AlgAvailable
	}
}

// ClusterController controller of clusters in one region
type ClusterController interface {
	// Region returns region in configuration
	Region() string
	// All returns all cluster info in this region
	All() []*cmapi.ClusterInfo
	// ChooseOne resolves the upload target for a given (size, codeMode, clusterID) triple.
	// It covers all four combinations of known/unknown inputs:
	//
	//   (size, None, 0)   → choose the best (cluster, mode) pair globally by size
	//   (size, mode, 0)   → choose a cluster that enables this mode (size is ignored)
	//   (size, None, cid) → choose a mode from the specified cluster's policies by size
	//   (size, mode, cid) → validate that this cluster enables this mode (size is ignored)
	//
	// Returns (resolved ClusterID, resolved CodeMode, error). Always returns error when no
	// valid candidate satisfies the constraints.
	ChooseOne(size int64, codeMode codemode.CodeMode, clusterID proto.ClusterID) (proto.ClusterID, codemode.CodeMode, error)
	// GetEncoder returns the EC encoder for the given CodeMode.
	// Panics if the encoder was not initialised during cluster policy load.
	GetEncoder(cm codemode.CodeMode) ec.Encoder
	// EnsureEncoder loads codemode policy for clusterID when decoder is missing (read path).
	EnsureEncoder(ctx context.Context, clusterID proto.ClusterID, mode codemode.CodeMode) error
	// GetServiceController return ServiceController in specified cluster
	GetServiceController(clusterID proto.ClusterID) (ServiceController, error)
	// GetVolumeGetter return VolumeGetter in specified cluster
	GetVolumeGetter(clusterID proto.ClusterID) (VolumeGetter, error)
	// GetConfig get specified config of key from cluster manager
	GetConfig(ctx context.Context, key string) (string, error)
	// GetShardController return IShardController in specified cluster
	GetShardController(clusterID proto.ClusterID) (IShardController, error)
	// MaxPolicySize returns the max enabled policy MaxSize across all loaded
	// clusters. Zero when no enabled policy has been loaded yet.
	MaxPolicySize() int64
	// ListCodeModes returns enabled CodeModes on currently writable clusters.
	ListCodeModes() map[codemode.CodeMode][]proto.ClusterID
}

// ClusterConfig cluster config
//
// Region and RegionMagic are paired,
// magic cannot change if one region was deployed.
type ClusterConfig struct {
	IDC               string       `json:"-"` // passing by stream config.
	Region            string       `json:"region"`
	RegionMagic       string       `json:"region_magic"`
	ClusterReloadSecs int          `json:"cluster_reload_secs"`
	ShardReloadSecs   int          `json:"shard_reload_secs"`
	CMClientConfig    cmapi.Config `json:"clustermgr_client_config"`

	ServiceConfig
	VolumeConfig

	ConsulAgentAddr string    `json:"consul_agent_addr"`
	ConsulToken     string    `json:"consul_token"`
	ConsulTokenFile string    `json:"consul_token_file"`
	Clusters        []Cluster `json:"clusters"`

	// ClusterChooseAlg selects how Access picks a cluster for Put/Alloc without assign_cluster_id.
	ClusterChooseAlg string `json:"cluster_choose_alg"`

	// EC encoder configuration shared across all CodeModes.
	EncoderEnableVerify bool `json:"-"`
	EncoderConcurrency  int  `json:"-"`
}

// Cluster cluster config, each clusterID related to hosts list
type Cluster struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Hosts     []string        `json:"hosts"`
	Space     SpaceConf       `json:"space"` // one space - one cluster
}

type cluster struct {
	clusterInfo *cmapi.ClusterInfo
	client      *cmapi.Client
}

type clusterMap map[proto.ClusterID]*cluster

type clusterQueue []*cmapi.ClusterInfo

type clusterControllerImpl struct {
	region          string
	kvClient        *api.Client
	allocAlg        uint32
	totalAvailable  int64        // available space of all clusters
	maxPolicySize   int64        // max enabled policy MaxSize across all clusters
	clusters        atomic.Value // all clusters
	available       atomic.Value // available clusters
	serviceMgrs     sync.Map
	volumeGetters   sync.Map
	shardMgrs       sync.Map // cid -> shardController
	roundRobinCount uint64   // a count for round robin
	proxy           proxy.Cacher
	stopCh          <-chan struct{}

	// clusterCodeModes caches per-cluster codemode policies loaded from clustermgr.
	// key: proto.ClusterID, value: []codemode.Policy
	clusterCodeModes sync.Map

	// encoders caches EC encoders indexed by CodeMode (uint8).
	// Each slot stores an ec.Encoder; nil Load means not initialised.
	encoders [256]atomic.Value

	// policyMu serializes codemode policy reload across all clusters.
	policyMu sync.Mutex

	config ClusterConfig
}

// NewClusterController returns a cluster controller
func NewClusterController(cfg *ClusterConfig, proxy proxy.Cacher, stopCh <-chan struct{}) (ClusterController, error) {
	defaulter.LessOrEqual(&cfg.ClusterReloadSecs, int(3))

	consulConf := api.DefaultConfig()
	consulConf.Address = cfg.ConsulAgentAddr
	if cfg.ConsulTokenFile != "" {
		consulConf.TokenFile = cfg.ConsulTokenFile
	}
	if cfg.ConsulToken != "" {
		consulConf.Token = cfg.ConsulToken
	}
	var client *api.Client
	var err error
	if cfg.ConsulAgentAddr != "" {
		client, err = api.NewClient(consulConf)
		if err != nil {
			return nil, fmt.Errorf("new consul client failed, err: %v", err)
		}
	}
	alg := ParseClusterChooseAlg(cfg.ClusterChooseAlg)

	controller := &clusterControllerImpl{
		region:   cfg.Region,
		kvClient: client,
		proxy:    proxy,
		stopCh:   stopCh,
		config:   *cfg,
		allocAlg: uint32(alg),
	}

	f := controller.loadWithConfig
	if client != nil {
		f = controller.loadWithConsul
	}
	if err := f(); err != nil {
		return nil, errors.Base(err, "load cluster failed")
	}
	if !controller.hasEnabledPolicy() {
		return nil, ErrNoCodeModeAvailable
	}

	if stopCh == nil {
		return controller, nil
	}
	go controller.runCodeModePolicyPeriodicReload()
	go func() {
		tick := time.NewTicker(time.Duration(cfg.ClusterReloadSecs) * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if err := f(); err != nil {
					log.Warn("load timer error", err)
				}
			case <-controller.stopCh:
				return
			}
		}
	}()
	log.Infof("cluster controller initialized, alg: %s", alg.String())
	return controller, nil
}

func (c *clusterControllerImpl) loadWithConfig() error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	if len(c.config.Clusters) == 0 {
		return ErrNoClusterAvailable
	}
	span.Debugf("clusters info: %+v", c.config.Clusters)
	allClusters := make(clusterMap)
	available := make([]*cmapi.ClusterInfo, 0, len(c.config.Clusters))
	totalAvailable := int64(0)
	for _, cs := range c.config.Clusters {
		conf := c.config.CMClientConfig
		conf.Hosts = cs.Hosts
		cmCli := getClusterClient(ctx, cs.ClusterID, conf)

		stat, err := cmCli.Stat(ctx)
		if err != nil {
			span.Warnf("get cluster info from clusterMgr failed: %v, clusterID: %d", err, cs.ClusterID)
			continue
		}
		clusterInfo := &cmapi.ClusterInfo{}
		clusterInfo.ClusterID = cs.ClusterID
		clusterInfo.Capacity = stat.BlobNodeSpaceStat.TotalSpace
		clusterInfo.Available = stat.BlobNodeSpaceStat.WritableSpace
		clusterInfo.Nodes = cs.Hosts
		clusterInfo.Readonly = stat.ReadOnly
		if clusterInfo.Readonly && ForceWriteReadonlyCluster {
			clusterInfo.Readonly = false
		}

		allClusters[cs.ClusterID] = &cluster{client: cmCli, clusterInfo: clusterInfo}
		if !clusterInfo.Readonly && clusterInfo.Available > 0 {
			available = append(available, clusterInfo)
			totalAvailable += clusterInfo.Available
			allClusters[clusterInfo.ClusterID].client = cmCli
		} else {
			span.Debug("readonly or no available cluster", clusterInfo.ClusterID)
		}
	}
	return c.deal(ctx, available, allClusters, totalAvailable)
}

func (c *clusterControllerImpl) loadWithConsul() error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	path := cmapi.GetConsulClusterPath(c.region)
	span.Debug("to list consul path", path)

	pairs, _, err := c.kvClient.KV().List(path, nil)
	if err != nil {
		return err
	}
	span.Debugf("found %d clusters", len(pairs))

	allClusters := make(clusterMap)
	available := make([]*cmapi.ClusterInfo, 0, len(pairs))
	totalAvailable := int64(0)
	for _, pair := range pairs {
		clusterInfo := &cmapi.ClusterInfo{}
		err := json.Unmarshal(pair.Value, clusterInfo)
		if err != nil {
			span.Warnf("decode failed, raw:%s, error:%s", string(pair.Value), err.Error())
			continue
		}

		clusterKey := filepath.Base(pair.Key)
		span.Debug("found cluster", clusterKey)

		clusterID, err := strconv.ParseUint(clusterKey, 10, 32)
		if err != nil {
			span.Warn("invalid cluster id", clusterKey, err)
			continue
		}
		if clusterInfo.ClusterID != proto.ClusterID(clusterID) {
			span.Warn("mismatch cluster id", clusterInfo.ClusterID, clusterID)
			continue
		}

		allClusters[proto.ClusterID(clusterID)] = &cluster{clusterInfo: clusterInfo}
		if !clusterInfo.Readonly && clusterInfo.Available > 0 {
			available = append(available, clusterInfo)
			totalAvailable += clusterInfo.Available
		} else {
			span.Debug("readonly or no available cluster", clusterID)
		}
	}
	return c.deal(ctx, available, allClusters, totalAvailable)
}

func (c *clusterControllerImpl) deal(ctx context.Context,
	available []*cmapi.ClusterInfo, allClusters clusterMap, totalAvailable int64,
) error {
	if len(allClusters) == 0 {
		return nil
	}
	span := trace.SpanFromContextSafe(ctx)

	sort.Slice(available, func(i, j int) bool {
		return available[i].Capacity < available[j].Capacity
	})

	newClusters := make([]*cmapi.ClusterInfo, 0, len(allClusters))
	for clusterID, cluster := range allClusters {
		if _, ok := c.serviceMgrs.Load(clusterID); !ok {
			newClusters = append(newClusters, cluster.clusterInfo)
		} else {
			// update cluster client if hosts changed
			conf := c.config.CMClientConfig
			conf.Hosts = cluster.clusterInfo.Nodes[:]
			cluster.client = getClusterClient(ctx, clusterID, conf)
		}
	}

	for _, newCluster := range newClusters {
		clusterID := newCluster.ClusterID

		if allClusters[clusterID].client == nil {
			conf := c.config.CMClientConfig
			conf.Hosts = newCluster.Nodes
			allClusters[clusterID].client = getClusterClient(ctx, clusterID, conf)
		}

		cmCli := allClusters[clusterID].client

		removeThisCluster := func() {
			delete(allClusters, clusterID)
			if !newCluster.Readonly && newCluster.Available > 0 {
				totalAvailable -= newCluster.Available
				for j := range available {
					if available[j].ClusterID == clusterID {
						available = append(available[:j], available[j+1:]...)
						break
					}
				}
			}
		}

		serviceConfig := c.config.ServiceConfig
		serviceConfig.ClusterID = clusterID
		serviceConfig.IDC = c.config.IDC
		serviceController, err := NewServiceController(serviceConfig, cmCli, c.proxy, c.stopCh)
		if err != nil {
			removeThisCluster()
			span.Warn("new service manager failed", clusterID, err)
			continue
		}

		volumeConfig := c.config.VolumeConfig
		volumeConfig.ClusterID = clusterID
		volumeGetter, err := NewVolumeGetter(volumeConfig, serviceController, c.proxy, c.stopCh)
		if err != nil {
			removeThisCluster()
			span.Warn("new volume getter failed", clusterID, err)
			continue
		}

		if err := c.loadCodeModePolicy(ctx, clusterID, cmCli, false, nil); err != nil {
			removeThisCluster()
			span.Warnf("load codemode policy for cluster(%d) failed: %v", clusterID, err)
			continue
		}

		space := c.getSpaceConf(clusterID)
		span.Debugf("get space config valid:%t, cluster:%d", space.IsValid(), clusterID)
		if space.IsValid() { // need shard node, meta system
			shardMgr, err := NewShardController(shardCtrlConf{
				clusterID:  clusterID,
				reloadSecs: c.config.ShardReloadSecs,
				space:      space,
			}, cmCli, serviceController, c.stopCh)
			if err != nil {
				span.Fatalf("new shard controller failed, clusterID=%d, err=%+v", clusterID, err)
			}
			c.shardMgrs.Store(clusterID, shardMgr)
		}

		c.serviceMgrs.Store(clusterID, serviceController)
		c.volumeGetters.Store(clusterID, volumeGetter)
		span.Debug("loaded new cluster", clusterID)
	}

	c.clusters.Store(allClusters)
	c.available.Store(clusterQueue(available))
	atomic.StoreInt64(&c.totalAvailable, totalAvailable)

	span.Infof("loaded %d clusters, and %d available, total available space %.3fTB",
		len(allClusters), len(available), float64(totalAvailable)/(1<<40))
	return nil
}

func (c *clusterControllerImpl) MaxPolicySize() int64 {
	return atomic.LoadInt64(&c.maxPolicySize)
}

func (c *clusterControllerImpl) GetEncoder(cm codemode.CodeMode) ec.Encoder {
	val := c.encoders[cm].Load()
	if val == nil {
		panic(fmt.Sprintf("encoder not found for codemode %d", cm))
	}
	return val.(ec.Encoder)
}

func (c *clusterControllerImpl) EnsureEncoder(
	ctx context.Context, clusterID proto.ClusterID, mode codemode.CodeMode,
) error {
	if c.encoders[mode].Load() != nil {
		return nil
	}
	cmCli, err := c.cmClient(clusterID)
	if err != nil {
		return err
	}
	span := trace.SpanFromContextSafe(ctx)
	if err = c.loadCodeModePolicy(ctx, clusterID, cmCli, false, func() bool {
		return c.encoders[mode].Load() != nil
	}); err != nil {
		span.Warnf("passive codemode policy reload cluster(%d) mode(%d) failed: %v", clusterID, mode, err)
		return err
	}
	span.Infof("passive codemode policy reload cluster(%d) mode(%d) ok", clusterID, mode)
	return nil
}

func (c *clusterControllerImpl) Region() string {
	return c.region
}

func (c *clusterControllerImpl) All() []*cmapi.ClusterInfo {
	allClusters := c.clusters.Load().(clusterMap)

	ret := make([]*cmapi.ClusterInfo, 0, len(allClusters))
	for _, clusterInfo := range allClusters {
		ret = append(ret, clusterInfo.clusterInfo)
	}

	return ret
}

// loadCodeModePolicy loads and applies codemode policies of clusterID under a
// process-wide mutex. With verify given, the reload is skipped when verify()
// already holds, and ErrIllegalArguments is returned when verify still fails
// after policies are applied.
func (c *clusterControllerImpl) loadCodeModePolicy(
	ctx context.Context, clusterID proto.ClusterID, cmCli *cmapi.Client, compareConfig bool, verify func() bool,
) error {
	c.policyMu.Lock()
	defer c.policyMu.Unlock()

	span := trace.SpanFromContextSafe(ctx)
	// Requests blocking on the lock may be raced by a finished reload, skip
	// when verify() already reports the requested mode as served.
	if verify != nil && verify() {
		return nil
	}
	policies, err := cmapi.LoadPolicies(ctx, cmCli)
	if err != nil {
		return err
	}
	if compareConfig {
		if cached, ok := c.clusterCodeModes.Load(clusterID); ok {
			if reflect.DeepEqual(policies, cached.([]codemode.Policy)) {
				span.Debugf("codemode config unchanged cluster(%d), skip periodic reload", clusterID)
				return nil
			}
		}
	}
	newMaxSize := int64(0)
	for _, p := range policies {
		codeMode := p.ModeName.GetCodeMode()
		if c.encoders[codeMode].Load() == nil {
			enc, encErr := ec.NewEncoder(ec.Config{
				CodeMode:     codeMode.Tactic(),
				EnableVerify: c.config.EncoderEnableVerify,
				Concurrency:  c.config.EncoderConcurrency,
			})
			if encErr != nil {
				return fmt.Errorf("init encoder for codemode(%d) failed: %w", codeMode, encErr)
			}
			c.encoders[codeMode].Store(enc)
		}
		if p.Enable && p.MaxSize > newMaxSize {
			newMaxSize = p.MaxSize
		}
	}
	c.clusterCodeModes.Store(clusterID, policies)
	for {
		old := atomic.LoadInt64(&c.maxPolicySize)
		if newMaxSize <= old {
			break
		}
		if atomic.CompareAndSwapInt64(&c.maxPolicySize, old, newMaxSize) {
			break
		}
	}
	if verify != nil && !verify() {
		span.Errorf("codemode policies reloaded for cluster(%d), requested mode still not served", clusterID)
		return errcode.ErrIllegalArguments
	}
	span.Infof("loaded codemode policies for cluster(%d): %+v, maxPolicySize=%d",
		clusterID, policies, atomic.LoadInt64(&c.maxPolicySize))
	return nil
}

func (c *clusterControllerImpl) cmClient(clusterID proto.ClusterID) (*cmapi.Client, error) {
	allClusters, ok := c.clusters.Load().(clusterMap)
	if !ok {
		return nil, ErrNoSuchCluster
	}
	cl, ok := allClusters[clusterID]
	if !ok || cl.client == nil {
		return nil, ErrNoSuchCluster
	}
	return cl.client, nil
}

// runCodeModePolicyPeriodicReload reloads policies of all managed clusters periodically.
func (c *clusterControllerImpl) runCodeModePolicyPeriodicReload() {
	tick := time.NewTicker(codeModePolicyReloadInterval())
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			c.reloadAllManagedCodeModePolicies(context.Background())
		case <-c.stopCh:
			return
		}
	}
}

func (c *clusterControllerImpl) reloadAllManagedCodeModePolicies(ctx context.Context) {
	span, ctx := trace.StartSpanFromContext(ctx, "")
	clusterIDs := make([]proto.ClusterID, 0)
	c.serviceMgrs.Range(func(key, _ any) bool {
		clusterIDs = append(clusterIDs, key.(proto.ClusterID))
		return true
	})
	for _, clusterID := range clusterIDs {
		cmCli, err := c.cmClient(clusterID)
		if err != nil {
			span.Warnf("periodic codemode policy reload cluster(%d): no client: %s", clusterID, err)
			continue
		}
		if err = c.loadCodeModePolicy(ctx, clusterID, cmCli, true, nil); err != nil {
			span.Warnf("periodic codemode policy reload cluster(%d) failed: %s", clusterID, err)
		}
	}
}

func (c *clusterControllerImpl) isClusterAvailable(clusterID proto.ClusterID) bool {
	available := c.available.Load().(clusterQueue)
	for _, ci := range available {
		if ci.ClusterID == clusterID {
			return true
		}
	}
	return false
}

// hasEnabledPolicy reports whether at least one enabled codemode policy has
// been loaded.
func (c *clusterControllerImpl) hasEnabledPolicy() bool {
	found := false
	c.clusterCodeModes.Range(func(_, val any) bool {
		policies, _ := val.([]codemode.Policy)
		for _, p := range policies {
			if p.Enable {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// clusterCandidate pairs a ClusterInfo with the codemode that satisfies the caller's constraints.
type clusterCandidate struct {
	ci   *cmapi.ClusterInfo
	mode codemode.CodeMode
}

// chooseFromCandidates selects one candidate using the configured choose algorithm.
// Available space is read from candidate.ci.Available for AlgAvailable.
func (c *clusterControllerImpl) chooseFromCandidates(candidates []clusterCandidate) (clusterCandidate, error) {
	alg := AlgChoose(atomic.LoadUint32(&c.allocAlg))

	switch alg {
	case AlgAvailable:
		total := int64(0)
		for _, cand := range candidates {
			total += cand.ci.Available
		}
		if total <= 0 {
			return clusterCandidate{}, errcode.ErrAccessNoClusterAvail
		}
		randValue := rand.Int63n(total)
		for _, cand := range candidates {
			if cand.ci.Available > randValue {
				return cand, nil
			}
			randValue -= cand.ci.Available
		}

	case AlgRoundRobin:
		if length := uint64(len(candidates)); length > 0 {
			count := atomic.AddUint64(&c.roundRobinCount, 1)
			return candidates[count%length], nil
		}

	case AlgRandom:
		if length := int64(len(candidates)); length > 0 {
			return candidates[rand.Int63()%length], nil
		}

	default:
		return clusterCandidate{}, errcode.ErrUnexpected
	}

	return clusterCandidate{}, errcode.ErrAccessNoClusterAvail
}

// pickModeFromPolicies finds a matching codemode from a cluster's policy list.
// If mode != None, it returns that mode when the cluster has it enabled; size is ignored.
// If mode == None, it returns the first enabled policy whose size range covers size.
// On no match, returns CodeModeNone.
func pickModeFromPolicies(policies []codemode.Policy, mode codemode.CodeMode, size int64) codemode.CodeMode {
	if mode != codemode.CodeModeNone {
		for _, p := range policies {
			if p.Enable && p.ModeName.GetCodeMode() == mode {
				return mode
			}
		}
		return codemode.CodeModeNone
	}

	for _, p := range policies {
		if !p.Enable {
			continue
		}
		if size >= p.MinSize && size <= p.MaxSize {
			return p.ModeName.GetCodeMode()
		}
	}

	return codemode.CodeModeNone
}

// ChooseOne resolves the upload target for the given (size, codeMode, clusterID) triple.
// See the ClusterController interface for the full contract.
func (c *clusterControllerImpl) ChooseOne(size int64, mode codemode.CodeMode, clusterID proto.ClusterID) (proto.ClusterID, codemode.CodeMode, error) {
	// Validate caller-specified mode early to avoid iterating policies needlessly.
	if mode != codemode.CodeModeNone {
		if !mode.IsValid() {
			return 0, codemode.CodeModeNone, errcode.ErrIllegalArguments
		}
		if c.encoders[mode].Load() == nil {
			return 0, codemode.CodeModeNone, errcode.ErrIllegalArguments
		}
	}

	if clusterID != 0 {
		val, ok := c.clusterCodeModes.Load(clusterID)
		if !ok {
			return 0, codemode.CodeModeNone, errcode.ErrIllegalArguments
		}
		if !c.isClusterAvailable(clusterID) {
			return 0, codemode.CodeModeNone, errcode.ErrAccessNoClusterAvail
		}
		// Cluster is known: validate or pick mode from that cluster's policies only.
		policies, _ := val.([]codemode.Policy)
		resolvedMode := pickModeFromPolicies(policies, mode, size)
		if resolvedMode == codemode.CodeModeNone {
			return 0, codemode.CodeModeNone, errcode.ErrAccessNoClusterAvail
		}
		return clusterID, resolvedMode, nil
	}

	// clusterID == 0: collect all available clusters that satisfy the constraints,
	// then apply the configured choose algorithm.
	available := c.available.Load().(clusterQueue)
	candidates := make([]clusterCandidate, 0, len(available))

	for _, ci := range available {
		val, ok := c.clusterCodeModes.Load(ci.ClusterID)
		if !ok {
			continue
		}
		policies, _ := val.([]codemode.Policy)
		resolvedMode := pickModeFromPolicies(policies, mode, size)
		if resolvedMode == codemode.CodeModeNone {
			continue
		}
		candidates = append(candidates, clusterCandidate{
			ci:   ci,
			mode: resolvedMode,
		})
	}

	if len(candidates) == 0 {
		// no available cluster can serve any policy for this size — service-side issue
		return 0, codemode.CodeModeNone, errcode.ErrAccessNoClusterAvail
	}

	chosen, err := c.chooseFromCandidates(candidates)
	if err != nil {
		return 0, codemode.CodeModeNone, err
	}
	return chosen.ci.ClusterID, chosen.mode, nil
}

func (c *clusterControllerImpl) ListCodeModes() map[codemode.CodeMode][]proto.ClusterID {
	grouped := make(map[codemode.CodeMode][]proto.ClusterID)
	available, ok := c.available.Load().(clusterQueue)
	if !ok {
		return grouped
	}

	for _, ci := range available {
		val, ok := c.clusterCodeModes.Load(ci.ClusterID)
		if !ok {
			continue
		}
		policies, _ := val.([]codemode.Policy)
		for _, p := range policies {
			if p.Enable {
				cm := p.ModeName.GetCodeMode()
				grouped[cm] = append(grouped[cm], ci.ClusterID)
			}
		}
	}
	return grouped
}

func (c *clusterControllerImpl) GetServiceController(clusterID proto.ClusterID) (ServiceController, error) {
	if serviceController, exist := c.serviceMgrs.Load(clusterID); exist {
		if controller, ok := serviceController.(ServiceController); ok {
			return controller, nil
		}
		return nil, fmt.Errorf("not service controller for %d", clusterID)
	}
	return nil, fmt.Errorf("no service controller of %d", clusterID)
}

func (c *clusterControllerImpl) GetVolumeGetter(clusterID proto.ClusterID) (VolumeGetter, error) {
	if volumeGetter, exist := c.volumeGetters.Load(clusterID); exist {
		if getter, ok := volumeGetter.(VolumeGetter); ok {
			return getter, nil
		}
		return nil, fmt.Errorf("not volume getter for %d", clusterID)
	}
	return nil, fmt.Errorf("no volume getter for %d", clusterID)
}

func (c *clusterControllerImpl) GetConfig(ctx context.Context, key string) (ret string, err error) {
	span := trace.SpanFromContextSafe(ctx)

	allClusters := c.clusters.Load().(clusterMap)
	if len(allClusters) == 0 {
		return "", ErrNoSuchCluster
	}

	for _, cluster := range allClusters {
		if ret, err = cluster.client.GetConfig(ctx, key); err == nil {
			return
		}
		span.Warnf("get config[%s] from cluster[%d] failed, err: %v", key, cluster.clusterInfo.ClusterID, err)
	}
	return
}

func (c *clusterControllerImpl) GetShardController(clusterID proto.ClusterID) (IShardController, error) {
	if shardMgr, exist := c.shardMgrs.Load(clusterID); exist {
		if controller, ok := shardMgr.(IShardController); ok {
			return controller, nil
		}
		return nil, fmt.Errorf("not shard controller for %d", clusterID)
	}
	return nil, fmt.Errorf("no shard controller of %d", clusterID)
}

func (c *clusterControllerImpl) getSpaceConf(clusterID proto.ClusterID) SpaceConf {
	for _, cs := range c.config.Clusters {
		if clusterID == cs.ClusterID {
			return cs.Space
		}
	}
	return SpaceConf{}
}
