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
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

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

// IsValid returns valid algorithm or not.
func (alg AlgChoose) IsValid() bool {
	return alg > minAlg && alg < maxAlg
}

func (alg AlgChoose) String() string {
	switch alg {
	case AlgAvailable:
		return "Available"
	case AlgRandom:
		return "Random"
	default:
		return "Unknow"
	}
}

// errors
var (
	ErrNoSuchCluster      = errors.New("controller: no such cluster")
	ErrNoClusterAvailable = errors.New("controller: no cluster available")
	ErrInvalidChooseAlg   = errors.New("controller: invalid cluster chosen algorithm")
)

// ClusterController controller of clusters in one region
type ClusterController interface {
	// Region returns region in configuration
	Region() string
	// All returns all cluster info in this region
	All() []*cmapi.ClusterInfo
	// ChooseOne returns a available cluster to upload
	ChooseOne() (*cmapi.ClusterInfo, error)
	// GetServiceController return ServiceController in specified cluster
	GetServiceController(clusterID proto.ClusterID) (ServiceController, error)
	// GetVolumeGetter return VolumeGetter in specified cluster
	GetVolumeGetter(clusterID proto.ClusterID) (VolumeGetter, error)
	// GetConfig get specified config of key from cluster manager
	GetConfig(ctx context.Context, key string) (string, error)
	// ChangeChooseAlg change alloc algorithm
	ChangeChooseAlg(alg AlgChoose) error
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
	ServiceReloadSecs int          `json:"service_reload_secs"`
	CMClientConfig    cmapi.Config `json:"clustermgr_client_config"`

	ServicePunishThreshold      uint32 `json:"service_punish_threshold"`
	ServicePunishValidIntervalS int    `json:"service_punish_valid_interval_s"`

	ConsulAgentAddr string    `json:"consul_agent_addr"`
	Clusters        []Cluster `json:"clusters"`
}

// Cluster cluster config, each clusterID related to hosts list
type Cluster struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Hosts     []string        `json:"hosts"`
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
	clusters        atomic.Value // all clusters
	available       atomic.Value // available clusters
	serviceMgrs     sync.Map
	volumeGetters   sync.Map
	roundRobinCount uint64 // a count for round robin
	proxy           proxy.Cacher
	stopCh          <-chan struct{}

	config ClusterConfig
}

// NewClusterController returns a cluster controller
func NewClusterController(cfg *ClusterConfig, proxy proxy.Cacher, stopCh <-chan struct{}) (ClusterController, error) {
	defaulter.LessOrEqual(&cfg.ClusterReloadSecs, int(3))

	consulConf := api.DefaultConfig()
	consulConf.Address = cfg.ConsulAgentAddr
	var client *api.Client
	var err error
	if consulConf.Address != "" {
		client, err = api.NewClient(consulConf)
		if err != nil {
			return nil, fmt.Errorf("new consul client failed, err: %v", err)
		}
	}
	controller := &clusterControllerImpl{
		region:   cfg.Region,
		kvClient: client,
		proxy:    proxy,
		stopCh:   stopCh,
		config:   *cfg,
	}
	atomic.StoreUint32(&controller.allocAlg, uint32(AlgAvailable))

	f := controller.loadWithConfig
	if client != nil {
		f = controller.loadWithConsul
	}
	if err := f(); err != nil {
		return nil, errors.Base(err, "load cluster failed")
	}

	if stopCh == nil {
		return controller, nil
	}
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
		cmCli := cmapi.New(&conf)

		stat, err := cmCli.Stat(ctx)
		if err != nil {
			span.Warnf("get cluster info from clusterMgr failed: %v, clusterID: %d", err, cs.ClusterID)
			continue
		}
		clusterInfo := &cmapi.ClusterInfo{}
		clusterInfo.ClusterID = cs.ClusterID
		clusterInfo.Capacity = stat.SpaceStat.TotalSpace
		clusterInfo.Available = stat.SpaceStat.WritableSpace
		clusterInfo.Nodes = cs.Hosts
		clusterInfo.Readonly = stat.ReadOnly

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

		clusterID, err := strconv.Atoi(clusterKey)
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

func (c *clusterControllerImpl) deal(ctx context.Context, available []*cmapi.ClusterInfo, allClusters clusterMap, totalAvailable int64) error {
	span := trace.SpanFromContextSafe(ctx)

	sort.Slice(available, func(i, j int) bool {
		return available[i].Capacity < available[j].Capacity
	})

	newClusters := make([]*cmapi.ClusterInfo, 0, len(allClusters))
	for clusterID := range allClusters {
		if _, ok := c.serviceMgrs.Load(clusterID); !ok {
			newClusters = append(newClusters, allClusters[clusterID].clusterInfo)
		}
	}

	for _, newCluster := range newClusters {
		clusterID := newCluster.ClusterID

		if allClusters[clusterID].client == nil {
			conf := c.config.CMClientConfig
			conf.Hosts = newCluster.Nodes
			allClusters[clusterID].client = cmapi.New(&conf)
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

		serviceController, err := NewServiceController(ServiceConfig{
			ClusterID:                   clusterID,
			IDC:                         c.config.IDC,
			ReloadSec:                   c.config.ServiceReloadSecs,
			ServicePunishThreshold:      c.config.ServicePunishThreshold,
			ServicePunishValidIntervalS: c.config.ServicePunishValidIntervalS,
		}, cmCli, c.stopCh)
		if err != nil {
			removeThisCluster()
			span.Warn("new service manager failed", clusterID, err)
			continue
		}

		volumeGetter, err := NewVolumeGetter(clusterID, serviceController, c.proxy, -1)
		if err != nil {
			removeThisCluster()
			span.Warn("new volume getter failed", clusterID, err)
			continue
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

func (c *clusterControllerImpl) ChooseOne() (*cmapi.ClusterInfo, error) {
	alg := AlgChoose(atomic.LoadUint32(&c.allocAlg))

	switch alg {
	case AlgAvailable:
		totalAvailable := atomic.LoadInt64(&c.totalAvailable)
		if totalAvailable <= 0 {
			return nil, fmt.Errorf("no available space %d", totalAvailable)
		}
		randValue := rand.Int63n(totalAvailable)
		available := c.available.Load().(clusterQueue)
		for _, cluster := range available {
			if cluster.Available >= randValue {
				return cluster, nil
			}
			randValue -= cluster.Available
		}

	case AlgRoundRobin:
		available := c.available.Load().(clusterQueue)
		if length := uint64(len(available)); length > 0 {
			count := atomic.AddUint64(&c.roundRobinCount, 1)
			return available[count%length], nil
		}

	case AlgRandom:
		available := c.available.Load().(clusterQueue)
		if length := int64(len(available)); length > 0 {
			return available[rand.Int63()%length], nil
		}

	default:
		return nil, fmt.Errorf("not implemented algorithm %s(%d)", alg.String(), alg)
	}

	return nil, fmt.Errorf("no available cluster by %s", alg.String())
}

func (c *clusterControllerImpl) ChangeChooseAlg(alg AlgChoose) error {
	if !alg.IsValid() {
		return ErrInvalidChooseAlg
	}

	atomic.StoreUint32(&c.allocAlg, uint32(alg))
	return nil
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
