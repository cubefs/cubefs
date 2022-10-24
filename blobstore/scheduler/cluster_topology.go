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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	shardCount = 32

	defaultMarker = proto.Vid(0)
	defaultCount  = 1000
)

// ErrFrequentlyUpdate frequently update
var ErrFrequentlyUpdate = errors.New("frequently update")

var errVolumeMissmatch = errors.New("volume missmatch during running task")

// IClusterTopology define the interface og cluster topology
type IClusterTopology interface {
	GetIDCs() map[string]*IDC
	GetIDCDisks(idc string) (disks []*client.DiskInfoSimple)
	MaxFreeChunksDisk(idc string) *client.DiskInfoSimple
	IsBrokenDisk(diskID proto.DiskID) bool
	IVolumeCache
	closer.Closer
}

// IVolumeCache define the interface used for volume cache manager
type IVolumeCache interface {
	UpdateVolume(vid proto.Vid) (*client.VolumeInfoSimple, error)
	GetVolume(vid proto.Vid) (*client.VolumeInfoSimple, error)
	LoadVolumes() error
}

type clusterTopologyConfig struct {
	ClusterID               proto.ClusterID
	Leader                  bool
	UpdateInterval          time.Duration
	VolumeUpdateInterval    time.Duration
	FreeChunkCounterBuckets []float64
}

// ClusterTopology cluster topology
type ClusterTopology struct {
	clusterID    proto.ClusterID
	idcMap       map[string]*IDC
	diskMap      map[string][]*client.DiskInfoSimple
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// IDC idc info
type IDC struct {
	name         string
	rackMap      map[string]*Rack
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// Rack rack info
type Rack struct {
	name         string
	diskMap      map[string]*Host
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// Host host info
type Host struct {
	host         string                   // ip+port
	disks        []*client.DiskInfoSimple // disk list
	FreeChunkCnt int64                    // host free chunk count
	MaxChunkCnt  int64                    // total chunk count
}

type (
	volumeTime struct {
		time   time.Time
		volume client.VolumeInfoSimple
	}
	shardCacher struct {
		sync.RWMutex
		m map[proto.Vid]*volumeTime
	}
	volumeCacher struct {
		interval time.Duration
		cache    [shardCount]*shardCacher
	}
)

func (vc *volumeCacher) getShard(vid proto.Vid) *shardCacher {
	return vc.cache[uint(vid)%shardCount]
}

func (vc *volumeCacher) Get(vid proto.Vid) (*client.VolumeInfoSimple, bool) {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if exist {
		return &val.volume, true
	}
	return nil, false
}

func (vc *volumeCacher) Set(vid proto.Vid, volume client.VolumeInfoSimple) {
	shard := vc.getShard(vid)
	shard.Lock()
	shard.m[vid] = &volumeTime{
		time:   time.Now(),
		volume: volume,
	}
	shard.Unlock()
}

func (vc *volumeCacher) Settable(vid proto.Vid) bool {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if !exist {
		return true
	}
	return time.Now().After(val.time.Add(vc.interval))
}

func newVolumeCacher(interval time.Duration) *volumeCacher {
	c := new(volumeCacher)
	c.interval = interval
	for idx := range c.cache {
		m := make(map[proto.Vid]*volumeTime, 32)
		c.cache[idx] = &shardCacher{m: m}
	}
	return c
}

// ClusterTopologyMgr cluster topology manager
type ClusterTopologyMgr struct {
	closer.Closer

	clusterMgrCli client.ClusterMgrAPI

	clusterTopology *ClusterTopology
	brokenDisks     *sync.Map
	volumeCache     IVolumeCache

	cfg *clusterTopologyConfig

	taskStatsMgr *base.ClusterTopologyStatsMgr
}

// NewClusterTopologyMgr returns cluster topology manager
func NewClusterTopologyMgr(topologyClient client.ClusterMgrAPI, cfg *clusterTopologyConfig) IClusterTopology {
	mgr := &ClusterTopologyMgr{
		Closer: closer.New(),

		clusterMgrCli: topologyClient,
		volumeCache:   NewVolumeCache(topologyClient, cfg.VolumeUpdateInterval),

		clusterTopology: &ClusterTopology{
			idcMap:  make(map[string]*IDC),
			diskMap: make(map[string][]*client.DiskInfoSimple),
		},
		brokenDisks:  &sync.Map{},
		cfg:          cfg,
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(cfg.ClusterID, cfg.FreeChunkCounterBuckets),
	}
	go mgr.loopUpdate()
	return mgr
}

func (m *ClusterTopologyMgr) loopUpdate() {
	t := time.NewTicker(m.cfg.UpdateInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.loadNormalDisks()
			m.loadBrokenDisks()
		case <-m.Closer.Done():
			return
		}
	}
}

func (m *ClusterTopologyMgr) loadNormalDisks() {
	if !m.cfg.Leader { // only leader need load normal disks
		return
	}
	span, ctx := trace.StartSpanFromContext(context.Background(), "loadNormalDisks")

	disks, err := m.clusterMgrCli.ListClusterDisks(ctx)
	if err != nil {
		span.Errorf("update cluster topology failed: err[%+v]", err)
		return
	}

	m.buildClusterTopology(disks, m.cfg.ClusterID)
}

func (m *ClusterTopologyMgr) loadBrokenDisks() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "loadBrokenDisks")
	brokenDiskIDs := make(map[proto.DiskID]struct{})

	// list broken disks
	brokenDisks, err := m.clusterMgrCli.ListBrokenDisks(ctx)
	if err != nil {
		span.Errorf("list broken disk failed: err[%+v]", err)
		return
	}
	for _, disk := range brokenDisks {
		brokenDiskIDs[disk.DiskID] = struct{}{}
	}
	// list repairing disks
	repairingDisks, err := m.clusterMgrCli.ListRepairingDisks(ctx)
	if err != nil {
		span.Errorf("list repairing disk failed: err[%+v]", err)
		return
	}
	for _, disk := range repairingDisks {
		brokenDiskIDs[disk.DiskID] = struct{}{}
	}
	// clear cache
	newBrokenDisks := &sync.Map{}
	for diskID := range brokenDiskIDs {
		newBrokenDisks.Store(diskID, struct{}{})
	}
	m.brokenDisks = newBrokenDisks
}

func (m *ClusterTopologyMgr) IsBrokenDisk(diskID proto.DiskID) bool {
	_, broken := m.brokenDisks.Load(diskID)
	return broken
}

func (m *ClusterTopologyMgr) UpdateVolume(vid proto.Vid) (*client.VolumeInfoSimple, error) {
	return m.volumeCache.UpdateVolume(vid)
}

func (m *ClusterTopologyMgr) GetVolume(vid proto.Vid) (*client.VolumeInfoSimple, error) {
	return m.volumeCache.GetVolume(vid)
}

func (m *ClusterTopologyMgr) LoadVolumes() error {
	return m.volumeCache.LoadVolumes()
}

// GetIDCs returns IDCs
func (m *ClusterTopologyMgr) GetIDCs() map[string]*IDC {
	return m.clusterTopology.idcMap
}

// GetIDCDisks returns disks with IDC
func (m *ClusterTopologyMgr) GetIDCDisks(idc string) (disks []*client.DiskInfoSimple) {
	return m.clusterTopology.diskMap[idc]
}

// MaxFreeChunksDisk returns disk which has max free chunks
func (m *ClusterTopologyMgr) MaxFreeChunksDisk(idc string) *client.DiskInfoSimple {
	disks := m.GetIDCDisks(idc)
	size := len(disks)
	if size > 0 {
		return disks[size-1]
	}
	return nil
}

// ReportFreeChunkCnt report free chunk cnt
func (m *ClusterTopologyMgr) ReportFreeChunkCnt(disk *client.DiskInfoSimple) {
	m.taskStatsMgr.ReportFreeChunk(disk)
}

func (m *ClusterTopologyMgr) buildClusterTopology(disks []*client.DiskInfoSimple, clusterID proto.ClusterID) {
	cluster := &ClusterTopology{
		clusterID: clusterID,
		idcMap:    make(map[string]*IDC),
		diskMap:   make(map[string][]*client.DiskInfoSimple),
	}

	for i := range disks {
		if cluster.clusterID != disks[i].ClusterID {
			log.Errorf("the disk does not belong to this cluster: cluster_id[%d], disk[%+v]", cluster.clusterID, disks[i])
			continue
		}
		cluster.addDisk(disks[i])
		m.ReportFreeChunkCnt(disks[i])
	}

	for idc := range cluster.diskMap {
		sortDiskByFreeChunkCnt(cluster.diskMap[idc])
	}
	m.clusterTopology = cluster
}

func (cluster *ClusterTopology) addDisk(disk *client.DiskInfoSimple) {
	cluster.addDiskToCluster(disk)
	cluster.addDiskToDiskMap(disk)
	cluster.addDiskToIdc(disk)
	cluster.addDiskToRack(disk)
	cluster.addDiskToHost(disk)
}

func (cluster *ClusterTopology) addDiskToCluster(disk *client.DiskInfoSimple) {
	// statistics cluster chunk info
	cluster.FreeChunkCnt += disk.FreeChunkCnt
	cluster.MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToDiskMap(disk *client.DiskInfoSimple) {
	if _, ok := cluster.diskMap[disk.Idc]; !ok {
		var disks []*client.DiskInfoSimple
		cluster.diskMap[disk.Idc] = disks
	}
	cluster.diskMap[disk.Idc] = append(cluster.diskMap[disk.Idc], disk)
}

func (cluster *ClusterTopology) addDiskToIdc(disk *client.DiskInfoSimple) {
	idcName := disk.Idc
	if _, ok := cluster.idcMap[idcName]; !ok {
		cluster.idcMap[idcName] = &IDC{
			name:    idcName,
			rackMap: make(map[string]*Rack),
		}
	}
	// statistics idc chunk info
	cluster.idcMap[idcName].FreeChunkCnt += disk.FreeChunkCnt
	cluster.idcMap[idcName].MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToRack(disk *client.DiskInfoSimple) {
	idc := cluster.idcMap[disk.Idc]
	rackName := disk.Rack
	if _, ok := idc.rackMap[rackName]; !ok {
		idc.rackMap[rackName] = &Rack{
			name:    rackName,
			diskMap: make(map[string]*Host),
		}
	}
	// statistics rack chunk info
	idc.rackMap[rackName].FreeChunkCnt += disk.FreeChunkCnt
	idc.rackMap[rackName].MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToHost(disk *client.DiskInfoSimple) {
	rack := cluster.idcMap[disk.Idc].rackMap[disk.Rack]
	if _, ok := rack.diskMap[disk.Host]; !ok {
		var disks []*client.DiskInfoSimple
		rack.diskMap[disk.Host] = &Host{
			host:  disk.Host,
			disks: disks,
		}
	}
	rack.diskMap[disk.Host].disks = append(rack.diskMap[disk.Host].disks, disk)

	// statistics host chunk info
	rack.diskMap[disk.Host].FreeChunkCnt += disk.FreeChunkCnt
	rack.diskMap[disk.Host].MaxChunkCnt += disk.MaxChunkCnt
}

func sortDiskByFreeChunkCnt(disks []*client.DiskInfoSimple) {
	sort.Slice(disks, func(i, j int) bool {
		return disks[i].FreeChunkCnt < disks[j].FreeChunkCnt
	})
}

// VolumeCache volume cache
type VolumeCache struct {
	clusterMgrCli client.ClusterMgrAPI
	group         singleflight.Group
	cache         *volumeCacher
}

// NewVolumeCache returns volume cache manager.
func NewVolumeCache(client client.ClusterMgrAPI, updateInterval time.Duration) *VolumeCache {
	return &VolumeCache{
		clusterMgrCli: client,
		cache:         newVolumeCacher(updateInterval),
	}
}

// Load list all volumes info memory cache.
func (c *VolumeCache) LoadVolumes() error {
	marker := defaultMarker
	for {
		log.Infof("to load volume marker[%d], count[%d]", marker, defaultCount)

		var (
			volInfos   []*client.VolumeInfoSimple
			nextMarker proto.Vid
			err        error
		)
		if err = retry.Timed(3, 200).On(func() error {
			volInfos, nextMarker, err = c.clusterMgrCli.ListVolume(context.Background(), marker, defaultCount)
			return err
		}); err != nil {
			log.Errorf("list volume: marker[%d], count[%+v], code[%d], error[%v]",
				marker, defaultCount, rpc.DetectStatusCode(err), err)
			return err
		}

		for _, v := range volInfos {
			c.cache.Set(v.Vid, *v)
		}
		if len(volInfos) == 0 || nextMarker == defaultMarker {
			break
		}

		marker = nextMarker
	}
	return nil
}

// GetVolume returns this volume info.
func (c *VolumeCache) GetVolume(vid proto.Vid) (*client.VolumeInfoSimple, error) {
	if vol, ok := c.cache.Get(vid); ok {
		return vol, nil
	}

	vol, err := c.UpdateVolume(vid)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

// UpdateVolume this volume info cache.
func (c *VolumeCache) UpdateVolume(vid proto.Vid) (*client.VolumeInfoSimple, error) {
	if !c.cache.Settable(vid) {
		return nil, ErrFrequentlyUpdate
	}

	val, err, _ := c.group.Do(fmt.Sprintf("volume-update-%d", vid), func() (interface{}, error) {
		vol, err := c.clusterMgrCli.GetVolumeInfo(context.Background(), vid)
		if err != nil {
			return nil, err
		}

		c.cache.Set(vid, *vol)
		return vol, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*client.VolumeInfoSimple), nil
}

// DoubleCheckedRun the scheduler updates volume mapping relation asynchronously,
// then some task(delete or repair) had started with old volume mapping.
//
// if delete on old relation, there will has garbage shard in new chunk. ==> garbage shard
// if repair on old relation, there still is missing shard in new chunk. ==> missing shard
func DoubleCheckedRun(ctx context.Context, c IClusterTopology, vid proto.Vid, task func(*client.VolumeInfoSimple) error) error {
	span := trace.SpanFromContextSafe(ctx)
	vol, err := c.GetVolume(vid)
	if err != nil {
		return err
	}

	for range [3]struct{}{} {
		if err := task(vol); err != nil {
			return err
		}

		newVol, err := c.GetVolume(vol.Vid)
		if err != nil {
			return err
		}
		if newVol.EqualWith(vol) {
			return nil
		}

		span.Warnf("volume changed from [%+v] to [%+v]", vol, newVol)
		vol = newVol
	}
	return errVolumeMissmatch
}
