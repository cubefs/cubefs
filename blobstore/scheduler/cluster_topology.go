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
	"sort"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// IClusterTopology define the interface og cluster topology
type IClusterTopology interface {
	GetIDCs() map[string]*IDC
	GetIDCDisks(idc string) (disks []*client.DiskInfoSimple)
	closer.Closer
}

type clusterTopoConf struct {
	ClusterID               proto.ClusterID
	UpdateInterval          time.Duration
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

// ClusterTopologyMgr cluster topology manager
type ClusterTopologyMgr struct {
	closer.Closer
	updateInterval time.Duration
	clusterID      proto.ClusterID

	clusterMgrCli client.ClusterMgrAPI

	clusterTopo  *ClusterTopology
	taskStatsMgr *base.ClusterTopologyStatsMgr
}

// NewClusterTopologyMgr returns cluster topology manager
func NewClusterTopologyMgr(topologyClient client.ClusterMgrAPI, conf *clusterTopoConf) IClusterTopology {
	mgr := &ClusterTopologyMgr{
		Closer:         closer.New(),
		updateInterval: conf.UpdateInterval,
		clusterID:      conf.ClusterID,

		clusterMgrCli: topologyClient,

		clusterTopo: &ClusterTopology{
			idcMap:  make(map[string]*IDC),
			diskMap: make(map[string][]*client.DiskInfoSimple),
		},
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(conf.ClusterID, conf.FreeChunkCounterBuckets),
	}
	go mgr.loopUpdate()
	return mgr
}

func (m *ClusterTopologyMgr) loopUpdate() {
	t := time.NewTicker(m.updateInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.updateClusterTopology()
		case <-m.Closer.Done():
			return
		}
	}
}

func (m *ClusterTopologyMgr) updateClusterTopology() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "updateClusterTopology")

	disks, err := m.clusterMgrCli.ListClusterDisks(ctx)
	if err != nil {
		span.Errorf("update cluster topology failed: err[%+v]", err)
	}

	m.buildClusterTopo(disks, m.clusterID)
}

// GetIDCs returns IDCs
func (m *ClusterTopologyMgr) GetIDCs() map[string]*IDC {
	return m.clusterTopo.idcMap
}

// GetIDCDisks returns disks with IDC
func (m *ClusterTopologyMgr) GetIDCDisks(idc string) (disks []*client.DiskInfoSimple) {
	return m.clusterTopo.diskMap[idc]
}

// ReportFreeChunkCnt report free chunk cnt
func (m *ClusterTopologyMgr) ReportFreeChunkCnt(disk *client.DiskInfoSimple) {
	m.taskStatsMgr.ReportFreeChunk(disk)
}

func (m *ClusterTopologyMgr) buildClusterTopo(disks []*client.DiskInfoSimple, clusterID proto.ClusterID) {
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
	m.clusterTopo = cluster
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
