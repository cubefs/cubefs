// Copyright 2026 The CubeFS Authors.
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
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetDataPartitionPeerSelectTag tests getting select tag of data partition peer
func TestGetDataPartitionPeerSelectTag(t *testing.T) {
	dp := &DataPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:8080", Tag: "tag1"},
			{ID: 2, Addr: "192.168.0.2:8080", Tag: "tag2"},
			{ID: 3, Addr: "192.168.0.3:8080", Tag: ""},
		},
	}

	tests := []struct {
		name     string
		addr     string
		expected string
	}{
		{"existing address with tag1", "192.168.0.1:8080", "tag1"},
		{"existing address with tag2", "192.168.0.2:8080", "tag2"},
		{"existing address with empty tag", "192.168.0.3:8080", ""},
		{"non-existent address", "192.168.0.4:8080", DefaultTag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetDataPartitionPeerTag(dp, tt.addr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSetDataPartitionPeerSelectTag tests setting select tag of data partition peer
func TestSetDataPartitionPeerSelectTag(t *testing.T) {
	dp := &DataPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:8080", Tag: "tag1"},
			{ID: 2, Addr: "192.168.0.2:8080", Tag: "tag2"},
		},
	}

	// Set tag for existing address
	SetDataPartitionPeerTag(dp, "192.168.0.1:8080", "newTag1")
	assert.Equal(t, "newTag1", dp.Peers[0].Tag)

	// Set tag for non-existent address (should not change anything)
	SetDataPartitionPeerTag(dp, "192.168.0.3:8080", "tag3")
	assert.Equal(t, 2, len(dp.Peers))
	assert.Equal(t, "newTag1", dp.Peers[0].Tag)
	assert.Equal(t, "tag2", dp.Peers[1].Tag)
}

// TestGetMetaPartitionPeerSelectTag tests getting select tag of meta partition peer
func TestGetMetaPartitionPeerSelectTag(t *testing.T) {
	mp := &MetaPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:9090", Tag: "meta-tag1"},
			{ID: 2, Addr: "192.168.0.2:9090", Tag: "meta-tag2"},
			{ID: 3, Addr: "192.168.0.3:9090", Tag: ""},
		},
	}

	tests := []struct {
		name     string
		addr     string
		expected string
	}{
		{"existing address with meta-tag1", "192.168.0.1:9090", "meta-tag1"},
		{"existing address with meta-tag2", "192.168.0.2:9090", "meta-tag2"},
		{"existing address with empty tag", "192.168.0.3:9090", ""},
		{"non-existent address", "192.168.0.4:9090", DefaultTag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetMetaPartitionPeerTag(mp, tt.addr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSetMetaPartitionPeerSelectTag tests setting select tag of meta partition peer
func TestSetMetaPartitionPeerSelectTag(t *testing.T) {
	mp := &MetaPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:9090", Tag: "meta-tag1"},
			{ID: 2, Addr: "192.168.0.2:9090", Tag: "meta-tag2"},
		},
	}

	// Set tag for existing address
	SetMetaPartitionPeerTag(mp, "192.168.0.1:9090", "newMetaTag1")
	assert.Equal(t, "newMetaTag1", mp.Peers[0].Tag)

	// Set tag for non-existent address (should not change anything)
	SetMetaPartitionPeerTag(mp, "192.168.0.3:9090", "meta-tag3")
	assert.Equal(t, 2, len(mp.Peers))
	assert.Equal(t, "newMetaTag1", mp.Peers[0].Tag)
	assert.Equal(t, "meta-tag2", mp.Peers[1].Tag)
}

// TestFormatMetaReplicaSelectTag tests formatting meta replica select tag
func TestFormatMetaReplicaSelectTag(t *testing.T) {
	metanode := &MetaNode{
		Tag: "node-tag",
	}

	tests := []struct {
		name      string
		selectTag string
		expected  string
	}{
		{"tag same as node", "node-tag", "node-tag"},
		{"default tag", DefaultTag, DefaultTag},
		{"already contains arrow", "old-tag->new-tag", "old-tag->new-tag"},
		{"different tag", "different-tag", "node-tag->different-tag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatMetaReplicaTag(tt.selectTag, metanode)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFormatDataReplicaSelectTag tests formatting data replica select tag
func TestFormatDataReplicaSelectTag(t *testing.T) {
	datanode := &DataNode{
		Tag: "data-node-tag",
	}

	tests := []struct {
		name      string
		selectTag string
		expected  string
	}{
		{"tag same as node", "data-node-tag", "data-node-tag"},
		{"default tag", DefaultTag, DefaultTag},
		{"already contains arrow", "old-tag->new-tag", "old-tag->new-tag"},
		{"different tag", "different-tag", "data-node-tag->different-tag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDataReplicaTag(tt.selectTag, datanode)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestClusterIsMetaPartitionSelectTagSet tests checking if meta partition select tag is set
func TestClusterIsMetaPartitionSelectTagSet(t *testing.T) {
	tests := []struct {
		name            string
		clusterTag      string
		volTag          string
		expectedWithVol bool
		expectedNoVol   bool
	}{
		{
			name:            "cluster has default tag",
			clusterTag:      "cluster-mp-tag",
			volTag:          "",
			expectedWithVol: true,
			expectedNoVol:   true,
		},
		{
			name:            "volume has tag",
			clusterTag:      "",
			volTag:          "vol-mp-tag",
			expectedWithVol: true,
			expectedNoVol:   false,
		},
		{
			name:            "both have no tag",
			clusterTag:      "",
			volTag:          "",
			expectedWithVol: false,
			expectedNoVol:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vol := &Vol{
				Name:  "test-vol",
				MpTag: tt.volTag,
			}

			c := &Cluster{
				cfg: &clusterConfig{
					DefaultMpTag: tt.clusterTag,
				},
				ClusterVolSubItem: ClusterVolSubItem{
					vols: map[string]*Vol{
						"test-vol": vol,
					},
				},
			}

			result := c.IsMetaPartitionTagSet("test-vol")
			assert.Equal(t, tt.expectedWithVol, result)

			// Test non-existent volume
			result = c.IsMetaPartitionTagSet("non-existent-vol")
			assert.Equal(t, tt.expectedNoVol, result)
		})
	}
}

// TestClusterIsDataPartitionSelectTagSet tests checking if data partition select tag is set
func TestClusterIsDataPartitionSelectTagSet(t *testing.T) {
	tests := []struct {
		name            string
		clusterTag      string
		volTag          string
		expectedWithVol bool
		expectedNoVol   bool
	}{
		{
			name:            "cluster has default tag",
			clusterTag:      "cluster-dp-tag",
			volTag:          "",
			expectedWithVol: true,
			expectedNoVol:   true,
		},
		{
			name:            "volume has tag",
			clusterTag:      "",
			volTag:          "vol-dp-tag",
			expectedWithVol: true,
			expectedNoVol:   false,
		},
		{
			name:            "both have no tag",
			clusterTag:      "",
			volTag:          "",
			expectedWithVol: false,
			expectedNoVol:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vol := &Vol{
				Name:  "test-vol",
				DpTag: tt.volTag,
			}

			c := &Cluster{
				cfg: &clusterConfig{
					DefaultDpTag: tt.clusterTag,
				},
				ClusterVolSubItem: ClusterVolSubItem{
					vols: map[string]*Vol{
						"test-vol": vol,
					},
				},
			}

			result := c.IsDataPartitionTagSet("test-vol")
			assert.Equal(t, tt.expectedWithVol, result)

			// Test non-existent volume
			result = c.IsDataPartitionTagSet("non-existent-vol")
			assert.Equal(t, tt.expectedNoVol, result)
		})
	}
}

// TestVolCountDpSelectTagUnmatch tests counting data partition select tag unmatches
func TestVolCountDpSelectTagUnmatch(t *testing.T) {
	vol := &Vol{
		Name: "test-vol",
		dataPartitions: &DataPartitionMap{
			partitionMap: make(map[uint64]*DataPartition),
			partitions: []*DataPartition{
				{
					PartitionID: 1,
					IsDiscard:   false,
					Replicas: []*DataReplica{
						{
							DataReplica: proto.DataReplica{
								Addr: "192.168.0.1:8080",
							},
							dataNode: &DataNode{
								Tag: "node-tag1",
							},
						},
					},
					Peers: []proto.Peer{
						{Addr: "192.168.0.1:8080", Tag: "node-tag1"}, // matches
					},
				},
				{
					PartitionID: 2,
					IsDiscard:   false,
					Replicas: []*DataReplica{
						{
							DataReplica: proto.DataReplica{
								Addr: "192.168.0.2:8080",
							},
							dataNode: &DataNode{
								Tag: "node-tag2",
							},
						},
					},
					Peers: []proto.Peer{
						{Addr: "192.168.0.2:8080", Tag: "different-tag"}, // mismatch
					},
				},
				{
					PartitionID: 3,
					IsDiscard:   true, // should be ignored
					Replicas: []*DataReplica{
						{
							DataReplica: proto.DataReplica{
								Addr: "192.168.0.3:8080",
							},
							dataNode: &DataNode{
								Tag: "node-tag3",
							},
						},
					},
					Peers: []proto.Peer{
						{Addr: "192.168.0.3:8080", Tag: "different-tag"},
					},
				},
			},
		},
	}
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}

	count := vol.countDpTagUnmatch(summary)
	assert.Equal(t, 1, count) // only PartitionID 2 mismatches
}

// TestVolCountMpSelectTagUnmatch tests counting meta partition select tag unmatches
func TestVolCountMpSelectTagUnmatch(t *testing.T) {
	vol := &Vol{
		Name: "test-vol",
		MetaPartitions: map[uint64]*MetaPartition{
			1: {
				PartitionID: 1,
				Replicas: []*MetaReplica{
					{
						Addr: "192.168.0.1:9090",
						metaNode: &MetaNode{
							Tag: "meta-tag1",
						},
					},
				},
				Peers: []proto.Peer{
					{Addr: "192.168.0.1:9090", Tag: "meta-tag1"}, // matches
				},
			},
			2: {
				PartitionID: 2,
				Replicas: []*MetaReplica{
					{
						Addr: "192.168.0.2:9090",
						metaNode: &MetaNode{
							Tag: "meta-tag2",
						},
					},
				},
				Peers: []proto.Peer{
					{Addr: "192.168.0.2:9090", Tag: "different-tag"}, // mismatch
				},
			},
		},
	}
	vol.mpsLock = newMpsLockManager(vol)

	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}
	count := vol.countMpTagUnmatch(summary)
	assert.Equal(t, 1, count) // only PartitionID 2 mismatches
}

// TestCheckDpSelectTagWithAutoFixDisabled tests no check is performed when AutoFixTag is disabled
func TestCheckDpSelectTagWithAutoFixDisabled(t *testing.T) {
	cfg := &clusterConfig{}
	cfg.AutoFixTag.Store(false)
	c := &Cluster{
		cfg: cfg,
	}

	// Record initial status
	initialStatus := DpTagThreadStatus

	// Execute check
	c.checkDpTag()

	// Status should return to sleeping immediately
	assert.Equal(t, StatusSleeping, DpTagThreadStatus)

	// Restore initial status
	DpTagThreadStatus = initialStatus
}

// TestCheckMpSelectTagWithAutoFixDisabled tests no check is performed when AutoFixTag is disabled
func TestCheckMpSelectTagWithAutoFixDisabled(t *testing.T) {
	cfg := &clusterConfig{}
	cfg.AutoFixTag.Store(false)
	c := &Cluster{
		cfg: cfg,
	}

	// Record initial status
	initialStatus := MpTagThreadStatus

	// Execute check
	c.checkMpTag()

	// Status should return to sleeping immediately
	assert.Equal(t, StatusSleeping, MpTagThreadStatus)

	// Restore initial status
	MpTagThreadStatus = initialStatus
}

// TestGetSelectTagSummary tests getting select tag summary
func TestGetSelectTagSummary(t *testing.T) {
	vol1 := &Vol{
		Name:   "vol1",
		MpTag:  "mp-tag1",
		DpTag:  "dp-tag1",
		Status: proto.VolStatusNormal,
		dataPartitions: &DataPartitionMap{
			partitionMap: make(map[uint64]*DataPartition),
			partitions:   []*DataPartition{},
		},
		MetaPartitions: map[uint64]*MetaPartition{},
	}
	vol1.mpsLock = newMpsLockManager(vol1)

	vol2 := &Vol{
		Name:   "vol2",
		MpTag:  "",
		DpTag:  "",
		Status: proto.VolStatusNormal,
		dataPartitions: &DataPartitionMap{
			partitionMap: make(map[uint64]*DataPartition),
			partitions:   []*DataPartition{},
		},
		MetaPartitions: map[uint64]*MetaPartition{},
	}
	vol2.mpsLock = newMpsLockManager(vol2)

	cfg := &clusterConfig{
		DefaultDpTag: "default-dp",
		DefaultMpTag: "default-mp",
	}
	cfg.AutoFixTag.Store(true)
	c := &Cluster{
		cfg: cfg,
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{
				"vol1": vol1,
				"vol2": vol2,
			},
		},
		ClusterDecommission: ClusterDecommission{
			BadMetaPartitionIds: new(sync.Map),
		},
	}

	summary, err := c.getTagSummary(false)
	require.NoError(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.AutoFixTag)
	assert.Equal(t, "default-dp", summary.ClusterDpTag)
	assert.Equal(t, "default-mp", summary.ClusterMpTag)
	assert.Equal(t, 2, summary.VolumeNum)
	assert.Equal(t, 2, summary.VolWithTagNum) // default tags make vol2 effective
	assert.Equal(t, 0, summary.VolWithoutTagNum)
}

// TestContainsHelper tests the contains helper function
func TestContainsHelper(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		item     string
		expected bool
	}{
		{"existing element", []string{"a", "b", "c"}, "b", true},
		{"non-existent element", []string{"a", "b", "c"}, "d", false},
		{"empty slice", []string{}, "a", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := contains(tt.slice, tt.item)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestClusterGetMetaNodeSelectTag tests getting meta node select tag
func TestClusterGetMetaNodeSelectTag(t *testing.T) {
	metaNode1 := &MetaNode{
		Addr: "192.168.0.1:9090",
		Tag:  "meta-node-tag1",
	}

	metaNode2 := &MetaNode{
		Addr: "192.168.0.2:9090",
		Tag:  "",
	}

	c := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			metaNodes: sync.Map{},
		},
	}

	c.metaNodes.Store("192.168.0.1:9090", metaNode1)
	c.metaNodes.Store("192.168.0.2:9090", metaNode2)

	// Test existing node
	tag := c.GetMetaNodeTag("192.168.0.1:9090")
	assert.Equal(t, "meta-node-tag1", tag)

	// Test node with empty tag
	tag = c.GetMetaNodeTag("192.168.0.2:9090")
	assert.Equal(t, "", tag)

	// Test non-existent node
	tag = c.GetMetaNodeTag("192.168.0.3:9090")
	assert.Equal(t, DefaultTag, tag)
}

// TestClusterGetDataNodeSelectTag tests getting data node select tag
func TestClusterGetDataNodeSelectTag(t *testing.T) {
	dataNode1 := &DataNode{
		Addr: "192.168.0.1:8080",
		Tag:  "data-node-tag1",
	}

	dataNode2 := &DataNode{
		Addr: "192.168.0.2:8080",
		Tag:  "",
	}

	c := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
		},
	}

	c.dataNodes.Store("192.168.0.1:8080", dataNode1)
	c.dataNodes.Store("192.168.0.2:8080", dataNode2)

	// Test existing node
	tag := c.GetDataNodeTag("192.168.0.1:8080")
	assert.Equal(t, "data-node-tag1", tag)

	// Test node with empty tag
	tag = c.GetDataNodeTag("192.168.0.2:8080")
	assert.Equal(t, "", tag)

	// Test non-existent node
	tag = c.GetDataNodeTag("192.168.0.3:8080")
	assert.Equal(t, DefaultTag, tag)
}

// TestEdgeCases tests edge cases
func TestEdgeCases(t *testing.T) {
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}

	t.Run("empty data partition list", func(t *testing.T) {
		vol := &Vol{
			Name: "empty-vol",
			dataPartitions: &DataPartitionMap{
				partitionMap: make(map[uint64]*DataPartition),
				partitions:   []*DataPartition{},
			},
		}
		c := &Cluster{cfg: &clusterConfig{}}
		count := vol.countTagDecommissionTask(c)
		assert.Equal(t, 0, count)
	})

	t.Run("empty meta partition list", func(t *testing.T) {
		vol := &Vol{
			Name:           "empty-vol",
			MetaPartitions: map[uint64]*MetaPartition{},
		}
		vol.mpsLock = newMpsLockManager(vol)
		count := vol.countMpTagUnmatch(summary)
		assert.Equal(t, 0, count)
	})

	t.Run("handling nil replica", func(t *testing.T) {
		dp := &DataPartition{
			PartitionID: 1,
			Replicas:    []*DataReplica{nil},
			Peers:       []proto.Peer{},
		}
		vol := &Vol{
			Name:         "test-vol",
			dpReplicaNum: 3,
			dataPartitions: &DataPartitionMap{
				partitionMap: map[uint64]*DataPartition{1: dp},
				partitions:   []*DataPartition{dp},
			},
		}
		// Should not panic
		count := vol.countDpTagUnmatch(summary)
		assert.Equal(t, 0, count)
	})
}
