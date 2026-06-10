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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Select-tag tests are mostly pure unit tests around peer tag normalization,
// rule parsing, mismatch counting, and summary helpers. The fixtures below
// intentionally initialize only the partition, volume, node, and topology fields
// that each helper reads.

// TestGetDataPartitionPeerSelectTag tests getting select tag of data partition peer
func TestGetDataPartitionPeerSelectTag(t *testing.T) {
	// Build one data partition peer list that covers tagged, explicitly default,
	// and missing-address lookup cases.
	dp := &DataPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:8080", Tag: "tag1"},
			{ID: 2, Addr: "192.168.0.2:8080", Tag: "tag2"},
			{ID: 3, Addr: "192.168.0.3:8080", Tag: ""},
		},
	}

	// Each case verifies lookup behavior only; the helper must not mutate Peers.
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
			// Unknown addresses should fall back to DefaultTag, while known peers
			// return the tag already stored in the peer slice.
			result := GetDataPartitionPeerTag(dp, tt.addr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSetDataPartitionPeerSelectTag tests setting select tag of data partition peer
func TestSetDataPartitionPeerSelectTag(t *testing.T) {
	// Keep the fixture small so it is clear which peer is expected to change.
	dp := &DataPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:8080", Tag: "tag1"},
			{ID: 2, Addr: "192.168.0.2:8080", Tag: "tag2"},
		},
	}

	// Set tag for existing address
	SetDataPartitionPeerTag(dp, "192.168.0.1:8080", "newTag1")
	assert.Equal(t, "newTag1", dp.Peers[0].Tag)

	// Set tag for non-existent address. The helper intentionally ignores it
	// instead of appending a new peer or changing unrelated peers.
	SetDataPartitionPeerTag(dp, "192.168.0.3:8080", "tag3")
	assert.Equal(t, 2, len(dp.Peers))
	assert.Equal(t, "newTag1", dp.Peers[0].Tag)
	assert.Equal(t, "tag2", dp.Peers[1].Tag)
}

// TestGetMetaPartitionPeerSelectTag tests getting select tag of meta partition peer
func TestGetMetaPartitionPeerSelectTag(t *testing.T) {
	// Meta partition peer lookup mirrors data partition peer lookup, so this
	// fixture keeps the same shape with meta-specific tags and ports.
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
			// The lookup path is pure and should return DefaultTag for addresses
			// that are not present in the peer list.
			result := GetMetaPartitionPeerTag(mp, tt.addr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSetMetaPartitionPeerSelectTag tests setting select tag of meta partition peer
func TestSetMetaPartitionPeerSelectTag(t *testing.T) {
	// Use two peers to prove the setter updates only the peer with a matching
	// address and leaves its sibling untouched.
	mp := &MetaPartition{
		Peers: []proto.Peer{
			{ID: 1, Addr: "192.168.0.1:9090", Tag: "meta-tag1"},
			{ID: 2, Addr: "192.168.0.2:9090", Tag: "meta-tag2"},
		},
	}

	// Set tag for existing address
	SetMetaPartitionPeerTag(mp, "192.168.0.1:9090", "newMetaTag1")
	assert.Equal(t, "newMetaTag1", mp.Peers[0].Tag)

	// Unknown addresses are ignored, which prevents historical peer lists from
	// growing during repair scans.
	SetMetaPartitionPeerTag(mp, "192.168.0.3:9090", "meta-tag3")
	assert.Equal(t, 2, len(mp.Peers))
	assert.Equal(t, "newMetaTag1", mp.Peers[0].Tag)
	assert.Equal(t, "meta-tag2", mp.Peers[1].Tag)
}

// TestFormatMetaReplicaSelectTag tests formatting meta replica select tag
func TestFormatMetaReplicaSelectTag(t *testing.T) {
	// The node tag is the source side used when a plain target tag must be
	// expanded into the "source->destination" representation.
	metanode := &MetaNode{
		Tag: "node-tag",
	}

	// Cover no-op formatting, default-tag replacement, already formatted values,
	// and newly generated source-to-destination mappings.
	tests := []struct {
		name      string
		selectTag string
		expected  string
	}{
		{"tag same as node", "node-tag", "node-tag"},
		{"default tag", DefaultTag, "node-tag"},
		{"already contains arrow", "old-tag->new-tag", "old-tag->new-tag"},
		{"different tag", "different-tag", "node-tag->different-tag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// formatMetaReplicaTag should normalize only when the tag is plain and
			// differs from the current meta node tag.
			result := formatMetaReplicaTag(tt.selectTag, metanode)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFormatDataReplicaSelectTag tests formatting data replica select tag
func TestFormatDataReplicaSelectTag(t *testing.T) {
	// Data replica formatting follows the same rules as meta replica formatting,
	// but uses the DataNode tag as the source side of a mapping.
	datanode := &DataNode{
		Tag: "data-node-tag",
	}

	// These cases pin the four meaningful formatting branches.
	tests := []struct {
		name      string
		selectTag string
		expected  string
	}{
		{"tag same as node", "data-node-tag", "data-node-tag"},
		{"default tag", DefaultTag, "data-node-tag"},
		{"already contains arrow", "old-tag->new-tag", "old-tag->new-tag"},
		{"different tag", "different-tag", "data-node-tag->different-tag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Already formatted strings must be preserved so existing API values
			// are not rewritten repeatedly.
			result := formatDataReplicaTag(tt.selectTag, datanode)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestClusterIsMetaPartitionSelectTagSet tests checking if meta partition select tag is set
func TestClusterIsMetaPartitionSelectTagSet(t *testing.T) {
	// The table distinguishes cluster defaults from volume-level configuration
	// and verifies behavior for missing volumes.
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
			// A minimal volume map is enough because IsMetaPartitionTagSet only
			// needs cluster defaults and per-volume MpTag.
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

			// Missing volumes can only be considered tagged when a cluster default
			// tag is configured.
			result = c.IsMetaPartitionTagSet("non-existent-vol")
			assert.Equal(t, tt.expectedNoVol, result)
		})
	}
}

// TestClusterIsDataPartitionSelectTagSet tests checking if data partition select tag is set
func TestClusterIsDataPartitionSelectTagSet(t *testing.T) {
	// This is the data-partition counterpart to the meta-partition tag-set
	// detection test above.
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
			// Only DpTag and cluster DefaultDpTag participate in this helper, so
			// the fixture avoids unrelated volume fields.
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

			// Missing volumes follow the same cluster-default fallback as meta
			// partitions.
			result = c.IsDataPartitionTagSet("non-existent-vol")
			assert.Equal(t, tt.expectedNoVol, result)
		})
	}
}

// TestVolCountDpSelectTagUnmatch tests counting data partition select tag unmatches
func TestVolCountDpSelectTagUnmatch(t *testing.T) {
	// Build three partitions: one healthy, one mismatched, and one discarded.
	// The discarded partition proves cleanup paths are excluded from counting.
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
	// The summary carries sample slices that countDpTagUnmatch appends into when
	// it finds a partition-level mismatch.
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}

	// Only the second partition should count because it is live and its peer tag
	// differs from the cached DataNode tag.
	count := vol.countDpTagUnmatch(summary)
	assert.Equal(t, 1, count) // only PartitionID 2 mismatches
}

// TestVolCountMpSelectTagUnmatch tests counting meta partition select tag unmatches
func TestVolCountMpSelectTagUnmatch(t *testing.T) {
	// Two meta partitions are enough to prove both the matching and mismatching
	// branches of countMpTagUnmatch.
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
	// cloneMetaPartitionMap requires the volume lock manager to be initialized.
	vol.mpsLock = newMpsLockManager(vol)

	// Samples are initialized because the helper records the first mismatch per
	// partition for later API reporting.
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}
	// Only PartitionID 2 should count because its peer tag differs from the
	// cached MetaNode tag.
	count := vol.countMpTagUnmatch(summary)
	assert.Equal(t, 1, count) // only PartitionID 2 mismatches
}

// TestCheckDpSelectTagWithAutoFixDisabled tests no check is performed when AutoFixTag is disabled
func TestCheckDpSelectTagWithAutoFixDisabled(t *testing.T) {
	// AutoFixTag is the first guard in checkDpTag, so no volume or partition
	// fixture is required for this branch.
	cfg := &clusterConfig{}
	cfg.AutoFixTag.Store(false)
	c := &Cluster{
		cfg: cfg,
	}

	// Record initial state so these package-level variables are restored after the
	// disabled-path assertion.
	initialStatus := DpTagThreadStatus
	initialReason := LastDpQuitReason

	// Execute check. It should return immediately and keep the checker sleeping.
	c.checkDpTag()

	// Status should return to sleeping immediately
	assert.Equal(t, StatusSleeping, DpTagThreadStatus)

	// Restore initial state.
	DpTagThreadStatus = initialStatus
	LastDpQuitReason = initialReason
}

// TestCheckMpSelectTagWithAutoFixDisabled tests no check is performed when AutoFixTag is disabled
func TestCheckMpSelectTagWithAutoFixDisabled(t *testing.T) {
	// This mirrors the DP disabled test for the meta checker's early exit.
	cfg := &clusterConfig{}
	cfg.AutoFixTag.Store(false)
	c := &Cluster{
		cfg: cfg,
	}

	// Record initial state so the global checker state is not leaked to later
	// tests.
	initialStatus := MpTagThreadStatus
	initialReason := LastMpQuitReason

	// Execute check. No cluster plan state or volume map is needed because the
	// disabled guard returns before those dependencies are touched.
	c.checkMpTag()

	// Status should return to sleeping immediately
	assert.Equal(t, StatusSleeping, MpTagThreadStatus)

	// Restore initial state.
	MpTagThreadStatus = initialStatus
	LastMpQuitReason = initialReason
}

func TestCheckDpTagLimitBranches(t *testing.T) {
	oldLimit := atomic.LoadUint64(&clusterDpTagDecommissionLimit)
	oldReason := LastDpQuitReason
	oldStatus := DpTagThreadStatus
	t.Cleanup(func() {
		atomic.StoreUint64(&clusterDpTagDecommissionLimit, oldLimit)
		tagStateMu.Lock()
		LastDpQuitReason = oldReason
		DpTagThreadStatus = oldStatus
		tagStateMu.Unlock()
	})

	cfg := newClusterConfig()
	cfg.AutoFixTag.Store(true)
	c := &Cluster{cfg: cfg}
	c.vols = make(map[string]*Vol)

	atomic.StoreUint64(&clusterDpTagDecommissionLimit, 2)
	c.checkDpTag()
	_, _, lastDpReason, _, _, _, _ := snapshotTagState()
	require.Equal(t, ReasonCloseOK, lastDpReason)

	const (
		dpID    = uint64(101)
		srcAddr = "10.0.0.1:17310"
	)
	c.t = newTopology()
	zone := newZone(DefaultZoneName, 0)
	ns := newNodeSet(nil, 1, 18, DefaultZoneName, "")
	zone.nodeSetMap[ns.ID] = ns
	require.NoError(t, c.t.putZone(zone))
	dataNode := &DataNode{Addr: srcAddr, ZoneName: DefaultZoneName, NodeSetID: ns.ID}
	c.dataNodes.Store(srcAddr, dataNode)
	require.True(t, ns.AcquireDecommissionToken(dpID, lowPriorityDecommissionWeight, c, false))

	dp := &DataPartition{
		PartitionID:         dpID,
		DecommissionType:    proto.TagDecommission,
		DecommissionSrcAddr: srcAddr,
		DecommissionWeight:  lowPriorityDecommissionWeight,
		DecommissionStatus:  markDecommission,
	}
	dpMap := newDataPartitionMap("vol-dp-limit")
	dpMap.put(dp)
	ns.decommissionDataPartitionList.Put(ns.ID, dp, c)
	c.vols["vol-dp-limit"] = &Vol{
		Name:           "vol-dp-limit",
		Status:         proto.VolStatusNormal,
		DpTag:          "tag-a",
		dpReplicaNum:   TagReplicaRuleNum,
		dataPartitions: dpMap,
	}

	atomic.StoreUint64(&clusterDpTagDecommissionLimit, 1)
	c.checkDpTag()
	_, _, lastDpReason, _, _, _, _ = snapshotTagState()
	require.Equal(t, ReasonReachMaxDecommissionNum, lastDpReason)
}

func TestCheckMpTagReachMaxDecommissionLimit(t *testing.T) {
	oldLimit := atomic.LoadUint64(&clusterMpTagDecommissionLimit)
	oldReason := LastMpQuitReason
	oldStatus := MpTagThreadStatus
	t.Cleanup(func() {
		atomic.StoreUint64(&clusterMpTagDecommissionLimit, oldLimit)
		tagStateMu.Lock()
		LastMpQuitReason = oldReason
		MpTagThreadStatus = oldStatus
		tagStateMu.Unlock()
	})
	tagStateMu.Lock()
	LastMpQuitReason = ""
	MpTagThreadStatus = StatusSleeping
	tagStateMu.Unlock()

	const addr = "10.0.0.2:17210"
	cfg := newClusterConfig()
	cfg.AutoFixTag.Store(true)
	mp := &MetaPartition{
		PartitionID: 201,
		Replicas: []*MetaReplica{
			{
				Addr:      addr,
				metaNode:  &MetaNode{Addr: addr, Tag: "node-tag"},
				StoreMode: proto.StoreModeMem,
			},
		},
		Peers: []proto.Peer{
			{Addr: addr, Tag: "target-tag"},
		},
		RecoverPair: proto.RecoverPair{DecommissionType: proto.TagDecommission},
	}
	vol := &Vol{
		Name:           "vol-mp-limit",
		Status:         proto.VolStatusNormal,
		MpTag:          "node-tag->target-tag",
		MetaPartitions: map[uint64]*MetaPartition{mp.PartitionID: mp},
	}
	vol.mpsLock = &mpsLockManager{}
	c := &Cluster{cfg: cfg}
	c.vols = map[string]*Vol{vol.Name: vol}
	c.BadMetaPartitionIds = new(sync.Map)
	c.RecoverMetaPartitionIds = new(sync.Map)
	c.BadMetaPartitionIds.Store(addr, []uint64{mp.PartitionID})

	atomic.StoreUint64(&clusterMpTagDecommissionLimit, 1)
	require.EqualValues(t, 1, c.GetMetaPartitionDecommissionCount(proto.TagDecommission))
	c.checkMpTag()
	_, _, _, lastMpReason, _, _, _ := snapshotTagState()
	require.Equal(t, ReasonReachMaxDecommissionNum, lastMpReason)
	require.False(t, c.IsClusterPlanNotIdle())
}

// TestGetSelectTagSummary tests getting select tag summary
func TestGetSelectTagSummary(t *testing.T) {
	// vol1 has explicit tags and vol2 relies on cluster defaults. Together they
	// verify that effective defaults count as tagged volumes in summaries.
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

	// This volume has no explicit tags, but the cluster defaults below should
	// still make it a tagged volume in the summary.
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

	// Default tags are set at the cluster level so empty volume tags still have
	// an effective select-tag configuration.
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
			BadMetaPartitionIds:     new(sync.Map),
			RecoverMetaPartitionIds: new(sync.Map),
		},
	}

	// Detail=false keeps this test focused on summary counters instead of node
	// space aggregation and mismatch samples.
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
	// contains is used heavily by selection and filtering code, so cover the
	// found, missing, and empty-slice branches directly.
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
			// The helper is intentionally order-insensitive and only checks
			// equality with the requested string.
			result := contains(tt.slice, tt.item)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestClusterGetMetaNodeSelectTag tests getting meta node select tag
func TestClusterGetMetaNodeSelectTag(t *testing.T) {
	// One node has a non-default tag and the other has DefaultTag, so both cache
	// hit variants are covered before the missing-node fallback.
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
	// Data node lookup follows the same cache-hit and cache-miss behavior as
	// meta node lookup.
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
	// Reuse an empty summary for branches that should return zero without adding
	// mismatch samples.
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagDecommissionNum),
	}

	t.Run("empty data partition list", func(t *testing.T) {
		// No partitions means no decommission work can be counted.
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
		// Empty meta partition maps should be safe as long as mpsLock is present.
		vol := &Vol{
			Name:           "empty-vol",
			MetaPartitions: map[uint64]*MetaPartition{},
		}
		vol.mpsLock = newMpsLockManager(vol)
		count := vol.countMpTagUnmatch(summary)
		assert.Equal(t, 0, count)
	})

	t.Run("handling nil replica", func(t *testing.T) {
		// Nil replicas can appear in partially built fixtures or defensive scan
		// paths; mismatch counting should skip them instead of panicking.
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

func TestSelectOneTargetMetaReplica(t *testing.T) {
	const (
		selectTag = "target-tag"
		srcAddr   = "10.0.0.1:17210"
	)

	// buildCluster creates a compact topology with a source node and optional
	// candidates at each fallback level: same nodeset, same zone, and other zone.
	buildCluster := func(withSameNodeSet, withSameZoneOtherNodeSet, withOtherZone bool) *Cluster {
		cluster := &Cluster{
			cfg: &clusterConfig{
				RackAwareLevel: proto.RackAwareNone,
			},
			ClusterTopoSubItem: ClusterTopoSubItem{
				t:         newTopology(),
				metaNodes: sync.Map{},
				dataNodes: sync.Map{},
			},
		}

		// addMetaNode wires the node into both topology and lookup cache, matching
		// the two data sources used by selectOneTargetMetaReplica.
		addMetaNode := func(ns *nodeSet, zoneName, addr, tag string, id uint64) {
			metaNode := &MetaNode{
				ID:                id,
				Addr:              addr,
				ZoneName:          zoneName,
				NodeSetID:         ns.ID,
				Tag:               tag,
				IsActive:          true,
				Total:             1024 * 1024 * 1024 * 10,
				Used:              0,
				MaxMemAvailWeight: defaultMetaNodeReservedMem * 2,
			}
			require.NoError(t, cluster.t.putMetaNode(metaNode))
			cluster.metaNodes.Store(addr, metaNode)
		}

		zone1 := newZone("zone1", proto.MediaType_Unspecified)
		ns11 := newNodeSet(nil, 1, 10, "zone1", "")
		ns12 := newNodeSet(nil, 2, 10, "zone1", "")
		require.NoError(t, zone1.putNodeSet(ns11))
		require.NoError(t, zone1.putNodeSet(ns12))
		require.NoError(t, cluster.t.putZone(zone1))

		zone2 := newZone("zone2", proto.MediaType_Unspecified)
		ns21 := newNodeSet(nil, 3, 10, "zone2", "")
		require.NoError(t, zone2.putNodeSet(ns21))
		require.NoError(t, cluster.t.putZone(zone2))

		// Source replica node.
		addMetaNode(ns11, "zone1", srcAddr, "src-tag", 1)

		// Same nodeset candidate.
		if withSameNodeSet {
			addMetaNode(ns11, "zone1", "10.0.0.2:17210", selectTag, 2)
		}

		// Same zone but different nodeset candidate.
		if withSameZoneOtherNodeSet {
			addMetaNode(ns12, "zone1", "10.0.0.3:17210", selectTag, 3)
		}

		// Different zone candidate.
		if withOtherZone {
			addMetaNode(ns21, "zone2", "10.0.0.4:17210", selectTag, 4)
		}

		// Ensure there is always at least one non-target node in other nodesets/zones,
		// so fallback path is exercised by tag filtering rather than empty topology.
		addMetaNode(ns12, "zone1", "10.0.0.13:17210", "other-tag", 13)
		addMetaNode(ns21, "zone2", "10.0.0.14:17210", "other-tag", 14)

		return cluster
	}

	// newMP builds a partition whose current host list contains only the source
	// replica. Target selection should exclude this address from candidates.
	newMP := func(src *MetaNode) *MetaPartition {
		return &MetaPartition{
			PartitionID: 100,
			Hosts:       []string{srcAddr},
			Replicas: []*MetaReplica{
				{
					Addr:     srcAddr,
					metaNode: src,
				},
			},
		}
	}

	t.Run("select from same nodeset", func(t *testing.T) {
		// When a tagged candidate exists in the source nodeset, it should be the
		// first choice.
		c := buildCluster(true, true, true)
		src, err := c.metaNode(srcAddr)
		require.NoError(t, err)

		addr, err := c.selectOneTargetMetaReplica(newMP(src), srcAddr, selectTag, proto.StoreModeMem)
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.2:17210", addr)
	})

	t.Run("select from same zone different nodeset", func(t *testing.T) {
		// Removing the same-nodeset candidate forces the same-zone fallback.
		c := buildCluster(false, true, true)
		src, err := c.metaNode(srcAddr)
		require.NoError(t, err)

		addr, err := c.selectOneTargetMetaReplica(newMP(src), srcAddr, selectTag, proto.StoreModeMem)
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.3:17210", addr)
	})

	t.Run("select from other zone", func(t *testing.T) {
		// With no local or same-zone candidate, selection should use a normal node
		// from another zone that satisfies the target tag.
		c := buildCluster(false, false, true)
		src, err := c.metaNode(srcAddr)
		require.NoError(t, err)

		addr, err := c.selectOneTargetMetaReplica(newMP(src), srcAddr, selectTag, proto.StoreModeMem)
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.4:17210", addr)
	})
}

func TestApplyTagRulesToPeers(t *testing.T) {
	t.Run("nil rules", func(t *testing.T) {
		// Nil rules are treated as a no-op. This protects callers that have no
		// effective tag configuration.
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "az1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(nil, peers, replicas)
		assert.False(t, changed)
		assert.Equal(t, "legacy", peers[0].Tag)
	})

	t.Run("apply one maps", func(t *testing.T) {
		// One explicit mapping plus padded default slots should preserve a
		// destination-tag replica and clear unrelated stale tags.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.3:17210", Tag: "legacy-learner"},
			{Addr: "10.0.0.4:17210", Tag: "legacy-1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag2", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		// The destination-tag replica consumes the explicit rule in the first
		// pass, while other stale peer tags are cleared to DefaultTag.
		assert.True(t, changed)
		assert.Equal(t, DefaultTag, peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag2", peers[3].Tag)
	})

	t.Run("apply two maps", func(t *testing.T) {
		// Two equivalent source rules allow two tag1 replicas to target tag2.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.3:17210", Tag: "legacy-learner"},
			{Addr: "10.0.0.4:17210", Tag: "legacy-1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		// The first and fourth replicas should receive tag2, the unmatched source
		// is cleared, and learner state is reset.
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag2", peers[3].Tag)
	})

	t.Run("apply three maps", func(t *testing.T) {
		// Three source mappings fill every slot, while a learner with a stale tag
		// still must be cleaned.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.3:17210", Tag: "tag2"},
			{Addr: "10.0.0.4:17210", Tag: "tag2"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, "tag2", peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag2", peers[3].Tag)
	})

	t.Run("apply two rules", func(t *testing.T) {
		// Multiple rule groups are allowed. This case verifies that only the
		// matching group contributes mappings for the current replica set.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
				{
					Groups: []*TagMapInfo{
						{Src: "tag3", Dst: "tag4"},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: DefaultTag},
			{Addr: "10.0.0.2:17210", Tag: DefaultTag},
			{Addr: "10.0.0.3:17210", Tag: DefaultTag},
			{Addr: "10.0.0.4:17210", Tag: DefaultTag},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, DefaultTag, peers[3].Tag)
	})

	t.Run("apply two rules two maps", func(t *testing.T) {
		// This exercises a second rule group with two available mappings and a
		// learner cleanup in the same peer list.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
				{
					Groups: []*TagMapInfo{
						{Src: "tag3", Dst: "tag4"},
						{Src: "tag3", Dst: "tag4"},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.2:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.3:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.4:17210", Tag: "oldtag1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag3", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag4", peers[0].Tag)
		assert.Equal(t, "tag4", peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, DefaultTag, peers[3].Tag)
	})

	t.Run("apply two rules three maps", func(t *testing.T) {
		// Destination-tag replicas can satisfy two slots before source mapping is
		// needed for the remaining source replica.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
					},
				},
				{
					Groups: []*TagMapInfo{
						{Src: "tag3", Dst: "tag4"},
						{Src: "tag3", Dst: "tag4"},
						{Src: "tag3", Dst: "tag4"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.2:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.3:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.4:17210", Tag: "oldtag1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag4", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag4", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag3", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag4", peers[0].Tag)
		assert.Equal(t, "tag4", peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag4", peers[3].Tag)
	})

	t.Run("apply null", func(t *testing.T) {
		// All-default rules represent "no special tag"; every stale peer tag
		// should be cleared.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.3:17210", Tag: "legacy-learner"},
			{Addr: "10.0.0.4:17210", Tag: "legacy-1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, DefaultTag, peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, DefaultTag, peers[3].Tag)
	})

	t.Run("apply destination and source rules with learner cleanup", func(t *testing.T) {
		// This mixes first-pass destination matching, second-pass source mapping,
		// and learner cleanup in one compact scenario.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: DefaultTag},
			{Addr: "10.0.0.3:17210", Tag: "legacy-learner"},
			{Addr: "10.0.0.4:17210", Tag: DefaultTag},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag2", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag2", peers[3].Tag)
	})

	t.Run("clear stale peer tag when no source rule matches", func(t *testing.T) {
		// A live replica with a node tag absent from all rules should not keep a
		// historical peer tag forever.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag4", Dst: "tag5"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.5:17210", Tag: "stale-tag"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.5:17210", nodeTag: "no-rule", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, DefaultTag, peers[0].Tag)
	})

	t.Run("apply unmatch source tag", func(t *testing.T) {
		// Every replica has an unmapped source tag, so applying the rules should
		// normalize all peer tags back to DefaultTag.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag3", Dst: "tag4"},
						{Src: "tag5", Dst: "tag6"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.2:17210", Tag: "legacy-1"},
			{Addr: "10.0.0.3:17210", Tag: "legacy-learner"},
			{Addr: "10.0.0.4:17210", Tag: "legacy-1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "legacy-1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "legacy-1", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "legacy-1", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, DefaultTag, peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, DefaultTag, peers[3].Tag)
	})

	t.Run("apply three rules", func(t *testing.T) {
		// Separate rule groups can each provide one source-to-destination mapping.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
					},
				},
				{
					Groups: []*TagMapInfo{
						{Src: "tag3", Dst: "tag4"},
					},
				},
				{
					Groups: []*TagMapInfo{
						{Src: "tag5", Dst: "tag6"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.2:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.3:17210", Tag: "oldtag1"},
			{Addr: "10.0.0.4:17210", Tag: "oldtag1"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag1", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag3", hasNodeTag: true},
			{addr: "10.0.0.3:17210", isLearner: true},
			{addr: "10.0.0.4:17210", nodeTag: "tag5", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, "tag4", peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
		assert.Equal(t, "tag6", peers[3].Tag)
	})

	t.Run("keep first pass result and skip second pass overwrite", func(t *testing.T) {
		// Once a destination-tag replica is handled in the first pass, the second
		// pass must not overwrite it with a default fallback.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: "legacy"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag2", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
	})

	t.Run("set null tag", func(t *testing.T) {
		// Default-to-default slots should clear a stale peer even when the node
		// tag is a non-default value.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
						{Src: DefaultTag, Dst: DefaultTag},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: DefaultTag},
			{Addr: "10.0.0.2:17210", Tag: DefaultTag},
			{Addr: "10.0.0.3:17210", Tag: "group20"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: DefaultTag, hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: DefaultTag, hasNodeTag: true},
			{addr: "10.0.0.3:17210", nodeTag: "groupali", hasNodeTag: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, DefaultTag, peers[0].Tag)
		assert.Equal(t, DefaultTag, peers[1].Tag)
		assert.Equal(t, DefaultTag, peers[2].Tag)
	})

	t.Run("learner stale tag should be cleared even when rules are fully marked early", func(t *testing.T) {
		// This guards against an early-exit regression: learner cleanup must still
		// run even after all destination slots have been matched.
		tagRules := &TagRulesInfo{
			Rules: []*TagGroupInfo{
				{
					Groups: []*TagMapInfo{
						{Src: "tag1", Dst: "tag2"},
						{Src: "tag3", Dst: "tag4"},
						{Src: "tag5", Dst: "tag6"},
					},
				},
			},
		}
		peers := []proto.Peer{
			{Addr: "10.0.0.1:17210", Tag: DefaultTag},
			{Addr: "10.0.0.2:17210", Tag: DefaultTag},
			{Addr: "10.0.0.3:17210", Tag: DefaultTag},
			{Addr: "10.0.0.4:17210", Tag: "stale-learner-tag"},
		}
		replicas := []tagReplicaInfo{
			{addr: "10.0.0.1:17210", nodeTag: "tag2", hasNodeTag: true},
			{addr: "10.0.0.2:17210", nodeTag: "tag4", hasNodeTag: true},
			{addr: "10.0.0.3:17210", nodeTag: "tag6", hasNodeTag: true},
			{addr: "10.0.0.4:17210", isLearner: true},
		}

		changed := applyTagRulesToPeers(tagRules, peers, replicas)
		assert.True(t, changed)
		assert.Equal(t, "tag2", peers[0].Tag)
		assert.Equal(t, "tag4", peers[1].Tag)
		assert.Equal(t, "tag6", peers[2].Tag)
		assert.Equal(t, DefaultTag, peers[3].Tag)
	})
}

// The tag-state helpers mutate package-level status and failed-key globals, so
// this test snapshots and restores them with t.Cleanup to avoid cross-test
// leakage. It also verifies snapshotTagState returns a defensive copy.
func TestTagStateSnapshotAndFailedKeys(t *testing.T) {
	// Capture every global touched by this test while holding tagStateMu, then
	// reset the globals to a controlled state for deterministic assertions.
	tagStateMu.Lock()
	oldDpStatus := DpTagThreadStatus
	oldMpStatus := MpTagThreadStatus
	oldDpReason := LastDpQuitReason
	oldMpReason := LastMpQuitReason
	oldDpTime := LastDpThreadTime
	oldMpTime := LastMpThreadTime
	oldFailedKeys := append([]string(nil), MpFailedKeys...)

	DpTagThreadStatus = StatusChecking
	MpTagThreadStatus = StatusCreatingPlan
	LastDpQuitReason = "dp-reason"
	LastMpQuitReason = "mp-reason"
	LastDpThreadTime = LastDpThreadTime.Add(1)
	LastMpThreadTime = LastMpThreadTime.Add(2)
	MpFailedKeys = nil
	tagStateMu.Unlock()

	// Restore the original global state even if an assertion fails.
	t.Cleanup(func() {
		tagStateMu.Lock()
		DpTagThreadStatus = oldDpStatus
		MpTagThreadStatus = oldMpStatus
		LastDpQuitReason = oldDpReason
		LastMpQuitReason = oldMpReason
		LastDpThreadTime = oldDpTime
		LastMpThreadTime = oldMpTime
		MpFailedKeys = oldFailedKeys
		tagStateMu.Unlock()
	})

	// Add one duplicate and then exceed the failed-key cap. The duplicate should
	// be ignored and the oldest retained keys should be trimmed.
	addMpFailedKey("key-0")
	addMpFailedKey("key-0")
	for i := 1; i <= MaxMpFailedKeys+1; i++ {
		addMpFailedKey(fmt.Sprintf("key-%d", i))
	}

	// The snapshot should reflect the controlled status fields and the bounded
	// failed-key window.
	dpStatus, mpStatus, lastDpReason, lastMpReason, _, _, failedKeys := snapshotTagState()
	require.Len(t, failedKeys, MaxMpFailedKeys)
	assert.Equal(t, StatusChecking, dpStatus)
	assert.Equal(t, StatusCreatingPlan, mpStatus)
	assert.Equal(t, "dp-reason", lastDpReason)
	assert.Equal(t, "mp-reason", lastMpReason)
	assert.Equal(t, "key-2", failedKeys[0])
	assert.Equal(t, fmt.Sprintf("key-%d", MaxMpFailedKeys+1), failedKeys[len(failedKeys)-1])

	// Mutating the returned slice must not mutate the package-level failed-key
	// storage.
	failedKeys[0] = "mutated"
	_, _, _, _, _, _, nextSnapshot := snapshotTagState()
	assert.Equal(t, "key-2", nextSnapshot[0])
}

// TagRulesInfo carries mutable match state. This test exercises the nil-safe
// helpers, rule parsing, destination marking, source matching, default-source
// fallback, and ClearMatch reset behavior in one focused place.
func TestTagRulesInfoAndParsing(t *testing.T) {
	// Nil receivers are accepted by all helper methods so callers can avoid
	// extra nil checks around optional tag configuration.
	assert.True(t, (*TagRulesInfo)(nil).IsEmpty())
	assert.True(t, (*TagRulesInfo)(nil).IsRuleAllTagMarked())
	assert.False(t, (*TagRulesInfo)(nil).MarkDestinationTag("dst"))
	_, ok := (*TagRulesInfo)(nil).FindDst("src")
	assert.False(t, ok)
	assert.Nil(t, (*TagRulesInfo)(nil).DstTags())

	// A two-slot rule is padded to TagReplicaRuleNum with a default-to-default
	// slot, which keeps the downstream matching logic uniform.
	rules := parseTagRules(" src1, src2 -> dst1, dst2 ")
	require.NotNil(t, rules)
	assert.False(t, rules.IsEmpty())
	assert.Equal(t, []string{"dst1", "dst2", DefaultTag}, rules.DstTags())
	assert.False(t, rules.IsRuleAllTagMarked())

	// Destination marking consumes a destination slot exactly once.
	assert.True(t, rules.MarkDestinationTag("dst2"))
	assert.False(t, rules.MarkDestinationTag("dst2"))
	// Exact source matches are preferred before default-source fallback.
	dst, ok := rules.FindDst("src1")
	assert.True(t, ok)
	assert.Equal(t, "dst1", dst)
	dst, ok = rules.FindDst("unknown")
	assert.True(t, ok)
	assert.Equal(t, DefaultTag, dst)
	assert.True(t, rules.IsRuleAllTagMarked())

	// ClearMatch resets every slot so the same parsed rules can be reused for a
	// new partition.
	rules.ClearMatch()
	assert.False(t, rules.IsRuleAllTagMarked())
	dst, ok = rules.FindDst("src2")
	assert.True(t, ok)
	assert.Equal(t, "dst2", dst)

	// Parsing should ignore null items, malformed groups, and null-only rules.
	multiRules := parseTagRules("az1,null,az2 -> dst1, null, dst2; bad-rule; az3->dst3; null")
	require.NotNil(t, multiRules)
	assert.Equal(t, []string{"dst1", "dst2", "dst3"}, multiRules.DstTags())
	assert.Nil(t, parseTagRules(""))
	assert.Nil(t, parseTagRules("null; ;bad-rule"))
	assert.Equal(t, []string{"a", "b"}, splitTagItems(" a, null, , b "))

	// Empty rule info still contains default slots so callers can normalize peer
	// tags without special-casing an empty configuration.
	emptyRules := getEmptyTagRulesInfo()
	require.NotNil(t, emptyRules)
	assert.False(t, emptyRules.IsEmpty())
	assert.Equal(t, []string{DefaultTag, DefaultTag, DefaultTag}, emptyRules.DstTags())
}

// TestGetEffectiveTagList verifies the precedence used to resolve effective
// data and meta tag rules: volume tag, then cluster default, then empty rules.
func TestGetEffectiveTagList(t *testing.T) {
	// Cluster defaults are used only after volume-level tags are cleared.
	cluster := &Cluster{
		cfg: &clusterConfig{
			DefaultDpTag: "cluster-dp-src->cluster-dp-dst",
			DefaultMpTag: "cluster-mp-src->cluster-mp-dst",
		},
	}

	// Start with explicit volume tags so they win over cluster defaults.
	vol := &Vol{
		DpTag: "vol-dp-src->vol-dp-dst",
		MpTag: "vol-mp-src->vol-mp-dst",
	}
	assert.Equal(t, []string{"vol-dp-dst", DefaultTag, DefaultTag}, vol.GetDpTagList(cluster).DstTags())
	assert.Equal(t, []string{"vol-mp-dst", DefaultTag, DefaultTag}, vol.GetMpTagList(cluster).DstTags())

	// Clearing volume tags exposes the cluster-level fallback.
	vol.DpTag = ""
	vol.MpTag = ""
	assert.Equal(t, []string{"cluster-dp-dst", DefaultTag, DefaultTag}, vol.GetDpTagList(cluster).DstTags())
	assert.Equal(t, []string{"cluster-mp-dst", DefaultTag, DefaultTag}, vol.GetMpTagList(cluster).DstTags())

	// Clearing cluster defaults leaves only the empty default mapping set.
	cluster.cfg.DefaultDpTag = ""
	cluster.cfg.DefaultMpTag = ""
	assert.Equal(t, []string{DefaultTag, DefaultTag, DefaultTag}, vol.GetDpTagList(cluster).DstTags())
	assert.Equal(t, []string{DefaultTag, DefaultTag, DefaultTag}, vol.GetMpTagList(cluster).DstTags())
}

// TestPartitionHasTagByPeerTag covers historical peer tags in addition to
// current volume and cluster tag configuration.
func TestPartitionHasTagByPeerTag(t *testing.T) {
	cluster := &Cluster{cfg: &clusterConfig{}}

	// One live peer has a tag and one discarded partition also has a tag. Only
	// the live partition should make IsDataPartitionHasTag return true.
	dataPartition := &DataPartition{
		PartitionID: 1,
		Peers: []proto.Peer{
			{Addr: "10.0.0.1:17310", Tag: DefaultTag},
			{Addr: "10.0.0.2:17310", Tag: "peer-dp-tag"},
		},
	}
	// Discarded partitions are intentionally ignored by data tag presence checks.
	discardPartition := &DataPartition{
		PartitionID: 2,
		IsDiscard:   true,
		Peers: []proto.Peer{
			{Addr: "10.0.0.3:17310", Tag: "discard-tag"},
		},
	}
	// Initialize both data and meta collections because the same volume is reused
	// for data and meta presence assertions.
	vol := &Vol{
		dataPartitions: &DataPartitionMap{
			partitionMap: map[uint64]*DataPartition{1: dataPartition, 2: discardPartition},
			partitions:   []*DataPartition{dataPartition, discardPartition},
		},
		MetaPartitions: map[uint64]*MetaPartition{},
	}
	vol.mpsLock = newMpsLockManager(vol)

	// Peer tag presence should be enough; once cleared, only configured tags can
	// make the helper return true.
	assert.True(t, vol.IsDataPartitionHasTag(cluster))
	dataPartition.Peers[1].Tag = DefaultTag
	assert.False(t, vol.IsDataPartitionHasTag(cluster))
	vol.DpTag = "vol-dp-tag"
	assert.True(t, vol.IsDataPartitionHasTag(cluster))

	// Meta partition peer tags follow the same effective-tag logic, but nil or
	// missing partitions are handled through cloneMetaPartitionMap.
	metaPartition := &MetaPartition{
		PartitionID: 3,
		Peers: []proto.Peer{
			{Addr: "10.0.0.4:17210", Tag: DefaultTag},
			{Addr: "10.0.0.5:17210", Tag: "peer-mp-tag"},
		},
	}
	vol.MpTag = DefaultTag
	vol.MetaPartitions = map[uint64]*MetaPartition{3: metaPartition}
	// A non-default meta peer tag should make the volume tag-aware.
	assert.True(t, vol.IsMetaPartitionHasTag(cluster))
	metaPartition.Peers[1].Tag = DefaultTag
	// With peer tags cleared and no effective volume or cluster tag, the helper
	// reports false.
	assert.False(t, vol.IsMetaPartitionHasTag(cluster))
	cluster.cfg.DefaultMpTag = "cluster-mp-tag"
	// Cluster defaults are the final fallback.
	assert.True(t, vol.IsMetaPartitionHasTag(cluster))
}

// TestAppendMismatchSampleLimit ensures API samples are capped to
// MaxTagSampleNum for both data and meta partition mismatch samples.
func TestAppendMismatchSampleLimit(t *testing.T) {
	// Preallocate the sample slices the same way summary construction does.
	summary := &proto.TagSummary{
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
	}

	// Append beyond the limit to prove excess samples are ignored.
	for i := 0; i < MaxTagSampleNum+2; i++ {
		appendMismatchDpSample(summary, "vol", uint64(i), fmt.Sprintf("dp-%d", i), "peer", "node")
		appendMismatchMpSample(summary, "vol", uint64(i), fmt.Sprintf("mp-%d", i), "peer", "node")
	}

	// The earliest samples are retained and the length never exceeds the cap.
	require.Len(t, summary.UnmatchDpSamples, MaxTagSampleNum)
	require.Len(t, summary.UnmatchMpSamples, MaxTagSampleNum)
	assert.Equal(t, uint64(0), summary.UnmatchDpSamples[0].PartitionID)
	assert.Equal(t, uint64(MaxTagSampleNum-1), summary.UnmatchDpSamples[MaxTagSampleNum-1].PartitionID)
	assert.Equal(t, uint64(0), summary.UnmatchMpSamples[0].PartitionID)
	assert.Equal(t, uint64(MaxTagSampleNum-1), summary.UnmatchMpSamples[MaxTagSampleNum-1].PartitionID)
}

// TestFormatTagAndJoinUint64 covers public tag-string normalization and the
// small formatting helpers used by tag summary output.
func TestFormatTagAndJoinUint64(t *testing.T) {
	// FormatTag should drop empty/null/malformed pieces and return a stable API
	// string for the remaining valid rules.
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty tag", "", DefaultTag},
		{"null tag", EmptyTag, DefaultTag},
		{"trim groups", " az1, null, az2 -> dst1, , dst2 ", "az1,az2->dst1,dst2"},
		{"skip invalid rules", "bad; src-> ; ->dst ; src3->dst3", "src3->dst3"},
		{"join valid rules", "src1->dst1; src2,src3 -> dst2,dst3", "src1->dst1;src2,src3->dst2,dst3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each case verifies the fully normalized output string.
			assert.Equal(t, tt.expected, FormatTag(tt.input))
		})
	}

	// Exercise helpers directly for their empty and non-empty behaviors.
	assert.Equal(t, "", formatTagGroup("null, , "))
	assert.Equal(t, "a,b", formatTagGroup(" a, null, b "))
	assert.Equal(t, "", joinUint64(nil))
	assert.Equal(t, "1,20,300", joinUint64([]uint64{1, 20, 300}))
}

// TestCollectAndSelectMpTagMismatchGroup verifies that meta partition mismatches
// are grouped by target tag and store mode, and that failed groups are skipped.
func TestCollectAndSelectMpTagMismatchGroup(t *testing.T) {
	// buildMetaPartition creates one-replica partitions with enough cached node
	// metadata for collectAndSelectMpTagMismatchGroup to avoid cluster lookups.
	buildMetaPartition := func(id uint64, addr, nodeTag, peerTag string, storeMode proto.StoreMode) *MetaPartition {
		metaNode := &MetaNode{Addr: addr, Tag: nodeTag}
		return &MetaPartition{
			PartitionID: id,
			Replicas: []*MetaReplica{
				{
					Addr:      addr,
					metaNode:  metaNode,
					StoreMode: storeMode,
				},
			},
			Peers: []proto.Peer{
				{Addr: addr, Tag: peerTag},
			},
		}
	}
	// buildVol initializes mpsLock because cloneMetaPartitionMap is called by
	// the grouping helper.
	buildVol := func(name string, partitions ...*MetaPartition) *Vol {
		vol := &Vol{
			Name:           name,
			MetaPartitions: make(map[uint64]*MetaPartition),
		}
		for _, partition := range partitions {
			vol.MetaPartitions[partition.PartitionID] = partition
		}
		vol.mpsLock = newMpsLockManager(vol)
		return vol
	}

	// target-a has two eligible mismatches, target-b has one, and the remaining
	// partitions prove matched/default/recovering cases are ignored.
	targetA1 := buildMetaPartition(1, "10.0.0.1:17210", "node-a", "target-a", proto.StoreModeMem)
	targetA2 := buildMetaPartition(2, "10.0.0.2:17210", "node-b", "target-a", proto.StoreModeMem)
	targetB := buildMetaPartition(3, "10.0.0.3:17210", "node-c", "target-b", proto.StoreModeMem)
	matched := buildMetaPartition(4, "10.0.0.4:17210", "same-tag", "same-tag", proto.StoreModeMem)
	defaultPeerTag := buildMetaPartition(5, "10.0.0.5:17210", "node-e", DefaultTag, proto.StoreModeMem)
	recovering := buildMetaPartition(6, "10.0.0.6:17210", "node-f", "target-a", proto.StoreModeMem)
	recovering.IsRecover.Store(true)

	// The cluster only needs cfg for this path because every replica already has
	// a cached MetaNode and StoreMode.
	cluster := &Cluster{cfg: &clusterConfig{}}
	vols := map[string]*Vol{
		"vol-1": buildVol("vol-1", targetA1, targetB, matched),
		"vol-2": buildVol("vol-2", targetA2, defaultPeerTag, recovering),
	}
	memMode := proto.StoreModeMem
	memModeKey := memMode.Str()

	// Without failed keys, the largest target-a group should be selected.
	selected := cluster.collectAndSelectMpTagMismatchGroup(vols, nil)
	require.Len(t, selected, 2)
	assert.Equal(t, "target-a", selected[0].tag)
	assert.Equal(t, "target-a", selected[1].tag)

	// Once target-a is marked failed for Memory mode, target-b becomes the best
	// remaining group.
	selected = cluster.collectAndSelectMpTagMismatchGroup(vols, []string{"target-a|" + memModeKey})
	require.Len(t, selected, 1)
	assert.Equal(t, "target-b", selected[0].tag)

	// When all candidate groups are failed, the helper returns no selected work.
	selected = cluster.collectAndSelectMpTagMismatchGroup(vols, []string{
		"target-a|" + memModeKey,
		"target-b|" + memModeKey,
	})
	assert.Empty(t, selected)
}
