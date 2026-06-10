// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package master

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
)

func TestNewClusterConfig_defaults(t *testing.T) {
	t.Parallel()
	cfg := newClusterConfig()
	require.NotNil(t, cfg)
	require.Equal(t, defaultNumberOfDataPartitionsToLoad, cfg.numberOfDataPartitionsToLoad)
	require.EqualValues(t, defaultMetaPartitionTimeOutSec, cfg.MetaPartitionTimeOutSec)
	require.EqualValues(t, defaultMPLearnerNum, cfg.MaxMPLearnerNum)
	require.EqualValues(t, defaultFollowerReadLeaseTime, cfg.FollowerReadLeaseTime)
	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, cfg.FollowerReadLeaseTime)
	require.True(t, cfg.EnableLeaderMetricsReset)
	require.Equal(t, defaultFlashNodeHandleReadTimeout, cfg.flashNodeHandleReadTimeout)
	require.Equal(t, defaultDpLimitSsdBaseCount, cfg.DpLimitSsdBaseCount)
	require.Equal(t, defaultDpLimitHddFactor, cfg.DpLimitHddFactor)
	require.Nil(t, cfg.peers)
	require.Nil(t, cfg.peerAddrs)
}

func TestNormalizeTagDecommissionLimit(t *testing.T) {
	t.Parallel()
	require.EqualValues(t, defaultMaxDpTagDecommissionLimit, normalizeDpTagDecommissionLimit(0))
	require.EqualValues(t, defaultMaxDpTagDecommissionLimit-1, normalizeDpTagDecommissionLimit(defaultMaxDpTagDecommissionLimit-1))
	require.EqualValues(t, defaultMaxDpTagDecommissionLimit+1, normalizeDpTagDecommissionLimit(defaultMaxDpTagDecommissionLimit+1))

	require.EqualValues(t, defaultMaxMpTagDecommissionLimit, normalizeMpTagDecommissionLimit(0))
	require.EqualValues(t, defaultMaxMpTagDecommissionLimit-1, normalizeMpTagDecommissionLimit(defaultMaxMpTagDecommissionLimit-1))
	require.EqualValues(t, defaultMaxMpTagDecommissionLimit+1, normalizeMpTagDecommissionLimit(defaultMaxMpTagDecommissionLimit+1))
}

func newTagLimitTestCluster(name string, partition raftstore.Partition) *Cluster {
	cfg := newClusterConfig()
	return &Cluster{
		Name:           name,
		cfg:            cfg,
		partition:      partition,
		QosAcceptLimit: rate.NewLimiter(rate.Limit(cfg.QosMasterAcceptLimit), proto.QosDefaultBurst),
	}
}

func TestClusterTagDecommissionLimitSetters(t *testing.T) {
	oldDp := atomic.LoadUint64(&clusterDpTagDecommissionLimit)
	oldMp := atomic.LoadUint64(&clusterMpTagDecommissionLimit)
	t.Cleanup(func() {
		atomic.StoreUint64(&clusterDpTagDecommissionLimit, oldDp)
		atomic.StoreUint64(&clusterMpTagDecommissionLimit, oldMp)
	})

	c := newTagLimitTestCluster("tag-limit-test", &mockPartition{isLeader: true})
	require.NoError(t, c.setMaxDpTagDecommissionLimit(7))
	require.EqualValues(t, 7, c.getMaxDpTagDecommissionLimit())
	require.NoError(t, c.setMaxDpTagDecommissionLimit(0))
	require.EqualValues(t, defaultMaxDpTagDecommissionLimit, c.getMaxDpTagDecommissionLimit())

	require.NoError(t, c.setMaxMpTagDecommissionLimit(3))
	require.EqualValues(t, 3, c.getMaxMpTagDecommissionLimit())
	require.NoError(t, c.setMaxMpTagDecommissionLimit(0))
	require.EqualValues(t, defaultMaxMpTagDecommissionLimit, c.getMaxMpTagDecommissionLimit())
}

func TestClusterTagDecommissionLimitSettersRollbackOnPersistFailure(t *testing.T) {
	oldDp := atomic.LoadUint64(&clusterDpTagDecommissionLimit)
	oldMp := atomic.LoadUint64(&clusterMpTagDecommissionLimit)
	t.Cleanup(func() {
		atomic.StoreUint64(&clusterDpTagDecommissionLimit, oldDp)
		atomic.StoreUint64(&clusterMpTagDecommissionLimit, oldMp)
	})

	c := newTagLimitTestCluster("tag-limit-fail-test", &failingSubmitPartition{mockPartition: mockPartition{isLeader: true}})

	atomic.StoreUint64(&clusterDpTagDecommissionLimit, 11)
	err := c.setMaxDpTagDecommissionLimit(12)
	require.ErrorIs(t, err, proto.ErrPersistenceByRaft)
	require.EqualValues(t, 11, atomic.LoadUint64(&clusterDpTagDecommissionLimit))

	atomic.StoreUint64(&clusterMpTagDecommissionLimit, 4)
	err = c.setMaxMpTagDecommissionLimit(5)
	require.ErrorIs(t, err, proto.ErrPersistenceByRaft)
	require.EqualValues(t, 4, atomic.LoadUint64(&clusterMpTagDecommissionLimit))
}

func TestParsePeerAddr(t *testing.T) {
	t.Parallel()
	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		id, ip, port, err := parsePeerAddr("1:192.168.1.1:8080")
		require.NoError(t, err)
		require.EqualValues(t, 1, id)
		require.Equal(t, "192.168.1.1", ip)
		require.EqualValues(t, 8080, port)
	})

	t.Run("invalid_id", func(t *testing.T) {
		t.Parallel()
		_, _, _, err := parsePeerAddr("not-a-number:192.168.1.1:8080")
		require.Error(t, err)
	})

	t.Run("invalid_port", func(t *testing.T) {
		t.Parallel()
		_, _, _, err := parsePeerAddr("1:192.168.1.1:not-a-port")
		require.Error(t, err)
	})
}

func TestUpdateFollowerReadLeaseTime(t *testing.T) {
	t.Parallel()
	c := &Cluster{cfg: newClusterConfig()}
	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, c.cfg.FollowerReadLeaseTime)

	c.updateFollowerReadLeaseTime(0)
	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, c.cfg.FollowerReadLeaseTime)

	c.updateFollowerReadLeaseTime(1800)
	require.EqualValues(t, 1800, c.cfg.FollowerReadLeaseTime)

	c.updateFollowerReadLeaseTime(proto.MaxFollowerReadLeaseTimeSec + 100)
	require.EqualValues(t, proto.MaxFollowerReadLeaseTimeSec, c.cfg.FollowerReadLeaseTime)

	c.updateFollowerReadLeaseTime(1)
	require.EqualValues(t, proto.MinFollowerReadLeaseTimeSec, c.cfg.FollowerReadLeaseTime)
}

func TestSetMetaNodeFollowerReadLeaseTime_validate(t *testing.T) {
	t.Parallel()
	c := &Cluster{cfg: newClusterConfig()}
	old := c.cfg.FollowerReadLeaseTime

	require.Error(t, c.setMetaNodeFollowerReadLeaseTime(0))
	require.EqualValues(t, old, c.cfg.FollowerReadLeaseTime)

	require.Error(t, c.setMetaNodeFollowerReadLeaseTime(proto.MaxFollowerReadLeaseTimeSec+1))
	require.EqualValues(t, old, c.cfg.FollowerReadLeaseTime)

	require.NoError(t, proto.ValidateFollowerReadLeaseTime(proto.DefaultFollowerReadLeaseTimeSec))
	require.NoError(t, proto.ValidateFollowerReadLeaseTime(proto.MaxFollowerReadLeaseTimeSec))
}

func TestClusterConfig_parsePeers(t *testing.T) {
	t.Run("ok_single", func(t *testing.T) {
		cfg := newClusterConfig()
		cfg.heartbeatPort = 5901
		cfg.replicaPort = 5902
		const peerLine = "7:10.0.0.7:17000"
		err := cfg.parsePeers(peerLine)
		require.NoError(t, err)
		require.Equal(t, []string{peerLine}, cfg.peerAddrs)
		require.Len(t, cfg.peers, 1)
		require.EqualValues(t, 7, cfg.peers[0].ID)
		require.Equal(t, "10.0.0.7", cfg.peers[0].Address)
		require.Equal(t, 5901, cfg.peers[0].HeartbeatPort)
		require.Equal(t, 5902, cfg.peers[0].ReplicaPort)
		require.Equal(t, "10.0.0.7:17000", AddrDatabase[7])
		delete(AddrDatabase, 7)
	})

	t.Run("ok_multiple", func(t *testing.T) {
		cfg := newClusterConfig()
		cfg.heartbeatPort = 1
		cfg.replicaPort = 2
		line := "101:10.1.1.1:9101,102:10.1.1.2:9102"
		err := cfg.parsePeers(line)
		require.NoError(t, err)
		require.Len(t, cfg.peers, 2)
		require.Equal(t, "10.1.1.2:9102", AddrDatabase[102])
		delete(AddrDatabase, 101)
		delete(AddrDatabase, 102)
	})

	t.Run("err_bad_peer", func(t *testing.T) {
		t.Cleanup(func() {
			delete(AddrDatabase, 1)
		})
		cfg := newClusterConfig()
		cfg.heartbeatPort = 1
		cfg.replicaPort = 2
		err := cfg.parsePeers("1:10.0.0.1:9000,bad-peer")
		require.Error(t, err)
	})
}

func TestClusterConfig_checkRaftPartitionCanUseDifferentPort(t *testing.T) {
	if server == nil || server.rocksDBStore == nil {
		t.Skip("no master server / rocksdb in this test run")
	}
	cfg := server.cluster.cfg
	before := cfg.raftPartitionCanUseDifferentPort.Load()
	t.Cleanup(func() {
		_ = cfg.checkRaftPartitionCanUseDifferentPort(server, before)
	})
	// Enable different port (idempotent when already on); avoids disable path that can fail if persisted cluster forbids it.
	require.NoError(t, cfg.checkRaftPartitionCanUseDifferentPort(server, true))
}
