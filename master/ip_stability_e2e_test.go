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
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// E2E tests simulate "address change" by re-registering nodes with localhost (same port as mock).
// They use an isolated Master (createIsolatedMasterServerForE2E) and per-test mock port ranges so they do not pollute the global server.

const (
	e2eRaftHeartbeat = "17311"
	e2eRaftReplica   = "17312"
)

// e2eLocalhostAddr returns "localhost:" + port from "host:port", so the mock remains reachable at the same port.
func e2eLocalhostAddr(hostPort string) string {
	_, port, _ := strings.Cut(hostPort, ":")
	if port == "" {
		return "localhost:0"
	}
	return "localhost:" + port
}

// TestCheckIpPortAcceptsFQDN ensures Master accepts FQDN for addDataNode/addMetaNode (K8s registerAddr).
func TestCheckIpPortAcceptsFQDN(t *testing.T) {
	require.True(t, checkIpPort("127.0.0.1:8080"), "IPv4:port should be accepted")
	require.True(t, checkIpPort("datanode-0.datanode-svc.cubefs.svc.cluster.local:17310"), "FQDN:port should be accepted")
	require.True(t, checkIpPort("master-0.master-svc.ns.svc.cluster.local:17010"), "FQDN:port should be accepted")
	require.False(t, checkIpPort(":8080"), "missing host should be rejected")
	require.False(t, checkIpPort("127.0.0.1:80"), "port < 1024 should be rejected")
}

// TestIPStabilityDataNodeReRegister is an e2e-style test for K8s Pod IP stability:
// 1) Enable dynamic addr on cluster.
// 2) Pick an existing data node (mds1), get its nodeID.
// 3) Re-register the same node with a new address via AddDataNode(nodeId=...).
// 4) Assert the node's address is updated and all data partitions that contained the old addr now have the new addr.
// Uses an isolated Master so it does not pollute the global server used by other tests.
func TestIPStabilityDataNodeReRegister(t *testing.T) {
	srv, e2eHost, firstDataAddr, _, cleanup := createIsolatedMasterServerForE2E("8081")
	defer cleanup()

	srv.cluster.cfg.EnableDynamicAddr = true
	defer func() { srv.cluster.cfg.EnableDynamicAddr = false }()

	oldAddr := firstDataAddr
	e2eDataNodeNewAddr := e2eLocalhostAddr(firstDataAddr)
	dn, err := srv.cluster.dataNode(oldAddr)
	require.NoError(t, err)
	require.NotNil(t, dn)
	nodeID := dn.ID
	require.NotZero(t, nodeID, "data node should have non-zero ID")

	// Collect data partitions that contain oldAddr before re-register.
	vol, err := srv.cluster.getVol(commonVolName)
	require.NoError(t, err)
	require.NotNil(t, vol)
	dpsAll := vol.dataPartitions.clonePartitions()
	var dpsWithOldAddr []*DataPartition
	for _, dp := range dpsAll {
		dp.RLock()
		for _, h := range dp.Hosts {
			if h == oldAddr {
				dpsWithOldAddr = append(dpsWithOldAddr, dp)
				break
			}
		}
		dp.RUnlock()
	}

	// Re-register the same node with new address (simulating pod IP change).
	reqURL := fmt.Sprintf("%s%s?addr=%s&heartbeatPort=%s&replicaPort=%s&zoneName=%s&nodeId=%d&mediaType=%d",
		e2eHost, proto.AddDataNode,
		url.QueryEscape(e2eDataNodeNewAddr), e2eRaftHeartbeat, e2eRaftReplica,
		url.QueryEscape(testZone1), nodeID, defaultMediaType)
	// AddDataNode is GET in proto; use GET.
	resp, err := http.Get(reqURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "addDataNode re-register should succeed")

	// Assert: node is now found at new address with same ID.
	dnNew, err := srv.cluster.dataNode(e2eDataNodeNewAddr)
	require.NoError(t, err)
	require.NotNil(t, dnNew)
	require.Equal(t, nodeID, dnNew.ID, "re-registered node should keep same NodeID")
	require.Equal(t, e2eDataNodeNewAddr, dnNew.Addr)

	// Assert: node should not be found at old address (index is by new addr).
	_, err = srv.cluster.dataNode(oldAddr)
	require.Error(t, err)

	// Assert: all data partitions that had oldAddr in Hosts now have new addr.
	for _, dp := range dpsWithOldAddr {
		dp.RLock()
		hasOld := false
		hasNew := false
		for _, h := range dp.Hosts {
			if h == oldAddr {
				hasOld = true
			}
			if h == e2eDataNodeNewAddr {
				hasNew = true
			}
		}
		dp.RUnlock()
		require.False(t, hasOld, "partition %d should no longer have old addr in Hosts", dp.PartitionID)
		require.True(t, hasNew, "partition %d should have new addr in Hosts", dp.PartitionID)
	}

	// Restore node to original address on isolated cluster (clean state for consistency).
	restoreURL := fmt.Sprintf("%s%s?addr=%s&heartbeatPort=%s&replicaPort=%s&zoneName=%s&nodeId=%d&mediaType=%d",
		e2eHost, proto.AddDataNode,
		url.QueryEscape(oldAddr), e2eRaftHeartbeat, e2eRaftReplica,
		url.QueryEscape(testZone1), nodeID, defaultMediaType)
	restoreResp, _ := http.Get(restoreURL)
	if restoreResp != nil {
		restoreResp.Body.Close()
	}
	for i := 0; i < 30; i++ {
		dnRestored, err := srv.cluster.dataNode(oldAddr)
		if err == nil && dnRestored != nil && dnRestored.ID == nodeID {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

const (
	e2eMetaRaftHeartbeat = "17411"
	e2eMetaRaftReplica   = "17412"
)

// TestIPStabilityMetaNodeReRegister is an e2e-style test for MetaNode re-registration (K8s Pod IP stability).
// Uses an isolated Master so it does not pollute the global server used by other tests.
func TestIPStabilityMetaNodeReRegister(t *testing.T) {
	srv, e2eHost, _, firstMetaAddr, cleanup := createIsolatedMasterServerForE2E("8082")
	defer cleanup()

	srv.cluster.cfg.EnableDynamicAddr = true
	defer func() { srv.cluster.cfg.EnableDynamicAddr = false }()

	oldAddr := firstMetaAddr
	e2eMetaNodeNewAddr := e2eLocalhostAddr(firstMetaAddr)
	mn, err := srv.cluster.metaNode(oldAddr)
	require.NoError(t, err)
	require.NotNil(t, mn)
	nodeID := mn.ID
	require.NotZero(t, nodeID)

	vol, err := srv.cluster.getVol(commonVolName)
	require.NoError(t, err)
	require.NotNil(t, vol)
	mpList := vol.getSortMetaPartitions()
	var mpsWithOldAddr []*MetaPartition
	for _, mp := range mpList {
		mp.RLock()
		for _, h := range mp.Hosts {
			if h == oldAddr {
				mpsWithOldAddr = append(mpsWithOldAddr, mp)
				break
			}
		}
		mp.RUnlock()
	}

	reqURL := fmt.Sprintf("%s%s?addr=%s&heartbeatPort=%s&replicaPort=%s&zoneName=%s&nodeId=%d",
		e2eHost, proto.AddMetaNode,
		url.QueryEscape(e2eMetaNodeNewAddr), e2eMetaRaftHeartbeat, e2eMetaRaftReplica,
		url.QueryEscape(testZone1), nodeID)
	resp, err := http.Get(reqURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "addMetaNode re-register should succeed")

	mnNew, err := srv.cluster.metaNode(e2eMetaNodeNewAddr)
	require.NoError(t, err)
	require.NotNil(t, mnNew)
	require.Equal(t, nodeID, mnNew.ID)
	require.Equal(t, e2eMetaNodeNewAddr, mnNew.Addr)

	_, err = srv.cluster.metaNode(oldAddr)
	require.Error(t, err)

	for _, mp := range mpsWithOldAddr {
		mp.RLock()
		hasOld, hasNew := false, false
		for _, h := range mp.Hosts {
			if h == oldAddr {
				hasOld = true
			}
			if h == e2eMetaNodeNewAddr {
				hasNew = true
			}
		}
		mp.RUnlock()
		require.False(t, hasOld, "meta partition %d should no longer have old addr", mp.PartitionID)
		require.True(t, hasNew, "meta partition %d should have new addr", mp.PartitionID)
	}

	// Restore node to original address on isolated cluster.
	restoreURL := fmt.Sprintf("%s%s?addr=%s&heartbeatPort=%s&replicaPort=%s&zoneName=%s&nodeId=%d",
		e2eHost, proto.AddMetaNode,
		url.QueryEscape(oldAddr), e2eMetaRaftHeartbeat, e2eMetaRaftReplica,
		url.QueryEscape(testZone1), nodeID)
	restoreResp, _ := http.Get(restoreURL)
	if restoreResp != nil {
		restoreResp.Body.Close()
	}
	for i := 0; i < 30; i++ {
		mnRestored, err := srv.cluster.metaNode(oldAddr)
		if err == nil && mnRestored != nil && mnRestored.ID == nodeID {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestIPStabilityReRegisterRejectedWhenDisabled verifies that when enableDynamicAddr is false,
// re-registration with existing nodeId returns an error (per design clarification).
// Uses an isolated Master so it does not pollute the global server used by other tests.
func TestIPStabilityReRegisterRejectedWhenDisabled(t *testing.T) {
	srv, e2eHost, firstDataAddr, _, cleanup := createIsolatedMasterServerForE2E("8083")
	defer cleanup()

	srv.cluster.cfg.EnableDynamicAddr = false
	defer func() { srv.cluster.cfg.EnableDynamicAddr = false }()

	// On isolated cluster node is at firstDataAddr
	dn, err := srv.cluster.dataNode(firstDataAddr)
	require.NoError(t, err)
	require.NotNil(t, dn)
	nodeID := dn.ID
	require.NotZero(t, nodeID)

	// Try to re-register with a new addr (different from current) so Master enters re-register path and rejects
	rejectAddr := "127.0.0.1:9198"
	reqURL := fmt.Sprintf("%s%s?addr=%s&heartbeatPort=%s&replicaPort=%s&zoneName=%s&nodeId=%d&mediaType=%d",
		e2eHost, proto.AddDataNode,
		url.QueryEscape(rejectAddr), e2eRaftHeartbeat, e2eRaftReplica,
		url.QueryEscape(testZone1), nodeID, defaultMediaType)
	resp, err := http.Get(reqURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	var reply proto.HTTPReply
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reply))
	if resp.StatusCode == http.StatusOK {
		require.NotEqual(t, proto.ErrCodeSuccess, reply.Code, "re-register with nodeId should fail when enableDynamicAddr is disabled")
	}
	require.Contains(t, reply.Msg, "enableDynamicAddr", "error message should mention enableDynamicAddr")
}
