package master

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/require"
)

func buildPanicCluster() *Cluster {
	c := newCluster(server.cluster.Name, server.cluster.leaderInfo, server.cluster.fsm, server.cluster.partition, server.config, server)
	v := buildPanicVol()
	c.putVol(v)
	return c
}

func buildPanicVol() *Vol {
	id, err := server.cluster.idAlloc.allocateCommonID()
	if err != nil {
		return nil
	}
	createTime := time.Now().Unix() // record create time of this volume

	vv := volValue{
		ID:                id,
		Name:              commonVol.Name,
		Owner:             commonVol.Owner,
		ZoneName:          "",
		DataPartitionSize: commonVol.dataPartitionSize,
		Capacity:          commonVol.Capacity,
		DpReplicaNum:      defaultReplicaNum,
		ReplicaNum:        defaultReplicaNum,
		FollowerRead:      false,
		Authenticate:      false,
		CrossZone:         false,
		DefaultPriority:   false,
		CreateTime:        createTime,
		Description:       "",
	}

	vol := newVol(vv)
	vol.dataPartitions = nil
	return vol
}

func TestCheckDataPartitions(t *testing.T) {
	server.cluster.checkDataPartitions()
}

func TestPanicCheckDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.checkDataPartitions()
	t.Logf("catched panic")
}

func TestCheckBackendLoadDataPartitions(t *testing.T) {
	server.cluster.scheduleToLoadDataPartitions()
}

func TestPanicBackendLoadDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.scheduleToLoadDataPartitions()
	t.Logf("catched panic")
}

func TestCheckReleaseDataPartitions(t *testing.T) {
	server.cluster.releaseDataPartitionAfterLoad()
}

func TestPanicCheckReleaseDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.releaseDataPartitionAfterLoad()
	t.Logf("catched panic")
}

func TestCheckHeartbeat(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
}

func TestCheckMetaPartitions(t *testing.T) {
	server.cluster.checkMetaPartitions()
}

func TestPanicCheckMetaPartitions(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateMetaPartitionID()
	if err != nil {
		t.Error(err)
	}
	mp := newMetaPartition(partitionID, 1, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name, vol.ID, 0)
	vol.addMetaPartition(mp)
	c.checkMetaPartitions()
	t.Logf("catched panic")
}

func TestCheckAvailSpace(t *testing.T) {
	server.cluster.scheduleToUpdateStatInfo()
}

func TestPanicCheckAvailSpace(t *testing.T) {
	c := buildPanicCluster()
	c.dataNodeStatInfo = nil
	c.scheduleToUpdateStatInfo()
}

func TestCheckCreateDataPartitions(t *testing.T) {
	server.cluster.scheduleToManageDp()
	// time.Sleep(150 * time.Second)
}

func TestPanicCheckCreateDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.scheduleToManageDp()
}

func TestPanicCheckBadDiskRecovery(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateDataPartitionID()
	if err != nil {
		t.Error(err)
	}
	dp := newDataPartition(partitionID, vol.dpReplicaNum, vol.Name, vol.ID,
		proto.PartitionTypeNormal, defaultMediaType, defaultPoolId)
	c.BadDataPartitionIds.Store(fmt.Sprintf("%v", dp.PartitionID), dp)
	c.scheduleToCheckDiskRecoveryProgress()
}

func TestCheckBadDiskRecovery(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	// clear
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		server.cluster.BadDataPartitionIds.Delete(key)
		return true
	})
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	vol.volLock.RLock()
	dps := make([]*DataPartition, 0)
	dps = append(dps, vol.dataPartitions.partitions...)
	dpsMapLen := len(vol.dataPartitions.partitionMap)
	vol.volLock.RUnlock()
	dpsLen := len(dps)
	if dpsLen != dpsMapLen {
		t.Errorf("dpsLen[%v],dpsMapLen[%v]", dpsLen, dpsMapLen)
		return
	}
	for _, dp := range dps {
		dp.RLock()
		if len(dp.Replicas) == 0 {
			dp.RUnlock()
			return
		}
		addr := dp.Replicas[0].dataNode.Addr
		server.cluster.putBadDataPartitionIDs(dp.Replicas[0], addr, dp.PartitionID)
		dp.RUnlock()
	}
	count := 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		count = count + len(badDataPartitionIds)
		return true
	})

	if count != dpsLen {
		t.Errorf("expect bad partition num[%v],real num[%v]", dpsLen, count)
		return
	}
	// check recovery
	server.cluster.checkDiskRecoveryProgress()

	count = 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expect bad partition num[0],real num[%v]", count)
		return
	}
}

func TestPanicCheckBadMetaPartitionRecovery(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateMetaPartitionID()
	if err != nil {
		t.Error(err)
	}
	dp := newMetaPartition(partitionID, 0, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name, vol.ID, 0)
	c.BadMetaPartitionIds.Store(fmt.Sprintf("%v", dp.PartitionID), dp)
	c.scheduleToCheckMetaPartitionRecoveryProgress()
}

func TestCheckBadMetaPartitionRecovery(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	// clear
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		server.cluster.BadMetaPartitionIds.Delete(key)
		return true
	})
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	vol.volLock.RLock()
	mps := make([]*MetaPartition, 0)
	for _, mp := range vol.MetaPartitions {
		mps = append(mps, mp)
	}
	mpsMapLen := len(vol.MetaPartitions)
	vol.volLock.RUnlock()
	mpsLen := len(mps)
	if mpsLen != mpsMapLen {
		t.Errorf("mpsLen[%v],mpsMapLen[%v]", mpsLen, mpsMapLen)
		return
	}
	for _, mp := range mps {
		mp.RLock()
		if len(mp.Replicas) == 0 {
			mp.RUnlock()
			return
		}
		addr := mp.Replicas[0].metaNode.Addr
		server.cluster.putBadMetaPartitions(addr, mp.PartitionID)
		mp.RUnlock()
	}
	count := 0
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		count = count + len(badMetaPartitionIds)
		return true
	})

	if count != mpsLen {
		t.Errorf("expect bad partition num[%v],real num[%v]", mpsLen, count)
		return
	}
	// check recovery
	server.cluster.checkMetaPartitionRecoveryProgress()

	count = 0
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expect bad partition num[0],real num[%v]", count)
		return
	}
}

func TestUpdateInodeIDUpperBound(t *testing.T) {
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID := vol.maxMetaPartitionID()
	vol.volLock.RLock()
	mp := vol.MetaPartitions[maxPartitionID]
	mpLen := len(vol.MetaPartitions)
	vol.volLock.RUnlock()
	mr := &proto.MetaPartitionReport{
		PartitionID: mp.PartitionID,
		Start:       mp.Start,
		End:         mp.End,
		Status:      int(mp.Status),
		MaxInodeID:  mp.Start + 1,
		IsLeader:    false,
		VolName:     mp.volName,
	}
	metaNode, err := server.cluster.metaNode(mp.Hosts[0])
	if err != nil {
		t.Error(err)
		return
	}
	if err = server.cluster.updateInodeIDUpperBound(mp, mr, true, metaNode); err != nil {
		t.Error(err)
		return
	}
	curMpLen := len(vol.MetaPartitions)
	if curMpLen == mpLen {
		t.Errorf("split failed,oldMpLen[%v],curMpLen[%v]", mpLen, curMpLen)
	}
}

func TestBalanceMetaPartition(t *testing.T) {
	// create volume and metaNode will create mp,sleep some time to wait cluster get latest meteNode info
	// cluster normal volume has 3 mps , total 3*3 =9 mp in metaNode
	req := &createVolReq{
		name:             commonVolName + "1",
		owner:            "cfs",
		dpSize:           3,
		mpCount:          30,
		dpReplicaNum:     3,
		capacity:         100,
		followerRead:     false,
		authenticate:     false,
		crossZone:        true,
		normalZonesFirst: false,
		zoneName:         testZone1 + "," + testZone2,
		description:      "",
		qosLimitArgs:     &qosArgs{},
		defaultPoolId:    defaultPoolId,
		allowedPools:     []uint8{defaultPoolId},
	}
	_, err := server.cluster.createVol(req)
	require.NoError(t, err)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(time.Second * 2)

	zoneM := make(map[string]struct{})
	nodeSetM := make(map[uint64]struct{})
	// get all metaNodes
	sortNodes := server.cluster.getSortLeaderMetaNodes(zoneM, nodeSetM)
	require.Equal(t, len(sortNodes.nodes), server.cluster.metaNodeCount())

	// get noeExist zone metaNodes, should has 0 node
	zoneM["noeExist"] = struct{}{}
	sortNodes = server.cluster.getSortLeaderMetaNodes(zoneM, nodeSetM)
	// if there are no nodes selected, sortNodes is nil
	if sortNodes != nil {
		require.Equal(t, len(sortNodes.nodes), 0)
	}

	// get testZone2 metaNodes, should has 4 node
	zoneM[testZone2] = struct{}{}
	sortNodes = server.cluster.getSortLeaderMetaNodes(zoneM, nodeSetM)
	require.Equal(t, len(sortNodes.nodes), 4)
	// get testZone1 metaNodes, should has 2 node
	delete(zoneM, testZone2)
	zoneM[testZone1] = struct{}{}
	sortNodes = server.cluster.getSortLeaderMetaNodes(zoneM, nodeSetM)
	require.Equal(t, len(sortNodes.nodes), 2)

	// zoneM has testZone1 and testZone2, should has all 6 node
	zoneM[testZone2] = struct{}{}
	sortNodes = server.cluster.getSortLeaderMetaNodes(zoneM, nodeSetM)
	require.Equal(t, len(sortNodes.nodes), 6)

	sortNodes.balanceLeader()
}

func TestMasterClientLeaderChange(t *testing.T) {
	server := &Server{
		leaderInfo: &LeaderInfo{
			addr: "",
		},
		user: &User{},
	}

	cluster := &Cluster{
		masterClient: masterSDK.NewMasterClient(nil, false),
		leaderInfo:   server.leaderInfo,
		ClusterFlashTopoSubItem: ClusterFlashTopoSubItem{
			flashNodeTopo:            new(sync.Map),
			delayDeleteFlashTopoInfo: make(map[string]*DelayDeleteFlashTopoInfo),
		},
	}
	topo := flashgroupmanager.NewFlashNodeTopology(proto.DefaultTopoName, proto.DefaultRegion, uint64(0), proto.TopoStatusNormal)
	topo.SyncFlashGroupFunc = cluster.syncUpdateFlashGroup
	cluster.flashNodeTopo.Store(proto.DefaultTopoName, topo)
	server.cluster = cluster

	cluster.t = newTopology()
	cluster.BadDataPartitionIds = new(sync.Map)

	// NOTE: avoid conflict
	AddrDatabase[5] = "192.168.0.11:17010"
	AddrDatabase[6] = "192.168.0.12:17010"
	server.handleLeaderChange(5)
	server.handleLeaderChange(6)
	require.True(t, cluster.leaderInfo.addr == AddrDatabase[6])
}

func TestCreateVolWithDpCount(t *testing.T) {
	// create volume and metaNode will create mp,sleep some time to wait cluster get latest meteNode info
	// cluster normal volume has 3 mps , total 3*3 =9 mp in metaNode

	t.Run("dpCount >= defaultInitDpCntForVolCreateCheck", func(t *testing.T) {
		req := &createVolReq{
			name:                    commonVolName + "001",
			owner:                   "cfs",
			dpSize:                  11,
			mpCount:                 30,
			dpCount:                 30,
			dpReplicaNum:            3,
			capacity:                100,
			followerRead:            false,
			authenticate:            false,
			crossZone:               true,
			normalZonesFirst:        false,
			zoneName:                testZone1 + "," + testZone2,
			description:             "",
			qosLimitArgs:            &qosArgs{},
			volStorageClass:         defaultVolStorageClass,
			defaultPoolId:           defaultPoolId,
			storeMode:               proto.StoreModeMem,
			accessTimeValidInterval: proto.MinAccessTimeValidInterval,
			remoteCacheReadTimeout:  proto.ReadDeadlineTime,
			allowedPools:            []uint8{defaultPoolId},
		}

		// auto set allowedStorageClass[] in createVolReq
		err := server.checkCreateVolReq(req)
		require.NoError(t, err)

		_, err = server.cluster.createVol(req)
		require.NoError(t, err)

		vol, err := server.cluster.getVol(req.name)
		require.NoError(t, err)

		dpCount := len(vol.dataPartitions.partitions)
		t.Logf("%v", dpCount)
		require.GreaterOrEqual(t, dpCount, defaultInitDataPartitionCnt)
	})

	t.Run("dpCount > max count", func(t *testing.T) {
		req := &createVolReq{
			name:                    commonVolName + "002",
			owner:                   "cfs",
			dpSize:                  3,
			mpCount:                 30,
			dpCount:                 300,
			dpReplicaNum:            3,
			capacity:                100,
			followerRead:            false,
			authenticate:            false,
			crossZone:               true,
			normalZonesFirst:        false,
			zoneName:                testZone1 + "," + testZone2,
			description:             "",
			qosLimitArgs:            &qosArgs{},
			volStorageClass:         defaultVolStorageClass,
			defaultPoolId:           defaultPoolId,
			storeMode:               proto.StoreModeMem,
			accessTimeValidInterval: proto.MinAccessTimeValidInterval,
			remoteCacheReadTimeout:  proto.ReadDeadlineTime,
			allowedPools:            []uint8{defaultPoolId},
		}

		err := server.checkCreateVolReq(req)
		require.Error(t, err)
	})
}

func TestStartCleanEmptyMetaPartition(t *testing.T) {
	err := server.cluster.StartCleanEmptyMetaPartition(commonVolName)
	require.NoError(t, err)
}

func TestDoCleanEmptyMetaPartition(t *testing.T) {
	err := server.cluster.DoCleanEmptyMetaPartition(commonVolName)
	require.NoError(t, err)
}

// Add the following test cases to cluster_test.go file

func TestAddMetaNode(t *testing.T) {
	t.Run("successfully add new meta node", func(t *testing.T) {
		// Prepare test data
		nodeAddr := "127.0.0.1:9501"
		heartbeatPort := "9502"
		replicaPort := "9503"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0) // Let system auto-allocate

		// Ensure node doesn't exist
		server.cluster.metaNodes.Delete(nodeAddr)

		// Call addMetaNode
		id, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, nodesetId, proto.DefaultRegion)

		// Verify results
		require.NoError(t, err)
		require.Greater(t, id, uint64(0))

		// Verify node is added to cache
		value, ok := server.cluster.metaNodes.Load(nodeAddr)
		require.True(t, ok)
		metaNode := value.(*MetaNode)
		require.Equal(t, nodeAddr, metaNode.Addr)
		require.Equal(t, heartbeatPort, metaNode.HeartbeatPort)
		require.Equal(t, replicaPort, metaNode.ReplicaPort)
		require.Equal(t, zoneName, metaNode.ZoneName)
		require.Equal(t, rack, metaNode.Rack)
		require.Equal(t, id, metaNode.ID)
		require.Greater(t, metaNode.NodeSetID, uint64(0))
	})

	t.Run("add meta node with default values", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9504"
		heartbeatPort := "9505"
		replicaPort := "9506"
		zoneName := "" // Empty zoneName, should use default value
		rack := ""     // Empty rack, should use default value
		nodesetId := uint64(0)

		// Ensure node doesn't exist
		server.cluster.metaNodes.Delete(nodeAddr)

		id, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, nodesetId, proto.DefaultRegion)

		require.NoError(t, err)
		require.Greater(t, id, uint64(0))

		// Verify default values
		value, ok := server.cluster.metaNodes.Load(nodeAddr)
		require.True(t, ok)
		metaNode := value.(*MetaNode)
		require.Equal(t, DefaultZoneName, metaNode.ZoneName)
		require.Equal(t, proto.DefaultRack, metaNode.Rack)
	})

	t.Run("add existing meta node with same parameters", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9507"
		heartbeatPort := "9508"
		replicaPort := "9509"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)

		// Add node first
		server.cluster.metaNodes.Delete(nodeAddr)
		firstId, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, nodesetId, "")
		require.NoError(t, err)

		// Add same node with same parameters again
		secondId, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, nodesetId, "")

		// Should return same ID, no error
		require.NoError(t, err)
		require.Equal(t, firstId, secondId)
	})

	t.Run("add existing meta node with different nodeset", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9510"
		heartbeatPort := "9511"
		replicaPort := "9512"
		zoneName := "test-zone"
		rack := "test-rack"
		firstNodesetId := uint64(0)

		// Add node first
		server.cluster.metaNodes.Delete(nodeAddr)
		_, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, firstNodesetId, proto.DefaultRegion)
		require.NoError(t, err)

		// Get actual allocated nodesetId
		value, _ := server.cluster.metaNodes.Load(nodeAddr)
		metaNode := value.(*MetaNode)
		actualNodesetId := metaNode.NodeSetID

		// Try to add with different nodesetId
		differentNodesetId := actualNodesetId + 1
		_, err = server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, differentNodesetId, proto.DefaultRegion)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "addr already in nodeset")
	})

	t.Run("add existing meta node with different zone", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9513"
		heartbeatPort := "9514"
		replicaPort := "9515"
		firstZoneName := "test-zone-1"
		secondZoneName := "test-zone-2"
		rack := "test-rack"
		nodesetId := uint64(0)

		// Add node first
		server.cluster.metaNodes.Delete(nodeAddr)
		_, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, firstZoneName, rack, nodesetId, proto.DefaultRegion)
		require.NoError(t, err)

		// Try to add with different zone
		_, err = server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, secondZoneName, rack, nodesetId, proto.DefaultRegion)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "zoneName not equal to old")
	})

	t.Run("add existing meta node with different rack", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9516"
		heartbeatPort := "9517"
		replicaPort := "9518"
		zoneName := "test-zone"
		firstRack := "test-rack-1"
		secondRack := "test-rack-2"
		nodesetId := uint64(0)

		// Add node first
		server.cluster.metaNodes.Delete(nodeAddr)
		_, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, firstRack, nodesetId, proto.DefaultRegion)
		require.NoError(t, err)

		// Try to add with different rack (non-default rack)
		_, err = server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, secondRack, nodesetId, proto.DefaultRegion)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "rack not equal to old")
	})

	t.Run("add existing meta node with different rack from default", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9519"
		heartbeatPort := "9520"
		replicaPort := "9521"
		zoneName := "test-zone"
		defaultRack := proto.DefaultRack
		newRack := "test-rack"
		nodesetId := uint64(0)

		// Add node first (using default rack)
		server.cluster.metaNodes.Delete(nodeAddr)
		_, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, defaultRack, nodesetId, proto.DefaultRegion)
		require.NoError(t, err)

		// Try to add with different rack (from default rack to new rack)
		_, err = server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, newRack, nodesetId, proto.DefaultRegion)

		// Should succeed (allow update from default rack)
		require.NoError(t, err)

		// Verify rack is updated
		value, ok := server.cluster.metaNodes.Load(nodeAddr)
		require.True(t, ok)
		metaNode := value.(*MetaNode)
		require.Equal(t, newRack, metaNode.Rack)
	})

	t.Run("add existing meta node with port update", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9522"
		firstHeartbeatPort := ""
		firstReplicaPort := ""
		newHeartbeatPort := "9523"
		newReplicaPort := "9524"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)

		// Add node first (without port info)
		server.cluster.metaNodes.Delete(nodeAddr)
		_, err := server.cluster.addMetaNode(nodeAddr, firstHeartbeatPort, firstReplicaPort, zoneName, rack, nodesetId, proto.DefaultRegion)
		require.NoError(t, err)

		// Update port info
		_, err = server.cluster.addMetaNode(nodeAddr, newHeartbeatPort, newReplicaPort, zoneName, rack, nodesetId, proto.DefaultRegion)

		// Should succeed
		require.NoError(t, err)

		// Verify ports are updated
		value, ok := server.cluster.metaNodes.Load(nodeAddr)
		require.True(t, ok)
		metaNode := value.(*MetaNode)
		require.Equal(t, newHeartbeatPort, metaNode.HeartbeatPort)
		require.Equal(t, newReplicaPort, metaNode.ReplicaPort)
	})

	t.Run("add meta node with specific nodeset", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9525"
		heartbeatPort := "9526"
		replicaPort := "9527"
		zoneName := "test-zone"
		rack := "test-rack"
		specificNodesetId := uint64(999) // Use a specific nodesetId

		// Ensure node doesn't exist
		server.cluster.metaNodes.Delete(nodeAddr)

		// Note: This test might fail because the specified nodeset may not exist
		// This depends on the test environment setup
		_, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, specificNodesetId, "")

		// If nodeset doesn't exist, should return error
		if err != nil {
			require.Contains(t, err.Error(), "nodeset")
		} else {
			// If successful, verify nodesetId
			value, ok := server.cluster.metaNodes.Load(nodeAddr)
			require.True(t, ok)
			metaNode := value.(*MetaNode)
			require.Equal(t, specificNodesetId, metaNode.NodeSetID)
		}
	})

	t.Run("add meta node with raft partition port requirement", func(t *testing.T) {
		// Save original configuration
		originalConfig := server.cluster.cfg.raftPartitionCanUseDifferentPort.Load()
		defer func() {
			server.cluster.cfg.raftPartitionCanUseDifferentPort.Store(originalConfig)
		}()

		// Enable raft partition port requirement
		server.cluster.cfg.raftPartitionCanUseDifferentPort.Store(true)

		nodeAddr := "127.0.0.1:9528"
		emptyHeartbeatPort := ""
		emptyReplicaPort := ""
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)

		// Ensure node doesn't exist
		server.cluster.metaNodes.Delete(nodeAddr)

		// Try to add node without ports
		_, err := server.cluster.addMetaNode(nodeAddr, emptyHeartbeatPort, emptyReplicaPort, zoneName, rack, nodesetId, "")

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "heartbeatPort and replicaPort")
		require.Contains(t, err.Error(), "raftPartitionCanUseDifferentPort")
	})

	t.Run("concurrent add meta node", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9529"
		heartbeatPort := "9530"
		replicaPort := "9531"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)

		// Ensure node doesn't exist
		server.cluster.metaNodes.Delete(nodeAddr)

		// Concurrently add same node
		var wg sync.WaitGroup
		results := make([]struct {
			id  uint64
			err error
		}, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				id, err := server.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, rack, nodesetId, "")
				results[index] = struct {
					id  uint64
					err error
				}{id, err}
			}(i)
		}

		wg.Wait()

		// Verify all calls succeed and return same ID
		firstId := results[0].id
		require.NoError(t, results[0].err)
		require.Greater(t, firstId, uint64(0))

		for i := 1; i < 10; i++ {
			require.NoError(t, results[i].err)
			require.Equal(t, firstId, results[i].id)
		}
	})
}

// Add the following test cases to cluster_test.go file

func TestAddDataNode(t *testing.T) {
	t.Run("successfully add new data node", func(t *testing.T) {
		// Prepare test data
		nodeAddr := "127.0.0.1:9601"
		raftHeartbeatPort := "9602"
		raftReplicaPort := "9603"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0) // Let system auto-allocate
		mediaType := proto.MediaType_SSD

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		// Call addDataNode
		id, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, mediaType, 0)

		// Verify results
		require.NoError(t, err)
		require.Greater(t, id, uint64(0))

		// Verify node is added to cache
		value, ok := server.cluster.dataNodes.Load(nodeAddr)
		require.True(t, ok)
		dataNode := value.(*DataNode)
		require.Equal(t, nodeAddr, dataNode.Addr)
		require.Equal(t, raftHeartbeatPort, dataNode.HeartbeatPort)
		require.Equal(t, raftReplicaPort, dataNode.ReplicaPort)
		require.Equal(t, zoneName, dataNode.ZoneName)
		require.Equal(t, rack, dataNode.Rack)
		require.Equal(t, id, dataNode.ID)
		require.Equal(t, mediaType, dataNode.MediaType)
		require.Greater(t, dataNode.NodeSetID, uint64(0))
	})

	t.Run("add data node with default values", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9604"
		raftHeartbeatPort := "9605"
		raftReplicaPort := "9606"
		zoneName := "" // Empty zoneName, should use default value
		rack := ""     // Empty rack, should use default value
		nodesetId := uint64(0)
		mediaType := proto.MediaType_HDD

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		id, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, mediaType, 0)

		require.NoError(t, err)
		require.Greater(t, id, uint64(0))

		// Verify default values
		value, ok := server.cluster.dataNodes.Load(nodeAddr)
		require.True(t, ok)
		dataNode := value.(*DataNode)
		require.Equal(t, DefaultZoneName, dataNode.ZoneName)
		require.Equal(t, proto.DefaultRack, dataNode.Rack)
		require.Equal(t, mediaType, dataNode.MediaType)
	})

	t.Run("add data node with invalid media type using legacy", func(t *testing.T) {
		// Save original legacy media type
		originalLegacyType := server.cluster.legacyDataMediaType
		defer func() {
			server.cluster.legacyDataMediaType = originalLegacyType
		}()

		// Set legacy media type
		server.cluster.legacyDataMediaType = proto.MediaType_SSD

		nodeAddr := "127.0.0.1:9607"
		raftHeartbeatPort := "9608"
		raftReplicaPort := "9609"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		invalidMediaType := uint32(999) // Invalid media type

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		id, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, invalidMediaType, 0)

		// Should succeed using legacy media type
		require.NoError(t, err)
		require.Greater(t, id, uint64(0))

		// Verify legacy media type is used
		value, ok := server.cluster.dataNodes.Load(nodeAddr)
		require.True(t, ok)
		dataNode := value.(*DataNode)
		require.Equal(t, proto.MediaType_SSD, dataNode.MediaType)
	})

	t.Run("add data node with invalid media type and no legacy", func(t *testing.T) {
		// Save original legacy media type
		originalLegacyType := server.cluster.legacyDataMediaType
		defer func() {
			server.cluster.legacyDataMediaType = originalLegacyType
		}()

		// Clear legacy media type
		server.cluster.legacyDataMediaType = 0

		nodeAddr := "127.0.0.1:9610"
		raftHeartbeatPort := "9611"
		raftReplicaPort := "9612"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		invalidMediaType := uint32(999) // Invalid media type

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, invalidMediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid mediaType")
		require.Contains(t, err.Error(), "LegacyDataMediaType not set")
	})

	t.Run("add existing data node with same parameters", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9613"
		raftHeartbeatPort := "9614"
		raftReplicaPort := "9615"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first
		server.cluster.dataNodes.Delete(nodeAddr)
		firstId, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, mediaType, 0)
		require.NoError(t, err)

		// Add same node with same parameters again
		secondId, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, mediaType, 0)

		// Should return same ID, no error
		require.NoError(t, err)
		require.Equal(t, firstId, secondId)
	})

	t.Run("add existing data node with different nodeset", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9616"
		raftHeartbeatPort := "9617"
		raftReplicaPort := "9618"
		zoneName := "test-zone"
		rack := "test-rack"
		firstNodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, firstNodesetId, mediaType, 0)
		require.NoError(t, err)

		// Get actual allocated nodesetId
		value, _ := server.cluster.dataNodes.Load(nodeAddr)
		dataNode := value.(*DataNode)
		actualNodesetId := dataNode.NodeSetID

		// Try to add with different nodesetId
		differentNodesetId := actualNodesetId + 1
		_, err = server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, differentNodesetId, mediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "addr already in nodeset")
	})

	t.Run("add existing data node with different zone", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9619"
		raftHeartbeatPort := "9620"
		raftReplicaPort := "9621"
		firstZoneName := "test-zone-1"
		secondZoneName := "test-zone-2"
		rack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, firstZoneName, rack, nodesetId, mediaType, 0)
		require.NoError(t, err)

		// Try to add with different zone
		_, err = server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, secondZoneName, rack, nodesetId, mediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "zoneName not equal old")
	})

	t.Run("add existing data node with different media type", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9622"
		raftHeartbeatPort := "9623"
		raftReplicaPort := "9624"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		firstMediaType := proto.MediaType_SSD
		secondMediaType := proto.MediaType_HDD

		// Add node first
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, firstMediaType, 0)
		require.NoError(t, err)

		// Try to add with different media type
		_, err = server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, secondMediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "mediaType not equal old")
	})

	t.Run("add existing data node with different rack", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9625"
		raftHeartbeatPort := "9626"
		raftReplicaPort := "9627"
		zoneName := "test-zone"
		firstRack := "test-rack-1"
		secondRack := "test-rack-2"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, firstRack, nodesetId, mediaType, 0)
		require.NoError(t, err)

		// Try to add with different rack (non-default rack)
		_, err = server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, secondRack, nodesetId, mediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "rack not equal to old")
	})

	t.Run("add existing data node with different rack from default", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9628"
		raftHeartbeatPort := "9629"
		raftReplicaPort := "9630"
		zoneName := "test-zone"
		defaultRack := proto.DefaultRack
		newRack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first (using default rack)
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, defaultRack, nodesetId, mediaType, 0)
		require.NoError(t, err)

		// Try to add with different rack (from default rack to new rack)
		_, err = server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, newRack, nodesetId, mediaType, 0)

		// Should succeed (allow update from default rack)
		require.NoError(t, err)

		// Verify rack is updated
		value, ok := server.cluster.dataNodes.Load(nodeAddr)
		require.True(t, ok)
		dataNode := value.(*DataNode)
		require.Equal(t, newRack, dataNode.Rack)
	})

	t.Run("add existing data node with port update", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9631"
		firstHeartbeatPort := ""
		firstReplicaPort := ""
		newHeartbeatPort := "9632"
		newReplicaPort := "9633"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Add node first (without port info)
		server.cluster.dataNodes.Delete(nodeAddr)
		_, err := server.cluster.addDataNode(nodeAddr, firstHeartbeatPort, firstReplicaPort, zoneName, rack, nodesetId, mediaType, 0)
		require.NoError(t, err)

		// Update port info
		_, err = server.cluster.addDataNode(nodeAddr, newHeartbeatPort, newReplicaPort, zoneName, rack, nodesetId, mediaType, 0)

		// Should succeed
		require.NoError(t, err)

		// Verify ports are updated
		value, ok := server.cluster.dataNodes.Load(nodeAddr)
		require.True(t, ok)
		dataNode := value.(*DataNode)
		require.Equal(t, newHeartbeatPort, dataNode.HeartbeatPort)
		require.Equal(t, newReplicaPort, dataNode.ReplicaPort)
	})

	t.Run("add data node with specific nodeset", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9634"
		raftHeartbeatPort := "9635"
		raftReplicaPort := "9636"
		zoneName := "test-zone"
		rack := "test-rack"
		specificNodesetId := uint64(999) // Use a specific nodesetId
		mediaType := proto.MediaType_SSD

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		// Note: This test might fail because the specified nodeset may not exist
		// This depends on the test environment setup
		_, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, specificNodesetId, mediaType, 0)

		// If nodeset doesn't exist, should return error
		if err != nil {
			require.Contains(t, err.Error(), "nodeset")
		} else {
			// If successful, verify nodesetId
			value, ok := server.cluster.dataNodes.Load(nodeAddr)
			require.True(t, ok)
			dataNode := value.(*DataNode)
			require.Equal(t, specificNodesetId, dataNode.NodeSetID)
		}
	})

	t.Run("add data node with raft partition port requirement", func(t *testing.T) {
		// Save original configuration
		originalConfig := server.cluster.cfg.raftPartitionCanUseDifferentPort.Load()
		defer func() {
			server.cluster.cfg.raftPartitionCanUseDifferentPort.Store(originalConfig)
		}()

		// Enable raft partition port requirement
		server.cluster.cfg.raftPartitionCanUseDifferentPort.Store(true)

		nodeAddr := "127.0.0.1:9637"
		emptyHeartbeatPort := ""
		emptyReplicaPort := ""
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		// Try to add node without ports
		_, err := server.cluster.addDataNode(nodeAddr, emptyHeartbeatPort, emptyReplicaPort, zoneName, rack, nodesetId, mediaType, 0)

		// Should return error
		require.Error(t, err)
		require.Contains(t, err.Error(), "heartbeatPort and replicaPort")
		require.Contains(t, err.Error(), "raftPartitionCanUseDifferentPort")
	})

	t.Run("concurrent add data node", func(t *testing.T) {
		nodeAddr := "127.0.0.1:9642"
		raftHeartbeatPort := "9643"
		raftReplicaPort := "9644"
		zoneName := "test-zone"
		rack := "test-rack"
		nodesetId := uint64(0)
		mediaType := proto.MediaType_SSD

		// Ensure node doesn't exist
		server.cluster.dataNodes.Delete(nodeAddr)

		// Concurrently add same node
		var wg sync.WaitGroup
		results := make([]struct {
			id  uint64
			err error
		}, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				id, err := server.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack, nodesetId, mediaType, 0)
				results[index] = struct {
					id  uint64
					err error
				}{id, err}
			}(i)
		}

		wg.Wait()

		// Verify all calls succeed and return same ID
		firstId := results[0].id
		require.NoError(t, results[0].err)
		require.Greater(t, firstId, uint64(0))

		for i := 1; i < 10; i++ {
			require.NoError(t, results[i].err)
			require.Equal(t, firstId, results[i].id)
		}
	})
}

func TestPreReservedSpaceFunctions(t *testing.T) {
	// Use existing data node from test environment
	var testDataNode *DataNode
	var testNodeAddr string

	// Find an existing active data node
	server.cluster.dataNodes.Range(func(addr, value interface{}) bool {
		dn := value.(*DataNode)
		if dn.isActive {
			testDataNode = dn
			testNodeAddr = addr.(string)
			return false // stop iteration
		}
		return true
	})

	if testDataNode == nil {
		t.Skip("No active data node found in test environment")
		return
	}

	// Create test data partition
	vol, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)

	partitionID, err := server.cluster.idAlloc.allocateDataPartitionID()
	require.NoError(t, err)

	dp := newDataPartition(partitionID, vol.dpReplicaNum, vol.Name, vol.ID,
		proto.PartitionTypeNormal, defaultMediaType, defaultPoolId)

	// Add test replica to data partition
	replica := newDataReplica(testDataNode)
	replica.Used = 1024 * 1024 * 1024 // 1GB
	dp.addReplica(replica)

	t.Run("addDataReservedResource", func(t *testing.T) {
		// Reset simulate reserved space
		testDataNode.PreReservedSpace = 0
		testDataNode.PreReservedDpCount = 0

		initialReservedSpace := testDataNode.PreReservedSpace
		initialReservedCount := testDataNode.PreReservedDpCount

		// Add reserved resource
		err := server.cluster.addDataReservedResource([]string{testNodeAddr}, dp)
		require.NoError(t, err)

		// Verify the reserved space is increased
		require.Equal(t, initialReservedSpace+replica.Used, testDataNode.PreReservedSpace)
		require.Equal(t, initialReservedCount+1, testDataNode.PreReservedDpCount)
	})

	t.Run("releaseDataReservedResource", func(t *testing.T) {
		// Set initial reserved space
		testDataNode.PreReservedSpace = 2 * 1024 * 1024 * 1024 // 2GB
		testDataNode.PreReservedDpCount = 2

		initialReservedSpace := testDataNode.PreReservedSpace
		initialReservedCount := testDataNode.PreReservedDpCount

		// Release reserved resource
		server.cluster.releaseDataReservedResource([]string{testNodeAddr}, dp)

		// Verify the reserved space is decreased
		expectedSpace := initialReservedSpace - replica.Used
		require.Equal(t, expectedSpace, testDataNode.PreReservedSpace)
		require.Equal(t, initialReservedCount-1, testDataNode.PreReservedDpCount)
	})

	t.Run("releaseDataReservedResource_underflow", func(t *testing.T) {
		// Set reserved space smaller than release amount
		testDataNode.PreReservedSpace = 512 * 1024 * 1024 // 512MB (smaller than replica.Used)
		testDataNode.PreReservedDpCount = 1

		// Release reserved resource
		server.cluster.releaseDataReservedResource([]string{testNodeAddr}, dp)

		// Verify the reserved space is set to 0 when underflow
		require.Equal(t, uint64(0), testDataNode.PreReservedSpace)
		require.Equal(t, uint32(0), testDataNode.PreReservedDpCount)
	})

	t.Run("getDataPartitionMaxUsedSize", func(t *testing.T) {
		// Create data partition with multiple replicas
		testDP := newDataPartition(partitionID+1, 3, vol.Name, vol.ID,
			proto.PartitionTypeNormal, defaultMediaType, defaultPoolId)

		// Add replicas with different used sizes
		replica1 := &DataReplica{DataReplica: proto.DataReplica{Used: 500 * 1024 * 1024}} // 500MB
		replica2 := &DataReplica{DataReplica: proto.DataReplica{Used: 800 * 1024 * 1024}} // 800MB
		replica3 := &DataReplica{DataReplica: proto.DataReplica{Used: 600 * 1024 * 1024}} // 600MB

		testDP.Replicas = []*DataReplica{replica1, replica2, replica3}

		// Get max used size
		maxUsed := server.cluster.getDataPartitionMaxUsedSize(testDP)

		// Should return the maximum used size (800MB)
		require.Equal(t, uint64(800*1024*1024), maxUsed)
	})

	t.Run("getDataPartitionMaxUsedSize_empty_replicas", func(t *testing.T) {
		// Create data partition with no replicas
		testDP := newDataPartition(partitionID+2, 3, vol.Name, vol.ID,
			proto.PartitionTypeNormal, defaultMediaType, defaultPoolId)

		// Get max used size
		maxUsed := server.cluster.getDataPartitionMaxUsedSize(testDP)

		// Should return 0 for empty replicas
		require.Equal(t, uint64(0), maxUsed)
	})

	t.Run("concurrent_simulate_reserved_space_operations", func(t *testing.T) {
		// Reset simulate reserved space
		testDataNode.PreReservedSpace = 0
		testDataNode.PreReservedDpCount = 0

		var wg sync.WaitGroup
		numOperations := 10 // Reduce number of operations for test stability

		// Concurrently add reserved resources
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := server.cluster.addDataReservedResource([]string{testNodeAddr}, dp)
				require.NoError(t, err)
			}()
		}

		wg.Wait()

		// Verify final state
		expectedSpace := uint64(numOperations) * replica.Used
		expectedCount := uint32(numOperations)
		require.Equal(t, expectedSpace, testDataNode.PreReservedSpace)
		require.Equal(t, expectedCount, testDataNode.PreReservedDpCount)

		// Concurrently release reserved resources
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				server.cluster.releaseDataReservedResource([]string{testNodeAddr}, dp)
			}()
		}

		wg.Wait()

		// Verify final state (should be back to 0)
		require.Equal(t, uint64(0), testDataNode.PreReservedSpace)
		require.Equal(t, uint32(0), testDataNode.PreReservedDpCount)
	})
}

func TestShouldDisableDiskDirectlyForDataNodeDecommission(t *testing.T) {
	t.Run("partial decommission keeps unrelated disks allocatable", func(t *testing.T) {
		dpToDecommissionByDisk := map[string]int{"/data1": 15}

		require.False(t, shouldDisableDiskDirectlyForDataNodeDecommission(20, "/data2", dpToDecommissionByDisk))
		require.False(t, shouldDisableDiskDirectlyForDataNodeDecommission(20, "/data1", dpToDecommissionByDisk))
	})

	t.Run("full decommission disables disks without partitions to migrate", func(t *testing.T) {
		dpToDecommissionByDisk := map[string]int{"/data1": 15}

		require.True(t, shouldDisableDiskDirectlyForDataNodeDecommission(0, "/data2", dpToDecommissionByDisk))
		require.False(t, shouldDisableDiskDirectlyForDataNodeDecommission(0, "/data1", dpToDecommissionByDisk))
	})
}
