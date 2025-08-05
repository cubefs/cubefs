package master

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
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
		proto.PartitionTypeNormal, 0, defaultMediaType)
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
	maxPartitionID := vol.maxPartitionID()
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
	cluster := &Cluster{
		masterClient:  masterSDK.NewMasterClient(nil, false),
		flashNodeTopo: newFlashNodeTopology(),
	}
	cluster.t = newTopology()
	cluster.BadDataPartitionIds = new(sync.Map)
	server := &Server{
		cluster: cluster,
		leaderInfo: &LeaderInfo{
			addr: "",
		},
		user: &User{},
	}
	// NOTE: avoid conflict
	AddrDatabase[5] = "192.168.0.11:17010"
	AddrDatabase[6] = "192.168.0.12:17010"
	server.handleLeaderChange(5)
	server.handleLeaderChange(6)
	masters := cluster.masterClient.GetMasterAddresses()
	require.EqualValues(t, 2, len(masters))
}

func TestCreateVolWithDpCount(t *testing.T) {
	// create volume and metaNode will create mp,sleep some time to wait cluster get latest meteNode info
	// cluster normal volume has 3 mps , total 3*3 =9 mp in metaNode

	t.Run("dpCount != default count", func(t *testing.T) {
		req := &createVolReq{
			name:             commonVolName + "001",
			owner:            "cfs",
			dpSize:           11,
			mpCount:          30,
			dpCount:          30,
			dpReplicaNum:     3,
			capacity:         100,
			followerRead:     false,
			authenticate:     false,
			crossZone:        true,
			normalZonesFirst: false,
			zoneName:         testZone1 + "," + testZone2,
			description:      "",
			qosLimitArgs:     &qosArgs{},
			volStorageClass:  defaultVolStorageClass,
		}

		// auto set allowedStorageClass[] in createVolReq
		err := server.checkCreateVolReq(req)
		require.NoError(t, err)

		_, err = server.cluster.createVol(req)
		require.NoError(t, err)

		vol, err := server.cluster.getVol(req.name)
		require.NoError(t, err)

		dpCount := len(vol.dataPartitions.partitions)
		require.Equal(t, req.dpCount, dpCount)
	})

	t.Run("dpCount > max count", func(t *testing.T) {
		req := &createVolReq{
			name:             commonVolName + "002",
			owner:            "cfs",
			dpSize:           3,
			mpCount:          30,
			dpCount:          300,
			dpReplicaNum:     3,
			capacity:         100,
			followerRead:     false,
			authenticate:     false,
			crossZone:        true,
			normalZonesFirst: false,
			zoneName:         testZone1 + "," + testZone2,
			description:      "",
			qosLimitArgs:     &qosArgs{},
			volStorageClass:  defaultVolStorageClass,
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
