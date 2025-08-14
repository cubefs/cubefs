package master

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

func TestDataPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDataPartitions()
	count := 20
	createDataPartition(commonVol, count, t)
	if len(commonVol.dataPartitions.partitions) <= 0 {
		t.Errorf("getDataPartition no dp")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	getDataPartition(partition.PartitionID, t)
	loadDataPartitionTest(partition, t)
	_ = decommissionDataPartition
	// decommissionDataPartition(partition, t)
}

func createDataPartition(vol *Vol, count int, t *testing.T) {
	oldCount := len(vol.dataPartitions.partitions)
	reqURL := fmt.Sprintf("%v%v?count=%v&name=%v&type=extent&force=true",
		hostAddr, proto.AdminCreateDataPartition, count, vol.Name)
	process(reqURL, t)

	newCount := len(vol.dataPartitions.partitions)
	total := oldCount + count
	if newCount != total {
		t.Errorf("createDataPartition failed,newCount[%v],total=%v,count[%v],oldCount[%v]",
			newCount, total, count, oldCount)
		return
	}
}

func getDataPartition(id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?id=%v",
		hostAddr, proto.AdminGetDataPartition, id)
	process(reqURL, t)
}

// test
func decommissionDataPartition(dp *DataPartition, t *testing.T) {
	offlineAddr := dp.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionDataPartition, dp.VolName, dp.PartitionID, offlineAddr)
	process(reqURL, t)
	if contains(dp.Hosts, offlineAddr) {
		t.Errorf("decommissionDataPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, dp.Hosts)
		return
	}
}

func loadDataPartitionTest(dp *DataPartition, t *testing.T) {
	dps := make([]*DataPartition, 0)
	dps = append(dps, dp)
	server.cluster.waitForResponseToLoadDataPartition(dps)
	time.Sleep(5 * time.Second)
	dp.RLock()
	for _, replica := range dp.Replicas {
		t.Logf("replica[%v],response[%v]", replica.Addr, replica.HasLoadResponse)
	}
	tinyFile := &FileInCore{}
	tinyFile.Name = "50000011"
	tinyFile.LastModify = 1562507765
	extentFile := &FileInCore{}
	extentFile.Name = "10"
	extentFile.LastModify = 1562507765
	for index, host := range dp.Hosts {
		fm := newFileMetadata(uint32(404551221)+uint32(index), host, index, 2*util.MB, 0)
		tinyFile.MetadataArray = append(tinyFile.MetadataArray, fm)
		extentFile.MetadataArray = append(extentFile.MetadataArray, fm)
	}

	dp.FileInCoreMap[tinyFile.Name] = tinyFile
	dp.FileInCoreMap[extentFile.Name] = extentFile
	dp.RUnlock()
	dp.getFileCount()
	dp.validateCRC(server.cluster.Name)
	dp.setToNormal()
}

func TestAcquireDecommissionFirstHostToken(t *testing.T) {
	partition := &DataPartition{PartitionID: 1, Hosts: []string{"host0", "host1", "host2"}, ReplicaNum: 3}
	partition.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: "host0", DiskPath: "/disk0"}},
		{DataReplica: proto.DataReplica{Addr: "host1", DiskPath: "/disk1"}},
		{DataReplica: proto.DataReplica{Addr: "host2", DiskPath: "/disk2"}},
	}
	partition.DecommissionSrcAddr = "host2"
	partition.DecommissionType = ManualDecommission

	cluster := &Cluster{
		ClusterDecommission: ClusterDecommission{DecommissionFirstHostDiskParallelLimit: 0},
	}
	dataNode := &DataNode{
		DecommissionFirstHostParallelLimit: 1,
	}
	cluster.dataNodes.Store("host0", dataNode)
	dataNodeInfo := &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster))

	cluster.DecommissionFirstHostDiskParallelLimit = 1
	dataNode.DecommissionFirstHostParallelLimit = 2
	dataNodeInfo = &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {CurParallel: 1, DiskPath: "/disk0"},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster))

	cluster.DecommissionFirstHostDiskParallelLimit = 2
	dataNode.DecommissionFirstHostParallelLimit = 2
	dataNodeInfo = &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {
				CurParallel: 1,
				DiskPath:    "/disk0",
				RepairingDps: map[uint64]struct{}{
					0: {},
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.True(t, partition.AcquireDecommissionFirstHostToken(cluster))
}

func TestReleaseDecommissionFirstHostToken(t *testing.T) {
	partition := &DataPartition{PartitionID: 1, Hosts: []string{"host0", "host1", "host2"}, ReplicaNum: 3}
	partition.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: "host0", DiskPath: "/disk0"}},
		{DataReplica: proto.DataReplica{Addr: "host1", DiskPath: "/disk1"}},
		{DataReplica: proto.DataReplica{Addr: "host2", DiskPath: "/disk2"}},
	}
	partition.DecommissionSrcAddr = "host2"
	partition.DecommissionType = ManualDecommission
	partition.DecommissionFirstHostDiskTokenKey = "host0_/disk0"

	cluster := &Cluster{
		ClusterDecommission: ClusterDecommission{DecommissionFirstHostDiskParallelLimit: 2},
	}
	dataNode := &DataNode{
		DecommissionFirstHostParallelLimit: 2,
	}
	cluster.dataNodes.Store("host0", dataNode)

	dataNodeInfo := &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 2,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {
				CurParallel: 2,
				DiskPath:    "/disk0",
				RepairingDps: map[uint64]struct{}{
					0: {},
					1: {},
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	partition.ReleaseDecommissionFirstHostToken(cluster)

	value, ok := cluster.DataNodeToDecommissionRepairDpMap.Load("host0")
	if !ok {
		t.Errorf("dataNode should not be removed")
	}
	dataNodeInfoAfter := value.(*DataNodeToDecommissionRepairDpInfo)
	diskInfo, ok := dataNodeInfoAfter.DiskToDecommissionRepairDpMap["/disk0"]
	if !ok {
		t.Errorf("disk should not be removed")
	}
	if len(diskInfo.RepairingDps) != 1 {
		t.Errorf("repairingDps should have one dp left %v", diskInfo.RepairingDps)
		return
	}
	if diskInfo.CurParallel != 1 {
		t.Errorf("disk curParallel should be updated to 1 %v", diskInfo.CurParallel)
		return
	}
	if dataNodeInfoAfter.CurParallel != 1 {
		t.Errorf("datanode curParallel should be updated to 1 %v", dataNodeInfoAfter.CurParallel)
		return
	}
}
