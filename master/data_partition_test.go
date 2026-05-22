package master

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

type recordedDataNodeAdminTask struct {
	opCode uint8
	task   *proto.AdminTask
}

const recordedCreateReplicaDiskPath = "/disk-created-by-master-test"

func startDataNodeAdminTaskRecorder(t *testing.T) (string, func() []recordedDataNodeAdminTask) {
	t.Helper()
	proto.InitBufferPool(int64(32768))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	tasks := make([]recordedDataNodeAdminTask, 0)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()

				packet := proto.NewPacket()
				if err := packet.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
					t.Errorf("read admin task packet: %v", err)
					return
				}

				task := &proto.AdminTask{}
				if err := json.Unmarshal(packet.Data, task); err != nil {
					t.Errorf("unmarshal admin task: %v", err)
					return
				}

				mu.Lock()
				tasks = append(tasks, recordedDataNodeAdminTask{opCode: packet.Opcode, task: task})
				mu.Unlock()

				if packet.Opcode == proto.OpCreateDataPartition {
					packet.PacketOkWithBody([]byte(recordedCreateReplicaDiskPath))
				} else {
					packet.PacketOkReply()
				}
				if err := packet.WriteToConn(conn); err != nil {
					t.Errorf("write admin task response: %v", err)
				}
			}(conn)
		}
	}()

	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})

	return listener.Addr().String(), func() []recordedDataNodeAdminTask {
		mu.Lock()
		defer mu.Unlock()

		copied := make([]recordedDataNodeAdminTask, len(tasks))
		copy(copied, tasks)
		return copied
	}
}

func newRecordedDataNode(id uint64, addr string) *DataNode {
	return &DataNode{
		ID:             id,
		Addr:           addr,
		TaskManager:    newTestManager(addr),
		MediaType:      proto.MediaType_SSD,
		Total:          util.TB,
		AvailableSpace: util.TB,
		isActive:       true,
	}
}

func newClusterWithDataNodesInNodeSet(t *testing.T, zoneName string, nodeSetID uint64, addrs ...string) (*Cluster, *nodeSet) {
	t.Helper()

	cluster := &Cluster{
		cfg: newClusterConfig(),
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		ClusterTopoSubItem: ClusterTopoSubItem{
			t: newTopology(),
		},
		partition: &mockPartition{isLeader: true},
	}
	zone := newZone(zoneName, proto.MediaType_SSD)
	ns := newNodeSet(nil, nodeSetID, 18, zoneName, "")
	require.NoError(t, zone.putNodeSet(ns))
	require.NoError(t, cluster.t.putZone(zone))

	for idx, addr := range addrs {
		node := newRecordedDataNode(uint64(idx+1), addr)
		node.ZoneName = zoneName
		node.NodeSetID = nodeSetID
		cluster.dataNodes.Store(addr, node)
		ns.dataNodes.Store(addr, node)
		zone.dataNodes.Store(addr, node)
	}
	return cluster, ns
}

type failingSubmitPartition struct {
	mockPartition
}

func (m *failingSubmitPartition) Submit([]byte) (interface{}, error) {
	return nil, fmt.Errorf("submit failed")
}

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

func TestAddDataReplicaSendsOfflineTasksToDataNodes(t *testing.T) {
	leaderAddr, leaderTasks := startDataNodeAdminTaskRecorder(t)
	host2Addr, _ := startDataNodeAdminTaskRecorder(t)
	host3Addr, _ := startDataNodeAdminTaskRecorder(t)
	targetAddr, targetTasks := startDataNodeAdminTaskRecorder(t)

	leaderNode := newRecordedDataNode(1, leaderAddr)
	host2Node := newRecordedDataNode(2, host2Addr)
	host3Node := newRecordedDataNode(3, host3Addr)
	targetNode := newRecordedDataNode(4, targetAddr)

	cluster := &Cluster{
		Name: "unit-test-cluster",
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		partition: &mockPartition{isLeader: true},
	}
	cluster.dataNodes.Store(leaderAddr, leaderNode)
	cluster.dataNodes.Store(host2Addr, host2Node)
	cluster.dataNodes.Store(host3Addr, host3Node)
	cluster.dataNodes.Store(targetAddr, targetNode)

	dp := &DataPartition{
		PartitionID:   102,
		VolName:       "unit-test-vol-add",
		VolID:         7,
		ReplicaNum:    3,
		PartitionType: proto.PartitionTypeNormal,
		MediaType:     proto.MediaType_SSD,
		Hosts:         []string{leaderAddr, host2Addr, host3Addr},
		Peers: []proto.Peer{
			{ID: leaderNode.ID, Addr: leaderAddr},
			{ID: host2Node.ID, Addr: host2Addr},
			{ID: host3Node.ID, Addr: host3Addr},
		},
		DecommissionType: ManualDecommission,
	}
	dp.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: leaderAddr, DiskPath: "/disk-leader", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), IsLeader: true, Used: 11, LocalPeers: dp.Peers}, dataNode: leaderNode},
		{DataReplica: proto.DataReplica{Addr: host2Addr, DiskPath: "/disk-host2", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), LocalPeers: dp.Peers}, dataNode: host2Node},
		{DataReplica: proto.DataReplica{Addr: host3Addr, DiskPath: "/disk-host3", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), LocalPeers: dp.Peers}, dataNode: host3Node},
	}

	vol := &Vol{
		ID:                dp.VolID,
		Name:              dp.VolName,
		dataPartitionSize: 120 * util.GB,
		dataPartitions:    newDataPartitionMap(dp.VolName),
	}
	vol.dataPartitions.put(dp)
	cluster.vols[vol.Name] = vol

	err := cluster.addDataReplica(dp, targetAddr, true, false)
	require.NoError(t, err)

	require.Contains(t, dp.Hosts, targetAddr)
	require.Len(t, dp.Peers, 4)
	require.Len(t, dp.Replicas, 4)
	newReplica, err := dp.getReplica(targetAddr)
	require.NoError(t, err)
	require.Equal(t, recordedCreateReplicaDiskPath, newReplica.DiskPath)
	require.EqualValues(t, proto.Unavailable, newReplica.Status)

	capturedLeaderTasks := leaderTasks()
	require.Len(t, capturedLeaderTasks, 1)
	require.Equal(t, proto.OpAddDataPartitionRaftMember, capturedLeaderTasks[0].opCode)
	addReqBytes, err := json.Marshal(capturedLeaderTasks[0].task.Request)
	require.NoError(t, err)
	addReq := &proto.AddDataPartitionRaftMemberRequest{}
	require.NoError(t, json.Unmarshal(addReqBytes, addReq))
	require.Equal(t, dp.PartitionID, addReq.PartitionId)
	require.Equal(t, targetNode.ID, addReq.AddPeer.ID)
	require.Equal(t, targetAddr, addReq.AddPeer.Addr)
	require.True(t, addReq.RepairingStatus)

	capturedTargetTasks := targetTasks()
	require.Len(t, capturedTargetTasks, 1)
	require.Equal(t, proto.OpCreateDataPartition, capturedTargetTasks[0].opCode)
	createReqBytes, err := json.Marshal(capturedTargetTasks[0].task.Request)
	require.NoError(t, err)
	createReq := &proto.CreateDataPartitionRequest{}
	require.NoError(t, json.Unmarshal(createReqBytes, createReq))
	require.Equal(t, dp.PartitionID, createReq.PartitionId)
	require.Equal(t, dp.VolName, createReq.VolumeId)
	require.Equal(t, proto.DecommissionedCreateDataPartition, createReq.CreateType)
	require.Equal(t, int(dp.ReplicaNum), createReq.ReplicaNum)
	require.Equal(t, int(vol.dataPartitionSize), createReq.PartitionSize)
	require.Contains(t, createReq.Hosts, targetAddr)
}

func TestDataPartitionDecommissionRunsMasterToDataNodeOfflineFlow(t *testing.T) {
	srcAddr, srcTasks := startDataNodeAdminTaskRecorder(t)
	leaderAddr, leaderTasks := startDataNodeAdminTaskRecorder(t)
	otherAddr, _ := startDataNodeAdminTaskRecorder(t)
	targetAddr, targetTasks := startDataNodeAdminTaskRecorder(t)

	srcNode := newRecordedDataNode(1, srcAddr)
	leaderNode := newRecordedDataNode(2, leaderAddr)
	otherNode := newRecordedDataNode(3, otherAddr)
	targetNode := newRecordedDataNode(4, targetAddr)

	cluster := &Cluster{
		Name: "unit-test-cluster",
		cfg: &clusterConfig{
			AllowMultipleReplicasOnSameMachine: true,
		},
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		ClusterDecommission: ClusterDecommission{
			BadDataPartitionIds: new(sync.Map),
		},
		partition: &mockPartition{isLeader: true},
	}
	cluster.dataNodes.Store(srcAddr, srcNode)
	cluster.dataNodes.Store(leaderAddr, leaderNode)
	cluster.dataNodes.Store(otherAddr, otherNode)
	cluster.dataNodes.Store(targetAddr, targetNode)

	dp := &DataPartition{
		PartitionID:                103,
		VolName:                    "unit-test-vol-decommission",
		VolID:                      8,
		ReplicaNum:                 3,
		PartitionType:              proto.PartitionTypeNormal,
		MediaType:                  proto.MediaType_SSD,
		Hosts:                      []string{srcAddr, leaderAddr, otherAddr},
		Peers:                      []proto.Peer{{ID: srcNode.ID, Addr: srcAddr}, {ID: leaderNode.ID, Addr: leaderAddr}, {ID: otherNode.ID, Addr: otherAddr}},
		DecommissionStatus:         markDecommission,
		DecommissionSrcAddr:        srcAddr,
		DecommissionDstAddr:        targetAddr,
		DecommissionDstAddrSpecify: true,
		DecommissionSrcDiskPath:    "/disk-src",
		DecommissionType:           ManualDecommission,
		DecommissionRaftForce:      false,
		DecommissionNeedRollback:   false,
		DecommissionRetry:          0,
	}
	dp.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: srcAddr, DiskPath: "/disk-src", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), Used: 11}, dataNode: srcNode},
		{DataReplica: proto.DataReplica{Addr: leaderAddr, DiskPath: "/disk-leader", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), IsLeader: true, Used: 11}, dataNode: leaderNode},
		{DataReplica: proto.DataReplica{Addr: otherAddr, DiskPath: "/disk-other", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), Used: 11}, dataNode: otherNode},
	}

	vol := &Vol{
		ID:                dp.VolID,
		Name:              dp.VolName,
		dpReplicaNum:      dp.ReplicaNum,
		dataPartitionSize: 120 * util.GB,
		dataPartitions:    newDataPartitionMap(dp.VolName),
	}
	vol.dataPartitions.put(dp)
	cluster.vols[vol.Name] = vol

	ok := dp.Decommission(cluster)
	require.True(t, ok)

	require.NotContains(t, dp.Hosts, srcAddr)
	require.Contains(t, dp.Hosts, targetAddr)
	require.Len(t, dp.Peers, 3)
	require.Len(t, dp.Replicas, 3)
	require.True(t, dp.isRecover)
	require.EqualValues(t, proto.ReadOnly, dp.Status)
	require.Equal(t, DecommissionRunning, dp.GetDecommissionStatus())
	newReplica, err := dp.getReplica(targetAddr)
	require.NoError(t, err)
	require.Equal(t, recordedCreateReplicaDiskPath, newReplica.DiskPath)
	require.EqualValues(t, proto.Recovering, newReplica.Status)

	capturedLeaderTasks := leaderTasks()
	require.Len(t, capturedLeaderTasks, 2)
	require.Equal(t, proto.OpRemoveDataPartitionRaftMember, capturedLeaderTasks[0].opCode)
	require.Equal(t, proto.OpAddDataPartitionRaftMember, capturedLeaderTasks[1].opCode)

	capturedSrcTasks := srcTasks()
	require.Len(t, capturedSrcTasks, 1)
	require.Equal(t, proto.OpDeleteDataPartition, capturedSrcTasks[0].opCode)

	capturedTargetTasks := targetTasks()
	require.Len(t, capturedTargetTasks, 1)
	require.Equal(t, proto.OpCreateDataPartition, capturedTargetTasks[0].opCode)

	badDps, ok := cluster.BadDataPartitionIds.Load(fmt.Sprintf("%s:%s", srcAddr, "/disk-src"))
	require.True(t, ok)
	require.Contains(t, badDps.([]uint64), dp.PartitionID)
	require.Equal(t, util.TB-uint64(11), targetNode.AvailableSpace)
}

func TestRemoveDataReplicaSendsOfflineTasksToDataNodes(t *testing.T) {
	srcAddr, srcTasks := startDataNodeAdminTaskRecorder(t)
	leaderAddr, leaderTasks := startDataNodeAdminTaskRecorder(t)
	otherAddr, _ := startDataNodeAdminTaskRecorder(t)

	srcNode := newRecordedDataNode(1, srcAddr)
	leaderNode := newRecordedDataNode(2, leaderAddr)
	otherNode := newRecordedDataNode(3, otherAddr)

	cluster := &Cluster{Name: "unit-test-cluster"}
	cluster.dataNodes.Store(srcAddr, srcNode)
	cluster.dataNodes.Store(leaderAddr, leaderNode)
	cluster.dataNodes.Store(otherAddr, otherNode)

	dp := &DataPartition{
		PartitionID:   101,
		VolName:       "unit-test-vol",
		ReplicaNum:    3,
		PartitionType: proto.PartitionTypeNormal,
		MediaType:     proto.MediaType_SSD,
		Hosts:         []string{srcAddr, leaderAddr, otherAddr},
		Peers: []proto.Peer{
			{ID: srcNode.ID, Addr: srcAddr},
			{ID: leaderNode.ID, Addr: leaderAddr},
			{ID: otherNode.ID, Addr: otherAddr},
		},
		DecommissionType: ManualDecommission,
	}
	dp.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: srcAddr, DiskPath: "/disk-src", Status: proto.ReadWrite, ReportTime: time.Now().Unix()}, dataNode: srcNode},
		{DataReplica: proto.DataReplica{Addr: leaderAddr, DiskPath: "/disk-leader", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), IsLeader: true}, dataNode: leaderNode},
		{DataReplica: proto.DataReplica{Addr: otherAddr, DiskPath: "/disk-other", Status: proto.ReadWrite, ReportTime: time.Now().Unix()}, dataNode: otherNode},
	}

	err := cluster.removeDataReplica(dp, srcAddr, false, false)
	require.NoError(t, err)

	require.NotContains(t, dp.Hosts, srcAddr)
	require.Len(t, dp.Peers, 2)
	require.Len(t, dp.Replicas, 2)
	require.EqualValues(t, 0, dp.OfflinePeerID)

	capturedLeaderTasks := leaderTasks()
	require.Len(t, capturedLeaderTasks, 1)
	require.Equal(t, proto.OpRemoveDataPartitionRaftMember, capturedLeaderTasks[0].opCode)
	removeReqBytes, err := json.Marshal(capturedLeaderTasks[0].task.Request)
	require.NoError(t, err)
	removeReq := &proto.RemoveDataPartitionRaftMemberRequest{}
	require.NoError(t, json.Unmarshal(removeReqBytes, removeReq))
	require.Equal(t, dp.PartitionID, removeReq.PartitionId)
	require.Equal(t, srcNode.ID, removeReq.RemovePeer.ID)
	require.Equal(t, srcAddr, removeReq.RemovePeer.Addr)
	require.False(t, removeReq.Force)

	capturedSrcTasks := srcTasks()
	require.Len(t, capturedSrcTasks, 1)
	require.Equal(t, proto.OpDeleteDataPartition, capturedSrcTasks[0].opCode)
	deleteReqBytes, err := json.Marshal(capturedSrcTasks[0].task.Request)
	require.NoError(t, err)
	deleteReq := &proto.DeleteDataPartitionRequest{}
	require.NoError(t, json.Unmarshal(deleteReqBytes, deleteReq))
	require.Equal(t, dp.PartitionID, deleteReq.PartitionId)
	require.Equal(t, ManualDecommission, deleteReq.DecommissionType)
	require.False(t, deleteReq.Force)
}

func TestDataPartitionPopHighestPriorityDecommissionTask(t *testing.T) {
	dp := &DataPartition{}
	low := DecommissionTask{
		DecommissionSrcAddr: "src-low",
		DecommissionWeight:  lowPriorityDecommissionWeight,
	}
	high := DecommissionTask{
		DecommissionSrcAddr: "src-high",
		DecommissionWeight:  highPriorityDecommissionWeight,
	}
	medium := DecommissionTask{
		DecommissionSrcAddr: "src-medium",
		DecommissionWeight:  mediumPriorityDecommissionWeight,
	}

	dp.enqueueDecommissionTask(low)
	dp.enqueueDecommissionTask(high)
	dp.enqueueDecommissionTask(medium)

	task, ok := dp.popHighestPriorityTask()
	require.True(t, ok)
	require.Equal(t, high.DecommissionSrcAddr, task.DecommissionSrcAddr)
	require.Equal(t, high.DecommissionWeight, task.DecommissionWeight)

	task, ok = dp.popHighestPriorityTask()
	require.True(t, ok)
	require.Equal(t, medium.DecommissionSrcAddr, task.DecommissionSrcAddr)
	require.Equal(t, medium.DecommissionWeight, task.DecommissionWeight)

	task, ok = dp.popHighestPriorityTask()
	require.True(t, ok)
	require.Equal(t, low.DecommissionSrcAddr, task.DecommissionSrcAddr)
	require.Equal(t, low.DecommissionWeight, task.DecommissionWeight)

	_, ok = dp.popHighestPriorityTask()
	require.False(t, ok)
}

func TestDataPartitionTraverseQueueSchedulesHighestPriorityNextTask(t *testing.T) {
	const (
		volName    = "vol_decommission_queue"
		srcLow     = "10.0.0.1:17310"
		srcHigh    = "10.0.0.2:17310"
		srcCurrent = "10.0.0.3:17310"
		zoneName   = "zone-queue"
		nodeSetID  = uint64(1)
	)
	cluster, ns := newClusterWithDataNodesInNodeSet(t, zoneName, nodeSetID, srcLow, srcHigh, srcCurrent)
	srcLowNode, err := cluster.dataNode(srcLow)
	require.NoError(t, err)
	srcHighNode, err := cluster.dataNode(srcHigh)
	require.NoError(t, err)
	srcCurrentNode, err := cluster.dataNode(srcCurrent)
	require.NoError(t, err)
	dp := &DataPartition{
		PartitionID:              104,
		VolName:                  volName,
		ReplicaNum:               3,
		PartitionType:            proto.PartitionTypeNormal,
		Status:                   proto.ReadOnly,
		Hosts:                    []string{srcLow, srcHigh, srcCurrent},
		Peers:                    []proto.Peer{{ID: 1, Addr: srcLow}, {ID: 2, Addr: srcHigh}, {ID: 3, Addr: srcCurrent}},
		DecommissionStatus:       DecommissionSuccess,
		DecommissionSrcAddr:      srcCurrent,
		DecommissionSrcDiskPath:  "/disk-current",
		DecommissionType:         ManualDecommission,
		DecommissionDiskRetryMap: make(map[string]int),
	}
	dp.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: srcLow, DiskPath: "/disk-low", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), LocalPeers: dp.Peers}, dataNode: srcLowNode},
		{DataReplica: proto.DataReplica{Addr: srcHigh, DiskPath: "/disk-high", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), LocalPeers: dp.Peers}, dataNode: srcHighNode},
		{DataReplica: proto.DataReplica{Addr: srcCurrent, DiskPath: "/disk-current", Status: proto.ReadWrite, ReportTime: time.Now().Unix(), LocalPeers: dp.Peers}, dataNode: srcCurrentNode},
	}
	vol := &Vol{Name: volName, dataPartitions: newDataPartitionMap(volName)}
	vol.dataPartitions.put(dp)
	cluster.vols[volName] = vol

	low := DecommissionTask{
		DecommissionSrcAddr:     srcLow,
		DecommissionSrcDiskPath: "/disk-low",
		DecommissionTerm:        1,
		DecommissionWeight:      lowPriorityDecommissionWeight,
		DecommissionType:        ManualDecommission,
	}
	high := DecommissionTask{
		DecommissionSrcAddr:     srcHigh,
		DecommissionSrcDiskPath: "/disk-high",
		DecommissionTerm:        2,
		DecommissionWeight:      highPriorityDecommissionWeight,
		DecommissionType:        ManualDecommission,
	}
	dp.enqueueDecommissionTask(low)
	dp.enqueueDecommissionTask(high)

	dp.traverseDecommissionTaskQueue(cluster)

	require.EqualValues(t, markDecommission, dp.GetDecommissionStatus())
	require.Equal(t, srcHigh, dp.DecommissionSrcAddr)
	require.Equal(t, "/disk-high", dp.DecommissionSrcDiskPath)
	require.EqualValues(t, 2, dp.DecommissionTerm)
	require.Equal(t, highPriorityDecommissionWeight, dp.DecommissionWeight)
	require.Equal(t, ManualDecommission, dp.DecommissionType)
	require.Equal(t, 1, dp.countQueuedTasks())

	remainingQueue := dp.cloneDecommissionTaskQueue()
	require.Len(t, remainingQueue, 1)
	require.Equal(t, srcLow, remainingQueue[0].DecommissionSrcAddr)
	require.Equal(t, lowPriorityDecommissionWeight, remainingQueue[0].DecommissionWeight)
	require.True(t, ns.decommissionDataPartitionList.Has(dp.PartitionID))
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
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster, false))

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
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster, false))

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
				IdToPriority: map[uint64]int{
					0: 2,
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.True(t, partition.AcquireDecommissionFirstHostToken(cluster, false))
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
				IdToPriority: map[uint64]int{
					0: 2,
					1: 2,
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

// Tests for selectOptimalNodes function
func TestSelectOptimalNodes(t *testing.T) {
	// Create a test cluster with topology
	cluster := createTestClusterForOptimalNodes()

	tests := []struct {
		name           string
		currentAddrs   []string
		targetNsID     uint64
		expectSrcCount int
		expectDstCount int
		expectError    bool
		description    string
	}{
		{
			name:           "optimal distribution - no migration needed",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			targetNsID:     1,
			expectSrcCount: 0,
			expectDstCount: 0,
			expectError:    false,
			description:    "All replicas in target NodeSet with different racks",
		},
		{
			name:           "rack conflict (two replicas) - need migration",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			targetNsID:     1,
			expectSrcCount: 1,
			expectDstCount: 1,
			expectError:    false,
			description:    "Two replicas in same rack, need to migrate one",
		},
		{
			name:           "cross nodeset - need migration",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.2.1:17310"},
			targetNsID:     1,
			expectSrcCount: 1,
			expectDstCount: 1,
			expectError:    false,
			description:    "One replica in different NodeSet, need migration",
		},
		{
			name:           "mixed scenario - rack conflict and cross nodeset",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.2.1:17310"},
			targetNsID:     1,
			expectSrcCount: 2,
			expectDstCount: 2,
			expectError:    false,
			description:    "Rack conflict in target NodeSet + cross NodeSet replica",
		},
		{
			name:           "rack conflict (three replicas)",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.5:17310"},
			targetNsID:     1,
			expectSrcCount: 2,
			expectDstCount: 2,
			expectError:    false,
			description:    "Multiple nodes in same rack should be migrated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAddrs, dstAddrs, err := selectOptimalNodes(tt.currentAddrs, tt.targetNsID, cluster)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				return
			}

			require.NoError(t, err, "Unexpected error: %v", err)
			require.Equal(t, tt.expectSrcCount, len(srcAddrs), "Source addresses count mismatch")
			require.Equal(t, tt.expectDstCount, len(dstAddrs), "Destination addresses count mismatch")

			// Verify no duplicate addresses in results
			srcSet := make(map[string]bool)
			for _, addr := range srcAddrs {
				require.False(t, srcSet[addr], "Duplicate source address: %s", addr)
				srcSet[addr] = true
			}

			dstSet := make(map[string]bool)
			for _, addr := range dstAddrs {
				require.False(t, dstSet[addr], "Duplicate destination address: %s", addr)
				dstSet[addr] = true
			}

			// Verify source and destination addresses don't overlap
			for _, srcAddr := range srcAddrs {
				require.False(t, dstSet[srcAddr], "Source address %s also in destination", srcAddr)
			}

			t.Logf("Test case: %s", tt.description)
			t.Logf("Source addresses: %v", srcAddrs)
			t.Logf("Destination addresses: %v", dstAddrs)
		})
	}
}

func createTestClusterForOptimalNodes() *Cluster {
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
			t:         &topology{zoneMap: &sync.Map{}},
		},
		cfg: &clusterConfig{
			RackAwareLevel: proto.RackAwareStrong,
		},
	}

	// Set up atomic values
	distributionOptimizationThreshold.Store(0.8)

	// Create zones
	zone1 := &Zone{
		name:       "zone1",
		status:     normalZone,
		nodeSetMap: make(map[uint64]*nodeSet),
		dataNodes:  &sync.Map{},
		metaNodes:  &sync.Map{},
	}
	cluster.t.zoneMap.Store("zone1", zone1)

	zone2 := &Zone{
		name:       "zone2",
		status:     normalZone,
		nodeSetMap: make(map[uint64]*nodeSet),
		dataNodes:  &sync.Map{},
		metaNodes:  &sync.Map{},
	}
	cluster.t.zoneMap.Store("zone2", zone2)

	// Create NodeSets using the proper constructor
	ns1 := newNodeSet(nil, 1, 18, "zone1", "")
	zone1.nodeSetMap[1] = ns1

	ns2 := newNodeSet(nil, 2, 18, "zone2", "")
	zone2.nodeSetMap[2] = ns2

	// Create mock data nodes with more nodes per rack to satisfy rack-aware requirements
	// Each node needs proper storage attributes to be considered available
	mockNodes := []*DataNode{
		// NodeSet 1 nodes - rack1 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.1:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 1},
		{Addr: "192.168.1.4:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 4},
		{Addr: "192.168.1.5:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 5},

		// NodeSet 1 nodes - rack2 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.2:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 2},
		{Addr: "192.168.1.8:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 8},
		{Addr: "192.168.1.9:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 9},

		// NodeSet 1 nodes - rack3 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.3:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 3},
		{Addr: "192.168.1.10:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 10},
		{Addr: "192.168.1.11:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 11},

		// NodeSet 1 nodes - rack4 (available for migration)
		{Addr: "192.168.1.6:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 6},
		{Addr: "192.168.1.12:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 12},
		{Addr: "192.168.1.13:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 13},

		// NodeSet 1 nodes - rack5 (available for migration)
		{Addr: "192.168.1.7:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 7},
		{Addr: "192.168.1.14:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 14},
		{Addr: "192.168.1.15:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 15},

		// NodeSet 2 nodes
		{Addr: "192.168.2.1:17310", Rack: "rack1", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 21},
		{Addr: "192.168.2.2:17310", Rack: "rack2", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 22},
		{Addr: "192.168.2.3:17310", Rack: "rack3", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 23},
	}

	for _, node := range mockNodes {
		cluster.dataNodes.Store(node.Addr, node)

		// Add to appropriate NodeSet and create rack structures
		var ns *nodeSet
		if node.NodeSetID == 1 {
			ns = ns1
		} else if node.NodeSetID == 2 {
			ns = ns2
		}

		if ns != nil {
			ns.dataNodes.Store(node.Addr, node)

			// Create rack if it doesn't exist and add node to it
			ns.racksLock.Lock()
			rack, exists := ns.racks[node.Rack]
			if !exists {
				rack = newNodeSet(nil, uint64(len(ns.racks)+100), 6, node.ZoneName, node.Rack)
				ns.racks[node.Rack] = rack
			}
			rack.dataNodes.Store(node.Addr, node)
			ns.racksLock.Unlock()
		}
	}

	return cluster
}

func TestSelectOptimalNodesEdgeCases(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	tests := []struct {
		name         string
		currentAddrs []string
		targetNsID   uint64
		expectError  bool
		errorMsg     string
	}{
		{
			name:         "empty address list",
			currentAddrs: []string{},
			targetNsID:   1,
			expectError:  true, // Empty list cannot find target NodeSet
			errorMsg:     "should return error for empty address list",
		},
		{
			name:         "invalid node address",
			currentAddrs: []string{"invalid:17310"},
			targetNsID:   1,
			expectError:  true, // Invalid addresses cannot find target NodeSet
			errorMsg:     "should return error for invalid node addresses",
		},
		{
			name:         "nonexistent target nodeset",
			currentAddrs: []string{"192.168.1.1:17310"},
			targetNsID:   999,
			expectError:  true,
			errorMsg:     "should handle nonexistent target NodeSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAddrs, dstAddrs, err := selectOptimalNodes(tt.currentAddrs, tt.targetNsID, cluster)

			if tt.expectError {
				require.Error(t, err, tt.errorMsg)
				require.Nil(t, srcAddrs, "Source addresses should be nil on error")
				require.Nil(t, dstAddrs, "Destination addresses should be nil on error")
			} else {
				require.NoError(t, err, "Unexpected error: %v", err)
			}
		})
	}
}

func TestSelectOptimalNodesRackLogic(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	// Test case: Multiple replicas in same rack should keep only the first one
	currentAddrs := []string{
		"192.168.1.1:17310", // rack1 - should be kept (first)
		"192.168.1.4:17310", // rack1 - should be migrated (second)
		"192.168.1.5:17310", // rack1 - should be migrated (third)
		"192.168.1.2:17310", // rack2 - should be kept (first in rack2)
	}

	srcAddrs, dstAddrs, err := selectOptimalNodes(currentAddrs, 1, cluster)

	require.NoError(t, err, "Unexpected error")
	require.Equal(t, 2, len(srcAddrs), "Should migrate 2 replicas from rack1")
	require.Equal(t, 2, len(dstAddrs), "Should provide 2 destination addresses")

	// Verify that the kept addresses are the first ones from each rack
	expectedMigrated := []string{"192.168.1.4:17310", "192.168.1.5:17310"}
	for _, addr := range expectedMigrated {
		require.Contains(t, srcAddrs, addr, "Address %s should be in migration list", addr)
	}

	// Verify destination addresses are from different racks
	destRacks := make(map[string]bool)
	for _, addr := range dstAddrs {
		if node, ok := cluster.dataNodes.Load(addr); ok {
			dataNode := node.(*DataNode)
			require.False(t, destRacks[dataNode.Rack], "Destination addresses should be in different racks")
			destRacks[dataNode.Rack] = true
		}
	}
}

func TestSelectOptimalNodesCrossNodeSet(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	// Test case: Replicas across different NodeSets
	currentAddrs := []string{
		"192.168.1.1:17310", // NodeSet 1, rack1 - should be kept
		"192.168.1.2:17310", // NodeSet 1, rack2 - should be kept
		"192.168.2.1:17310", // NodeSet 2, rack1 - should be migrated
	}

	srcAddrs, dstAddrs, err := selectOptimalNodes(currentAddrs, 1, cluster)

	require.NoError(t, err, "Unexpected error")
	require.Equal(t, 1, len(srcAddrs), "Should migrate 1 replica from different NodeSet")
	require.Equal(t, 1, len(dstAddrs), "Should provide 1 destination address")

	// Verify the cross-NodeSet replica is migrated
	require.Contains(t, srcAddrs, "192.168.2.1:17310", "Cross-NodeSet replica should be migrated")

	// Verify destination is in target NodeSet and different rack
	destAddr := dstAddrs[0]
	if node, ok := cluster.dataNodes.Load(destAddr); ok {
		dataNode := node.(*DataNode)
		require.Equal(t, uint64(1), dataNode.NodeSetID, "Destination should be in target NodeSet")
		require.NotEqual(t, "rack1", dataNode.Rack, "Destination should not be in rack1")
		require.NotEqual(t, "rack2", dataNode.Rack, "Destination should not be in rack2")
	}
}

// testLiveDataReplica builds a replica that passes isLive() for needReplicaMetaRestore / checkReplicaMeta paths.
func testLiveDataReplica(addr string, applyMemberChangeID uint64, isLeader bool, localPeers []proto.Peer) *DataReplica {
	dn := &DataNode{isActive: true, Addr: addr}
	return &DataReplica{
		DataReplica: proto.DataReplica{
			Addr:                addr,
			Status:              proto.ReadWrite,
			ReportTime:          time.Now().Unix(),
			IsLeader:            isLeader,
			AppliedID:           applyMemberChangeID,
			ApplyMemberChangeID: applyMemberChangeID,
			LocalPeers:          localPeers,
		},
		dataNode: dn,
	}
}

func TestDataPartition_getLeaderApplyMemberChangeID(t *testing.T) {
	p := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h2", 7, false, nil),
			testLiveDataReplica("h1", 42, true, nil),
		},
	}
	id, ok := p.getLeaderApplyMemberChangeID()
	require.True(t, ok)
	require.EqualValues(t, 42, id)

	p2 := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 10, false, nil),
		},
	}
	id, ok = p2.getLeaderApplyMemberChangeID()
	require.False(t, ok)
	require.Zero(t, id)
}

func TestDataPartition_maxApplyMemberChangeID(t *testing.T) {
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	p := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 50, true, peers),
			testLiveDataReplica("h2", 100, false, peers),
		},
	}
	require.EqualValues(t, 50, p.maxApplyMemberChangeID(), "leader applyMemberChangeID should be used as baseline")

	pNoLeader := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 50, false, peers),
			testLiveDataReplica("h2", 100, false, peers),
		},
	}
	require.EqualValues(t, 100, pNoLeader.maxApplyMemberChangeID(), "without leader, max reported applyMemberChangeID should be used")
}

func TestDataReplica_isBehindApplyMemberChange(t *testing.T) {
	peer := testLiveDataReplica("h1", 99, false, nil)
	require.True(t, peer.isBehindApplyMemberChange(100), "non-zero applyMemberChangeID below baseline is behind")

	peer.ApplyMemberChangeID = 100
	require.False(t, peer.isBehindApplyMemberChange(100))

	peer.ApplyMemberChangeID = 0
	peer.AppliedID = 0
	require.False(t, peer.isBehindApplyMemberChange(100), "zero applyMemberChangeID means local peers can be trusted")

	leader := testLiveDataReplica("h2", 1, true, nil)
	require.False(t, leader.isBehindApplyMemberChange(100), "leader is always trusted as baseline owner")
}

// TestDataPartition_needReplicaMetaRestore_applyMemberChangeID covers e8f69a29: lagging followers must not
// drive replica-meta restore.
func TestDataPartition_needReplicaMetaRestore_applyMemberChangeID(t *testing.T) {
	c := &Cluster{cfg: newClusterConfig()}
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	orphan := proto.Peer{Addr: "orphan", Type: raftProto.PeerNormal}
	peersWithOrphan := append(append([]proto.Peer(nil), peers...), orphan)

	// Leader caught up; follower still behind on member-change log but carries redundant local peer — ignore follower.
	dpLaggingFollower := &DataPartition{
		PartitionID: 9901,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 50, false, peersWithOrphan),
		},
	}
	require.False(t, dpLaggingFollower.needReplicaMetaRestore(c), "lagging follower should not trigger restore")

	// A zero ApplyMemberChangeID means the replica's local peers can be trusted, even when AppliedID is also zero
	// (for example, a new replica created from snapshot and not yet seeing random writes).
	dpZeroApplyMemberChangeID := &DataPartition{
		PartitionID: 9905,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 0, false, peersWithOrphan),
		},
	}
	dpZeroApplyMemberChangeID.Replicas[1].AppliedID = 0
	require.True(t, dpZeroApplyMemberChangeID.needReplicaMetaRestore(c), "zero applyMemberChangeID replica should participate in restore checks")

	// Same topology but follower caught up to leader — redundant local peer should be detected.
	dpCaughtUp := &DataPartition{
		PartitionID: 9902,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 200, false, peersWithOrphan),
		},
	}
	require.True(t, dpCaughtUp.needReplicaMetaRestore(c), "caught-up follower with redundant peers should need restore")

	// Leader is skipped by the catch-up guard; the follower with redundant peers can drive restore.
	dpDivergent := &DataPartition{
		PartitionID: 9903,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 50, true, peers),
			testLiveDataReplica("h2", 100, false, peersWithOrphan),
		},
	}
	require.True(t, dpDivergent.needReplicaMetaRestore(c), "leader applyMemberChangeID is skipped by catch-up guard")
}

func TestDataPartition_needReplicaMetaRestore_manualAddMergedPeerChecks(t *testing.T) {
	c := &Cluster{cfg: newClusterConfig()}
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	orphan := proto.Peer{Addr: "orphan", Type: raftProto.PeerNormal}
	peersWithOrphan := append(append([]proto.Peer(nil), peers...), orphan)

	dpManualAddExtraLocalPeer := &DataPartition{
		PartitionID:      9906,
		ReplicaNum:       2,
		Peers:            peers,
		Hosts:            []string{"h1", "h2"},
		DecommissionType: ManualAddReplica,
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 100, true, peersWithOrphan),
		},
	}
	require.False(t, dpManualAddExtraLocalPeer.needReplicaMetaRestore(c),
		"ManualAddReplica should ignore replica-local peers that are not yet in master peers")

	dpManualAddMissingLocalPeer := &DataPartition{
		PartitionID:      9907,
		ReplicaNum:       2,
		Peers:            peers,
		Hosts:            []string{"h1", "h2"},
		DecommissionType: ManualAddReplica,
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 100, true, []proto.Peer{peers[0]}),
		},
	}
	require.True(t, dpManualAddMissingLocalPeer.needReplicaMetaRestore(c),
		"ManualAddReplica should still detect master peers missing from replica local peers")
}

func TestDataPartition_checkReplicaMetaSkipsLaggingApplyMemberChange(t *testing.T) {
	c := &Cluster{cfg: newClusterConfig()}
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	dp := &DataPartition{
		PartitionID: 9904,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 100, false, peers),
		},
	}

	require.NoError(t, dp.checkReplicaMeta(c))
}

// TestDataPartitionValue_replicaApplyMemberChangeIDJSON ensures raft-persisted DP replica value carries ApplyMemberChangeID (e8f69a29).
func TestDataPartitionValue_replicaApplyMemberChangeIDJSON(t *testing.T) {
	dp := &DataPartition{
		PartitionID: 88001,
		ReplicaNum:  1,
		VolName:     "vol_mc_json",
		VolID:       1,
		Hosts:       []string{"10.0.0.1:17310"},
		Replicas: []*DataReplica{
			{DataReplica: proto.DataReplica{Addr: "10.0.0.1:17310", DiskPath: "/data1", AppliedID: 999, ApplyMemberChangeID: 888}},
		},
	}
	dpv := newDataPartitionValue(dp)
	require.Len(t, dpv.Replicas, 1)
	require.EqualValues(t, 999, dpv.Replicas[0].AppliedID)
	require.EqualValues(t, 888, dpv.Replicas[0].ApplyMemberChangeID)

	raw, err := json.Marshal(dpv)
	require.NoError(t, err)
	require.Contains(t, string(raw), "appliedID")
	require.Contains(t, string(raw), "applyMemberChangeID")

	var decoded dataPartitionValue
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded.Replicas, 1)
	require.EqualValues(t, 999, decoded.Replicas[0].AppliedID)
	require.EqualValues(t, 888, decoded.Replicas[0].ApplyMemberChangeID)

	c := &Cluster{cfg: newClusterConfig()}
	c.dataNodes.Store("10.0.0.1:17310", &DataNode{Addr: "10.0.0.1:17310"})
	restored := decoded.Restore(c)
	require.Len(t, restored.Replicas, 1)
	require.EqualValues(t, 999, restored.Replicas[0].AppliedID)
	require.EqualValues(t, 888, restored.Replicas[0].ApplyMemberChangeID)
}

func TestDataPartition_updateMetricPersistsReplicaApplyProgress(t *testing.T) {
	const (
		volName     = "vol_apply_progress"
		partitionID = uint64(88002)
		addr        = "10.0.0.2:17310"
	)
	peers := []proto.Peer{{Addr: addr, Type: raftProto.PeerNormal}}
	replica := testLiveDataReplica(addr, 7, false, peers)
	replica.AppliedID = 5
	dp := &DataPartition{
		PartitionID: partitionID,
		ReplicaNum:  1,
		VolName:     volName,
		VolID:       1,
		Hosts:       []string{addr},
		Peers:       peers,
		Replicas:    []*DataReplica{replica},
	}
	vol := &Vol{Name: volName, dataPartitions: newDataPartitionMap(volName)}
	vol.dataPartitions.put(dp)
	c := &Cluster{
		cfg:       newClusterConfig(),
		partition: &mockPartition{isLeader: true},
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{volName: vol},
		},
	}
	dataNode := &DataNode{Addr: addr, ReplicaPort: "17320", HeartbeatPort: "17330"}

	dp.updateMetric(&proto.DataPartitionReport{
		PartitionID:         partitionID,
		PartitionStatus:     proto.ReadWrite,
		Total:               100,
		Used:                10,
		IsLeader:            false,
		LocalPeers:          peers,
		AppliedID:           11,
		ApplyMemberChangeID: 9,
	}, dataNode, c)

	require.EqualValues(t, 11, replica.AppliedID)
	require.EqualValues(t, 9, replica.ApplyMemberChangeID)
	dpv := newDataPartitionValue(dp)
	require.Len(t, dpv.Replicas, 1)
	require.EqualValues(t, 11, dpv.Replicas[0].AppliedID)
	require.EqualValues(t, 9, dpv.Replicas[0].ApplyMemberChangeID)

	dp.updateMetric(&proto.DataPartitionReport{
		PartitionID:         partitionID,
		PartitionStatus:     proto.ReadWrite,
		Total:               100,
		Used:                10,
		IsLeader:            false,
		LocalPeers:          peers,
		AppliedID:           0,
		ApplyMemberChangeID: 0,
	}, dataNode, c)

	require.EqualValues(t, 11, replica.AppliedID)
	require.EqualValues(t, 9, replica.ApplyMemberChangeID)
}

func TestDataPartition_updateMetricRollbackApplyProgressOnSyncFailure(t *testing.T) {
	const (
		volName     = "vol_apply_progress_rollback"
		partitionID = uint64(88003)
		addr        = "10.0.0.3:17310"
	)
	peers := []proto.Peer{{Addr: addr, Type: raftProto.PeerNormal}}
	replica := testLiveDataReplica(addr, 7, false, peers)
	replica.AppliedID = 5
	dp := &DataPartition{
		PartitionID: partitionID,
		ReplicaNum:  1,
		VolName:     volName,
		VolID:       1,
		Hosts:       []string{addr},
		Peers:       peers,
		Replicas:    []*DataReplica{replica},
	}
	vol := &Vol{Name: volName, dataPartitions: newDataPartitionMap(volName)}
	vol.dataPartitions.put(dp)
	c := &Cluster{
		cfg:       newClusterConfig(),
		partition: &failingSubmitPartition{mockPartition: mockPartition{isLeader: true}},
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{volName: vol},
		},
	}

	dp.updateMetric(&proto.DataPartitionReport{
		PartitionID:         partitionID,
		PartitionStatus:     proto.ReadWrite,
		Total:               100,
		Used:                10,
		LocalPeers:          peers,
		AppliedID:           11,
		ApplyMemberChangeID: 9,
	}, &DataNode{Addr: addr}, c)

	require.EqualValues(t, 5, replica.AppliedID)
	require.EqualValues(t, 7, replica.ApplyMemberChangeID)
}
