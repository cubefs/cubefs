// Copyright 2024 The CubeFS Authors.
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

package datanode

import (
	"encoding/json"
	"net"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newExtentStoreForOperatorTest(t *testing.T) (store *storage.ExtentStore) {
	path, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	store, err = storage.NewExtentStore(path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, true)
	require.NoError(t, err)
	return
}

func newDiskForOperatorTest(t *testing.T, dn *DataNode) (d *Disk) {
	var _ interface{} = t
	d = &Disk{
		Status:    proto.ReadWrite,
		Total:     1 * util.TB,
		Available: 1 * util.TB,
		Used:      0,
		dataNode:  dn,
	}
	d.limitFactor = make(map[uint32]*rate.Limiter)
	d.limitFactor[proto.FlowReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.FlowWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.IopsReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitFactor[proto.IopsWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitFactor[proto.IopsDeleteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitRead = util.NewIOLimiter(1*util.MB, 10)
	d.limitWrite = util.NewIOLimiter(1*util.MB, 10)
	d.limitAsyncRead = util.NewIOLimiter(1*util.MB, 10)
	d.limitDelete = util.NewIOLimiter(1*util.MB, 10)
	return
}

func newDpForOperatorTest(t *testing.T, dn *DataNode) (dp *DataPartition) {
	dp = &DataPartition{
		disk:        newDiskForOperatorTest(t, dn),
		extentStore: newExtentStoreForOperatorTest(t),
		config: &dataPartitionCfg{
			Forbidden:                false,
			ForbidWriteOpOfProtoVer0: false,
		},
		partitionSize: 1 * util.TB,
		dataNode:      dn,
	}
	dn.space.partitions[dp.partitionID] = dp
	return
}

func newPacketForOperatorTest(t *testing.T, dp *DataPartition, extentId uint64) (p *repl.Packet) {
	var _ interface{} = t
	p = &repl.Packet{
		Object: dp,
		Packet: proto.Packet{
			ExtentID: extentId,
		},
	}
	return
}

func newDataNodeForOperatorTest(t *testing.T) (dn *DataNode) {
	var _ interface{} = t
	dn = &DataNode{
		metrics: &DataNodeMetrics{
			dataNode: dn,
		},
		diskQosEnable:      true,
		diskAsyncQosEnable: true,
	}
	dn.space = NewSpaceManager(dn)
	return
}

func TestSkipAppendWrite(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)
	extentId := uint64(1000)
	p := newPacketForOperatorTest(t, dp, extentId)

	dataStr := "HelloWorld"

	p.Opcode = proto.OpCreateExtent
	dn.handlePacketToCreateExtent(p)
	t.Logf("handle create extent, result code(%v)", p.ResultCode)
	require.EqualValues(t, proto.OpOk, p.ResultCode)

	p = newPacketForOperatorTest(t, dp, extentId)
	p.Opcode = proto.OpWrite
	p.Data = []byte(dataStr)
	p.ExtentOffset = int64(len(p.Data))
	p.Size = uint32(len(p.Data))
	dn.handleWritePacket(p)
	t.Logf("handle write packet, result code(%v)", p.ResultCode)
	require.EqualValues(t, proto.OpArgMismatchErr, p.ResultCode)
}

func newPacketForTest(task *proto.AdminTask) *repl.Packet {
	data, _ := json.Marshal(task)
	return &repl.Packet{
		Packet: proto.Packet{
			Data: data,
		},
	}
}

type recordedRaftMemberChange struct {
	changeType raftProto.ConfChangeType
	peer       raftProto.Peer
	context    []byte
}

type mockRaftPartitionForDecommission struct {
	leaderID uint64
	changes  []recordedRaftMemberChange
}

func (m *mockRaftPartitionForDecommission) Submit([]byte) (interface{}, error) {
	return nil, nil
}

func (m *mockRaftPartitionForDecommission) ChangeMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (interface{}, error) {
	m.changes = append(m.changes, recordedRaftMemberChange{
		changeType: changeType,
		peer:       peer,
		context:    append([]byte(nil), context...),
	})
	return nil, nil
}

func (m *mockRaftPartitionForDecommission) Stop() error { return nil }

func (m *mockRaftPartitionForDecommission) Delete() error { return nil }

func (m *mockRaftPartitionForDecommission) Status() *raftstore.PartitionStatus {
	return &raftstore.PartitionStatus{}
}

func (m *mockRaftPartitionForDecommission) IsRestoring() bool { return false }

func (m *mockRaftPartitionForDecommission) LeaderTerm() (uint64, uint64) {
	return m.leaderID, 1
}

func (m *mockRaftPartitionForDecommission) IsRaftLeader() bool { return true }

func (m *mockRaftPartitionForDecommission) AppliedIndex() uint64 { return 0 }

func (m *mockRaftPartitionForDecommission) CommittedIndex() uint64 { return 0 }

func (m *mockRaftPartitionForDecommission) Truncate(uint64) {}

func (m *mockRaftPartitionForDecommission) TryToLeader(uint64) error { return nil }

func (m *mockRaftPartitionForDecommission) IsOfflinePeer() bool { return false }

func (m *mockRaftPartitionForDecommission) CloseAndBackup() error { return nil }

func (m *mockRaftPartitionForDecommission) Closed() bool { return false }

type mockRaftStoreForOperatorTest struct {
	cfg        *raft.Config
	partitions []*raftstore.PartitionConfig
}

func (m *mockRaftStoreForOperatorTest) CreatePartition(cfg *raftstore.PartitionConfig) (raftstore.Partition, error) {
	m.partitions = append(m.partitions, cfg)
	return &mockRaftPartitionForDecommission{leaderID: cfg.ID}, nil
}

func (m *mockRaftStoreForOperatorTest) Stop() {}

func (m *mockRaftStoreForOperatorTest) RaftConfig() *raft.Config {
	if m.cfg == nil {
		m.cfg = &raft.Config{
			TransportConfig: raft.TransportConfig{
				HeartbeatAddr: "127.0.0.1:17310",
				ReplicateAddr: "127.0.0.1:17320",
			},
		}
	}
	return m.cfg
}

func (m *mockRaftStoreForOperatorTest) RaftStatus(uint64) *raft.Status { return &raft.Status{} }

func (m *mockRaftStoreForOperatorTest) AddNodeWithPort(uint64, string, int, int) {}

func (m *mockRaftStoreForOperatorTest) DeleteNode(uint64) {}

func (m *mockRaftStoreForOperatorTest) RaftServer() *raft.RaftServer { return nil }

func (m *mockRaftStoreForOperatorTest) RemoveBackup(uint64) error { return nil }

func (m *mockRaftStoreForOperatorTest) GetPeers(uint64) []uint64 { return nil }

func (m *mockRaftStoreForOperatorTest) SetTruncateBlockMax(uint64, int) error { return nil }

func TestHandlePacketToDecommissionDataPartitionChangesRaftMembers(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)
	delete(dn.space.partitions, dp.partitionID)

	partitionID := uint64(101)
	addPeer := proto.Peer{ID: 3, Addr: "127.0.0.1:17312"}
	removePeer := proto.Peer{ID: 2, Addr: "127.0.0.1:17311"}
	mockRaft := &mockRaftPartitionForDecommission{leaderID: 1}

	dp.partitionID = partitionID
	dp.raftStatus = RaftStatusRunning
	dp.raftPartition = mockRaft
	dp.config.NodeID = 1
	dp.config.Peers = []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17310"},
		removePeer,
	}
	dn.space.partitions[partitionID] = dp

	task := proto.NewAdminTask(proto.OpDecommissionDataPartition, "", &proto.DataPartitionDecommissionRequest{
		PartitionId: partitionID,
		AddPeer:     addPeer,
		RemovePeer:  removePeer,
	})
	packet := newPacketForTest(task)

	dn.handlePacketToDecommissionDataPartition(packet)

	require.EqualValues(t, proto.OpOk, packet.ResultCode)
	require.Len(t, mockRaft.changes, 2)
	require.Equal(t, raftProto.ConfAddNode, mockRaft.changes[0].changeType)
	require.EqualValues(t, addPeer.ID, mockRaft.changes[0].peer.ID)
	require.Equal(t, raftProto.ConfRemoveNode, mockRaft.changes[1].changeType)
	require.EqualValues(t, removePeer.ID, mockRaft.changes[1].peer.ID)

	req := &proto.DataPartitionDecommissionRequest{}
	require.NoError(t, json.Unmarshal(mockRaft.changes[0].context, req))
	require.Equal(t, partitionID, req.PartitionId)
	require.Equal(t, addPeer, req.AddPeer)
	require.Equal(t, removePeer, req.RemovePeer)
}

func TestHandlePacketToAddDataPartitionRaftMemberChangesRaftMember(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)
	delete(dn.space.partitions, dp.partitionID)

	partitionID := uint64(102)
	addPeer := proto.Peer{ID: 3, Addr: "127.0.0.1:17312"}
	mockRaft := &mockRaftPartitionForDecommission{leaderID: 1}

	dp.partitionID = partitionID
	dp.raftStatus = RaftStatusRunning
	dp.raftPartition = mockRaft
	dp.config.NodeID = 1
	dp.config.Peers = []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17310"},
		{ID: 2, Addr: "127.0.0.1:17311"},
	}
	dn.space.partitions[partitionID] = dp

	task := proto.NewAdminTask(proto.OpAddDataPartitionRaftMember, "", &proto.AddDataPartitionRaftMemberRequest{
		PartitionId:     partitionID,
		AddPeer:         addPeer,
		RepairingStatus: true,
	})
	packet := newPacketForTest(task)

	dn.handlePacketToAddDataPartitionRaftMember(packet)

	require.EqualValues(t, proto.OpOk, packet.ResultCode)
	require.Len(t, mockRaft.changes, 1)
	require.Equal(t, raftProto.ConfAddNode, mockRaft.changes[0].changeType)
	require.EqualValues(t, addPeer.ID, mockRaft.changes[0].peer.ID)

	req := &proto.AddDataPartitionRaftMemberRequest{}
	require.NoError(t, json.Unmarshal(mockRaft.changes[0].context, req))
	require.Equal(t, partitionID, req.PartitionId)
	require.Equal(t, addPeer, req.AddPeer)
	require.True(t, req.RepairingStatus)
}

func TestHandlePacketToRemoveDataPartitionRaftMemberChangesRaftMember(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)
	delete(dn.space.partitions, dp.partitionID)

	partitionID := uint64(103)
	removePeer := proto.Peer{ID: 2, Addr: "127.0.0.1:17311"}
	mockRaft := &mockRaftPartitionForDecommission{leaderID: 1}

	dp.partitionID = partitionID
	dp.raftStatus = RaftStatusRunning
	dp.raftPartition = mockRaft
	dp.config.NodeID = 1
	dp.config.Peers = []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17310"},
		removePeer,
	}
	dp.replicas = []string{"127.0.0.1:17310", removePeer.Addr}
	dn.space.partitions[partitionID] = dp

	task := proto.NewAdminTask(proto.OpRemoveDataPartitionRaftMember, "", &proto.RemoveDataPartitionRaftMemberRequest{
		PartitionId:     partitionID,
		RemovePeer:      removePeer,
		RepairingStatus: true,
		AutoRemove:      true,
	})
	packet := newPacketForTest(task)

	dn.handlePacketToRemoveDataPartitionRaftMember(packet)

	require.EqualValues(t, proto.OpOk, packet.ResultCode)
	require.Len(t, mockRaft.changes, 1)
	require.Equal(t, raftProto.ConfRemoveNode, mockRaft.changes[0].changeType)
	require.EqualValues(t, removePeer.ID, mockRaft.changes[0].peer.ID)

	req := &proto.RemoveDataPartitionRaftMemberRequest{}
	require.NoError(t, json.Unmarshal(mockRaft.changes[0].context, req))
	require.Equal(t, partitionID, req.PartitionId)
	require.Equal(t, removePeer, req.RemovePeer)
	require.True(t, req.RepairingStatus)
	require.True(t, req.AutoRemove)
}

func TestHandlePacketToCreateDataPartitionCreatesReplica(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	raftStore := &mockRaftStoreForOperatorTest{}
	dn.space.raftStore = raftStore
	dn.space.nodeID = 1
	dn.space.clusterID = "cluster-create"
	diskPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(diskPath)
	})
	disk := newDiskForOperatorTest(t, dn)
	disk.Path = diskPath
	disk.space = dn.space
	disk.partitionMap = make(map[uint64]*DataPartition)
	dn.space.disks[diskPath] = disk

	partitionID := uint64(104)
	hosts := []string{"127.0.0.1:17310", "127.0.0.1:17311"}
	peers := []proto.Peer{
		{ID: 1, Addr: hosts[0]},
		{ID: 2, Addr: hosts[1]},
	}

	task := proto.NewAdminTask(proto.OpCreateDataPartition, "", &proto.CreateDataPartitionRequest{
		VolumeId:      "vol-create",
		PartitionId:   partitionID,
		ReplicaNum:    len(peers),
		PartitionSize: int(1 * util.GB),
		Members:       peers,
		Hosts:         hosts,
		PartitionTyp:  proto.PartitionTypeNormal,
		CreateType:    proto.NormalCreateDataPartition,
	})
	packet := newPacketForTest(task)

	dn.handlePacketToCreateDataPartition(packet)

	require.EqualValues(t, proto.OpOk, packet.ResultCode)
	require.Equal(t, diskPath, string(packet.Data[:packet.Size]))
	created := dn.space.Partition(partitionID)
	require.NotNil(t, created)
	require.Equal(t, partitionID, created.partitionID)
	require.Equal(t, "vol-create", created.volumeID)
	require.Equal(t, diskPath, created.Disk().Path)
	require.Len(t, raftStore.partitions, 1)
	require.Equal(t, partitionID, raftStore.partitions[0].ID)

	t.Cleanup(func() {
		if created := dn.space.Partition(partitionID); created != nil {
			created.Stop()
		}
	})
}

func TestHandlePacketToDeleteDataPartitionRemovesReplica(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)

	partitionID := uint64(105)
	partitionPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	dp.partitionID = partitionID
	dp.path = partitionPath
	dp.disk.Path = path.Dir(partitionPath)
	dp.disk.partitionMap = map[uint64]*DataPartition{partitionID: dp}
	dn.space.partitions = map[uint64]*DataPartition{partitionID: dp}

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "", &proto.DeleteDataPartitionRequest{
		PartitionId: partitionID,
		Force:       false,
	})
	packet := newPacketForTest(task)

	dn.handlePacketToDeleteDataPartition(packet)

	require.EqualValues(t, proto.OpOk, packet.ResultCode)
	require.Nil(t, dn.space.Partition(partitionID))
	require.Nil(t, dp.disk.GetDataPartition(partitionID))
	_, err = os.Stat(partitionPath)
	require.True(t, os.IsNotExist(err))
}

func TestMarkDeleteIopsLimit(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  net.Conn
	)

	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)

	for i := 100; i < 900; i++ {
		p := newPacketForOperatorTest(t, dp, uint64(i))
		p.Opcode = proto.OpCreateExtent
		dn.handlePacketToCreateExtent(p)
		require.EqualValues(t, proto.OpOk, p.ResultCode)
	}

	dp.dataNode.diskAsyncQosEnable = true
	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(200))
	startTime := time.Now()
	for i := 100; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime1 := time.Since(startTime)
	t.Logf("cost time1(%v)", costTime1)

	time.Sleep(time.Second)

	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(50))
	startTime = time.Now()
	for i := 500; i < 900; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime2 := time.Since(startTime)
	t.Logf("cost time2(%v)", costTime2)

	require.Greater(t, costTime2, costTime1)
}

func TestMarkDeleteIoccLimit(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  net.Conn
	)

	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)

	for i := 100; i < 700; i++ {
		p := newPacketForOperatorTest(t, dp, uint64(i))
		p.Opcode = proto.OpCreateExtent
		dn.handlePacketToCreateExtent(p)
		require.EqualValues(t, proto.OpOk, p.ResultCode)
	}

	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(100))
	startTime := time.Now()
	for i := 100; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
		}(i)
	}
	wg.Wait()

	costTime1 := time.Since(startTime)
	t.Logf("cost time1(%v)", costTime1)

	dp.disk.limitDelete.ResetIO(2, 0)
	startTime = time.Now()
	for i := 400; i < 700; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime2 := time.Since(startTime)
	t.Logf("cost time2(%v)", costTime2)

	require.Greater(t, costTime2, costTime1)
}

func TestDeleteLostDisk(t *testing.T) {
	dn := &DataNode{
		space: &SpaceManager{
			disks:     make(map[string]*Disk),
			diskList:  []string{},
			diskUtils: make(map[string]*atomicutil.Float64),
		},
	}

	testDiskPath1 := "/test/disk1"
	testDiskPath2 := "/test/disk2"

	lostDisk1 := NewLostDisk(
		testDiskPath1,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	lostDisk2 := NewLostDisk(
		testDiskPath2,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	dn.space.putDisk(lostDisk1)
	dn.space.putDisk(lostDisk2)

	t.Run("normal delete disk", func(t *testing.T) {
		req := &proto.DeleteLostDiskRequest{DiskPath: testDiskPath1}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpOk, p.ResultCode)
		_, err := dn.space.GetDisk(testDiskPath1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not exist")
	})

	t.Run("delete unexist disk", func(t *testing.T) {
		invalidReq := &proto.DeleteLostDiskRequest{DiskPath: "/invalid/path"}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: invalidReq,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not exist")
	})

	t.Run("delete unlost disk", func(t *testing.T) {
		lostDisk2.isLost = false
		invalidReq := &proto.DeleteLostDiskRequest{DiskPath: testDiskPath2}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: invalidReq,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		t.Logf("%v", string(p.Data))
		require.Contains(t, string(p.Data), "not lost")
	})
}

func TestReloadDisk(t *testing.T) {
	tmpDir, err := os.MkdirTemp(".", "")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, err)

	dn := &DataNode{
		diskReadFlow:      1 * util.MB,
		diskAsyncReadFlow: 1 * util.MB,
		diskWriteFlow:     1 * util.MB,
		diskReadIocc:      10,
		diskAsyncReadIocc: 10,
		diskWriteIocc:     10,
	}
	sm := &SpaceManager{
		disks:     make(map[string]*Disk),
		dataNode:  dn,
		diskList:  []string{},
		diskUtils: make(map[string]*atomicutil.Float64),
	}
	dn.space = sm

	testDiskPath := path.Join(tmpDir, "disk1")
	err = os.Mkdir(testDiskPath, 0o755)
	require.NoError(t, err)

	disk := NewLostDisk(
		testDiskPath,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	dn.space.putDisk(disk)

	t.Run("normal reload disk", func(t *testing.T) {
		req := &proto.ReloadDiskRequest{DiskPath: testDiskPath}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpOk, p.ResultCode)
		require.Eventually(t, func() bool {
			disk, _ := dn.space.GetDisk(testDiskPath)
			return disk != nil && !disk.isLost
		}, 3*time.Second, 100*time.Millisecond, "disk not loaded")
	})

	t.Run("reload unexist disk", func(t *testing.T) {
		req := &proto.ReloadDiskRequest{DiskPath: "/invalid/path"}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not exist")
	})

	t.Run("reload unlost disk", func(t *testing.T) {
		disk.isLost = false
		req := &proto.ReloadDiskRequest{DiskPath: testDiskPath}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not lost")
	})
}
