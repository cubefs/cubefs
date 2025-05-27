package datanode

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type repairWorker struct {
	proto.Packet
	// followersAddrs  []string
	// followerPackets []*repl.FollowerPacket
	IsReleased int32 // TODO what is released?
	Object     interface{}
	TpObject   *exporter.TimePointCount
	NeedReply  bool
	OrgBuffer  []byte

	// used locally
	shallDegrade bool
	AfterPre     bool
	packChannel  chan repl.PacketInterface
	dp           *DataPartition
	dstWorker    *repairWorker
	role         string
}

func (p *repairWorker) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
}

func (p *repairWorker) SetArglen(len uint32) {
	p.ArgLen = len
}

func (p *repairWorker) SetArg(data []byte) {
	p.Arg = data
}

func (p *repairWorker) GetCRC() uint32 {
	return p.CRC
}

func (p *repairWorker) GetStartT() int64 {
	return p.StartT
}

func (p *repairWorker) GetOpcode() uint8 {
	return p.Opcode
}

func (p *repairWorker) GetUniqueLogId() (m string) {
	return ""
}

func (p *repairWorker) GetReqID() int64 {
	return p.ReqID
}

func (p *repairWorker) GetPartitionID() uint64 {
	return p.PartitionID
}

func (p *repairWorker) GetExtentID() uint64 {
	return p.ExtentID
}

func (p *repairWorker) GetSize() uint32 {
	return p.Size
}

func (p *repairWorker) SetSize(size uint32) {
	p.Size = size
}

func (p *repairWorker) GetArg() []byte {
	return p.Arg
}

func (p *repairWorker) GetArgLen() uint32 {
	return p.ArgLen
}

func (p *repairWorker) GetData() []byte {
	return p.Data
}

func (p *repairWorker) GetResultCode() uint8 {
	return p.ResultCode
}

func (p *repairWorker) GetExtentOffset() int64 {
	return p.ExtentOffset
}

func (p *repairWorker) SetResultCode(code uint8) {
	p.ResultCode = code
}

func (p *repairWorker) SetCRC(crc uint32) {
	p.CRC = crc
}

func (p *repairWorker) SetExtentOffset(offset int64) {
	p.ExtentOffset = offset
}

func (p *repairWorker) ShallDegrade() bool {
	return p.shallDegrade
}

func (p *repairWorker) SetStartT(StartT int64) {
	p.StartT = StartT
}

func (p *repairWorker) SetData(data []byte) {
	p.Data = data
}

func (p *repairWorker) SetOpCode(op uint8) {
	p.Opcode = op
}

func (p *repairWorker) PackErrorBody(action, msg string) {
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

var sendWorker, recvWorker *repairWorker

func (p *repairWorker) WriteToConn(c net.Conn) (err error) {
	pr := new(repl.Packet)
	pr.Opcode = p.Opcode
	pr.Data = make([]byte, len(p.Data))
	copy(pr.Data, p.Data)
	pr.ArgLen = p.ArgLen
	pr.Arg = make([]byte, len(p.Arg))
	copy(pr.Arg, p.Arg)
	pr.Size = p.Size
	pr.ResultCode = p.ResultCode
	pr.StartT = p.StartT
	pr.PartitionID = p.PartitionID
	pr.ExtentID = p.ExtentID
	pr.ExtentOffset = p.ExtentOffset
	pr.ExtentType = p.ExtentType
	pr.ReqID = p.ReqID
	pr.CRC = p.CRC

	p.dstWorker.packChannel <- pr
	return nil
}

func (p *repairWorker) ReadFromConnWithVer(c net.Conn, timeoutSec int) (err error) {
	pr := <-p.packChannel
	{
		p.CRC = pr.GetCRC()
		p.Data = make([]byte, len(pr.GetData()))
		copy(p.Data, pr.GetData())
		p.ArgLen = pr.GetArgLen()
		p.Arg = make([]byte, p.ArgLen)
		copy(p.Arg, pr.GetArg())

		p.ExtentID = pr.GetExtentID()
		p.PartitionID = pr.GetPartitionID()
		p.ResultCode = pr.GetResultCode()
		p.Size = pr.GetSize()
		p.ExtentOffset = pr.GetExtentOffset()
	}
	return nil
}

func getSrcPathExtentStore(role string) (string, func(), error) {
	pattern := "cfs_storage_extentstore_" + role + "_"
	dir, err := os.MkdirTemp(os.TempDir(), pattern)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s/extents", dir), func() { os.RemoveAll(dir) }, nil
}

func reciverMakeTinyPacket(partitionID uint64, extentID uint64, offset, size int) (p repl.PacketInterface) {
	return &repairWorker{}
}

func reciverMakeNormalPacket(partitionID uint64, extentID uint64, offset, size int) (p repl.PacketInterface) {
	recvWorker.ExtentOffset = int64(offset)
	recvWorker.Size = uint32(size)
	recvWorker.PartitionID = partitionID
	recvWorker.ExtentID = extentID
	recvWorker.Opcode = proto.OpExtentRepairRead

	return recvWorker
}

func reciverMakeExtentWithHoleRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) (p repl.PacketInterface) {
	recvWorker.ExtentOffset = int64(offset)
	recvWorker.Size = uint32(size)
	recvWorker.PartitionID = partitionID
	recvWorker.ExtentID = extentID
	recvWorker.Opcode = proto.OpSnapshotExtentRepairRead

	return recvWorker
}

func reciverNewPacket() (p repl.PacketInterface) {
	pr := recvWorker
	pr.Magic = proto.ProtoMagic
	pr.StartT = time.Now().UnixNano()
	pr.NeedReply = true
	return pr
}

func sendNewNormalReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) (p repl.PacketInterface) {
	pr := sendWorker
	pr.ExtentID = extentID
	pr.PartitionID = partitionID
	pr.Magic = proto.ProtoMagic
	pr.Opcode = proto.OpOk
	pr.ReqID = requestID
	pr.ExtentType = proto.NormalExtentType

	return pr
}

func mockMakeDp(path string) *DataPartition {
	return &DataPartition{
		volumeID:                "dpCfg.VolName",
		clusterID:               "clusterID",
		partitionID:             1,
		replicaNum:              3,
		disk:                    &Disk{},
		dataNode:                &DataNode{},
		path:                    path,
		partitionType:           proto.PartitionTypeNormal,
		replicas:                make([]string, 0),
		stopC:                   make(chan bool),
		stopRaftC:               make(chan uint64),
		storeC:                  make(chan uint64, 128),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		config:                  &dataPartitionCfg{},
		raftStatus:              RaftStatusStopped,
		DataPartitionCreateType: proto.NormalCreateDataPartition,
		volVersionInfoList:      &proto.VolVersionInfoList{},
	}
}

func extentStoreNormalRwTest(t *testing.T, s *storage.ExtentStore, id uint64, crc uint32, data []byte) {
	// append write
	param := &storage.WriteParam{
		ExtentID:      id,
		Offset:        0,
		Size:          int64(len(data)),
		Data:          data,
		Crc:           crc,
		WriteType:     storage.AppendWriteType,
		IsSync:        true,
		IsHole:        false,
		IsRepair:      false,
		IsBackupWrite: false,
	}
	_, err := s.Write(param)
	require.NoError(t, err)
	actualCrc, err := s.Read(id, 0, int64(len(data)), data, false, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// random write
	param.WriteType = storage.RandomWriteType
	_, err = s.Write(param)
	require.NoError(t, err)
	actualCrc, err = s.Read(id, 0, int64(len(data)), data, false, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// TODO: append random write
	require.NotEqualValues(t, s.GetStoreUsedSize(), 0)
}

func extentReloadCheckNormalCrc(t *testing.T, s *storage.ExtentStore, id uint64, crc uint32) {
	var err error
	// extent crc check
	var e *storage.Extent
	e, err = s.LoadExtentFromDisk(id, true)
	assert.True(t, err == nil)
	extCrc := e.GetCrc(0)
	assert.True(t, crc == extCrc)
}

func extentStoreSnapshotRwTest(t *testing.T, s *storage.ExtentStore, id uint64, crc uint32, data []byte) {
	// append write
	param := &storage.WriteParam{
		ExtentID:      id,
		Offset:        int64(util.ExtentSize),
		Size:          int64(len(data)),
		Data:          data,
		Crc:           crc,
		IsSync:        true,
		IsHole:        false,
		IsRepair:      false,
		IsBackupWrite: false,
	}

	offset := int64(util.ExtentSize)
	param.WriteType = storage.AppendRandomWriteType
	_, err := s.Write(param)
	require.NoError(t, err)

	param.WriteType = storage.AppendRandomWriteType
	param.Offset = 0
	_, err = s.Write(param)
	assert.True(t, err != nil)

	param.WriteType = storage.AppendWriteType
	param.Offset = offset
	_, err = s.Write(param)
	assert.True(t, err != nil)

	actualCrc, err := s.Read(id, offset, int64(len(data)), data, false, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// random write
	param.WriteType = storage.RandomWriteType
	_, err = s.Write(param)
	require.NoError(t, err)
	actualCrc, err = s.Read(id, offset, int64(len(data)), data, false, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// TODO: append random write
	require.NotEqualValues(t, s.GetStoreUsedSize(), 0)

	param.WriteType = storage.AppendRandomWriteType
	_, err = s.Write(param)
	require.NoError(t, err)

	// extent crc check
	var e *storage.Extent
	e, err = s.LoadExtentFromDisk(id, true)
	assert.True(t, err == nil)
	extCrc := e.GetCrc(offset / util.BlockSize)
	assert.True(t, crc == extCrc)

	// check
	param.Offset = int64(util.ExtentSize)*2 + util.BlockSize
	_, err = s.Write(param)
	require.NoError(t, err)

	e, err = s.LoadExtentFromDisk(id, true)
	assert.True(t, err == nil)
	extCrc = e.GetCrc(param.Offset / util.BlockSize)
	assert.True(t, crc == extCrc)
	extCrc = e.GetCrc(param.Offset/util.BlockSize + 1)
	assert.True(t, 0 == extCrc)
}

func extentReloadCheckSnapshotCrc(t *testing.T, path string, id uint64, crc uint32) (s *storage.ExtentStore) {
	var err error
	s, err = storage.NewExtentStore(path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, false)
	require.NoError(t, err)

	offset := int64(util.ExtentSize)
	// extent crc check
	var e *storage.Extent
	e, err = s.LoadExtentFromDisk(id, true)
	assert.True(t, err == nil)
	extCrc := e.GetCrc(offset / util.BlockSize)
	require.EqualValues(t, crc, extCrc)

	// check
	offset = int64(util.ExtentSize)*2 + util.BlockSize
	e, err = s.LoadExtentFromDisk(id, true)
	assert.True(t, err == nil)
	extCrc = e.GetCrc(offset / util.BlockSize)
	require.EqualValues(t, crc, extCrc)
	extCrc = e.GetCrc(offset/util.BlockSize + 1)
	assert.True(t, 0 == extCrc)
	return s
}

func mockInitWorker(t *testing.T, role string) *repairWorker {
	worker := &repairWorker{role: role}
	worker.packChannel = make(chan repl.PacketInterface, 100)
	path, _, err := getSrcPathExtentStore(role)
	assert.True(t, err == nil)
	s, err := storage.NewExtentStore(path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, true)

	require.NoError(t, err)
	worker.dp = mockMakeDp(path)
	// spaceManager := NewSpaceManager(worker.dp.dataNode)
	spaceManager := &SpaceManager{
		dataNode:   worker.dp.dataNode,
		partitions: make(map[uint64]*DataPartition),
	}
	worker.dp.disk, err = NewDisk("/tmp", 200, 2000, 10, spaceManager, false)
	require.NoError(t, err)
	spaceManager.partitions[worker.dp.partitionID] = worker.dp
	worker.dp.dataNode.space = spaceManager
	worker.dp.extentStore = s
	worker.dp.dataNode.getRepairConnFunc = func(target string) (net.Conn, error) {
		return new(net.TCPConn), nil
	}
	worker.dp.dataNode.putRepairConnFunc = func(con net.Conn, force bool) {
		_ = struct{}{}
	}
	return worker
}

func genDataAndGetCrc(repeatWord string, size int) (data []byte, crc uint32) {
	repeatWordBytes := []byte(repeatWord)
	repeatWordLen := len(repeatWordBytes)
	numRepeats := size / repeatWordLen
	remainingBytes := size % repeatWordLen

	data = bytes.Repeat(repeatWordBytes, numRepeats)
	data = append(data, repeatWordBytes[:remainingBytes]...)
	crc = crc32.ChecksumIEEE(data)
	return
}

func workerInit(t *testing.T, id uint64, data []byte, crc uint32) {
	_ = id
	_ = data
	_ = crc
	sendWorker = mockInitWorker(t, "sender")
	recvWorker = mockInitWorker(t, "receiver")
	sendWorker.dstWorker = recvWorker
	recvWorker.dstWorker = sendWorker
}

func senderRepairWorker(t *testing.T, exitCh chan struct{}) {
	con := new(net.TCPConn)
	for {
		select {
		case pr := <-sendWorker.packChannel:
			if pr.GetOpcode() == proto.OpSnapshotExtentRepairRead {
				t.Logf("TestExtentRepair role %v recive packet (%v)", "sender", pr)
				getReplyPacket := func() repl.PacketInterface {
					sendWorker.ExtentID = pr.GetExtentID()
					sendWorker.PartitionID = pr.GetPartitionID()
					sendWorker.Magic = proto.ProtoMagic
					sendWorker.Opcode = proto.OpOk
					sendWorker.ReqID = pr.GetReqID()
					sendWorker.ExtentType = proto.NormalExtentType
					return sendWorker
				}
				sendWorker.dp.ExtentWithHoleRepairRead(pr, con, getReplyPacket)
			} else if pr.GetOpcode() == proto.OpExtentRepairRead {
				t.Logf("TestExtentRepair role %v recive packet (%v)", "sender", pr)
				sendWorker.dp.NormalExtentRepairRead(pr, con, true, nil, sendNewNormalReadResponsePacket)
			}

		case <-exitCh:
			return
		}
	}
}

func recvRepairWorker(t *testing.T, id uint64, exitCh chan struct{}) {
	t.Logf("TestExtentRepair role %v start", "reciver")
	ei, err := sendWorker.dp.extentStore.Watermark(id)
	assert.True(t, err == nil)
	t.Logf("TestExtentRepair role %v streamRepairExtent", "reciver")
	repairExtent := &RepairExtentInfo{
		ExtentInfo: *ei,
		Source:     "",
	}
	err = recvWorker.dp.streamRepairExtent(repairExtent, reciverMakeTinyPacket, reciverMakeNormalPacket, reciverMakeExtentWithHoleRepairReadPacket, reciverNewPacket)
	assert.True(t, err == nil)
	exitCh <- struct{}{}
}

func testDoRepair(t *testing.T, normalId uint64) {
	var wg sync.WaitGroup
	exitCh := make(chan struct{}, 1)
	wg.Add(1)
	go func(role string) {
		defer func() {
			wg.Done()
			t.Logf("TestExtentRepair role %v finished", role)
		}()
		t.Logf("TestExtentRepair role %v start", role)
		senderRepairWorker(t, exitCh)
	}("sender")

	wg.Add(1)
	go func(role string) {
		defer func() {
			wg.Done()
			t.Logf("TestExtentRepair role %v finished", role)
		}()
		recvRepairWorker(t, normalId, exitCh)
	}("receiver")
	wg.Wait()
	t.Logf("TestExtentRepair finished")
}

func testDoNormalRepair(t *testing.T, normalId uint64, data []byte, crc uint32, isCreate bool) {
	if isCreate {
		s1 := sendWorker.dp.extentStore
		err := s1.Create(normalId)
		require.NoError(t, err)

		s2 := recvWorker.dp.extentStore
		err = s2.Create(normalId)
		assert.True(t, err == nil)
	}
	extentStoreNormalRwTest(t, sendWorker.dp.extentStore, normalId, crc, data)
	testDoRepair(t, normalId)
	sendWorker.dp.extentStore.Close()
	recvWorker.dp.extentStore.Close()

	var err error
	recvWorker.dp.extentStore, err = storage.NewExtentStore(recvWorker.dp.path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, false)
	require.NoError(t, err)
	extentReloadCheckNormalCrc(t, recvWorker.dp.extentStore, normalId, crc)
	recvWorker.dp.extentStore.Close()
}

func testDoSnapshotRepair(t *testing.T, normalId uint64, data []byte, crc uint32, isCreate bool) {
	var err error

	recvWorker.dp.extentStore, err = storage.NewExtentStore(recvWorker.dp.path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, false)
	require.NoError(t, err)
	sendWorker.dp.extentStore, err = storage.NewExtentStore(sendWorker.dp.path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, false)
	require.NoError(t, err)
	if isCreate {
		s1 := sendWorker.dp.extentStore
		err := s1.Create(normalId)
		require.NoError(t, err)

		s2 := recvWorker.dp.extentStore
		err = s2.Create(normalId)
		assert.True(t, err == nil)
	}

	// write snapshot
	extentStoreSnapshotRwTest(t, sendWorker.dp.extentStore, normalId, crc, data)

	// do append data large than 128MB
	testDoRepair(t, normalId)

	// check crc
	recvWorker.dp.extentStore.Close()
	recvWorker.dp.extentStore = extentReloadCheckSnapshotCrc(t, recvWorker.dp.path, normalId, crc)
}

func TestExtentRepair(t *testing.T) {
	proto.InitBufferPool(int64(32768))
	t.Logf("TestExtentRepair initWorker")
	normalId := uint64(1025)
	data, crc := genDataAndGetCrc("normal", util.BlockSize)
	workerInit(t, normalId, data, crc)
	defer func() {
		os.RemoveAll(filepath.Dir(sendWorker.dp.path))
		os.RemoveAll(filepath.Dir(recvWorker.dp.path))
	}()
	testDoNormalRepair(t, normalId, data, crc, true)

	data, crc = genDataAndGetCrc("snapshot", util.BlockSize)
	testDoSnapshotRepair(t, normalId, data, crc, false)
}

func TestExtentRepair_512KB(t *testing.T) {
	proto.InitBufferPool(int64(32768))
	t.Logf("TestExtentRepair_512KB initWorker")
	normalId := uint64(1025)
	data, crc := genDataAndGetCrc("normal", util.BlockSize)
	workerInit(t, normalId, data, crc)
	// NOTE: set repair size to 512KB
	sendWorker.dp.SetRepairBlockSize(512 * util.KB)
	t.Logf("Repair size is %v", sendWorker.dp.GetRepairBlockSize())
	defer func() {
		os.RemoveAll(filepath.Dir(sendWorker.dp.path))
		os.RemoveAll(filepath.Dir(recvWorker.dp.path))
	}()
	testDoNormalRepair(t, normalId, data, crc, true)
	data, crc = genDataAndGetCrc("snapshot", util.BlockSize)
	testDoSnapshotRepair(t, normalId, data, crc, false)
}

func TestExtentRepairWithIOLimit(t *testing.T) {
	proto.InitBufferPool(int64(32768))
	t.Logf("TestExtentRepairWithIOLimit initWorker")

	const repairRateLimit = 1 * util.MB

	// normalIds := []uint64{1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034}
	normalIds := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		normalIds[i] = uint64(1025 + i)
	}
	data, crc := genDataAndGetCrc("normal", util.BlockSize)

	sendWorker = mockInitWorker(t, "sender")
	recvWorker = mockInitWorker(t, "receiver")
	sendWorker.dstWorker = recvWorker
	recvWorker.dstWorker = sendWorker

	sendWorker.dp.disk.limitAsyncRead = util.NewIOLimiter(repairRateLimit, 0)
	recvWorker.dp.disk.limitAsyncWrite = util.NewIOLimiter(repairRateLimit, 0)

	for _, id := range normalIds {
		err := sendWorker.dp.extentStore.Create(id)
		require.NoError(t, err)
		err = recvWorker.dp.extentStore.Create(id)
		require.NoError(t, err)
		extentStoreNormalRwTest(t, sendWorker.dp.extentStore, id, crc, data)
	}

	defer func() {
		os.RemoveAll(filepath.Dir(sendWorker.dp.path))
		os.RemoveAll(filepath.Dir(recvWorker.dp.path))
	}()
	testConcurrentRepairs(t, normalIds, repairRateLimit)
}

func testConcurrentRepairs(t *testing.T, normalIds []uint64, repairRateLimit int64) {
	var wg sync.WaitGroup
	exitCh := make(chan struct{})
	startTime := time.Now()

	defer func() {
		totalDuration := time.Since(startTime)
		t.Logf("totalDuration: %v", totalDuration)
		expectedMinDuration := time.Duration((len(normalIds)*util.BlockSize)/(int(repairRateLimit))) * time.Second
		t.Logf("expectedMinDuration: %v", expectedMinDuration)
		assert.True(t, totalDuration > expectedMinDuration,
			fmt.Sprintf("actural cost time %v should > expected cost time %v", totalDuration, expectedMinDuration))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		senderRepairWorkerWithConcurrent(t, exitCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		recvRepairWorkerWithConcurrent(t, normalIds, exitCh)
	}()

	wg.Wait()
	close(exitCh)
}

func senderRepairWorkerWithConcurrent(t *testing.T, exitCh chan struct{}) {
	con := new(net.TCPConn)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for {
		select {
		case pr := <-sendWorker.packChannel:
			if pr.GetOpcode() == proto.OpExtentRepairRead {
				sem <- struct{}{}
				wg.Add(1)
				go func(p repl.PacketInterface) {
					defer func() {
						<-sem
						wg.Done()
					}()
					start := time.Now()
					t.Logf("Sender: limitAsyncRead %v", sendWorker.dp.disk.limitAsyncRead.Status(false))
					sendWorker.dp.NormalExtentRepairRead(pr, con, true, nil, sendNewNormalReadResponsePacket)
					elapsed := time.Since(start)
					t.Logf("request %d read cost time: %v", pr.GetExtentID(), elapsed)
				}(pr)
			}
		case <-exitCh:
			wg.Wait()
			return
		}
	}
}

func recvRepairWorkerWithConcurrent(t *testing.T, ids []uint64, exitCh chan struct{}) {
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(eid uint64) {
			defer wg.Done()
			ei, err := sendWorker.dp.extentStore.Watermark(eid)
			require.NoError(t, err)
			t.Logf("Recver: limitAsyncWrite %v", recvWorker.dp.disk.limitAsyncWrite.Status(false))
			repairExtent := &RepairExtentInfo{ExtentInfo: *ei}
			start := time.Now()
			err = recvWorker.dp.streamRepairExtent(repairExtent,
				reciverMakeTinyPacket,
				reciverMakeNormalPacket,
				reciverMakeExtentWithHoleRepairReadPacket,
				reciverNewPacket)
			assert.NoError(t, err)
			elapsed := time.Since(start)
			t.Logf("request %d write cost time: %v", eid, elapsed)
		}(id)
	}
	wg.Wait()
	exitCh <- struct{}{}
}
