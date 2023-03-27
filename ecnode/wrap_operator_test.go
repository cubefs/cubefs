package ecnode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/unit"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

const (
	testDiskPath = "/cfs/testDisk/"
	Master       = "master"
)

var (
	fakeNode        *fakeEcNode
	startMasterHttp bool
	partitionIdNum  uint64
)

type fakeEcNode struct {
	EcNode
	Hosts              []string
	path               string
	fakeNormalExtentId uint64
	fakeTinyExtentId   uint64
}

func newFakeEcNode(t *testing.T) *fakeEcNode {
	e := &fakeEcNode{
		EcNode: EcNode{
			clusterID:       "ecnode-cluster",
			port:            "11310",
			cellName:        "cell-01",
			localIP:         "127.0.0.1",
			localServerAddr: "127.0.0.1:11310",
			nodeID:          uint64(123),
			stopC:           make(chan bool),
			scrubEnable:     true,
		},
		Hosts: []string{
			"127.0.0.1:11310",
			"127.0.0.1:11311",
			"127.0.0.1:11312",
			"127.0.0.1:11313",
			"127.0.0.1:11314",
			"127.0.0.1:11315",
		},
		fakeNormalExtentId: 1025,
		fakeTinyExtentId:   1,
		path:               testDiskPath,
	}
	/*configFilePath   := "../docker/conf/ecnode_test.json"
	cfg, err := config.LoadConfigFile(configFilePath)
	if err != nil {
		_ = daemonize.SignalOutcome(err)
		return nil
	}*/

	runMasterHttp(t)
	masters := []string{
		"127.0.0.1:6666",
	}
	MasterClient = masterSDK.NewMasterClient(masters, false)
	cfgStr := "{\n\t\t\t\"cell\": \"test-cell\",\n\t\t\t\"disks\": [\"/cfs/testDisk:5368709120\"],\n\t\t\t\"listen\": \"11310\",\n\t\t\t\"logDir\":\"/cfs/log\",\n\t\t\t\"masterAddr\": [\"127.0.0.1:12310\", \"127.0.0.1:12311\", \"127.0.0.1:12312\"],\n\t\t\t\"prof\": \"11410\"}"
	cfg := config.LoadConfigString(cfgStr)
	e.EcNode.Start(cfg)

	for _, host := range e.Hosts {
		if host == e.localServerAddr {
			continue
		}
		en := &fakeEcNode{}
		go en.fakeTCPService(t, host)
	}

	return e
}
func (e *fakeEcNode) fakeTCPService(t *testing.T, host string) {
	l, err := net.Listen("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}

		//fmt.Printf("recive conn[local:%v remote:%v]\n", conn.LocalAddr(), conn.RemoteAddr())
		go e.fakeServiceHandler(conn, t)
	}
}
func (e *fakeEcNode) fakeServiceHandler(conn net.Conn, t *testing.T) {
	defer conn.Close()
	request := repl.NewPacket(context.Background())
	if _, err := request.ReadFromConnFromCli(conn, proto.NoReadDeadlineTime); err != nil {
		//fmt.Println(err)
		return
	}
	switch request.Opcode {
	case proto.OpEcTinyDelete, proto.OpCreateExtent:
		request.PacketOkReply()
	default:
		t.Logf("unkown Opcode :%v", request.Opcode)
	}
	if err := request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		t.Fatal(err)
	}
}

func (e *fakeEcNode) fakeCreateExtent(ep *EcPartition, t *testing.T, extentId uint64) error {
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:        proto.ProtoMagic,
			ReqID:        proto.GenerateRequestID(),
			Opcode:       proto.OpCreateExtent,
			PartitionID:  ep.PartitionID,
			ExtentOffset: int64(ep.EcMaxUnitSize) * int64(ep.EcDataNum),
			StartT:       time.Now().UnixNano(),
			ExtentID:     extentId,
		},
	}

	opCode, err, msg := e.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	if opCode != proto.OpOk {
		return errors.NewErrorf("fakeCreateExtent fail %v", msg)
	}

	if p.ExtentID != extentId {
		return errors.NewErrorf("fakeCreateExtent fail, error not set ExtentId")
	}
	return nil
}

func (e *fakeEcNode) fakeCreateECPartition(t *testing.T, partitionId uint64) (ep *EcPartition) {
	req := &proto.CreateEcPartitionRequest{
		PartitionID:   partitionId,
		PartitionSize: uint64(5 * unit.GB), // 5GB
		VolumeID:      "testVol",
		EcMaxUnitSize: 32 * unit.MB, // 32 MB
		DataNodeNum:   uint32(4),
		ParityNodeNum: uint32(2),
		Hosts:         e.Hosts,
	}

	task := proto.NewAdminTask(proto.OpCreateEcDataPartition, e.Hosts[0], req)
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateEcDataPartition,
			PartitionID: partitionId,
			Data:        body,
			Size:        uint32(len(body)),
			StartT:      time.Now().UnixNano(),
		},
	}

	opCode, err, msg := e.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}
	return e.space.Partition(partitionId)
}

func (e *fakeEcNode) fakeDeleteECPartition(t *testing.T, partitionId uint64) {
	req := &proto.DeleteEcPartitionRequest{
		PartitionId: partitionId,
	}

	task := proto.NewAdminTask(proto.OpDeleteEcDataPartition, e.Hosts[0], req)
	body, err := json.Marshal(task)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpDeleteEcDataPartition,
			PartitionID: partitionId,
			Data:        body,
			Size:        uint32(len(body)),
			StartT:      time.Now().UnixNano(),
		},
	}
	if err != nil {
		t.Fatal(err)
		return
	}
	e.handlePacketToDeleteEcPartition(p)
	if p.ResultCode != proto.OpOk {
		t.Fatalf("delete partiotion failed msg[%v]", p.ResultCode)
	}

}

func (e *fakeEcNode) prepareTestData(t *testing.T, ep *EcPartition, extentId uint64) (crc uint32, err error) {
	size := int(ep.EcMaxUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}

	crc = crc32.ChecksumIEEE(data)
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpEcWrite,
			PartitionID: ep.PartitionID,
			ExtentID:    extentId,
			Size:        uint32(size),
			CRC:         crc,
			Data:        data,
			StartT:      time.Now().UnixNano(),
		},
	}

	opCode, err, msg := e.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}
	return
}

func TestEcNode_handlePacketToCreateExtent(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)

	ep.disk.Status = proto.Unavailable
	p := FakeCreatePacket(ep, proto.OpCreateExtent, fakeNode.fakeNormalExtentId)
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Error(err)
	}
	if opCode != proto.OpIntraGroupNetErr {
		t.Errorf("disk unavailable test createExtent err[%v]", msg)
	}

	ep.disk.Status = proto.ReadWrite
	p = FakeCreatePacket(ep, proto.OpCreateExtent, fakeNode.fakeNormalExtentId)
	opCode, err, msg = fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Error(err)
		return
	}
	if opCode != proto.OpOk {
		t.Errorf("handlePacketToCreateExtent fail, error msg:%v", msg)
	}
}

func TestEcNode_handleMarkDeletePacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	p := FakeCreatePacket(ep, proto.OpMarkDelete, fakeNode.fakeNormalExtentId)
	p.ExtentType = proto.NormalExtentType
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Error(err)
	}
	if opCode != proto.OpOk {
		t.Errorf("handleEcMarkDeletePacket fail, error msg:%v", msg)
	}

	p = FakeCreatePacket(ep, proto.OpEcTinyDelete, fakeNode.fakeTinyExtentId)
	p.ExtentType = proto.TinyExtentType
	ext := new(proto.TinyExtentDeleteRecord)
	ext.ExtentId = fakeNode.fakeTinyExtentId
	ext.ExtentOffset = 0
	ext.PartitionId = ep.PartitionID
	ext.Size = uint32(ep.EcMaxUnitSize)
	p.Data, _ = json.Marshal(ext)
	p.Size = uint32(len(p.Data))
	_, err, msg = fakeNode.FakeOperateHandle(t, p)
	errStr := fmt.Sprintf("%v", msg)
	if strings.Contains(errStr, "no enough data to reconstruct") {
		t.Fatalf("err[%v]", msg)
	}
}

func handleEcBatchTinyExtentDelete(t *testing.T, ep *EcPartition) {
	for i := uint64(2); i <= 4; i++ {
		if err := fakeNode.fakeCreateExtent(ep, t, fakeNode.fakeTinyExtentId+i); err != nil {
			t.Fatal(err)
			return
		}
		if _, err := fakeNode.prepareTestData(t, ep, fakeNode.fakeTinyExtentId+i); err != nil {
			t.Fatal(err)
			return
		}
	}
	p := FakeCreatePacket(ep, proto.OpBatchDeleteExtent, fakeNode.fakeTinyExtentId)
	p.ExtentType = proto.NormalExtentType
	exts := make([]proto.ExtentKey, 0)
	for i := uint64(2); i <= 4; i++ {
		var ext proto.ExtentKey
		ext.ExtentId = fakeNode.fakeTinyExtentId + i
		ext.ExtentOffset = 0
		ext.Size = 1024
		exts = append(exts, ext)
	}
	p.Data, _ = json.Marshal(exts)
	p.Size = uint32(len(p.Data))
	p.CRC = crc32.ChecksumIEEE(p.Data)
	_, err, _ := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
}

func handleEcBatchNormalExtentDelete(t *testing.T, ep *EcPartition) {
	for i := uint64(0); i < 3; i++ {
		if err := fakeNode.fakeCreateExtent(ep, t, fakeNode.fakeNormalExtentId+i); err != nil {
			t.Fatal(err)
			return
		}
	}
	p := FakeCreatePacket(ep, proto.OpBatchDeleteExtent, fakeNode.fakeNormalExtentId)
	p.ExtentType = proto.NormalExtentType
	exts := make([]proto.ExtentKey, 0)
	for i := uint64(0); i < 3; i++ {
		var ext proto.ExtentKey
		ext.ExtentId = fakeNode.fakeNormalExtentId + i
		exts = append(exts, ext)
	}
	p.Data, _ = json.Marshal(exts)
	p.Size = uint32(len(p.Data))
	p.CRC = crc32.ChecksumIEEE(p.Data)
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleEcMarkDeletePacket err [%v]", msg)
	}
}

func TestEcNode_handleEcBatchMarkDeletePacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	handleEcBatchNormalExtentDelete(t, ep)
	handleEcBatchTinyExtentDelete(t, ep)
}

func testHandleWrite(t *testing.T, ep *EcPartition, extentId uint64, partitionStatus int) {
	ep.disk.Status = partitionStatus
	p := FakeCreatePacket(ep, proto.OpEcWrite, extentId)
	size := int(ep.EcMaxUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}
	p.Data = data
	p.CRC = crc32.ChecksumIEEE(p.Data)
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Error(err)
	}
	if partitionStatus == proto.ReadOnly && opCode != proto.OpDiskNoSpaceErr {
		t.Errorf(" disk readOnly testHandleWrite err[%v] opCode[%v]", msg, opCode)
	}
	if partitionStatus == proto.Unavailable && opCode != proto.OpIntraGroupNetErr {
		t.Errorf("disk unavailable testHandleWrite err[%v]", msg)
	}

	if partitionStatus == proto.ReadWrite && opCode != proto.OpOk {
		t.Errorf(" disk readWrite testHandleWrite err[%v] opCode[%v]", msg, opCode)
	}
}

func TestEcNode_handleWritePacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	testHandleWrite(t, ep, fakeNode.fakeNormalExtentId, proto.ReadOnly)
	testHandleWrite(t, ep, fakeNode.fakeNormalExtentId, proto.Unavailable)
	testHandleWrite(t, ep, fakeNode.fakeNormalExtentId, proto.ReadWrite)
	testHandleWrite(t, ep, fakeNode.fakeTinyExtentId, proto.ReadWrite)
}

func TestEcNode_handleRepairWrite(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	size := int(ep.EcMaxUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}
	newData := &extentData{
		OriginExtentSize: ep.EcMaxUnitSize * uint64(ep.EcDataNum),
		RepairFlag:       proto.DataRepair,
		Data:             data,
	}
	p := FakeCreatePacket(ep, proto.OpNotifyReplicasToRepair, fakeNode.fakeNormalExtentId)
	p.ExtentType = proto.NormalExtentType
	p.Data, _ = json.Marshal(newData)
	p.Size = uint32(len(p.Data))
	p.CRC = crc32.ChecksumIEEE(p.Data)

	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("repair write err[%v]", msg)
	}

}

func TestEcNode_handleReadPacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	beforeCrc, err := fakeNode.prepareTestData(t, ep, fakeNode.fakeNormalExtentId)
	if err != nil {
		t.Fatalf("perpare data err[%v]", err)
	}
	p := FakeCreatePacket(ep, proto.OpEcRead, fakeNode.fakeNormalExtentId)

	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", msg)
	}
	if p.Size != 0 && p.CRC != beforeCrc {
		t.Fatalf("handleReadPacket crc is not same")
	}

	p = FakeCreatePacket(ep, proto.OpEcRead, fakeNode.fakeNormalExtentId)
	p.Size = uint32(ep.EcMaxUnitSize) + uint32(10)
	opCode, err, msg = fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", msg)
	}
	if len(p.Data) != int(p.Size) {
		t.Fatalf("handleReadPacket size err[%v]", len(p.Data))
	}

}

func TestEcNode_handleChangeMember(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ep.ecNode.localServerAddr = ep.Hosts[0]
	failChangeMemberTest(ep, t)
	normalChangeMemberTest(ep, t)
}

func TestEcNode_handleUpdateEcPartition(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	request := proto.ChangeEcPartitionMembersRequest{
		PartitionId: partitionIdNum,
		Hosts:       ep.Hosts,
	}
	task := proto.NewAdminTask(proto.OpChangeEcPartitionMembers, ep.Hosts[0], request)
	data, _ := json.Marshal(task)
	p := FakeCreatePacket(ep, proto.OpUpdateEcDataPartition, fakeNode.fakeNormalExtentId)
	p.Data = data
	p.Size = uint32(len(data))

	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleUpdateEcPartition fail, error msg:%v", msg)
	}
}

func TestEcNode_handleGetAllWatermarks(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	p := FakeCreatePacket(ep, proto.OpGetAllWatermarks, fakeNode.fakeNormalExtentId)
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleGetAllWatermarks fail, error msg:%v", msg)
	}

}

func TestEcNode_handleHeartBeatPacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	request := &proto.HeartBeatRequest{
		CurrTime: time.Now().Unix(),
	}
	task := proto.NewAdminTask(proto.OpEcNodeHeartbeat, ep.Hosts[0], request)
	data, _ := json.Marshal(task)
	crc := crc32.ChecksumIEEE(data)
	p := FakeCreatePacket(ep, proto.OpEcNodeHeartbeat, fakeNode.fakeNormalExtentId)
	p.Data = data
	p.Size = uint32(len(data))
	p.CRC = crc
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("handleHeartbeatPacket fail, error msg:%v", msg)
	}
}

func TestEcNode_handleTinyRepairRead(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	beforeCrc, err := fakeNode.prepareTestData(t, ep, fakeNode.fakeTinyExtentId)
	if err != nil {
		t.Fatalf("perpare data err[%v]", err)
	}
	// read
	p := FakeCreatePacket(ep, proto.OpEcTinyRepairRead, fakeNode.fakeTinyExtentId)
	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode != proto.OpOk {
		t.Fatalf("test handleTinyRepairRead err[%v]", msg)
	}

	if p.Size != 0 && p.CRC != beforeCrc {
		t.Fatalf("handleTinyRepairRead crc is not same")
	}
}

func TestEcNode_handleStreamReadPacket(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	// read
	p := FakeCreatePacket(ep, proto.OpStreamEcRead, fakeNode.fakeNormalExtentId)

	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if opCode == proto.OpIntraGroupNetErr {
		t.Logf("test handleStreamReadPacket success")
	} else {
		t.Fatalf("handleStreamReadPacket fail, error msg:%v", msg)
	}
	AdjustEcHostsForTest(ep)
	p1 := FakeCreatePacket(ep, proto.OpStreamEcRead, fakeNode.fakeNormalExtentId)
	opCode, _, msg = fakeNode.FakeOperateHandle(t, p1)
	if opCode != proto.OpOk {
		t.Fatalf("test handleStreamRead err[%v]", msg)
	}
}
func TestEcNode_repairReadStripeProcess(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	repairHost := ep.Hosts[1]
	AdjustEcHostsForTest(ep)
	ecStripe, _ := NewEcStripe(ep, ep.EcMaxUnitSize, fakeNode.fakeNormalExtentId)
	originExtentSize := ep.EcMaxUnitSize * uint64(ep.EcDataNum)
	_, _, err := repairReadStripeProcess(ecStripe, 0, originExtentSize, ep.EcMaxUnitSize, repairHost)
	errStr := fmt.Sprintf("%v", err)
	if !strings.Contains(errStr, "repairStripeData is empty") {
		t.Fatalf("err[%v]", err)
	}
}

func TestEcNode_checkDisk(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ecNode := fakeNode.EcNode
	disks := ecNode.space.GetDisks()
	ecNode.getEcScrubInfo()
	ep.MaxScrubDoneId = fakeNode.fakeNormalExtentId //scrub done test
	for _, d := range disks {
		d.checkDiskStatus()
		if d.Status == proto.Unavailable {
			t.Fatalf("disk[%v] Unavailable", d.Path)
		}
		d.ecCheckScrub()
		if ep.MaxScrubDoneId != fakeNode.fakeNormalExtentId {
			t.Fatalf("scrub process MaxScrubDoneId(%v) err", ep.MaxScrubDoneId)
		}
	}
}

func TestEcNode_httpApi(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	if err := fakeNode.fakeCreateExtent(ep, t, fakeNode.fakeNormalExtentId); err != nil {
		t.Fatal(err)
		return
	}
	arr := strings.Split(fakeNode.Hosts[0], ":")
	addr := arr[0] + ":8090"

	if err := getPartitions(addr, t); err != nil {
		t.Fatal(err)
	}

	if err := getExtent(addr, t); err != nil {
		t.Fatal(err)
	}

	if err := getStats(addr, t); err != nil {
		t.Fatal(err)
	}

	if err := releasePartitions(addr, t); err != nil {
		t.Fatal(err)
	}

	if err := setEcPartitionSize(addr, ep.PartitionID, t); err != nil {
		t.Fatal(err)
	}
}

func TestEcNode_updateMetrics(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ecNode := fakeNode.EcNode
	ecNode.space.updateMetrics()
	stats := ecNode.space.Stats()
	if stats.Used == 0 {
		t.Fatalf("test updateMetrics failed used[%v]", stats.Used)
	}
}

func TestEcPartition_PersistOriginTinyExtentDelete(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ep.ecNode.localServerAddr = ep.Hosts[0]
	normalPersistOriginTinyExtentDeleteTest(ep, t)
}

func TestEcNode_handlePacketToReadOriginTinyDelRecord(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	p := FakeCreatePacket(ep, proto.OpEcOriginTinyDelInfoRead, fakeNode.fakeTinyExtentId)
	holes := []*proto.TinyExtentHole{}
	ep.originTinyExtentDeleteMap[fakeNode.fakeTinyExtentId] = holes
	_, err, _ := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEcNode_handleRepairOriginTinyDelInfo(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	p := FakeCreatePacket(ep, proto.OpNotifyEcRepairOriginTinyDelInfo, fakeNode.fakeTinyExtentId)
	_, err, _ := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
	task := &tinyFileRepairData{
		MaxTinyDelCount: 1,
		NormalHost:      ep.Hosts[0],
	}
	p.Data, _ = json.Marshal(task)
	p.Size = uint32(len(p.Data))
	_, err, _ = fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEcNode_ServerFunc(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
		fakeNode = nil
	}()
	partitionIdNum++
	if ep := fakeNode.fakeCreateECPartition(t, partitionIdNum); ep == nil {
		t.Fatalf("create EcPartition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	fakeNode.EcNode.Shutdown()
	fakeNode.EcNode.Sync()

	_, noClose := <-fakeNode.EcNode.stopC
	if noClose {
		t.Fatalf("ecNode stop err")
	}
}

type httpRespInfo struct {
	Code int32       `json:"code"`
	Data interface{} `json:"data"`
}

func normalPersistOriginTinyExtentDeleteTest(ep *EcPartition, t *testing.T) {
	AdjustEcHostsForTest(ep)
	request := []*proto.TinyExtentHole{
		{Offset: 1, Size: 1, PreAllSize: 1},
	}
	ep.originTinyExtentDeleteMap[fakeNode.fakeNormalExtentId] = request
	data, _ := json.Marshal(request)
	p := FakeCreatePacket(ep, proto.OpPersistTinyExtentDelete, fakeNode.fakeNormalExtentId)
	p.Data = data
	p.Size = uint32(len(data))
	opCode, _, msg := fakeNode.FakeOperateHandle(t, p)
	if opCode != proto.OpOk {
		t.Fatalf("OpPersistTinyExtentDelete fail, error msg:%v", msg)
	}

	request = []*proto.TinyExtentHole{
		{Offset: 1, Size: 1, PreAllSize: 1},
		{Offset: 1, Size: 1, PreAllSize: 1},
	}
	data, _ = json.Marshal(request)
	p.Data = data
	p.Size = uint32(len(data))
	opCode, _, msg = fakeNode.FakeOperateHandle(t, p)
	if opCode != proto.OpOk {
		t.Fatalf("OpPersistTinyExtentDelete fail, error msg:%v", msg)
	}
}

func normalChangeMemberTest(ep *EcPartition, t *testing.T) {
	AdjustEcHostsForTest(ep)
	request := proto.ChangeEcPartitionMembersRequest{
		PartitionId: partitionIdNum,
		Hosts:       ep.Hosts,
	}

	task := proto.NewAdminTask(proto.OpChangeEcPartitionMembers, ep.Hosts[0], request)
	data, _ := json.Marshal(task)
	p := FakeCreatePacket(ep, proto.OpChangeEcPartitionMembers, fakeNode.fakeNormalExtentId)
	p.Data = data
	p.Size = uint32(len(data))

	opCode, _, msg := fakeNode.FakeOperateHandle(t, p)
	if opCode != proto.OpOk {
		t.Fatalf("handleChangeMember fail, error msg:%v", msg)
	}
}

func failChangeMemberTest(ep *EcPartition, t *testing.T) {
	request := proto.ChangeEcPartitionMembersRequest{
		PartitionId: partitionIdNum,
		Hosts:       ep.Hosts,
	}

	task := proto.NewAdminTask(proto.OpChangeEcPartitionMembers, ep.Hosts[0], request)
	data, _ := json.Marshal(task)
	p := FakeCreatePacket(ep, proto.OpChangeEcPartitionMembers, fakeNode.fakeNormalExtentId)
	p.Data = data
	p.Size = uint32(len(data))

	opCode, _, msg := fakeNode.FakeOperateHandle(t, p)
	if opCode != proto.OpIntraGroupNetErr {
		t.Errorf("handleChangeMember fail, error msg:%v", msg)
	}
}

func getHttpRequestResp(url string, t *testing.T) (body []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http getPartitions error")
		return
	}
	body, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return
}

func getPartitions(addr string, t *testing.T) (err error) {
	partitionsUrl := fmt.Sprintf("http://%s/partitions", addr)
	reply := &httpRespInfo{}
	body, err := getHttpRequestResp(partitionsUrl, t)
	err = json.Unmarshal(body, reply)
	if err != nil {
		t.Fatalf("Unmarshal err[%v]", err)
	}
	partitionsMap := reply.Data.(map[string]interface{})
	partitionCount := partitionsMap["partitionCount"].(float64)
	if partitionCount != 1 {
		t.Fatalf("get partitions err")
	}
	return
}

func getExtent(addr string, t *testing.T) (err error) {
	var (
		body    []byte
		errbody []byte
	)
	errExtentUrl := fmt.Sprintf("http://%s/extent?partitionID=%d&extentID=%d", addr, partitionIdNum+1, fakeNode.fakeNormalExtentId)
	extentUrl := fmt.Sprintf("http://%s/extent?partitionID=%d&extentID=%d", addr, partitionIdNum, fakeNode.fakeNormalExtentId)
	reply := &httpRespInfo{}
	errReply := &httpRespInfo{}
	errbody, err = getHttpRequestResp(errExtentUrl, t)
	if err != nil {
		t.Fatal(err)
	}

	body, err = getHttpRequestResp(extentUrl, t)
	if err != nil {
		t.Fatalf("http getExtent error")
	}
	err = json.Unmarshal(errbody, errReply)
	if err != nil {
		t.Fatal(err)
	}
	err = json.Unmarshal(body, reply)
	if err != nil {
		t.Fatalf("Unmarshal err[%v]", err)
	}

	if errReply.Code != http.StatusNotFound {
		t.Fatalf("test get err")
	}

	extentMap := reply.Data.(map[string]interface{})
	if extentMap["fileId"].(float64) != float64(fakeNode.fakeNormalExtentId) {
		t.Fatalf(" get extent err")
	}
	return
}

func getStats(addr string, t *testing.T) (err error) {
	var body []byte
	statsUrl := fmt.Sprintf("http://%s/stats", addr)
	reply := &httpRespInfo{}
	body, err = getHttpRequestResp(statsUrl, t)
	if err != nil {
		t.Fatalf("http getStats error")
	}
	err = json.Unmarshal(body, reply)
	if err != nil {
		t.Fatalf("Unmarshal err[%v]", err)
	}
	statsMap := reply.Data.(map[string]interface{})
	if statsMap["Status"].(float64) != proto.TaskSucceeds {
		t.Fatalf("get stats err")
	}
	return
}

func releasePartitions(addr string, t *testing.T) (err error) {
	key := generateAuthKey()
	releasePartitionsUrl := fmt.Sprintf("http://%s/releasePartitions?key=%s", addr, key)
	_, err = getHttpRequestResp(releasePartitionsUrl, t)
	if err != nil {
		t.Fatalf("http releasePartitions error")
	}
	return
}

func setEcPartitionSize(addr string, partitionId uint64, t *testing.T) (err error) {
	url := fmt.Sprintf("http://%s/setEcPartitionSize?partitionId=%v&size=%v", addr, partitionId, 1*unit.GB)
	_, err = getHttpRequestResp(url, t)
	if err != nil {
		t.Fatalf("http setEcPartitionSize error")
	}
	return
}

func PrepareEcPartition(extentCreate, dataPrepare bool, t *testing.T, partitionId uint64) *EcPartition {
	ep := fakeNode.fakeCreateECPartition(t, partitionId)
	if ep == nil {
		t.Fatalf("create EcPartition failed")
		return nil
	}

	if !extentCreate {
		return ep
	}

	if err := fakeNode.fakeCreateExtent(ep, t, fakeNode.fakeNormalExtentId); err != nil {
		t.Fatal(err)
		return nil
	}

	if err := fakeNode.fakeCreateExtent(ep, t, fakeNode.fakeTinyExtentId); err != nil {
		t.Fatal(err)
		return nil
	}

	if !dataPrepare {
		return ep
	}

	if _, err := fakeNode.prepareTestData(t, ep, fakeNode.fakeNormalExtentId); err != nil {
		return nil
	}

	if _, err := fakeNode.prepareTestData(t, ep, fakeNode.fakeTinyExtentId); err != nil {
		return nil
	}

	return ep
}

func FakeDirCreate(t *testing.T) (err error) {
	_, err = os.Stat(testDiskPath)
	if err == nil {
		os.RemoveAll(testDiskPath)
	}
	if err = os.MkdirAll(testDiskPath, 0766); err != nil {
		t.Fatalf("mkdir err[%v]", err)
	}
	dataPath := path.Join(testDiskPath, fmt.Sprintf(EcPartitionPrefix+"_%v_%v", 0, 1024))
	_, err = os.Stat(dataPath)
	if err == nil {
		os.RemoveAll(dataPath)
	}
	if err = os.MkdirAll(dataPath, 0766); err != nil {
		t.Fatalf("mkdir err[%v]", err)
	}
	return
}

func FakeNodePrepare(t *testing.T) (err error) {
	if err = FakeDirCreate(t); err != nil {
		t.Fatalf("mkdir err[%v]", err)
		return
	}
	if fakeNode == nil {
		fakeNode = newFakeEcNode(t)
	}
	return
}

func FakeCreatePacket(ep *EcPartition, opCode uint8, extentId uint64) (packet *repl.Packet) {
	packet = &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      opCode,
			PartitionID: partitionIdNum,
			ExtentID:    extentId,
			Size:        uint32(ep.EcMaxUnitSize),
			StartT:      time.Now().UnixNano(),
		},
	}
	return
}

func (e *fakeEcNode) FakeOperateHandle(t *testing.T, p *repl.Packet) (opCode uint8, err error, msg string) {
	err = e.Prepare(p, e.Hosts[0])
	if err != nil {
		t.Errorf("prepare err %v", err)
		return
	}

	conn, err := net.Dial("tcp", e.Hosts[0])
	if err != nil {
		return
	}

	defer conn.Close()
	err = e.OperatePacket(p, conn.(*net.TCPConn))
	if err != nil {
		msg = fmt.Sprintf("%v", err)
	}

	err = e.Post(p)
	if err != nil {
		return
	}
	opCode = p.ResultCode
	return
}
func AdjustEcHostsForTest(ep *EcPartition) {
	for i := 1; i < len(ep.Hosts); i++ {
		ep.Hosts[i] = ep.Hosts[0]
	}
}

func buildJSONResp(w http.ResponseWriter, stateCode int, data interface{}, send, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(stateCode)
	w.Header().Set("Content-Type", "application/json")
	code := 200
	if send == Master {
		code = 0
	}
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func runMasterHttp(t *testing.T) {
	if !startMasterHttp {
		profNetListener, err := net.Listen("tcp", fmt.Sprintf(":%v", 6666))
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			_ = http.Serve(profNetListener, http.DefaultServeMux)
		}()
		go func() {
			http.HandleFunc(proto.AdminGetIP, fakeGetClusterInfo)
			http.HandleFunc(proto.AdminClusterGetScrub, fakeGetEcScrubInfo)
			http.HandleFunc(proto.AddEcNode, fakeAddEcNode)
			http.HandleFunc(proto.AdminGetEcPartition, fakeGetEcPartition)
			http.HandleFunc(proto.GetEcNode, fakeGetEcNode)
		}()
		startMasterHttp = true
	}
}

func fakeGetClusterInfo(w http.ResponseWriter, r *http.Request) {
	info := &proto.ClusterInfo{
		Cluster: "chubaofs",
		Ip:      "127.0.0.1",
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetEcPartition(w http.ResponseWriter, r *http.Request) {
	partition := &proto.DataPartitionInfo{
		PartitionID: partitionIdNum,
		Hosts:       fakeNode.Hosts,
	}
	info := &proto.EcPartitionInfo{
		DataPartitionInfo: partition,
		ParityUnitsNum:    4,
		DataUnitsNum:      2,
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeAddEcNode(w http.ResponseWriter, r *http.Request) {
	buildJSONResp(w, http.StatusOK, 1, Master, "")
}

func fakeGetEcNode(w http.ResponseWriter, r *http.Request) {
	ecNode := &proto.EcNodeInfo{}
	buildJSONResp(w, http.StatusOK, ecNode, Master, "")
}

func fakeGetEcScrubInfo(w http.ResponseWriter, r *http.Request) {
	scrubInfo := &proto.UpdateEcScrubInfoRequest{
		ScrubEnable:     true,
		MaxScrubExtents: 3,
		ScrubPeriod:     0,
	}

	scrubInfo.StartScrubTime = time.Now().Unix()
	buildJSONResp(w, http.StatusOK, scrubInfo, Master, "")
}
