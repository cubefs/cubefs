package datanode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/unit"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

var (
	partitionIdNum uint64
)

type fakeDataNode struct {
	DataNode
	fakeNormalExtentId uint64
	fakeTinyExtentId   uint64
}

func Test_getTinyExtentHoleInfo(t *testing.T) {
	partitionIdNum++
	dp := PrepareDataPartition(true, true, t, partitionIdNum)
	if dp == nil {
		t.Fatalf("prepare data partition failed")
		return
	}
	defer fakeNode.fakeDeleteDataPartition(t, partitionIdNum)
	if _, err := dp.getTinyExtentHoleInfo(fakeNode.fakeTinyExtentId); err != nil {
		t.Fatalf("getHttpRequestResp err(%v)", err)
	}
}

func Test_handleTinyExtentAvaliRead(t *testing.T) {
	partitionIdNum++
	dp := PrepareDataPartition(true, true, t, partitionIdNum)
	if dp == nil {
		t.Fatalf("prepare data partition failed")
		return
	}
	defer fakeNode.fakeDeleteDataPartition(t, partitionIdNum)
	p := repl.NewTinyExtentRepairReadPacket(context.Background(), partitionIdNum, fakeNode.fakeTinyExtentId, 0, 10)
	opCode, err, msg := fakeNode.operateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}

}

func (fdn *fakeDataNode) fakeDeleteDataPartition(t *testing.T, partitionId uint64) {
	req := &proto.DeleteDataPartitionRequest{
		PartitionId: partitionId,
	}

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, localNodeAddress, req)
	body, err := json.Marshal(task)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpDeleteDataPartition,
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
	fdn.handlePacketToDeleteDataPartition(p)
	if p.ResultCode != proto.OpOk {
		t.Fatalf("delete partiotion failed msg[%v]", p.ResultCode)
	}

}

func PrepareDataPartition(extentCreate, dataPrepare bool, t *testing.T, partitionId uint64) *DataPartition {
	dp := fakeNode.fakeCreateDataPartition(t, partitionId)
	if dp == nil {
		t.Fatalf("create Partition failed")
		return nil
	}

	if !extentCreate {
		return dp
	}

	if err := fakeNode.fakeCreateExtent(dp, t, fakeNode.fakeNormalExtentId); err != nil {
		t.Fatal(err)
		return nil
	}

	if err := fakeNode.fakeCreateExtent(dp, t, fakeNode.fakeTinyExtentId); err != nil {
		t.Fatal(err)
		return nil
	}

	if !dataPrepare {
		return dp
	}

	if _, err := fakeNode.prepareTestData(t, dp, fakeNode.fakeNormalExtentId); err != nil {
		return nil
	}

	if _, err := fakeNode.prepareTestData(t, dp, fakeNode.fakeTinyExtentId); err != nil {
		return nil
	}
	dp.ReloadSnapshot()
	return dp
}

func (fdn *fakeDataNode) fakeCreateDataPartition(t *testing.T, partitionId uint64) (dp *DataPartition) {
	req := &proto.CreateDataPartitionRequest{
		PartitionId:   partitionId,
		PartitionSize: 5 * unit.GB, // 5GB
		VolumeId:      "testVol",
		Hosts: []string{
			localNodeAddress,
			localNodeAddress,
			localNodeAddress,
		},
		Members: []proto.Peer{
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
		},
	}

	task := proto.NewAdminTask(proto.OpCreateDataPartition, localNodeAddress, req)
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateDataPartition,
			PartitionID: partitionId,
			Data:        body,
			Size:        uint32(len(body)),
			StartT:      time.Now().UnixNano(),
		},
	}

	opCode, err, msg := fdn.operateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}
	return fdn.space.Partition(partitionId)
}

func (e *fakeDataNode) fakeCreateExtent(dp *DataPartition, t *testing.T, extentId uint64) error {
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateExtent,
			PartitionID: dp.partitionID,
			StartT:      time.Now().UnixNano(),
			ExtentID:    extentId,
		},
	}

	opCode, err, msg := e.operateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	if opCode != proto.OpOk && !strings.Contains(msg, "extent already exists") {
		return errors.NewErrorf("fakeCreateExtent fail %v", msg)
	}

	if p.ExtentID != extentId {
		return errors.NewErrorf("fakeCreateExtent fail, error not set ExtentId")
	}
	return nil
}

func (fdn *fakeDataNode) prepareTestData(t *testing.T, dp *DataPartition, extentId uint64) (crc uint32, err error) {
	size := 1 * unit.MB
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = 1
	}

	crc = crc32.ChecksumIEEE(bytes)
	p := &repl.Packet{
		Object: dp,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpWrite,
			PartitionID: dp.partitionID,
			ExtentID:    extentId,
			Size:        uint32(size),
			CRC:         crc,
			Data:        bytes,
			StartT:      time.Now().UnixNano(),
		},
	}

	opCode, err, msg := fdn.operateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}
	return
}

func (fdn *fakeDataNode) operateHandle(t *testing.T, p *repl.Packet) (opCode uint8, err error, msg string) {
	err = fdn.Prepare(p, fdn.localServerAddr)
	if err != nil {
		t.Errorf("prepare err %v", err)
		return
	}

	conn, err := net.Dial("tcp", localNodeAddress)
	if err != nil {
		return
	}

	defer conn.Close()
	err = fdn.OperatePacket(p, conn.(*net.TCPConn))
	if err != nil {
		msg = fmt.Sprintf("%v", err)
	}

	err = fdn.Post(p)
	if err != nil {
		return
	}
	opCode = p.ResultCode
	return
}

func getHttpRequestResp(url string, t *testing.T) (body []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http getPartitions error(%v)", err)
		return
	}
	body, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return
}
