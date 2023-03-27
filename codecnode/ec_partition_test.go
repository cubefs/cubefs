package codecnode

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"hash/crc32"
	"net"
	"testing"
)

func TestEcPartition_NewEcPartition(t *testing.T) {
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	/*
		if _, err := NewEcPartition(req, masters); err != nil {
			t.Fatalf("NewEcPartition success")
		}*/

	if _, err := NewEcPartition(req, masters); err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

}

func TestEcPartition_GetOriginExtentInfo(t *testing.T) {
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	ecp.ProfHosts = []string{
		"127.0.0.1:" + HttpPort,
	}
	if err = ecp.GetOriginExtentInfo(); err != nil {
		t.Fatalf("GetOriginExtentInfo fail, error :%v", err)
	}
	/*
		ecp.DataNodeIp = "1.1.1.1:9500"
		if err = ecp.GetOriginExtentInfo(); err == nil {
			t.Fatalf("GetOriginExtentInfo success")
		}*/
}

func TestEcPartition_CreateExtentForWrite(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{
		VolName:     "ltptest",
		PartitionId: 1,
		Hosts: []string{
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:" + TcpPort,
		},
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	if err = ecp.CreateExtentForWrite(context.Background(), 1025, 10); err != nil {
		t.Fatalf("CreateExtentForWrite fail, error :%v", err)
	}

	if err = ecp.CreateExtentForWrite(context.Background(), 0, 0); err == nil {
		t.Fatalf("CreateExtentForWrite success")
	}

	ecp.Hosts = []string{"127.0.0.1:2222", "127.0.0.1:" + TcpPort}
	if err = ecp.CreateExtentForWrite(context.Background(), 0, 0); err == nil {
		t.Fatalf("CreateExtentForWrite success")
	}
}

func TestEcPartition_Read(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{
		Hosts: []string{
			"127.0.0.1:" + TcpPort,
		},
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	needSize := 10
	inbuf := make([]byte, needSize)
	if readSize, err := ecp.Read(context.Background(), 1025, inbuf, 0, needSize); err == nil && readSize == needSize {
		t.Fatalf("Read success")
	}

	ecp.PartitionId = 1
	if readSize, err := ecp.Read(context.Background(), 1025, inbuf, 0, needSize); err != nil || readSize != needSize {
		t.Fatalf("Read fail, error :%v, readSize: %v", err, readSize)
	}

	if readSize, err := ecp.Read(context.Background(), 1026, inbuf, 0, needSize); err == nil && readSize == needSize {
		t.Fatalf("Read success")
	}

	if readSize, err := ecp.Read(context.Background(), 0, inbuf, 0, 0); err == nil && readSize == needSize {
		t.Fatalf("Read success")
	}
}

func TestEcPartition_Write(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{
		VolName:     "ltptest",
		PartitionId: 1,
		Hosts: []string{
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:2222",
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:" + TcpPort,
		},
		EcDataNum:   4,
		EcParityNum: 2,
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	if err = ecp.Write(context.Background(), nil, 0, 0); err == nil {
		t.Fatalf("Write success")
	}

	shards := [][]byte{
		{1}, {2}, {}, {}, {}, {},
	}

	if err = ecp.Write(context.Background(), shards, 1025, 0); err != nil {
		t.Fatalf("Write fail, error :%v", err)
	}

	if err = ecp.Write(context.Background(), shards, 0, 0); err == nil {
		t.Fatalf("Write success")
	}

}

func TestEcPartition_CalMisAlignMentShards(t *testing.T) {
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{
		EcDataNum:   4,
		EcParityNum: 2,
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	shards := [][]byte{
		{1}, {2}, {}, {}, {},
	}
	if err = ecp.CalMisAlignMentShards(shards, 1, 1); err == nil {
		t.Fatalf("CalMisAlignMentShards success")
	}

	shards = [][]byte{
		{1}, {2}, {}, {}, {}, {},
	}
	if err = ecp.CalMisAlignMentShards(shards, 1, 1); err != nil {
		t.Fatalf("CalMisAlignMentShards fail, error :%v", err)
	}
}

func TestEcPartition_GetTinyExtentHolesAndAvaliSize(t *testing.T) {
	runHttp(t)

	req := &proto.IssueMigrationTaskRequest{
		PartitionId: 1,
		EcDataNum:   4,
		EcParityNum: 2,
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}
	ecp.ProfHosts = []string{
		"127.0.0.1:" + HttpPort,
	}
	if _, _, err := ecp.GetTinyExtentHolesAndAvaliSize(1); err != nil {
		t.Fatalf("GetTinyExtentHolesAndAvaliSize fail, error :%v", err)
	}
}

func TestEcPartition_PersistHolesInfoToEcnode(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)
	req := &proto.IssueMigrationTaskRequest{
		VolName:     "ltptest",
		PartitionId: 1,
		Hosts: []string{
			"127.0.0.1:" + TcpPort,
			"127.0.0.1:" + TcpPort,
		},
	}
	masters := []string{
		"127.0.0.1:" + HttpPort,
	}

	ecp, err := NewEcPartition(req, masters)
	if err != nil {
		t.Fatalf("NewEcPartition fail, error :%v", err)
	}

	if err := ecp.PersistHolesInfoToEcnode(context.Background(), 1, nil); err != nil {
		t.Fatalf("PersistHolesInfoToEcnode fail, error :%v", err)
	}

	holes := []*proto.TinyExtentHole{
		{
			Offset:     1,
			Size:       1,
			PreAllSize: 1,
		},
	}
	if err := ecp.PersistHolesInfoToEcnode(context.Background(), 1025, holes); err != nil {
		t.Fatalf("PersistHolesInfoToEcnode fail, error :%v", err)
	}

	if err := ecp.PersistHolesInfoToEcnode(context.Background(), 1, holes); err == nil {
		t.Fatalf("PersistHolesInfoToEcnode fail, error :%v", err)
	}

	ecp.Hosts[1] = "127.0.0.1:" + "1234"
	if err := ecp.PersistHolesInfoToEcnode(context.Background(), 1, holes); err == nil {
		t.Fatalf("PersistHolesInfoToEcnode fail, error :%v", err)
	}
}

func fakeCreateExtent(e *fakeCodecServer, p *repl.Packet, conn net.Conn) {
	p.ResultCode = proto.OpErr
	if p.ExtentID == 1025 {
		p.ResultCode = proto.OpOk
	}
}

func fakeRead(e *fakeCodecServer, p *repl.Packet, conn net.Conn) {
	p.ResultCode = proto.OpErr
	if p.ExtentID == 1025 {
		p.Data = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		p.Size = uint32(len(p.Data))
		p.CRC = crc32.ChecksumIEEE(p.Data)
		p.ResultCode = proto.OpOk
	} else if p.ExtentID == 1026 {
		p.Data = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		p.Size = uint32(len(p.Data))
		p.ResultCode = proto.OpOk
	}
}

func fakeWrite(e *fakeCodecServer, p *repl.Packet, conn net.Conn) {
	p.ResultCode = proto.OpErr
	if p.ExtentID == 1025 {
		p.ResultCode = proto.OpOk
	}
}

func fakePersistHolesInfo(e *fakeCodecServer, p *repl.Packet, conn net.Conn) {
	p.ResultCode = proto.OpErr
	if p.ExtentID == 1025 {
		p.ResultCode = proto.OpOk
	}
}
