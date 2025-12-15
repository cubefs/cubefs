package stream

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"golang.org/x/time/rate"
)

// MockTCPServer handles requests from ExtentReader and RemoteCacheClient
type MockTCPServer struct {
	listener net.Listener
	addr     string
	dataA    []byte
	offsetA  uint64
	dataB    []byte
	offsetB  uint64
	fileSize uint64
}

func NewMockTCPServer(dataA []byte, offsetA uint64, dataB []byte, offsetB uint64, fileSize uint64) (*MockTCPServer, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &MockTCPServer{
		listener: l,
		addr:     l.Addr().String(),
		dataA:    dataA,
		offsetA:  offsetA,
		dataB:    dataB,
		offsetB:  offsetB,
		fileSize: fileSize,
	}, nil
}

func (s *MockTCPServer) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *MockTCPServer) Close() {
	s.listener.Close()
}

func (s *MockTCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		header := make([]byte, util.PacketHeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		p := &proto.Packet{}
		if err := p.UnmarshalHeader(header); err != nil {
			return
		}

		// Read extra data if any (ArgLen/Size)
		// For simplicity, we just read Size + ArgLen bytes if needed, but UnmarshalHeader doesn't read body
		// We need to read body based on p.Size and p.ArgLen
		dataLen := p.Size
		if p.Opcode == proto.OpRead || p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpStreamFollowerRead {
			dataLen = 0
		}

		totalLen := dataLen + p.ArgLen
		if totalLen > 0 {
			buf := make([]byte, totalLen)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}
			p.Arg = buf[:p.ArgLen]
			p.Data = buf[p.ArgLen:]
		}

		reply := &proto.Packet{
			ReqID:       p.ReqID,
			ResultCode:  proto.OpOk,
			Magic:       proto.ProtoMagic,
			Opcode:      p.Opcode,
			PartitionID: p.PartitionID,
			ExtentID:    p.ExtentID,
		}
		switch p.Opcode {
		case proto.OpRead, proto.OpStreamRead: // DataNode Read
			// p.KernelOffset is the file offset
			// Check if request falls into A
			if p.KernelOffset >= s.offsetA && p.KernelOffset+uint64(p.Size) <= s.offsetA+uint64(len(s.dataA)) {
				reply.Data = make([]byte, p.Size)
				copy(reply.Data, s.dataA[p.KernelOffset-s.offsetA:])
				reply.Size = uint32(len(reply.Data))
				reply.CRC = crc32.ChecksumIEEE(reply.Data)
			} else if p.KernelOffset >= s.offsetB && p.KernelOffset+uint64(p.Size) <= s.offsetB+uint64(len(s.dataB)) {
				reply.Data = make([]byte, p.Size)
				copy(reply.Data, s.dataB[p.KernelOffset-s.offsetB:])
				reply.Size = uint32(len(reply.Data))
				reply.CRC = crc32.ChecksumIEEE(reply.Data)
			} else {
				reply.ResultCode = proto.OpErr
			}

		case proto.OpFlashSDKHeartbeat:
			time.Sleep(time.Millisecond)
			reply.ResultCode = proto.OpOk

		case proto.OpFlashNodeCacheRead: // RemoteCache Read
			// Data contains marshaled CacheReadRequest
			req := &proto.CacheReadRequest{}
			if err := req.Unmarshal(p.Data); err != nil {
				reply.ResultCode = proto.OpErr
			} else {
				fullData := make([]byte, s.fileSize)
				for i := range fullData {
					fullData[i] = 0xEE
				}
				if uint64(len(fullData)) >= s.offsetA+uint64(len(s.dataA)) {
					copy(fullData[s.offsetA:s.offsetA+uint64(len(s.dataA))], s.dataA)
				}
				if uint64(len(fullData)) >= s.offsetB+uint64(len(s.dataB)) {
					copy(fullData[s.offsetB:s.offsetB+uint64(len(s.dataB))], s.dataB)
				}

				if req.Offset < uint64(len(fullData)) {
					// The requested offset is relative to the start of the cache block (1048576)
					// So if req.Offset is 10485, the file offset is 1048576 + 10485 = 1059061
					// We need to map req.Offset to fullData offset.
					// However, fullData holds the *whole file* content (simulated).
					// Wait, CacheReadRequest.Offset is relative to FixedFileOffset.
					// FixedFileOffset is 1048576 (Block 1).
					// So file offset = FixedFileOffset + req.Offset.

					// Let's get the absolute file offset
					fileOffset := req.CacheRequest.FixedFileOffset + req.Offset

					end := fileOffset + req.Size_
					if end > uint64(len(fullData)) {
						end = uint64(len(fullData))
					}

					reply.Data = make([]byte, req.Size_)
					if fileOffset < uint64(len(fullData)) {
						copy(reply.Data, fullData[fileOffset:end])
					}
				} else {
					reply.Data = make([]byte, req.Size_)
				}

				reply.Size = uint32(len(reply.Data))
				reply.CRC = crc32.ChecksumIEEE(reply.Data)
			}
		default:
			reply.ResultCode = proto.OpErr
		}

		// Write reply
		headerBuf := make([]byte, reply.CalcPacketHeaderSize())
		reply.MarshalHeader(headerBuf)
		if _, err := conn.Write(headerBuf); err != nil {
			return
		}
		if reply.ArgLen > 0 {
			if _, err := conn.Write(reply.Arg); err != nil {
				return
			}
		}
		if reply.Size > 0 {
			if _, err := conn.Write(reply.Data); err != nil {
				return
			}
		}
	}
}

func TestStreamerRead_WithHoles_Consistency(t *testing.T) {
	// 1. Prepare Data
	filesize := uint64(1153434)

	// Extent 1: 100-500
	offsetA := uint64(100)
	sizeA := 400
	mockDataA := make([]byte, sizeA)
	for i := range mockDataA {
		mockDataA[i] = 0xAA
	}

	// Extent 2: 1.01M - 1.02M
	// Use explicit integer offsets to ensure no float ambiguity
	offsetB := uint64(1059061) // approx 1.01 * 1024 * 1024
	sizeB := 10486             // approx 0.01 * 1024 * 1024
	mockDataB := make([]byte, sizeB)
	for i := range mockDataB {
		mockDataB[i] = 0xBB
	}

	// 2. Start Mock TCP Server
	tcpServer, err := NewMockTCPServer(mockDataA, offsetA, mockDataB, offsetB, filesize)
	if err != nil {
		t.Fatalf("Failed to start mock tcp server: %v", err)
	}
	defer tcpServer.Close()
	go tcpServer.Serve()

	// Compute slots for RemoteCache
	// Block 0: Offset 0
	slot0 := proto.ComputeCacheBlockSlot("testvol", 1, 0)
	// Block 1: Offset 1MB (1048576)
	slot1 := proto.ComputeCacheBlockSlot("testvol", 1, 1048576)

	// 3. Start Mock Master Server
	masterHandler := http.NewServeMux()
	masterHandler.HandleFunc(proto.ClientFlashGroups, func(w http.ResponseWriter, r *http.Request) {
		view := proto.FlashGroupView{
			Enable: true,
			FlashGroups: []*proto.FlashGroupInfo{
				{
					ID:    1,
					Hosts: []string{tcpServer.addr},
					Slot:  []uint32{slot0, slot1}, // Ensure both blocks are mapped
				},
			},
		}

		data, _ := json.Marshal(view)
		rsp := proto.HTTPReplyRaw{
			Code: proto.ErrCodeSuccess,
			Data: data,
		}
		rspData, _ := json.Marshal(rsp)
		w.Write(rspData)
	})
	masterHandler.HandleFunc(proto.AdminGetRemoteCacheConfig, func(w http.ResponseWriter, r *http.Request) {
		config := proto.RemoteCacheConfig{
			RemoteCacheTTL:         3600,
			RemoteCacheReadTimeout: 1000,
		}
		data, _ := json.Marshal(config)
		rsp := proto.HTTPReplyRaw{
			Code: proto.ErrCodeSuccess,
			Data: data,
		}
		rspData, _ := json.Marshal(rsp)
		w.Write(rspData)
	})

	mockMaster := httptest.NewServer(masterHandler)
	defer mockMaster.Close()

	// 4. Setup ExtentClient
	masters := []string{strings.TrimPrefix(mockMaster.URL, "http://")}

	// Create wrapper manually
	w := &wrapper.Wrapper{
		HostsStatus: make(map[string]bool),
	}
	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 1,
			Hosts:       []string{tcpServer.addr},
			LeaderAddr:  tcpServer.addr,
			Status:      proto.ReadWrite,
			MediaType:   proto.MediaType_HDD,
		},
	}
	wrapper.InsertPartitionForTest(w, dp)
	dp.ClientWrapper = w
	w.InitFollowerRead(false)

	client := &ExtentClient{
		dataWrapper:  w,
		LimitManager: manager.NewLimitManager(nil),
		readLimiter:  rate.NewLimiter(rate.Inf, 128),
		streamers:    make(map[uint64]*Streamer),
		extentConfig: &ExtentConfig{
			Volume:  "testvol",
			Masters: masters,
		},
		metaWrapper: nil,
		multiVerMgr: &MultiVerMgr{verList: &proto.VolVersionInfoList{}},
	}
	client.dataWrapper.HostsStatus[tcpServer.addr] = true
	client.LimitManager.WrapperUpdate = func(clientInfo wrapper.SimpleClientInfo) (bWork bool, err error) { return }

	// Mock getInodeInfo
	client.getInodeInfo = func(ino uint64) (*proto.InodeInfo, error) {
		return &proto.InodeInfo{
			Inode:        ino,
			Size:         filesize,
			StorageClass: proto.StorageClass_Replica_HDD,
		}, nil
	}

	// 5. Initialize RemoteCache
	client.RemoteCache.Init(client)
	client.RemoteCache.remoteCacheClient.SameZoneTimeout = 1000000
	client.RemoteCache.remoteCacheClient.SameRegionTimeout = 1000

	time.Sleep(100 * time.Millisecond)
	client.RemoteCache.VolumeEnabled = true
	client.RemoteCache.remoteCacheMaxFileSizeGB = 100
	client.RemoteCache.remoteCacheClient.SetClusterEnable(true)

	if err := client.RemoteCache.remoteCacheClient.UpdateFlashGroups(); err != nil {
		t.Fatalf("UpdateFlashGroups failed: %v", err)
	}

	// 6. Create Streamer and Extents
	extents := []proto.ExtentKey{
		{FileOffset: offsetA, Size: uint32(sizeA), PartitionId: 1, ExtentId: 1},
		{FileOffset: offsetB, Size: uint32(sizeB), PartitionId: 1, ExtentId: 2},
	}
	getExtents := func(inode uint64, isCache bool, openForWrite bool, isMigration bool) (uint64, uint64, []proto.ExtentKey, error) {
		return 0, filesize, extents, nil
	}
	client.getExtents = getExtents

	s := NewStreamer(client, 1, false, false, "/test/file")
	// Pre-fill extents
	s.extents.RefreshForce(1, true, getExtents, false, false, false)

	// 7. Verify Data (Data Node - RemoteCache Disabled)
	s.client.forceRemoteCache = true
	client.RemoteCache.Path = "/"

	// Prepare buffer
	data := make([]byte, filesize)
	for i := range data {
		data[i] = 0xFF
	}

	// Expected Result
	expected := make([]byte, filesize)
	copy(expected[offsetA:offsetA+uint64(sizeA)], mockDataA)
	copy(expected[offsetB:offsetB+uint64(sizeB)], mockDataB)

	t.Log("Testing with RemoteCache Enabled (Mocked via TCP)")
	total, err := s.read(data, 0, int(filesize), 0)
	if err != nil {
		t.Errorf("read failed: %v", err)
	}
	if uint64(total) != filesize {
		t.Errorf("read total mismatch: got %v want %v", total, filesize)
	}

	if !bytes.Equal(data, expected) {
		t.Errorf("data mismatch with remote cache.\n")
		if !bytes.Equal(data[offsetA:offsetA+uint64(sizeA)], mockDataA) {
			t.Error("Extent 1 mismatch")
		}
		if !bytes.Equal(data[0:offsetA], expected[0:offsetA]) {
			t.Error("Hole before Extent 1 not zeroed")
		}
		if !bytes.Equal(data[offsetA+uint64(sizeA):offsetB], expected[offsetA+uint64(sizeA):offsetB]) {
			t.Error("Hole between Extents not zeroed")
		}
		if !bytes.Equal(data[offsetB:offsetB+uint64(sizeB)], mockDataB) {
			t.Error("Extent 2 mismatch")
		}
		if !bytes.Equal(data[offsetB+uint64(sizeB):], expected[offsetB+uint64(sizeB):]) {
			t.Error("Hole after Extent 2 not zeroed")
		}
	} else {
		t.Log("RemoteCache read verification successful")
	}

	// 8. Test Data Node Read (Remote Cache Disabled)
	s.client.forceRemoteCache = false
	client.RemoteCache.VolumeEnabled = false

	for i := range data {
		data[i] = 0xEE
	}

	t.Log("Testing with RemoteCache Disabled (DataNode via TCP)")
	total, err = s.read(data, 0, int(filesize), 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if uint64(total) != filesize {
		t.Errorf("read total mismatch: got %v want %v", total, filesize)
	}
	if !bytes.Equal(data, expected) {
		t.Errorf("data mismatch without remote cache.\n")
		if !bytes.Equal(data[offsetA:offsetA+uint64(sizeA)], mockDataA) {
			t.Error("Extent 1 mismatch")
		}
		if !bytes.Equal(data[offsetB:offsetB+uint64(sizeB)], mockDataB) {
			t.Error("Extent 2 mismatch")
		}
	} else {
		t.Log("DataNode read verification successful")
	}
}
