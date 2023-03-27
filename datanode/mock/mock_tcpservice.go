package mock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/unit"
	"hash/crc32"
	"io"
	"net"
)

const (
	RemoteNormalExtentCount        = 6
	LocalCreateExtentId     uint64 = 1000
)

var (
	ReplyGetAllWatermarksV2Err                = false
	ReplyGetRemoteExtentInfoForValidateCRCCount = 3
)

type MockTcp struct {
	ln     net.Listener
	listen int
}

func NewMockTcp(listen int) *MockTcp {
	return &MockTcp{
		listen: listen,
	}
}

func (m *MockTcp) Start() (err error) {
	if m.ln, err = net.Listen("tcp", fmt.Sprintf(":%d", m.listen)); err != nil {
		return
	}
	go m.serveAccept()
	return
}

func (m *MockTcp) Stop() {
	if m.ln != nil {
		_ = m.ln.Close()
	}
}

func (m *MockTcp) serveAccept() {
	var conn net.Conn
	var err error
	for {
		if conn, err = m.ln.Accept(); err != nil {
			return
		}
		go m.serveConn(conn)
	}
}

func (m *MockTcp) serveConn(conn net.Conn) (err error) {
	p := new(repl.Packet)
	_, err = p.ReadFromConnFromCli(conn, repl.ReplProtocalServerTimeOut)
	if err != nil {
		return
	}
	switch p.Opcode {
	case proto.OpGetAllWatermarks:
		m.handleGetAllWatermarks(p, conn)
	case proto.OpGetAllWatermarksV2:
		m.handleGetAllWatermarksV2(p, conn)
	case proto.OpExtentRepairRead:
		m.handleExtentRepairReadPacket(p, conn)
	case proto.OpTinyExtentRepairRead:
		m.handleTinyExtentRepairRead(p, conn)
	case proto.OpNotifyReplicasToRepair:
		m.handlePacketToNotifyExtentRepair(p, conn)
	case proto.OpGetAllExtentInfo:
		m.handlePacketToGetAllExtentInfo(p, conn)
	}
	return
}

func (m *MockTcp) handleGetAllWatermarks(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	extents := make([]proto.ExtentInfoBlock, 0)
	var data []byte
	if p.ExtentType == proto.NormalExtentType {
		normalExtentCount := RemoteNormalExtentCount
		for i := 0; i < normalExtentCount-1; i++ {
			ei := proto.ExtentInfoBlock{
				uint64(i + 65),
				uint64(i+65) * 1024,
			}
			extents = append(extents, ei)
		}
		extents = append(extents, proto.ExtentInfoBlock{
			LocalCreateExtentId,
			LocalCreateExtentId * 1024,
		})
	} else {
		extentIDs := make([]uint64, 0)
		_ = json.Unmarshal(p.Data, &extentIDs)

		for _, eid := range extentIDs {
			ei := proto.ExtentInfoBlock{
				eid,
				eid * 1024,
			}
			extents = append(extents, ei)
		}
	}
	data, err = json.Marshal(extents)
	if err != nil {
		replyData(p, 0, data, proto.OpErr, c)
		p.PacketOkReply()
		return
	}
	if err = replyData(p, 0, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handleGetAllWatermarksV2(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	var data []byte
	if p.ExtentType == proto.NormalExtentType {
		normalExtentCount := RemoteNormalExtentCount
		data = make([]byte, 16*normalExtentCount)
		index := 0
		for i := 0; i < normalExtentCount-1; i++ {
			ei := &proto.ExtentInfoBlock{
				uint64(i + 65),
				uint64(i+65) * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		ei := &proto.ExtentInfoBlock{
			LocalCreateExtentId,
			LocalCreateExtentId * 1024,
		}
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
		index += 8
		data = data[:index]
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		data = make([]byte, 16*len(extentIDs))
		index := 0
		for _, eid := range extentIDs {
			ei := &proto.ExtentInfoBlock{
				eid,
				eid * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		data = data[:index]
	}
	if !ReplyGetAllWatermarksV2Err {
		if err = replyData(p, 0, data, proto.OpOk, c); err != nil {
			fmt.Printf("replyData err: %v", err)
		}
	} else {
		if err = replyData(p, 0, data, proto.OpErr, c); err != nil {
			fmt.Printf("replyData err: %v", err)
		}
	}
	p.PacketOkReply()
}

func replyData(p *repl.Packet, ExtentOffset int64, data []byte, resultCode uint8, c net.Conn) (err error) {
	reply := repl.NewStreamReadResponsePacket(p.Ctx(), p.ReqID, p.PartitionID, p.ExtentID)
	reply.ArgLen = 17
	reply.Arg = make([]byte, reply.ArgLen)
	binary.BigEndian.PutUint64(reply.Arg[9:17], uint64(len(data)))
	reply.Data = data
	reply.Size = uint32(len(reply.Data))
	reply.ResultCode = resultCode
	reply.Opcode = p.Opcode
	reply.ExtentOffset = ExtentOffset
	reply.CRC = crc32.ChecksumIEEE(reply.Data[:reply.Size])
	header := make([]byte, unit.PacketHeaderSize)
	reply.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(reply.Arg[:int(reply.ArgLen)]); err == nil {
			if reply.Data != nil && reply.Size != 0 {
				_, err = c.Write(reply.Data[:reply.Size])
			}
		}
	}
	return
}

func (m *MockTcp) handleExtentRepairReadPacket(p *repl.Packet, c net.Conn) {
	size := p.ExtentID * 1024
	data := make([]byte, size-uint64(p.ExtentOffset))
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handleTinyExtentRepairRead(p *repl.Packet, c net.Conn) {
	tinyExtentFinfoSize := p.ExtentID * 1024
	needReplySize := int64(tinyExtentFinfoSize - uint64(p.ExtentOffset))
	avaliReplySize := uint64(needReplySize)
	data := make([]byte, avaliReplySize)
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]storage.ExtentInfoBlock
	ExtentsToBeCreated             []storage.ExtentInfoBlock
	ExtentsToBeRepaired            []storage.ExtentInfoBlock
	ExtentsToBeRepairedSource      map[uint64]string
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func (m *MockTcp) handlePacketToNotifyExtentRepair(p *repl.Packet, c net.Conn) {
	mf := new(DataPartitionRepairTask)
	err := json.Unmarshal(p.Data, mf)
	if err != nil {
		return
	}
	if err = replyData(p, p.ExtentOffset, []byte{}, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handlePacketToGetAllExtentInfo(p *repl.Packet, c net.Conn) {
	if ReplyGetRemoteExtentInfoForValidateCRCCount > 0 {
		_ = replyData(p, p.ExtentOffset, []byte{}, proto.OpErr, c)
		ReplyGetRemoteExtentInfoForValidateCRCCount--
		return
	}
	var extentCount = LocalCreateExtentId
	var extents []storage.ExtentInfoBlock
	var i uint64 = 1
	for ; i <= extentCount; i++ {
		extents = append(extents, storage.ExtentInfoBlock{
			i,
			i * 1024,
			1668494561,
		})
	}
	data := make([]byte, extentCount*20)
	index := 0
	for _, ext := range extents {
		binary.BigEndian.PutUint64(data[index:index+8], ext[storage.FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ext[storage.Size])
		index += 8
		binary.BigEndian.PutUint32(data[index:index+4], uint32(ext[storage.Crc]))
		index += 4
	}
	data = data[:index]
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}
