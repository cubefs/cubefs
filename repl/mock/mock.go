package mock

import (
	"fmt"
	"github.com/cubefs/cubefs/util/connpool"
	"hash/crc32"
	"io"
	"net"
	"sort"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
)

type MockResult int

const (
	MockResult_Success MockResult = iota
	MockResult_Failure
)

type MockHostRecord struct {
	ReqID  int64
	Result MockResult
}

type MockHostRecords struct {
	records []MockHostRecord
	cursor  int
}

func (m *MockHostRecords) AppendRecord(record ...MockHostRecord) {
	m.records = append(m.records, record...)
}

func (m *MockHostRecords) Len() int {
	return len(m.records)
}

func (m *MockHostRecords) Cursor() int {
	return m.cursor
}

func (m *MockHostRecords) Reset() {
	m.cursor = 0
}

func (m *MockHostRecords) HasNext() bool {
	return m.cursor < len(m.records)
}

func (m *MockHostRecords) Next() (MockHostRecord, error) {
	if m.cursor < len(m.records) {
		record := m.records[m.cursor]
		m.cursor++
		return record, nil
	}
	return MockHostRecord{}, io.EOF
}

func NewMockHostRecords() MockHostRecords {
	r := MockHostRecords{
		records: make([]MockHostRecord, 0),
		cursor:  0,
	}
	return r
}

type MockMessageConfig struct {
	ReqID        int64
	ExpectResult MockResult
	HostResults  map[int]MockResult // ID -> Result
}

type MockTestConfig struct {
	Quorum   int
	Hosts    map[int]int // ID -> Listen
	Messages []MockMessageConfig
}

func RunMockTest(config *MockTestConfig, t *testing.T) {
	var err error
	if len(config.Hosts) == 0 {
		t.Fatalf("non hosts specified")
	}
	if config.Quorum > len(config.Hosts) {
		t.Fatalf("illegal mock test config: quorum [%v] larger than hosts [%v]", config.Quorum, config.Hosts)
		return
	}

	var hostIds = make([]int, 0)
	for hostID := range config.Hosts {
		hostIds = append(hostIds, hostID)
	}
	sort.SliceStable(hostIds, func(i, j int) bool {
		return hostIds[i] < hostIds[j]
	})

	var mockHosts = make([]*MockHost, 0)
	var hostAddrs = make([]string, 0)
	for _, hostID := range hostIds {
		hostListen := config.Hosts[hostID]
		hostAddress := fmt.Sprintf("127.0.0.1:%v", hostListen)
		hostRecords := NewMockHostRecords()
		for _, message := range config.Messages {
			result, defined := message.HostResults[hostID]
			if !defined {
				t.Fatalf("message [%v] not defined for host [%v]", message.ReqID, hostID)
			}
			hostRecords.AppendRecord(MockHostRecord{
				ReqID:  message.ReqID,
				Result: result,
			})
		}
		host := NewMockHost(hostID, hostListen, hostRecords)
		mockHosts = append(mockHosts, host)
		hostAddrs = append(hostAddrs, hostAddress)
	}

	repl.SetConnectPool(connpool.NewConnectPool())

	// Start all mockHosts
	for _, host := range mockHosts {
		if err = host.Start(); err != nil {
			t.Fatalf("start mock host %v start failed: %v", host.ID(), err)
		}
	}

	defer func() {
		// Stop all hosts
		for _, host := range mockHosts {
			host.Stop()
		}
	}()

	var conn net.Conn

	if conn, err = net.Dial("tcp", hostAddrs[0]); err != nil {
		t.Fatalf("connect to leader host [%v] failed: %v", hostAddrs[0], err)
	}

	defer func() {
		_ = conn.Close()
	}()

	// Send messages to host
	sendMockMessage(conn, hostAddrs[1:], config.Quorum, config.Messages, t)

	// Wait for response and validate result
	validateMockResult(conn, config.Messages, t)

}

func sendMockMessage(conn net.Conn, followers []string, quorum int, messages []MockMessageConfig, t *testing.T) {
	var err error
	packets := make([]*proto.Packet, 0)

	for _, message := range messages {
		packet := new(proto.Packet)
		packet.ReqID = message.ReqID
		packet.Magic = proto.ProtoMagic
		packet.Opcode = proto.OpWrite
		packet.Arg = repl.EncodeReplPacketArg(followers, quorum)
		packet.ArgLen = uint32(len(packet.Arg))
		packet.Data = nil
		packet.CRC = crc32.ChecksumIEEE(nil)
		packet.RemainingFollowers = uint8(len(followers))
		packets = append(packets, packet)
	}

	for _, packet := range packets {
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			t.Fatalf("send message [%v] to leader host [%v] failed: %v", packet.ReqID, followers, err)
		}
	}
}

func validateMockResult(conn net.Conn, messages []MockMessageConfig, t *testing.T) {
	var err error
	for _, message := range messages {
		packet := new(proto.Packet)
		if err = packet.ReadFromConn(conn, 5); err != nil {
			t.Fatalf("read message [%v] reply failed: %v", message.ReqID, err)
		}
		if packet.ReqID != message.ReqID {
			t.Fatalf("reply message mismatch: expect [%v], actual [%v]", message.ReqID, packet.ReqID)
		}

		if (packet.ResultCode == proto.OpOk) && message.ExpectResult == MockResult_Success {
			continue
		}

		if (packet.ResultCode != proto.OpOk) && message.ExpectResult == MockResult_Failure {
			return
		}

		t.Fatalf("reply message [%v] result mismatch: expect [%v], actual [%v]", message.ReqID, message.ExpectResult == MockResult_Success, packet.ResultCode == proto.OpOk)
	}
}
