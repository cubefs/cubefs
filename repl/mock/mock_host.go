package mock

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"net"
	"sync"
)

type MockHost struct {
	id      int
	records MockHostRecords
	ln      net.Listener
	listen  int
	stopCh  chan struct{}

	protocols   map[int64]*repl.ReplProtocol
	protocolsMu sync.Mutex
}

func (m *MockHost) ID() int {
	return m.id
}

func (m *MockHost) Start() (err error) {
	if m.ln, err = net.Listen("tcp", fmt.Sprintf(":%d", m.listen)); err != nil {
		return
	}
	go m.serveAccept()
	return
}

func (m *MockHost) serveAccept() {
	var conn net.Conn
	var err error
	for {
		if conn, err = m.ln.Accept(); err != nil {
			return
		}
		go m.serveConn(conn)
	}
}

func (m *MockHost) serveConn(conn net.Conn) {

	var protocol *repl.ReplProtocol = nil
	protocol = repl.NewReplProtocol(conn.(*net.TCPConn), m.prepare, m.operator, m.post)
	select {
	case <- m.stopCh:
		return
	default:
	}
	m.protocolsMu.Lock()
	m.protocols[protocol.GetID()] = protocol
	m.protocolsMu.Unlock()
	defer func() {
		m.protocolsMu.Lock()
		delete(m.protocols, protocol.GetID())
		m.protocolsMu.Unlock()
	}()
	protocol.ServerConn()
}

func (m *MockHost) prepare(p *repl.Packet, remote string) error {
	return nil
}

func (m *MockHost) operator(p *repl.Packet, conn *net.TCPConn) (err error) {
	var record MockHostRecord
	if record, err = m.records.Next(); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if p.ReqID != record.ReqID {
		err = fmt.Errorf("message ID mismatch: host [%v], expect [%v], actual [%v]", m.id, record.ReqID, p.ReqID)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if record.Result == MockResult_Failure {
		err = fmt.Errorf("operate failure: host [%v]", m.id)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (m *MockHost) post(p *repl.Packet) error {
	return nil
}

func (m *MockHost) Stop() {
	close(m.stopCh)
	if m.ln != nil {
		_ = m.ln.Close()
	}
	m.protocolsMu.Lock()
	for _, protocol := range m.protocols {
		protocol.Stop(nil)
	}
	m.protocolsMu.Unlock()
}

func NewMockHost(id, listen int, records MockHostRecords) *MockHost {
	return &MockHost{
		id:      id,
		listen:  listen,
		records: records,
		stopCh:  make(chan struct{}),

		protocols: make(map[int64]*repl.ReplProtocol),
	}
}
