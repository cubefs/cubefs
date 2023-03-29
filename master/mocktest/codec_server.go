package mocktest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"net"
	"time"
)

type MockCodecServer struct {
	nodeID    uint64
	TcpAddr   string
	httpPort  string
	Zone      string
	ClusterID string
	Available uint64
	zoneName  string
	mc        *master.MasterClient
	stopC     chan bool
}

func NewMockCodecServer(addr string, httpPort string, zoneName string) *MockCodecServer {
	mcs := &MockCodecServer{
		TcpAddr:  addr,
		httpPort: httpPort,
		zoneName: zoneName,
		mc:       master.NewMasterClient([]string{hostAddr}, false),
		stopC:    make(chan bool),
	}

	return mcs
}

func (mcs *MockCodecServer) Start() {
	mcs.register()
	go mcs.start()
}

func (mcs *MockCodecServer) Stop() {
	close(mcs.stopC)
}

func (mcs *MockCodecServer) register() {
	var err error
	var nodeID uint64
	var retry int
	for retry < 20 {
		nodeID, err = mcs.mc.NodeAPI().AddCodecNode(mcs.TcpAddr, proto.BaseVersion)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		retry++
	}
	if err != nil {
		panic(err)
	}
	mcs.nodeID = nodeID
}

func (mcs *MockCodecServer) start() {
	listener, err := net.Listen("tcp", mcs.TcpAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	go func() {
		for {
			select {
			case <-mcs.stopC:
				return
			default:
			}
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go mcs.serveConn(conn)
	}
}

func (mcs *MockCodecServer) serveConn(rc net.Conn) {
	conn, ok := rc.(*net.TCPConn)
	if !ok {
		rc.Close()
		return
	}
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	req := proto.NewPacket(context.Background())
	err := req.ReadFromConn(conn, proto.NoReadDeadlineTime)
	if err != nil {
		return
	}
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(req.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		responseAckErrToMaster(conn, req, err)
		return
	}
	switch req.Opcode {
	case proto.OpCodecNodeHeartbeat:
		err = mcs.handleHeartbeats(conn, req, adminTask)
	case proto.OpIssueMigrationTask:
		err = mcs.handleMigrationTask(conn, req, adminTask)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}

func (mcs *MockCodecServer) handleHeartbeats(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	response := &proto.CodecNodeHeartbeatResponse{}
	response.Status = proto.TaskSucceeds

	task.Response = response
	if err = mcs.mc.NodeAPI().ResponseCodecNodeTask(task); err != nil {
		return
	}
	return
}

func (mcs *MockCodecServer) handleMigrationTask(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	return
}
