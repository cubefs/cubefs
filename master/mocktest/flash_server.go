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

type MockFlashNodeServer struct {
	nodeID    uint64
	TcpAddr   string
	ClusterID string
	Available uint64
	zoneName  string
	mc        *master.MasterClient
	stopC     chan bool
}

func NewMockFlashNodeServer(addr string, zoneName string) *MockFlashNodeServer {
	mfs := &MockFlashNodeServer{
		TcpAddr:  addr,
		zoneName: zoneName,
		mc:       master.NewMasterClient([]string{hostAddr}, false),
		stopC:    make(chan bool),
	}

	return mfs
}

func (mfs *MockFlashNodeServer) Start() {
	mfs.register()
	go mfs.start()
}

func (mfs *MockFlashNodeServer) Stop() {
	close(mfs.stopC)
}

func (mfs *MockFlashNodeServer) register() {
	var err error
	var nodeID uint64
	var retry int
	for retry < 20 {
		nodeID, err = mfs.mc.NodeAPI().AddFlashNode(mfs.TcpAddr, mfs.zoneName, proto.BaseVersion)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		retry++
	}
	if err != nil {
		panic(err)
	}
	mfs.nodeID = nodeID
}

func (mfs *MockFlashNodeServer) start() {
	listener, err := net.Listen("tcp", mfs.TcpAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	go func() {
		for {
			select {
			case <-mfs.stopC:
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
		go mfs.serveConn(conn)
	}
}

func (mfs *MockFlashNodeServer) serveConn(rc net.Conn) {
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
	case proto.OpFlashNodeHeartbeat:
		err = responseAckOKToMaster(conn, req, nil)
		fmt.Printf("flash node [%v] report heartbeat to master,err:%v\n", mfs.TcpAddr, err)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}
