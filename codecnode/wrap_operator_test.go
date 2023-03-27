package codecnode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"hash/crc32"
	"net"
	"net/http"
	"testing"
	"time"
)

const (
	fakePartitionID = 1
	fakeExtentId    = 0
	Master          = "master"
	DataNode        = "datanode"
	TcpPort         = "9501"
	HttpPort        = "8888"
)

var (
	runHttpServer bool
	fcs           *fakeCodecServer
)

type fakeCodecServer struct {
	CodecServer
	getClusterInfoTimes int
}

func runHttp(t *testing.T) {
	if !runHttpServer {
		runHttpServer = true
		profNetListener, err := net.Listen("tcp", fmt.Sprintf(":%v", HttpPort))
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			_ = http.Serve(profNetListener, http.DefaultServeMux)
		}()
		go func() {
			http.HandleFunc(proto.AdminGetIP, fakeGetIp)
			http.HandleFunc(proto.AdminGetVol, fakeGetVol)
			http.HandleFunc(proto.ClientDataPartitions, fakeGetDataPartitions)
			http.HandleFunc(proto.AdminGetCluster, fakeGetCluster)
			http.HandleFunc("/partition", fakeDataNodeGetPartition)
			http.HandleFunc(proto.AddCodecNode, fakeAddCodecNode)
			http.HandleFunc("/tinyExtentHoleInfo", fakeGetTinyExtentHolesAndAvali)
		}()
	}
}
func newFakeCodecServer(t *testing.T, port string) *fakeCodecServer {
	fcs := &fakeCodecServer{
		CodecServer: CodecServer{
			clusterID:       "chubaofs-test",
			port:            port,
			nodeID:          111,
			localServerAddr: fmt.Sprintf("127.0.0.1:%s", port),
			stopC:           make(chan bool),
		},
		getClusterInfoTimes: 0,
	}
	go fcs.startFakeServiceForTest(t, fcs.localServerAddr)

	return fcs
}

func (fcs *fakeCodecServer) startFakeServiceForTest(t *testing.T, host string) {
	//fmt.Println(fmt.Sprintf("host:%v listening", host))
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

		fmt.Printf("recive conn[local:%v remote:%v]\n", conn.LocalAddr(), conn.RemoteAddr())
		go fcs.fakeServiceHandler(conn, t)
	}
}

func (fcs *fakeCodecServer) fakeServiceHandler(conn net.Conn, t *testing.T) {
	defer conn.Close()
	request := repl.NewPacket(context.Background())
	if _, err := request.ReadFromConnFromCli(conn, proto.NoReadDeadlineTime); err != nil {
		//fmt.Println(err)
		return
	}

	switch request.Opcode {
	case proto.OpCreateExtent:
		fakeCreateExtent(fcs, request, conn)
	case proto.OpStreamRead, proto.OpStreamFollowerRead:
		fakeRead(fcs, request, conn)
	case proto.OpEcWrite:
		fakeWrite(fcs, request, conn)
	case proto.OpPersistTinyExtentDelete:
		fakePersistHolesInfo(fcs, request, conn)
	default:
		t.Fatalf("unkown Opcode :%v", request.Opcode)
	}
	if err := request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		t.Fatal(err)
	}
}

func TestCodecServer_handleHeartbeatPacket(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	request := &proto.HeartBeatRequest{
		CurrTime: time.Now().Unix(),
	}
	task := proto.NewAdminTask(proto.OpCodecNodeHeartbeat, fcs.localServerAddr, request)
	data, _ := json.Marshal(task)
	crc := crc32.ChecksumIEEE(data)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:  proto.ProtoMagic,
			ReqID:  proto.GenerateRequestID(),
			Opcode: proto.OpCodecNodeHeartbeat,
			Size:   uint32(len(data)),
			Data:   data,
			CRC:    crc,
			StartT: time.Now().UnixNano(),
		},
	}

	fcs.handleHeartbeatPacket(p)
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleHeartbeatPacket fail, error msg:%v", p.GetResultMsg())
	}
}

func TestCodecServer_handleEcMigrationTask(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	request := &proto.IssueMigrationTaskRequest{
		VolName:         "ltptest",
		PartitionId:     fakePartitionID,
		CurrentExtentID: fakeExtentId,
		ProfHosts:       []string{"127.0.0.1:17310"},
		EcDataNum:       4,
		EcParityNum:     2,
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17315",
		},
		EcMaxUnitSize: 1024,
	}

	task := proto.NewAdminTask(proto.OpIssueMigrationTask, fcs.localServerAddr, request)
	data, _ := json.Marshal(task)
	crc := crc32.ChecksumIEEE(data)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:  proto.ProtoMagic,
			ReqID:  proto.GenerateRequestID(),
			Opcode: proto.OpIssueMigrationTask,
			Size:   uint32(len(data)),
			Data:   data,
			CRC:    crc,
			StartT: time.Now().UnixNano(),
		},
	}
	fcs.handleEcMigrationTask(p)
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleHeartbeatPacket fail, error msg:%v", p.GetResultMsg())
	}

}

func TestCodecServer_sendStatusToMaster(t *testing.T) {
	if fcs == nil {
		fcs = newFakeCodecServer(t, TcpPort)
	}
	runHttp(t)

	_ = fcs.sendStatusToMaster(0, 0, 0, "")
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

func fakeGetIp(w http.ResponseWriter, r *http.Request) {
	info := &proto.ClusterInfo{
		Cluster: "chubaofs",
		Ip:      "127.0.0.1",
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetVol(w http.ResponseWriter, r *http.Request) {
	info := &proto.SimpleVolView{}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetDataPartitions(w http.ResponseWriter, r *http.Request) {
	dps := []*proto.DataPartitionResponse{
		{
			PartitionID: 1,
			Hosts:       []string{"127.0.0.1:" + TcpPort},
		},
	}
	info := &proto.DataPartitionsView{
		DataPartitions: dps,
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetCluster(w http.ResponseWriter, r *http.Request) {
	info := &proto.ClusterView{}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeDataNodeGetPartition(w http.ResponseWriter, r *http.Request) {
	files := []proto.ExtentInfoBlock{
		{1},
		{2},
	}

	info := &proto.DNDataPartitionInfo{
		Files: files,
	}
	buildJSONResp(w, http.StatusOK, info, DataNode, "")
}

func fakeAddCodecNode(w http.ResponseWriter, r *http.Request) {
	info := 1
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetTinyExtentHolesAndAvali(w http.ResponseWriter, r *http.Request) {
	holes := []*proto.TinyExtentHole{}
	info := &struct {
		Holes           []*proto.TinyExtentHole `json:"holes"`
		ExtentAvaliSize uint64                  `json:"extentAvaliSize"`
	}{
		Holes:           holes,
		ExtentAvaliSize: 1,
	}
	buildJSONResp(w, http.StatusOK, info, DataNode, "")
}
