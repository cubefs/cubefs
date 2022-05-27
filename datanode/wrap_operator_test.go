package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	testDiskPath = "/cfs/testDataNodeDisk"
	Master       = "master"
)

var (
	startHttp      bool
	fakeNode       *fakeDataNode
	partitionIdNum uint64
)

type fakeDataNode struct {
	DataNode
	Hosts              []string
	path               string
	fakeNormalExtentId uint64
	fakeTinyExtentId   uint64
}

func Test_getTinyExtentHoleInfo(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	dp := PrepareDataPartition(true, true, t, partitionIdNum)
	if dp == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteDataPartition(t, partitionIdNum)
	url := fmt.Sprintf("http://127.0.0.1:6767/fakeTinyExtentHoleInfo?partitionID=%v&extentID=%v", partitionIdNum, fakeNode.fakeTinyExtentId)
	if _, err := getHttpRequestResp(url, t); err != nil {
		t.Fatalf("getHttpRequestResp err(%v)", err)
	}
}

func Test_handleTinyExtentAvaliRead(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	dp := PrepareDataPartition(true, true, t, partitionIdNum)
	if dp == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteDataPartition(t, partitionIdNum)
	p := new(repl.Packet)
	p.ExtentID = fakeNode.fakeTinyExtentId
	p.PartitionID = partitionIdNum
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(0)
	p.Size = uint32(10)
	p.Opcode = proto.OpTinyExtentAvaliRead
	p.ExtentType = proto.TinyExtentType
	p.ReqID = proto.GenerateRequestID()

	opCode, err, msg := fakeNode.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}

}

func newFakeDataNode(t *testing.T) *fakeDataNode {
	fdn := &fakeDataNode{
		DataNode: DataNode{
			clusterID:       "datanode-cluster",
			raftHeartbeat:   "17331",
			raftReplica:     "17341",
			port:            "11010",
			zoneName:        "zone-01",
			httpPort:        "11210",
			localIP:         "127.0.0.1",
			localServerAddr: "127.0.0.1:11010",
			nodeID:          uint64(111),
			stopC:           make(chan bool),
		},
		Hosts: []string{
			"127.0.0.1:11010",
			"127.0.0.1:11011",
			"127.0.0.1:11012",
		},
		fakeNormalExtentId: 1025,
		fakeTinyExtentId:   1,
		path:               testDiskPath,
	}

	runHttp(t)
	masters := []string{
		"127.0.0.1:6767",
	}
	MasterClient = masterSDK.NewMasterClient(masters, false)
	cfgStr := "{\n\t\t\t\"disks\": [\"/cfs/testDataNodeDisk:5368709120\"],\n\t\t\t\"listen\": \"11010\",\n\t\t\t\"raftHeartbeat\": \"17331\",\n\t\t\t\"raftReplica\": \"17341\",\n\t\t\t\"logDir\":\"/cfs/log\",\n\t\t\t\"raftDir\":\"/cfs/log\",\n\t\t\t\"masterAddr\": [\"127.0.0.1:6767\",\"127.0.0.1:6767\",\"127.0.0.1:6767\"],\n\t\t\t\"prof\": \"11210\"}"
	cfg := config.LoadConfigString(cfgStr)
	if err := fdn.Start(cfg); err != nil {
		t.Fatalf("startTCPService err(%v)", err)
	}

	return fdn
}

func runHttp(t *testing.T) {
	if !startHttp {
		profNetListener, err := net.Listen("tcp", fmt.Sprintf(":%v", 6767))
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			_ = http.Serve(profNetListener, http.DefaultServeMux)
		}()
		go func() {
			http.HandleFunc(proto.AdminGetIP, fakeGetClusterInfo)
			http.HandleFunc(proto.AddDataNode, fakeAddDataNode)
			http.HandleFunc(proto.GetDataNode, fakeGetDataNode)
			http.HandleFunc("/fakeTinyExtentHoleInfo", fakeGetTinyExtentHoleInfo)
		}()
		startHttp = true
	}
}

func (fdn *fakeDataNode) fakeDeleteDataPartition(t *testing.T, partitionId uint64) {
	req := &proto.DeleteDataPartitionRequest{
		PartitionId: partitionId,
	}

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, fdn.Hosts[0], req)
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

func FakeDirCreate(t *testing.T) (err error) {
	_, err = os.Stat(testDiskPath)
	if err == nil {
		os.RemoveAll(testDiskPath)
	}
	if err = os.MkdirAll(testDiskPath, 0766); err != nil {
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
		fakeNode = newFakeDataNode(t)
	}
	return
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

	return dp
}

func (fdn *fakeDataNode) fakeCreateDataPartition(t *testing.T, partitionId uint64) (dp *DataPartition) {
	req := &proto.CreateDataPartitionRequest{
		PartitionId:   partitionId,
		PartitionSize: 5 * util.GB, // 5GB
		VolumeId:      "testVol",
		Hosts:         fdn.Hosts,
		Members: []proto.Peer{
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
			{ID: fdn.nodeID, Addr: fdn.localServerAddr},
		},
	}

	task := proto.NewAdminTask(proto.OpCreateDataPartition, fdn.Hosts[0], req)
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

	opCode, err, msg := fdn.FakeOperateHandle(t, p)
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

	opCode, err, msg := e.FakeOperateHandle(t, p)
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
	size := 1 * util.MB
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 1
	}

	crc = crc32.ChecksumIEEE(data)
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
			Data:        data,
			StartT:      time.Now().UnixNano(),
		},
	}

	opCode, err, msg := fdn.FakeOperateHandle(t, p)
	if err != nil {
		t.Fatal(err)
		return
	}
	if opCode != proto.OpOk {
		t.Fatal(msg)
	}
	return
}

func (fdn *fakeDataNode) FakeOperateHandle(t *testing.T, p *repl.Packet) (opCode uint8, err error, msg string) {
	err = fdn.Prepare(p, fdn.localServerAddr)
	if err != nil {
		t.Errorf("prepare err %v", err)
		return
	}

	conn, err := net.Dial("tcp", fdn.Hosts[0])
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

func fakeGetClusterInfo(w http.ResponseWriter, r *http.Request) {
	info := &proto.ClusterInfo{
		Cluster: "chubaofs",
		Ip:      "127.0.0.1",
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeAddDataNode(w http.ResponseWriter, r *http.Request) {
	buildJSONResp(w, http.StatusOK, 1, Master, "")
}

func fakeGetDataNode(w http.ResponseWriter, r *http.Request) {
	info := &proto.DataNodeInfo{}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeGetTinyExtentHoleInfo(w http.ResponseWriter, r *http.Request) {
	fakeNode.getTinyExtentHoleInfo(w, r)
}
