package mocktest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"io/ioutil"
	"net"
	"net/http"
)

type MockDataServer struct {
	nodeID                          uint64
	TcpAddr                         string
	Zone                            string
	ClusterID                       string
	Total                           uint64
	Used                            uint64
	Available                       uint64
	CreatedPartitionWeights         uint64 //dataPartitionCnt*dataPartitionSize
	RemainWeightsForCreatePartition uint64 //all-useddataPartitionsWieghts
	CreatedPartitionCnt             uint64
	MaxWeightsForCreatePartition    uint64
	partitions                      []*MockDataPartition
	rackName                        string
}

func NewMockDataServer(addr string, rackName string) *MockDataServer {
	mds := &MockDataServer{TcpAddr: addr, rackName: rackName,
		partitions: make([]*MockDataPartition, 0)}

	return mds
}

func (mds *MockDataServer) Start() {
	mds.register()
	go mds.start()
}

func (mds *MockDataServer) register() {
	reqUrl := fmt.Sprintf("%v?addr=%v", urlAddDataNode, mds.TcpAddr)
	resp, err := http.Get(reqUrl)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
	rst := &proto.HTTPReply{}
	err = json.Unmarshal(body, rst)
	if err != nil {
		panic(err)
	}
	nodeIDFloat := rst.Data.(float64)
	mds.nodeID = uint64(nodeIDFloat)
}

func (mds *MockDataServer) start() {
	listener, err := net.Listen("tcp", mds.TcpAddr)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go mds.serveConn(conn)
	}
}

func (mds *MockDataServer) serveConn(rc net.Conn) {
	conn, ok := rc.(*net.TCPConn)
	if !ok {
		rc.Close()
		return
	}
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	req := proto.NewPacket()
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
	case proto.OpCreateDataPartition:
		err = mds.handleCreateDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] create data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDeleteDataPartition:
		err = mds.handleDeleteDataPartition(conn, req)
		fmt.Printf("data node [%v] delete data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDataNodeHeartbeat:
		err = mds.handleHeartbeats(conn, req, adminTask)
		fmt.Printf("data node [%v] report heartbeat to master,err:%v\n", mds.TcpAddr, err)
	case proto.OpLoadDataPartition:
		err = mds.handleLoadDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] load data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDecommissionDataPartition:
		err = mds.handleDecommissionDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] decommission data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpAddDataPartitionRaftMember:
		err = mds.handleAddDataPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("data node [%v] add data partition raft member,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpRemoveDataPartitionRaftMember:
		err = mds.handleRemoveDataPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("data node [%v] remove data partition raft member,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}

func (mds *MockDataServer) handleAddDataPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleRemoveDataPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleDecommissionDataPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	defer func() {
		if err != nil {
			responseAckErrToMaster(conn, p, err)
		} else {
			p.PacketOkWithBody([]byte("/cfs"))
			p.WriteToConn(conn)
		}
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.DataPartitionDecommissionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	partitions := make([]*MockDataPartition, 0)
	for index, dp := range mds.partitions {
		if dp.PartitionID == req.PartitionId {
			partitions = append(partitions, mds.partitions[:index]...)
			partitions = append(partitions, mds.partitions[index+1:]...)
		}
	}
	if len(partitions) != 0 {
		mds.partitions = partitions
	}
	return
}

func (mds *MockDataServer) handleCreateDataPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	defer func() {
		if err != nil {
			responseAckErrToMaster(conn, p, err)
		} else {
			responseAckOKToMaster(conn, p, nil)
		}
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateDataPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  metaPartition.
	partition := &MockDataPartition{
		PartitionID: req.PartitionId,
		VolName:     req.VolumeId,
		total:       req.PartitionSize,
		used:        10 * util.GB,
	}
	mds.partitions = append(mds.partitions, partition)
	return
}

// Handle OpHeartbeat packet.
func (mds *MockDataServer) handleHeartbeats(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	response := &proto.DataNodeHeartbeatResponse{}
	response.Status = proto.TaskSucceeds
	response.Used = 5 * util.GB
	response.Total = 1024 * util.GB
	response.Available = 1024 * util.GB
	response.CreatedPartitionCnt = 3
	response.TotalPartitionSize = 120 * util.GB
	response.MaxCapacity = 800 * util.GB
	response.RemainingCapacity = 800 * util.GB

	response.RackName = mds.rackName
	response.PartitionReports = make([]*proto.PartitionReport, 0)

	for _, partition := range mds.partitions {
		vr := &proto.PartitionReport{
			PartitionID:     partition.PartitionID,
			PartitionStatus: proto.ReadWrite,
			Total:           120 * util.GB,
			Used:            20 * util.GB,
			DiskPath:        "/cfs",
			ExtentCount:     10,
			NeedCompare:     true,
			IsLeader:        true, //todo
			VolName:         partition.VolName,
		}
		response.PartitionReports = append(response.PartitionReports, vr)
	}

	task.Response = response
	data, err := json.Marshal(task)
	if err != nil {
		return
	}
	_, err = PostToMaster(http.MethodPost, urlDataNodeResponse, data)
	return
}

func (mds *MockDataServer) handleDeleteDataPartition(conn net.Conn, pkg *proto.Packet) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	return
}

func (mds *MockDataServer) handleLoadDataPartition(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	// Marshal request body.
	requestJson, err := json.Marshal(task.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.LoadDataPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	partitionID := uint64(req.PartitionId)
	response := &proto.LoadDataPartitionResponse{}
	response.PartitionId = partitionID
	response.Used = 10 * util.GB
	response.PartitionSnapshot = buildSnapshot()
	response.Status = proto.TaskSucceeds
	var partition *MockDataPartition
	for _, partition = range mds.partitions {
		if partition.PartitionID == partitionID {
			break
		}
	}
	if partition == nil {
		return
	}
	//response.VolName = partition.VolName
	task.Response = response
	data, err := json.Marshal(task)
	if err != nil {
		response.PartitionId = partitionID
		response.Status = proto.TaskFailed
		response.Result = err.Error()
	}
	if err != nil {
		return
	}
	_, err = PostToMaster(http.MethodPost, urlDataNodeResponse, data)
	return
}

func buildSnapshot() (files []*proto.File) {
	files = make([]*proto.File, 0)
	f1 := &proto.File{
		Name:     "1",
		Crc:      4045512210,
		Size:     2 * util.MB,
		Modified: 1562507765,
	}
	files = append(files, f1)

	f2 := &proto.File{
		Name:     "2",
		Crc:      4045512210,
		Size:     2 * util.MB,
		Modified: 1562507765,
	}
	files = append(files, f2)

	f3 := &proto.File{
		Name:     "50000010",
		Crc:      4045512210,
		Size:     2 * util.MB,
		Modified: 1562507765,
	}
	files = append(files, f3)
	return
}
