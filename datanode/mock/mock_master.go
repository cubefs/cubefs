package mock

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"go.uber.org/atomic"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	TestCluster    = "cluster-mock"
	TestMasterHost = "127.0.0.1:10729"
	Master         = "master"
)
var mockMasterServer *mockMaster
type mockDataNode struct {
	HttpPort                  string
	ZoneName                  string
	Host                      string
	Version                   string
	ID                        uint64
	PersistenceDataPartitions []uint64
}

type mockMaster struct {
	MasterAddr   string
	CurNodeID    *atomic.Int64
	DataNodeMap  map[string]*mockDataNode
}

func NewMockMaster() {
	fmt.Println("init mock master")
	mockMasterServer = &mockMaster{
		MasterAddr: TestMasterHost,
		DataNodeMap: make(map[string]*mockDataNode, 0),
		CurNodeID: new(atomic.Int64),
	}
	profNetListener, err := net.Listen("tcp", TestMasterHost)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()
	go func() {
		http.HandleFunc(proto.AdminGetIP, fakeGetClusterInfo)
		http.HandleFunc(proto.AddDataNode, fakeAddDataNode)
		http.HandleFunc(proto.GetDataNode, fakeGetDataNode)
		http.HandleFunc(proto.AdminCreateDataPartition, fakeCreateDataPartition)
		http.HandleFunc(proto.AdminDeleteDataReplica, fakeDeleteDataReplica)
	}()
}

func fakeGetClusterInfo(w http.ResponseWriter, r *http.Request) {
	info := &proto.ClusterInfo{
		Cluster: TestCluster,
		Ip:      strings.Split(r.RemoteAddr, ":")[0],
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeAddDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr, httpPort, zoneName, version string
		err error
	)
	if nodeAddr, httpPort, zoneName, version, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	mockMasterServer.DataNodeMap[nodeAddr] = &mockDataNode{
		ID: uint64(mockMasterServer.CurNodeID.Add(1)),
		Host: nodeAddr,
		HttpPort: httpPort,
		ZoneName: zoneName,
		Version: version,
		PersistenceDataPartitions: make([]uint64, 0),
	}
	buildJSONResp(w, http.StatusOK, 1, Master, "")
}

func fakeGetDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		err       error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mockMasterServer.DataNodeMap[nodeAddr] == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "node not found"})
		return
	}

	info := &proto.DataNodeInfo{
		ID: 1060,
		Addr : mockMasterServer.DataNodeMap[nodeAddr].Host,
		PersistenceDataPartitions: mockMasterServer.DataNodeMap[nodeAddr].PersistenceDataPartitions,
	}
	buildJSONResp(w, http.StatusOK, info, Master, "")
}

func fakeCreateDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		id        uint64
		err       error
	)
	if nodeAddr, id, err = parseRequestForCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	node := mockMasterServer.DataNodeMap[nodeAddr]
	if node == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "node not found"})
		return
	}
	node.PersistenceDataPartitions = append(node.PersistenceDataPartitions, id)
	buildJSONResp(w, http.StatusOK, "success", Master, "")
}

func fakeDeleteDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		id        uint64
		err       error
	)
	if nodeAddr, id, err = parseRequestForCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	node := mockMasterServer.DataNodeMap[nodeAddr]
	if node == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "node not found"})
		return
	}
	newPersist := make([]uint64, 0)
	for _, dp := range node.PersistenceDataPartitions {
		if dp == id {
			continue
		}
		newPersist = append(newPersist, dp)
	}
	node.PersistenceDataPartitions = newPersist
	buildJSONResp(w, http.StatusOK, "success", Master, "")
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

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	// fmt.Printf("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		// fmt.Printf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		// fmt.Printf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}
func parseRequestForAddNode(r *http.Request) (nodeAddr, httpPort, zoneName, version string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if zoneName = r.FormValue("zoneName"); zoneName == "" {
		zoneName = "default"
	}

	if versionStr := r.FormValue("version"); versionStr == "" {
		version = "3.0.0"
	}

	if httpPort = r.FormValue("httpPort"); httpPort == "" {
		httpPort = "17320"
	}
	return
}
func parseRequestForCreateDataPartition(r *http.Request) (nodeAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractPartitionID(r); err != nil {
		return
	}
	return
}
func extractPartitionID(r *http.Request) (id uint64, err error) {
	var idStr string
	if idStr = r.FormValue("id"); idStr == "" {
		err = keyNotFound("id")
		return
	}
	id, err = strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return
	}
	return
}
func extractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue("addr"); nodeAddr == "" {
		err = keyNotFound("addr")
		return
	}
	return
}
func keyNotFound(name string) (err error) {
	return fmt.Errorf("parameter %v not found", name)
}

func parseAndExtractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractNodeAddr(r)
}