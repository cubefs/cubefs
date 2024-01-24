package metanode

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

var ClusterHasAuthKey bool
var RegErrorFlag bool
var GetIpErrorFlag bool

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func mockSend(w http.ResponseWriter, r *http.Request, reply []byte, contentType string) {
	w.Header().Set("content-type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func mockSendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	mockSend(w, r, reply, proto.JsonType)
	return
}

func mockGetIp(w http.ResponseWriter, r *http.Request) {
	if GetIpErrorFlag {
		mockSendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "get ip failed"})
		return
	}
	cInfo := &proto.ClusterInfo{
		Cluster:              "test",
		Ip:                   localIp,
		ClientReadLimitRate:  0,
		ClientWriteLimitRate: 0,
	}
	mockSendReply(w, r, newSuccessHTTPReply(cInfo))
}

func mockRegNode(w http.ResponseWriter, r *http.Request) {
	var checkKey string
	if ClusterHasAuthKey {
		checkKey = hex.EncodeToString(md5.New().Sum([]byte("test")))
	}

	if RegErrorFlag {
		mockSendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "invalid auth key"})
		return
	}
	mockSendReply(w, r, newSuccessHTTPReply(&proto.RegNodeRsp{
		Addr:    localIp + ":9092",
		Id:      10,
		Cluster: "test",
		AuthKey: checkKey}))
}

func mockMetaNodeAdd(w http.ResponseWriter, r *http.Request) {
	if RegErrorFlag {
		mockSendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "invalid auth key"})
		return
	}
	id := 10

	mockSendReply(w, r, newSuccessHTTPReply(id))
}

func mockInitHttpListen(port int) net.Listener {
	profNetListener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil
	}
	// 在prof端口监听上启动http API.
	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()

	return profNetListener
}

func mockNewMasterHttpReq() {
	//http.HandleFunc(proto.AdminGetIP, mockGetIp)
	http.HandleFunc(proto.RegNode, mockRegNode)
}

func mockOldMasterHttpReq() {
	http.HandleFunc(proto.AdminGetIP, mockGetIp)
	http.HandleFunc(proto.AddMetaNode, mockMetaNodeAdd)
}

func ClusterNotLocNot(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
	}()
	os.Mkdir(mt.metadataDir, 0655)
	if err := mt.register(); err != nil {
		t.Fatalf("reg metanode failed, %s", err.Error())
	}
}

func ClusterNotLocHas(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
	}()
	os.Mkdir(mt.metadataDir, 0655)
	os.WriteFile(path.Join(proto.AuthFilePath, masterSDK.AuthFileName+proto.RoleMeta), []byte("test"), 0655)

	if err := mt.register(); err != nil {
		t.Fatalf("reg metanode failed, %s", err.Error())
	}
}

func ClusterHasLocNot(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
		ClusterHasAuthKey = false
	}()
	ClusterHasAuthKey = true
	os.Mkdir(mt.metadataDir, 0655)

	if err := mt.register(); err != nil {
		t.Fatalf("reg metanode failed, %s", err.Error())
	}
}

func ClusterHasLocHas(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
		ClusterHasAuthKey = false
	}()
	ClusterHasAuthKey = true
	os.Mkdir(mt.metadataDir, 0655)
	os.WriteFile(path.Join(proto.AuthFilePath, masterSDK.AuthFileName+proto.RoleMeta),
		[]byte(hex.EncodeToString(md5.New().Sum([]byte("test")))), 0655)

	if err := mt.register(); err != nil {
		t.Fatalf("reg metanode failed, %s", err.Error())
	}
}

func GetIpFailed(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
		ClusterHasAuthKey = false
		GetIpErrorFlag = false
	}()
	ClusterHasAuthKey = true
	GetIpErrorFlag = true

	os.Mkdir(mt.metadataDir, 0655)
	os.WriteFile(path.Join(proto.AuthFilePath, masterSDK.AuthFileName+proto.RoleMeta), []byte("test"), 0655)
	if err := mt.register(); err == nil {
		t.Fatalf("local failed: reg metanode expect failed, but now success")
	} else {
		t.Logf("reg metanode failed:%s", err.Error())
	}

}

func LocCheckFailed(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
		ClusterHasAuthKey = false
	}()
	ClusterHasAuthKey = true

	os.Mkdir(mt.metadataDir, 0655)
	os.WriteFile(path.Join(proto.AuthFilePath, masterSDK.AuthFileName+proto.RoleMeta), []byte("test"), 0655)
	if err := mt.register(); err == nil {
		t.Fatalf("local failed: reg metanode expect failed, but now success")
	} else {
		t.Logf("reg metanode failed:%s", err.Error())
	}

}

func ClusterCheckError(mt *MetaNode, t *testing.T) {
	defer func() {
		os.RemoveAll(mt.metadataDir)
		RegErrorFlag = false
		ClusterHasAuthKey = false
	}()
	ClusterHasAuthKey = true
	RegErrorFlag = true
	os.Mkdir(mt.metadataDir, 0655)

	if err := mt.register(); err == nil {
		t.Fatalf("cluster failed: reg metanode expect failed, but now success")
	} else {
		t.Logf("reg metanode failed:%s", err.Error())
	}
}

var localIp string

func getLocalIp (t *testing.T) {
	nets, err := net.Interfaces()
	if err != nil {
		t.Fatalf("can not get sys interfaces info:%v", err.Error())
	}

	for _, iter := range nets {
		var addrIp []net.Addr
		addrIp, err = iter.Addrs()
		if err != nil {
			continue
		}
		if strings.Contains(iter.Name, "bo") ||  strings.Contains(iter.Name, "eth")  || strings.Contains(iter.Name, "enp") {
			for _, addr := range addrIp {
				if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					localIp = strings.Split(addr.String(), "/")[0]
					t.Logf("get local addr:%s", localIp)
					return
				}
			}
		}
	}
}

func TestRegMetaNodeOld(t *testing.T) {
	getLocalIp(t)
	var oldMaster net.Listener
	defer func() {
		if oldMaster != nil {
			oldMaster.Close()
		}
	}()
	oldMaster = mockInitHttpListen(18001)
	mockOldMasterHttpReq()
	masterClient = masterSDK.NewMasterClient([]string{localIp + ":18001"}, false)
	ClusterHasAuthKey = false
	mt := &MetaNode{
		zoneName:    "zone",
		listen:      "9092",
		metadataDir: proto.AuthFilePath,
	}

	ClusterNotLocNot(mt, t)
	ClusterCheckError(mt, t)
}

func TestRegMetaNodeNew(t *testing.T) {
	getLocalIp(t)
	var newMaster net.Listener

	defer func() {
		if newMaster != nil {
			newMaster.Close()
		}
	}()
	newMaster = mockInitHttpListen(18000)
	mockNewMasterHttpReq()
	masterClient = masterSDK.NewMasterClient([]string{localIp + ":18000"}, false)

	ClusterHasAuthKey = false
	mt := &MetaNode{
		zoneName:    "zone",
		listen:      "9092",
		metadataDir: proto.AuthFilePath,
	}

	ClusterNotLocNot(mt, t)
	ClusterNotLocHas(mt, t)
	ClusterHasLocNot(mt, t)
	ClusterHasLocHas(mt, t)
	GetIpFailed(mt, t)
	LocCheckFailed(mt, t)
	ClusterCheckError(mt, t)
}
