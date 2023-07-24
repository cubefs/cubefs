// Copyright 2018 The CFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/master/mocktest"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
)

const (
	hostAddr          = "http://127.0.0.1:8080"
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
	mds1Addr          = "127.0.0.1:9101"
	mds2Addr          = "127.0.0.1:9102"
	mds3Addr          = "127.0.0.1:9103"
	mds4Addr          = "127.0.0.1:9104"
	mds5Addr          = "127.0.0.1:9105"
	mds6Addr          = "127.0.0.1:9106"
	mds7Addr          = "127.0.0.1:9107"
	mds8Addr          = "127.0.0.1:9108"
	mds9Addr          = "127.0.0.1:9109"
	mds10Addr         = "127.0.0.1:9110"
	mds11Addr         = "127.0.0.1:9111"
	mds12Addr         = "127.0.0.1:9112"
	mds13Addr         = "127.0.0.1:9113"
	mds14Addr         = "127.0.0.1:9114"
	mds15Addr         = "127.0.0.1:9115"
	mds16Addr         = "127.0.0.1:9116"
	mds17Addr         = "127.0.0.1:9117"

	mms1Addr      = "127.0.0.1:8101"
	mms2Addr      = "127.0.0.1:8102"
	mms3Addr      = "127.0.0.1:8103"
	mms4Addr      = "127.0.0.1:8104"
	mms5Addr      = "127.0.0.1:8105"
	mms6Addr      = "127.0.0.1:8106"
	mms7Addr      = "127.0.0.1:8107"
	mms8Addr      = "127.0.0.1:8108"
	mms9Addr      = "127.0.0.1:8109"
	mms10Addr     = "127.0.0.1:8110"
	mms11Addr     = "127.0.0.1:8111"
	mms12Addr     = "127.0.0.1:8112"
	mms13Addr     = "127.0.0.1:8113"
	mms14Addr     = "127.0.0.1:8114"
	mms15Addr     = "127.0.0.1:8115"
	commonVolName = "commonVol"
	quorumVolName = "quorumVol"
	smartVolName  = "smartVol"
	testZone1     = "zone1"
	testZone2     = "zone2"
	testZone3     = "zone3"
	testZone4     = "zone4"
	testZone5     = "zone5"
	testZone6     = "zone6"
	testZone7     = "zone7"
	testZone8     = "zone8"
	testZone9     = "zone9"
	testRegion1   = "masterRegion1"
	testRegion2   = "masterRegion2"
	testRegion3   = "slaveRegion3"
	testRegion4   = "slaveRegion4"

	testUserID     = "testUser"
	ak             = "0123456789123456"
	sk             = "01234567891234560123456789123456"
	description    = "testUser"
	testSmartRules = "actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5:hdd"

	httpPort = "17520"
	ecs1Addr = "127.0.0.1:10301"
	ecs2Addr = "127.0.0.1:10302"
	ecs3Addr = "127.0.0.1:10303"
	ecs4Addr = "127.0.0.1:10304"
	ecs5Addr = "127.0.0.1:10305"
	ecs6Addr = "127.0.0.1:10306"
	ecs7Addr = "127.0.0.1:10307"
	ecs8Addr = "127.0.0.1:10308"
	ecs9Addr = "127.0.0.1:10309"

	mcs1Addr = "127.0.0.1:10201"
	mcs2Addr = "127.0.0.1:10202"
	mcs3Addr = "127.0.0.1:10203"

	mfs1Addr = "127.0.0.1:10501"
	mfs2Addr = "127.0.0.1:10502"
	mfs3Addr = "127.0.0.1:10503"
	mfs4Addr = "127.0.0.1:10504"
	mfs5Addr = "127.0.0.1:10505"
	mfs6Addr = "127.0.0.1:10506"
	mfs7Addr = "127.0.0.1:10507"
)

var server = createDefaultMasterServerForTest()
var commonVol *Vol
var quorumVol *Vol
var cfsUser *proto.UserInfo
var mc = master.NewMasterClient([]string{"127.0.0.1:8080"}, false)

func createDefaultMasterServerForTest() *Server {
	cfgJSON := `{
		"role": "master",
		"ip": "127.0.0.1",
		"listen": "8080",
		"prof":"10088",
		"id":"1",
		"peers": "1:127.0.0.1:8080",
		"retainLogs":"20000",
		"tickInterval":500,
		"electionTick":6,
		"logDir": "/tmp/chubaofs/Logs",
		"logLevel":"DEBUG",
		"walDir":"/tmp/chubaofs/raft",
		"storeDir":"/tmp/chubaofs/rocksdbstore",
		"clusterName":"chubaofs"
	}`
	testServer, err := createMasterServer(cfgJSON)
	if err != nil {
		panic(err)
	}
	testServer.config.nodeSetCapacity = defaultNodeSetCapacity
	//add data node
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)
	addDataServer(mds14Addr, testZone7)
	addDataServer(mds15Addr, testZone8)
	addDataServer(mds16Addr, testZone9)
	addDataServer(mds17Addr, testZone9)
	// add meta node
	addMetaServer(mms1Addr, testZone1)
	addMetaServer(mms2Addr, testZone1)
	addMetaServer(mms3Addr, testZone2)
	addMetaServer(mms4Addr, testZone2)
	addMetaServer(mms5Addr, testZone2)
	addMetaServer(mms7Addr, testZone3)
	addMetaServer(mms8Addr, testZone3)
	addMetaServer(mms11Addr, testZone6)
	addMetaServer(mms12Addr, testZone6)
	addMetaServer(mms13Addr, testZone6)
	addMetaServer(mms14Addr, testZone9)
	addMetaServer(mms15Addr, testZone9)

	// add ec node
	addEcServer(ecs1Addr, httpPort, testZone1)
	addEcServer(ecs2Addr, httpPort, testZone1)
	addEcServer(ecs3Addr, httpPort, testZone1)
	addEcServer(ecs4Addr, httpPort, testZone2)
	addEcServer(ecs5Addr, httpPort, testZone2)
	addEcServer(ecs6Addr, httpPort, testZone2)
	addEcServer(ecs7Addr, httpPort, testZone3)
	addEcServer(ecs8Addr, httpPort, testZone3)
	addEcServer(ecs9Addr, httpPort, testZone3)

	//add codec node
	addCodecServer(mcs1Addr, httpPort, testZone1)
	addCodecServer(mcs2Addr, httpPort, testZone1)
	addCodecServer(mcs3Addr, httpPort, testZone1)
	//add flash node
	addFlashServer(mfs1Addr, testZone1)
	addFlashServer(mfs2Addr, testZone1)
	addFlashServer(mfs3Addr, testZone1)
	addFlashServer(mfs4Addr, testZone2)
	addFlashServer(mfs5Addr, testZone2)
	addFlashServer(mfs6Addr, testZone2)
	addFlashServer(mfs7Addr, testZone2)
	time.Sleep(5 * time.Second)
	testServer.cluster.cfg = newClusterConfig()
	testServer.cluster.cfg.DataPartitionsRecoverPoolSize = maxDataPartitionsRecoverPoolSize
	testServer.cluster.cfg.MetaPartitionsRecoverPoolSize = maxMetaPartitionsRecoverPoolSize
	testServer.cluster.cfg.DeleteMarkDelVolInterval = defaultDeleteMarkDelVolInterval
	testServer.cluster.checkDataNodeHeartbeat()
	testServer.cluster.checkMetaNodeHeartbeat()
	testServer.cluster.checkEcNodeHeartbeat()
	testServer.cluster.checkCodecNodeHeartbeat()
	testServer.cluster.checkFlashNodeHeartbeat()
	testServer.cluster.cfg.nodeSetCapacity = defaultNodeSetCapacity
	time.Sleep(5 * time.Second)
	testServer.cluster.scheduleToUpdateStatInfo()
	vol, err := testServer.cluster.createVol(commonVolName, "cfs", testZone2, "", 3, 3, 3, 3, 100, 0, defaultEcDataNum, defaultEcParityNum, defaultEcEnable,
		false, false, false, false, true, false, false, false, 0, 0, defaultChildFileMaxCount,
		proto.StoreModeMem, proto.MetaPartitionLayout{0, 0}, []string{}, proto.CompactDefault, proto.DpFollowerReadDelayConfig{false, 0}, 0, 0, false)
	if err != nil {
		panic(err)
	}
	vol, err = testServer.cluster.getVol(commonVolName)
	if err != nil {
		panic(err)
	}
	commonVol = vol
	if err = createUserWithPolicy(testServer); err != nil {
		panic(err)
	}

	return testServer
}

func createUserWithPolicy(testServer *Server) (err error) {
	param := &proto.UserCreateParam{ID: "cfs", Type: proto.UserTypeNormal}
	if cfsUser, err = testServer.user.createKey(param); err != nil {
		return
	}
	paramTransfer := &proto.UserTransferVolParam{Volume: commonVolName, UserSrc: "cfs", UserDst: "cfs", Force: false}
	if cfsUser, err = testServer.user.transferVol(paramTransfer); err != nil {
		return
	}
	return nil
}

func createMasterServer(cfgJSON string) (server *Server, err error) {
	cfg := config.LoadConfigString(cfgJSON)
	server = NewServer()
	useConnPool = false
	logDir := cfg.GetString(ConfigKeyLogDir)
	walDir := cfg.GetString(WalDir)
	storeDir := cfg.GetString(StoreDir)
	profPort := cfg.GetString("prof")
	os.RemoveAll(logDir)
	os.RemoveAll(walDir)
	os.RemoveAll(storeDir)
	os.Mkdir(walDir, 0755)
	os.Mkdir(storeDir, 0755)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	var level log.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	if _, err = log.InitLog(logDir, "master", level, nil); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
		return
	}
	if profPort != "" {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf(":%v", profPort), nil)
			if err != nil {
				panic(fmt.Sprintf("cannot listen pprof %v err %v", profPort, err.Error()))
			}
		}()
	}
	if err = server.Start(cfg); err != nil {
		return
	}
	time.Sleep(5 * time.Second)
	return server, nil
}

func addDataServer(addr, zoneName string) (mds *mocktest.MockDataServer) {
	mds = mocktest.NewMockDataServer(addr, zoneName)
	mds.Start()
	return mds
}

func stopDataServer(mds *mocktest.MockDataServer) {
	dn, _ := server.cluster.dataNode(mds.TcpAddr)
	server.cluster.delDataNodeFromCache(dn)
	mds.Stop()
}

func addMetaServer(addr, zoneName string) (mms *mocktest.MockMetaServer) {
	mms = mocktest.NewMockMetaServer(addr, zoneName)
	mms.Start()
	return mms
}

func stopMetaServer(mms *mocktest.MockMetaServer) {
	mn, _ := server.cluster.metaNode(mms.TcpAddr)
	server.cluster.deleteMetaNodeFromCache(mn)
	mms.Stop()
}

func TestGetClusterView(t *testing.T) {
	server.cluster.responseCache = nil
	clusterView, err := mc.AdminAPI().GetCluster()
	assert.NoError(t, err)
	if len(server.cluster.responseCache) == 0 {
		t.Errorf("responseCache should not be empty")
	}

	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	err = json.Unmarshal(server.cluster.responseCache, body)
	assert.NoErrorf(t, err, "unmarshal response cache err:%v", err)
	cv := &proto.ClusterView{}
	err = json.Unmarshal(body.Data, &cv)
	assert.NoErrorf(t, err, "unmarshal data err:%v", err)
	assert.Equalf(t, cv, clusterView, "clusterView not equal clusterView:%v cv:%v", clusterView, cv)
}

func TestGetVol(t *testing.T) {
	volName := commonVolName
	volView, err := mc.ClientAPI().GetVolumeWithoutAuthKey(volName)
	if err != nil {
		t.Error(err)
	}

	vol, err := server.cluster.getVol(volName)
	if err != nil {
		t.Error(err)
	}
	viewCache := vol.getViewCache(proto.JsonType)
	if len(viewCache) == 0 {
		vol.updateViewCache(server.cluster, proto.JsonType)
		viewCache = vol.getViewCache(proto.JsonType)
	}
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(viewCache, body); err != nil {
		t.Errorf("unmarshal view cache err:%v", err)
	}
	vv := &proto.VolView{}
	if err = json.Unmarshal(body.Data, &vv); err != nil {
		t.Errorf("unmarshal data err:%v", err)
	}

	mpsJson := make(map[uint64]*proto.MetaPartitionView, len(vv.MetaPartitions))
	for _, v := range vv.MetaPartitions {
		mpsJson[v.PartitionID] = v
	}
	vv.MetaPartitions = nil
	mpsPb := make(map[uint64]*proto.MetaPartitionView, len(volView.MetaPartitions))
	for _, v := range volView.MetaPartitions {
		mpsPb[v.PartitionID] = v
	}
	volView.MetaPartitions = nil

	dpsJson := make(map[uint64]*proto.DataPartitionResponse, len(vv.DataPartitions))
	for _, v := range vv.DataPartitions {
		dpsJson[v.PartitionID] = v
	}
	vv.DataPartitions = nil
	dpsPb := make(map[uint64]*proto.DataPartitionResponse, len(volView.DataPartitions))
	for _, v := range volView.DataPartitions {
		dpsPb[v.PartitionID] = v
	}
	volView.DataPartitions = nil

	epsJson := make(map[uint64]*proto.EcPartitionResponse, len(vv.EcPartitions))
	for _, v := range vv.EcPartitions {
		epsJson[v.PartitionID] = v
	}
	vv.EcPartitions = nil
	epsPb := make(map[uint64]*proto.EcPartitionResponse, len(volView.EcPartitions))
	for _, v := range volView.EcPartitions {
		epsPb[v.PartitionID] = v
	}
	volView.EcPartitions = nil
	if !reflect.DeepEqual(volView, vv) {
		t.Errorf("volView from protobuf: %v not equal volView from json:%v", volView, vv)
	}
	if !reflect.DeepEqual(mpsJson, mpsPb) {
		t.Errorf("volView.MetaPartitions from protobuf: %v not equal volView.MetaPartitions from json:%v", mpsPb, mpsJson)
	}
	if !reflect.DeepEqual(dpsJson, dpsPb) {
		t.Errorf("volView.DataPartitions from protobuf: %v not equal volView.DataPartitions from json:%v", dpsPb, dpsJson)
	}
	if !reflect.DeepEqual(epsJson, epsPb) {
		t.Errorf("volView.EcPartitions from protobuf: %v not equal volView.EcPartitions from json:%v", epsPb, epsJson)
	}
}

func TestGetVolSimpleInfo(t *testing.T) {
	volName := commonVolName
	simpleVolView, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		t.Error(err)
	}

	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, commonVol.Name)
	reply := processReturnRawReply(reqURL, t)
	svv := &proto.SimpleVolView{}
	if err = json.Unmarshal(reply.Data, svv); err != nil {
		t.Errorf("unmarshal data err:%v", err)
	}
	if !reflect.DeepEqual(simpleVolView, svv) {
		t.Errorf("simpleVolView from protobuf: %v not equal simpleVolView from json:%v", simpleVolView, svv)
	}
}

func TestSetMetaNodeThreshold(t *testing.T) {
	threshold := 0.5
	reqURL := fmt.Sprintf("%v%v?threshold=%v", hostAddr, proto.AdminSetMetaNodeThreshold, threshold)
	process(reqURL, t)
	if !assert.Equalf(t, float32(threshold), server.cluster.cfg.MetaNodeThreshold, "set metanode threshold to %v failed", threshold) {
		return
	}
}

func TestSetDisableAutoAlloc(t *testing.T) {
	enable := true
	reqURL := fmt.Sprintf("%v%v?enable=%v", hostAddr, proto.AdminClusterFreeze, enable)
	process(reqURL, t)
	if !assert.Equalf(t, enable, server.cluster.DisableAutoAllocate, "set disableAutoAlloc to %v failed", enable) {
		return
	}
	server.cluster.DisableAutoAllocate = false
}

func TestGetCluster(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	process(reqURL, t)
}

func TestGetIpAndClusterName(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetIP)
	process(reqURL, t)
}

func TestGetLimitInfo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	process(reqURL, t)
}

// HTTPReply uniform response structure
type RawHTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func processReturnRawReply(reqURL string, t *testing.T) (reply *RawHTTPReply) {
	resp, err := http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply = &RawHTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Zerof(t, reply.Code, "failed,msg[%v],data[%v]", reply.Msg, reply.Data) {
		return
	}
	return
}

func processNoTerminal(reqURL string, t *testing.T) (reply *proto.HTTPReply, err error) {
	var resp *http.Response
	resp, err = http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply = &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	if reply.Code != 0 {
		err = fmt.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	return
}

func process(reqURL string, t *testing.T) (reply *proto.HTTPReply) {
	resp, err := http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply = &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Zerof(t, reply.Code, "failed,msg[%v],data[%v]", reply.Msg, reply.Data) {
		return
	}
	return
}

func TestDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs"
	decommissionDisk(addr, disk, t)
	decommissionDiskWithAuto(mds6Addr, disk, t)
}

func decommissionDisk(addr, path string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v",
		hostAddr, proto.DecommissionDisk, addr, path)
	resp, err := http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply := &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDiskRecoveryProgress()
}

func decommissionDiskWithAuto(addr, path string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v&auto=true",
		hostAddr, proto.DecommissionDisk, addr, path)
	resp, err := http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply := &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDiskRecoveryProgress()
}

func TestMarkDeleteVol(t *testing.T) {
	name := "delVol"
	createVol(name, testZone2, t)
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey("cfs"))
	process(reqURL, t)
	oldVol, err := server.cluster.getVol(name)
	assertErrNilOtherwiseFailNow(t, err)
	assert.NotEqual(t, oldVol.NewVolName, "")
	assert.NotEqual(t, oldVol.MarkDeleteTime, 0)
	assert.Equal(t, oldVol.RenameConvertStatus, proto.VolRenameConvertStatusOldVolInConvertPartition)
	newRenamedVol, err := server.cluster.getVol(oldVol.NewVolName)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, newRenamedVol.OldVolName, oldVol.Name)
	assert.Equal(t, newRenamedVol.ID, oldVol.NewVolID)
	assert.Equal(t, newRenamedVol.RenameConvertStatus, proto.VolRenameConvertStatusNewVolInConvertPartition)
	assert.Equal(t, newRenamedVol.FinalVolStatus, proto.VolStMarkDelete)
}

func assertErrNilOtherwiseFailNow(t *testing.T, err error) {
	if err != nil {
		assert.FailNow(t, err.Error())
	}
}
func assertErrNotNilOtherwiseFailNow(t *testing.T, err error) {
	if err == nil {
		assert.FailNow(t, err.Error())
	}
}
func TestUpdateVol(t *testing.T) {
	capacity := 2000
	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, proto.AdminUpdateVol, commonVol.Name, capacity, buildAuthKey("cfs"))
	process(reqURL, t)
	vol, err := server.cluster.getVol(commonVolName)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Falsef(t, vol.FollowerRead, "expect FollowerRead is false, but is %v", vol.FollowerRead) {
		return
	}

	reqURL = fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v&followerRead=true",
		hostAddr, proto.AdminUpdateVol, commonVol.Name, capacity, buildAuthKey("cfs"))
	process(reqURL, t)
	if !assert.Truef(t, vol.FollowerRead, "expect FollowerRead is true, but is %v", vol.FollowerRead) {
		return
	}

	reqURL = fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v&forceROW=true&ekExpireSec=60",
		hostAddr, proto.AdminUpdateVol, commonVol.Name, capacity, buildAuthKey("cfs"))
	process(reqURL, t)
	vol, err = server.cluster.getVol(commonVolName)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Truef(t, vol.ForceROW, "expect ForceROW is true, but is %v", vol.ForceROW) {
		return
	}
	if !assert.Equalf(t, int64(60), vol.ExtentCacheExpireSec, "expect ExtentCacheExpireSec is 60, but is %v", vol.ExtentCacheExpireSec) {
		return
	}
	// test enableWriteCache
	reqURL = fmt.Sprintf("%v%v?name=%v&authKey=%v&writeCache=true",
		hostAddr, proto.AdminUpdateVol, commonVol.Name, buildAuthKey("cfs"))
	process(reqURL, t)
	vol, err = server.cluster.getVol(commonVolName)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Truef(t, vol.enableWriteCache, "expect enableWriteCache is true, but is %v", vol.enableWriteCache) {
		return
	}
}
func buildAuthKey(owner string) string {
	h := md5.New()
	h.Write([]byte(owner))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func TestCreateVol(t *testing.T) {
	name := "test_create_vol"
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfstest&zoneName=%v", hostAddr, proto.AdminCreateVol, name, testZone2)
	process(reqURL, t)
	userInfo, err := server.user.getUserInfo("cfstest")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Containsf(t, userInfo.Policy.OwnVols, name, "expect vol %v in own vols, but is not", name) {
		return
	}
}

func TestCreateMetaPartition(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	commonVol.checkMetaPartitions(server.cluster)
	createMetaPartition(commonVol, t)
	commonVol.createTime = time.Now().Unix() - defaultAutoCreateDPAfterVolCreateSecond*2
}

func TestCreateDataPartition(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?count=2&name=%v&type=extent",
		hostAddr, proto.AdminCreateDataPartition, commonVol.Name)
	process(reqURL, t)
}

func TestGetDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.partitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	reqURL := fmt.Sprintf("%v%v?id=%v", hostAddr, proto.AdminGetDataPartition, partition.PartitionID)
	process(reqURL, t)

	reqURL = fmt.Sprintf("%v%v?id=%v&name=%v", hostAddr, proto.AdminGetDataPartition, partition.PartitionID, partition.VolName)
	process(reqURL, t)

	reqURL = fmt.Sprintf("%v%v?id=%v&name=%v", hostAddr, proto.AdminGetDataPartition, partition.PartitionID, partition.VolName+"wrongInfo")
	process(reqURL, t)
}

func TestLoadDataPartition(t *testing.T) {
	if !assert.NotZero(t, len(commonVol.dataPartitions.partitions), "no data partitions") {
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	reqURL := fmt.Sprintf("%v%v?id=%v&name=%v",
		hostAddr, proto.AdminLoadDataPartition, partition.PartitionID, commonVol.Name)
	process(reqURL, t)
}

func TestDataPartitionDecommission(t *testing.T) {
	if !assert.NotZero(t, len(commonVol.dataPartitions.partitions), "no data partitions") {
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	partition := commonVol.dataPartitions.partitions[0]
	partition.isRecover = false
	offlineAddr := partition.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v&force=true",
		hostAddr, proto.AdminDecommissionDataPartition, commonVol.Name, partition.PartitionID, offlineAddr)
	process(reqURL, t)
	if !assert.NotContainsf(t, partition.Hosts, offlineAddr, "offlineAddr[%v],hosts[%v]", offlineAddr, partition.Hosts) {
		return
	}
	partition.isRecover = false
}

func TestDataPartitionDecommissionWithoutReplica(t *testing.T) {
	if !assert.NotZero(t, len(commonVol.dataPartitions.partitions), "no data partitions") {
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	partition := commonVol.dataPartitions.partitions[0]
	partition.RLock()
	partition.isRecover = false
	offlineAddr := partition.Hosts[0]
	for _, replica := range partition.Replicas {
		if replica.DataReplica.Addr == offlineAddr {
			partition.removeReplicaByAddr(offlineAddr)
		}
	}
	partition.RUnlock()
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v&force=true",
		hostAddr, proto.AdminDecommissionDataPartition, commonVol.Name, partition.PartitionID, offlineAddr)
	process(reqURL, t)
	if !assert.NotContainsf(t, partition.Hosts, offlineAddr, "offlineAddr[%v],hosts[%v]", offlineAddr, partition.Hosts) {
		return
	}
	if len(partition.Hosts) == 2 || len(partition.Replicas) == 2 {
		t.Errorf("dp decommissionWithoutReplica failed,hosts[%v],replicas[%v]", len(partition.Hosts), len(partition.Replicas))
		return
	}
	partition.isRecover = false
}

//	func TestGetAllVols(t *testing.T) {
//		reqURL := fmt.Sprintf("%v%v", hostAddr, proto.GetALLVols)
//		process(reqURL, t)
//	}
func TestGetMetaPartitions(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientMetaPartitions, commonVolName)
	process(reqURL, t)
}

func TestGetDataPartitions(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientDataPartitions, commonVolName)
	process(reqURL, t)
}

func TestGetTopo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.GetTopologyView)
	process(reqURL, t)
}

func TestGetMetaNode(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, mms1Addr)
	process(reqURL, t)
}

func TestAddDataReplica(t *testing.T) {
	partition := commonVol.dataPartitions.partitions[0]
	dsAddr := mds10Addr
	addDataServer(dsAddr, "zone2")
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if !assert.Containsf(t, partition.Hosts, dsAddr, "hosts[%v] should contains dsAddr[%v]", partition.Hosts, dsAddr) {
		partition.RUnlock()
		return
	}
	partition.RUnlock()
	server.cluster.BadDataPartitionIds.Range(
		func(key, value interface{}) bool {
			addr, ok := key.(string)
			if !ok {
				return true
			}
			if strings.HasPrefix(addr, dsAddr) {
				server.cluster.BadDataPartitionIds.Delete(key)
			}
			return true
		})
}

func TestRemoveDataReplica(t *testing.T) {
	partition := commonVol.dataPartitions.partitions[0]
	partition.isRecover = false
	dsAddr := mds10Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if !assert.NotContainsf(t, partition.Hosts, dsAddr, "hosts[%v] should not contains dsAddr[%v]", partition.Hosts, dsAddr) {
		partition.RUnlock()
		return
	}
	partition.isRecover = false
	partition.RUnlock()
}

func TestResetDataReplica(t *testing.T) {
	partition := commonVol.dataPartitions.partitions[9]
	var inActiveDataNode []*DataNode
	var activeHosts []string
	for i, host := range partition.Hosts {
		if i < 2 {
			dataNode, _ := server.cluster.dataNode(host)
			dataNode.isActive = false
			inActiveDataNode = append(inActiveDataNode, dataNode)
			continue
		}
		activeHosts = append(activeHosts, host)
	}
	partition.isRecover = false
	reqURL := fmt.Sprintf("%v%v?id=%v", hostAddr, proto.AdminResetDataPartition, partition.PartitionID)
	process(reqURL, t)
	partition.Lock()
	defer partition.Unlock()
	if !assert.Lenf(t, partition.Hosts, 1, "hosts[%v] reset peers of data partition failed", partition.Hosts) {
		return
	}
	partition.isRecover = false
	for _, dataNode := range inActiveDataNode {
		if !assert.NotContainsf(t, partition.Hosts, dataNode.Addr, "hosts[%v] should not contains inactiveAddr[%v]", partition.Hosts, dataNode.Addr) {
			return
		}
		dataNode.isActive = true
	}
}

func TestAddMetaReplica(t *testing.T) {
	maxPartitionID := commonVol.maxPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if !assert.NotNil(t, partition, "no meta partition") {
		return
	}
	msAddr := mms9Addr
	addMetaServer(msAddr, testZone2)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(2 * time.Second)
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddMetaReplica, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if !assert.Containsf(t, partition.Hosts, msAddr, "hosts[%v] should contains msAddr[%v]", partition.Hosts, msAddr) {
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestRemoveMetaReplica(t *testing.T) {
	maxPartitionID := commonVol.maxPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if !assert.NotNil(t, partition, "no meta partition") {
		return
	}
	partition.IsRecover = false
	msAddr := mms9Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteMetaReplica, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if !assert.NotContainsf(t, partition.Hosts, msAddr, "hosts[%v] should contains msAddr[%v]", partition.Hosts, msAddr) {
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestAddDataLearner(t *testing.T) {
	partition := commonVol.dataPartitions.partitions[0]
	dsAddr := mds10Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddDataReplicaLearner, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, dsAddr) || !containsLearner(partition.Learners, dsAddr) {
		t.Errorf("hosts[%v] and learners[%v] should contains dsAddr[%v]", partition.Hosts, partition.Learners, dsAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
	// remove
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, dsAddr) || containsLearner(partition.Learners, dsAddr) {
		t.Errorf("hosts[%v] or learners[%v] shouldn't contains dsAddr[%v]", partition.Hosts, partition.Learners, dsAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestPromoteDataLearner(t *testing.T) {
	time.Sleep(2 * time.Second)
	partition := commonVol.dataPartitions.partitions[0]
	dsAddr := mds10Addr
	// add
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddDataReplicaLearner, partition.PartitionID, dsAddr)
	process(reqURL, t)
	// promote
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminPromoteDataReplicaLearner, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, dsAddr) || containsLearner(partition.Learners, dsAddr) {
		t.Errorf("hosts[%v] should contains dsAddr[%v], but learners[%v] shouldn't contain", partition.Hosts, dsAddr, partition.Learners)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
	partition.isRecover = false
	// remove
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, dsAddr) || containsLearner(partition.Learners, dsAddr) {
		t.Errorf("hosts[%v] or learners[%v] shouldn't contains dsAddr[%v]", partition.Hosts, partition.Learners, dsAddr)
		partition.RUnlock()
		return
	}
	partition.isRecover = false
	partition.RUnlock()
}

func TestAddMetaLearner(t *testing.T) {
	maxPartitionID := commonVol.maxPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if !assert.NotNil(t, partition, "no meta partition") {
		return
	}
	msAddr := mms9Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddMetaReplicaLearner, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, msAddr) || !containsLearner(partition.Learners, msAddr) {
		t.Errorf("hosts[%v] and learners[%v] should contains msAddr[%v]", partition.Hosts, partition.Learners, msAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
	// remove
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteMetaReplica, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, msAddr) || containsLearner(partition.Learners, msAddr) {
		t.Errorf("hosts[%v] or learners[%v] shouldn't contains msAddr[%v]", partition.Hosts, partition.Learners, msAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func containsLearner(learners []proto.Learner, addr string) bool {
	for _, learner := range learners {
		if learner.Addr == addr {
			return true
		}
	}
	return false
}

func TestPromoteMetaLearner(t *testing.T) {
	time.Sleep(2 * time.Second)
	maxPartitionID := commonVol.maxPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if !assert.NotNil(t, partition, "no meta partition") {
		return
	}
	msAddr := mms9Addr
	// add
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddMetaReplicaLearner, partition.PartitionID, msAddr)
	process(reqURL, t)
	// promote
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminPromoteMetaReplicaLearner, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, msAddr) || containsLearner(partition.Learners, msAddr) {
		t.Errorf("hosts[%v] should contains msAddr[%v], but learners[%v] shouldn't contain", partition.Hosts, msAddr, partition.Learners)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
	partition.IsRecover = false
	// remove
	reqURL = fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteMetaReplica, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, msAddr) || containsLearner(partition.Learners, msAddr) {
		t.Errorf("hosts[%v] or learners[%v] shouldn't contains msAddr[%v]", partition.Hosts, partition.Learners, msAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestResetMetaReplica(t *testing.T) {
	maxPartitionID := commonVol.maxPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if !assert.NotNil(t, partition, "no meta partition") {
		return
	}
	var inActiveMetaNode []*MetaNode
	for i, host := range partition.Hosts {
		if i < 2 {
			metaNode, _ := server.cluster.metaNode(host)
			metaNode.IsActive = false
			inActiveMetaNode = append(inActiveMetaNode, metaNode)
		}
	}
	partition.IsRecover = false
	reqURL := fmt.Sprintf("%v%v?id=%v", hostAddr, proto.AdminResetMetaPartition, partition.PartitionID)
	process(reqURL, t)
	partition.Lock()
	defer partition.Unlock()

	if len(partition.Hosts) != 1 {
		t.Errorf("hosts[%v] reset peers of meta partition  failed", partition.Hosts)
		return
	}
	partition.IsRecover = false
	for _, metaNode := range inActiveMetaNode {
		if !assert.NotContainsf(t, partition.Hosts, metaNode.Addr, "hosts[%v] should not contains inactiveAddr[%v]", partition.Hosts, metaNode.Addr) {
			return
		}
		metaNode.IsActive = true
	}

}

func TestAddToken(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&tokenType=%v&authKey=%v",
		hostAddr, proto.TokenAddURI, commonVol.Name, proto.ReadWriteToken, buildAuthKey("cfs"))
	process(reqUrl, t)
}

func TestDelToken(t *testing.T) {
	for _, token := range commonVol.tokens {
		reqUrl := fmt.Sprintf("%v%v?name=%v&token=%v&authKey=%v",
			hostAddr, proto.TokenDelURI, commonVol.Name, token.Value, buildAuthKey("cfs"))
		process(reqUrl, t)
		_, ok := commonVol.tokens[token.Value]
		if ok {
			t.Errorf("delete token[%v] failed\n", token.Value)
			return
		}

		reqUrl = fmt.Sprintf("%v%v?name=%v&tokenType=%v&authKey=%v",
			hostAddr, proto.TokenAddURI, commonVol.Name, token.TokenType, buildAuthKey("cfs"))
		process(reqUrl, t)
	}
}

func TestUpdateToken(t *testing.T) {
	var tokenType int8
	for _, token := range commonVol.tokens {
		if token.TokenType == proto.ReadWriteToken {
			tokenType = proto.ReadOnlyToken
		} else {
			tokenType = proto.ReadWriteToken
		}

		reqUrl := fmt.Sprintf("%v%v?name=%v&token=%v&tokenType=%v&authKey=%v",
			hostAddr, proto.TokenUpdateURI, commonVol.Name, token.Value, tokenType, buildAuthKey("cfs"))
		process(reqUrl, t)
		token := commonVol.tokens[token.Value]
		if !assert.Equalf(t, tokenType, token.TokenType, "expect tokenType[%v],real tokenType[%v]\n", tokenType, token.TokenType) {
			return
		}
	}
}

func TestGetToken(t *testing.T) {
	for _, token := range commonVol.tokens {
		reqUrl := fmt.Sprintf("%v%v?name=%v&token=%v",
			hostAddr, proto.TokenGetURI, commonVol.Name, token.Value)
		process(reqUrl, t)
	}
}

func TestClusterStat(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminClusterStat)
	process(reqUrl, t)
}

func TestClusterStatWithZoneTag(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?zoneTag=ssd", hostAddr, proto.AdminClusterStat)
	process(reqUrl, t)
	reqUrl = fmt.Sprintf("%v%v?zoneTag=hdd", hostAddr, proto.AdminClusterStat)
	process(reqUrl, t)
}

func TestListVols(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?keywords=%v", hostAddr, proto.AdminListVols, commonVolName)
	process(reqURL, t)
}

func post(reqURL string, data []byte, t *testing.T) (reply *proto.HTTPReply) {
	reader := bytes.NewReader(data)
	req, err := http.NewRequest(http.MethodPost, reqURL, reader)
	if !assert.NoErrorf(t, err, "generate request err: %v", err) {
		return
	}
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if !assert.NoErrorf(t, err, "post err: %v", err) {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply = &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Zerof(t, reply.Code, "failed,msg[%v],data[%v]", reply.Msg, reply.Data) {
		return
	}
	return
}

func TestCreateUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserCreate)
	param := &proto.UserCreateParam{ID: testUserID, Type: proto.UserTypeNormal}
	data, err := json.Marshal(param)
	if !assert.NoError(t, err) {
		return
	}
	post(reqURL, data, t)
}

func TestGetUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?user=%v", hostAddr, proto.UserGetInfo, testUserID)
	process(reqURL, t)
}

func TestUpdateUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserUpdate)
	param := &proto.UserUpdateParam{UserID: testUserID, AccessKey: ak, SecretKey: sk, Type: proto.UserTypeAdmin, Description: description}
	data, err := json.Marshal(param)
	if !assert.NoError(t, err) {
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, ak, userInfo.AccessKey, "expect ak[%v], real ak[%v]\n", ak, userInfo.AccessKey) {
		return
	}
	if !assert.Equalf(t, sk, userInfo.SecretKey, "expect sk[%v], real sk[%v]\n", sk, userInfo.SecretKey) {
		return
	}
	if !assert.Equalf(t, proto.UserTypeAdmin, userInfo.UserType, "expect ak[%v], real ak[%v]\n", proto.UserTypeAdmin, userInfo.UserType) {
		return
	}
	if !assert.Equalf(t, description, userInfo.Description, "expect description[%v], real description[%v]\n", description, userInfo.Description) {
		return
	}
}

func TestGetAKInfo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?ak=%v", hostAddr, proto.UserGetAKInfo, ak)
	process(reqURL, t)
}

func TestUpdatePolicy(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserUpdatePolicy)
	param := &proto.UserPermUpdateParam{UserID: testUserID, Volume: commonVolName, Policy: []string{proto.BuiltinPermissionWritable.String()}}
	data, err := json.Marshal(param)
	if !assert.NoError(t, err) {
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if !assert.NoError(t, err) {
		return
	}
	_, exist := userInfo.Policy.AuthorizedVols[commonVolName]
	if !assert.Truef(t, exist, "expect vol %v in authorized vols, but is not", commonVolName) {
		return
	}
}

func TestRemovePolicy(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserRemovePolicy)
	param := &proto.UserPermRemoveParam{UserID: testUserID, Volume: commonVolName}
	data, err := json.Marshal(param)
	if !assert.NoError(t, err) {
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if !assert.NoError(t, err) {
		return
	}
	_, exist := userInfo.Policy.AuthorizedVols[commonVolName]
	if !assert.Falsef(t, exist, "expect no vol %v in authorized vols, but is exist", commonVolName) {
		return
	}
}

func TestTransferVol(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserTransferVol)
	param := &proto.UserTransferVolParam{Volume: commonVolName, UserSrc: "cfs", UserDst: testUserID, Force: false}
	data, err := json.Marshal(param)
	if !assert.NoError(t, err) {
		return
	}
	post(reqURL, data, t)
	userInfo1, err := server.user.getUserInfo(testUserID)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Containsf(t, userInfo1.Policy.OwnVols, commonVolName, "expect vol %v in own vols, but is not", commonVolName) {
		return
	}
	userInfo2, err := server.user.getUserInfo("cfs")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotContainsf(t, userInfo2.Policy.OwnVols, commonVolName, "expect no vol %v in own vols, but is exist", commonVolName) {
		return
	}
	vol, err := server.cluster.getVol(commonVolName)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, testUserID, vol.Owner, "expect owner is %v, but is %v", testUserID, vol.Owner) {
		return
	}
}

func TestDeleteVolPolicy(t *testing.T) {
	param := &proto.UserPermUpdateParam{UserID: "cfs", Volume: commonVolName, Policy: []string{proto.BuiltinPermissionWritable.String()}}
	_, err := server.user.updatePolicy(param)
	if !assert.NoError(t, err) {
		return
	}
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.UserDeleteVolPolicy, commonVolName)
	process(reqURL, t)
	userInfo1, err := server.user.getUserInfo(testUserID)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotContainsf(t, userInfo1.Policy.OwnVols, commonVolName, "expect no vol %v in own vols, but is not", commonVolName) {
		return
	}
	userInfo2, err := server.user.getUserInfo("cfs")
	if !assert.NoError(t, err) {
		return
	}
	_, exist := userInfo2.Policy.AuthorizedVols[commonVolName]
	if !assert.Falsef(t, exist, "expect no vols %v in authorized vol is 0, but is exist", commonVolName) {
		return
	}
}

func TestListUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?keywords=%v", hostAddr, proto.UserList, "test")
	process(reqURL, t)
}

func TestDeleteUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?user=%v", hostAddr, proto.UserDelete, testUserID)
	process(reqURL, t)
	_, err := server.user.getUserInfo(testUserID)
	assert.ErrorIs(t, err, proto.ErrUserNotExists)
}

func TestListUsersOfVol(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.UsersOfVol, "test_create_vol")
	process(reqURL, t)
}

func TestMergeNodeSetAPI(t *testing.T) {
	topo := server.cluster.t
	zoneNodeSet1 := newZone("zone-ns1")
	nodeSet1 := newNodeSet(501, 18, zoneNodeSet1.name)
	nodeSet2 := newNodeSet(502, 18, zoneNodeSet1.name)
	zoneNodeSet1.putNodeSet(nodeSet1)
	zoneNodeSet1.putNodeSet(nodeSet2)
	topo.putZone(zoneNodeSet1)
	topo.putDataNode(createDataNodeForNodeSet(mds6Addr, httpPort, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putDataNode(createDataNodeForNodeSet(mds7Addr, httpPort, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putDataNode(createDataNodeForNodeSet(mds8Addr, httpPort, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putDataNode(createDataNodeForNodeSet(mds9Addr, httpPort, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putMetaNode(createMetaNodeForNodeSet(mms6Addr, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putMetaNode(createMetaNodeForNodeSet(mms7Addr, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putMetaNode(createMetaNodeForNodeSet(mms8Addr, zoneNodeSet1.name, server.cluster.Name, nodeSet1))
	topo.putMetaNode(createMetaNodeForNodeSet(mms9Addr, zoneNodeSet1.name, server.cluster.Name, nodeSet1))

	// with node addr
	fmt.Printf("before merge dataNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())
	reqURL := fmt.Sprintf("%v%v?nodeType=%v&zoneName=%v&source=%v&target=%v&addr=%v", hostAddr, proto.AdminMergeNodeSet,
		"dataNode", zoneNodeSet1.name, nodeSet1.ID, nodeSet2.ID, mds6Addr)
	process(reqURL, t)
	fmt.Printf("after merge dataNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())
	_, existInNodeSet1 := nodeSet1.dataNodes.Load(mds6Addr)
	_, existInNodeSet2 := nodeSet1.dataNodes.Load(mds6Addr)
	fmt.Printf("node:%v,existInNodeSet1:%v,existInNodeSet2:%v\n", mds6Addr, existInNodeSet1, existInNodeSet2)

	fmt.Printf("before merge metaNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())
	reqURL = fmt.Sprintf("%v%v?nodeType=%v&zoneName=%v&source=%v&target=%v&addr=%v", hostAddr, proto.AdminMergeNodeSet,
		"metaNode", zoneNodeSet1.name, nodeSet1.ID, nodeSet2.ID, mms9Addr)
	process(reqURL, t)
	fmt.Printf("after merge metaNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())
	_, existInNodeSet1 = nodeSet1.metaNodes.Load(mms9Addr)
	_, existInNodeSet2 = nodeSet1.metaNodes.Load(mms9Addr)
	fmt.Printf("node:%v,existInNodeSet1:%v,existInNodeSet2:%v\n", mms9Addr, existInNodeSet1, existInNodeSet2)

	// with count
	fmt.Printf("before batch merge dataNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())
	reqURL = fmt.Sprintf("%v%v?nodeType=%v&zoneName=%v&source=%v&target=%v&count=2", hostAddr, proto.AdminMergeNodeSet,
		"dataNode", zoneNodeSet1.name, nodeSet1.ID, nodeSet2.ID)
	process(reqURL, t)
	fmt.Printf("after batch merge dataNode [nodeSet:dataNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.dataNodeCount(), nodeSet2.ID, nodeSet2.dataNodeCount())

	fmt.Printf("before batch merge metaNode [nodeSet:metaNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.metaNodeCount(), nodeSet2.ID, nodeSet2.metaNodeCount())
	reqURL = fmt.Sprintf("%v%v?nodeType=%v&zoneName=%v&source=%v&target=%v&count=9", hostAddr, proto.AdminMergeNodeSet,
		"metaNode", zoneNodeSet1.name, nodeSet1.ID, nodeSet2.ID)
	process(reqURL, t)
	fmt.Printf("after batch merge metaNode [nodeSet:metaNodeCount] [%v:%v],[%v:%v]\n", nodeSet1.ID, nodeSet1.metaNodeCount(), nodeSet2.ID, nodeSet2.metaNodeCount())

}

func TestCheckMergeZoneNodeset(t *testing.T) {
	// test merge node set of zone automatically
	topo := server.cluster.t
	clusterID := server.cluster.Name
	zoneNodeSet1, err := topo.getZone("zone-ns1")
	if !assert.NoError(t, err) {
		return
	}
	nodeSet3 := newNodeSet(503, 18, zoneNodeSet1.name)
	zoneNodeSet1.putNodeSet(nodeSet3)
	batchCreateDataNodeForNodeSet(topo, nodeSet3, zoneNodeSet1.name, clusterID, "127.0.0.1:3", 3)
	batchCreateMetaNodeForNodeSet(topo, nodeSet3, zoneNodeSet1.name, clusterID, "127.0.0.1:3", 18)

	nodeSet4 := newNodeSet(504, 18, zoneNodeSet1.name)
	zoneNodeSet1.putNodeSet(nodeSet4)
	batchCreateDataNodeForNodeSet(topo, nodeSet4, zoneNodeSet1.name, clusterID, "127.0.0.1:4", 9)
	batchCreateMetaNodeForNodeSet(topo, nodeSet4, zoneNodeSet1.name, clusterID, "127.0.0.1:4", 8)

	zoneNodeSet2 := newZone("zone-ns2")
	nodeSet21 := newNodeSet(521, 18, zoneNodeSet2.name)
	nodeSet22 := newNodeSet(522, 18, zoneNodeSet2.name)
	nodeSet23 := newNodeSet(523, 18, zoneNodeSet2.name)
	nodeSet24 := newNodeSet(524, 18, zoneNodeSet2.name)
	nodeSet25 := newNodeSet(525, 18, zoneNodeSet2.name)
	nodeSet26 := newNodeSet(526, 18, zoneNodeSet2.name)
	nodeSet27 := newNodeSet(527, 18, zoneNodeSet2.name)
	nodeSet28 := newNodeSet(528, 18, zoneNodeSet2.name)
	nodeSet29 := newNodeSet(529, 18, zoneNodeSet2.name)
	zoneNodeSet2.putNodeSet(nodeSet21)
	zoneNodeSet2.putNodeSet(nodeSet22)
	zoneNodeSet2.putNodeSet(nodeSet23)
	zoneNodeSet2.putNodeSet(nodeSet24)
	zoneNodeSet2.putNodeSet(nodeSet25)
	zoneNodeSet2.putNodeSet(nodeSet26)
	zoneNodeSet2.putNodeSet(nodeSet27)
	zoneNodeSet2.putNodeSet(nodeSet28)
	zoneNodeSet2.putNodeSet(nodeSet29)
	topo.putZone(zoneNodeSet2)
	batchCreateDataNodeForNodeSet(topo, nodeSet21, zoneNodeSet2.name, clusterID, "127.0.0.1:21", 18)
	batchCreateDataNodeForNodeSet(topo, nodeSet22, zoneNodeSet2.name, clusterID, "127.0.0.1:22", 17)
	batchCreateDataNodeForNodeSet(topo, nodeSet23, zoneNodeSet2.name, clusterID, "127.0.0.1:23", 3)
	batchCreateDataNodeForNodeSet(topo, nodeSet24, zoneNodeSet2.name, clusterID, "127.0.0.1:24", 1)
	batchCreateDataNodeForNodeSet(topo, nodeSet25, zoneNodeSet2.name, clusterID, "127.0.0.1:25", 1)
	batchCreateDataNodeForNodeSet(topo, nodeSet26, zoneNodeSet2.name, clusterID, "127.0.0.1:26", 2)
	batchCreateMetaNodeForNodeSet(topo, nodeSet21, zoneNodeSet2.name, clusterID, "127.0.0.1:221", 3)
	batchCreateMetaNodeForNodeSet(topo, nodeSet22, zoneNodeSet2.name, clusterID, "127.0.0.1:222", 4)
	batchCreateMetaNodeForNodeSet(topo, nodeSet24, zoneNodeSet2.name, clusterID, "127.0.0.1:223", 5)
	batchCreateMetaNodeForNodeSet(topo, nodeSet25, zoneNodeSet2.name, clusterID, "127.0.0.1:224", 8)
	batchCreateMetaNodeForNodeSet(topo, nodeSet26, zoneNodeSet2.name, clusterID, "127.0.0.1:225", 8)
	batchCreateMetaNodeForNodeSet(topo, nodeSet27, zoneNodeSet2.name, clusterID, "127.0.0.1:226", 9)
	batchCreateMetaNodeForNodeSet(topo, nodeSet28, zoneNodeSet2.name, clusterID, "127.0.0.1:227", 15)
	batchCreateMetaNodeForNodeSet(topo, nodeSet29, zoneNodeSet2.name, clusterID, "127.0.0.1:228", 13)

	getZoneNodeSetStatus(zoneNodeSet1)
	getZoneNodeSetStatus(zoneNodeSet2)
	server.cluster.checkMergeZoneNodeset()
	getZoneNodeSetStatus(zoneNodeSet1)
	getZoneNodeSetStatus(zoneNodeSet2)
	getZoneNodeSetStatus(zoneNodeSet1)
	getZoneNodeSetStatus(zoneNodeSet2)
	for i := 0; i < 30; i++ {
		server.cluster.checkMergeZoneNodeset()
	}
	getZoneNodeSetStatus(zoneNodeSet1)
	getZoneNodeSetStatus(zoneNodeSet2)

}

func TestApplyAndGetVolWriteMutex(t *testing.T) {
	// apply volume write mutex
	applyReqURL := fmt.Sprintf("%v%v?app=coraldb&name=%v&addr=127.0.0.1:10090&slaves=127.0.0.1:10094,127.0.0.1:10095&addslave=127.0.0.1:10096", hostAddr, proto.AdminApplyVolMutex, commonVolName)
	applyReply := process(applyReqURL, t)
	assert.Equalf(t, "apply volume mutex success", applyReply.Data.(string), "apply volume mutex failed, responseInfo: %v", applyReply.Data)

	// get volume write mutex
	getReqURL := fmt.Sprintf("%v%v?app=coraldb&name=%v", hostAddr, proto.AdminGetVolMutex, commonVolName)
	reply := processReturnRawReply(getReqURL, t)
	mutexInfo := &proto.VolWriteMutexInfo{}
	err := json.Unmarshal(reply.Data, mutexInfo)
	assert.NoError(t, err)
	if mutexInfo.Enable != true || mutexInfo.Holder != "127.0.0.1:10090" || len(mutexInfo.Slaves) != 3 {
		expect := &proto.VolWriteMutexInfo{
			Enable: true,
			Holder: "127.0.0.1:10090",
			Slaves: map[string]string{"127.0.0.1:10094": "xxx", "127.0.0.1:10095": "xxx", "127.0.0.1:10096": "xxx"},
		}
		t.Errorf("Get wrong volume write mutex. expect: %v, actual: %v", expect, mutexInfo)
	}
}

func TestReleaseAndGetVolWriteMutex(t *testing.T) {
	// release volume write mutex
	releaseReqURL := fmt.Sprintf("%v%v?app=coraldb&name=%v&addr=127.0.0.1:10090", hostAddr, proto.AdminReleaseVolMutex, commonVolName)
	releaseReply := process(releaseReqURL, t)
	assert.Equalf(t, "release volume mutex success", releaseReply.Data.(string), "Release volume write mutest failed, errorInfo: %v", releaseReply.Data)

	// get volume write mutex
	getReqURL := fmt.Sprintf("%v%v?app=coraldb&name=%v", hostAddr, proto.AdminGetVolMutex, commonVolName)
	reply := processReturnRawReply(getReqURL, t)
	mutexInfo := &proto.VolWriteMutexInfo{}
	err := json.Unmarshal(reply.Data, mutexInfo)
	assert.NoError(t, err)
	if mutexInfo.Enable != true || mutexInfo.Holder != "" || len(mutexInfo.Slaves) != 3 {
		expect := &proto.VolWriteMutexInfo{
			Enable: true,
			Holder: "",
			Slaves: map[string]string{"127.0.0.1:10094": "xxx", "127.0.0.1:10095": "xxx", "127.0.0.1:10096": "xxx"},
		}
		t.Errorf("Get wrong volume write mutex. expect: %v, actual: %v", expect, mutexInfo)
	}
}

func TestSetNodeInfoHandler(t *testing.T) {

	deleteRecord := 10
	reqURL := fmt.Sprintf("%v%v?fixTinyDeleteRecordKey=%v", hostAddr, proto.AdminSetNodeInfo, deleteRecord)
	process(reqURL, t)
	if !assert.Equalf(t, uint64(deleteRecord), server.cluster.dnFixTinyDeleteRecordLimit, "set fixTinyDeleteRecordKey to %v failed", deleteRecord) {
		return
	}
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	reply := processReturnRawReply(reqURL, t)
	limitInfo := &proto.LimitInfo{}

	err := json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	assert.Equalf(t, uint64(deleteRecord), limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk, "deleteRecordLimit expect:%v,real:%v", deleteRecord, limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
}

func TestSetVolConvertModeOfDPConvertMode(t *testing.T) {
	volName := commonVolName
	reqURL := fmt.Sprintf("%v%v?name=%v&partitionType=dataPartition&convertMode=1", hostAddr, proto.AdminSetVolConvertMode, volName)
	process(reqURL, t)
	volumeSimpleInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoErrorf(t, err, "vol:%v GetVolumeSimpleInfo err:%v", volName, err) {
		return
	}
	assert.Equalf(t, proto.IncreaseReplicaNum, volumeSimpleInfo.DPConvertMode, "expect volName:%v DPConvertMode is 1, but is %v", volName, volumeSimpleInfo.DPConvertMode)

	reqURL = fmt.Sprintf("%v%v?name=%v&partitionType=dataPartition&convertMode=0", hostAddr, proto.AdminSetVolConvertMode, volName)
	process(reqURL, t)
	volumeSimpleInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoErrorf(t, err, "vol:%v GetVolumeSimpleInfo err:%v", volName, err) {
		return
	}
	assert.Zerof(t, volumeSimpleInfo.DPConvertMode, "expect volName:%v DPConvertMode is 0, but is %v", volName, volumeSimpleInfo.DPConvertMode)
}

func TestSetVolConvertModeOfMPConvertMode(t *testing.T) {
	volName := commonVolName
	reqURL := fmt.Sprintf("%v%v?name=%v&partitionType=metaPartition&convertMode=1", hostAddr, proto.AdminSetVolConvertMode, volName)
	process(reqURL, t)
	volumeSimpleInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoErrorf(t, err, "vol:%v GetVolumeSimpleInfo err:%v", volName, err) {
		return
	}
	assert.Equalf(t, proto.IncreaseReplicaNum, volumeSimpleInfo.MPConvertMode, "expect volName:%v MPConvertMode is 1, but is %v", volName, volumeSimpleInfo.MPConvertMode)

	reqURL = fmt.Sprintf("%v%v?name=%v&partitionType=metaPartition&convertMode=0", hostAddr, proto.AdminSetVolConvertMode, volName)
	process(reqURL, t)
	volumeSimpleInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoErrorf(t, err, "vol:%v GetVolumeSimpleInfo err:%v", volName, err) {
		return
	}
	assert.Zerof(t, volumeSimpleInfo.MPConvertMode, "expect volName:%v MPConvertMode is 0, but is %v", volName, volumeSimpleInfo.MPConvertMode)
}

func TestCreateRegion(t *testing.T) {
	regionMap := make(map[string]proto.RegionType)
	regionMap[testRegion1] = proto.MasterRegion
	regionMap[testRegion2] = proto.MasterRegion
	regionMap[testRegion3] = proto.SlaveRegion
	regionMap[testRegion4] = proto.SlaveRegion
	for regionName, regionType := range regionMap {
		reqURL := fmt.Sprintf("%v%v?regionName=%v&regionType=%d", hostAddr, proto.CreateRegion, regionName, regionType)
		process(reqURL, t)
	}

	regionList, err := mc.AdminAPI().RegionList()
	if !assert.NoErrorf(t, err, "getRegionList err:%v", err) {
		return
	}
	assert.Equalf(t, len(regionMap), len(regionList), "expect regionCount is %v, but is %v", len(regionMap), len(regionList))
	for _, regionView := range regionList {
		regionType, ok := regionMap[regionView.Name]
		if !ok {
			t.Errorf("get unexpect region:%v ", regionView.Name)
			continue
		}
		assert.Equalf(t, regionType, regionView.RegionType, "region:%v expect regionType is %v, but is %v", regionView.Name, regionType, regionView.RegionType)
	}
}

func TestZoneSetRegion(t *testing.T) {
	zoneRegionMap := make(map[string]string)
	zoneRegionMap[testZone1] = testRegion1
	zoneRegionMap[testZone2] = testRegion3
	for zoneName, regionName := range zoneRegionMap {
		reqURL := fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, zoneName, regionName)
		process(reqURL, t)
	}

	zoneList, err := mc.AdminAPI().ZoneList()
	if !assert.NoErrorf(t, err, "get ZoneList err:%v", err) {
		return
	}
	for zoneName, regionName := range zoneRegionMap {
		flag := false
		for _, zoneView := range zoneList {
			if zoneView.Name == zoneName {
				flag = true
				assert.Equalf(t, regionName, zoneView.Region, "zone:%v expect region is %v, but is %v", zoneName, regionName, zoneView.Region)
				break
			}
		}
		if !flag {
			t.Errorf("can not find zoneName:%v from zone list", zoneName)
		}
	}
}

func TestDefaultRegion(t *testing.T) {
	server.cluster.t.putZoneIfAbsent(newZone(testZone6))
	server.cluster.t.putZoneIfAbsent(newZone(testZone9))
	topologyView, err := getTopologyView()
	if !assert.NoErrorf(t, err, "get topologyView err:%v", err) {
		return
	}
	flag := false
	for _, regionView := range topologyView.Regions {
		if regionView.Name == "default" {
			flag = true
			assert.Containsf(t, regionView.Zones, testZone6, "zone:%v is expected in default region but is not", testZone6)
			assert.Containsf(t, regionView.Zones, testZone9, "zone:%v is expected in default region but is not", testZone9)
			break
		}
	}
	if !flag {
		t.Errorf("can not find default region ")
	}
}

func TestUpdateRegion(t *testing.T) {
	server.cluster.t.putZoneIfAbsent(newZone(testZone4))
	server.cluster.t.putZoneIfAbsent(newZone(testZone5))
	regionName := testRegion4
	// add one zone
	reqURL := fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, testZone4, regionName)
	process(reqURL, t)
	// update region type
	reqURL = fmt.Sprintf("%v%v?regionName=%v&regionType=%d", hostAddr, proto.UpdateRegion, regionName, proto.MasterRegion)
	process(reqURL, t)
	// add a new zone
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, testZone5, regionName)
	process(reqURL, t)

	regionView, err := mc.AdminAPI().GetRegionView(regionName)
	if !assert.NoErrorf(t, err, "region:%v GetRegionView err:%v", regionName, err) {
		return
	}
	if !assert.Equalf(t, regionName, regionView.Name, "expect regionName is %v, but is %v", regionName, regionView.Name) {
		return
	}
	assert.Equalf(t, proto.MasterRegion, regionView.RegionType, "expect RegionType is %v, but is %v", proto.MasterRegion, regionView.RegionType)
	assert.Lenf(t, regionView.Zones, 2, "expect region zones count is 2, but is %v", len(regionView.Zones))
	assert.Containsf(t, regionView.Zones, testZone4, "zone:%v is expected in region:%v but is not", testZone4, regionName)
	assert.Containsf(t, regionView.Zones, testZone5, "zone:%v is expected in region:%v but is not", testZone5, regionName)
}

func getTopologyView() (topologyView TopologyView, err error) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.GetTopologyView)
	resp, err := http.Get(reqURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return
	}
	if err = json.Unmarshal(body.Data, &topologyView); err != nil {
		return
	}
	return
}

func TestVolDpWriteableThreshold(t *testing.T) {

	// create vol with dpWriteableThreshold
	name := "vol_dpWriteable"
	owner := "cfs"
	dpWriteableThreshold := 0.6
	reqURL := fmt.Sprintf("%v%v?name=%v&owner=%v&capacity=100&zoneName=%v&dpWriteableThreshold=%v", hostAddr, proto.AdminCreateVol, name, owner, testZone2, dpWriteableThreshold)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	assert.NoErrorf(t, err, "get vol %v failed,err:%v", name, err)
	if !assert.Equalf(t, dpWriteableThreshold, vol.dpWriteableThreshold, "expect dpWriteableThreshold :%v,real:%v", dpWriteableThreshold, vol.dpWriteableThreshold) {
		return
	}

	// update vol dpWriteableThreshold
	dpWriteableThreshold = 0.7
	reqURL = fmt.Sprintf("%v%v?name=%v&authKey=%v&capacity=100&dpWriteableThreshold=%v", hostAddr, proto.AdminUpdateVol, name, buildAuthKey("cfs"), dpWriteableThreshold)
	process(reqURL, t)
	vol, err = server.cluster.getVol(name)
	assert.NoErrorf(t, err, "get vol %v failed,err:%v", name, err)
	if !assert.Equalf(t, dpWriteableThreshold, vol.dpWriteableThreshold, "expect dpWriteableThreshold :%v,real:%v", dpWriteableThreshold, vol.dpWriteableThreshold) {
		return
	}

	//check dp status
	vol.RLock()
	defer vol.RUnlock()
	vol.dpWriteableThreshold = 0.9
	assert.NotZero(t, len(vol.dataPartitions.partitions), "vol has 0 partitions")
	partition := vol.dataPartitions.partitions[0]
	partition.lastStatus = proto.ReadOnly
	partition.lastModifyStatusTime = 0
	canReset := partition.canResetStatusToWrite(vol.dpWriteableThreshold)
	assert.Truef(t, canReset, "expect canReset:%v,real canReset :%v ", true, canReset)

	partition.lastModifyStatusTime = time.Now().Unix()
	canReset = partition.canResetStatusToWrite(vol.dpWriteableThreshold)
	assert.Falsef(t, canReset, "expect canReset:%v,real canReset :%v ", false, canReset)

	vol.dpWriteableThreshold = 0.6
	partition.lastStatus = proto.ReadOnly
	canReset = partition.canResetStatusToWrite(vol.dpWriteableThreshold)
	assert.Falsef(t, canReset, "expect canReset:%v,real canReset :%v ", false, canReset)
}

func TestCreateVolForUpdateToCrossRegionVol(t *testing.T) {
	volName := quorumVolName
	slaveRegion := testRegion3
	newSlaveRegionZone := testZone6 // mds11Addr mds12Addr mds13Addr, mms11Addr mms12Addr mms13Addr
	reqURL := fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, newSlaveRegionZone, slaveRegion)
	process(reqURL, t)

	masterRegion := testRegion1
	masterRegionZone1 := testZone1 // mds1Addr mds2Addr, mms1Addr mms2Addr
	masterRegionZone2 := testZone2 // mds3Addr mds4Addr mds5Addr, mms3Addr mms4Addr mms5Addr
	masterRegionZone3 := testZone3 // mds7Addr mds8Addr, mms7Addr mms8Addr
	zoneName := fmt.Sprintf("%s,%s,%s", masterRegionZone1, masterRegionZone2, masterRegionZone3)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, masterRegionZone2, masterRegion)
	process(reqURL, t)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, masterRegionZone3, masterRegion)
	process(reqURL, t)
	// create a normal vol
	err := mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, 1,
		false, false, false, true, false, false, zoneName, "0,0", "", 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	assert.NoErrorf(t, err, "CreateVolume err:%v", err)
}

func TestUpdateVolToCrossRegionVol(t *testing.T) {
	volName := quorumVolName
	newZoneName := fmt.Sprintf("%s,%s,%s,%s", testZone1, testZone2, testZone3, testZone6)
	// update to cross region vol
	err := mc.AdminAPI().UpdateVolume(volName, 200, 5, 0, 0, 1, false, false, false, false, false, false,
		true, false, false, buildAuthKey("cfs"), newZoneName, "0,0", "", 0, 1, 120, "default", 0, 0, 0, 0, 0, exporter.UMPCollectMethodUnknown, -1, -1, false,
		"", false, false, 0)
	if !assert.NoErrorf(t, err, "UpdateVolume err:%v", err) {
		return
	}
	volumeSimpleInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equalf(t, proto.CrossRegionHATypeQuorum, volumeSimpleInfo.CrossRegionHAType, "vol:%v expect CrossRegionHAType is %v, but is %v", volName, proto.CrossRegionHATypeQuorum, volumeSimpleInfo.CrossRegionHAType)
	assert.Equalf(t, uint8(5), volumeSimpleInfo.DpReplicaNum, "vol:%v expect DpReplicaNum,DpLearnerNum is (5,0), but is (%v,%v)", volName, volumeSimpleInfo.DpReplicaNum, volumeSimpleInfo.DpLearnerNum)
	assert.Zerof(t, volumeSimpleInfo.DpLearnerNum, "vol:%v expect DpReplicaNum,DpLearnerNum is (5,0), but is (%v,%v)", volName, volumeSimpleInfo.DpReplicaNum, volumeSimpleInfo.DpLearnerNum)
	assert.Equalf(t, uint8(3), volumeSimpleInfo.MpReplicaNum, "vol:%v expect MpReplicaNum,MpLearnerNum is (3,2), but is (%v,%v)", volName, volumeSimpleInfo.MpReplicaNum, volumeSimpleInfo.MpLearnerNum)
	assert.Equalf(t, uint8(2), volumeSimpleInfo.MpLearnerNum, "vol:%v expect MpReplicaNum,MpLearnerNum is (3,2), but is (%v,%v)", volName, volumeSimpleInfo.MpReplicaNum, volumeSimpleInfo.MpLearnerNum)
	assert.Equalf(t, proto.IncreaseReplicaNum, volumeSimpleInfo.DPConvertMode, "vol:%v expect DPConvertMode,MPConvertMode is (%v,%v), but is (%v,%v)", volName,
		proto.IncreaseReplicaNum, proto.DefaultConvertMode, volumeSimpleInfo.DPConvertMode, volumeSimpleInfo.MPConvertMode)
	assert.Equalf(t, proto.DefaultConvertMode, volumeSimpleInfo.MPConvertMode, "vol:%v expect DPConvertMode,MPConvertMode is (%v,%v), but is (%v,%v)", volName,
		proto.IncreaseReplicaNum, proto.DefaultConvertMode, volumeSimpleInfo.DPConvertMode, volumeSimpleInfo.MPConvertMode)
	assert.Equalf(t, newZoneName, volumeSimpleInfo.ZoneName, "vol:%v expect ZoneName is %v, but is %v", volName, newZoneName, volumeSimpleInfo.ZoneName)
	assert.Equalf(t, int64(120), volumeSimpleInfo.ExtentCacheExpireSec, "vol:%v expect ExtentCacheExpireSec is 120, but is %v", volName, volumeSimpleInfo.ExtentCacheExpireSec)
}

func TestAddDataReplicaForCrossRegionVol(t *testing.T) {
	var (
		partition *DataPartition
		dataNode  *DataNode
		err       error
	)
	quorumVol, err = server.cluster.getVol(quorumVolName)
	if !assert.NoErrorf(t, err, "getVol:%v err:%v", quorumVolName, err) {
		return
	}
	if !assert.NotNilf(t, quorumVol, "getVol:%v err:%v", quorumVolName, err) {
		return
	}
	if !assert.NotZerof(t, len(quorumVol.dataPartitions.partitions), "vol:%v no data partition ", quorumVolName) {
		return
	}
	partition = quorumVol.dataPartitions.partitions[0]
	if !assert.NotNilf(t, partition, "vol:%v no data partition ", quorumVolName) {
		return
	}
	if !assert.Equalf(t, uint8(5), quorumVol.dpReplicaNum, "vol:%v dpReplicaNum should be 5, but get:%v", quorumVolName, quorumVol.dpReplicaNum) {
		return
	}
	// make the data catch up
	if len(partition.Replicas) != 0 {
		used := partition.Replicas[0].Used
		for _, replica := range partition.Replicas {
			replica.Used = used
		}
	}
	partition.isRecover = false
	reqURL := fmt.Sprintf("%v%v?id=%v&addReplicaType=%d", hostAddr, proto.AdminAddDataReplica, partition.PartitionID, proto.AutoChooseAddrForQuorumVol)
	process(reqURL, t)

	partition.RLock()
	if partition.ReplicaNum != 4 || len(partition.Hosts) != 4 || len(partition.Peers) != 4 {
		t.Errorf("dp:%v expect ReplicaNum,Hosts len,Peers len is (4,4,4) but is (%v,%v,%v)",
			partition.PartitionID, partition.ReplicaNum, len(partition.Hosts), len(partition.Peers))
		partition.RUnlock()
		return
	}
	newAddr := partition.Hosts[3]
	partition.RUnlock()

	load, ok := server.cluster.t.dataNodes.Load(newAddr)
	if !assert.Truef(t, ok, "can not get datanode:%v", newAddr) {
		return
	}
	dataNode, ok = load.(*DataNode)
	if !assert.Truef(t, ok, "can not get datanode:%v", newAddr) {
		return
	}
	if !assert.Equalf(t, testZone6, dataNode.ZoneName, "dp:%v dataNode:%v expect ZoneName is %v but is %v ", partition.PartitionID, dataNode.Addr, testZone6, dataNode.ZoneName) {
		return
	}
	regionType, err := server.cluster.getDataNodeRegionType(newAddr)
	if !assert.NoErrorf(t, err, "dp:%v expect regionType is %v but is %v err:%v", partition.PartitionID, proto.SlaveRegion, regionType, err) {
		return
	}
	if !assert.Equalf(t, proto.SlaveRegion, regionType, "dp:%v expect regionType is %v but is %v err:%v", partition.PartitionID, proto.SlaveRegion, regionType, err) {
		return
	}
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		addr, ok := key.(string)
		if !ok {
			return true
		}
		if strings.HasPrefix(addr, newAddr) {
			server.cluster.BadDataPartitionIds.Delete(key)
		}
		return true
	})
}

func TestAddMetaLearnerForCrossRegionVol(t *testing.T) {
	var (
		partition *MetaPartition
		metaNode  *MetaNode
	)
	partition = quorumVol.MetaPartitions[quorumVol.maxPartitionID()]
	if !assert.NotNilf(t, partition, "vol:%v no meta partition ", quorumVolName) {
		return
	}
	reqURL := fmt.Sprintf("%v%v?id=%v&addReplicaType=%d", hostAddr, proto.AdminAddMetaReplicaLearner, partition.PartitionID, proto.AutoChooseAddrForQuorumVol)
	process(reqURL, t)

	partition.RLock()
	if len(partition.Hosts) != 4 || len(partition.Peers) != 4 || partition.LearnerNum != 1 || len(partition.Learners) != 1 {
		t.Errorf("mp:%v expect Hosts len,Peers len,LearnerNum,Learners len is (4,4,1,1) but is (%v,%v,%v,%v)",
			partition.PartitionID, len(partition.Hosts), len(partition.Peers), partition.LearnerNum, len(partition.Learners))
		partition.RUnlock()
		return
	}
	newAddr := partition.Hosts[3]
	if partition.IsRecover != false {
		t.Errorf("mp:%v expect IsRecover is false but is %v", partition.PartitionID, partition.IsRecover)
	}
	partition.RUnlock()
	load, ok := server.cluster.t.metaNodes.Load(newAddr)
	if !assert.Truef(t, ok, "can not get metanode:%v", newAddr) {
		return
	}
	metaNode, ok = load.(*MetaNode)
	if !assert.Truef(t, ok, "can not get metanode:%v", newAddr) {
		return
	}
	if !assert.Equalf(t, testZone6, metaNode.ZoneName, "mp:%v metaNode:%v expect ZoneName is %v but is %v ", partition.PartitionID, metaNode.Addr, testZone6, metaNode.ZoneName) {
		return
	}
	regionType, err := server.cluster.getMetaNodeRegionType(newAddr)
	if !assert.NoErrorf(t, err, "mp:%v expect regionType is %v but is %v err:%v", partition.PartitionID, proto.SlaveRegion, regionType, err) {
		return
	}
	if !assert.Equalf(t, proto.SlaveRegion, regionType, "mp:%v expect regionType is %v but is %v err:%v", partition.PartitionID, proto.SlaveRegion, regionType, err) {
		return
	}
}

func TestAddDataPartitionForCrossRegionVol(t *testing.T) {
	var dataPartition *DataPartition
	oldPartitionIDMap := make(map[uint64]bool)
	quorumVol.createTime = 0
	quorumVol.dataPartitions.RLock()
	for id := range quorumVol.dataPartitions.partitionMap {
		oldPartitionIDMap[id] = true
	}
	quorumVol.dataPartitions.RUnlock()
	// create a new data partition
	reqURL := fmt.Sprintf("%v%v?count=1&name=%v", hostAddr, proto.AdminCreateDataPartition, quorumVolName)
	process(reqURL, t)
	// find the new data partition and check
	quorumVol.dataPartitions.RLock()
	for dpID, partition := range quorumVol.dataPartitions.partitionMap {
		if _, ok := oldPartitionIDMap[dpID]; !ok {
			dataPartition = partition
			break
		}
	}
	quorumVol.dataPartitions.RUnlock()
	if !assert.NotNilf(t, dataPartition, "can not find new dataPartition") {
		return
	}
	validateCrossRegionDataPartition(dataPartition, t)
}

func TestCreateCrossRegionVol(t *testing.T) {
	var (
		vol           *Vol
		dataPartition *DataPartition
		metaPartition *MetaPartition
		err           error
	)
	volName := "crossRegionVol"
	zoneName := fmt.Sprintf("%s,%s,%s,%s", testZone1, testZone2, testZone3, testZone6)
	// create a cross region vol
	reqURL := fmt.Sprintf("%v%v?name=%v&replicaNum=5&capacity=200&owner=cfs&mpCount=3&zoneName=%v&crossRegion=1",
		hostAddr, proto.AdminCreateVol, volName, zoneName)
	process(reqURL, t)
	vol, err = server.cluster.getVol(volName)
	if !assert.NoErrorf(t, err, "getVol:%v err:%v", volName, err) {
		return
	}
	if !assert.NotNilf(t, vol, "getVol:%v err:%v", volName, err) {
		return
	}

	if assert.NotZerof(t, len(vol.dataPartitions.partitions), "vol:%v no data partition ", volName) {
		dataPartition = vol.dataPartitions.partitions[0]
	}
	if assert.NotNilf(t, dataPartition, "vol:%v no data partition ", volName) {
		validateCrossRegionDataPartition(dataPartition, t)
	}
	metaPartition = vol.MetaPartitions[vol.maxPartitionID()]
	if !assert.NotNilf(t, metaPartition, "vol:%v no meta partition ", volName) {
		return
	}
	validateCrossRegionMetaPartition(metaPartition, t)
}

func validateCrossRegionDataPartition(dataPartition *DataPartition, t *testing.T) {
	dataPartition.RLock()
	dataPartitionHosts := dataPartition.Hosts
	if dataPartition.ReplicaNum != 5 || len(dataPartitionHosts) != 5 || len(dataPartition.Peers) != 5 {
		t.Errorf("dp expect ReplicaNum,Hosts len,Peers len,Learners len is (5,5,5) but is (%v,%v,%v) ",
			dataPartition.ReplicaNum, len(dataPartitionHosts), len(dataPartition.Peers))
	}
	dataPartition.RUnlock()
	masterRegionAddrs, slaveRegionAddrs, err := server.cluster.getMasterAndSlaveRegionAddrsFromDataNodeAddrs(dataPartitionHosts)
	if !assert.NoErrorf(t, err, "getMasterAndSlaveRegionAddrsFromDataNodeAddrs err:%v", err) {
		return
	}
	if len(masterRegionAddrs) != 3 || len(slaveRegionAddrs) != 2 {
		t.Errorf("expect masterRegionAddrs len,slaveRegionAddrs len is (3,2) but is (%v,%v) ", len(masterRegionAddrs), len(slaveRegionAddrs))
		return
	}
	if masterRegionAddrs[0] != dataPartitionHosts[0] || masterRegionAddrs[1] != dataPartitionHosts[1] || masterRegionAddrs[2] != dataPartitionHosts[2] {
		t.Errorf("expect masterRegionAddrs(%v) is equal to dataPartitionHosts(%v) but is not", masterRegionAddrs, dataPartitionHosts[:3])
	}
	if slaveRegionAddrs[0] != dataPartitionHosts[3] || slaveRegionAddrs[1] != dataPartitionHosts[4] {
		t.Errorf("expect masterRegionAddrs(%v) is equal to dataPartitionHosts(%v) but is not", masterRegionAddrs, dataPartitionHosts[3:])
	}
}

func validateCrossRegionMetaPartition(metaPartition *MetaPartition, t *testing.T) {
	metaPartition.RLock()
	metaPartitionHosts := metaPartition.Hosts
	learnerHosts := metaPartition.getLearnerHosts()
	if metaPartition.ReplicaNum != 3 || metaPartition.LearnerNum != 2 {
		t.Errorf("mp expect ReplicaNum,LearnerNum is (3,2) but is (%v,%v) ", metaPartition.ReplicaNum, metaPartition.LearnerNum)
	}
	if len(metaPartitionHosts) != 5 || len(metaPartition.Peers) != 5 || len(metaPartition.Learners) != 2 {
		t.Errorf("mp expect Hosts len,Peers len,Learners len is (5,5,2) but is (%v,%v,%v) ",
			len(metaPartitionHosts), len(metaPartition.Peers), len(metaPartition.Learners))
		return
	}
	metaPartition.RUnlock()
	masterRegionAddrs, slaveRegionAddrs, err := server.cluster.getMasterAndSlaveRegionAddrsFromMetaNodeAddrs(metaPartitionHosts)
	if !assert.NoErrorf(t, err, "getMasterAndSlaveRegionAddrsFromMetaNodeAddrs err:%v", err) {
		return
	}
	if len(masterRegionAddrs) != 3 || len(slaveRegionAddrs) != 2 {
		t.Errorf("expect masterRegionAddrs len,slaveRegionAddrs len is (3,2) but is (%v,%v)", len(masterRegionAddrs), len(slaveRegionAddrs))
		return
	}
	if masterRegionAddrs[0] != metaPartitionHosts[0] || masterRegionAddrs[1] != metaPartitionHosts[1] || masterRegionAddrs[2] != metaPartitionHosts[2] {
		t.Errorf("expect masterRegionAddrs(%v) is equal to metaPartitionHosts(%v) but is not", masterRegionAddrs, metaPartitionHosts[:3])
	}
	if slaveRegionAddrs[0] != metaPartitionHosts[3] || slaveRegionAddrs[1] != metaPartitionHosts[4] {
		t.Errorf("expect masterRegionAddrs(%v) is equal to metaPartitionHosts(%v) but is not", masterRegionAddrs, metaPartitionHosts[3:])
	}
	if slaveRegionAddrs[0] != learnerHosts[0] || slaveRegionAddrs[1] != learnerHosts[1] {
		t.Errorf("expect masterRegionAddrs(%v) is equal to learnerHosts(%v) but is not", masterRegionAddrs, learnerHosts)
	}
}

func TestSetVolMinRWPartition(t *testing.T) {
	volName := commonVolName
	minRwMPNum := 10
	minRwDPNum := 4
	err := mc.AdminAPI().SetVolMinRWPartition(volName, minRwMPNum, minRwDPNum)
	if !assert.NoErrorf(t, err, "vol:%v SetVolMinRWPartition err:%v", volName, err) {
		return
	}
	volumeSimpleInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if !assert.NoErrorf(t, err, "vol:%v GetVolumeSimpleInfo err:%v", volName, err) {
		return
	}
	assert.Equal(t, minRwMPNum, volumeSimpleInfo.MinWritableMPNum)
	assert.Equal(t, minRwDPNum, volumeSimpleInfo.MinWritableDPNum)
}

func TestCreateDataPartitionOfDesignatedZoneName(t *testing.T) {
	err := validateCreateDataPartition(commonVolName, "", 5, t)
	assert.NoError(t, err)
	err = validateCreateDataPartition(commonVolName, fmt.Sprintf("%v,%v", testZone3, testZone2), 5, t)
	assert.NoError(t, err)
	err = validateCreateDataPartition(quorumVolName, "", 5, t)
	assert.NoError(t, err)
	err = validateCreateDataPartition(quorumVolName, fmt.Sprintf("%s,%s,%s", testZone6, testZone3, testZone2), 5, t)
	assert.NoError(t, err)
}

func validateCreateDataPartition(volName, designatedZoneName string, createCount int, t *testing.T) (err error) {
	var (
		oldMaxDpID     uint64
		oldDpCount     int
		newDpCount     int
		reqURL         string
		expectZoneName string
		dataNode       *DataNode
	)
	// record old dp info
	vol, err := server.cluster.getVol(volName)
	if !assert.NoError(t, err) {
		return
	}
	vol.createTime = time.Now().Unix() - defaultAutoCreateDPAfterVolCreateSecond*2
	for dpID := range vol.cloneDataPartitionMap() {
		oldDpCount++
		if dpID > oldMaxDpID {
			oldMaxDpID = dpID
		}
	}
	//do create dp
	if designatedZoneName == "" {
		expectZoneName = vol.zoneName
		reqURL = fmt.Sprintf("%v%v?count=%v&name=%v", hostAddr, proto.AdminCreateDataPartition, createCount, vol.Name)
	} else {
		expectZoneName = designatedZoneName
		reqURL = fmt.Sprintf("%v%v?count=%v&name=%v&zoneName=%v", hostAddr, proto.AdminCreateDataPartition, createCount, vol.Name, designatedZoneName)
	}
	process(reqURL, t)
	zoneList := strings.Split(expectZoneName, ",")
	expectZoneMap := make(map[string]bool)
	for _, zone := range zoneList {
		expectZoneMap[zone] = true
	}
	// check create count and dp zone info
	for _, dataPartition := range vol.cloneDataPartitionMap() {
		if dataPartition.PartitionID > oldMaxDpID {
			newDpCount++
			dpZones := make([]string, 0)
			// check dp zone, must be in given zones
			for _, nodeAddr := range dataPartition.Hosts {
				load, ok := server.cluster.t.dataNodes.Load(nodeAddr)
				if !assert.Truef(t, ok, "can not get datanode:%v", nodeAddr) {
					return
				}
				dataNode, ok = load.(*DataNode)
				if !assert.Truef(t, ok, "can not get datanode:%v", nodeAddr) {
					return
				}
				dpZones = append(dpZones, dataNode.ZoneName)
				assert.Truef(t, expectZoneMap[dataNode.ZoneName], "expect zones:%v but get:%v", expectZoneName, dataNode.ZoneName)
			}
			if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
				validateCrossRegionDataPartition(dataPartition, t)
			}
		}
	}
	assert.Equalf(t, createCount, newDpCount, "expect createCount:%v but get newDpCount:%v", createCount, newDpCount)
	return
}

func TestSetReadDirLimitNum(t *testing.T) {
	readDirLimitNum := 500000
	reqURL := fmt.Sprintf("%v%v?metaNodeReadDirLimit=%v", hostAddr, proto.AdminSetNodeInfo, readDirLimitNum)
	process(reqURL, t)
	if !assert.Equalf(t, uint64(readDirLimitNum), server.cluster.cfg.MetaNodeReadDirLimitNum, "set readDirLimitNum to %v failed", readDirLimitNum) {
		return
	}
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	reply := processReturnRawReply(reqURL, t)
	limitInfo := &proto.LimitInfo{}
	err := json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	assert.Equalf(t, uint64(readDirLimitNum), limitInfo.MetaNodeReadDirLimitNum, "readDirLimitNum expect:%v, real:%v", readDirLimitNum, limitInfo.MetaNodeReadDirLimitNum)
}

func TestSetDataNodeRepairTaskCountZoneLimit(t *testing.T) {
	limitNum := uint64(10)
	zone := testZone1
	reqURL := fmt.Sprintf("%v%v?%v=%v&zoneName=%v", hostAddr, proto.AdminSetNodeInfo, dataNodeRepairTaskCntZoneKey, limitNum, zone)
	process(reqURL, t)
	if !assert.Equalf(t, limitNum, server.cluster.cfg.DataNodeRepairTaskCountZoneLimit[zone], "set zone:%v DataNodeRepairTaskCountZoneLimit to %v failed", zone, limitNum) {
		return
	}
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	reply := processReturnRawReply(reqURL, t)
	limitInfo := &proto.LimitInfo{}
	err := json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	assert.Equalf(t, limitNum, limitInfo.DataNodeRepairTaskCountZoneLimit[zone], "DataNodeRepairTaskCountZoneLimit expect:%v, real:%v", limitNum, limitInfo.DataNodeRepairTaskCountZoneLimit)
}

func TestSetDataNodeRepairTaskLimit(t *testing.T) {
	limitInfoReqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	reply := processReturnRawReply(limitInfoReqURL, t)
	limitInfo := &proto.LimitInfo{}
	err := json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	assert.Equalf(t, uint64(defaultSSDZoneTaskLimit), limitInfo.DataNodeRepairSSDZoneTaskLimitOnDisk, "DataNodeRepairSSDZoneTaskLimitOnDisk expect:%v, real:%v", defaultSSDZoneTaskLimit, limitInfo.DataNodeRepairSSDZoneTaskLimitOnDisk)

	ssdLimitNum := uint64(29)
	clusterLimitNum := uint64(5)
	reqURL := fmt.Sprintf("%v%v?%v=%v&%v=%v", hostAddr, proto.AdminSetNodeInfo, dataNodeRepairTaskSSDKey, ssdLimitNum, dataNodeRepairTaskCountKey, clusterLimitNum)
	process(reqURL, t)
	if !assert.Equalf(t, ssdLimitNum, server.cluster.cfg.DataNodeRepairSSDZoneTaskCount, "set DataNodeRepairSSDZoneTaskCount failed expect:%v, real:%v", ssdLimitNum, server.cluster.cfg.DataNodeRepairSSDZoneTaskCount) {
		return
	}
	if !assert.Equalf(t, clusterLimitNum, server.cluster.cfg.DataNodeRepairTaskCount, "set DataNodeRepairTaskCount failed expect:%v, real:%v", clusterLimitNum, server.cluster.cfg.DataNodeRepairTaskCount) {
		return
	}

	reply = processReturnRawReply(limitInfoReqURL, t)
	limitInfo = &proto.LimitInfo{}
	err = json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	assert.Equalf(t, ssdLimitNum, limitInfo.DataNodeRepairSSDZoneTaskLimitOnDisk, "DataNodeRepairSSDZoneTaskLimitOnDisk expect:%v, real:%v", ssdLimitNum, limitInfo.DataNodeRepairSSDZoneTaskLimitOnDisk)
	assert.Equalf(t, clusterLimitNum, limitInfo.DataNodeRepairClusterTaskLimitOnDisk, "DataNodeRepairClusterTaskLimitOnDisk expect:%v, real:%v", clusterLimitNum, limitInfo.DataNodeRepairClusterTaskLimitOnDisk)
}

func TestValidSmartRules(t *testing.T) {
	clusterID := "cfs"
	volName := "vol"
	{
		ruleStr := "actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5:hdd"
		rules := strings.Split(ruleStr, ",")
		err := proto.CheckLayerPolicy(clusterID, volName, rules)
		assertErrNilOtherwiseFailNow(t, err)
	}
	{
		ruleStr := "actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5"
		rules := strings.Split(ruleStr, ",")
		err := proto.CheckLayerPolicy(clusterID, volName, rules)
		if err == nil {
			t.Errorf("%v is invalid", ruleStr)
			t.FailNow()
		}
	}

	{
		ruleStr := ""
		rules := strings.Split(ruleStr, ",")
		err := proto.CheckLayerPolicy(clusterID, volName, rules)
		if err == nil {
			t.Errorf("%v is invalid", ruleStr)
			t.FailNow()
		}
	}

	{
		ruleStr := "actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5:hdd,"
		for i := 0; i < 4; i++ {
			ruleStr += "actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5:hdd"
		}
		ruleStr += ",actionMetrics:dp:read:count:minute:1000:5:hdd,actionMetrics:dp:appendWrite:count:minute:1000:5:hdd"
		rules := strings.Split(ruleStr, ",")
		err := proto.CheckLayerPolicy(clusterID, volName, rules)
		if err == nil {
			t.Errorf("%v is invalid", ruleStr)
			t.FailNow()
		}
	}
}

func TestIDCAPI(t *testing.T) {
	idc1Name := "TestIDCAPI1"
	var (
		reqURL string
		err    error
	)
	reqURL = fmt.Sprintf("%v%v?name1=%v", hostAddr, proto.CreateIDC, idc1Name)
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), "parameter name not found")
	reqURL = fmt.Sprintf("%v%v?name=%v", hostAddr, proto.CreateIDC, "")
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), "parameter name not found")
	reqURL = fmt.Sprintf("%v%v?name=%v", hostAddr, proto.CreateIDC, idc1Name)
	_, err = processNoTerminal(reqURL, t)
	assert.Nil(t, err)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName1=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, testZone1, idc1Name, "hdd")
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), "idc name is empty")
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, "zone11", idc1Name, "hdd")
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), "zone[zone11] is not found")
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, testZone1, idc1Name, "hdd1")
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), "invalid medium type: hdd1")
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType1=%v", hostAddr, proto.SetZoneIDC, testZone1, idc1Name, "hdd")
	_, err = processNoTerminal(reqURL, t)
	assert.Nil(t, err)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName1=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, testZone1, idc1Name, "hdd")
	_, err = processNoTerminal(reqURL, t)
	assert.Nil(t, err)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, testZone2, idc1Name, "ssd")
	_, err = processNoTerminal(reqURL, t)
	assert.Nil(t, err)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, "", idc1Name, "hdd")
	_, err = processNoTerminal(reqURL, t)
	assert.Contains(t, err.Error(), " parameter zoneName not found")
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&idcName=%v&mediumType=%v", hostAddr, proto.SetZoneIDC, testZone1, "", "ssd")
	_, err = processNoTerminal(reqURL, t)
	assert.Nil(t, err)
}

func TestSetIDC(t *testing.T) {
	idcName1 := "idcTestSetIDC1"
	idcName2 := "idcTestSetIDC2"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName2, c)
	assertErrNilOtherwiseFailNow(t, err)

	err = c.setZoneIDC(testZone2, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)

	err = c.setZoneIDC(testZone1, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	idc, err := c.t.getIDC(idcName1)
	assertErrNilOtherwiseFailNow(t, err)
	if idc.getMediumType(testZone1) != proto.MediumSSD {
		t.Error(idc.getMediumType(testZone1).String())
		t.FailNow()
	}

	err = c.setZoneIDC(testZone1, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	idc, err = c.t.getIDC(idcName1)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumHDD, idc.getMediumType(testZone1)) {
		t.FailNow()
	}

	err = c.setZoneIDC(testZone1, idcName1, proto.MediumInit)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumHDD, idc.getMediumType(testZone1)) {
		t.FailNow()
	}

	err = c.setZoneIDC(testZone1, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	idc, err = c.t.getIDC(idcName2)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumHDD, idc.getMediumType(testZone1)) {
		t.FailNow()
	}

	err = c.setZoneIDC(testZone2, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	idc, err = c.t.getIDC(idcName2)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumHDD, idc.getMediumType(testZone2)) {
		t.FailNow()
	}
}

func TestSmartVolRules(t *testing.T) {
	volName := "volTestSmartVolRules"
	idcName := "idcTestSmartVolRules"
	idc1 := new(IDCInfo)
	idc1.Name = idcName
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 3, int(proto.StoreModeMem),
		false, false, false, true, true, false, testZone2, "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoErrorf(t, err, "CreateVolume err:%v", err) {
		return
	}
	vol, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.True(t, vol.IsSmart) {
		t.FailNow()
	}
	if !assert.NotZero(t, len(vol.SmartRules)) {
		t.FailNow()
	}
	defer log.LogFlush()
	var reqURL string
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminSmartVolList)
	resp := process(reqURL, t)
	vols := resp.Data.([]interface{})
	/*
		vols := make([]*proto.VolInfo, 0)
		for _, value := range resp.Data.([]interface{}) {
			vols = append(vols, value.(*proto.VolInfo))
		}

	*/
	if !assert.GreaterOrEqualf(t, len(vols), 1, "len: %v, data: %v\n", len(vols), resp.Data) {
		t.FailNow()
	}

	reqURL = fmt.Sprintf("%v%v?name=%v&authKey=7b2f1bf38b87d32470c4557c7ff02e75&smart=%v", hostAddr, proto.AdminUpdateVol, volName, true)
	_, err = processNoTerminal(reqURL, t)
	assertErrNilOtherwiseFailNow(t, err)
	vol, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.True(t, vol.IsSmart) {
		t.FailNow()
	}

	rules := "actionMetrics:dp:read:count:minute:1000:10:hdd,actionMetrics:dp:appendWrite:count:minute:1000:10:hdd"
	reqURL = fmt.Sprintf("%v%v?name=%v&authKey=7b2f1bf38b87d32470c4557c7ff02e75&smart=%v&smartRules=%v", hostAddr, proto.AdminUpdateVol, volName, false, rules)
	_, err = processNoTerminal(reqURL, t)
	assertErrNilOtherwiseFailNow(t, err)
	vol, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
	assertErrNilOtherwiseFailNow(t, err)
	rulesArr := strings.Split(rules, ",")
	if !assert.Equal(t, len(vol.SmartRules), len(rulesArr)) {
		t.FailNow()
	}
	for index, rule := range vol.SmartRules {
		if !assert.Equal(t, rulesArr[index], rule) {
			t.FailNow()
		}
	}
	/*
		reqURL = fmt.Sprintf("%v%v?name=%v&authKey=7b2f1bf38b87d32470c4557c7ff02e75&smartRules=%v", hostAddr, proto.AdminUpdateVol, volName, "")
		t.Log(reqURL)
		_, err = processNoTerminal(reqURL, t)
		if err == nil {
			t.Errorf("url: %v, should be failed", reqURL)
			t.FailNow()
		}
		vol, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		//todo: how to delete the rules
		if len(vol.SmartRules) != 2 {
			t.Errorf("%v", len(vol.SmartRules))
			t.FailNow()
		}
	*/
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

idc1: zone1->hdd, zone2->sdd
idc2: zone3->hdd, zone6->sdd
*/
func TestGetTargetAddressForDataPartitionSmartTransferForOneZone(t *testing.T) {
	idcName := "IDCTestGetTargetAddressForDataPartitionSmartTransferForOneZone"
	volName := smartVolName
	idc1 := new(IDCInfo)
	idc1.Name = idcName
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 3, int(proto.StoreModeMem),
		false, false, false, true, true, false, testZone2, "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	vol, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.True(t, vol.IsSmart) {
		t.FailNow()
	}
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminSmartVolList)
	process(reqURL, t)
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)
	replica := dp.Replicas[0]
	dn, err := c.dataNode(replica.Addr)
	assertErrNilOtherwiseFailNow(t, err)
	oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, replica.Addr, oldAddr) {
		t.FailNow()
	}
	// check the new add is belong to old hosts
	zone, err := c.t.getZone(dn.ZoneName)
	assertErrNilOtherwiseFailNow(t, err)
	_, err = zone.getDataNode(newAddr)
	if !assert.Error(t, err) {
		t.FailNow()
	}

	// check the new addr is belong to expected zone
	zone, err = c.t.getZone(testZone1)
	assertErrNilOtherwiseFailNow(t, err)
	_, err = zone.getDataNode(newAddr)
	assertErrNilOtherwiseFailNow(t, err)
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone1-ssd
idc1: zone1->ssd, zone2->hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase1(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase1"
	idcName := "IDCTestGetTargetAddressForDataPartitionSmartCase1"
	idc1 := new(IDCInfo)
	idc1.Name = idcName
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone6, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false, testZone2, "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if err != nil {
		t.Errorf("CreateVolume err:%v", err)
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for _, replica := range dp.Replicas {
		dn, err := c.dataNode(replica.Addr)
		assertErrNilOtherwiseFailNow(t, err)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		if !assert.Error(t, err) {
			t.FailNow()
		}

		// check the new add is belong to expected zone
		zone, err = c.t.getZone(testZone6)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		assertErrNilOtherwiseFailNow(t, err)
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone1-ssd
idc1: zone1-ssd zone1-hdd zone3-hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase2(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase2"
	idcName := "idcTestGetTargetAddressForDataPartitionSmartCase2"
	idc1 := new(IDCInfo)
	idc1.Name = idcName
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false, testZone2, "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)
	assert.NoError(t, err)

	for index, replica := range dp.Replicas {
		dn, err := c.dataNode(replica.Addr)
		assertErrNilOtherwiseFailNow(t, err)
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		if !assert.Error(t, err) {
			t.FailNow()
		}

		// check the new add is belong to expected zone
		zone1, err1 := c.t.getZone(testZone1)
		assertErrNilOtherwiseFailNow(t, err1)
		_, err1 = zone1.getDataNode(newAddr)

		zone3, err3 := c.t.getZone(testZone3)
		assertErrNilOtherwiseFailNow(t, err3)
		_, err3 = zone3.getDataNode(newAddr)

		if err1 != nil && err3 != nil {
			t.Errorf(err1.Error())
			t.FailNow()
		}

		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone1-ssd zone3-ssd
idc1: zone1-ssd zone3-sdd zone2-hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase3(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase3"
	idcName := "idcTestGetTargetAddressForDataPartitionSmartCase3"
	idc1 := new(IDCInfo)
	idc1.Name = idcName
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false,
		fmt.Sprintf("%v,%v", testZone1, testZone3), "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for _, replica := range dp.Replicas {
		dn, err := c.dataNode(replica.Addr)
		assertErrNilOtherwiseFailNow(t, err)
		t.Logf("dps: %v, pid: %v, addr: %v", len(dps.DataPartitions), pid, replica.Addr)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		if !assert.Error(t, err) {
			t.FailNow()
		}

		// check the new add is belong to expected zone
		zone, err = c.t.getZone(testZone2)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		assertErrNilOtherwiseFailNow(t, err)
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone1-ssd zone3-ssd
idc1: zone1-ssd zone2-hdd
idc2: zone3-ssd zone6-hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase4(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase4"
	idcName1 := "idc1TestGetTargetAddressForDataPartitionSmartCase4"
	idcName2 := "idc2TestGetTargetAddressForDataPartitionSmartCase4"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName2, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false,
		fmt.Sprintf("%v,%v", testZone1, testZone3), "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for index, replica := range dp.Replicas {
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		dn, err := c.dataNode(replica.Addr)
		assertErrNilOtherwiseFailNow(t, err)
		t.Logf("dps: %v, pid: %v, addr: %v", len(dps.DataPartitions), pid, replica.Addr)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		if !assert.Error(t, err) {
			t.FailNow()
		}

		var expectedZoneName string
		switch zone.name {
		case testZone1:
			expectedZoneName = testZone2
		case testZone3:
			expectedZoneName = testZone6
		default:
			t.Error(zone.name)
			t.FailNow()
		}
		zone, err = c.t.getZone(expectedZoneName)
		assertErrNilOtherwiseFailNow(t, err)

		// check the new add is belong to expected zone
		_, err = zone.getDataNode(newAddr)
		assertErrNilOtherwiseFailNow(t, err)

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone2-ssd zone3-ssd
idc1: zone2-ssd zone7-hdd zone8-hdd
idc2: zone3-ssd zone6-hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase5(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase5"
	idcName1 := "idc1TestGetTargetAddressForDataPartitionSmartCase5"
	idcName2 := "idc2TestGetTargetAddressForDataPartitionSmartCase5"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	assertErrNilOtherwiseFailNow(t, err)
	defer log.LogFlush()
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone7, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone8, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName2, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false,
		fmt.Sprintf("%v,%v", testZone2, testZone3), "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	var pid uint64
	for _, dp := range dps.DataPartitions {
		zone2Count := 0
		for _, host := range dp.Hosts {
			zn, err := c.dataNode(host)
			assertErrNilOtherwiseFailNow(t, err)
			if zn.ZoneName == testZone2 {
				zone2Count++
			}
			if zone2Count >= 2 {
				pid = dp.PartitionID
				break
			}
		}
	}
	// todo: the pid may be 0
	if pid == 0 {
		t.Error(pid)
		t.FailNow()
	}

	dp, err := c.getDataPartitionByID(pid)
	count := 0
	for _, replica := range dp.Replicas {
		dn, err := c.dataNode(replica.Addr)
		assertErrNilOtherwiseFailNow(t, err)
		t.Logf("dps: %v, pid: %v, addr: %v", len(dps.DataPartitions), pid, replica.Addr)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		assertErrNilOtherwiseFailNow(t, err)
		_, err = zone.getDataNode(newAddr)
		if !assert.Error(t, err) {
			t.FailNow()
		}

		// check the new add is belong to expected zone
		switch zone.name {
		case testZone2:
			for {
				zone, _ = c.t.getZone(testZone7)
				_, err = zone.getDataNode(newAddr)
				if err == nil {
					count++
					break
				}
				zone, _ = c.t.getZone(testZone8)
				_, err = zone.getDataNode(newAddr)
				if err == nil {
					break
				}
				t.Error(err.Error())
				t.FailNow()
			}
		case testZone3:
			zone, err = c.t.getZone(testZone6)
			if err != nil {
				t.Error(err.Error())
				t.FailNow()
			}
			_, err = zone.getDataNode(newAddr)
			if err != nil {
				t.Error(err.Error())
				t.FailNow()
			}
		default:
			t.Error(zone.name)
			t.FailNow()
		}

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
	}
	if !assert.Equal(t, 1, count) {
		t.FailNow()
	}
}

func TestSetMonitorTime(t *testing.T) {
	summarySecond := 30
	reportSecond := 60
	reqURL := fmt.Sprintf("%v%v?monitorSummarySec=%v&monitorReportSec=%v", hostAddr, proto.AdminSetNodeInfo, summarySecond, reportSecond)
	fmt.Println(reqURL)
	process(reqURL, t)
	if server.cluster.cfg.MonitorSummarySec != uint64(summarySecond) || server.cluster.cfg.MonitorReportSec != uint64(reportSecond) {
		t.Errorf("set monitorTime failed: expect summarySecond(%v) but(%v), expect reportSecond(%v) but(%v)",
			summarySecond, server.cluster.cfg.MonitorSummarySec, reportSecond, server.cluster.cfg.MonitorReportSec)
		return
	}
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	fmt.Println(reqURL)
	reply := processReturnRawReply(reqURL, t)
	limitInfo := &proto.LimitInfo{}
	err := json.Unmarshal(reply.Data, limitInfo)
	assert.NoErrorf(t, err, "unmarshal limitinfo failed,err:%v", err)
	if limitInfo.MonitorSummarySec != uint64(summarySecond) || limitInfo.MonitorReportSec != uint64(reportSecond) {
		t.Errorf("get monitorTime failed: expect summarySecond(%v) but(%v), expect reportSecond(%v) but(%v)",
			summarySecond, limitInfo.MonitorSummarySec, reportSecond, limitInfo.MonitorReportSec)
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)

vol: zone1-ssd zone3-ssd zone7-ssd
idc1: zone7-ssd zone2-hdd
idc2: zone3-ssd zone6-hdd
idc2: zone1-ssd zone8-hdd
*/
func TestGetTargetAddressForDataPartitionSmartCase6(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase6"
	idcName1 := "idc1TestGetTargetAddressForDataPartitionSmartCase6"
	idcName2 := "idc2TestGetTargetAddressForDataPartitionSmartCase6"
	idcName3 := "idc3TestGetTargetAddressForDataPartitionSmartCase6"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone7, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName2, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName3, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName3, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone8, idcName3, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false,
		fmt.Sprintf("%v,%v,%v", testZone1, testZone3, testZone7), "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}

	zone, err := c.t.getZone(testZone8)
	assertErrNilOtherwiseFailNow(t, err)
	zone.setStatus(unavailableZone)
	defer zone.setStatus(normalZone)
	if !assert.Equal(t, unavailableZone, zone.getStatus()) {
		t.FailNow()
	}

	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}
	for _, dpv := range dps.DataPartitions {
		pid := dpv.PartitionID
		dp, err := c.getDataPartitionByID(pid)
		assertErrNilOtherwiseFailNow(t, err)

		count := 0
		for _, replica := range dp.Replicas {
			dn, err := c.dataNode(replica.Addr)
			assertErrNilOtherwiseFailNow(t, err)

			if dn.ZoneName == testZone1 {
				count++
			}
		}

		if count != 1 {
			continue
		}

		count = 0
		for index, replica := range dp.Replicas {
			t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
			dn, err := c.dataNode(replica.Addr)
			assertErrNilOtherwiseFailNow(t, err)

			t.Logf("dps: %v, pid: %v, addr: %v", len(dps.DataPartitions), pid, replica.Addr)
			oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
			assertErrNilOtherwiseFailNow(t, err)
			if !assert.Equal(t, replica.Addr, oldAddr) {
				t.FailNow()
			}
			// check the new add is belong to old hosts
			zone, err := c.t.getZone(dn.ZoneName)
			assertErrNilOtherwiseFailNow(t, err)
			_, err = zone.getDataNode(newAddr)
			if !assert.Error(t, err) {
				t.FailNow()
			}

			zone, err = c.t.getZone(testZone6)
			assertErrNilOtherwiseFailNow(t, err)

			// check the new add is belong to expected zone
			_, err = zone.getDataNode(newAddr)
			// update the dp's replicas and hosts
			replica.Addr = newAddr
			hosts := make([]string, 0)
			for _, host := range dp.Hosts {
				if host == oldAddr {
					host = newAddr
				}
				hosts = append(hosts, host)
			}
			dp.Hosts = hosts
			t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
			if err == nil {
				count++
			}
		}
		if count == 0 || count == 3 {
			t.Error(count)
			t.FailNow()
		}
	}
}

/*
master-region:

	zone1:
		datanode:
			127.0.0.1:9101
			127.0.0.1:9102
		metanode:
			127.0.0.1:8101
			127.0.0.1:8102
	zone9:
		datanode:
			127.0.0.1:9116
			127.0.0.1:9117
		metanode:
			127.0.0.1:8114
			127.0.0.1:8115

slave-region:

	zone3:
		datanode:
			127.0.0.1:9107
			127.0.0.1:9107
		metanode:
			127.0.0.1:8107
			127.0.0.1:8108

zoneName: zone1,zone9,zone3
*/
func TestGetTargetAddressForDataPartitionSmartCase7(t *testing.T) {
	volName := "smartVolTestGetTargetAddressForDataPartitionSmartCase7"
	idcName1 := "idc1TestGetTargetAddressForDataPartitionSmartCase7"
	idcName2 := "idc2TestGetTargetAddressForDataPartitionSmartCase7"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone9, idcName1, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	_, err = c.t.createIDC(idcName2, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	masterRegionName := "smartCaseMasterRegion1"
	slaveRegionName := "smartCaseSlaveRegion1"
	regionMap := make(map[string]proto.RegionType)
	regionMap[masterRegionName] = proto.MasterRegion
	regionMap[slaveRegionName] = proto.SlaveRegion
	for regionName, regionType := range regionMap {
		reqURL := fmt.Sprintf("%v%v?regionName=%v&regionType=%d", hostAddr, proto.CreateRegion, regionName, regionType)
		t.Log(reqURL)
		process(reqURL, t)
	}

	// slave region: zone6
	reqURL := fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, testZone3, slaveRegionName)
	process(reqURL, t)

	//master region zone: zone1, zone7
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, testZone1, masterRegionName)
	process(reqURL, t)
	reqURL = fmt.Sprintf("%v%v?zoneName=%v&regionName=%v", hostAddr, proto.SetZoneRegion, testZone9, masterRegionName)
	process(reqURL, t)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 5, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false,
		fmt.Sprintf("%v,%v,%v", testZone1, testZone9, testZone3), "", testSmartRules, 1, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	if !assert.NoError(t, err) {
		return
	}
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	t.Logf("replicas: %v", len(dp.Replicas))
	for index, replica := range dp.Replicas {
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		oldAddr, newAddr, err := getTargetAddressForDataPartitionSmartTransfer(c, replica.Addr, dp, nil, "", true)
		assertErrNilOtherwiseFailNow(t, err)
		if !assert.Equal(t, replica.Addr, oldAddr) {
			t.FailNow()
		}

		var zone *Zone
		if oldAddr == "127.0.0.1:9101" || oldAddr == "127.0.0.1:9102" ||
			oldAddr == "127.0.0.1:9116" || oldAddr == "127.0.0.1:9117" {
			zone, err = c.t.getZone(testZone2)
			assertErrNilOtherwiseFailNow(t, err)
		} else if oldAddr == "127.0.0.1:9107" || oldAddr == "127.0.0.1:9108" {
			zone, err = c.t.getZone(testZone6)
			assertErrNilOtherwiseFailNow(t, err)
		} else {
			assert.FailNow(t, oldAddr)
		}
		// check the new add is belong to expected zone
		_, err = zone.getDataNode(newAddr)
		assertErrNilOtherwiseFailNow(t, err)

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("dps: %v, pid: %v, addr: %v, new addr: %v", len(dps.DataPartitions), pid, oldAddr, newAddr)
	}
}

func TestFreezeDataPartition(t *testing.T) {
	volName := "TestFreezeDataPartition"
	idcName := "idcTestFreezeDataPartition"
	c := server.cluster
	_, err := c.t.createIDC(idcName, c)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone2, idcName, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	err = c.setZoneIDC(testZone1, idcName, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, false, testZone2, "", testSmartRules, 0, "default", defaultEcDataNum, defaultEcParityNum, false, 0, 0, 0, false)
	assertErrNilOtherwiseFailNow(t, err)
	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.NotZero(t, len(dps.DataPartitions)) {
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)
	if !assert.Equal(t, proto.ReadWrite, int(dp.Status)) {
		t.FailNow()
	}

	dpInfo, err := mc.AdminAPI().GetDataPartition(volName, pid)
	if !assert.Falsef(t, dpInfo.IsFrozen, "%v should not be frozen", dp.PartitionID) {
		t.FailNow()
	}

	err = c.freezeDataPartition(volName, dp.PartitionID)
	assertErrNilOtherwiseFailNow(t, err)

	dpInfo, err = mc.AdminAPI().GetDataPartition(volName, pid)
	if !assert.Truef(t, dp.IsFrozen, "%v should not be frozen", dp.PartitionID) {
		t.FailNow()
	}

	err = c.unfreezeDataPartition(volName, dp.PartitionID)
	assertErrNilOtherwiseFailNow(t, err)

	dpInfo, err = mc.AdminAPI().GetDataPartition(volName, pid)
	if !assert.Falsef(t, dpInfo.IsFrozen, "%v should not be frozen", dp.PartitionID) {
		t.FailNow()
	}
}

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)
vol: zone2-ssd -> zone2-ssd,zone3-ssd
idc1: zone2-ssd zone1-hdd
idc2: zone3-ssd zone6-hdd
*/
/*
func TestGetTargetAddressForBalanceDataPartitionZone1(t *testing.T) {
	volName := "TestGetTargetAddressForBalanceDataPartitionZone1"
	idcName1 := "idc1TestGetTargetAddressForBalanceDataPartitionZone1"
	idcName2 := "idc2TestGetTargetAddressForBalanceDataPartitionZone1"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone1, idcName1, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	_, err = c.t.createIDC(idcName2, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, testZone2, "", testSmartRules, 0, "default")
	if err != nil {
		t.Errorf("CreateVolume err:%v", err)
		return
	}

	var vol *Vol
	vol, err = c.getVol(volName)
	if err != nil {
		return
	}
	t.Logf("smart: %v", vol.isSmart)

	vol.zoneName = fmt.Sprintf("%v,%v", testZone2, testZone3)

	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	if len(dps.DataPartitions) == 0 {
		t.Error(len(dps.DataPartitions))
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for index, replica := range dp.Replicas {
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		dn, err := c.dataNode(replica.Addr)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		//isNeed, err := dp.needToRebalanceZoneForSmartVol(c, []string{testZone1, testZone3})
		isNeed, err := dp.needToRebalanceZoneForSmartVol(c, strings.Split(vol.zoneName, ","))
		if err != nil {
			t.Errorf(err.Error())
			t.FailNow()
		}
		t.Logf("isNeed: %v, vol: %v", isNeed, vol.zoneName)
		if index > 0 && isNeed {
			t.Logf("index: %v, isNeed: %v", index, isNeed)
			t.FailNow()
		}
		if index > 0 && !isNeed {
			break
		}
		oldAddr, newAddr, err := getTargetAddressForBalanceDataPartitionZone(c, replica.Addr, dp, nil, "", true)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		_, err = zone.getDataNode(newAddr)
		if err == nil {
			t.FailNow()
		}

		expectedZoneName := testZone3
		zone, err = c.t.getZone(expectedZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		// check the new add is belong to expected zone
		_, err = zone.getDataNode(newAddr)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("index: %v, old addr: %v, new addr: %v", index, oldAddr, newAddr)
	}
}

*/

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)
vol: zone2-ssd -> zone6-ssd
idc1: zone2-ssd zone1-hdd
idc2: zone6-ssd zone3-hdd
*/
/*
func TestGetTargetAddressForBalanceDataPartitionZone2(t *testing.T) {
	volName := "TestGetTargetAddressForBalanceDataPartitionZone2"
	idcName1 := "idc1TestGetTargetAddressForBalanceDataPartitionZone2"
	idcName2 := "idc2TestGetTargetAddressForBalanceDataPartitionZone2"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone1, idcName1, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	_, err = c.t.createIDC(idcName2, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true, testZone2, "", testSmartRules, 0, "default")
	if err != nil {
		t.Errorf("CreateVolume err:%v", err)
		return
	}

	var vol *Vol
	vol, err = c.getVol(volName)
	if err != nil {
		return
	}
	t.Logf("smart: %v", vol.isSmart)

	vol.zoneName = testZone6

	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	if len(dps.DataPartitions) == 0 {
		t.Error(len(dps.DataPartitions))
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for index, replica := range dp.Replicas {
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		dn, err := c.dataNode(replica.Addr)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		isNeed, err := dp.needToRebalanceZoneForSmartVol(c, strings.Split(vol.zoneName, ","))
		if err != nil {
			t.Errorf(err.Error())
			t.FailNow()
		}
		t.Logf("isNeed: %v, vol: %v", isNeed, vol.zoneName)
		if !isNeed {
			t.Logf("index: %v, isNeed: %v", index, isNeed)
			t.FailNow()
		}
		oldAddr, newAddr, err := getTargetAddressForBalanceDataPartitionZone(c, replica.Addr, dp, nil, "", true)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		_, err = zone.getDataNode(newAddr)
		if err == nil {
			t.FailNow()
		}

		expectedZoneName := testZone6
		zone, err = c.t.getZone(expectedZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		// check the new add is belong to expected zone
		_, err = zone.getDataNode(newAddr)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("index: %v, old addr: %v, new addr: %v", index, oldAddr, newAddr)
	}
}

*/

/*
	addDataServer(mds1Addr, testZone1)
	addDataServer(mds2Addr, testZone1)
	addDataServer(mds3Addr, testZone2)
	addDataServer(mds4Addr, testZone2)
	addDataServer(mds5Addr, testZone2)
	addDataServer(mds6Addr, testZone2)
	addDataServer(mds7Addr, testZone3)
	addDataServer(mds8Addr, testZone3)
	addDataServer(mds11Addr, testZone6)
	addDataServer(mds12Addr, testZone6)
	addDataServer(mds13Addr, testZone6)
vol: zone1-ssd,zone6-ssd -> zone6-ssd
idc1: zone1-ssd zone2-hdd
idc2: zone6-ssd zone3-hdd
*/
/*
func TestGetTargetAddressForBalanceDataPartitionZone3(t *testing.T) {
	volName := "TestGetTargetAddressForBalanceDataPartitionZone3"
	idcName1 := "idc1TestGetTargetAddressForBalanceDataPartitionZone3"
	idcName2 := "idc2TestGetTargetAddressForBalanceDataPartitionZone3"
	c := server.cluster
	_, err := c.t.createIDC(idcName1, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone1, idcName1, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone2, idcName1, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	_, err = c.t.createIDC(idcName2, c)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone6, idcName2, proto.MediumSSD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	err = c.setZoneIDC(testZone3, idcName2, proto.MediumHDD)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	defer log.LogFlush()
	err = mc.AdminAPI().CreateVolume(volName, "cfs", 3, 120, 200, 3, 3, 0, int(proto.StoreModeMem),
		false, false, false, true, true,
		fmt.Sprintf("%v,%v", testZone1, testZone6), "", testSmartRules, 0, "default")
	if err != nil {
		t.Errorf("CreateVolume err:%v", err)
		return
	}

	var vol *Vol
	vol, err = c.getVol(volName)
	if err != nil {
		return
	}
	t.Logf("smart: %v", vol.isSmart)

	vol.zoneName = testZone6

	dps, err := mc.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	if len(dps.DataPartitions) == 0 {
		t.Error(len(dps.DataPartitions))
		t.FailNow()
	}

	pid := dps.DataPartitions[0].PartitionID
	dp, err := c.getDataPartitionByID(pid)

	for index, replica := range dp.Replicas {
		t.Logf("index; %v, mType: %v, dps: %v, pid: %v, addr: %v", index, replica.MType, len(dps.DataPartitions), pid, replica.Addr)
		dn, err := c.dataNode(replica.Addr)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		if dn.ZoneName == testZone6 {
			continue
		}

		isNeed, err := dp.needToRebalanceZoneForSmartVol(c, strings.Split(vol.zoneName, ","))
		if err != nil {
			t.Errorf(err.Error())
			t.FailNow()
		}
		if !isNeed {
			t.Errorf("index: %v, isNeed: %v", index, isNeed)
			t.FailNow()
		}
		oldAddr, newAddr, err := getTargetAddressForBalanceDataPartitionZone(c, replica.Addr, dp, nil, "", true)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		// check the new add is belong to old hosts
		zone, err := c.t.getZone(dn.ZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		_, err = zone.getDataNode(newAddr)
		if err == nil {
			t.Errorf("not found the data node: %v, from zone: %v", newAddr, zone.name)
			t.FailNow()
		}

		expectedZoneName := testZone6
		zone, err = c.t.getZone(expectedZoneName)
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}

		// check the new add is belong to expected zone
		_, err = zone.getDataNode(newAddr)
		if err != nil {
			t.Errorf("new addr: %v, err: %v", newAddr, err.Error())
			t.FailNow()
		}

		// update the dp's replicas and hosts
		replica.Addr = newAddr
		hosts := make([]string, 0)
		for _, host := range dp.Hosts {
			if host == oldAddr {
				host = newAddr
			}
			hosts = append(hosts, host)
		}
		dp.Hosts = hosts
		t.Logf("index: %v, old addr: %v, new addr: %v", index, oldAddr, newAddr)
	}
}
*/

func addEcServer(addr, httpPort, zoneName string) {
	ecs := mocktest.NewMockEcServer(addr, httpPort, zoneName)
	ecs.Start()
}

func addCodecServer(addr, httpPort, zoneName string) {
	mcs := mocktest.NewMockCodecServer(addr, httpPort, zoneName)
	mcs.Start()
}

func TestSetDisableStrictVolZone(t *testing.T) {
	testCases := []struct {
		Value  bool
		Expect bool
	}{
		{true, true},
		{false, false},
	}
	for _, testCase := range testCases {
		reqURL := fmt.Sprintf("%v%v?disableStrictVolZone=%v", hostAddr, proto.AdminSetNodeInfo, strconv.FormatBool(testCase.Value))
		process(reqURL, t)
		limitInfo, err := mc.AdminAPI().GetLimitInfo("")
		if assert.Nil(t, err) {
			return
		}
		assert.Equal(t, limitInfo.DisableStrictVolZone, testCase.Expect)
	}
}

func TestSetAutoUpdatePartitionReplicaNum(t *testing.T) {
	server.cluster.cfg.AutoUpdatePartitionReplicaNum = true
	server.cluster.scheduleToCheckUpdatePartitionReplicaNum()
	server.cluster.cfg.AutoUpdatePartitionReplicaNum = false
	testCases := []struct {
		Value  bool
		Expect bool
	}{
		{true, true},
		{false, false},
	}
	for _, testCase := range testCases {
		reqURL := fmt.Sprintf("%v%v?autoUpdatePartitionReplicaNum=%v", hostAddr, proto.AdminSetNodeInfo, strconv.FormatBool(testCase.Value))
		process(reqURL, t)
		limitInfo, err := mc.AdminAPI().GetLimitInfo("")
		if err != nil {
			assert.Fail(t, err.Error())
		}
		assert.Equal(t, limitInfo.AutoUpdatePartitionReplicaNum, testCase.Expect)
	}
}

func addFlashServer(addr, zoneName string) (mfs *mocktest.MockFlashNodeServer) {
	mfs = mocktest.NewMockFlashNodeServer(addr, zoneName)
	mfs.Start()
	return mfs
}

func TestGetFlashNode(t *testing.T) {
	testCases := []struct {
		NodeAddr string
		ZoneName string
	}{
		{mfs1Addr, testZone1},
		{mfs2Addr, testZone1},
		{mfs3Addr, testZone1},
		{mfs4Addr, testZone2},
		{mfs5Addr, testZone2},
		{mfs6Addr, testZone2},
		{mfs7Addr, testZone2},
	}
	for _, testCase := range testCases {
		flashNodeViewInfo, err := mc.NodeAPI().GetFlashNode(testCase.NodeAddr)
		if !assert.NoError(t, err) {
			continue
		}
		msg := fmt.Sprintf("expect Addr,ZoneName:(%v,%v), but get:(%v,%v)", testCase.NodeAddr, testCase.ZoneName,
			flashNodeViewInfo.Addr, flashNodeViewInfo.ZoneName)
		if !assert.Equalf(t, testCase.NodeAddr, flashNodeViewInfo.Addr, msg) || !assert.Equalf(t, testCase.ZoneName, flashNodeViewInfo.ZoneName, msg) {
			continue
		}
		t.Logf("flashNode Info:%v", *flashNodeViewInfo)
	}
}

func TestSetFlashNode(t *testing.T) {
	testCases := []struct {
		NodeAddr string
		ZoneName string
	}{
		{mfs1Addr, testZone1},
	}
	testAddr := testCases[0].NodeAddr
	flashNodeViewInfo, err := mc.NodeAPI().GetFlashNode(testAddr)
	if !assert.NoError(t, err) {
		return
	}

	if !assert.Equalf(t, defaultFlashNodeOnlineState, flashNodeViewInfo.IsEnable, "expect defaultFlashNodeOnlineState:%v", defaultFlashNodeOnlineState) {
		return
	}
	err = mc.NodeAPI().SetFlashNodeState(testAddr, "false")
	if !assert.NoError(t, err) {
		return
	}
	flashNodeViewInfo, err = mc.NodeAPI().GetFlashNode(testAddr)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equalf(t, false, flashNodeViewInfo.IsEnable, "expect isEnable:false")
	err = mc.NodeAPI().SetFlashNodeState(testAddr, "true")
	if !assert.NoError(t, err) {
		return
	}
}

func TestUpdateVolCacheConfig(t *testing.T) {
	if err := updateVolCacheConfig(commonVolName, "", true, false, 0); err != nil {
		t.Error(err)
		return
	}
	if err := updateVolCacheConfig(commonVolName, "/cache", true, true, 100); err != nil {
		t.Error(err)
		return
	}
	if err := updateVolCacheConfig(commonVolName, "/cache/cnn", true, false, 500); err != nil {
		t.Error(err)
		return
	}
	if err := updateVolCacheConfig(commonVolName, "/", false, false, 0); err != nil {
		t.Error(err)
		return
	}
}

func updateVolCacheConfig(volName, remoteCacheBoostPath string, remoteCacheBoostEnable, remoteCacheAutoPrepare bool, remoteCacheTTL int64) (err error) {
	vv, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		return
	}
	err = mc.AdminAPI().UpdateVolume(
		vv.Name,
		vv.Capacity,
		int(vv.DpReplicaNum),
		int(vv.MpReplicaNum),
		int(vv.TrashRemainingDays),
		int(vv.DefaultStoreMode),
		vv.FollowerRead,
		vv.VolWriteMutexEnable,
		vv.NearRead,
		vv.Authenticate,
		vv.EnableToken,
		vv.AutoRepair,
		vv.ForceROW,
		vv.IsSmart,
		vv.EnableWriteCache,
		buildAuthKey(vv.Owner),
		vv.ZoneName,
		fmt.Sprintf("%v,%v", vv.MpLayout.PercentOfMP, vv.MpLayout.PercentOfReplica), // MP Layout
		strings.Join(vv.SmartRules, ","),                                            // Smart Rules
		uint8(vv.OSSBucketPolicy),
		uint8(vv.CrossRegionHAType),
		vv.ExtentCacheExpireSec,
		vv.CompactTag,
		vv.DpFolReadDelayConfig.DelaySummaryInterval,
		vv.FolReadHostWeight,
		0,
		0,
		0,
		exporter.UMPCollectMethodFile,
		-1,
		-1,
		false,
		remoteCacheBoostPath,
		remoteCacheBoostEnable,
		remoteCacheAutoPrepare,
		remoteCacheTTL)
	if err != nil {
		return
	}
	vv, err = mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		return
	}
	if vv.RemoteCacheBoostPath != remoteCacheBoostPath {
		return fmt.Errorf("expect RemoteCacheBoostPath:%v but get:%v", remoteCacheBoostPath, vv.RemoteCacheBoostPath)
	}
	if vv.RemoteCacheBoostEnable != remoteCacheBoostEnable {
		return fmt.Errorf("expect RemoteCacheBoostEnable:%v but get:%v", remoteCacheBoostEnable, vv.RemoteCacheBoostEnable)
	}
	if vv.RemoteCacheAutoPrepare != remoteCacheAutoPrepare {
		return fmt.Errorf("expect RemoteCacheAutoPrepare:%v but get:%v", remoteCacheAutoPrepare, vv.RemoteCacheAutoPrepare)
	}
	if vv.RemoteCacheTTL != remoteCacheTTL {
		return fmt.Errorf("expect RemoteCacheTTL:%v but get:%v", remoteCacheTTL, vv.RemoteCacheTTL)
	}
	return
}

func TestSetRemoteCacheHandler(t *testing.T) {
	remoteCacheEnableState := 1
	reqURL := fmt.Sprintf("%v%v?remoteCacheBoostEnable=%v", hostAddr, proto.AdminSetNodeInfo, remoteCacheEnableState)
	fmt.Println(reqURL)
	process(reqURL, t)
	if server.cluster.cfg.RemoteCacheBoostEnable != true {
		t.Errorf("set remoteCacheEnableState to %v failed", remoteCacheEnableState)
		return
	}
	reqURL = fmt.Sprintf("%v%v", hostAddr, proto.AdminGetLimitInfo)
	fmt.Println(reqURL)
	reply := processReturnRawReply(reqURL, t)
	limitInfo := &proto.LimitInfo{}

	if err := json.Unmarshal(reply.Data, limitInfo); err != nil {
		t.Errorf("unmarshal limitinfo failed,err:%v", err)
	}
	if limitInfo.RemoteCacheBoostEnable != true {
		t.Errorf("remoteCacheEnableState expect:true,real:%v", limitInfo.RemoteCacheBoostEnable)
	}

}

func TestSetOfflineState(t *testing.T) {
	var (
		addrList []string
		nodeType string
		zoneName string
		state    string
		err      error
	)
	zoneName = "zone1"
	state = "true"
	nodeType = "all"
	addrList = []string{
		"127.0.0.1:9101",
		"127.0.0.1:9102",
		"127.0.0.1:8101",
		"127.0.0.1:8102",
		"127.0.0.1:10301",
		"127.0.0.1:10302",
	}
	err = mc.AdminAPI().SetNodeToOfflineState(addrList, nodeType, zoneName, state)
	assert.NoError(t, err)
	for _, addr := range addrList {
		if v, ok := server.cluster.dataNodes.Load(addr); ok {
			node := v.(*DataNode)
			assert.Equal(t, true, node.ToBeMigrated)
		}
		if v, ok := server.cluster.metaNodes.Load(addr); ok {
			node := v.(*MetaNode)
			assert.Equal(t, true, node.ToBeMigrated)
		}
		if v, ok := server.cluster.ecNodes.Load(addr); ok {
			node := v.(*ECNode)
			assert.Equal(t, true, node.ToBeMigrated)
		}
	}
}

func TestSetNodeSetCapacity(t *testing.T) {
	cv, _ := mc.AdminAPI().GetCluster()
	fmt.Println(cv.NodeSetCapacity)
	t.Run("test update setNodeCapacity", func(t *testing.T) {
		_, err := mc.AdminAPI().SetNodeSetCapacity(512)
		assert.NoError(t, err)
		assert.Equal(t, 512, server.cluster.cfg.nodeSetCapacity)
	})
	t.Run("test invalid setNodeCapacity: 60", func(t *testing.T) {
		_, err := mc.AdminAPI().SetNodeSetCapacity(60)
		assert.Error(t, err)
		assert.Equal(t, 512, server.cluster.cfg.nodeSetCapacity)
	})
	t.Run("test invalid setNodeCapacity: -1", func(t *testing.T) {
		_, err := mc.AdminAPI().SetNodeSetCapacity(-1)
		assert.Error(t, err)
		assert.Equal(t, 512, server.cluster.cfg.nodeSetCapacity)
	})
}
