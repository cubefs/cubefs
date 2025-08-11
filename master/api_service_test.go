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
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/master/mocktest"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	masterAddr        = "127.0.0.1:8080"
	hostAddr          = "http://" + masterAddr
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
	mds1Addr          = "127.0.0.1:9101"
	mds2Addr          = "127.0.0.1:9102"
	mds3Addr          = "127.0.0.1:9103"
	mds4Addr          = "127.0.0.1:9104"
	mds5Addr          = "127.0.0.1:9105"
	mds6Addr          = "127.0.0.1:9106"
	mds7Addr          = "127.0.0.1:9107"
	mds1HddAddr       = "127.0.0.1:9111"
	mds2HddAddr       = "127.0.0.1:9112"
	mds3HddAddr       = "127.0.0.1:9113"

	mms1Addr      = "127.0.0.1:8101"
	mms2Addr      = "127.0.0.1:8102"
	mms3Addr      = "127.0.0.1:8103"
	mms4Addr      = "127.0.0.1:8104"
	mms5Addr      = "127.0.0.1:8105"
	mms6Addr      = "127.0.0.1:8106"
	mms7Addr      = "127.0.0.1:8107"
	mms8Addr      = "127.0.0.1:8108"
	commonVolName = "commonVol"
	testZone1     = "zone1"
	testZone2     = "zone2"
	testZone3     = "zone3"
	testHddZone1  = "hdd_zone1"

	mfs1Addr = "127.0.0.1:10501"
	mfs2Addr = "127.0.0.1:10502"
	mfs3Addr = "127.0.0.1:10503"
	mfs4Addr = "127.0.0.1:10504"
	mfs5Addr = "127.0.0.1:10505"
	mfs6Addr = "127.0.0.1:10506"
	mfs7Addr = "127.0.0.1:10507"
	mfs8Addr = "127.0.0.1:10508"

	testUserID  = "testUser"
	ak          = "0123456789123456"
	sk          = "01234567891234560123456789123456"
	description = "testUser"
)

var (
	server                 = createDefaultMasterServerForTest()
	mc                     = master.NewMasterClient([]string{masterAddr}, false)
	commonVol              *Vol
	defaultVolStorageClass = proto.StorageClass_Replica_SSD
	defaultMediaType       = proto.MediaType_SSD
	cfsUser                *proto.UserInfo

	mockServerLock   sync.Mutex
	mockDataServers  []*mocktest.MockDataServer
	mockMetaServers  []*mocktest.MockMetaServer
	mockFlashServers []*mocktest.MockFlashServer
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	server.clearMetadata()
	os.Exit(exitCode)
}

func rangeMockDataServers(fun func(*mocktest.MockDataServer) bool) (count int, passed int) {
	mockServerLock.Lock()
	defer mockServerLock.Unlock()
	count = len(mockDataServers)
	for _, mds := range mockDataServers {
		passed += 1
		if !fun(mds) {
			return
		}
	}
	return
}

func rangeMockMetaServers(fun func(*mocktest.MockMetaServer) bool) (count int, passed int) {
	mockServerLock.Lock()
	defer mockServerLock.Unlock()
	count = len(mockMetaServers)
	for _, mms := range mockMetaServers {
		passed += 1
		if !fun(mms) {
			return
		}
	}
	return
}

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
		"logDir": "/tmp/cubefs/Logs",
		"logLevel":"DEBUG",
		"walDir":"/tmp/cubefs/raft",
		"storeDir":"/tmp/cubefs/rocksdbstore",
		"clusterName":"cubefs",
		"bStoreAddr":"127.0.0.1:8500",
		"bStoreServicePath":"access",
        "enableDirectDeleteVol":true,
		"legacyDataMediaType": 1
	}`

	testServer, err := createMasterServer(cfgJSON)
	testServer.cluster.cfg.volForceDeletion = true

	if err != nil {
		panic(err)
	}
	// add data node
	mockDataServers = make([]*mocktest.MockDataServer, 0)
	mockDataServers = append(mockDataServers, addDataServer(mds1Addr, testZone1, defaultMediaType))
	mockDataServers = append(mockDataServers, addDataServer(mds2Addr, testZone1, defaultMediaType))
	mockDataServers = append(mockDataServers, addDataServer(mds3Addr, testZone2, defaultMediaType))
	mockDataServers = append(mockDataServers, addDataServer(mds4Addr, testZone2, defaultMediaType))
	mockDataServers = append(mockDataServers, addDataServer(mds5Addr, testZone2, defaultMediaType))
	mockDataServers = append(mockDataServers, addDataServer(mds6Addr, testZone2, defaultMediaType))
	mockDataServers = append(mockDataServers,
		addDataServer(mds1HddAddr, testHddZone1, proto.MediaType_HDD),
		addDataServer(mds2HddAddr, testHddZone1, proto.MediaType_HDD),
		addDataServer(mds3HddAddr, testHddZone1, proto.MediaType_HDD),
	)

	// add meta node
	mockMetaServers = make([]*mocktest.MockMetaServer, 0)
	mockMetaServers = append(mockMetaServers, addMetaServer(mms1Addr, testZone1))
	mockMetaServers = append(mockMetaServers, addMetaServer(mms2Addr, testZone1))
	mockMetaServers = append(mockMetaServers, addMetaServer(mms3Addr, testZone2))
	mockMetaServers = append(mockMetaServers, addMetaServer(mms4Addr, testZone2))
	mockMetaServers = append(mockMetaServers, addMetaServer(mms5Addr, testZone2))
	mockMetaServers = append(mockMetaServers, addMetaServer(mms6Addr, testZone2))

	// add flash node
	mockFlashServers = append(mockFlashServers,
		addFlashServer(mfs1Addr, testZone1),
		addFlashServer(mfs2Addr, testZone1),
		addFlashServer(mfs3Addr, testZone2),
		addFlashServer(mfs4Addr, testZone2),
		addFlashServer(mfs5Addr, testZone3),
		addFlashServer(mfs6Addr, testZone3),
		addFlashServer(mfs7Addr, testZone3),
	)

	// we should wait 5 seoncds for master to prepare state
	time.Sleep(5 * time.Second)
	testServer.cluster.checkDataNodeHeartbeat()
	testServer.cluster.checkMetaNodeHeartbeat()
	testServer.cluster.checkFlashNodeHeartbeat()
	time.Sleep(5 * time.Second)
	testServer.cluster.scheduleToUpdateStatInfo()
	// set load factor
	err = testServer.cluster.setClusterLoadFactor(100)
	if err != nil {
		panic("set load factor fail" + err.Error())
	}

	req := &createVolReq{
		name:             commonVolName,
		owner:            "cfs",
		dpSize:           11,
		mpCount:          3,
		dpReplicaNum:     3,
		capacity:         300,
		followerRead:     false,
		authenticate:     false,
		crossZone:        false,
		normalZonesFirst: false,
		zoneName:         testZone2,
		description:      "",
		qosLimitArgs:     &qosArgs{},
		volStorageClass:  defaultVolStorageClass,
	}

	err = testServer.checkCreateVolReq(req)
	if err != nil {
		panic("checkCreateVolReq failed: " + err.Error())
	}

	_, err = testServer.cluster.createVol(req)
	if err != nil {
		log.LogFlush()
		panic(err)
	}

	vol, err := testServer.cluster.getVol(req.name)
	if err != nil {
		panic(err)
	}

	commonVol = vol
	fmt.Printf("Volume[%+v] has created\n", newSimpleView(commonVol))

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
	fmt.Printf("user[%v] has created\n", cfsUser.UserID)
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
	os.Mkdir(walDir, 0o755)
	os.Mkdir(storeDir, 0o755)
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
	if mocktest.LogOn {
		if _, err = log.InitLog(logDir, "master", level, nil, log.DefaultLogLeftSpaceLimitRatio); err != nil {
			fmt.Println("Fatal: failed to start the cubefs daemon - ", err)
			return
		}
	}
	if profPort != "" {
		go func() {
			mainMux := http.NewServeMux()
			mux := http.NewServeMux()
			mux.Handle("/debug/pprof", http.HandlerFunc(pprof.Index))
			mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
			mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
			mux.Handle("/debug/", http.HandlerFunc(pprof.Index))
			mainHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasPrefix(req.URL.Path, "/debug/") {
					mux.ServeHTTP(w, req)
				} else {
					http.DefaultServeMux.ServeHTTP(w, req)
				}
			})
			mainMux.Handle("/", mainHandler)
			err := http.ListenAndServe(fmt.Sprintf(":%v", profPort), mainMux)
			if err != nil {
				panic(fmt.Sprintf("cannot listen pprof %v err %v", profPort, err.Error()))
			}
		}()
	}
	if err = server.Start(cfg); err != nil {
		return
	}
	time.Sleep(5 * time.Second)
	fmt.Println(server.config.peerAddrs, server.leaderInfo.addr)
	return server, nil
}

func addDataServer(addr, zoneName string, mediaType uint32) *mocktest.MockDataServer {
	mds := mocktest.NewMockDataServer(addr, zoneName, mediaType)
	mds.Start()
	return mds
}

func addMetaServer(addr, zoneName string) *mocktest.MockMetaServer {
	mms := mocktest.NewMockMetaServer(addr, zoneName)
	mms.Start()
	return mms
}

func addFlashServer(addr, zoneName string) *mocktest.MockFlashServer {
	mms := mocktest.NewMockFlashServer(addr, zoneName)
	mms.Start()
	return mms
}

func TestSetMetaNodeThreshold(t *testing.T) {
	threshold := 0.5
	reqURL := fmt.Sprintf("%v%v?threshold=%v", hostAddr, proto.AdminSetMetaNodeThreshold, threshold)
	process(reqURL, t)
	if server.cluster.cfg.MetaNodeThreshold != float32(threshold) {
		t.Errorf("set metanode threshold to %v failed", threshold)
		return
	}
}

func TestSetDisableAutoAlloc(t *testing.T) {
	req := map[string]interface{}{"enable": true}
	processWithFatalV2(proto.AdminClusterFreeze, true, req, t)

	if !server.cluster.DisableAutoAllocate {
		t.Errorf("set disableAutoAlloc to %v failed", true)
		return
	}

	req = map[string]interface{}{"enable": false}
	processWithFatalV2(proto.AdminClusterFreeze, true, req, t)
	if server.cluster.DisableAutoAllocate {
		t.Errorf("set disableAutoAlloc to %v failed", false)
		return
	}
}

func TestGetCluster(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	process(reqURL, t)
}

func TestGetClusterDataNodes(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetClusterDataNodes)
	process(reqURL, t)
}

func TestGetClusterMetaNodes(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetClusterMetaNodes)
	process(reqURL, t)
}

func TestGetIpAndClusterName(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetIP)
	process(reqURL, t)
}

type httpReply = proto.HTTPReplyRaw

func processWithFatalV2(url string, success bool, req map[string]interface{}, t *testing.T) (reply *httpReply) {
	reqURL := buildUrl(hostAddr, url, req)

	resp, err := http.Get(reqURL)
	assert.Nil(t, err)
	assert.True(t, resp.StatusCode == http.StatusOK)

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	assert.Nil(t, err)

	mocktest.Log(t, string(body), reqURL)

	reply = &httpReply{}
	err = json.Unmarshal(body, reply)
	assert.Nil(t, err)

	if success {
		require.EqualValues(t, proto.ErrCodeSuccess, reply.Code)
		return reply
	}

	require.NotEqualValues(t, proto.ErrCodeSuccess, reply.Code)

	return
}

func processNoCheck(reqURL string, t testing.TB) (reply *proto.HTTPReply) {
	mocktest.Log(t, reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply = &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
	return
}

func process(reqURL string, t testing.TB) (reply *proto.HTTPReply) {
	reply = processNoCheck(reqURL, t)
	if reply.Code != 0 {
		t.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	return
}

func processCompression(reqURL string, compress bool, t testing.TB) (reply *proto.HTTPReply) {
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		t.Errorf("new request failed.err:%+v", err)
		return
	}
	if compress {
		req.Header.Add(proto.HeaderAcceptEncoding, compressor.EncodingGzip)
	}
	mocktest.Log(t, reqURL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	body, err = compressor.New(resp.Header.Get(proto.HeaderContentEncoding)).Decompress(body)
	if err != nil {
		t.Errorf("decompress failed. err is %v", err)
		return
	}
	mocktest.Println(string(body))
	reply = &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
	if reply.Code != 0 {
		t.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	return
}

func TestDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs"
	decommissionDisk(addr, disk, t)
	cancelDecommissionDisk(addr, disk, t)
}

func cancelDecommissionDisk(addr, path string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v",
		hostAddr, proto.CancelDecommissionDisk, addr, path)
	mocktest.Log(t, reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply := &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
	key := fmt.Sprintf("%s_%s", addr, path)
	_, ok := server.cluster.DecommissionDisks.Load(key)
	if ok {
		t.Errorf("disk should be removed from DecommissionDisks")
	}
}

func decommissionDisk(addr, path string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v",
		hostAddr, proto.DecommissionDisk, addr, path)
	mocktest.Log(t, reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply := &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDiskRecoveryProgress()
}

func TestMarkDeleteVol(t *testing.T) {
	name := "delVol"
	createVol(map[string]interface{}{nameKey: name}, t)

	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
	process(reqURL, t)

	userInfo, err := server.user.getUserInfo("cfs")
	if err != nil {
		t.Error(err)
		return
	}

	if contains(userInfo.Policy.OwnVols, name) {
		t.Errorf("expect no vol %v in own vols, but is exist", name)
		return
	}
}

func TestVolSetBucketLifecycle(t *testing.T) {
	req := &createVolReq{
		name:                "bktLifecycle",
		owner:               "cfs",
		dpSize:              11,
		mpCount:             3,
		dpReplicaNum:        3,
		capacity:            300,
		followerRead:        false,
		authenticate:        false,
		crossZone:           true,
		zoneName:            "",
		description:         "",
		qosLimitArgs:        &qosArgs{},
		volStorageClass:     defaultVolStorageClass,
		allowedStorageClass: []uint32{defaultVolStorageClass, proto.StorageClass_Replica_HDD},
	}

	_, err := server.cluster.createVol(req)
	if err != nil {
		log.LogFlush()
		t.FailNow()
	}

	days := 10

	rule := proto.Rule{
		ID:     "r1",
		Status: proto.RuleEnabled,
		Filter: &proto.Filter{
			Prefix:  "test/",
			MinSize: 1024,
		},
		Transitions: []*proto.Transition{
			{
				Days:         &days,
				StorageClass: "HDD",
			},
		},
	}

	lc := &proto.LcConfiguration{
		VolName: req.name,
		Rules: []*proto.Rule{
			&rule,
		},
	}

	data, err := json.Marshal(lc)
	require.NoError(t, err)

	url := fmt.Sprintf("%s%s", hostAddr, proto.SetBucketLifecycle)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	require.NoError(t, err)

	defer resp.Body.Close()

	data1, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("data: %s, url %s", string(data1), url)

	require.True(t, resp.StatusCode == http.StatusOK)

	lc2 := server.cluster.GetBucketLifecycle(lc.VolName)
	require.True(t, len(lc2.Rules) == 1)
	require.True(t, lc2.Rules[0].MinSize() == lc.Rules[0].MinSize())
}

func TestSetVolCapacity(t *testing.T) {
	setVolCapacity(600, proto.AdminVolExpand, t)
	setVolCapacity(300, proto.AdminVolShrink, t)
}

func TestUpdateVol(t *testing.T) {
	volName := "updateVol"
	req := map[string]interface{}{}
	req[nameKey] = volName
	req[volStorageClassKey] = proto.StorageClass_BlobStore
	req[remoteCacheReadTimeout] = proto.ReadDeadlineTime

	createVol(req, t)

	view := getSimpleVol(volName, true, t)

	// name can't be empty
	checkParam(nameKey, proto.AdminUpdateVol, req, "", volName, t)
	// vol name not exist
	checkParam(nameKey, proto.AdminUpdateVol, req, "tt", volName, t)
	// auth key can't be empty
	checkParam(volAuthKey, proto.AdminUpdateVol, req, "", buildAuthKey(testOwner), t)

	view2 := getSimpleVol(volName, true, t)
	// if not set use default
	assert.True(t, view.Capacity == view2.Capacity)
	assert.True(t, view.Description == view2.Description)
	assert.True(t, view.ZoneName == view2.ZoneName)
	assert.True(t, view.Authenticate == view2.Authenticate)
	assert.True(t, view.FollowerRead == view2.FollowerRead)
	assert.True(t, view.ObjBlockSize == view2.ObjBlockSize)

	// update
	cap := 1024
	desc := "hello_test"
	blkSize := 6 * 1024

	checkParam(volCapacityKey, proto.AdminUpdateVol, req, "tt", cap, t)
	setParam(descriptionKey, proto.AdminUpdateVol, req, desc, t)
	checkParam(zoneNameKey, proto.AdminUpdateVol, req, "default", testZone1, t)
	checkParam(authenticateKey, proto.AdminUpdateVol, req, "tt", true, t)
	checkParam(followerReadKey, proto.AdminUpdateVol, req, "test", true, t)
	checkParam(ebsBlkSizeKey, proto.AdminUpdateVol, req, "-1", blkSize, t)
	checkParam("remoteCacheEnable", proto.AdminUpdateVol, req, "not-bool", true, t)
	checkParam("remoteCacheAutoPrepare", proto.AdminUpdateVol, req, "not-bool", false, t)
	checkParam("remoteCacheTTL", proto.AdminUpdateVol, req, "not-number", int64(77), t)
	checkParam("remoteCacheReadTimeout", proto.AdminUpdateVol, req, "not-number", int64(7), t)
	setParam("remoteCachePath", proto.AdminUpdateVol, req, "cache-path,a-path", t)

	view = getSimpleVol(volName, true, t)
	// check update result
	assert.True(t, int(view.Capacity) == cap)
	assert.True(t, view.Description == desc)
	assert.True(t, view.ZoneName == testZone1)
	assert.True(t, view.Authenticate)
	// LF vol always be true
	assert.True(t, view.FollowerRead)
	assert.True(t, view.ObjBlockSize == blkSize)
	require.True(t, view.RemoteCacheEnable)
	require.Equal(t, "a-path,cache-path", view.RemoteCachePath)
	require.False(t, view.RemoteCacheAutoPrepare)
	require.Equal(t, int64(77), view.RemoteCacheTTL)
	require.Equal(t, int64(7), view.RemoteCacheReadTimeout)

	for id, name := range []string{"z1", "z2", "z3"} {
		zone := newZone(name, defaultMediaType)
		nodeSet1 := newNodeSet(server.cluster, uint64(id), 6, name)

		zone.putNodeSet(nodeSet1)
		server.cluster.t.putZone(zone)
	}

	req[zoneNameKey] = "z1,z2,z3"
	processWithFatalV2(proto.AdminUpdateVol, false, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v", view.ZoneName)
	assert.True(t, view.ZoneName != "z1,z2,z3")
	assert.True(t, view.CrossZone == false)

	req[zoneNameKey] = "z1,z2,z3"
	req[crossZoneKey] = true
	processWithFatalV2(proto.AdminUpdateVol, true, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v", view.ZoneName)
	assert.True(t, view.ZoneName == "z1,z2,z3")
	assert.True(t, view.CrossZone == true)

	req[zoneNameKey] = "z1"
	req[crossZoneKey] = false
	processWithFatalV2(proto.AdminUpdateVol, true, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v crosszone %v", view.ZoneName, view.CrossZone)
	assert.True(t, view.ZoneName == "z1")
	assert.True(t, view.CrossZone == false)

	req[zoneNameKey] = "z2"
	req[crossZoneKey] = true
	processWithFatalV2(proto.AdminUpdateVol, false, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v crosszone %v", view.ZoneName, view.CrossZone)
	assert.True(t, view.ZoneName == "z1")
	assert.True(t, view.CrossZone == false)

	// zonename cann't be set from nonempty to empty, because volume update always have no param zoneName
	req[zoneNameKey] = ""
	req[crossZoneKey] = true
	processWithFatalV2(proto.AdminUpdateVol, false, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v crosszone %v", view.ZoneName, view.CrossZone)
	assert.True(t, view.ZoneName == "z1")
	assert.True(t, view.CrossZone == false)

	req[zoneNameKey] = "z1,z2"
	req[crossZoneKey] = false
	processWithFatalV2(proto.AdminUpdateVol, false, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v", view.ZoneName)
	assert.True(t, view.ZoneName == "z1")
	assert.True(t, view.CrossZone == false)

	req[zoneNameKey] = "z1,z2"
	req[crossZoneKey] = true
	processWithFatalV2(proto.AdminUpdateVol, true, req, t)
	view = getSimpleVol(volName, true, t)
	t.Logf("zonename %v", view.ZoneName)
	assert.True(t, view.ZoneName == "z1,z2")
	assert.True(t, view.CrossZone == true)

	// vol cann't be delete except no inode and dentry exist
	// delVol(volName, t)
	//
	// time.Sleep(10 * time.Second)
	// // can't update vol after delete
	// checkParam(cacheLRUIntervalKey, proto.AdminUpdateVol, req, lru, lru, t)
}

func delVol(name string, t *testing.T) {
	req := map[string]interface{}{
		nameKey:    name,
		volAuthKey: buildAuthKey(testOwner),
	}

	processWithFatalV2(proto.AdminDeleteVol, true, req, t)

	vol, err := server.cluster.getVol(name)
	assert.True(t, err == nil)

	assert.True(t, vol.Status == proto.VolStatusMarkDelete)
}

func setVolCapacity(capacity uint64, url string, t *testing.T) {
	req := map[string]interface{}{
		"name":     commonVol.Name,
		"capacity": capacity,
		"authKey":  buildAuthKey(testOwner),
	}

	processWithFatalV2(url, true, req, t)

	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}

	if vol.Capacity != capacity {
		t.Errorf("expect capacity is %v, but is %v", capacity, vol.Capacity)
		return
	}

	mocktest.Printf("update capacity to %d success\n", capacity)
}

func buildAuthKey(owner string) string {
	h := md5.New()
	h.Write([]byte(owner))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func TestGetVolSimpleInfo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, commonVol.Name)
	process(reqURL, t)
}

func TestCreateVol(t *testing.T) {
	name := "test_create_vol"
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfstest&zoneName=%v", hostAddr, proto.AdminCreateVol, name, testZone2)
	process(reqURL, t)
	userInfo, err := server.user.getUserInfo("cfstest")
	if err != nil {
		t.Error(err)
		return
	}
	if !contains(userInfo.Policy.OwnVols, name) {
		t.Errorf("expect vol %v in own vols, but is not", name)
		return
	}
}

func TestCreateMetaPartition(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	commonVol.checkMetaPartitions(server.cluster)
	createMetaPartition(commonVol, t)
}

func TestCreateDataPartition(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?count=2&name=%v&type=extent&force=true",
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
}

func TestLoadDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.partitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	reqURL := fmt.Sprintf("%v%v?id=%v&name=%v",
		hostAddr, proto.AdminLoadDataPartition, partition.PartitionID, commonVol.Name)
	process(reqURL, t)
}

func TestDataPartitionDecommission(t *testing.T) {
	name := "decommissionTestVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	require.NoError(t, err)
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	require.NotEqualValues(t, 0, len(vol.dataPartitions.partitions))
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	partition := vol.dataPartitions.partitions[0]
	offlineAddr := partition.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v&weight=%v",
		hostAddr, proto.AdminDecommissionDataPartition, vol.Name, partition.PartitionID, offlineAddr, highPriorityDecommissionWeight)
	for _, replica := range partition.Replicas {
		replica.LocalPeers = partition.Peers
	}
	process(reqURL, t)
	require.EqualValues(t, markDecommission, partition.GetDecommissionStatus())
	require.EqualValues(t, highPriorityDecommissionWeight, partition.DecommissionWeight)
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

func TestGetDataPartitionsNotCompress(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientDataPartitions, commonVolName)
	processCompression(reqURL, false, t)
}

func TestGetDataPartitionsGzip(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientDataPartitions, commonVolName)
	processCompression(reqURL, true, t)
}

func TestGetTopo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.GetTopologyView)
	process(reqURL, t)
}

func TestGetMetaNode(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, mms1Addr)
	process(reqURL, t)
}

func TestGetNodeInfo(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetNodeInfo)
	process(reqURL, t)
}

func TestSetNodeMaxDpCntLimit(t *testing.T) {
	// change current max data partition count limit to 4000
	limit := uint64(4000)
	addr := mds1Addr
	reqURL := fmt.Sprintf("%v%v?addr=%v&maxDpCntLimit=%v", hostAddr, proto.SetDpCntLimit, addr, limit)
	process(reqURL, t)
	// query data node info
	reqURL = fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetDataNode, addr)
	reply := process(reqURL, t)
	data := reply.Data.(map[string]interface{})
	dataNodeLimit := uint64((data[maxDpCntLimitKey]).(float64))
	assert.True(t, dataNodeLimit == limit)
}

func TestSetNodeMaxMpCntLimit(t *testing.T) {
	// change current max meta partition count limit to 600
	limit := uint64(600)
	addr := mms1Addr
	reqURL := fmt.Sprintf("%v%v?addr=%v&maxMpCntLimit=%v", hostAddr, proto.SetMpCntLimit, addr, limit)
	process(reqURL, t)
	// query data node info
	reqURL = fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, addr)
	reply := process(reqURL, t)
	data := reply.Data.(map[string]interface{})
	dataNodeLimit := uint64((data[maxMpCntLimitKey]).(float64))
	assert.True(t, dataNodeLimit == limit)
}

func TestAddDataReplica(t *testing.T) {
	partition := commonVol.dataPartitions.partitions[0]
	dsAddr := mds7Addr
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockDataServers = append(mockDataServers, addDataServer(dsAddr, "zone2", defaultMediaType))
	}()
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v&force=true", hostAddr, proto.AdminAddDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, dsAddr) {
		t.Errorf("hosts[%v] should contains dsAddr[%v]", partition.Hosts, dsAddr)
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
	dsAddr := mds7Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v&force=true", hostAddr, proto.AdminDeleteDataReplica, partition.PartitionID, dsAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, dsAddr) {
		t.Errorf("hosts[%v] should not contains dsAddr[%v]", partition.Hosts, dsAddr)
		partition.RUnlock()
		return
	}
	partition.isRecover = false
	partition.RUnlock()
}

func TestAddMetaReplica(t *testing.T) {
	maxPartitionID := commonVol.maxMetaPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if partition == nil {
		t.Error("no meta partition")
		return
	}
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockMetaServers = append(mockMetaServers, addMetaServer(mms8Addr, testZone3))
	}()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(2 * time.Second)
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminAddMetaReplica, partition.PartitionID, mms8Addr)
	process(reqURL, t)
	partition.RLock()
	if !contains(partition.Hosts, mms8Addr) {
		t.Errorf("hosts[%v] should contains dsAddr[%v]", partition.Hosts, mms8Addr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestRemoveMetaReplica(t *testing.T) {
	maxPartitionID := commonVol.maxMetaPartitionID()
	partition := commonVol.MetaPartitions[maxPartitionID]
	if partition == nil {
		t.Error("no meta partition")
		return
	}
	partition.IsRecover = false
	msAddr := mms8Addr
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.AdminDeleteMetaReplica, partition.PartitionID, msAddr)
	process(reqURL, t)
	partition.RLock()
	if contains(partition.Hosts, msAddr) {
		t.Errorf("hosts[%v] should contains dsAddr[%v]", partition.Hosts, msAddr)
		partition.RUnlock()
		return
	}
	partition.RUnlock()
}

func TestClusterStat(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminClusterStat)
	process(reqUrl, t)
}

func TestListVols(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?keywords=%v", hostAddr, proto.AdminListVols, commonVolName)
	process(reqURL, t)
}

func TestUpdateNodesetNodeSelector(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	ns := nsc[0]
	reqUrl := fmt.Sprintf("%v%v?zoneName=%v&id=%v", hostAddr, proto.AdminUpdateNodeSetNodeSelector, testZone2, ns.ID)
	updateDataSelectorUrl := fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, RoundRobinNodeSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, RoundRobinNodeSelectorName)
	process(updateDataSelectorUrl, t)
	if ns.GetDataNodeSelector() != RoundRobinNodeSelectorName {
		t.Errorf("failed to change data node selector")
		return
	}
	process(updateMetaSelectorUrl, t)
	if ns.GetMetaNodeSelector() != RoundRobinNodeSelectorName {
		t.Errorf("failed to change meta node selector")
		return
	}
	updateDataSelectorUrl = fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

func TestUpdateZoneNodesetSelector(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	reqUrl := fmt.Sprintf("%v%v?name=%v&enable=1", hostAddr, proto.UpdateZone, testZone2)
	updateDataSelectorUrl := fmt.Sprintf("%v&dataNodesetSelector=%v", reqUrl, CarryWeightNodesetSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v&metaNodesetSelector=%v", reqUrl, CarryWeightNodesetSelectorName)
	process(updateDataSelectorUrl, t)
	if zone.GetDataNodesetSelector() != CarryWeightNodesetSelectorName {
		t.Errorf("failed to change data nodeset selector")
	}
	process(updateMetaSelectorUrl, t)
	if zone.GetMetaNodesetSelector() != CarryWeightNodesetSelectorName {
		t.Errorf("failed to change meta nodeset selector")
	}
	updateDataSelectorUrl = fmt.Sprintf("%v&dataNodesetSelector=%v", reqUrl, RoundRobinNodesetSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v&metaNodesetSelector=%v", reqUrl, RoundRobinNodesetSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

func TestUpdateZoneNodeSelector(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	reqUrl := fmt.Sprintf("%v%v?name=%v&enable=1", hostAddr, proto.UpdateZone, testZone2)
	updateDataSelectorUrl := fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	process(updateDataSelectorUrl, t)
	failed := false
	func() {
		zone.nsLock.RLock()
		defer zone.nsLock.RUnlock()
		for _, ns := range zone.nodeSetMap {
			if ns.GetDataNodeSelector() != StrawNodeSelectorName {
				t.Errorf("failed to change data nodeset selector")
				failed = true
			}
		}
	}()
	if failed {
		return
	}
	process(updateMetaSelectorUrl, t)
	func() {
		zone.nsLock.RLock()
		defer zone.nsLock.RUnlock()
		for _, ns := range zone.nodeSetMap {
			if ns.GetMetaNodeSelector() != StrawNodeSelectorName {
				t.Errorf("failed to change data nodeset selector")
				failed = true
			}
		}
	}()
	if failed {
		return
	}
	updateDataSelectorUrl = fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

func TestUpdateZoneStatus(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.UpdateZone, testZone2)
	enableUrl := fmt.Sprintf("%v&enable=1", reqUrl)
	disableUrl := fmt.Sprintf("%v&enable=0", reqUrl)
	process(disableUrl, t)
	if zone.getStatus() != unavailableZone {
		t.Errorf("failed to update zone status")
		return
	}
	process(enableUrl, t)
}

func post(reqURL string, data []byte, t *testing.T) (reply *proto.HTTPReply) {
	mocktest.Log(t, reqURL)
	reader := bytes.NewReader(data)
	req, err := http.NewRequest(http.MethodPost, reqURL, reader)
	if err != nil {
		t.Errorf("generate request err: %v", err)
		return
	}
	var resp *http.Response
	if resp, err = http.DefaultClient.Do(req); err != nil {
		t.Errorf("post err: %v", err)
		return
	}
	mocktest.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	mocktest.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply = &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
	if reply.Code != 0 {
		t.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	return
}

func TestCreateUser(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserCreate)
	param := &proto.UserCreateParam{ID: testUserID, Type: proto.UserTypeNormal}
	data, err := json.Marshal(param)
	if err != nil {
		t.Error(err)
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
	if err != nil {
		t.Error(err)
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	if userInfo.AccessKey != ak {
		t.Errorf("expect ak[%v], real ak[%v]\n", ak, userInfo.AccessKey)
		return
	}
	if userInfo.SecretKey != sk {
		t.Errorf("expect sk[%v], real sk[%v]\n", sk, userInfo.SecretKey)
		return
	}
	if userInfo.UserType != proto.UserTypeAdmin {
		t.Errorf("expect ak[%v], real ak[%v]\n", proto.UserTypeAdmin, userInfo.UserType)
		return
	}
	if userInfo.Description != description {
		t.Errorf("expect description[%v], real description[%v]\n", description, userInfo.Description)
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
	if err != nil {
		t.Error(err)
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	if _, exist := userInfo.Policy.AuthorizedVols[commonVolName]; !exist {
		t.Errorf("expect vol %v in authorized vols, but is not", commonVolName)
		return
	}
}

func TestRemovePolicy(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserRemovePolicy)
	param := &proto.UserPermRemoveParam{UserID: testUserID, Volume: commonVolName}
	data, err := json.Marshal(param)
	if err != nil {
		t.Error(err)
		return
	}
	post(reqURL, data, t)
	userInfo, err := server.user.getUserInfo(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	if _, exist := userInfo.Policy.AuthorizedVols[commonVolName]; exist {
		t.Errorf("expect no vol %v in authorized vols, but is exist", commonVolName)
		return
	}
}

func TestTransferVol(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.UserTransferVol)
	param := &proto.UserTransferVolParam{Volume: commonVolName, UserSrc: "cfs", UserDst: testUserID, Force: false}
	data, err := json.Marshal(param)
	if err != nil {
		t.Error(err)
		return
	}
	post(reqURL, data, t)
	userInfo1, err := server.user.getUserInfo(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	if !contains(userInfo1.Policy.OwnVols, commonVolName) {
		t.Errorf("expect vol %v in own vols, but is not", commonVolName)
		return
	}
	userInfo2, err := server.user.getUserInfo("cfs")
	if err != nil {
		t.Error(err)
		return
	}
	if contains(userInfo2.Policy.OwnVols, commonVolName) {
		t.Errorf("expect no vol %v in own vols, but is exist", commonVolName)
		return
	}
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Owner != testUserID {
		t.Errorf("expect owner is %v, but is %v", testUserID, vol.Owner)
		return
	}
}

func TestDeleteVolPolicy(t *testing.T) {
	param := &proto.UserPermUpdateParam{UserID: "cfs", Volume: commonVolName, Policy: []string{proto.BuiltinPermissionWritable.String()}}
	if _, err := server.user.updatePolicy(param); err != nil {
		t.Error(err)
		return
	}
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.UserDeleteVolPolicy, commonVolName)
	process(reqURL, t)
	userInfo1, err := server.user.getUserInfo(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	if contains(userInfo1.Policy.OwnVols, commonVolName) {
		t.Errorf("expect no vol %v in own vols, but is not", commonVolName)
		return
	}
	userInfo2, err := server.user.getUserInfo("cfs")
	if err != nil {
		t.Error(err)
		return
	}
	if _, exist := userInfo2.Policy.AuthorizedVols[commonVolName]; exist {
		t.Errorf("expect no vols %v in authorized vol is 0, but is exist", commonVolName)
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
	if _, err := server.user.getUserInfo(testUserID); err != proto.ErrUserNotExists {
		t.Errorf("expect err ErrUserNotExists, but err is %v", err)
		return
	}
}

func TestListUsersOfVol(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.UsersOfVol, "test_create_vol")
	process(reqURL, t)
}

func TestListNodeSets(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.GetAllNodeSets)
	process(reqURL, t)

	reqURL = fmt.Sprintf("%v%v?zoneName=%v", hostAddr, proto.GetAllNodeSets, testZone2)
	process(reqURL, t)
}

func TestGetNodeSets(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?nodesetId=1", hostAddr, proto.GetNodeSet)
	process(reqURL, t)
}

func TestUpdateNodeSet(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	ns := nsc[0]
	reqUrl := fmt.Sprintf("%v%v?nodesetId=%v", hostAddr, proto.UpdateNodeSet, ns.ID)
	updateDataSelectorUrl := fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	process(updateDataSelectorUrl, t)
	if ns.GetDataNodeSelector() != StrawNodeSelectorName {
		t.Errorf("failed to change data nodeset selector")
		return
	}
	process(updateMetaSelectorUrl, t)
	if ns.GetMetaNodeSelector() != StrawNodeSelectorName {
		t.Errorf("failed to change data nodeset selector")
		return
	}
	updateDataSelectorUrl = fmt.Sprintf("%v&dataNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v&metaNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

func TestUpdateClusterNodeSelector(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	updateDataSelectorUrl := fmt.Sprintf("%v?dataNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v?metaNodeSelector=%v", reqUrl, StrawNodeSelectorName)
	process(updateDataSelectorUrl, t)
	failed := false
	func() {
		zone.nsLock.RLock()
		defer zone.nsLock.RUnlock()
		for _, ns := range zone.nodeSetMap {
			if ns.GetDataNodeSelector() != StrawNodeSelectorName {
				t.Errorf("failed to change data nodeset selector")
				failed = true
			}
		}
	}()
	if failed {
		return
	}
	process(updateMetaSelectorUrl, t)
	func() {
		zone.nsLock.RLock()
		defer zone.nsLock.RUnlock()
		for _, ns := range zone.nodeSetMap {
			if ns.GetMetaNodeSelector() != StrawNodeSelectorName {
				t.Errorf("failed to change data nodeset selector")
				failed = true
			}
		}
	}()
	if failed {
		return
	}
	updateDataSelectorUrl = fmt.Sprintf("%v?dataNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v?metaNodeSelector=%v", reqUrl, CarryWeightNodeSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

func TestUpdateClusterNodesetSelector(t *testing.T) {
	zone, err := server.cluster.t.getZone(testZone2)
	if err != nil {
		t.Errorf("failed to get zone, %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Error("nodeset count could not be 0")
		return
	}
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	updateDataSelectorUrl := fmt.Sprintf("%v?dataNodesetSelector=%v", reqUrl, CarryWeightNodesetSelectorName)
	updateMetaSelectorUrl := fmt.Sprintf("%v?metaNodesetSelector=%v", reqUrl, CarryWeightNodesetSelectorName)
	process(updateDataSelectorUrl, t)
	if zone.GetDataNodesetSelector() != CarryWeightNodesetSelectorName {
		t.Errorf("failed to change data nodeset selector")
		return
	}
	process(updateMetaSelectorUrl, t)
	if zone.GetMetaNodesetSelector() != CarryWeightNodesetSelectorName {
		t.Errorf("failed to change meta nodeset selector")
		return
	}
	updateDataSelectorUrl = fmt.Sprintf("%v?dataNodesetSelector=%v", reqUrl, RoundRobinNodesetSelectorName)
	updateMetaSelectorUrl = fmt.Sprintf("%v?metaNodesetSelector=%v", reqUrl, RoundRobinNodesetSelectorName)
	process(updateDataSelectorUrl, t)
	process(updateMetaSelectorUrl, t)
}

const volPartitionCheckTimeout = 60

func checkVolForbidden(name string, forbidden bool) (success bool) {
	dataChecker := func(mdp *mocktest.MockDataPartition) bool {
		return mdp.IsForbidden() == forbidden
	}
	for i := 0; i < volPartitionCheckTimeout; i++ {
		okCount := 0
		count, _ := rangeMockDataServers(func(mds *mocktest.MockDataServer) bool {
			if mds.CheckVolPartition(name, dataChecker) {
				okCount += 1
			}
			return true
		})
		if count == okCount {
			success = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !success {
		return
	}
	success = false
	metaChecker := func(mmp *mocktest.MockMetaPartition) bool {
		return mmp.IsForbidden() == forbidden
	}
	for i := 0; i < volPartitionCheckTimeout; i++ {
		okCount := 0
		count, _ := rangeMockMetaServers(func(mms *mocktest.MockMetaServer) bool {
			if mms.CheckVolPartition(name, metaChecker) {
				okCount += 1
			}
			return true
		})
		if count == okCount {
			success = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	return
}

func TestForbiddenVolume(t *testing.T) {
	name := "forbiddenVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminVolForbidden)
	forbidUrl := fmt.Sprintf("%v?name=%v&%v=true", reqUrl, vol.Name, forbiddenKey)
	unforbidUrl := fmt.Sprintf("%v?name=%v&%v=false", reqUrl, vol.Name, forbiddenKey)
	process(forbidUrl, t)
	t.Logf("check forbid volume")
	dps := vol.cloneDataPartitionMap()
	mps := vol.cloneMetaPartitionMap()
	for _, dp := range dps {
		if dp.Status == proto.ReadWrite {
			t.Errorf("failed to set dp rd only")
			return
		}
	}
	for _, mp := range mps {
		if mp.Status == proto.ReadWrite {
			t.Errorf("failed to set mp rd only")
			return
		}
	}
	ok := checkVolForbidden(vol.Name, vol.Forbidden)
	if !ok {
		t.Errorf("failed to forbid volume, check timeout")
		return
	}
	process(unforbidUrl, t)
	t.Logf("check unforbid volume")
	ok = checkVolForbidden(vol.Name, vol.Forbidden)
	if !ok {
		t.Errorf("failed to unforbid volume, check timeout")
		return
	}
	for _, dp := range dps {
		if dp.Status != proto.ReadWrite {
			t.Errorf("failed to set dp rd only")
			return
		}
	}
	for _, mp := range mps {
		if mp.Status != proto.ReadWrite {
			t.Errorf("failed to set mp rd only")
			return
		}
	}
}

func checkVolAuditLog(name string, enable bool) (success bool) {
	metaChecker := func(mmp *mocktest.MockMetaPartition) bool {
		return mmp.IsEnableAuditLog() == enable
	}
	for i := 0; i < volPartitionCheckTimeout; i++ {
		okCount := 0
		count, _ := rangeMockMetaServers(func(mms *mocktest.MockMetaServer) bool {
			if mms.CheckVolPartition(name, metaChecker) {
				okCount += 1
			}
			return true
		})
		if count == okCount {
			success = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	return
}

func TestVolumeEnableAuditLog(t *testing.T) {
	name := "auditLogVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminVolEnableAuditLog)
	enableUrl := fmt.Sprintf("%v?name=%v&%v=true", reqUrl, vol.Name, enableKey)
	disableUrl := fmt.Sprintf("%v?name=%v&%v=false", reqUrl, vol.Name, enableKey)
	process(disableUrl, t)
	require.False(t, vol.DisableAuditLog)
	require.True(t, checkVolAuditLog(name, true))
	process(enableUrl, t)
	require.True(t, vol.DisableAuditLog)
	require.True(t, checkVolAuditLog(name, false))
}

func checkVolDpRepairBlockSize(name string, size uint64) (success bool) {
	dataChecker := func(dp *mocktest.MockDataPartition) (ok bool) {
		return dp.GetDpRepairBlockSize() == size
	}
	for i := 0; i < volPartitionCheckTimeout; i++ {
		okCount := 0
		count, _ := rangeMockDataServers(func(mds *mocktest.MockDataServer) bool {
			if mds.CheckVolPartition(name, dataChecker) {
				okCount++
			}
			return true
		})
		if count == okCount {
			success = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	return
}

func TestSetVolumeDpRepairBlockSize(t *testing.T) {
	name := "dpRepairBlockSizeVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminVolSetDpRepairBlockSize)
	repairSize := 1 * util.MB
	setUrl := fmt.Sprintf("%v?name=%v&%v=%v", reqUrl, vol.Name, dpRepairBlockSizeKey, repairSize)
	unsetUrl := fmt.Sprintf("%v?name=%v&%v=%v", reqUrl, vol.Name, dpRepairBlockSizeKey, proto.DefaultDpRepairBlockSize)
	process(setUrl, t)
	require.EqualValues(t, repairSize, vol.dpRepairBlockSize)
	require.True(t, checkVolDpRepairBlockSize(name, uint64(repairSize)))
	process(unsetUrl, t)
	require.EqualValues(t, proto.DefaultDpRepairBlockSize, vol.dpRepairBlockSize)
	require.True(t, checkVolDpRepairBlockSize(name, proto.DefaultDpRepairBlockSize))
}

func TestSetMarkDiskBrokenThreshold(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	setVal := 0.5
	oldVal := server.cluster.getMarkDiskBrokenThreshold()
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, markDiskBrokenThresholdKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, markDiskBrokenThresholdKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.getMarkDiskBrokenThreshold())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.getMarkDiskBrokenThreshold())
}

func TestSetEnableAutoDecommissionDisk(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.EnableAutoDecommissionDisk.Load()
	setVal := !oldVal
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDecommissionDiskKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDecommissionDiskKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.EnableAutoDecommissionDisk.Load())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.EnableAutoDecommissionDisk.Load())
}

func TestSetDecommissionDiskInterval(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.GetAutoDecommissionDiskInterval()
	setVal := oldVal + 10*time.Second
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDecommissionDiskIntervalKey, int64(setVal))
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDecommissionDiskIntervalKey, int64(oldVal))
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.GetAutoDecommissionDiskInterval())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.GetAutoDecommissionDiskInterval())
}

func TestSetEnableAutoDpMetaRepair(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.getEnableAutoDpMetaRepair()
	setVal := !oldVal
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDpMetaRepairKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDpMetaRepairKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.getEnableAutoDpMetaRepair())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.getEnableAutoDpMetaRepair())
}

func TestSetAutoDpMetaRepairParallelCnt(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.GetAutoDpMetaRepairParallelCnt()
	setVal := 200
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDpMetaRepairParallelCntKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, autoDpMetaRepairParallelCntKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.GetAutoDpMetaRepairParallelCnt())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.GetAutoDpMetaRepairParallelCnt())
}

func TestSetDpTimeout(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.getDataPartitionTimeoutSec()
	setVal := int64(10)
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, dpTimeoutKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, dpTimeoutKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.getDataPartitionTimeoutSec())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.getDataPartitionTimeoutSec())
}

func TestSetMpTimeout(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.getMetaPartitionTimeoutSec()
	setVal := int64(10)
	setUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, mpTimeoutKey, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&dirSizeLimit=0", reqUrl, mpTimeoutKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.getMetaPartitionTimeoutSec())
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.getMetaPartitionTimeoutSec())
}

func TestSetDpRepairTimeout(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetNodeInfo)
	oldVal := server.cluster.cfg.DpRepairTimeOut
	setVal := 4 * time.Hour
	setUrl := fmt.Sprintf("%v?%v=%v", reqUrl, nodeDpRepairTimeOutKey, uint64(setVal))
	unsetUrl := fmt.Sprintf("%v?%v=%v", reqUrl, nodeDpRepairTimeOutKey, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, time.Duration(server.cluster.cfg.DpRepairTimeOut))
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.cfg.DpRepairTimeOut)
}

func TestSetDiscardDp(t *testing.T) {
	name := "setDiscardVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	dpMap := vol.cloneDataPartitionMap()
	var dp *DataPartition
	for _, dp = range dpMap {
		break
	}
	require.NotNil(t, dp)
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminSetDpDiscard)
	setUrl := fmt.Sprintf("%v?%v=%v&%v=true", reqUrl, idKey, dp.PartitionID, dpDiscardKey)
	forceSetUrl := fmt.Sprintf("%v&force=true", setUrl)
	unsetUrl := fmt.Sprintf("%v?%v=%v&%v=false", reqUrl, idKey, dp.PartitionID, dpDiscardKey)
	processNoCheck(setUrl, t)
	time.Sleep(2 * time.Second)
	require.False(t, dp.IsDiscard)

	process(forceSetUrl, t)
	time.Sleep(2 * time.Second)
	require.True(t, dp.IsDiscard)

	process(unsetUrl, t)
	require.False(t, dp.IsDiscard)
}

func TestUpdateDecommissionFirstHostParallelLimit(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminUpdateDecommissionFirstHostParallelLimit)
	dataNode, _ := server.cluster.dataNode(mds1Addr)
	oldVal := dataNode.DecommissionFirstHostParallelLimit
	setVal := oldVal + 1
	setUrl := fmt.Sprintf("%v?%v=%v&%v=%v", reqUrl, addrKey, mds1Addr, decommissionFirstHostParallelLimit, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v&%v=%v", reqUrl, addrKey, mds1Addr, decommissionFirstHostParallelLimit, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, dataNode.DecommissionFirstHostParallelLimit)
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, dataNode.DecommissionFirstHostParallelLimit)
}

func TestUpdateDecommissionFirstHostDiskParallelLimit(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminUpdateDecommissionFirstHostDiskParallelLimit)
	oldVal := server.cluster.DecommissionFirstHostDiskParallelLimit
	setVal := oldVal + 1
	setUrl := fmt.Sprintf("%v?%v=%v", reqUrl, decommissionFirstHostDiskParallelLimit, setVal)
	unsetUrl := fmt.Sprintf("%v?%v=%v", reqUrl, decommissionFirstHostDiskParallelLimit, oldVal)
	process(setUrl, t)
	require.EqualValues(t, setVal, server.cluster.DecommissionFirstHostDiskParallelLimit)
	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.DecommissionFirstHostDiskParallelLimit)
}

func TestSetDecommissionDiskLimit(t *testing.T) {
	oldVal := server.cluster.GetDecommissionDiskLimit()
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminUpdateDecommissionDiskLimit)
	setUrl := fmt.Sprintf("%v?%v=%v", reqUrl, decommissionDiskLimit, oldVal+1)
	unsetUrl := fmt.Sprintf("%v?%v=%v", reqUrl, decommissionDiskLimit, oldVal)

	process(setUrl, t)
	require.EqualValues(t, oldVal+1, server.cluster.GetDecommissionDiskLimit())

	process(unsetUrl, t)
	require.EqualValues(t, oldVal, server.cluster.GetDecommissionDiskLimit())
}

func TestUpdateVolAutoDpMetaRepair(t *testing.T) {
	name := "enableAutoDpMetaRepairVol"
	createVol(map[string]interface{}{nameKey: name, remoteCacheReadTimeout: proto.ReadDeadlineTime}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}
	defer func() {
		reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey(testOwner))
		process(reqURL, t)
	}()
	reqUrl := fmt.Sprintf("%v%v?%v=%v&%v=%v", hostAddr, proto.AdminUpdateVol, nameKey, vol.Name, volAuthKey, buildAuthKey(testOwner))
	oldVal := vol.EnableAutoMetaRepair.Load()
	setVal := !oldVal
	setUrl := fmt.Sprintf("%v&%v=%v", reqUrl, autoDpMetaRepairKey, setVal)
	unsetUrl := fmt.Sprintf("%v&%v=%v", reqUrl, autoDpMetaRepairKey, oldVal)

	process(setUrl, t)
	require.EqualValues(t, setVal, vol.EnableAutoMetaRepair.Load())

	process(unsetUrl, t)
	require.EqualValues(t, oldVal, vol.EnableAutoMetaRepair.Load())
}

func TestGetMetaPartitionEmptyStatus(t *testing.T) {
	name := "emptyMpVol"
	createVol(map[string]interface{}{nameKey: name}, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}

	if err = vol.addMetaPartitions(server.cluster, 3); err != nil {
		t.Errorf("failed to get vol %v, err %v", name, err)
		return
	}

	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminMetaPartitionEmptyStatus)

	process(reqUrl, t)
}

func TestMetaPartitionFreezeEmpty(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=emptyMpVol&count=3", hostAddr, proto.AdminMetaPartitionFreezeEmpty)

	process(reqUrl, t)
}

func TestCleanEmptyMetaPartition(t *testing.T) {
	name := "emptyMpVol2"
	createVol(map[string]interface{}{nameKey: name}, t)
	reqUrl := fmt.Sprintf("%v%v?name=emptyMpVol2", hostAddr, proto.AdminMetaPartitionCleanEmpty)

	process(reqUrl, t)
}

func TestDeleteLostDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs/disk2"
	reqUrl := fmt.Sprintf("%v%v?addr=%v&disk=%v", hostAddr, proto.DeleteLostDisk, addr, disk)

	process(reqUrl, t)
}

func TestReloadDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs/disk2"
	reqUrl := fmt.Sprintf("%v%v?addr=%v&disk=%v", hostAddr, proto.ReloadDisk, addr, disk)

	process(reqUrl, t)
}

func TestSetFileStats(t *testing.T) {
	enable := true
	thresholds := []uint64{1024, 2048}
	thresholdStr := strings.Join(func() []string {
		result := make([]string, len(thresholds))
		for i, v := range thresholds {
			result[i] = strconv.FormatUint(v, 10)
		}
		return result
	}(), ",")
	reqURL := fmt.Sprintf("%v%v?enable=%v&threshold=%v", hostAddr, proto.AdminSetFileStats, enable, thresholdStr)

	process(reqURL, t)

	require.EqualValues(t, enable, server.cluster.fileStatsEnable)
	require.EqualValues(t, thresholds, server.cluster.fileStatsThresholds)
}
