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
	"fmt"
	"net/http"
	"testing"
	_ "net/http/pprof"
	"github.com/chubaofs/chubaofs/master/mocktest"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"time"
	"io/ioutil"
	"os"
	"crypto/md5"
	"encoding/hex"
	"github.com/chubaofs/chubaofs/proto"
	"encoding/json"
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

	mms1Addr      = "127.0.0.1:8101"
	mms2Addr      = "127.0.0.1:8102"
	mms3Addr      = "127.0.0.1:8103"
	mms4Addr      = "127.0.0.1:8104"
	mms5Addr      = "127.0.0.1:8105"
	mms6Addr      = "127.0.0.1:8106"
	commonVolName = "commonVol"
)

var server = createMasterServer()
var commonVol *Vol

func createMasterServer() *Server {
	cfgJson := `{
	"role": "master",
		"ip": "127.0.0.1",
		"port": "8080",
		"prof":"10088",
		"id":"1",
		"peers": "1:127.0.0.1:8080",
		"retainLogs":"20000",
		"logDir": "/export/chubaofs/Logs",
		"logLevel":"DEBUG",
		"walDir":"/export/chubaofs/raft",
		"storeDir":"/export/chubaofs/rocksdbstore",
		"clusterName":"chubaofs"
	}`
	cfg := config.LoadConfigString(cfgJson)
	server := NewServer()
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
	if _, err := log.InitLog(logDir, "master", level, nil); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
	}
	if profPort != "" {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf(":%v", profPort), nil)
			if err != nil {
				panic(fmt.Sprintf("cannot listen pprof %v err %v", profPort, err.Error()))
			}
		}()
	}
	server.Start(cfg)
	time.Sleep(5 * time.Second)
	fmt.Println(server.config.peerAddrs, server.leaderInfo.addr)
	//add data node
	addDataServer(mds1Addr, "rack1")
	addDataServer(mds2Addr, "rack1")
	addDataServer(mds3Addr, "rack2")
	addDataServer(mds4Addr, "rack2")
	addDataServer(mds5Addr, "rack2")
	// add meta node
	addMetaServer(mms1Addr)
	addMetaServer(mms2Addr)
	addMetaServer(mms3Addr)
	addMetaServer(mms4Addr)
	addMetaServer(mms5Addr)
	time.Sleep(5 * time.Second)
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.scheduleToUpdateStatInfo()
	fmt.Printf("nodeSet len[%v]\n", len(server.cluster.t.nodeSetMap))
	server.cluster.createVol(commonVolName, "cfs", 3, 3, 100)
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		panic(err)
	}
	commonVol = vol
	fmt.Printf("vol[%v] has created\n", commonVol.Name)
	return server
}

func addDataServer(addr, rackName string) {
	mds := mocktest.NewMockDataServer(addr, rackName)
	mds.Start()
}

func addMetaServer(addr string) {
	mms := mocktest.NewMockMetaServer(addr)
	mms.Start()
}

func TestSetMetaNodeThreshold(t *testing.T) {
	threshold := 0.5
	reqUrl := fmt.Sprintf("%v%v?threshold=%v", hostAddr, proto.AdminSetMetaNodeThreshold, threshold)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if server.cluster.cfg.MetaNodeThreshold != float32(threshold) {
		t.Errorf("set metanode threshold to %v failed", threshold)
		return
	}
}

func TestSetDisableAutoAlloc(t *testing.T) {
	enable := true
	reqUrl := fmt.Sprintf("%v%v?enable=%v", hostAddr, proto.AdminClusterFreeze, enable)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if server.cluster.DisableAutoAllocate != enable {
		t.Errorf("set disableAutoAlloc to %v failed", enable)
		return
	}
}

func TestGetCluster(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestGetIpAndClusterName(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetIP)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func process(reqUrl string, t *testing.T) (reply *proto.HTTPReply) {
	resp, err := http.Get(reqUrl)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(string(body))
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
		t.Errorf("failed,msg[%v]", reply.Data)
		return
	}
	return
}

func TestDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs"
	decommissionDisk(addr, disk, t)
}

func decommissionDisk(addr, path string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v&disk=%v",
		hostAddr, proto.DecommissionDisk, addr, path)
	fmt.Println(reqUrl)
	resp, err := http.Get(reqUrl)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	reply := &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		t.Error(err)
		return
	}
}

func TestMarkDeleteVol(t *testing.T) {
	name := "delVol"
	createVol(name, t)
	reqUrl := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.AdminDeleteVol, name, buildAuthKey())
	process(reqUrl, t)
}

func TestUpdateVol(t *testing.T) {
	capacity := 2000
	reqUrl := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, proto.AdminUpdateVol, commonVol.Name, capacity, buildAuthKey())
	process(reqUrl, t)
}
func buildAuthKey() string {
	h := md5.New()
	h.Write([]byte("cfs"))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func TestGetVolSimpleInfo(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, commonVol.Name)
	process(reqUrl, t)
}

func TestCreateVol(t *testing.T) {
	name := "test_create_vol"
	reqUrl := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs", hostAddr, proto.AdminCreateVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestCreateMetaPartition(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	commonVol.checkMetaPartitions(server.cluster)
	createMetaPartition(commonVol, t)
}

func TestCreateDataPartition(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?count=2&name=%v&type=extent",
		hostAddr, proto.AdminCreateDataPartition, commonVol.Name)
	process(reqUrl, t)
}

func TestGetDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.partitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	reqUrl := fmt.Sprintf("%v%v?id=%v", hostAddr, proto.AdminGetDataPartition, partition.PartitionID)
	process(reqUrl, t)

	reqUrl = fmt.Sprintf("%v%v?id=%v&name=%v", hostAddr, proto.AdminGetDataPartition, partition.PartitionID, partition.VolName)
	process(reqUrl, t)
}

func TestLoadDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.partitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	reqUrl := fmt.Sprintf("%v%v?id=%v&name=%v",
		hostAddr, proto.AdminLoadDataPartition, partition.PartitionID, commonVol.Name)
	process(reqUrl, t)
}

func TestDataPartitionDecommission(t *testing.T) {
	if len(commonVol.dataPartitions.partitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	partition := commonVol.dataPartitions.partitions[0]
	offlineAddr := partition.Hosts[0]
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionDataPartition, commonVol.Name, partition.PartitionID, offlineAddr)
	process(reqUrl, t)
	if contains(partition.Hosts, offlineAddr) {
		t.Errorf("offlineAddr[%v],hosts[%v]", offlineAddr, partition.Hosts)
		return
	}
}

//func TestGetAllVols(t *testing.T) {
//	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.GetALLVols)
//	process(reqUrl, t)
//}
//
func TestGetMetaPartitions(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientMetaPartitions, commonVolName)
	process(reqUrl, t)
}

func TestGetDataPartitions(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientDataPartitions, commonVolName)
	process(reqUrl, t)
}

func TestGetTopo(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.GetTopologyView)
	process(reqUrl, t)
}

func TestGetMetaNode(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, mms1Addr)
	process(reqUrl, t)
}
