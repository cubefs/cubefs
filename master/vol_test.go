package master

import (
	"testing"
	"github.com/chubaofs/chubaofs/util"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"time"
	"github.com/chubaofs/chubaofs/proto"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	commonVol.Capacity = 300 * util.TB
	dpCount := len(commonVol.dataPartitions.partitions)
	commonVol.dataPartitions.readableAndWritableCnt = 0
	server.cluster.DisableAutoAllocate = false
	t.Logf("status[%v],disableAutoAlloc[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAllocate, commonVol.Capacity)
	commonVol.checkAutoDataPartitionCreation(server.cluster)
	newDpCount := len(commonVol.dataPartitions.partitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	log.LogFlush()
	fmt.Printf("writable data partitions[%v]\n", commonVol.dataPartitions.readableAndWritableCnt)
}

func TestVol(t *testing.T) {
	capacity := 200
	name := "test1"
	createVol(name, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	server.cluster.checkLoadMetaPartitions()
	server.cluster.doLoadDataPartitions()
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	vol.checkStatus(server.cluster)
	getVol(name, t)
	updateVol(name, capacity, t)
	statVol(name, t)
	markDeleteVol(name, t)
	getSimpleVol(name, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func createVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs&mpCount=2", hostAddr, proto.AdminCreateVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func getSimpleVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func getVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.ClientVol, name, buildAuthKey())
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func updateVol(name string, capacity int, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, proto.AdminUpdateVol, name, capacity, buildAuthKey())
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Capacity != uint64(capacity) {
		t.Errorf("update vol failed,expect[%v],real[%v]", capacity, vol.Capacity)
		return
	}
}

func statVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.ClientVolStat, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func markDeleteVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&authKey=%v",
		hostAddr, proto.AdminDeleteVol, name, buildAuthKey())
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Status != markDelete {
		t.Errorf("markDeleteVol failed,expect[%v],real[%v]", markDelete, vol.Status)
		return
	}
}
