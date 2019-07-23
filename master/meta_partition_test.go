package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"testing"
	"time"
)

func TestMetaPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaPartitions()
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	createMetaPartition(commonVol, t)
	maxPartitionID := commonVol.maxPartitionID()
	getMetaPartition(commonVol.Name, maxPartitionID, t)
	loadMetaPartitionTest(commonVol, maxPartitionID, t)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	decommissionMetaPartition(commonVol, maxPartitionID, t)
}

func createMetaPartition(vol *Vol, t *testing.T) {
	maxPartitionID := commonVol.maxPartitionID()
	mp, err := commonVol.metaPartition(maxPartitionID)
	if err != nil {
		t.Error(err)
		return
	}
	var start uint64
	start = mp.Start + defaultMetaPartitionInodeIDStep
	reqUrl := fmt.Sprintf("%v%v?name=%v&start=%v",
		hostAddr, proto.AdminCreateMP, vol.Name, start)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err = server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID = vol.maxPartitionID()
	mp, err = vol.metaPartition(maxPartitionID)
	if err != nil {
		t.Errorf("createMetaPartition,err [%v]", err)
		return
	}
	start = start + 1
	if mp.Start != start {
		t.Errorf("expect start[%v],mp.start[%v],not equal", start, mp.Start)
		return
	}
}

func getMetaPartition(volName string, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v",
		hostAddr, proto.ClientMetaPartition, volName, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func loadMetaPartitionTest(vol *Vol, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v", hostAddr, proto.AdminLoadMetaPartition, vol.Name, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func decommissionMetaPartition(vol *Vol, id uint64, t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	reqUrl := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	mp, err := vol.metaPartition(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	offlineAddr := mp.Hosts[0]
	reqUrl = fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionMetaPartition, vol.Name, id, offlineAddr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	mp, err = server.cluster.getMetaPartitionByID(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	if contains(mp.Hosts, offlineAddr) {
		t.Errorf("decommissionMetaPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, mp.Hosts)
		return
	}
}
