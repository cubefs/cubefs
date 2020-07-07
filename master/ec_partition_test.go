package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func TestEcPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	server.cluster.checkEcNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkEcDataPartitions()
	createEcPartition(commonVol, t)
	if len(commonVol.ecDataPartitions.partitions) <= 0 {
		t.Errorf("getEcDataPartition no ecdp")
		return
	}
	partition := commonVol.ecDataPartitions.partitions[0]
	getEcPartition(partition.PartitionID, t)
}

func createEcPartition(vol *Vol, t *testing.T) {
	oldCount := len(vol.dataPartitions.partitions)
	reqURL := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.CreateEcDataPartition, vol.Name)
	fmt.Println(reqURL)
	process(reqURL, t)
	newCount := len(vol.dataPartitions.partitions)
	if newCount != oldCount + 1 {
		t.Errorf("createEcPartition failed,newCount[%v], oldCount[%v]",
			newCount, oldCount)
		return
	}
}

func getEcPartition(id uint64, t *testing.T) {

	reqURL := fmt.Sprintf("%v%v?id=%v",
		hostAddr, proto.AdminGetEcPartition, id)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func decommissionEcPartition(ecdp *EcDataPartition, t *testing.T) {
	offlineAddr := ecdp.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionEcPartition, ecdp.VolName, ecdp.PartitionID, offlineAddr)
	fmt.Println(reqURL)
	process(reqURL, t)
	if contains(ecdp.Hosts, offlineAddr) {
		t.Errorf("decommissionEcPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, ecdp.Hosts)
		return
	}
}
