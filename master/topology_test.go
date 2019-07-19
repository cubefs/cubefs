package master

import (
	"github.com/chubaofs/chubaofs/util"
	"time"
	"testing"
	"fmt"
)

func createDataNodeForTopo(addr, rackName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, "test")
	dn.RackName = rackName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.AvailableSpace = 1024 * util.GB
	dn.Carry = 0.9
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	return
}

func TestSingleRack(t *testing.T) {
	//rack name must be DefaultRackName
	dataNode, err := server.cluster.dataNode(mds1Addr)
	if err != nil {
		t.Error(err)
		return
	}
	if dataNode.RackName != DefaultRackName {
		t.Errorf("rack name should be [%v],but now it's [%v]", DefaultRackName, dataNode.RackName)
	}
	topo := newTopology()
	nodeSet := newNodeSet(1, 6)
	topo.putNodeSet(nodeSet)
	rackName := "test"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, rackName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, rackName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, rackName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, rackName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, rackName, nodeSet))
	if !nodeSet.isSingleRack() {
		racks := nodeSet.getAllRacks()
		t.Errorf("topo should be single rack,rack num [%v]", len(racks))
		return
	}
	replicaNum := 2
	//single rack exclude,if it is a single rack excludeRacks don't take effect
	excludeRacks := make([]string, 0)
	excludeRacks = append(excludeRacks, rackName)
	racks, err := nodeSet.allocRacks(replicaNum, excludeRacks)
	if err != nil {
		t.Error(err)
		return
	}
	if len(racks) != 1 {
		t.Errorf("expect rack num [%v],len(racks) is %v", 0, len(racks))
		fmt.Println(racks)
		return
	}

	//single rack normal
	racks, err = nodeSet.allocRacks(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, _, err := racks[0].getAvailDataNodeHosts(nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)
	racks[0].removeDataNode(mds1Addr)
	nodeSet.removeRack(rackName)
}

func TestAllocRacks(t *testing.T) {
	topo := newTopology()
	nodeSet := newNodeSet(1, 6)
	topo.putNodeSet(nodeSet)
	rackCount := 3
	//add three racks
	rackName1 := "rack1"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, rackName1, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, rackName1, nodeSet))
	rackName2 := "rack2"
	topo.putDataNode(createDataNodeForTopo(mds3Addr, rackName2, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, rackName2, nodeSet))
	rackName3 := "rack3"
	topo.putDataNode(createDataNodeForTopo(mds5Addr, rackName3, nodeSet))
	nodeSet.dataNodeLen = 5
	racks := nodeSet.getAllRacks()
	if len(racks) != rackCount {
		t.Errorf("expect racks num[%v],len(racks) is %v", rackCount, len(racks))
		return
	}
	//only pass replica num
	replicaNum := 2
	racks, err := nodeSet.allocRacks(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if len(racks) != replicaNum {
		t.Errorf("expect racks num[%v],len(racks) is %v", replicaNum, len(racks))
		return
	}
	cluster := new(Cluster)
	cluster.t = topo
	hosts, _, err := cluster.chooseTargetDataNodes(replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("ChooseTargetDataHosts in multi racks,hosts[%v]", hosts)
	//test exclude rack
	excludeRacks := make([]string, 0)
	excludeRacks = append(excludeRacks, rackName1)
	racks, err = nodeSet.allocRacks(replicaNum, excludeRacks)
	if err != nil {
		t.Error(err)
		return
	}
	for _, rack := range racks {
		if rack.name == rackName1 {
			t.Errorf("rack [%v] should be exclued", rackName1)
			return
		}
	}
	nodeSet.removeRack(rackName1)
	nodeSet.removeRack(rackName2)
	nodeSet.removeRack(rackName3)
}
