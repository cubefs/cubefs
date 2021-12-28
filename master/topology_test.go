package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"testing"
	"time"
)

func createDataNodeForTopo(addr, zoneName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, zoneName, "test")
	dn.ZoneName = zoneName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.AvailableSpace = 1024 * util.GB
	dn.Carry = 0.9
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	return
}

func TestSingleZone(t *testing.T) {
	topo := newTopology()
	zoneName := "test"
	zone := newZone(zoneName)
	topo.putZone(zone)
	nodeSet := newNodeSet(1, 6, zoneName)
	zone.putNodeSet(nodeSet)
	topo.putDataNode(createDataNodeForTopo(mds1Addr, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, zoneName, nodeSet))
	if !topo.isSingleZone() {
		zones := topo.getAllZones()
		t.Errorf("topo should be single zone,zone num [%v]", len(zones))
		return
	}
	replicaNum := 2
	//single zone exclude,if it is a single zone excludeZones don't take effect
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName)
	zones, err := topo.allocZonesForDataNode(replicaNum, replicaNum, excludeZones)
	if err != nil {
		t.Error(err)
		return
	}
	if len(zones) != 1 {
		t.Errorf("expect zone num [%v],len(zones) is %v", 0, len(zones))
		return
	}

	//single zone normal
	zones, err = topo.allocZonesForDataNode(replicaNum, replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, _, err := zones[0].getAvailNodeHosts(TypeDataPartition, nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)
	topo.deleteDataNode(createDataNodeForTopo(mds1Addr, zoneName, nodeSet))
}

func TestAllocZones(t *testing.T) {
	topo := newTopology()

	zoneCount := 3
	//add three zones
	zoneName1 := "zone1"
	zone1 := newZone(zoneName1)
	nodeSet1 := newNodeSet(1, 6, zoneName1)
	zone1.putNodeSet(nodeSet1)
	topo.putZone(zone1)
	topo.putDataNode(createDataNodeForTopo(mds1Addr, zoneName1, nodeSet1))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, zoneName1, nodeSet1))

	zoneName2 := "zone2"
	zone2 := newZone(zoneName2)
	nodeSet2 := newNodeSet(2, 6, zoneName2)
	zone2.putNodeSet(nodeSet2)
	topo.putZone(zone2)
	topo.putDataNode(createDataNodeForTopo(mds3Addr, zoneName2, nodeSet2))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, zoneName2, nodeSet2))

	zoneName3 := "zone3"
	zone3 := newZone(zoneName3)
	nodeSet3 := newNodeSet(3, 6, zoneName3)
	zone3.putNodeSet(nodeSet3)
	topo.putZone(zone3)
	topo.putDataNode(createDataNodeForTopo(mds5Addr, zoneName3, nodeSet3))

	zones := topo.getAllZones()
	if len(zones) != zoneCount {
		t.Errorf("expect zones num[%v],len(zones) is %v", zoneCount, len(zones))
		return
	}
	//only pass replica num
	replicaNum := 2
	zones, err := topo.allocZonesForDataNode(replicaNum, replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}

	if len(zones) != replicaNum {
		t.Errorf("expect zones num[%v],len(zones) is %v", replicaNum, len(zones))
		return
	}

	cluster := new(Cluster)
	cluster.t = topo
	cluster.cfg = newClusterConfig()

	//don't cross zone
	hosts, _, err := cluster.getHostFromNormalZone(TypeDataPartition, nil, nil, nil, replicaNum, 1, "")
	if err != nil {
		t.Error(err)
		return
	}

	//cross zone
	hosts, _, err = cluster.getHostFromNormalZone(TypeDataPartition, nil, nil, nil, replicaNum, 2, "")
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("ChooseTargetDataHosts in multi zones,hosts[%v]", hosts)
	// after excluding zone3, alloc zones will be success
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName3)

	zones, err = topo.allocZonesForDataNode(2, replicaNum, excludeZones)
	if err != nil {
		t.Logf("allocZonesForDataNode failed,err[%v]", err)
	}

	for _, zone := range zones {
		if zone.name == zoneName3 {
			t.Errorf("zone [%v] should be exclued", zoneName3)
			return
		}
	}
}
