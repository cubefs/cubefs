package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"strings"
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

func getZoneNodeSetStatus(zone *Zone) {
	zone.nsLock.RLock()
	defer zone.nsLock.RUnlock()
	dataNodeSb := new(strings.Builder)
	metaNodeSb := new(strings.Builder)
	dataNodeSb.WriteString(fmt.Sprintf("zone:%v  datanodeCount info ", zone.name))
	metaNodeSb.WriteString(fmt.Sprintf("zone:%v  metanodeCount info ", zone.name))
	for _, ns := range zone.nodeSetMap {
		dataNodeSb.WriteString(fmt.Sprintf(" ns:%v:%v ", ns.ID, ns.dataNodeLen()))
		metaNodeSb.WriteString(fmt.Sprintf(" ns:%v:%v ", ns.ID, ns.metaNodeLen()))
	}
	fmt.Println(dataNodeSb.String())
	fmt.Println(metaNodeSb.String())
}
func batchCreateDataNodeForNodeSet(topo *topology, ns *nodeSet, zoneName, clusterID, baseAddr string, count int) {
	if count > ns.Capacity-ns.dataNodeCount() {
		count = ns.Capacity - ns.dataNodeCount()
	}
	for i := 0; i < count; i++ {
		topo.putDataNode(createDataNodeForNodeSet(fmt.Sprintf("%v%v", baseAddr, i), zoneName, clusterID, ns))
	}
}
func batchCreateMetaNodeForNodeSet(topo *topology, ns *nodeSet, zoneName, clusterID, baseAddr string, count int) {
	if count > ns.Capacity-ns.metaNodeCount() {
		count = ns.Capacity - ns.metaNodeCount()
	}
	for i := 0; i < count; i++ {
		topo.putMetaNode(createMetaNodeForNodeSet(fmt.Sprintf("%v%v", baseAddr, i), zoneName, clusterID, ns))
	}
}
func createDataNodeForNodeSet(addr, zoneName, clusterID string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, zoneName, clusterID)
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

func createMetaNodeForNodeSet(addr, zoneName, clusterID string, ns *nodeSet) (mn *MetaNode) {
	mn = newMetaNode(addr, zoneName, clusterID)
	mn.ZoneName = zoneName
	mn.Total = 1024 * util.GB
	mn.Used = 10 * util.GB
	mn.Carry = 0.9
	mn.ReportTime = time.Now()
	mn.IsActive = true
	mn.NodeSetID = ns.ID
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
	zones, err := topo.allocZonesForDataNode("", zoneName, replicaNum, excludeZones, false)
	if err != nil {
		t.Error(err)
		return
	}
	if len(zones) != 1 {
		t.Errorf("expect zone num [%v],len(zones) is %v", 0, len(zones))
		return
	}

	//single zone normal
	zones, err = topo.allocZonesForDataNode("", zoneName, replicaNum, nil, false)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, _, err := zones[0].getAvailDataNodeHosts(nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)

	// single zone with exclude hosts
	excludeHosts := []string{mds1Addr, mds2Addr, mds3Addr}
	newHosts, _, err = zones[0].getAvailDataNodeHosts(nil, excludeHosts, replicaNum)
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
	replicaNum := 2
	zones, err := topo.allocZonesForDataNode("", "zone1,zone2", replicaNum, nil, false)
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
	cluster.cfg.DataPartitionsRecoverPoolSize = maxDataPartitionsRecoverPoolSize
	cluster.cfg.MetaPartitionsRecoverPoolSize = maxMetaPartitionsRecoverPoolSize

	//don't cross zone
	hosts, _, err := cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum, "zone1", false)
	if err != nil {
		t.Error(err)
		return
	}
	//cross zone
	hosts, _, err = cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum, "zone1,zone2,zone3", false)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("ChooseTargetDataHosts in multi zones,hosts[%v]", hosts)
	// after excluding zone3, alloc zones will be success
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName3)
	zones, err = topo.allocZonesForDataNode("", zoneName3, replicaNum, excludeZones, false)
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
