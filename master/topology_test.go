package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func createDataNodeForTopo(addr, httpPort, zoneName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, httpPort, zoneName, "test", "1.0.0")
	dn.ZoneName = zoneName
	dn.Total = 1024 * unit.GB
	dn.Used = 10 * unit.GB
	dn.AvailableSpace = 1024 * unit.GB
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
}
func batchCreateDataNodeForNodeSet(topo *topology, ns *nodeSet, zoneName, clusterID, baseAddr string, count int) {
	if count > ns.Capacity-ns.dataNodeCount() {
		count = ns.Capacity - ns.dataNodeCount()
	}
	for i := 0; i < count; i++ {
		topo.putDataNode(createDataNodeForNodeSet(fmt.Sprintf("%v%v", baseAddr, i), httpPort, zoneName, clusterID, ns))
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
func createDataNodeForNodeSet(addr, httpPort, zoneName, clusterID string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, httpPort, zoneName, clusterID, "1.0.0")
	dn.ZoneName = zoneName
	dn.Total = 1024 * unit.GB
	dn.Used = 10 * unit.GB
	dn.AvailableSpace = 1024 * unit.GB
	dn.Carry = 0.9
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	return
}

func createMetaNodeForNodeSet(addr, zoneName, clusterID string, ns *nodeSet) (mn *MetaNode) {
	mn = newMetaNode(addr, zoneName, clusterID, "1.0.0")
	mn.ZoneName = zoneName
	mn.Total = 1024 * unit.GB
	mn.Used = 10 * unit.GB
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
	topo.putDataNode(createDataNodeForTopo(mds1Addr, httpPort, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, httpPort, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, httpPort, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, httpPort, zoneName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, httpPort, zoneName, nodeSet))
	if !assert.Truef(t, topo.isSingleZone(), "topo should be single zone,zone num [%v]", len(topo.getAllZones())) {
		return
	}
	replicaNum := 2
	//single zone exclude,if it is a single zone excludeZones don't take effect
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName)
	zones, err := topo.allocZonesForDataNode("", zoneName, replicaNum, excludeZones, false)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Lenf(t, zones, 1, "expect zone num [%v],len(zones) is %v", 0, len(zones)) {
		return
	}

	//single zone normal
	zones, err = topo.allocZonesForDataNode("", zoneName, replicaNum, nil, false)
	if !assert.NoError(t, err) {
		return
	}
	_, _, err = zones[0].getAvailDataNodeHosts(nil, nil, replicaNum)
	if !assert.NoError(t, err) {
		return
	}

	// single zone with exclude hosts
	excludeHosts := []string{mds1Addr, mds2Addr, mds3Addr}
	newHosts, _, err := zones[0].getAvailDataNodeHosts(nil, excludeHosts, replicaNum)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotContains(t, newHosts, excludeHosts)
	topo.deleteDataNode(createDataNodeForTopo(mds1Addr, httpPort, zoneName, nodeSet))
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
	topo.putDataNode(createDataNodeForTopo(mds1Addr, httpPort, zoneName1, nodeSet1))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, httpPort, zoneName1, nodeSet1))
	zoneName2 := "zone2"
	zone2 := newZone(zoneName2)
	nodeSet2 := newNodeSet(2, 6, zoneName2)
	zone2.putNodeSet(nodeSet2)
	topo.putZone(zone2)
	topo.putDataNode(createDataNodeForTopo(mds3Addr, httpPort, zoneName2, nodeSet2))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, httpPort, zoneName2, nodeSet2))
	zoneName3 := "zone3"
	zone3 := newZone(zoneName3)
	nodeSet3 := newNodeSet(3, 6, zoneName3)
	zone3.putNodeSet(nodeSet3)
	topo.putZone(zone3)
	topo.putDataNode(createDataNodeForTopo(mds5Addr, httpPort, zoneName3, nodeSet3))
	zones := topo.getAllZones()
	if !assert.Lenf(t, zones, zoneCount, "expect zones num[%v],len(zones) is %v", zoneCount, len(zones)) {
		return
	}
	replicaNum := 2
	zones, err := topo.allocZonesForDataNode("", "zone1,zone2", replicaNum, nil, false)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Lenf(t, zones, replicaNum, "expect zones num[%v],len(zones) is %v", replicaNum, len(zones)) {
		return
	}
	cluster := new(Cluster)
	cluster.t = topo
	cluster.cfg = newClusterConfig()
	cluster.cfg.DataPartitionsRecoverPoolSize = maxDataPartitionsRecoverPoolSize
	cluster.cfg.MetaPartitionsRecoverPoolSize = maxMetaPartitionsRecoverPoolSize

	//don't cross zone
	_, _, err = cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum, "zone1", false)
	if !assert.NoError(t, err) {
		return
	}
	//cross zone
	_, _, err = cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum, "zone1,zone2,zone3", false)
	if !assert.NoError(t, err) {
		return
	}
	// after excluding zone3, alloc zones will be success
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName3)
	zones, err = topo.allocZonesForDataNode("", zoneName3, replicaNum, excludeZones, false)
	assert.NoErrorf(t, err, "allocZonesForDataNode failed,err[%v]", err)
	for _, zone := range zones {
		if !assert.NotEqualf(t, zone.name, zoneName3, "zone [%v] should be exclued", zoneName3) {
			return
		}
	}
}

func TestIDC(t *testing.T) {
	topo := newTopology()
	idc1Name := "TestIDC1"
	idc2Name := "TestIDC2"
	defer log.LogFlush()
	idc1 := newIDC(idc1Name)
	topo.putIDC(idc1)
	idc2 := newIDC(idc2Name)
	topo.putIDC(idc2)
	idc, err := topo.getIDC(idc1Name)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, idc1Name, idc.Name) {
		t.FailNow()
	}

	_, err = server.cluster.t.createIDC(idc1Name, server.cluster)
	assertErrNilOtherwiseFailNow(t, err)
	err = server.cluster.t.deleteIDC(idc1Name, server.cluster)
	assertErrNilOtherwiseFailNow(t, err)
	idc, err = server.cluster.t.getIDC(idc1Name)
	if err == nil {
		t.Errorf("idc: %v has been deleted, %v.", idc1Name, idc.Name)
		t.FailNow()
	}
	view, err := server.cluster.t.getIDCView(idc1Name)
	if err == nil {
		t.Errorf("idc: %v has been deleted, %v.", idc1Name, idc.Name)
		t.FailNow()
	}

	_, err = server.cluster.t.createIDC(idc1Name, server.cluster)
	assertErrNilOtherwiseFailNow(t, err)
	_, err = server.cluster.t.createIDC(idc2Name, server.cluster)
	assertErrNilOtherwiseFailNow(t, err)

	view, err = server.cluster.t.getIDCView(idc1Name)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, idc1Name, view.Name) {
		t.FailNow()
	}
	if !assert.Zero(t, len(view.Zones)) {
		t.FailNow()
	}

	// set thd idc info, idc: nil->idc1, MediumInit->HDD
	err = server.cluster.setZoneIDC(testZone1, idc1Name, proto.MediumHDD)
	assertErrNilOtherwiseFailNow(t, err)
	zone1, err := server.cluster.t.getZone(testZone1)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumHDD, zone1.MType) {
		t.FailNow()
	}
	if !assert.Equal(t, idc1Name, zone1.idcName) {
		t.FailNow()
	}

	view, err = server.cluster.t.getIDCView(idc1Name)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, idc1Name, view.Name) {
		t.FailNow()
	}
	if !assert.NotZero(t, len(view.Zones)) {
		t.FailNow()
	}
	for name, mType := range view.Zones {
		if !assert.Equal(t, testZone1, name) {
			t.FailNow()
		}
		if !assert.Equal(t, proto.MediumHDDName, mType) {
			t.FailNow()
		}
	}

	views := server.cluster.t.getIDCViews()
	if !assert.NotNil(t, views) {
		t.FailNow()
	}
	for _, view := range views {
		if views[0].Name != idc1Name {
			continue
		}
		for name, mType := range view.Zones {
			if !assert.Equal(t, testZone1, name) {
				t.FailNow()
			}
			if !assert.Equal(t, proto.MediumHDDName, mType) {
				t.FailNow()
			}
		}
	}
	if !assert.NotZero(t, len(views)) {
		t.FailNow()
	}
	// set thd idc info, HDD->SDD
	err = server.cluster.setZoneIDC(testZone1, idc1Name, proto.MediumSSD)
	assertErrNilOtherwiseFailNow(t, err)
	zone1, err = server.cluster.t.getZone(testZone1)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumSSD, zone1.MType) {
		t.FailNow()
	}
	if !assert.Equal(t, idc1Name, zone1.idcName) {
		t.FailNow()
	}

	// set thd idc info, idc1->idc2
	err = server.cluster.setZoneIDC(testZone1, idc2Name, proto.MediumInit)
	assertErrNilOtherwiseFailNow(t, err)
	zone1, err = server.cluster.t.getZone(testZone1)
	assertErrNilOtherwiseFailNow(t, err)
	if !assert.Equal(t, proto.MediumSSD, zone1.MType) {
		t.FailNow()
	}
	if !assert.Equal(t, idc2Name, zone1.idcName) {
		t.FailNow()
	}

	err = server.cluster.t.deleteIDC(idc2Name, server.cluster)
	if !assert.Errorf(t, err, "idc: %v should not be deleted", idc2Name) {
		t.FailNow()
	}
	_, err = server.cluster.t.getIDC(idc2Name)
	assertErrNilOtherwiseFailNow(t, err)

	err = server.cluster.t.deleteIDC(idc1Name, server.cluster)
	assertErrNilOtherwiseFailNow(t, err)
	_, err = server.cluster.t.getIDC(idc1Name)
	if !assert.Error(t, err) {
		t.FailNow()
	}
}
