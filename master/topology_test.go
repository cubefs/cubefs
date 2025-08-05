package master

import (
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func createDataNodeForTopo(addr, zoneName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, strconv.Itoa(raftstore.DefaultHeartbeatPort), strconv.Itoa(raftstore.DefaultReplicaPort), zoneName, "test", proto.MediaType_HDD)
	dn.ZoneName = zoneName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.AvailableSpace = 1024 * util.GB
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	dn.AllDisks = []string{"/cfs/disk"}
	dn.DpCntLimit = defaultMaxDpCntLimit
	return
}

func TestSingleZone(t *testing.T) {
	topo := newTopology()
	zoneName := "test"
	zone := newZone(zoneName, proto.MediaType_Unspecified)
	topo.putZone(zone)
	c := new(Cluster)
	nodeSet := newNodeSet(c, 1, 6, zoneName)
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
	// single zone exclude,if it is a single zone excludeZones don't take effect
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName)
	zones, err := topo.allocZonesForNode(&topo.metaTopology, replicaNum, replicaNum, excludeZones, []*Zone{}, proto.MediaType_Unspecified)
	require.Error(t, err)
	require.EqualValues(t, 0, len(zones))

	// single zone normal
	zones, err = topo.allocZonesForNode(&topo.dataTopology, replicaNum, replicaNum, nil, []*Zone{}, proto.MediaType_Unspecified)
	require.NoError(t, err)
	newHosts, _, err := zones[0].getAvailNodeHosts(TypeDataPartition, nil, nil, replicaNum)
	require.NoError(t, err)
	t.Log(newHosts)
	topo.deleteDataNode(createDataNodeForTopo(mds1Addr, zoneName, nodeSet))
}

func TestAllocZones(t *testing.T) {
	topo := newTopology()
	c := new(Cluster)
	zoneCount := 3

	hostZoneMap := make(map[string]string)
	hostZoneMap[mds1Addr] = testZone1
	hostZoneMap[mds2Addr] = testZone1
	hostZoneMap[mds3Addr] = testZone2
	hostZoneMap[mds4Addr] = testZone2
	hostZoneMap[mds5Addr] = testZone3

	zoneMap := make(map[string]bool)
	zoneMap[testZone1] = false
	zoneMap[testZone2] = false
	zoneMap[testZone3] = false

	getZoneCntFunc := func(hosts []string) int {
		for _, host := range hosts {
			zoneNm := hostZoneMap[host]
			zoneMap[zoneNm] = true
		}
		var zoneCnt int
		for _, v := range zoneMap {
			if v {
				zoneCnt++
			}
		}
		for k := range zoneMap {
			zoneMap[k] = false
		}
		return zoneCnt
	}

	// add three zones
	zoneName1 := testZone1
	zone1 := newZone(zoneName1, proto.MediaType_Unspecified)
	nodeSet1 := newNodeSet(c, 1, 6, zoneName1)

	zone1.putNodeSet(nodeSet1)
	topo.putZone(zone1)
	topo.putDataNode(createDataNodeForTopo(mds1Addr, zoneName1, nodeSet1))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, zoneName1, nodeSet1))

	zoneName2 := testZone2
	zone2 := newZone(zoneName2, proto.MediaType_Unspecified)
	nodeSet2 := newNodeSet(c, 2, 6, zoneName2)

	zone2.putNodeSet(nodeSet2)
	topo.putZone(zone2)
	topo.putDataNode(createDataNodeForTopo(mds3Addr, zoneName2, nodeSet2))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, zoneName2, nodeSet2))

	zoneName3 := "zone3"
	zone3 := newZone(zoneName3, proto.MediaType_Unspecified)
	nodeSet3 := newNodeSet(c, 3, 6, zoneName3)

	zone3.putNodeSet(nodeSet3)
	topo.putZone(zone3)
	topo.putDataNode(createDataNodeForTopo(mds5Addr, zoneName3, nodeSet3))

	zones := topo.getAllZones()
	require.EqualValues(t, zoneCount, len(zones))
	// only pass replica num
	replicaNum := 2
	zones, err := topo.allocZonesForNode(&topo.dataTopology, replicaNum, replicaNum, nil, []*Zone{}, proto.MediaType_Unspecified)
	require.NoError(t, err)
	require.EqualValues(t, 2, len(zones))

	cluster := new(Cluster)
	cluster.t = topo
	cluster.cfg = newClusterConfig()

	// don't cross zone
	hosts, _, err := cluster.getHostFromNormalZone(TypeDataPartition, nil, nil, nil, replicaNum, 1, "", proto.MediaType_Unspecified)
	require.NoError(t, err)

	t.Logf("ChooseTargetDataHosts in single zone,hosts[%v]", hosts)

	// cross zone
	_, _, err = cluster.getHostFromNormalZone(TypeDataPartition, nil, nil, nil, replicaNum, 2, "", proto.MediaType_Unspecified)
	require.NoError(t, err)

	// specific zone
	hosts, _, err = cluster.getHostFromNormalZone(TypeDataPartition, nil, nil, nil, 3, 2, zoneName1+","+zoneName2, proto.MediaType_Unspecified)
	require.NoError(t, err)
	require.EqualValues(t, getZoneCntFunc(hosts), 2)

	t.Logf("ChooseTargetDataHosts in multi zones,hosts[%v]", hosts)
	// after excluding zone3, alloc zones will be success
	excludeZones := make([]string, 0)
	excludeZones = append(excludeZones, zoneName3)

	zones, err = topo.allocZonesForNode(&topo.dataTopology, 2, replicaNum, excludeZones, []*Zone{}, proto.MediaType_Unspecified)
	if err != nil {
		t.Logf("allocZonesForNode(data) failed,err[%v]", err)
	}

	for _, zone := range zones {
		if zone.name == zoneName3 {
			t.Errorf("zone [%v] should be exclued", zoneName3)
			return
		}
	}
}
