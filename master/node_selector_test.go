package master

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/unit"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func newMockMetaNode(addr, zoneName, clusterID string, version string) *MetaNode {
	return &MetaNode{
		Addr:                   addr,
		ZoneName:               zoneName,
		Sender:                 newAdminTaskManager(addr, zoneName, clusterID),
		Carry:                  rand.Float64(),
		RocksdbHostSelectCarry: rand.Float64(),
		Version:                version,
		Threshold:              0.75,
		RocksdbDiskThreshold:   0.6,
		ReportTime:             time.Now(),
		IsActive:               true,
		ToBeMigrated:           false,
		ToBeOffline:            false,
	}
}

func mockMetaNodes() (nodes *sync.Map) {
	nodes = new(sync.Map)
	metaNode := newMockMetaNode("192.168.0.1", "test", "test", "3.0.0")
	metaNode.ID = 1
	metaNode.MaxMemAvailWeight = 64 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 64 * unit.GB
	metaNode.Ratio = 0.5
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 1 * unit.TB, Used: 12 * unit.GB, Path: "/data0", UsageRatio: float64(12) / float64(1024)},
		{Total: 1 * unit.TB, Used: 0, Path: "/data1", UsageRatio: 0},
		{Total: 1 * unit.TB, Used: 120 * unit.GB, Path: "/data2", UsageRatio: float64(120) / float64(1024)},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.2", "test", "test", "3.0.0")
	metaNode.ID = 2
	metaNode.MaxMemAvailWeight = 128 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 0
	metaNode.Ratio = 0
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 500 * unit.GB, Used: 12 * unit.GB, Path: "/data0", UsageRatio: float64(12) / float64(500)},
		{Total: 500 * unit.GB, Used: 120 * unit.GB, Path: "/data1", UsageRatio: float64(120) / float64(500)},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.3", "test", "test", "3.0.0")
	metaNode.ID = 3
	metaNode.MaxMemAvailWeight = 126 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 2 * unit.GB
	metaNode.Ratio = 0.016
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 500 * unit.GB, Used: 300 * unit.GB, Path: "/data0", UsageRatio: 0.6},
		{Total: 500 * unit.GB, Used: 0, Path: "/data1", UsageRatio: 0},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.4", "test", "test", "3.0.0")
	metaNode.ID = 4
	metaNode.MaxMemAvailWeight = 16 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 108 * unit.GB
	metaNode.Ratio = 0.843
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 4 * unit.TB, Used: 1 * unit.TB, Path: "/data0", UsageRatio: 0.25},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.5", "test", "test", "3.0.0")
	metaNode.ID = 5
	metaNode.MaxMemAvailWeight = 118 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 10 * unit.GB
	metaNode.Ratio = 0.078
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 4 * unit.TB, Used: 3 * unit.TB, Path: "/data0", UsageRatio: 0.75},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.6", "test", "test", "3.0.0")
	metaNode.ID = 6
	metaNode.MaxMemAvailWeight = 40 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 88 * unit.GB
	metaNode.Ratio = 0.688
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 4 * unit.TB, Used: 120 * unit.GB, Path: "/data0", UsageRatio: float64(120) / float64(1024*4)},
	}
	nodes.Store(metaNode.Addr, metaNode)

	metaNode = newMockMetaNode("192.168.0.7", "test", "test", "3.0.0")
	metaNode.ID = 7
	metaNode.MaxMemAvailWeight = 40 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 88 * unit.GB
	metaNode.Ratio = 0.688
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 4 * unit.TB, Used: 120 * unit.GB, Path: "/data0", UsageRatio: float64(120) / float64(1024*4)},
	}
	nodes.Store(metaNode.Addr, metaNode)

	return
}

func TestMetaNode_SelectNodeCase01(t *testing.T) {
	gConfig = newClusterConfig()
	ns := newNodeSet(1, 7, "test")
	ns.metaNodes = mockMetaNodes()
	newHosts, _, err := ns.getAvailMetaNodeHosts([]string{"192.168.0.7"}, 1, proto.StoreModeRocksDb)
	if err != nil {
		t.Logf("error:%v\n", err)
		t.FailNow()
	}
	for _, host := range newHosts {
		t.Logf("select host:%v\n", host)
		impossibleHosts := []string{"192.168.0.7", "192.168.0.4", "192.168.0.5"}
		if contains(impossibleHosts, host) {
			t.Errorf("impossible host:%s\n", host)
			t.FailNow()
		}
	}

	newHosts, _, err = ns.getAvailMetaNodeHosts([]string{"192.168.0.7"}, 1, proto.StoreModeRocksDb)
	if err != nil {
		t.Logf("error:%v\n", err)
		t.FailNow()
	}
	for _, host := range newHosts {
		t.Logf("select host:%v\n", host)
		impossibleHosts := []string{"192.168.0.7", "192.168.0.4", "192.168.0.5"}
		if contains(impossibleHosts, host) {
			t.Errorf("impossible host:%s\n", host)
			t.FailNow()
		}
	}

}

func TestMetaNode_SelectNodeCase02(t *testing.T) {
	gConfig = newClusterConfig()
	ns := newNodeSet(1, 1, "test")
	metaNode := newMockMetaNode("192.168.0.1", "test", "test", "3.0.0")
	metaNode.ID = 1
	metaNode.MaxMemAvailWeight = 20 * unit.GB
	metaNode.Total = 128 * unit.GB
	metaNode.Used = 108 * unit.GB
	metaNode.Ratio = 0.844
	metaNode.RocksdbDisks = []*proto.MetaNodeDiskInfo{
		{Total: 4 * unit.TB, Used: 2 * unit.TB, Path: "/data0", UsageRatio: 0.5},
	}
	ns.metaNodes.Store(metaNode.Addr, metaNode)

	_, _, err := ns.getAvailMetaNodeHosts([]string{"192.168.0.7"}, 1, proto.StoreModeRocksDb)
	if err == nil {
		t.Errorf("error mismatch, expect not nil, actual is nil\n")
		t.FailNow()
	}
}
