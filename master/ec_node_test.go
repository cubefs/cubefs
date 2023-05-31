package master

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func TestEC_EcNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	addr := "127.0.0.1:10108"
	addEcServer(addr, httpPort, DefaultZoneName)
	server.cluster.loadEcNodes()
	server.cluster.checkEcNodeHeartbeat()
	time.Sleep(5 * time.Second)
	allEcNodes := server.cluster.allEcNodes()
	assert.NotNilf(t, allEcNodes, "add ecNode [%v] failed", addr)

	getEcNodeInfo(addr, t)
	ecNode, err := server.cluster.ecNode(addr)
	assert.NoErrorf(t, err, "add ecNode [%v] failed", addr)

	setEcNodeProto(ecNode, t)
	getEcNodeProto(ecNode, t)
	modifyEcNodeProto(ecNode, t)
	decommissionDiskById(addr, t)
	decommissionEcNode(addr, t)
	ecNode, err = server.cluster.ecNode(addr)
	assert.Errorf(t, err, "decommission datanode [%v] failed", addr)

	server.cluster.ecNodes.Delete(addr)
}

func getEcNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetEcNode, addr)
	process(reqURL, t)
}

func setEcNodeProto(ecnode *ECNode, t *testing.T) {
	ecnode.SetCarry(1.0, proto.StoreModeDef)
	assert.Equal(t, 1.0, ecnode.Carry, "setCarry fail")
	server.cluster.adjustEcNode(ecnode)
}

func getEcNodeProto(ecnode *ECNode, t *testing.T) {
	ecnode.isWriteAble()
	ecnode.isAvailCarryNode()
	ecnode.GetID()
	ecnode.GetAddr()
	badParitions := ecnode.badPartitions("/cfs/disk", server.cluster)
	assert.NotNilf(t, badParitions, "add ecNode [%v] failed", ecnode.Addr)
}

func modifyEcNodeProto(ecnode *ECNode, t *testing.T) {
	oldCarry := ecnode.Carry
	ecnode.SelectNodeForWrite(proto.StoreModeDef)
	newCarry := ecnode.Carry
	assert.Equal(t, float64(1), oldCarry-newCarry, "select node fail")
}

func decommissionDiskById(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v&auto=true",
		hostAddr, proto.DecommissionEcDisk, addr, 1)
	process(reqURL, t)
}

func decommissionEcNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionEcNode, addr)
	process(reqURL, t)
}
