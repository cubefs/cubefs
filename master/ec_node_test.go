package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func TestEC_EcNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	addr := "127.0.0.1:10108"
	addEcServer(addr, httpPort, DefaultZoneName)
	server.cluster.loadEcNodes()
	server.cluster.checkEcNodeHeartbeat()
	time.Sleep(5 * time.Second)
	if allEcNodes := server.cluster.allEcNodes(); allEcNodes == nil {
		t.Errorf("add ecNode [%v] failed", addr)
	}

	getEcNodeInfo(addr, t)
	ecNode, err := server.cluster.ecNode(addr)
	if err != nil {
		t.Errorf("add ecNode [%v] failed", addr)
	}

	setEcNodeProto(ecNode, t)
	getEcNodeProto(ecNode, t)
	modifyEcNodeProto(ecNode, t)
	decommissionDiskById(addr, t)
	decommissionEcNode(addr, t)
	ecNode, err = server.cluster.ecNode(addr)
	if err == nil {
		t.Errorf("decommission datanode [%v] failed", addr)
	}

	server.cluster.ecNodes.Delete(addr)
}

func getEcNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetEcNode, addr)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func setEcNodeProto(ecnode *ECNode, t *testing.T) {
	ecnode.SetCarry(1.0)
	if ecnode.Carry != 1.0 {
		t.Errorf("setCarry fail")
	}
	server.cluster.adjustEcNode(ecnode)
}

func getEcNodeProto(ecnode *ECNode, t *testing.T) {
	ecnode.isWriteAble()
	ecnode.isAvailCarryNode()
	ecnode.GetID()
	ecnode.GetAddr()
	if badParitions := ecnode.badPartitions("/cfs/disk", server.cluster); badParitions == nil {
		t.Errorf("add ecNode [%v] failed", ecnode.Addr)
	}
}

func modifyEcNodeProto(ecnode *ECNode, t *testing.T) {
	oldCarry := ecnode.Carry
	ecnode.SelectNodeForWrite()
	if newCarry := ecnode.Carry; oldCarry-newCarry != 1 {
		t.Errorf("select node fail")
	}
}

func decommissionDiskById(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v&disk=%v&auto=true",
		hostAddr, proto.DecommissionEcDisk, addr, 1)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func decommissionEcNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionEcNode, addr)
	fmt.Println(reqURL)
	process(reqURL, t)
}
