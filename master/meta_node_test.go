package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"testing"
	"time"
)

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := mms7Addr
	addMetaServer(addr, testZone3)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	decommissionMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, addr)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func decommissionMetaNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionMetaNode, addr)
	fmt.Println(reqURL)
	process(reqURL, t)
}
