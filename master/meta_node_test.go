package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"testing"
	"time"
)

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := mms6Addr
	addMetaServer(addr)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	decommissionMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func decommissionMetaNode(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionMetaNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}
