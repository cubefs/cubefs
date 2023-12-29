package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := mms7Addr
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockMetaServers = append(mockMetaServers, addMetaServer(addr, testZone3))
	}()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	decommissionMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, addr)
	process(reqURL, t)
}

func decommissionMetaNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionMetaNode, addr)
	process(reqURL, t)
}
