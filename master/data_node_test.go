package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func TestDataNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	var err error
	addr := "127.0.0.1:9096"
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockDataServers = append(mockDataServers, addDataServer(addr, DefaultZoneName))
	}()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getDataNodeInfo(addr, t)
	decommissionDataNode(addr, t)
	for i := 0; i < 10; i++ { // decommission is async process
		_, err = server.cluster.dataNode(addr)
		if err == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		t.Errorf("decommission datanode [%v] failed", addr)
	}
	server.cluster.dataNodes.Delete(addr)
}

func getDataNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetDataNode, addr)
	process(reqURL, t)
}

func decommissionDataNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionDataNode, addr)
	process(reqURL, t)
}
