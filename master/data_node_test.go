package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDataNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	addr := "127.0.0.1:9096"
	addDataServer(addr, DefaultZoneName)
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getDataNodeInfo(addr, t)
	decommissionDataNode(addr, t)
	_, err := server.cluster.dataNode(addr)
	assert.Errorf(t, err, "decommission datanode [%v] failed", addr)
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
