package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_CodecNode(t *testing.T) {
	addr := "127.0.0.1:10200"
	addCodecServer(addr, httpPort, DefaultZoneName)
	server.cluster.checkCodecNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getCodecNodeInfo(addr, t)
	getAllCodecNodeInfo(addr, t)
	decommissionCodecNode(addr, t)
	_, err := server.cluster.codecNode(addr)
	assert.Errorf(t, err, "decommission codecNode [%v] failed", addr)
	server.cluster.codecNodes.Delete(addr)
}

func getCodecNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetCodecNode, addr)
	process(reqURL, t)
}

func getAllCodecNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetAllCodecNodes, addr)
	process(reqURL, t)
}

func decommissionCodecNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionCodecNode, addr)
	process(reqURL, t)
}
