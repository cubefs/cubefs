package datanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/datanode/mock"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/jacobsa/daemonize"
	"os"
)

var fakeNode *fakeDataNode

const (
	mockClusterName   = "datanode-cluster"
	testLogDir        = "/cfs/log"
	testDiskPath      = "/data1/mock_disk1"
	localIP           = "127.0.0.1"
	localNodeAddress  = localIP + ":" + tcpProtoPort
	raftHeartBeatPort = "17331"
	raftReplicaPort   = "17341"
	tcpProtoPort      = "11010"
	profPort          = "11210"
	mockZone01        = "zone-01"
)

func init() {
	fmt.Println("init datanode")
	mock.NewMockMaster()
	if err := FakeNodePrepare(); err != nil {
		panic(err.Error())
	}
}

func newFakeDataNode() *fakeDataNode {
	fdn := &fakeDataNode{
		DataNode: DataNode{
			clusterID:       mockClusterName,
			raftHeartbeat:   raftHeartBeatPort,
			raftReplica:     raftReplicaPort,
			port:            tcpProtoPort,
			zoneName:        mockZone01,
			httpPort:        profPort,
			localIP:         localIP,
			localServerAddr: localNodeAddress,
			nodeID:          uint64(111),
			stopC:           make(chan bool),
		},
		fakeNormalExtentId: 1025,
		fakeTinyExtentId:   1,
	}
	MasterClient.AddNode(mock.TestMasterHost)

	cfgStr := `{
		"disks": [
			"` + testDiskPath + `:5368709120"
		],
		"listen": "` + tcpProtoPort + `",
		"raftHeartbeat": "` + raftHeartBeatPort + `",
		"raftReplica": "` + raftReplicaPort + `",
		"logDir":"` + testLogDir + `",
		"raftDir":"` + testLogDir + `",
		"masterAddr": [
			"` + mock.TestMasterHost + `",
			"` + mock.TestMasterHost + `",
			"` + mock.TestMasterHost + `"
		],
		"prof": "` + profPort + `"
	}`
	_, err := log.InitLog(testLogDir, "mock_datanode", log.DebugLevel, nil)
	if err != nil {
		_ = daemonize.SignalOutcome(fmt.Errorf("Fatal: failed to init log - %v ", err))
		panic(err.Error())
	}
	defer log.LogFlush()
	cfg := config.LoadConfigString(cfgStr)

	if err = fdn.Start(cfg); err != nil {
		panic(fmt.Sprintf("startTCPService err(%v)", err))
	}
	return fdn
}

func FakeDirCreate() (err error) {
	_, err = os.Stat(testDiskPath)
	if err == nil {
		os.RemoveAll(testDiskPath)
	}
	if err = os.MkdirAll(testDiskPath, 0766); err != nil {
		panic(err)
	}
	return
}

func FakeNodePrepare() (err error) {
	if err = FakeDirCreate(); err != nil {
		return err
	}
	if fakeNode == nil {
		fakeNode = newFakeDataNode()
	}
	return
}
