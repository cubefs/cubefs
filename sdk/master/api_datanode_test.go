package master

import (
	"github.com/cubefs/cubefs/proto"
	"strings"
	"testing"
	"time"
)

var (
	testNodeAddr = "127.0.0.1:9980"
)

func TestDataNodeAPI(t *testing.T) {
	// add datanode
	_, err := testMc.NodeAPI().AddDataNode(testNodeAddr, testZoneName, "1.0.0")
	if err != nil {
		t.Fatalf("Add data node failed: err(%v), addr(%v)", err, testNodeAddr)
	}
	// reset datanode
	if err = testMc.AdminAPI().ResetCorruptDataNode(testNodeAddr); err == nil {
		t.Errorf("Reset corrupt data node expect fail, but success, addr(%v)", testNodeAddr)
	}
	// task
	task := &proto.AdminTask{}
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: testMc.leaderAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, testNodeAddr, request)
	if err = testMc.NodeAPI().ResponseDataNodeTask(task); err != nil {
		t.Errorf("Response data node task failed: err(%v), task(%v)", err, task)
	}
	//datanode disk decommission
	var diskPath = "/cfs"
	if err = testMc.NodeAPI().DataNodeDiskDecommission(testNodeAddr, diskPath); err != nil {
		t.Errorf("Data node disk decommission failed: err(%v), node(%v), disk(%v)", err, testNodeAddr, diskPath)
	}
	//datanode decommission
	if err = testMc.NodeAPI().DataNodeDecommission(testNodeAddr); err != nil {
		t.Fatalf("Data node decommission failed: err(%v), addr(%v)", err, testNodeAddr)
	}
	// get datanode info
	cv, err := testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.DataNodes) < 1 {
		t.Fatalf("datanodes[] len < 1")
	}
	nodeAddr := cv.DataNodes[0].Addr
	info, err := testMc.NodeAPI().GetDataNode(nodeAddr)
	if err != nil {
		t.Fatalf("Get data node failed: err(%v), addr(%v)", err, nodeAddr)
	}
	//datanode get partition info
	if len(info.PersistenceDataPartitions) < 1 {
		t.Fatalf("partaitions[] len < 1")
	}
	testMc.DataNodeProfPort = 17320
	dpInfo, err := testMc.NodeAPI().DataNodeGetPartition(strings.Split(nodeAddr, ":")[0], info.PersistenceDataPartitions[0])
	if err != nil {
		t.Errorf("Data node get partition info failed: err(%v), node(%v), pid(%v), pinfo(%v)",
			err, info.Addr, info.PersistenceDataPartitions[0], dpInfo)
	}
}
