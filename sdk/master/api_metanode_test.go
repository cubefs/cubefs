package master

import (
	"github.com/cubefs/cubefs/proto"
	"strings"
	"testing"
	"time"
)

var (
	testMetaAddr = "127.0.0.1:8890"
)

func TestMetaNodeAPI(t *testing.T) {
	// add meta node
	_, err := testMc.NodeAPI().AddMetaNode(testMetaAddr, testZoneName, "1.0.0")
	if err != nil {
		t.Fatalf("Add mete node failed: err(%v), addr(%v)", err, testMetaAddr)
	}
	// reset meta node
	if err = testMc.AdminAPI().ResetCorruptMetaNode(testMetaAddr); err != nil {
		t.Errorf("Reset corrupt meta node failed: err(%v), addr(%v)", err, testMetaAddr)
	}
	// task
	task := &proto.AdminTask{}
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: testMc.leaderAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, testMetaAddr, request)
	if err = testMc.NodeAPI().ResponseMetaNodeTask(task); err != nil {
		t.Errorf("Response meta node task failed: err(%v), task(%v)", err, task)
	}
	//meta node decommission
	if err = testMc.NodeAPI().MetaNodeDecommission(testMetaAddr); err != nil {
		t.Fatalf("Meta node decommission failed: err(%v), node(%v)", err, testMetaAddr)
	}
	// set meta node threshold
	var threshold = 0.7
	if err = testMc.AdminAPI().SetMetaNodeThreshold(threshold); err != nil {
		t.Errorf("Set threshold for meta node failed: err(%v)", err)
	}
	// get meta node info
	cv, err := testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.MetaNodes) < 1 {
		t.Fatalf("metanodes[] len < 1")
	}
	nodeAddr := cv.MetaNodes[0].Addr
	mInfo, err := testMc.NodeAPI().GetMetaNode(nodeAddr)
	if err != nil {
		t.Fatalf("Get meta node failed: err(%v), addr(%v)", err, testMetaAddr)
	}
	//meta node get partition info
	if len(mInfo.PersistenceMetaPartitions) < 1 {
		t.Fatalf("Mpartaitions[] len < 1")
	}
	testMc.MetaNodeProfPort = 17220
	mpInfo, err := testMc.NodeAPI().MetaNodeGetPartition(strings.Split(nodeAddr, ":")[0], mInfo.PersistenceMetaPartitions[0])
	if err != nil {
		t.Errorf("Meta node get partition info failed: err(%v), node(%v), pid(%v), pinfo(%v)",
			err, mInfo.Addr, mInfo.PersistenceMetaPartitions[0], mpInfo)
	}
}
