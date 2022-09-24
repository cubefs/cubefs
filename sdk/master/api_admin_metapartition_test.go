package master

import (
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func TestMetaPartitionAPI(t *testing.T) {
	var err error
	var count int = 10
	//Create Meta Partition
	testVolName := "ltptest"
	var testInodeStart uint64 = 1
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().CreateMetaPartition(testVolName, testInodeStart)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("CreateMetaPartition failed, err %v", err)
	}

	// get meta node info
	var cv *proto.ClusterView
	cv, err = testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.MetaNodes) < 1 {
		t.Fatalf("metanodes[] len < 1")
	}
	nodes := cv.MetaNodes
	maxMetaPartitionId := cv.MaxMetaPartitionID

	testMetaPartitionID := maxMetaPartitionId

	//Get Meta Partition Info
	var metaPartitionInfo *proto.MetaPartitionInfo
	metaPartitionInfo, err = testMc.ClientAPI().GetMetaPartition(testMetaPartitionID)
	if err != nil {
		t.Fatalf("GetMetaPartition failed, err %v", err)
	}
	replicas := metaPartitionInfo.Replicas

	//Get a non-leader address
	nonLeaderAddr := metaFindNonLeaderAddr(replicas)
	if nonLeaderAddr == "" {
		t.Fatalf("do not find a non-leader addr")
	}

	//Gets the new replica address
	newAddr := metaFindNewAddr(replicas, nodes)
	if newAddr == "" {
		t.Fatalf("do not find a new addr")
	}

	//Decommission Meta Partition
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DecommissionMetaPartition(testMetaPartitionID, nonLeaderAddr, "", 0)
		time.Sleep(time.Duration(1+i/2) * time.Second)
	}
	if err != nil {
		t.Fatalf("DecommissionMetaPartition failed, err %v", err)
	}

	//Reset Meta Partition
	err = testMc.AdminAPI().ResetMetaPartition(testMetaPartitionID)
	if err == nil {
		t.Fatalf("expected err, but nil")
	}
	if err.Error() != "live replica num more than half, can not be reset" {
		t.Fatalf("expected err: 'live replica num more than half, can not be reset', but it's not")
	}

	//Manual Reset Meta Partition
	wrongAddr := "127.0.0.1:9980"
	err = testMc.AdminAPI().ManualResetMetaPartition(testMetaPartitionID, wrongAddr)
	if err == nil {
		t.Fatalf("expected err, but nil")
	}
	if err.Error() != "meta node not exists" {
		t.Fatalf("expected err: 'meta node not exists', but it's not")
	}

	//Diagnose Meta Partition
	_, err = testMc.AdminAPI().DiagnoseMetaPartition()
	if err != nil {
		t.Fatalf("DiagnoseMetaPartition failed, err %v", err)
	}

	//Delete Meta Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteMetaReplica(testMetaPartitionID, newAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("DeleteMetaReplica failed, err %v", err)
	}

	//Add Meta Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().AddMetaReplica(testMetaPartitionID, nonLeaderAddr, 0, 0)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("AddMetaReplica failed, %v", err)
	}
}

func TestMetaLearner(t *testing.T) {
	var err error
	// get meta node info
	var cv *proto.ClusterView
	cv, err = testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.MetaNodes) < 1 {
		t.Fatalf("metanodes[] len < 1")
	}
	nodes := cv.MetaNodes
	maxMetaPartitionId := cv.MaxMetaPartitionID
	var testMetaPartitionID uint64
	if maxMetaPartitionId >= 1 {
		testMetaPartitionID = 1
	} else {
		t.Fatalf("maxMetaPartitionID is not correct")
	}

	//Get Meta Partition Info
	var metaPartitionInfo *proto.MetaPartitionInfo
	metaPartitionInfo, err = testMc.ClientAPI().GetMetaPartition(testMetaPartitionID)
	if err != nil {
		t.Fatalf("GetMetaPartition failed, err %v", err)
	}
	replicas := metaPartitionInfo.Replicas

	//Get a non-leader address
	nonLeaderAddr := metaFindNonLeaderAddr(replicas)
	if nonLeaderAddr == "" {
		t.Fatalf("do not find a non-leader addr")
	}

	//Gets the new replica address
	newAddr := metaFindNewAddr(replicas, nodes)
	if newAddr == "" {
		t.Fatalf("do not find a new addr")
	}

	//Add Meta Replica Learner
	autoPromote := false
	var threshold uint8 = 10
	err = testMc.AdminAPI().AddMetaReplicaLearner(testMetaPartitionID, newAddr, autoPromote, threshold, 0, testStoreMode)
	if err != nil {
		t.Fatalf("AddMetaReplicaLearner failed, err %v", err)
	}

	//Promote Meta Replica Learner
	err = testMc.AdminAPI().PromoteMetaReplicaLearner(testMetaPartitionID, newAddr)
	if err != nil {
		t.Fatalf("PromoteMetaReplicaLearner failed, err %v", err)
	}

	//Delete Meta Replica
	var count int = 60
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteMetaReplica(testMetaPartitionID, newAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("DeleteMetaReplica failed, err %v", err)
	}
}

func metaFindNewAddr(replicas []*proto.MetaReplicaInfo, nodes []proto.NodeView) string {
	newAddr := ""
	for _, i := range nodes {
		flag := false
		for _, j := range replicas {
			if i.Addr == j.Addr {
				flag = true
				break
			}
		}
		if !flag {
			newAddr = i.Addr
			break
		}
	}
	return newAddr
}

func metaFindNonLeaderAddr(replicas []*proto.MetaReplicaInfo) string {
	addr := ""
	for _, i := range replicas {
		if i.IsLeader == false {
			addr = i.Addr
		}
	}
	return addr
}
