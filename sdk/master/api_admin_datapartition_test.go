package master

import (
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func init() {
	log.InitLog("/tmp/cfs/sdk/master/Logs", "test", log.DebugLevel, nil)
}

func TestDataPartitionAPI(t *testing.T) {
	var count int = 60
	testVolName := "ltptest"
	var err error
	//Create Data Partition
	testVolCount := 1
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().CreateDataPartition(testVolName, testVolCount)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("CreateDataPartition failed, err %v", err)
	}

	// get datanode info
	var cv *proto.ClusterView
	cv, err = testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.DataNodes) < 1 {
		t.Fatalf("datanodes[] len < 1")
	}
	nodes := cv.DataNodes
	maxDataPartitionID := cv.MaxDataPartitionID
	testDataPartitionID := maxDataPartitionID

	//Get Data Partition info
	var dataPartitionInfo *proto.DataPartitionInfo
	dataPartitionInfo, err = testMc.AdminAPI().GetDataPartition(testVolName, testDataPartitionID)
	if err != nil {
		t.Fatalf("GetDataPartition failed, err %v", err)
	}
	replicas := dataPartitionInfo.Replicas

	//Get a non-leader address
	nonLeaderAddr := findNonLeaderAddr(replicas)
	if nonLeaderAddr == "" {
		t.Fatalf("do not find a non-leader addr")
	}

	//Gets the new replica address
	newAddr := findNewAddr(replicas, nodes)
	if newAddr == "" {
		t.Fatalf("do not find a new addr")
	}

	//Decommission Data Partition
	wrongAddr := "127.0.0.1:9980"
	err = testMc.AdminAPI().DecommissionDataPartition(testDataPartitionID, wrongAddr, "")
	if err == nil {
		t.Fatalf("expected err, but nil")
	}
	if !strings.Contains(err.Error(), "internal error") {
		t.Fatalf("expected err: 'internal error', but it's not")
	}

	//Diagnose Data Partition
	_, err = testMc.AdminAPI().DiagnoseDataPartition()
	if err != nil {
		t.Fatalf("DiagnoseDataPartition failed, err %v", err)
	}

	//Delete Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteDataReplica(testDataPartitionID, nonLeaderAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("DeleteDataReplica failed, err %v", err)
	}

	//Add Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().AddDataReplica(testDataPartitionID, nonLeaderAddr, 0)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("AddDataReplica failed, err %v", err)
	}

	//Add Data Learner
	autoPromote := false
	var threshold uint8 = 10
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().AddDataLearner(testDataPartitionID, newAddr, autoPromote, threshold)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("AddDataLearner failed, err %v", err)
	}

	//Promote Data Learner
	err = testMc.AdminAPI().PromoteDataLearner(testDataPartitionID, newAddr)
	if err != nil {
		t.Fatalf("PromoteDataLearner failed, err %v", err)
	}

	//Delete Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteDataReplica(testDataPartitionID, newAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		t.Fatalf("DeleteDataReplica failed, err %v", err)
	}

	//Reset Data Partition
	err = testMc.AdminAPI().ResetDataPartition(testDataPartitionID)
	if err == nil {
		t.Fatalf("expected err, but nil")
	}
	if err.Error() != "live replica num more than half, can not be reset" {
		t.Fatalf("expected err: 'live replica num more than half, can not be reset', but it's not")
	}

	//Manual Reset Data Partition
	err = testMc.AdminAPI().ManualResetDataPartition(testDataPartitionID, wrongAddr)
	if err == nil {
		t.Fatalf("expected err, but nil")
	}
	if err.Error() != "data node not exists" {
		t.Fatalf("expected err: 'data node not exists', but it's not")
	}
}

func findNewAddr(replicas []*proto.DataReplica, nodes []proto.NodeView) string {
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

func findNonLeaderAddr(replicas []*proto.DataReplica) string {
	addr := ""
	for _, i := range replicas {
		if i.IsLeader == false {
			addr = i.Addr
		}
	}
	return addr
}
