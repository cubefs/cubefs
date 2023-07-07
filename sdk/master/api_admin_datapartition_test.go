package master

import (
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
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
	if !assert.Nil(t, err) {
		return
	}

	// get datanode info
	var cv *proto.ClusterView
	cv, err = testMc.AdminAPI().GetCluster()
	if !assert.Nil(t, err) {
		return
	}
	if !assert.GreaterOrEqual(t, len(cv.DataNodes), 1) {
		return
	}

	nodes := cv.DataNodes
	dps, err := testMc.ClientAPI().GetDataPartitions(testVolName)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Greater(t, len(dps.DataPartitions), 0) {
		return
	}

	testDataPartitionID := dps.DataPartitions[0].PartitionID

	//Get Data Partition info
	var dataPartitionInfo *proto.DataPartitionInfo
	dataPartitionInfo, err = testMc.AdminAPI().GetDataPartition(testVolName, testDataPartitionID)
	if !assert.Nil(t, err) {
		return
	}

	replicas := dataPartitionInfo.Replicas

	//Get a non-leader address
	nonLeaderAddr := findNonLeaderAddr(replicas)
	if !assert.NotEqual(t, nonLeaderAddr, "") {
		return
	}

	//Gets the new replica address
	newAddr := findNewAddr(replicas, nodes)
	if !assert.NotEqual(t, newAddr, "") {
		return
	}

	//Decommission Data Partition
	wrongAddr := "127.0.0.1:9980"
	err = testMc.AdminAPI().DecommissionDataPartition(testDataPartitionID, wrongAddr, "")
	if !assert.NotNil(t, err) {
		return
	}
	if !assert.Contains(t, err.Error(), "internal error") {
		return
	}

	//Diagnose Data Partition
	_, err = testMc.AdminAPI().DiagnoseDataPartition()
	if !assert.Nil(t, err) {
		return
	}

	//Delete Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteDataReplica(testDataPartitionID, nonLeaderAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if !assert.Nil(t, err) {
		return
	}

	//Add Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().AddDataReplica(testDataPartitionID, nonLeaderAddr, 0)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if !assert.Nil(t, err) {
		return
	}

	//Add Data Learner
	autoPromote := false
	var threshold uint8 = 10
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().AddDataLearner(testDataPartitionID, newAddr, autoPromote, threshold)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if !assert.Nil(t, err) {
		return
	}

	//Promote Data Learner
	err = testMc.AdminAPI().PromoteDataLearner(testDataPartitionID, newAddr)
	if !assert.Nil(t, err) {
		return
	}

	//Delete Data Replica
	for i := 0; i == 0 || i < count && err != nil; i++ {
		err = testMc.AdminAPI().DeleteDataReplica(testDataPartitionID, newAddr)
		time.Sleep(time.Duration(1) * time.Second)
	}
	if !assert.Nil(t, err) {
		return
	}

	//Reset Data Partition
	err = testMc.AdminAPI().ResetDataPartition(testDataPartitionID)
	if !assert.NotNil(t, err) {
		return
	}
	if !assert.Equal(t, err.Error(), "live replica num more than half, can not be reset") {
		return
	}

	//Manual Reset Data Partition
	err = testMc.AdminAPI().ManualResetDataPartition(testDataPartitionID, wrongAddr)
	if !assert.NotNil(t, err) {
		return
	}
	if !assert.Equal(t, err.Error(), "data node not exists") {
		return
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
