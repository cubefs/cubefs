package master

import (
	"github.com/chubaofs/chubaofs/proto"
	"testing"
)

func TestGetDataPartitions(t *testing.T) {
	testVolName := "ltptest"
	_, err := testMc.ClientAPI().GetDataPartitions(testVolName)
	if err != nil {
		t.Fatalf("GetDataPartitions failed, err %v", err)
	}
}

func TestGetMetaPartition(t *testing.T) {
	testVolName := "ltptest"
	// get meta node info
	cv, err := testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.MetaNodes) < 1 {
		t.Fatalf("metanodes[] len < 1")
	}
	mps, err := testMc.ClientAPI().GetMetaPartitions(testVolName)
	if err != nil {
		t.Error(err.Error())
	}
	if len(mps) == 0 {
		t.Errorf("get metapartitions failed, the metapartitions count is 0")
	}
	testMetaPartitionID := mps[0].PartitionID
	_, err = testMc.ClientAPI().GetMetaPartition(testMetaPartitionID, "")
	if err != nil {
		t.Fatalf("GetMetaPartition failed, err %v", err)
	}
}

func TestGetMetaPartitions(t *testing.T) {
	testVolName := "ltptest"
	_, err := testMc.ClientAPI().GetMetaPartitions(testVolName)
	if err != nil {
		t.Fatalf("GetMetaPartitions failed, err %v", err)
	}
}

func TestApplyVolMutex(t *testing.T) {
	testVolName := "ltptest"
	err := testMc.ClientAPI().ApplyVolMutex(testVolName, false)
	if err != proto.ErrVolWriteMutexUnable {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestReleaseVolMutex(t *testing.T) {
	testVolName := "ltptest"
	err := testMc.ClientAPI().ReleaseVolMutex(testVolName)
	if err != proto.ErrVolWriteMutexUnable {
		t.Fatalf("unexpected err: %v", err)
	}
}
