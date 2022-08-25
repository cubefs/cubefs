package master

import (
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
	// get meta node info
	cv, err := testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Fatalf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if len(cv.MetaNodes) < 1 {
		t.Fatalf("metanodes[] len < 1")
	}
	testVolName := "ltptest"
	views, err := testMc.ClientAPI().GetMetaPartitions(testVolName)
	if err != nil {
		t.Fatalf("GetMetaPartitions failed, err %v", err)
	}
	for _, view := range views {
		_, err = testMc.ClientAPI().GetMetaPartition(view.PartitionID)
		if err != nil {
			t.Fatalf("GetMetaPartition failed, err %v", err)
		}
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
	err := testMc.ClientAPI().ApplyVolMutex(testVolName)
	if err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestReleaseVolMutex(t *testing.T) {
	testVolName := "ltptest"
	err := testMc.ClientAPI().ReleaseVolMutex(testVolName)
	if err != nil {
		t.Fatalf("unexpected err")
	}
}
