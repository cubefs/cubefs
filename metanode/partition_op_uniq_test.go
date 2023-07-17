// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"testing"

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/golang/mock/gomock"
)

func TestInodeOnce(t *testing.T) {
	ino := &InodeOnce{
		UniqID: 123,
		Inode:  456,
	}

	val := ino.Marshal()
	ino2 := InodeOnceUnmarshal(val)
	if *ino2 != *ino {
		t.Fatal("inode once unmarshal failed")
	}
}

func TestAllocateUniqId(t *testing.T) {
	mp := metaPartition{config: &MetaPartitionConfig{UniqId: 0}}

	s, e := mp.allocateUniqID(1)
	if s != 1 || e != 1 {
		t.Errorf("allocateUniqID failed: %v, %v", s, e)
	}

	s, e = mp.allocateUniqID(1000)
	if s != 2 || e != 1001 {
		t.Errorf("allocateUniqID failed: %v, %v", s, e)
	}
}

func TestDoEvit(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaft(mockCtrl)
	checker := mp.uniqChecker

	mp.uniqCheckerEvict()
	if checker.inQue.len() != 0 || len(checker.op) != 0 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue.len(), len(checker.op))
	}

	for i := 1; i <= 1; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	mp.uniqCheckerEvict()
	if checker.inQue.len() != 1 || len(checker.op) != 1 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue, checker.op)
	}
	mp.uniqChecker.keepTime = 0
	mp.uniqCheckerEvict()
	if checker.inQue.len() != 0 || len(checker.op) != 0 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue, checker.op)
	}

	for i := 1; i <= 1000; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	if checker.inQue.len() != 1000 || len(checker.op) != 1000 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue.len(), len(checker.op))
	}

	mp.uniqCheckerEvict()
	mp.uniqChecker.keepOps = 100
	for i := 1001; i <= 1100; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	mp.uniqCheckerEvict()
	if checker.inQue.len() != 100 || len(checker.op) != 100 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue.len(), len(checker.op))
	}
}

func TestDoEvit1(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaft(mockCtrl)
	checker := mp.uniqChecker
	mp.uniqChecker.keepTime = 0

	for i := 1; i <= 10240; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	mp.uniqCheckerEvict()

	if checker.inQue.len() != 0 || len(checker.op) != 0 {
		t.Errorf("failed, inQue %v, op %v", checker.inQue, checker.op)
	}

}

func TestDoEvit2(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaft(mockCtrl)
	checker := mp.uniqChecker
	mp.uniqChecker.keepTime = 0

	for i := 1; i <= 9000; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	for i := 9001; i <= 13000; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	mp.uniqCheckerEvict()
	if checker.inQue.len() != len(checker.op) {
		t.Errorf("failed, inQue %v, op %v", checker.inQue.len(), len(checker.op))
	}
}

func mockPartitionRaft(ctrl *gomock.Controller) *metaPartition {
	partition := NewMetaPartition(nil, nil).(*metaPartition)
	partition.uniqChecker.keepTime = 1
	partition.uniqChecker.keepOps = 0
	raft := raftstoremock.NewMockPartition(ctrl)
	idx := uint64(0)
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		idx++
		return partition.Apply(cmd, idx)
	}).AnyTimes()

	partition.raftPartition = raft

	return partition
}
