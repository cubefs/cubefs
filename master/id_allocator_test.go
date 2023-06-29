// Copyright 2023 The CubeFS Authors.
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

package master

import "testing"

const idallocTestCount = 1010

func SelfIncreaseIdAllocTest(t *testing.T, allocator *IDAllocator, allocFunc func() (uint64, error)) {
	var id uint64
	for i := 0; i != idallocTestCount; i++ {
		newId, err := allocFunc()
		if err != nil {
			t.Errorf("failed to allocate id %v", err.Error())
		}
		if newId < id {
			t.Errorf("id should be uniqued and self-increased")
		}
		t.Logf("new id is %v", newId)
		id = newId
	}
	allocator.restore()
	newId, err := allocFunc()
	if err != nil {
		t.Errorf("failed to allocate id %v", err.Error())
	}
	if newId < id {
		t.Errorf("id should be uniqued and self-increased")
	}
	t.Logf("new id is %v", newId)
}

func TestIdAlloc(t *testing.T) {
	allocator := newIDAllocator(server.rocksDBStore, server.partition)
	t.Logf("testing client id alloc")
	SelfIncreaseIdAllocTest(t, allocator, func() (uint64, error) {
		return allocator.allocateClientID()
	})
	t.Logf("testing common id alloc")
	SelfIncreaseIdAllocTest(t, allocator, func() (uint64, error) {
		return allocator.allocateCommonID()
	})
	t.Logf("testing data partition id alloc")
	SelfIncreaseIdAllocTest(t, allocator, func() (uint64, error) {
		return allocator.allocateDataPartitionID()
	})
	t.Logf("testing meta partition id alloc")
	SelfIncreaseIdAllocTest(t, allocator, func() (uint64, error) {
		return allocator.allocateMetaPartitionID()
	})
	t.Logf("testing quota id alloc")
	SelfIncreaseIdAllocTest(t, allocator, func() (uint64, error) {
		id, err := allocator.allocateQuotaID()
		return uint64(id), err
	})
}
