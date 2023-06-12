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

func TestClientAllocate(t *testing.T) {
	allocator := newIDAllocator(server.rocksDBStore, server.partition)
	var id uint64
	for i := 0; i != 1010; i++ {
		newId, err := allocator.allocateClientID()
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
	newId, err := allocator.allocateClientID()
	if err != nil {
		t.Errorf("failed to allocate id %v", err.Error())
	}
	if newId < id {
		t.Errorf("id should be uniqued and self-increased")
	}
	t.Logf("new id is %v", newId)
}
