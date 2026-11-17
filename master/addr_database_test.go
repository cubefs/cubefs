// Copyright 2026 The CubeFS Authors.
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

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestAddrDatabaseMembership(t *testing.T) {
	db := newAddrDatabase()
	db.Set(1, "192.0.2.11:17010")
	db.Set(2, "192.0.2.12:17010")
	db.Set(3, "192.0.2.13:17010")
	want := db.Snapshot()

	for _, id := range []uint64{4, 5} {
		addr := fmt.Sprintf("192.0.2.%d:17010", id+10)
		db.Set(id, addr)
		if got := db.Get(id); got != addr {
			t.Fatalf("added peer %d: got %q, want %q", id, got, addr)
		}
		db.Delete(id)
		db.Delete(id)
		if got := db.Get(id); got != "" {
			t.Fatalf("removed peer %d remains at %q", id, got)
		}
	}

	if got := db.Snapshot(); !reflect.DeepEqual(want, got) {
		t.Fatalf("members after two add/remove cycles: got %v, want %v", got, want)
	}
}

func TestAddrDatabaseSnapshot(t *testing.T) {
	db := newAddrDatabase()
	db.Set(1, "192.0.2.11:17010")
	snapshot := db.Snapshot()

	db.Set(2, "192.0.2.12:17010")
	db.Delete(1)
	if len(snapshot) != 1 || snapshot[1] != "192.0.2.11:17010" {
		t.Fatalf("membership changes modified an existing snapshot: %v", snapshot)
	}

	snapshot[2] = "192.0.2.99:17010"
	if got := db.Get(2); got != "192.0.2.12:17010" {
		t.Fatalf("snapshot mutation changed the database: %q", got)
	}
}

func TestAddrDatabaseConcurrentAccess(t *testing.T) {
	db := newAddrDatabase()
	db.Set(1, "192.0.2.11:17010")
	var workers sync.WaitGroup
	start := make(chan struct{})

	for workerID := uint64(2); workerID <= 9; workerID++ {
		workers.Add(1)
		go func(id uint64) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 1000; iteration++ {
				db.Set(id, "192.0.2.14:17010")
				db.Get(id)
				for peerID := range db.Snapshot() {
					db.Get(peerID)
				}
				db.Delete(id)
			}
		}(workerID)
	}
	close(start)
	workers.Wait()

	want := map[uint64]string{1: "192.0.2.11:17010"}
	if got := db.Snapshot(); !reflect.DeepEqual(want, got) {
		t.Fatalf("members after concurrent updates: got %v, want %v", got, want)
	}
}
