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

import "sync"

type addrDatabase struct {
	mu    sync.RWMutex
	addrs map[uint64]string
}

func newAddrDatabase() *addrDatabase {
	return &addrDatabase{addrs: make(map[uint64]string)}
}

func (db *addrDatabase) Get(id uint64) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.addrs[id]
}

func (db *addrDatabase) Set(id uint64, addr string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.addrs[id] = addr
}

func (db *addrDatabase) Delete(id uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.addrs, id)
}

func (db *addrDatabase) Snapshot() map[uint64]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	addrs := make(map[uint64]string, len(db.addrs))
	for id, addr := range db.addrs {
		addrs[id] = addr
	}
	return addrs
}
