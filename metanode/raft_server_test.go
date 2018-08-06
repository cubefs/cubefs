// Copyright 2018 The ChuBao Authors.
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
	"os"
	"testing"
)

func Test_startRaftServer(t *testing.T) {
	m := NewServer()
	m.raftDir = "raft_logs"
	m.nodeId = 55555
	m.localAddr = "127.0.0.1"
	defer os.RemoveAll(m.raftDir)
	if err := m.startRaftServer(); err != nil {
		t.Fatalf("raftServer test failed!")
	}
	m.stopRaftServer()
}
