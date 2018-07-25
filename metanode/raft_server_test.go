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
