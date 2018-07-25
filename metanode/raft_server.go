package metanode

import (
	"os"
	"strconv"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/raftstore"
)

// StartRaftServer init address resolver and raftStore server instance.
func (m *MetaNode) startRaftServer() (err error) {
	if _, err = os.Stat(m.raftDir); err != nil {
		if err = os.MkdirAll(m.raftDir, 0755); err != nil {
			err = errors.Errorf("create raft server dir: %s", err.Error())
			return
		}
	}

	heartbeatPort, _ := strconv.Atoi(m.raftHeartbeatPort)
	replicatePort, _ := strconv.Atoi(m.raftReplicatePort)

	raftConf := &raftstore.Config{
		NodeID:        m.nodeId,
		WalPath:       m.raftDir,
		IpAddr:        m.localAddr,
		HeartbeatPort: heartbeatPort,
		ReplicatePort: replicatePort,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = errors.Errorf("new raftStore: %s", err.Error())
	}
	return
}

func (m *MetaNode) stopRaftServer() {
	if m.raftStore != nil {
		m.raftStore.Stop()
	}
}
