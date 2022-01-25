package raftstore

import (
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft/proto"
	"time"
)

const (
	reportDuration          = time.Minute * 3
	zombieThreshold         = time.Minute * 3
	raftNotHealthyThreshold = time.Second * 30
)

type zombiePeer struct {
	partitionID uint64
	peer        proto.Peer
}

type monitor struct {
	zombieDurations   map[zombiePeer]time.Duration
	noLeaderDurations map[uint64]time.Duration
}

func newMonitor() *monitor {
	return &monitor{
		zombieDurations:   make(map[zombiePeer]time.Duration),
		noLeaderDurations: make(map[uint64]time.Duration),
	}
}

func (d *monitor) MonitorZombie(id uint64, peer proto.Peer, replicasMsg string, du time.Duration) {
	if du < zombieThreshold {
		return
	}
	needReport := false
	zombiePeer := zombiePeer{
		partitionID: id,
		peer:        peer,
	}
	oldDu := d.zombieDurations[zombiePeer]
	if oldDu == 0 || du < oldDu || du-oldDu > reportDuration {
		d.zombieDurations[zombiePeer] = du
		needReport = true
	}
	if !needReport {
		return
	}
	errMsg := fmt.Sprintf("[MonitorZombie] raft partitionID[%d] replicaID[%v] replicasMsg[%s] zombiePeer[%v] zombieDuration[%v]",
		id, peer.PeerID, replicasMsg, peer, du)
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}

func (d *monitor) MonitorElection(id uint64, replicaMsg string, du time.Duration) {
	if du < raftNotHealthyThreshold {
		return
	}
	needReport := false
	oldDu := d.noLeaderDurations[id]
	if oldDu == 0 || du < oldDu || du-oldDu > reportDuration {
		d.noLeaderDurations[id] = du
		needReport = true
	}
	if !needReport {
		return
	}
	errMsg := fmt.Sprintf("[MonitorElection] raft status not health partitionID[%d]_replicas[%v]_noLeaderDuration[%v]",
		id, replicaMsg, du)
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}
