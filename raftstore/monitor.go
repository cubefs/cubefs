package raftstore

import (
	"fmt"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	defaultReportDuration    = time.Minute * 3
	defaultZombieThreshold   = time.Minute * 3
	defaultNoLeaderThreshold = time.Second * 30
)

const (
	cfgZombieThresholdSec          = "raftMonZombieThrSec"
	cfgZombieTooLongThresholdSec   = "raftMonZombieTooLongThrSec"
	cfgNoLeaderThresholdSec        = "raftMonNoLeaderThrSec"
	cfgNoLeaderTooLongThresholdSec = "raftMonNoLeaderTooLongThrSec"
)

type monitorConf struct {
	ZombieThreshold          time.Duration
	ZombieTooLongThreshold   time.Duration
	NoLeaderThreshold        time.Duration
	NoLeaderTooLongThreshold time.Duration
}

var gMonConf = monitorConf{
	ZombieThreshold:          defaultZombieThreshold,
	ZombieTooLongThreshold:   defaultReportDuration,
	NoLeaderThreshold:        defaultNoLeaderThreshold,
	NoLeaderTooLongThreshold: defaultReportDuration,
}

func setMonitorConf(cfg *config.Config) {
	if cfg == nil {
		return
	}

	cfgZomThr := cfg.GetInt64(cfgZombieThresholdSec)
	if cfgZomThr > 0 {
		gMonConf.ZombieThreshold = time.Second * time.Duration(cfgZomThr)
	}

	cfgZomTooLongThr := cfg.GetInt64(cfgZombieTooLongThresholdSec)
	if cfgZomTooLongThr > 0 {
		gMonConf.ZombieTooLongThreshold = time.Second * time.Duration(cfgZomTooLongThr)
	}

	cfgNoLeaderThr := cfg.GetInt64(cfgNoLeaderThresholdSec)
	if cfgNoLeaderThr > 0 {
		gMonConf.NoLeaderThreshold = time.Second * time.Duration(cfgNoLeaderThr)
	}

	cfgNoLeaderTooLongThr := cfg.GetInt64(cfgNoLeaderTooLongThresholdSec)
	if cfgNoLeaderTooLongThr > 0 {
		gMonConf.NoLeaderTooLongThreshold = time.Second * time.Duration(cfgNoLeaderTooLongThr)
	}

	log.LogInfof("set raft monitor cfg: zombieThreshold:[%v], zombieTooLongThreshold:[%v],"+
		" noLeaderThreshold:[%v], noLeaderTooLongThreshold:[%v]",
		gMonConf.ZombieThreshold, gMonConf.ZombieTooLongThreshold,
		gMonConf.NoLeaderThreshold, gMonConf.NoLeaderTooLongThreshold)
}

type zombiePeer struct {
	partitionID uint64
	peer        proto.Peer
}

type monitor struct {
	zombieDurations     map[zombiePeer]time.Duration
	zombieDurationMutex sync.RWMutex

	noLeaderDurations      map[uint64]time.Duration
	noLeaderDurationsMutex sync.RWMutex
}

func newMonitor() *monitor {
	var m *monitor
	m = &monitor{}

	m.zombieDurations = make(map[zombiePeer]time.Duration)
	m.noLeaderDurations = make(map[uint64]time.Duration)
	return m
}

func (d *monitor) MonitorZombie(id uint64, peer proto.Peer, replicasMsg string, du time.Duration) {
	if du < gMonConf.ZombieThreshold {
		return
	}

	needReport := true
	var errMsg string

	zombiePeer := zombiePeer{
		partitionID: id,
		peer:        peer,
	}

	d.zombieDurationMutex.RLock()
	oldDu := d.zombieDurations[zombiePeer]
	d.zombieDurationMutex.RUnlock()

	if oldDu == 0 || du < oldDu {
		// peer became zombie recently
		errMsg = fmt.Sprintf("[MonitorZombie] raft peer zombie, "+
			"partitionID[%d] replicaID[%v] replicasMsg[%s] zombiePeer[%v] zombieDuration[%v]",
			id, peer.PeerID, replicasMsg, peer, du)
	} else if du-oldDu > gMonConf.ZombieTooLongThreshold {
		// peer keeping zombie for too long
		errMsg = fmt.Sprintf("[MonitorZombieTooLong] raft peer zombie too long, "+
			"partitionID[%d] replicaID[%v] replicasMsg[%s] zombiePeer[%v] zombieDuration[%v]",
			id, peer.PeerID, replicasMsg, peer, du)
	} else {
		// peer keeping zombie, but it's not time for another too-long-report yet
		needReport = false
	}

	if !needReport {
		return
	}
	d.zombieDurationMutex.Lock()
	d.zombieDurations[zombiePeer] = du
	d.zombieDurationMutex.Unlock()
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}

func (d *monitor) MonitorElection(id uint64, replicaMsg string, du time.Duration) {
	if du < gMonConf.NoLeaderThreshold {
		return
	}
	needReport := true
	var errMsg string

	d.noLeaderDurationsMutex.RLock()
	oldDu := d.noLeaderDurations[id]
	d.noLeaderDurationsMutex.RUnlock()

	if oldDu == 0 || du < oldDu {
		// became no leader recently
		errMsg = fmt.Sprintf("[RaftNoLeader] raft no leader partitionID[%d]_replicas[%v]_Duration[%v]",
			id, replicaMsg, du)
	} else if du-oldDu > gMonConf.NoLeaderTooLongThreshold {
		// keeping no leader for too long
		errMsg = fmt.Sprintf("[RaftNoLeaderTooLong] raft no leader too long, "+
			"partitionID[%d]_replicas[%v]_Duration[%v]",
			id, replicaMsg, du)
	} else {
		// keeping not health, but it's not time for another too-long-report yet
		needReport = false
	}

	if !needReport {
		return
	}

	d.noLeaderDurationsMutex.Lock()
	d.noLeaderDurations[id] = du
	d.noLeaderDurationsMutex.Unlock()
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}

func (d *monitor) RemovePeer(id uint64, p proto.Peer) {
	zp := zombiePeer{
		partitionID: id,
		peer:        p,
	}

	d.zombieDurationMutex.Lock()
	_, present := d.zombieDurations[zp]
	if present {
		delete(d.zombieDurations, zp)
		log.LogInfof("remove peer from raft monitor, partitionID: %v, peer: %v", id, p)
	}
	d.zombieDurationMutex.Unlock()
}

func (d *monitor) RemovePartition(id uint64, peers []proto.Peer) {
	d.noLeaderDurationsMutex.Lock()
	_, present := d.noLeaderDurations[id]
	if present {
		delete(d.noLeaderDurations, id)
		log.LogInfof("remove partition from raft monitor, partitionID: %v, peers: %v", id, peers)
	}
	d.noLeaderDurationsMutex.Unlock()

	for _, p := range peers {
		d.RemovePeer(id, p)
	}
}
