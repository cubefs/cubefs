package raftstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Default monitoring configuration constants
const (
	defaultReportDuration    = time.Minute * 3
	defaultZombieThreshold   = time.Minute * 3
	defaultNoLeaderThreshold = time.Second * 30
)

// Configuration key constants for monitor settings
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
	zombieDurations   sync.Map
	noLeaderDurations sync.Map
}

func newMonitor() *monitor {
	m := &monitor{}
	return m
}

func (d *monitor) MonitorZombie(id uint64, peer proto.Peer, replicasMsg string, du time.Duration) {
	if du < gMonConf.ZombieThreshold {
		return
	}

	zombiePeer := zombiePeer{
		partitionID: id,
		peer:        peer,
	}

	oldDuInterface, exists := d.zombieDurations.Load(zombiePeer)
	var oldDu time.Duration
	if exists {
		oldDu = oldDuInterface.(time.Duration)
	}

	needReport := true
	var errMsg string

	if !exists || du < oldDu {
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

	d.zombieDurations.Store(zombiePeer, du)
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}

func (d *monitor) MonitorElection(id uint64, replicaMsg string, du time.Duration) {
	if du < gMonConf.NoLeaderThreshold {
		return
	}
	needReport := true
	var errMsg string

	oldDuInterface, exists := d.noLeaderDurations.Load(id)
	var oldDu time.Duration
	if exists {
		oldDu = oldDuInterface.(time.Duration)
	}

	if !exists || du < oldDu {
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

	d.noLeaderDurations.Store(id, du)
	log.LogError(errMsg)
	exporter.Warning(errMsg)
}

func (d *monitor) RemovePeer(id uint64, p proto.Peer) {
	zp := zombiePeer{
		partitionID: id,
		peer:        p,
	}

	_, present := d.zombieDurations.Load(zp)
	if present {
		d.zombieDurations.Delete(zp)
		log.LogInfof("remove peer from raft monitor, partitionID: %v, peer: %v", id, p)
	}
}

func (d *monitor) RemovePartition(id uint64, peers []proto.Peer) {
	_, present := d.noLeaderDurations.Load(id)
	if present {
		d.noLeaderDurations.Delete(id)
		log.LogInfof("remove partition from raft monitor, partitionID: %v, peers: %v", id, peers)
	}

	for _, p := range peers {
		d.RemovePeer(id, p)
	}
}
