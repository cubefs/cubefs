package raft

import (
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"time"
)

//The Monitor interface is used to monitor the health status of the raft.
type Monitor interface {
	//If a peer in replica has no respond for long time (2*TickInterval), MonitorZombie will be called.
	MonitorZombie(id uint64, peer proto.Peer, replicasMsg string, du time.Duration)
	//If raft election failed continuously. MonitorElection will be called
	MonitorElection(id uint64, replicaMsg string, du time.Duration)

	RemovePeer(id uint64, peer proto.Peer)
	RemovePartition(id uint64, peers []proto.Peer)
}
