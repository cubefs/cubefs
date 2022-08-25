// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"context"
	"fmt"
	"sync/atomic"
)

type (
	MsgType        byte
	EntryType      byte
	ConfChangeType byte
	PeerType       byte
)

const (
	ReqMsgAppend MsgType = iota
	ReqMsgVote
	ReqMsgHeartBeat
	ReqMsgSnapShot
	ReqMsgElectAck
	RespMsgAppend
	RespMsgVote
	RespMsgHeartBeat
	RespMsgSnapShot
	RespMsgElectAck
	LocalMsgHup
	LocalMsgProp
	LeaseMsgOffline
	LeaseMsgTimeout
	ReqCheckQuorum
	RespCheckQuorum
)

const (
	ConfAddNode        ConfChangeType = 0
	ConfRemoveNode     ConfChangeType = 1
	ConfUpdateNode     ConfChangeType = 2
	ConfAddLearner     ConfChangeType = 3
	ConfPromoteLearner ConfChangeType = 4

	EntryNormal     EntryType = 0
	EntryConfChange EntryType = 1

	PeerNormal  PeerType = 0
	PeerArbiter PeerType = 1

	LearnerProgress = 90
)

// The Snapshot interface is supplied by the application to access the snapshot data of application.
type Snapshot interface {
	SnapIterator
	ApplyIndex() uint64
	Close()
	Version() uint32
}

type SnapIterator interface {
	// if error=io.EOF represent snapshot terminated.
	Next() ([]byte, error)
}

type SnapshotMeta struct {
	Index    uint64
	Term     uint64
	Peers    []Peer
	Learners []Learner
	SnapV    uint32
}



type Peer struct {
	Type     PeerType
	Priority uint16
	ID       uint64 // NodeID
	PeerID   uint64 // Replica ID, unique over all raft groups and all replicas in the same group
}

type Learner struct {
	ID         uint64         `json:"id"` // NodeID
	PromConfig *PromoteConfig `json:"promote_config"`
}

// HardState is the repl state,must persist to the storage.
type HardState struct {
	Term   uint64
	Commit uint64
	Vote   uint64
}

var (
	LeaderGetEntryCnt uint64
	LeaderPutEntryCnt uint64
	FollowerGetEntryCnt uint64
	FollowerPutEntryCnt uint64
)



func LoadLeaderGetEntryCnt() uint64 {
	return atomic.LoadUint64(&LeaderGetEntryCnt)
}

func LoadLeaderPutEntryCnt() uint64 {
	return atomic.LoadUint64(&LeaderPutEntryCnt)
}

func LoadFollowerGetEntryCnt() uint64 {
	return atomic.LoadUint64(&FollowerGetEntryCnt)
}

func LoadFollowerPutEntryCnt() uint64 {
	return atomic.LoadUint64(&FollowerPutEntryCnt)
}

const (
	LeaderLogEntryRefCnt=4
	FollowerLogEntryRefCnt=2
)

// Entry is the repl log entry.
type Entry struct {
	Type  EntryType
	Term  uint64
	Index uint64
	Data  []byte
	ctx   context.Context // Tracer context
	RefCnt    int32
	OrgRefCnt uint8
}

func (e *Entry) SetCtx(ctx context.Context) {
	e.ctx = ctx
}

func (e *Entry)IsLeaderLogEntry() bool {
	return e.OrgRefCnt > MinLeaderLogEntryRefCnt
}


func (e *Entry)IsFollowerLogEntry() bool {
	return e.OrgRefCnt==FollowerLogEntryRefCnt
}

func (e *Entry)DecRefCnt() {
	if e.IsLeaderLogEntry() || e.IsFollowerLogEntry() {
		atomic.AddInt32(&e.RefCnt, -1)
	}
}

func (e *Entry) Ctx() context.Context {
	return e.ctx
}

// Message is the transport message.
type Message struct {
	Type         MsgType
	ForceVote    bool
	Reject       bool
	RejectIndex  uint64
	ID           uint64
	From         uint64
	To           uint64
	Term         uint64
	LogTerm      uint64
	Index        uint64
	Commit       uint64
	SnapshotMeta SnapshotMeta
	Entries      []*Entry
	Context      []byte
	Snapshot     Snapshot // No need for codec
	ctx          context.Context
	magic        uint8
}

func (m *Message) Ctx() context.Context {
	return m.ctx
}
func (m *Message) SetCtx(ctx context.Context) {
	m.ctx = ctx
}

func (m *Message) ToString() (mesg string) {
	return fmt.Sprintf("Mesg:[%v] type(%v) ForceVote(%v) Reject(%v) RejectIndex(%v) "+
		"From(%v) To(%v) Term(%v) LogTrem(%v) Index(%v) Commit(%v)", m.ID, m.Type.String(), m.ForceVote,
		m.Reject, m.RejectIndex, m.From, m.To, m.Term, m.LogTerm, m.Index, m.Commit)
}

type ConfChange struct {
	Type    ConfChangeType
	Peer    Peer
	Context []byte
}

type PromoteConfig struct {
	PromThreshold uint8 `json:"prom_threshold"`
	AutoPromote   bool  `json:"auto_prom"`
}

type ConfChangeLearnerReq struct {
	Id            uint64  `json:"pid"`
	ChangeLearner Learner `json:"learner"`
}
type ResetPeers struct {
	NewPeers []Peer
	Context  []byte
}
type HeartbeatContext []uint64

func (t MsgType) String() string {
	switch t {
	case 0:
		return "ReqMsgAppend"
	case 1:
		return "ReqMsgVote"
	case 2:
		return "ReqMsgHeartBeat"
	case 3:
		return "ReqMsgSnapShot"
	case 4:
		return "ReqMsgElectAck"
	case 5:
		return "RespMsgAppend"
	case 6:
		return "RespMsgVote"
	case 7:
		return "RespMsgHeartBeat"
	case 8:
		return "RespMsgSnapShot"
	case 9:
		return "RespMsgElectAck"
	case 10:
		return "LocalMsgHup"
	case 11:
		return "LocalMsgProp"
	case 12:
		return "LeaseMsgOffline"
	case 13:
		return "LeaseMsgTimeout"
	case 14:
		return "ReqCheckQuorum"
	case 15:
		return "RespCheckQuorum"
	}
	return "unkown"
}

func (t EntryType) String() string {
	switch t {
	case 0:
		return "EntryNormal"
	case 1:
		return "EntryConfChange"
	}
	return "unkown"
}

func (t ConfChangeType) String() string {
	switch t {
	case 0:
		return "ConfAddNode"
	case 1:
		return "ConfRemoveNode"
	case 2:
		return "ConfUpdateNode"
	case 3:
		return "ConfAddLearner"
	case 4:
		return "ConfPromoteLearner"
	}
	return "unkown"
}

func (t PeerType) String() string {
	switch t {
	case 0:
		return "PeerNormal"
	case 1:
		return "PeerArbiter"
	}
	return "unkown"
}

func (p Peer) String() string {
	return fmt.Sprintf(`"nodeID":"%v","peerID":"%v","priority":"%v","type":"%v"`,
		p.ID, p.PeerID, p.Priority, p.Type.String())
}

func (cc *ConfChange) String() string {
	return fmt.Sprintf(`{"type":"%v",%v}`, cc.Type, cc.Peer.String())
}

func (m *Message) IsResponseMsg() bool {
	return m.Type == RespMsgAppend || m.Type == RespMsgHeartBeat || m.Type == RespMsgVote ||
		m.Type == RespMsgElectAck || m.Type == RespMsgSnapShot || m.Type == RespCheckQuorum
}

func (m *Message) IsElectionMsg() bool {
	return m.Type == ReqMsgHeartBeat || m.Type == RespMsgHeartBeat || m.Type == ReqMsgVote || m.Type == RespMsgVote ||
		m.Type == ReqMsgElectAck || m.Type == RespMsgElectAck || m.Type == LeaseMsgOffline || m.Type == LeaseMsgTimeout
}

func (m *Message) IsHeartbeatMsg() bool {
	return m.Type == ReqMsgHeartBeat || m.Type == RespMsgHeartBeat
}

func (m *Message) IsAppendMsg() bool {
	switch m.Type {
	case ReqMsgAppend, RespMsgAppend:
		return true
	default:
	}
	return false
}

func (s *HardState) IsEmpty() bool {
	return s.Term == 0 && s.Vote == 0 && s.Commit == 0
}
