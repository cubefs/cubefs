// Copyright 2018 The Containerfs Authors.
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
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	raftproto "github.com/tiglabs/raft/proto"
	"io/ioutil"
	"os"
	"path"
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range")
)

type sortPeers []proto.Peer

func (sp sortPeers) Len() int {
	return len(sp)
}
func (sp sortPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

/* MetRangeConfig used by create metaPartition and serialize
PartitionID: Identity for raftStore group,RaftStore nodes in same raftStore group must have same groupID.
Start: Minimal Inode ID of this range. (Required when initialize)
End: Maximal Inode ID of this range. (Required when initialize)
Cursor: Cursor ID value of Inode what have been already assigned.
Peers: Peers information for raftStore.
*/
type MetaPartitionConfig struct {
	PartitionId uint64              `json:"partition_id"`
	VolName     string              `json:"vol_name"`
	Start       uint64              `json:"start"`
	End         uint64              `json:"end"`
	Peers       []proto.Peer        `json:"peers"`
	Cursor      uint64              `json:"-"`
	NodeId      uint64              `json:"-"`
	RootDir     string              `json:"-"`
	BeforeStart func()              `json:"-"`
	AfterStart  func()              `json:"-"`
	BeforeStop  func()              `json:"-"`
	AfterStop   func()              `json:"-"`
	RaftStore   raftstore.RaftStore `json:"-"`
	ConnPool    *util.ConnectPool   `json:"-"`
}

func (c *MetaPartitionConfig) checkMeta() (err error) {
	if c.PartitionId <= 0 {
		err = errors.Errorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
		return
	}
	if c.Start < 0 {
		err = errors.Errorf("[checkMeta]: start at least 0")
		return
	}
	if c.End <= c.Start {
		err = errors.Errorf("[checkMeta]: end=%v, "+
			"start=%v; end <= start", c.End, c.Start)
		return
	}
	if len(c.Peers) <= 0 {
		err = errors.Errorf("[checkMeta]: must have peers, now peers is 0")
		return
	}
	return
}

func (c *MetaPartitionConfig) sortPeers() {
	sp := sortPeers(c.Peers)
	sort.Sort(sp)
}

type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	DeleteInode(req *DeleteInoReq, p *Packet) (err error)
	InodeGet(req *InodeGetReq, p *Packet) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	Open(req *OpenReq, p *Packet) (err error)
	ReleaseOpen(req *ReleaseReq, p *Packet) (err error)
	CreateLinkInode(req *LinkInodeReq, p *Packet) (err error)
	EvictInode(req *EvictInodeReq, p *Packet) (err error)
	SetAttr(reqData []byte, p *Packet) (err error)
	GetInodeTree() *BTree
}

type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	GetDentryTree() *BTree
}

type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error)
}

type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
	OpPartition
}

type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	GetCursor() uint64
	GetBaseConfig() MetaPartitionConfig
	LoadSnapshotSign(p *Packet) (err error)
	StoreMeta() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	DeletePartition() (err error)
	UpdatePartition(req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
}

type MetaPartition interface {
	Start() error
	Stop()
	OpMeta
}

// metaPartition manages necessary information of meta range, include ID, boundary of range and raftStore identity.
// When a new Inode is requested, metaPartition allocates the Inode id for this Inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config        *MetaPartitionConfig
	isDump        *AtomicBool
	size          uint64 // For partition all file size
	applyID       uint64 // For store Inode/Dentry max applyID, this index will be update after restore from dump data.
	dentryTree    *BTree
	inodeTree     *BTree              // B-Tree for Inode.
	raftPartition raftstore.Partition // RaftStore partition instance of this meta partition.
	stopC         chan bool
	storeChan     chan *storeMsg
	state         uint32
	delInodeFp    *os.File
	freeList      *freeList // Free inode list
	extDelCh      chan BtreeItem
	extReset      chan struct{}
	vol           *Vol
}

func (mp *metaPartition) Start() (err error) {
	if atomic.CompareAndSwapUint32(&mp.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&mp.state, newState)
		}()
		if mp.config.BeforeStart != nil {
			mp.config.BeforeStart()
		}
		if err = mp.onStart(); err != nil {
			err = errors.Errorf("[Start]->%s", err.Error())
			return
		}
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

func (mp *metaPartition) Stop() {
	if atomic.CompareAndSwapUint32(&mp.state, StateRunning, StateShutdown) {
		defer atomic.StoreUint32(&mp.state, StateStopped)
		if mp.config.BeforeStop != nil {
			mp.config.BeforeStop()
		}
		mp.onStop()
		if mp.config.AfterStop != nil {
			mp.config.AfterStop()
			log.LogDebugf("[AfterStop]: partition id=%d execute ok.",
				mp.config.PartitionId)
		}
	}
}

func (mp *metaPartition) onStart() (err error) {
	defer func() {
		if err == nil {
			return
		}
		mp.onStop()
	}()
	if err = mp.load(); err != nil {
		err = errors.Errorf("[onStart]:load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	mp.startSchedule(mp.applyID)
	if err = mp.startFreeList(); err != nil {
		err = errors.Errorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.Errorf("[onStart]start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}

	return
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stop()
	if mp.delInodeFp != nil {
		mp.delInodeFp.Sync()
		mp.delInodeFp.Close()
	}
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicatePort int
		peers         []raftstore.PeerAddress
	)
	if heartbeatPort, replicatePort, err = mp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range mp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicatePort: replicatePort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition id=%d raft peers: %s",
		mp.config.PartitionId, peers)
	pc := &raftstore.PartitionConfig{
		ID:      mp.config.PartitionId,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	mp.raftPartition, err = mp.config.RaftStore.CreatePartition(pc)
	return
}

func (mp *metaPartition) stopRaft() {
	if mp.raftPartition != nil {
		mp.raftPartition.Stop()
	}
	return
}

func (mp *metaPartition) getRaftPort() (heartbeat, replicate int, err error) {
	raftConfig := mp.config.RaftStore.RaftConfig()
	heartbeatAddrParts := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicateAddrParts := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrParts) != 2 {
		err = ErrIllegalHeartbeatAddress
		return
	}
	if len(replicateAddrParts) != 2 {
		err = ErrIllegalReplicateAddress
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrParts[1])
	if err != nil {
		return
	}
	replicate, err = strconv.Atoi(replicateAddrParts[1])
	if err != nil {
		return
	}
	return
}

// NewMetaPartition create and init a new meta partition with specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig) MetaPartition {
	mp := &metaPartition{
		config:     conf,
		isDump:     NewAtomicBool(),
		dentryTree: NewBtree(),
		inodeTree:  NewBtree(),
		stopC:      make(chan bool),
		storeChan:  make(chan *storeMsg, 5),
		freeList:   newFreeList(),
		extDelCh:   make(chan BtreeItem, 10000),
		extReset:   make(chan struct{}),
		vol:        NewVol(),
	}
	return mp
}

func (mp *metaPartition) IsLeader() (leaderAddr string, ok bool) {
	if mp.raftPartition == nil {
		return
	}
	leaderID, _ := mp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == mp.config.NodeId
	for _, peer := range mp.config.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}
	return
}

func (mp *metaPartition) GetCursor() uint64 {
	return mp.config.Cursor
}

func (mp *metaPartition) StoreMeta() (err error) {
	mp.config.sortPeers()
	err = mp.storeMeta()
	return
}

// Load used when metaNode start and recover data from snapshot
func (mp *metaPartition) load() (err error) {
	if err = mp.loadMeta(); err != nil {
		return
	}
	loadSnapshotDir := path.Join(mp.config.RootDir, snapShotDir)
	if err = mp.loadInode(loadSnapshotDir); err != nil {
		return
	}
	if err = mp.loadDentry(loadSnapshotDir); err != nil {
		return
	}
	err = mp.loadApplyID(loadSnapshotDir)
	return
}

func (mp *metaPartition) store(sm *storeMsg) (err error) {
	tmpDir := path.Join(mp.config.RootDir, snapShotDirTmp)
	if _, err = os.Stat(tmpDir); err == nil {
		os.RemoveAll(tmpDir)
	}
	err = nil
	if err = os.MkdirAll(tmpDir, 0775); err != nil {
		return
	}

	defer func() {
		if err != nil {
			os.RemoveAll(tmpDir)
		}
	}()
	var (
		inoCRC, denCRC uint32
	)
	if inoCRC, err = mp.storeInode(tmpDir, sm); err != nil {
		return
	}
	if denCRC, err = mp.storeDentry(tmpDir, sm); err != nil {
		return
	}
	if err = mp.storeApplyID(tmpDir, sm); err != nil {
		return
	}
	// Write crc to file
	if err = ioutil.WriteFile(path.Join(tmpDir, SnapshotSign),
		[]byte(fmt.Sprintf("%d %d", inoCRC, denCRC)), 0775); err != nil {
		return
	}
	snapshotDir := path.Join(mp.config.RootDir, snapShotDir)
	// check snapshotBack if or not exist
	backupDir := path.Join(mp.config.RootDir, snapShotBackup)
	if _, err = os.Stat(backupDir); err == nil {
		if err = os.RemoveAll(backupDir); err != nil {
			return
		}
	}
	err = nil

	// rename snapshot to backup
	if _, err = os.Stat(snapshotDir); err == nil {
		if err = os.Rename(snapshotDir, backupDir); err != nil {
			return
		}
	}
	err = nil

	// rename snapshotTmp to snapshot
	if err = os.Rename(tmpDir, snapshotDir); err != nil {
		os.Rename(backupDir, snapshotDir)
		return
	}
	err = os.RemoveAll(backupDir)
	return
}

// UpdatePeers
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

func (mp *metaPartition) DeleteRaft() (err error) {
	err = mp.raftPartition.Delete()
	return
}

// NextInodeId returns a new ID value of Inode and update offset.
// If Inode ID is out of this metaPartition limit then return ErrInodeOutOfRange error.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := atomic.LoadUint64(&mp.config.Cursor)
		end := mp.config.End
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
	}
}

func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

func (mp *metaPartition) DeletePartition() (err error) {
	_, err = mp.Put(opDeletePartition, nil)
	return
}

func (mp *metaPartition) UpdatePartition(req *UpdatePartitionReq,
	resp *UpdatePartitionResp) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		return
	}
	r, err := mp.Put(opUpdatePartition, reqData)
	if err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		resp.Status = proto.TaskFail
		p := &Packet{}
		p.ResultCode = status
		err = errors.Errorf("[UpdatePartition]: %s", p.GetResultMesg())
		resp.Result = p.GetResultMesg()
	}
	resp.Status = proto.TaskSuccess
	return
}

func (mp *metaPartition) OfflinePartition(req []byte) (err error) {
	_, err = mp.Put(opOfflinePartition, req)
	return
}

func (mp *metaPartition) LoadSnapshotSign(p *Packet) (err error) {
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: mp.config.PartitionId,
		DoCompare:   true,
	}
	resp.ApplyID, resp.InodeSign, resp.DentrySign, err = mp.loadSnapshotSign(
		snapShotDir)
	if err != nil {
		if !os.IsNotExist(err) {
			err = errors.Annotate(err, "[LoadSnapshotSign] 1st check snapshot")
			p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp.ApplyID, resp.InodeSign, resp.DentrySign, err = mp.loadSnapshotSign(
			snapShotBackup)
	}
	if err != nil {
		if !os.IsNotExist(err) {
			err = errors.Annotate(err,
				"[LoadSnapshotSign] 2st check snapshot")
			p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		resp.DoCompare = false
		log.LogWarnf("[LoadSnapshotSign] check snapshot not exist")
	}
	data, err := json.Marshal(resp)
	if err != nil {
		err = errors.Annotate(err, "[LoadSnapshotSign] marshal")
		return
	}
	p.PackOkWithBody(data)
	return
}

func (mp *metaPartition) loadSnapshotSign(baseDir string) (applyID uint64,
	inoCRC, dentryCRC uint32, err error) {
	snapDir := path.Join(mp.config.RootDir, baseDir)
	// check snapshot
	if _, err = os.Stat(snapDir); err != nil {
		return
	}
	// load sign
	data, err := ioutil.ReadFile(path.Join(snapDir, SnapshotSign))
	if err != nil {
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d %d", &inoCRC, &dentryCRC); err != nil {
		return
	}
	// load apply
	if data, err = ioutil.ReadFile(path.Join(snapDir, applyIDFile)); err != nil {
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &applyID); err != nil {
		return
	}
	return
}

// Marshal json marshal interface
func (mp *metaPartition) MarshalJSON() ([]byte, error) {
	return json.Marshal(mp.config)
}

func (mp *metaPartition) Reset() (err error) {
	mp.inodeTree.Reset()
	mp.dentryTree.Reset()
	mp.config.Cursor = 0
	mp.applyID = 0
	// delete ino/dentry applyID file
	mp.deleteApplyFile()
	mp.deleteDentryFile()
	mp.deleteInodeFile()
	return
}
