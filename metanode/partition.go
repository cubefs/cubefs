// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
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
	ErrInodeIDOutOfRange = errors.New("inode ID out of range")
)

type sortedPeers []proto.Peer

func (sp sortedPeers) Len() int {
	return len(sp)
}
func (sp sortedPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortedPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

// MetaPartitionConfig is used to create a meta partition.
type MetaPartitionConfig struct {
	// Identity for raftStore group. RaftStore nodes in the same raftStore group must have the same groupID.
	PartitionId uint64              `json:"partition_id"`
	VolName     string              `json:"vol_name"`
	Start       uint64              `json:"start"` // Minimal Inode ID of this range. (Required during initialization)
	End         uint64              `json:"end"`   // Maximal Inode ID of this range. (Required during initialization)
	Peers       []proto.Peer        `json:"peers"` // Peers information of the raftStore
	Cursor      uint64              `json:"-"`     // Cursor ID of the inode that have been assigned
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
		err = errors.NewErrorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
		return
	}
	if c.Start < 0 {
		err = errors.NewErrorf("[checkMeta]: start at least 0")
		return
	}
	if c.End <= c.Start {
		err = errors.NewErrorf("[checkMeta]: end=%v, "+
			"start=%v; end <= start", c.End, c.Start)
		return
	}
	if len(c.Peers) <= 0 {
		err = errors.NewErrorf("[checkMeta]: must have peers, now peers is 0")
		return
	}
	return
}

func (c *MetaPartitionConfig) sortPeers() {
	sp := sortedPeers(c.Peers)
	sort.Sort(sp)
}

// OpInode defines the interface for the inode operations.
type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	UnlinkInode(req *UnlinkInoReq, p *Packet) (err error)
	InodeGet(req *InodeGetReq, p *Packet) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	CreateInodeLink(req *LinkInodeReq, p *Packet) (err error)
	EvictInode(req *EvictInodeReq, p *Packet) (err error)
	SetAttr(reqData []byte, p *Packet) (err error)
	GetInodeTree() *BTree
}

// OpDentry defines the interface for the dentry operations.
type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	GetDentryTree() *BTree
}

// OpExtent defines the interface for the extent operations.
type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error)
}

// OpMeta defines the interface for the metadata operations.
type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
	OpPartition
}

// OpPartition defines the interface for the partition operations.
type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	GetCursor() uint64
	GetBaseConfig() MetaPartitionConfig
	LoadSnapshotSign(p *Packet) (err error)
	PersistMetadata() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	DeletePartition() (err error)
	UpdatePartition(req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
}

// MetaPartition defines the interface for the meta partition operations.
type MetaPartition interface {
	Start() error
	Stop()
	OpMeta
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config        *MetaPartitionConfig
	size          uint64 // For partition all file size
	applyID       uint64 // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	dentryTree    *BTree
	inodeTree     *BTree // btree for inodes
	raftPartition raftstore.Partition
	stopC         chan bool
	storeChan     chan *storeMsg
	state         uint32
	delInodeFp    *os.File
	freeList      *freeList // free inode list
	extDelCh      chan BtreeItem
	extReset      chan struct{}
	vol           *Vol
}

// Start starts a meta partition.
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
			err = errors.NewErrorf("[Start]->%s", err.Error())
			return
		}
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

// Stop stops a meta partition.
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
		err = errors.NewErrorf("[onStart]:load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	mp.startSchedule(mp.applyID)
	if err = mp.startFreeList(); err != nil {
		err = errors.NewErrorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.NewErrorf("[onStart]start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}

	return
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stop()
	if mp.delInodeFp != nil {
		// TODO Unhandled errors
		mp.delInodeFp.Sync()
		mp.delInodeFp.Close()
	}
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
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
			ReplicaPort:   replicaPort,
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
		// TODO Unhandled errors
		mp.raftPartition.Stop()
		mp.raftPartition = nil
	}
	return
}

func (mp *metaPartition) getRaftPort() (heartbeat, replica int, err error) {
	raftConfig := mp.config.RaftStore.RaftConfig()
	heartbeatAddrSplits := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicaAddrSplits := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrSplits) != 2 {
		err = ErrIllegalHeartbeatAddress
		return
	}
	if len(replicaAddrSplits) != 2 {
		err = ErrIllegalReplicateAddress
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrSplits[1])
	if err != nil {
		return
	}
	replica, err = strconv.Atoi(replicaAddrSplits[1])
	if err != nil {
		return
	}
	return
}

// NewMetaPartition creates a new meta partition with the specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig) MetaPartition {
	mp := &metaPartition{
		config:     conf,
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

// IsLeader returns the raft leader address and if the current meta partition is the leader.
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

func (mp *metaPartition) GetPeers() (peers []string) {
	peers = make([]string, 0)
	for _, peer := range mp.config.Peers {
		if mp.config.NodeId == peer.ID {
			continue
		}
		peers = append(peers, peer.Addr)
	}
	return
}

// GetCursor returns the cursor stored in the config.
func (mp *metaPartition) GetCursor() uint64 {
	return mp.config.Cursor
}

// PersistMetadata is the wrapper of persistMetadata.
func (mp *metaPartition) PersistMetadata() (err error) {
	mp.config.sortPeers()
	err = mp.persistMetadata()
	return
}

func (mp *metaPartition) load() (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}
	loadSnapshotDir := path.Join(mp.config.RootDir, snapshotDir)
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
	tmpDir := path.Join(mp.config.RootDir, snapshotDirTmp)
	if _, err = os.Stat(tmpDir); err == nil {
		// TODO Unhandled errors
		os.RemoveAll(tmpDir)
	}
	err = nil
	if err = os.MkdirAll(tmpDir, 0775); err != nil {
		return
	}

	defer func() {
		if err != nil {
			// TODO Unhandled errors
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
	// write crc to file
	if err = ioutil.WriteFile(path.Join(tmpDir, SnapshotSign),
		[]byte(fmt.Sprintf("%d %d", inoCRC, denCRC)), 0775); err != nil {
		return
	}
	snapshotDir := path.Join(mp.config.RootDir, snapshotDir)
	// check snapshot backup
	backupDir := path.Join(mp.config.RootDir, snapshotBackup)
	if _, err = os.Stat(backupDir); err == nil {
		if err = os.RemoveAll(backupDir); err != nil {
			return
		}
	}
	err = nil

	// rename snapshot
	if _, err = os.Stat(snapshotDir); err == nil {
		if err = os.Rename(snapshotDir, backupDir); err != nil {
			return
		}
	}
	err = nil

	if err = os.Rename(tmpDir, snapshotDir); err != nil {
		os.Rename(backupDir, snapshotDir)
		return
	}
	err = os.RemoveAll(backupDir)
	return
}

// UpdatePeers updates the peers.
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

// DeleteRaft deletes the raft partition.
func (mp *metaPartition) DeleteRaft() (err error) {
	err = mp.raftPartition.Delete()
	return
}

// Return a new inode ID and update the offset.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := atomic.LoadUint64(&mp.config.Cursor)
		end := mp.config.End
		if cur >= end {
			return 0, ErrInodeIDOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
	}
}

// ChangeMember changes the raft member with the specified one.
func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

// GetBaseConfig returns the configuration stored in the meta partition. TODO remove? no usage?
func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

// DeletePartition deletes the meta partition. TODO remove? no usage?
func (mp *metaPartition) DeletePartition() (err error) {
	_, err = mp.Put(opFSMDeletePartition, nil)
	return
}

// UpdatePartition updates the meta partition. TODO remove? no usage?
func (mp *metaPartition) UpdatePartition(req *UpdatePartitionReq,
	resp *UpdatePartitionResp) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	r, err := mp.Put(opFSMUpdatePartition, reqData)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		resp.Status = proto.TaskFailed
		p := &Packet{}
		p.ResultCode = status
		err = errors.NewErrorf("[UpdatePartition]: %s", p.GetResultMsg())
		resp.Result = p.GetResultMsg()
	}
	resp.Status = proto.TaskSucceeds
	return
}

func (mp *metaPartition) DecommissionPartition(req []byte) (err error) {
	_, err = mp.Put(opFSMDecommissionPartition, req)
	return
}

// LoadSnapshotSign loads the snapshot signature. TODO remove? no usage?
func (mp *metaPartition) LoadSnapshotSign(p *Packet) (err error) {
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: mp.config.PartitionId,
		DoCompare:   true,
	}
	resp.ApplyID, resp.InodeSign, resp.DentrySign, err = mp.loadSnapshotSign(
		snapshotDir)
	if err != nil {
		if !os.IsNotExist(err) {
			err = errors.Trace(err, "[LoadSnapshotSign] 1st check snapshot")
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp.ApplyID, resp.InodeSign, resp.DentrySign, err = mp.loadSnapshotSign(
			snapshotBackup)
	}
	if err != nil {
		if !os.IsNotExist(err) {
			err = errors.Trace(err,
				"[LoadSnapshotSign] 2st check snapshot")
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		resp.DoCompare = false
		log.LogWarnf("[LoadSnapshotSign] check snapshot not exist")
	}
	data, err := json.Marshal(resp)
	if err != nil {
		err = errors.Trace(err, "[LoadSnapshotSign] marshal")
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (mp *metaPartition) loadSnapshotSign(baseDir string) (applyID uint64,
	inoCRC, dentryCRC uint32, err error) {
	snapDir := path.Join(mp.config.RootDir, baseDir)
	// check snapshot
	if _, err = os.Stat(snapDir); err != nil {
		return
	}
	// load signature
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

// MarshalJSON is the wrapper of json.Marshal.
func (mp *metaPartition) MarshalJSON() ([]byte, error) {
	return json.Marshal(mp.config)
}

// TODO remove? no usage?
// Reset resets the meta partition.
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
