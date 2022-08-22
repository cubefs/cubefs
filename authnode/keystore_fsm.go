// Copyright 2018 The CubeFS Authors.
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

package authnode

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/keystore"
	"github.com/cubefs/cubefs/util/log"
)

const (
	applied = "applied"
)

type raftLeaderChangeHandler func(leader uint64)

type raftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type raftCmdApplyHandler func(cmd *RaftCmd) (err error)

type raftApplySnapshotHandler func()

// KeystoreFsm represents the finite state machine of a keystore
type KeystoreFsm struct {
	store               *raftstore.RocksDBStore
	rs                  *raft.RaftServer
	applied             uint64
	retainLogs          uint64
	leaderChangeHandler raftLeaderChangeHandler
	peerChangeHandler   raftPeerChangeHandler
	snapshotHandler     raftApplySnapshotHandler

	keystore       map[string]*keystore.KeyInfo
	accessKeystore map[string]*keystore.AccessKeyInfo
	ksMutex        sync.RWMutex // keystore mutex
	aksMutex       sync.RWMutex //accesskeystore mutex
	opKeyMutex     sync.RWMutex // operations on key mutex
	id             uint64       // current id of server
}

func newKeystoreFsm(store *raftstore.RocksDBStore, retainsLog uint64, rs *raft.RaftServer) (fsm *KeystoreFsm) {
	fsm = new(KeystoreFsm)
	fsm.store = store
	fsm.rs = rs
	fsm.retainLogs = retainsLog
	return
}

// Corresponding to the LeaderChange interface in Raft library.
func (mf *KeystoreFsm) registerLeaderChangeHandler(handler raftLeaderChangeHandler) {
	mf.leaderChangeHandler = handler
}

// Corresponding to the PeerChange interface in Raft library.
func (mf *KeystoreFsm) registerPeerChangeHandler(handler raftPeerChangeHandler) {
	mf.peerChangeHandler = handler
}

// Corresponding to the ApplySnapshot interface in Raft library.
func (mf *KeystoreFsm) registerApplySnapshotHandler(handler raftApplySnapshotHandler) {
	mf.snapshotHandler = handler
}

func (mf *KeystoreFsm) restore() {
	mf.restoreApplied()
}

func (mf *KeystoreFsm) restoreApplied() {
	value, err := mf.Get(applied)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}
	byteValues := value.([]byte)
	if len(byteValues) == 0 {
		mf.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(byteValues), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	mf.applied = applied
}

// Apply implements the interface of raft.StateMachine
func (mf *KeystoreFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	var (
		keyInfo keystore.KeyInfo
		leader  uint64
	)

	cmd := new(RaftCmd)
	if err = cmd.Unmarshal(command); err != nil {
		log.LogErrorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		panic(err)
	}
	log.LogInfof("action[fsmApply],cmd.op[%v],cmd.K[%v],cmd.V[%v]", cmd.Op, cmd.K, string(cmd.V))

	if err = json.Unmarshal(cmd.V, &keyInfo); err != nil {
		panic(err)
	}

	s := strings.Split(cmd.K, idSeparator)
	if len(s) != 2 {
		panic(fmt.Errorf("cmd.K format error %s", cmd.K))
	}
	leader, err = strconv.ParseUint(s[1], 10, 64)
	if err != nil {
		panic(fmt.Errorf("leaderID format error %s", s[1]))
	}

	cmdMap := make(map[string][]byte)
	cmdMap[s[0]] = cmd.V
	cmdMap[applied] = []byte(strconv.FormatUint(uint64(index), 10))

	switch cmd.Op {
	case opSyncDeleteKey:
		if err = mf.delKeyAndPutIndex(cmd.K, cmdMap); err != nil {
			panic(err)
		}
		//if mf.leader != mf.id {
		// We don't use above statement to avoid "leader double-change of keystore cache"
		// Because there may a race condition: before "Apply" raftlog, leader change happens
		// so that cache changes may not happen in newly selected leader-node and "double-change"
		// of cache may happen in newly demoted leader node. Therefore, we use the following
		// statement: "id" indicates which server has changed keystore cache (typical leader).
		if mf.id != leader {
			mf.DeleteKey(keyInfo.ID)
			mf.DeleteAKInfo(keyInfo.AccessKey)
			log.LogInfof("action[Apply], Successfully delete key in node[%d]", mf.id)
		} else {
			log.LogInfof("action[Apply], Already delete key in node[%d]", mf.id)
		}
	default:
		if err = mf.batchPut(cmdMap); err != nil {
			panic(err)
		}
		//if mf.leader != mf.id {
		// Same reasons as the description above
		if mf.id != leader {
			mf.PutKey(&keyInfo)
			accessKeyInfo := &keystore.AccessKeyInfo{
				AccessKey: keyInfo.AccessKey,
				ID:        keyInfo.ID,
			}
			mf.PutAKInfo(accessKeyInfo)
			log.LogInfof("action[Apply], Successfully put key in node[%d]", mf.id)
		} else {
			log.LogInfof("action[Apply], Already put key in node[%d]", mf.id)
		}
	}
	mf.applied = index
	if mf.applied > 0 && (mf.applied%mf.retainLogs) == 0 {
		log.LogWarnf("action[Apply],truncate raft log,retainLogs[%v],index[%v]", mf.retainLogs, mf.applied)
		mf.rs.Truncate(GroupID, mf.applied)
	}
	return
}

// ApplyMemberChange implements the interface of raft.StateMachine
func (mf *KeystoreFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

// Snapshot implements the interface of raft.StateMachine
func (mf *KeystoreFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := mf.store.RocksDBSnapshot()
	iterator := mf.store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &KeystoreSnapshot{
		applied:  mf.applied,
		snapshot: snapshot,
		fsm:      mf,
		iterator: iterator,
	}, nil
}

// ApplySnapshot implements the interface of raft.StateMachine
func (mf *KeystoreFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] begin,applied[%v]", mf.applied))
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			break
		}
		cmd := &RaftCmd{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errHandler
		}
		if _, err = mf.store.Put(cmd.K, cmd.V, true); err != nil {
			goto errHandler
		}
	}
	if err != nil && err != io.EOF {
		goto errHandler
	}
	mf.snapshotHandler()
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] success,applied[%v]", mf.applied))
	return nil
errHandler:
	log.LogError(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
}

// HandleFatalEvent implements the interface of raft.StateMachine
func (mf *KeystoreFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

// HandleLeaderChange implements the interface of raft.StateMachine
func (mf *KeystoreFsm) HandleLeaderChange(leader uint64) {
	if mf.leaderChangeHandler != nil {
		go mf.leaderChangeHandler(leader)
	}
}

// Put implements the interface of raft.StateMachine
func (mf *KeystoreFsm) Put(key, val interface{}) (interface{}, error) {
	return mf.store.Put(key, val, true)
}

func (mf *KeystoreFsm) batchPut(cmdMap map[string][]byte) (err error) {
	return mf.store.BatchPut(cmdMap, true)
}

// Get implements the interface of raft.StateMachine
func (mf *KeystoreFsm) Get(key interface{}) (interface{}, error) {
	return mf.store.Get(key)
}

// Del implements the interface of raft.StateMachine
func (mf *KeystoreFsm) Del(key interface{}) (interface{}, error) {
	return mf.store.Del(key, true)
}

func (mf *KeystoreFsm) delKeyAndPutIndex(key string, cmdMap map[string][]byte) (err error) {
	return mf.store.DeleteKeyAndPutIndex(key, cmdMap, true)
}
