// Copyright 2022 The CubeFS Authors.
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

package base

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type RaftNodeConfig struct {
	FlushNumInterval    uint64       `json:"flush_num_interval"`
	FlushTimeIntervalS  int          `json:"flush_time_interval_s"`
	TruncateNumInterval uint64       `json:"truncate_num_interval"`
	NodeProtocol        string       `json:"node_protocol"`
	Members             []RaftMember `json:"members"`
	ApplyFlush          bool         `json:"apply_flush"`

	ApplyIndex uint64 `json:"-"`
}

type RaftMembers struct {
	Mbs []RaftMember `json:"members"`
}

type RaftMember struct {
	ID       uint64 `json:"id"`
	Host     string `json:"host"`
	Learner  bool   `json:"learner"`
	NodeHost string `json:"node_host"`
}

type RaftNode struct {
	leaderHost string
	// currentApplyIndex is the memory current apply index, it's not stable
	currentApplyIndex uint64
	// stableApplyIndex is the persistent apply index and it's stable
	stableApplyIndex uint64
	// truncateApplyIndex record last truncated apply index
	truncateApplyIndex uint64
	// openSnapshotsNum record opening snapshot num currently
	openSnapshotsNum int32
	// appliers record all registered RaftApplier
	appliers []RaftApplier
	nodes    map[uint64]string

	lock        sync.RWMutex
	closeCh     chan interface{}
	raftDB      *raftdb.RaftDB
	snapshotDBs map[string]SnapshotDB

	raftserver.RaftServer
	*RaftNodeConfig
}

func NewRaftNode(cfg *RaftNodeConfig, raftDB *raftdb.RaftDB, snapshotDBs map[string]SnapshotDB) (*RaftNode, error) {
	if cfg.FlushNumInterval == 0 {
		cfg.FlushNumInterval = defaultFlushNumInterval
	}
	if cfg.FlushTimeIntervalS == 0 {
		cfg.FlushTimeIntervalS = defaultFlushTimeIntervalS
	}
	if cfg.TruncateNumInterval == 0 {
		cfg.TruncateNumInterval = defaultTruncateNumInterval
	}

	raftNode := &RaftNode{
		snapshotDBs:    snapshotDBs,
		raftDB:         raftDB,
		RaftNodeConfig: cfg,

		currentApplyIndex: cfg.ApplyIndex,
		stableApplyIndex:  cfg.ApplyIndex,
		// set truncateApplyIndex into last apply index - truncate num interval
		// it may not equal to the actual value, but it'll be fix by next truncation
		truncateApplyIndex: cfg.ApplyIndex - cfg.TruncateNumInterval,

		closeCh: make(chan interface{}),

		nodes: make(map[uint64]string),
	}

	members, err := raftNode.GetRaftMembers(context.Background())
	if err != nil {
		return nil, err
	}

	needWrite := false
	if len(members) == 0 {
		needWrite = true
		members = raftNode.RaftNodeConfig.Members
	}

	for i := 0; i < len(members); i++ {
		// Configuration compatible with older versions
		member := &members[i]
		if member.NodeHost == "" {
			needWrite = true
			for _, cm := range raftNode.RaftNodeConfig.Members {
				if member.ID == cm.ID && cm.Host == member.Host {
					member.NodeHost = cm.NodeHost
					raftNode.nodes[member.ID] = cm.NodeHost
					break
				}
			}
			if member.NodeHost == "" {
				return nil, errors.New("Configuration is not compatible")
			}
		}
		raftNode.nodes[member.ID] = member.NodeHost
	}

	if needWrite {
		mbrs := &RaftMembers{}
		mbrs.Mbs = append(mbrs.Mbs, members...)
		val, err := json.Marshal(mbrs)
		if err != nil {
			return nil, err
		}
		if err = raftDB.Put(RaftMemberKey, val); err != nil {
			return nil, err
		}
	}

	return raftNode, nil
}

func (r *RaftNode) SetRaftServer(raftServer raftserver.RaftServer) {
	r.RaftServer = raftServer
}

// registRaftApplier use reflect to find out all RaftApplier and register
func (r *RaftNode) RegistRaftApplier(target interface{}) {
	// reflect all mgr's method, get the Applies and regist
	applies := make([]RaftApplier, 0)
	iface := reflect.TypeOf(new(RaftApplier)).Elem()
	vals := reflect.ValueOf(target).Elem()
	typs := vals.Type()
	for i := 0; i < vals.NumField(); i++ {
		field := typs.Field(i)
		if field.Type.Implements(iface) {
			applier := vals.Field(i).Interface().(RaftApplier)
			// set module name by reflect, no necessary to do it by self
			applier.SetModuleName(field.Name)
			applies = append(applies, applier)
		}
	}
	r.appliers = applies
}

func (r *RaftNode) GetStableApplyIndex() uint64 {
	return atomic.LoadUint64(&r.stableApplyIndex)
}

func (r *RaftNode) GetCurrentApplyIndex() uint64 {
	return atomic.LoadUint64(&r.currentApplyIndex)
}

func (r *RaftNode) GetNodes() map[uint64]string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	nodes := make(map[uint64]string, len(r.nodes))
	for key, value := range r.nodes {
		nodes[key] = value
	}
	return nodes
}

func (r *RaftNode) RecordApplyIndex(ctx context.Context, index uint64, isFlush bool) (err error) {
	old := atomic.LoadUint64(&r.currentApplyIndex)
	if old < index {
		for {
			// update success, break
			if isSwap := atomic.CompareAndSwapUint64(&r.currentApplyIndex, old, index); isSwap {
				break
			}
			// already update, break
			old = atomic.LoadUint64(&r.currentApplyIndex)
			if old >= index {
				break
			}
			// otherwise, retry cas
		}
	}

	// no flush model, just record apply index into currentApplyIndex
	if !isFlush {
		return nil
	}
	err = r.flushAll(ctx)
	if err != nil {
		return
	}

	return r.saveStableApplyIndex(index)
}

func (r *RaftNode) GetRaftMembers(ctx context.Context) ([]RaftMember, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	val, err := r.raftDB.Get(RaftMemberKey)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}

	mbrs := &RaftMembers{}
	err = json.Unmarshal(val, mbrs)
	if err != nil {
		return nil, err
	}

	return mbrs.Mbs, nil
}

func (r *RaftNode) RecordRaftMember(ctx context.Context, member RaftMember, isDelete bool) error {
	if isDelete {
		return r.delRaftMember(ctx, member.ID)
	}
	return r.addRaftMember(ctx, member)
}

func (r *RaftNode) addRaftMember(ctx context.Context, member RaftMember) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	val, err := r.raftDB.Get(RaftMemberKey)
	if err != nil {
		return err
	}
	mbrs := &RaftMembers{}
	if len(val) != 0 {
		err = json.Unmarshal(val, mbrs)
		if err != nil {
			return err
		}
	}

	for i := range mbrs.Mbs {
		if mbrs.Mbs[i].ID == member.ID {
			mbrs.Mbs[i].Host = member.Host
			mbrs.Mbs[i].Learner = member.Learner
			goto SAVE
		}
	}
	r.nodes[member.ID] = member.NodeHost
	mbrs.Mbs = append(mbrs.Mbs, member)

SAVE:
	val, err = json.Marshal(mbrs)
	if err != nil {
		return err
	}
	return r.raftDB.Put(RaftMemberKey, val)
}

func (r *RaftNode) delRaftMember(ctx context.Context, nodeID uint64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	val, err := r.raftDB.Get(RaftMemberKey)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return nil
	}

	mbrs := &RaftMembers{}
	err = json.Unmarshal(val, mbrs)
	if err != nil {
		return err
	}
	after := make([]RaftMember, 0)
	for i := range mbrs.Mbs {
		if mbrs.Mbs[i].ID != nodeID {
			after = append(after, mbrs.Mbs[i])
		}
	}
	mbrs.Mbs = after
	val, err = json.Marshal(mbrs)
	if err != nil {
		return err
	}
	delete(r.nodes, nodeID)
	return r.raftDB.Put(RaftMemberKey, val)
}

func (r *RaftNode) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
	wg := sync.WaitGroup{}
	for i := range r.appliers {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			r.appliers[idx].NotifyLeaderChange(ctx, leader, host)
		}()
	}
	wg.Wait()
}

func (r *RaftNode) ModuleApply(ctx context.Context, module string, operTypes []int32, datas [][]byte, contexts []ProposeContext) error {
	moduleApplies := r.getApplierByModule(module)
	if moduleApplies == nil {
		return errors.New("raft node can't found applies in map")
	}
	start := time.Now()
	span := trace.SpanFromContextSafe(ctx)
	err := moduleApplies.Apply(ctx, operTypes, datas, contexts)
	span.Debugf("module:%s, types:%v, contexts:%v, apply cost time:%dus", module, operTypes, contexts, time.Since(start)/time.Microsecond)
	if err != nil {
		return errors.Info(err, "raft statemachine Apply call module method failed").Detail(err)
	}
	return nil
}

func (r *RaftNode) GetLeaderHost() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.leaderHost
}

func (r *RaftNode) SetLeaderHost(idx uint64, host string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.leaderHost = r.nodes[idx]
}

func (r *RaftNode) CreateRaftSnapshot(patchNum int) raftserver.Snapshot {
	applyIndex := r.GetStableApplyIndex()
	items := make([]SnapshotItem, 0)
	dbs := r.snapshotDBs
	for dbName := range dbs {
		cfs := dbs[dbName].GetAllCfNames()
		if len(cfs) == 0 {
			snap := dbs[dbName].NewSnapshot()
			iter := dbs[dbName].NewIterator(snap)
			iter.SeekToFirst()
			items = append(items, SnapshotItem{DbName: dbName, snap: snap, iter: iter})
			continue
		}
		for i := range cfs {
			snap := dbs[dbName].Table(cfs[i]).NewSnapshot()
			iter := dbs[dbName].Table(cfs[i]).NewIterator(snap)
			iter.SeekToFirst()
			items = append(items, SnapshotItem{DbName: dbName, CfName: cfs[i], snap: snap, iter: iter})
		}
	}
	// atomic add openSnapshotsNum
	atomic.AddInt32(&r.openSnapshotsNum, 1)

	return &raftSnapshot{
		name:          "snapshot-" + strconv.FormatInt(time.Now().Unix(), 10) + strconv.Itoa(rand.Intn(100000)),
		items:         items,
		dbs:           dbs,
		applyIndex:    applyIndex,
		patchNum:      patchNum,
		closeCallback: r.closeSnapshotCallback,
	}
}

// ApplyRaftSnapshot apply snapshot's data into db
func (r *RaftNode) ApplyRaftSnapshot(ctx context.Context, st raftserver.Snapshot) error {
	var (
		err   error
		data  []byte
		count uint64
		dbs   = r.snapshotDBs
	)
	span := trace.SpanFromContextSafe(ctx)

	for data, err = st.Read(); err == nil; data, err = st.Read() {
		reader := bytes.NewBuffer(data)
		for {
			snapData, err := DecodeSnapshotData(reader)
			if err != nil {
				if err == io.EOF {
					break
				}
				span.Errorf("ApplyRaftSnapshot decode snapshot data failed, src snapshot data: %v, err: %v", data, err)
				return err
			}
			count++
			dbName := snapData.Header.DbName
			cfName := snapData.Header.CfName

			if snapData.Header.CfName != "" {
				err = dbs[dbName].Table(cfName).Put(kvstore.KV{Key: snapData.Key, Value: snapData.Value})
			} else {
				err = dbs[dbName].Put(kvstore.KV{Key: snapData.Key, Value: snapData.Value})
			}
			if err != nil {
				span.Errorf("ApplyRaftSnapshot put snapshot data failed, snapshot data: %v, err: %v", snapData, err)
				return err
			}
		}
	}
	span.Infof("apply snapshot read data count: %d", count)
	if err != io.EOF {
		span.Errorf("ApplyRaftSnapshot read unexpected error, err: %v", err)
		return err
	}
	// applier LoadData callback
	for _, applier := range r.appliers {
		if err := applier.LoadData(ctx); err != nil {
			span.Errorf("applier[%s] load data failed, err: %s", applier.GetModuleName(), err.Error())
			return err
		}
	}

	return nil
}

func (r *RaftNode) Start() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "raft-node-loop")
	ticker := time.NewTicker(time.Duration(defaultFlushCheckIntervalS) * time.Second)
	lastFlushTime := time.Now()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			current := atomic.LoadUint64(&r.currentApplyIndex)
			stable := r.GetStableApplyIndex()
			if current > stable+r.FlushNumInterval || time.Since(lastFlushTime) > time.Duration(r.FlushTimeIntervalS)*time.Second {
				err := r.flushAll(ctx)
				raftApplierFlushCost := time.Since(now).Milliseconds()
				now = time.Now()
				lastFlushTime = time.Now()
				if err != nil {
					span.Error("raft node loop flush all failed, err: ", err)
					break
				}
				// flush dbs when apply flush switch on
				if r.ApplyFlush {
					for name, db := range r.snapshotDBs {
						if err := db.Flush(); err != nil {
							panic(fmt.Sprintf("flush db[%s] failed: %s", name, err.Error()))
						}
					}
				}
				flushDBCost := time.Since(now).Milliseconds()
				now = time.Now()
				// it doesn't matter whether raft db's apply index is flush or not, as user's data has been flushed safely
				err = r.saveStableApplyIndex(current)
				if err != nil {
					span.Error("raft node loop save stable apply index failed, err: ", err)
					break
				}
				saveRaftApplyIndexCost := time.Since(now).Milliseconds()
				now = time.Now()
				truncateCost := int64(0)
				// also, we can try to truncate wal log after stable apply index update
				if stable-r.truncateApplyIndex > r.TruncateNumInterval*2 && atomic.LoadInt32(&r.openSnapshotsNum) == 0 {
					truncatedIndex := stable - r.TruncateNumInterval
					err = r.Truncate(truncatedIndex)
					if err != nil {
						span.Errorf("raft node truncate wal log failed, stable[%d], truncate[%d], err: %s", stable, r.truncateApplyIndex, err.Error())
						break
					}
					r.truncateApplyIndex = truncatedIndex
					truncateCost = time.Since(now).Milliseconds()
				}
				span.Infof("raft node flush all raft applier cost: %d ms, flush all dbs cost: %d ms, "+
					"save apply index cost: %d ms, truncate log cost: %d ms", raftApplierFlushCost, flushDBCost, saveRaftApplyIndexCost, truncateCost)
			}
		case <-r.closeCh:
			ctx.Done()
			return
		}
	}
}

func (r *RaftNode) Stop() {
	// stop background flush checker
	close(r.closeCh)
	time.Sleep(1 * time.Second)
	// stop raft server
	r.RaftServer.Stop()
	r.raftDB.Close()
}

func (r *RaftNode) saveStableApplyIndex(new uint64) error {
	old := atomic.LoadUint64(&r.stableApplyIndex)
	if old >= new {
		return nil
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// double check
	old = atomic.LoadUint64(&r.stableApplyIndex)
	if old >= new {
		return nil
	}

	indexValue := make([]byte, 8)
	binary.BigEndian.PutUint64(indexValue, new)

	err := r.raftDB.Put(ApplyIndexKey, indexValue)
	if err != nil {
		return errors.Info(err, "put flush apply index failed").Detail(err)
	}
	atomic.StoreUint64(&r.stableApplyIndex, new)

	return nil
}

// FlushAll will call all applier's flush method and record flush_apply_index into persistent storage
func (r *RaftNode) flushAll(ctx context.Context) error {
	wg := sync.WaitGroup{}
	errs := make([]error, len(r.appliers))
	for i := range r.appliers {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[idx] = r.appliers[idx].Flush(ctx)
		}()
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			return errors.Info(errs[i], fmt.Sprintf("flush applier %d failed", i)).Detail(errs[i])
		}
	}
	return nil
}

func (r *RaftNode) closeSnapshotCallback() {
	atomic.AddInt32(&r.openSnapshotsNum, -1)
}

func (r *RaftNode) getApplierByModule(module string) RaftApplier {
	for _, applier := range r.appliers {
		if applier.GetModuleName() == module {
			return applier
		}
	}
	return nil
}
