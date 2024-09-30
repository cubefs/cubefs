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

package main

import (
	"context"
	"flag"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

var (
	concurrency  = flag.Int("c", 10, "input concurrency")
	maxGroupNum  = flag.Uint64("n", 100, "input max raft group num")
	nodeIndex    = flag.Int("i", 0, "input current node index, from 0 to 2")
	tickInterval = flag.Int("t", 1000, "input tick interval(ms)")
	action       = flag.String("a", "read", "input action, read or write")
)

var (
	testRaftCF         = kvstore.CF("test-raft")
	testStateMachineCF = kvstore.CF("test-sm")

	testNodes = []raft.Member{
		{NodeID: 1, Host: "127.0.0.1:31400"},
		{NodeID: 2, Host: "127.0.0.1:31401"},
		{NodeID: 3, Host: "127.0.0.1:31402"},
	}
)

func init() {
	flag.Parse()

	port := 8888
	port += *nodeIndex
	addr := "127.0.0.1:" + strconv.Itoa(port)
	ph := profile.NewProfileHandler(addr)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: rpc.MiddlewareHandlerWith(rpc.DefaultRouter, ph),
	}
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil {
			log.Fatalf("listen and serve failed: %s", err)
		}
		log.Info("Server is running at", addr)
	}()
}

func main() {
	// log.SetOutputLevel(0)
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	// initial multi state machine and manager
	allNodes := testNodes

	// nodeManagers := make([]raft.manager, len(allNodes))
	// nodeCfgs := make([]*raft.Config, len(allNodes))
	// closeManagers := make([]func(), len(allNodes))

	// for i := 0; i < len(allNodes); i++ {
	storagePath, err := util.GenTmpPath()
	if err != nil {
		log.Fatalf("gen temp path failed: %s", err)
	}

	nodeManager, nodeCfg, closeManager := initManager(allNodes[*nodeIndex], storagePath)
	time.Sleep(3 * time.Second)

	maxGroupNum := *maxGroupNum
	leaderGroupLock := sync.Mutex{}
	taskPool := taskpool.New(256, 256)
	wg := sync.WaitGroup{}
	leaderGroup := make([]raft.Group, 0, maxGroupNum)
	wg.Add(int(maxGroupNum))
	for gid := uint64(1); gid <= maxGroupNum; gid++ {
		groupID := gid
		taskPool.Run(func() {
			defer wg.Done()

			storage := &testStorage{
				cf:      testStateMachineCF,
				kvStore: nodeCfg.Storage.(*testStorage).kvStore,
			}
			sm := newTestStateMachine(storage)

			groupConfig := &raft.GroupConfig{
				ID:      groupID,
				Applied: 0,
				Members: allNodes,
				SM:      sm,
			}

			group, err := nodeManager.CreateRaftGroup(ctx, groupConfig)
			if err != nil {
				span.Fatalf("create raft group failed: %s", err)
			}
			leaderIndex := int(groupID) % len(testNodes)
			if leaderIndex == *nodeIndex {
				if err := group.Campaign(ctx); err != nil {
					span.Fatalf("campaign failed: %s", err)
				}
			}

			span.Info("start to wait for leader change")
			// wait for all node receive leader change and read index done
			sm.WaitLeaderChange()
			span.Info("leader change done")

			stat, _ := group.Stat()
			if stat.Leader == allNodes[*nodeIndex].NodeID {
				leaderGroupLock.Lock()
				leaderGroup = append(leaderGroup, group)
				leaderGroupLock.Unlock()
			}
		})

		/*storage := &testStorage{
			cf:      testStateMachineCF,
			kvStore: nodeCfg.Storage.(*testStorage).kvStore,
		}
		sm := newTestStateMachine(storage)

		groupConfig := &raft.GroupConfig{
			ID:      groupID,
			Applied: 0,
			Members: allNodes,
			SM:      sm,
		}

		group, err := nodeManager.CreateRaftGroup(ctx, groupConfig)
		if err != nil {
			span.Fatalf("create raft group failed: %s", err)
		}
		leaderIndex := int(groupID) % len(testNodes)
		if leaderIndex == *nodeIndex {
			if err := group.Campaign(ctx); err != nil {
				span.Fatalf("campaign failed: %s", err)
			}
		}

		span.Info("start to wait for leader change")
		// wait for all node receive leader change and read index done
		sm.WaitLeaderChange()
		span.Info("leader change done")

		stat, _ := group.Stat()
		if stat.Leader == allNodes[*nodeIndex].NodeID {
			leaderGroup = append(leaderGroup, group)
		}*/
	}
	wg.Wait()
	taskPool.Close()
	span.Infof("all group create done, leader group num: %d", len(leaderGroup))
	// nodeManager.RestartTickLoop(30000)

	globalGroupIndex := uint64(0)
	countM := make([]uint64, *concurrency)
	start := time.Now()
	// only first node start benchmark test
	if *nodeIndex == 0 {
		for i := 0; i < *concurrency; i++ {
			idx := i
			go func() {
				// rand.Seed(int64(time.Now().Nanosecond()))
				// startGroupIdx := rand.Intn(len(leaderGroup))
				for {
					groupIndex := atomic.AddUint64(&globalGroupIndex, 1)

					switch *action {
					case "read":
						if err := leaderGroup[groupIndex%uint64(len(leaderGroup))].ReadIndex(ctx); err != nil {
							span.Fatalf("g[%+v] read index failed: %s", err)
						}
						countM[idx]++
					case "write":
						testKV := &testKV{
							key:   "k" + strconv.FormatUint(groupIndex, 10),
							value: "v" + strconv.FormatUint(groupIndex, 10),
						}
						resp, err := leaderGroup[groupIndex%uint64(len(leaderGroup))].Propose(ctx, &raft.ProposalData{
							Data: testKV.Marshal(),
						})
						if err != nil {
							span.Fatalf("g[%+v] propose failed: %s", err)
						}
						if resp.Data.(string) != testKV.key {
							span.Fatalf("return result invalid")
						}
						countM[idx]++
					}
				}
			}()
		}
	}

	// wait for signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	cost := time.Since(start)

	closeManager()

	if *nodeIndex == 0 {
		count := uint64(0)
		for i := range countM {
			count += countM[i]
		}
		span.Infof("qps: %d, avg latency: %d", time.Duration(count)/(cost/time.Second), cost/time.Microsecond/time.Duration(count))
	}
}

func initManager(member raft.Member, storagePath string) (raft.Manager, *raft.Config, func()) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	kvStore, err := kvstore.NewKVStore(ctx, storagePath, kvstore.RocksdbLsmKVType, &kvstore.Option{
		CreateIfMissing: true,
		ColumnFamily:    []kvstore.CF{testRaftCF, testStateMachineCF},
	})
	if err != nil {
		log.Fatalf("new kv store failed: %s", err)
	}

	nodesMap := make(map[uint64]raft.Member)
	for i := range testNodes {
		nodesMap[testNodes[i].NodeID] = testNodes[i]
	}
	storage := &testStorage{kvStore: kvStore, cf: testRaftCF}
	cfg := &raft.Config{
		NodeID:                        member.NodeID,
		TickIntervalMS:                *tickInterval,
		CoalescedHeartbeatsIntervalMS: 500,
		HeartbeatTick:                 1,
		ElectionTick:                  40,
		MaxWorkerNum:                  160,
		MaxWorkerBufferSize:           200,
		MaxSnapshotWorkerNum:          2,
		ReadIndexTimeoutMS:            30000,
		TransportConfig: raft.TransportConfig{
			Addr:                    member.Host,
			MaxTimeoutMs:            30000,
			ConnectTimeoutMs:        30000,
			KeepaliveTimeoutS:       60,
			ServerKeepaliveTimeoutS: 10,
			MaxInflightMsgSize:      1024,
		},
		Logger:   log.DefaultLogger,
		Storage:  storage,
		Resolver: &addressResolver{nodes: nodesMap},
	}
	m, err := raft.NewManager(cfg)
	if err != nil {
		log.Fatalf("new manager failed: %s", err)
	}

	return m, cfg, func() {
		m.Close()
		os.RemoveAll(storagePath)
	}
}

type testKV struct {
	key   string
	value string
}

func (k *testKV) Marshal() []byte {
	return []byte(k.key + "/" + k.value)
}

func (k *testKV) Unmarshal(raw []byte) {
	str := string(raw)
	strArr := strings.Split(str, "/")
	k.key = strArr[0]
	k.value = strArr[1]
}

type addr string

func (a addr) String() string {
	return string(a)
}

type addressResolver struct {
	nodes map[uint64]raft.Member
}

func (a *addressResolver) Resolve(ctx context.Context, nodeID uint64) (raft.Addr, error) {
	return (addr)(a.nodes[nodeID].Host), nil
}

func newTestStateMachine(storage *testStorage) *testStateMachine {
	return &testStateMachine{
		storage:      storage,
		waitLeaderCh: make(chan struct{}),
	}
}

type testStateMachine struct {
	appliedIndex uint64
	leader       uint64
	storage      *testStorage
	waitLeaderCh chan struct{}
	once         sync.Once

	sync.Mutex
}

func (t *testStateMachine) Apply(cxt context.Context, pd []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	t.Lock()
	defer t.Unlock()

	batch := t.storage.NewBatch()
	for i := range pd {
		kv := &testKV{}
		kv.Unmarshal(pd[i].Data)
		batch.Put([]byte(kv.key), []byte(kv.value))
		rets = append(rets, kv.key)
	}
	if err := t.storage.Write(batch); err != nil {
		return nil, err
	}

	t.appliedIndex = index

	return rets, nil
}

func (t *testStateMachine) LeaderChange(peerID uint64) error {
	log.Infof("receive leader change notify: %d", peerID)
	if peerID == 0 {
		return nil
	}

	t.leader = peerID
	t.once.Do(func() {
		close(t.waitLeaderCh)
	})

	return nil
}

func (t *testStateMachine) WaitLeaderChange() {
	<-t.waitLeaderCh
}

func (t *testStateMachine) ApplyMemberChange(m *raft.Member, index uint64) error {
	log.Infof("receice member change: %+v", m)
	return nil
}

func (t *testStateMachine) Snapshot() raft.Snapshot {
	t.Lock()
	defer t.Unlock()

	snap := &testSnapshot{
		index:        t.appliedIndex,
		stg:          t.storage,
		iter:         t.storage.Iter(nil),
		maxBatchSize: 10,
	}

	return snap
}

func (t *testStateMachine) ApplySnapshot(h raft.RaftSnapshotHeader, s raft.Snapshot) error {
	t.Lock()
	defer t.Unlock()
	defer s.Close()

	// drop all data first
	/*if err := t.storage.Drop(); err != nil {
		return err
	}*/

	for {
		batch, err := s.ReadBatch()
		if err != nil && err != io.EOF {
			return err
		}

		if batch != nil {
			if err = t.storage.Write(batch); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	t.appliedIndex = s.Index()
	return nil
}

type testSnapshot struct {
	index        uint64
	stg          raft.Storage
	iter         raft.Iterator
	maxBatchSize int
}

func (t *testSnapshot) ReadBatch() (raft.Batch, error) {
	batch := t.stg.NewBatch()

	for i := 0; i < t.maxBatchSize; i++ {
		keyGetter, valGetter, err := t.iter.ReadNext()
		if err != nil {
			return nil, err
		}
		if valGetter == nil {
			return batch, io.EOF
		}

		batch.Put(keyGetter.Key(), valGetter.Value())
	}

	return batch, nil
}

func (t *testSnapshot) Index() uint64 {
	return t.index
}

func (t *testSnapshot) Close() error {
	t.iter.Close()
	return nil
}

type testStorage struct {
	cf      kvstore.CF
	kvStore kvstore.Store
}

func (t *testStorage) Get(key []byte) (raft.ValGetter, error) {
	vg, err := t.kvStore.Get(context.TODO(), t.cf, key, nil)
	if err != nil {
		if err == kvstore.ErrNotFound {
			err = raft.ErrNotFound
		}
		return nil, err
	}
	return vg, err
}

func (t *testStorage) Iter(prefix []byte) raft.Iterator {
	return &testIterator{lr: t.kvStore.List(context.TODO(), t.cf, prefix, nil, nil)}
}

func (t *testStorage) NewBatch() raft.Batch {
	return &testBatch{cf: t.cf, batch: t.kvStore.NewWriteBatch()}
}

func (t *testStorage) Write(b raft.Batch) error {
	return t.kvStore.Write(context.TODO(), b.(*testBatch).batch, nil)
}

func (t *testStorage) Put(key, value []byte) error {
	return t.kvStore.SetRaw(context.TODO(), t.cf, key, value, nil)
}

type testIterator struct {
	lr kvstore.ListReader
}

func (i *testIterator) SeekTo(key []byte) { i.lr.Seek(key) }

func (i *testIterator) SeekForPrev(prev []byte) error { return i.lr.SeekForPrev(prev) }

func (i *testIterator) ReadNext() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadNext()
}

func (i *testIterator) ReadPrev() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadPrev()
}

func (i *testIterator) Close() { i.lr.Close() }

type testBatch struct {
	cf    kvstore.CF
	batch kvstore.WriteBatch
}

func (t *testBatch) Put(key, value []byte) { t.batch.Put(t.cf, key, value) }

func (t *testBatch) Delete(key []byte) { t.batch.Delete(t.cf, key) }

func (t *testBatch) DeleteRange(start []byte, end []byte) {
	t.batch.DeleteRange(t.cf, start, end)
}

func (t *testBatch) Data() []byte { return t.batch.Data() }

func (t *testBatch) From(data []byte) { t.batch.From(data) }

func (t *testBatch) Close() { t.batch.Close() }
