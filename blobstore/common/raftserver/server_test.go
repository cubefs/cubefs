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

package raftserver

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var members = []Member{
	{
		NodeID:  1,
		Host:    "127.0.0.1:9090",
		Learner: false,
	},
	{
		NodeID:  2,
		Host:    "127.0.0.1:9091",
		Learner: false,
	},
	{
		NodeID:  3,
		Host:    "127.0.0.1:9092",
		Learner: false,
	},
}

var cfgs = [3]*Config{
	{
		NodeId:         1,
		ListenPort:     9090,
		WalDir:         "/tmp/raftserver/wal1",
		TickIntervalMs: 100,
		ElectionTick:   3,
		Members:        members,
	},
	{
		NodeId:         2,
		ListenPort:     9091,
		WalDir:         "/tmp/raftserver/wal2",
		TickIntervalMs: 100,
		ElectionTick:   3,
		Members:        members,
	},
	{
		NodeId:         3,
		ListenPort:     9092,
		WalDir:         "/tmp/raftserver/wal3",
		TickIntervalMs: 100,
		ElectionTick:   3,
		Members:        members,
	},
}

type applyEntry struct {
	datas [][]byte
	index uint64
}

type confChangeEntry struct {
	cc    ConfChange
	index uint64
}

func TestRaftServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	log.SetOutputLevel(0)
	os.RemoveAll("/tmp/raftserver")
	defer ctrl.Finish()

	var (
		sms         [3]*MockStateMachine
		rss         [3]RaftServer
		applyc      [3]chan applyEntry
		confChangeC [3]chan confChangeEntry
		leaderC     [3]chan uint64
		err         error
	)

	for i := 0; i < 3; i++ {
		sms[i] = NewMockStateMachine(ctrl)
		cfgs[i].SM = sms[i]
		applyc[i] = make(chan applyEntry, 1)
		confChangeC[i] = make(chan confChangeEntry, 1)
		leaderC[i] = make(chan uint64, 1)

		ch := applyc[i]
		leadc := leaderC[i]
		sms[i].EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(datas [][]byte, index uint64) error {
			ch <- applyEntry{datas, index}
			return nil
		}).AnyTimes()
		sms[i].EXPECT().LeaderChange(gomock.Any(), gomock.Any()).DoAndReturn(func(leader uint64, host string) {
			if leader == 0 {
				return
			}
			leadc <- leader
		}).AnyTimes()
	}

	for i := 0; i < 3; i++ {
		rss[i], err = NewRaftServer(cfgs[i])
		require.Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		<-leaderC[i]
	}

	log.Info("====================test propose========================")
	err = rss[0].Propose(context.TODO(), []byte("dfakliuerooioitirw"))
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		entry := <-applyc[i]
		require.Equal(t, len(entry.datas), 1)
		require.True(t, bytes.Equal([]byte("dfakliuerooioitirw"), entry.datas[0]))
	}

	// test raft leader restart and recover data from wal log
	log.Info("====================test restart leader========================")
	for i := 0; i < 3; i++ {
		if !rss[i].IsLeader() {
			continue
		}
		rss[i].Stop()
		cfgs[i].Applied = 0
		rss[i], err = NewRaftServer(cfgs[i])
		require.Nil(t, err)
		entry := <-applyc[i]
		require.Equal(t, len(entry.datas), 1)
		require.True(t, bytes.Equal([]byte("dfakliuerooioitirw"), entry.datas[0]))
	}

	for i := 0; i < 3; i++ {
		<-leaderC[i]
	}

	// test remove leader
	log.Info("====================test remove leader========================")
	for i := 0; i < 3; i++ {
		ch := confChangeC[i]
		idx := i
		sms[i].EXPECT().ApplyMemberChange(gomock.Any(), gomock.Any()).DoAndReturn(func(cc ConfChange, index uint64) error {
			log.Infof("[%d] apply member change cc: %v index: %d", idx, cc, index)
			ch <- confChangeEntry{cc, index}
			return nil
		}).AnyTimes()
	}

	idx := 0
	for i := 0; i < 3; i++ {
		if rss[i].IsLeader() {
			err = rss[i].RemoveMember(context.TODO(), cfgs[i].NodeId)
			require.Nil(t, err)
			idx = i
			break
		}
	}

	lead := uint64(0)
	for i := 0; i < 3; i++ {
		if idx != i {
			lead = <-leaderC[i]
		}
		ccEntry := <-confChangeC[i]
		require.Equal(t, ccEntry.cc.NodeID, cfgs[idx].NodeId)
		require.Equal(t, ccEntry.cc.Type, pb.ConfChangeRemoveNode)
	}
	rss[idx].Stop()
	os.RemoveAll(cfgs[idx].WalDir)

	// test add member cfgs[idx]
	log.Info("====================test add member========================")
	err = rss[lead-1].AddMember(context.TODO(), cfgs[idx].Members[idx])
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		if i == idx {
			continue
		}
		ccEntry := <-confChangeC[i]
		require.Equal(t, ccEntry.cc.NodeID, cfgs[idx].NodeId)
		require.Equal(t, ccEntry.cc.Type, pb.ConfChangeAddNode)
	}
	rss[idx], err = NewRaftServer(cfgs[idx])
	require.Nil(t, err)
	// replay wal log
	v := <-leaderC[idx]
	require.Equal(t, v, lead)
	entry := <-applyc[idx]
	require.Equal(t, len(entry.datas), 1)
	require.True(t, bytes.Equal([]byte("dfakliuerooioitirw"), entry.datas[0]))
	ccEntry := <-confChangeC[idx]
	require.Equal(t, ccEntry.cc.NodeID, cfgs[idx].NodeId)
	require.Equal(t, ccEntry.cc.Type, pb.ConfChangeRemoveNode)
	ccEntry = <-confChangeC[idx]
	require.Equal(t, ccEntry.cc.NodeID, cfgs[idx].NodeId)
	require.Equal(t, ccEntry.cc.Type, pb.ConfChangeAddNode)

	// test transfer leadership and status
	log.Info("====================test transfer leadership========================")
	rss[idx].TransferLeadership(context.TODO(), lead, cfgs[idx].NodeId)
	for i := 0; i < 3; i++ {
		lead = <-leaderC[i]
		require.Equal(t, lead, cfgs[idx].NodeId)
	}
	sm := NewMockStateMachine(ctrl)
	// test add learner
	log.Info("====================test add learner========================")
	learnerCfg := Config{
		NodeId:         4,
		ListenPort:     9093,
		WalDir:         "/tmp/raftserver/wal4",
		TickIntervalMs: 100,
		ElectionTick:   3,
		Members:        members,
		SM:             sm,
	}
	learnerCfg.Members = append(learnerCfg.Members, Member{
		NodeID:  4,
		Host:    "127.0.0.1:9093",
		Learner: true,
	})

	sm.EXPECT().ApplyMemberChange(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	sm.EXPECT().LeaderChange(gomock.Any(), gomock.Any()).Return().AnyTimes()
	sm.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = rss[idx].AddMember(context.TODO(), learnerCfg.Members[3])
	require.Nil(t, err)
	rs, err := NewRaftServer(&learnerCfg)
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		ccEntry := <-confChangeC[i]
		require.Equal(t, ccEntry.cc.NodeID, learnerCfg.NodeId)
		require.Equal(t, ccEntry.cc.Type, pb.ConfChangeAddLearnerNode)
	}
	// learner sync wal  from leader
	err = rs.ReadIndex(context.TODO())
	require.Nil(t, err)
	// change learner to member
	log.Info("====================test change learner========================")
	err = rss[idx].AddMember(context.TODO(), Member{
		NodeID: 4,
		Host:   "127.0.0.1:9093",
	})
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		ccEntry := <-confChangeC[i]
		require.Equal(t, ccEntry.cc.NodeID, learnerCfg.NodeId)
		require.Equal(t, ccEntry.cc.Type, pb.ConfChangeAddNode)
	}
	rs.Stop()

	log.Info("====================test snapshot========================")
	var wg sync.WaitGroup
	var applyIndexes [3]uint64
	wg.Add(3)
	for i := 0; i < 3; i++ {
		ch := applyc[i]
		applyIdx := i
		go func() {
			defer wg.Done()
			for {
				entry := <-ch
				applyIndexes[applyIdx] = entry.index
				for _, data := range entry.datas {
					if bytes.Equal(data, []byte("stop")) {
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 10000; i++ {
		err = rss[idx].Propose(context.TODO(), []byte("1234567890abcdefg"))
		require.Nil(t, err)
	}
	err = rss[idx].Propose(context.TODO(), []byte("stop"))
	require.Nil(t, err)
	wg.Wait()
	require.Equal(t, applyIndexes[0], applyIndexes[1])
	require.Equal(t, applyIndexes[1], applyIndexes[2])
	err = rss[idx].Truncate(applyIndexes[idx])
	require.Nil(t, err)

	snap := NewMockSnapshot(ctrl)
	snap.EXPECT().Name().Return("snap-123435").AnyTimes()
	snap.EXPECT().Index().Return(applyIndexes[0]).AnyTimes()
	cnt := 0
	snap.EXPECT().Read().DoAndReturn(func() ([]byte, error) {
		if cnt == 10000 {
			return nil, nil
		}
		cnt++
		return []byte("1234567890abcdefg"), nil
	}).AnyTimes()
	snap.EXPECT().Close().Return().AnyTimes()
	sms[idx].EXPECT().Snapshot().Return(snap, nil)

	snapC := make(chan []byte, 10000)
	snapMeta := SnapshotMeta{}
	sm.EXPECT().ApplySnapshot(gomock.Any(), gomock.Any()).DoAndReturn(func(meta SnapshotMeta, st Snapshot) error {
		snapMeta = meta
		for {
			data, _ := st.Read()
			if len(data) == 0 {
				break
			}
			snapC <- data
		}
		close(snapC)
		return nil
	}).AnyTimes()

	rs, _ = NewRaftServer(&learnerCfg)
	for snapData := range snapC {
		require.True(t, bytes.Equal(snapData, []byte("1234567890abcdefg")))
	}
	require.Equal(t, snapMeta.Name, "snap-123435")
	require.Equal(t, snapMeta.Index, applyIndexes[0])
	for i := 0; i < 3; i++ {
		rss[i].Stop()
	}
	rs.Stop()
}
