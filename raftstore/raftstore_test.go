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

package raftstore

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

type raftAddr struct {
	ip string
}

type testKV struct {
	Opt uint32 `json:"op"`
	K   []byte `json:"k"`
	V   []byte `json:"v"`
}

var TestAddresses = make(map[uint64]*raftAddr)
var maxVolID uint64 = 1

type testSM struct {
	dir string
}

type TestFsm struct {
	RocksDBStore
	testSM
}

func (*testSM) Apply(command []byte, index uint64) (interface{}, error) {
	fmt.Printf("===test raft apply index %d\n", index)
	return nil, nil
}

func (*testSM) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	fmt.Printf("===test raft member change index %d===\n", index)
	return nil, nil
}

func (*testSM) Snapshot() (proto.Snapshot, error) {
	fmt.Printf("===test raft snapshot===\n")
	return nil, nil
}

func (*testSM) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	fmt.Printf("===test raft apply snapshot===\n")
	return nil
}

func (*testSM) HandleFatalEvent(err *raft.FatalError) {
	fmt.Printf("===test raft fatal event===\n")
	return
}

func (*testSM) HandleLeaderChange(leader uint64) {
	fmt.Printf("===test raft leader change to %d===\n", leader)
	return
}

func TestRaftStore_CreateRaftStore(t *testing.T) {

	var (
		cfg     Config
		err     error
		testFsm TestFsm
		peers   []PeerAddress
		data    []byte
	)

	for nid := 1; nid <= 3; nid++ {
		TestAddresses[uint64(nid)] = &raftAddr{
			ip: fmt.Sprintf("127.0.0.%d", nid),
		}

		fmt.Println(TestAddresses[uint64(nid)])
	}

	raftServers := make(map[uint64]*raftStore)
	partitions := make(map[uint64]*partition)

	for n := 1; n <= 3; n++ {
		cfg.NodeID = uint64(n)
		cfg.RaftPath = path.Join("wal", strconv.FormatUint(cfg.NodeID, 10))
		cfg.IPAddr = TestAddresses[uint64(n)].ip

		raftServer, err := NewRaftStore(&cfg)
		if err != nil {
			t.Fatal(err)
		}

		raftServers[uint64(n)] = raftServer.(*raftStore)

		peers = append(peers, PeerAddress{Peer: proto.Peer{ID: uint64(n)}, Address: cfg.IPAddr})

		for k, v := range TestAddresses {
			raftServer.AddNode(uint64(k), v.ip)
		}

		fmt.Printf("================new raft store %d\n", n)

		for i := 1; i <= 5; i++ {
			partitionCfg := &PartitionConfig{
				ID:      uint64(i),
				Applied: 0,
				Leader:  3,
				Term:    10,
				SM:      &testFsm,
				Peers:   peers,
			}

			var p Partition
			p, err = raftServer.CreatePartition(partitionCfg)

			partitions[uint64(i)] = p.(*partition)

			fmt.Printf("==========new partition %d\n", i)

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	kv := &testKV{Opt: 1}
	atomic.AddUint64(&maxVolID, 1)
	value := strconv.FormatUint(maxVolID, 10)
	kv.K = []byte("max_value_key")
	kv.V = []byte(value)

	if data, err = json.Marshal(kv); err != nil {
		err = fmt.Errorf("action[KvsmAllocateVolID],marshal kv:%v,err:%v", kv, err.Error())
		if err != nil {
			t.Fatal(err)
		}
	}

	fmt.Printf("==========encode kv end ===========\n")

	for k := range raftServers {
		fmt.Printf("==raftServer %d==nodeid %d==\n", k, raftServers[k].nodeID)

		for kp := range partitions {
			leader, term := partitions[kp].LeaderTerm()

			fmt.Printf("==partition %d==leader %d term %d==\n", kp, leader, term)
			isLeader := partitions[kp].IsRaftLeader()
			fmt.Printf("==isLeader %t\n", isLeader)
			if partitions[kp].IsRaftLeader() {
				fmt.Printf("==partition can submit==\n")
				_, err = partitions[kp].Submit(data)
				if err != nil {
					t.Fatal(err)
				}

				time.Sleep(5 * time.Second)
				t.SkipNow()
			}
		}
	}
}
