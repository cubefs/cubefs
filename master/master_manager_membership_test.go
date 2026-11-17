// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	rproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/require"
)

type peerChangeTestStore struct {
	raftstore.RaftStore
	resolver raftstore.NodeResolver
}

func (store *peerChangeTestStore) AddNodeWithPort(id uint64, addr string, heartbeat, replica int) {
	store.resolver.AddNodeWithPort(id, addr, heartbeat, replica)
}

func (store *peerChangeTestStore) DeleteNode(id uint64) {
	store.resolver.DeleteNode(id)
}

func newPeerChangeTestServer() (*Server, raftstore.NodeResolver) {
	resolver := raftstore.NewNodeResolver()
	cfg := newClusterConfig()
	cfg.heartbeatPort = 5901
	cfg.replicaPort = 5902
	return &Server{
		clusterName: "peer-change-test",
		config:      cfg,
		raftStore:   &peerChangeTestStore{resolver: resolver},
	}, resolver
}

func TestPeerChangeReplayUpdatesHTTPAddresses(t *testing.T) {
	master, resolver := newPeerChangeTestServer()
	peerIDs := []uint64{100004, 100005}
	t.Cleanup(func() {
		for _, id := range peerIDs {
			AddrDatabase.Delete(id)
		}
	})

	for replay := 0; replay < 2; replay++ {
		fsm := &MetadataFsm{}
		fsm.registerPeerChangeHandler(master.handlePeerChange)
		for _, id := range peerIDs {
			addr := fmt.Sprintf("127.0.0.%d:17010", id-99990)
			_, err := fsm.ApplyMemberChange(&rproto.ConfChange{
				Type:    rproto.ConfAddNode,
				Peer:    rproto.Peer{ID: id},
				Context: []byte(addr),
			}, 1)
			require.NoError(t, err)
			require.Equal(t, addr, AddrDatabase.Get(id))
			_, err = resolver.NodeAddress(id, raft.HeartBeat)
			require.NoError(t, err)

			remove := &rproto.ConfChange{Type: rproto.ConfRemoveNode, Peer: rproto.Peer{ID: id}}
			for repeat := 0; repeat < 2; repeat++ {
				_, err = fsm.ApplyMemberChange(remove, 2)
				require.NoError(t, err)
				require.Empty(t, AddrDatabase.Get(id))
				require.NotContains(t, AddrDatabase.Snapshot(), id)
				_, err = resolver.NodeAddress(id, raft.HeartBeat)
				require.ErrorIs(t, err, raftstore.ErrNoSuchNode)
			}
		}
	}
}

type followerViewTestTransport struct {
	requests  []*http.Request
	onRequest func()
}

func (transport *followerViewTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.requests = append(transport.requests, request)
	if transport.onRequest != nil {
		onRequest := transport.onRequest
		transport.onRequest = nil
		onRequest()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"code":0,"msg":"success","data":null}`)),
		Request:    request,
	}, nil
}

func TestFollowerViewPushSkipsRemovedPeers(t *testing.T) {
	master, _ := newPeerChangeTestServer()
	peerIDs := []uint64{100002, 100003, 100004, 100005}
	master.id = peerIDs[0]
	t.Cleanup(func() {
		for _, id := range peerIDs {
			AddrDatabase.Delete(id)
		}
	})
	addrs := make(map[uint64]string)
	for _, id := range peerIDs {
		addr := fmt.Sprintf("127.0.0.%d:17010", id-99990)
		addrs[id] = addr
		require.NoError(t, master.handlePeerChange(&rproto.ConfChange{
			Type:    rproto.ConfAddNode,
			Peer:    rproto.Peer{ID: id},
			Context: []byte(addr),
		}))
		if id == peerIDs[2] || id == peerIDs[3] {
			require.NoError(t, master.handlePeerChange(&rproto.ConfChange{
				Type: rproto.ConfRemoveNode,
				Peer: rproto.Peer{ID: id},
			}))
		}
	}

	transport := &followerViewTestTransport{}
	client := masterSDK.NewMasterClient(nil, false)
	client.SetTransport(transport)
	vol := &Vol{
		Name:           "test1",
		Status:         proto.VolStatusNormal,
		dataPartitions: &DataPartitionMap{responseCache: []byte(`{"code":0,"data":{}}`)},
	}
	cluster := &Cluster{
		leaderInfo:   &LeaderInfo{addr: master.getCurrAddr()},
		masterClient: client,
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{vol.Name: vol},
		},
	}
	newFollowerReadManager(cluster).sendFollowerVolumeDpView()

	pushed := make(map[string]int)
	for _, request := range transport.requests {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, proto.AdminPutDataPartitions, request.URL.Path)
		require.Equal(t, vol.Name, request.URL.Query().Get("name"))
		pushed[request.URL.Host]++
	}
	require.Equal(t, 1, pushed[addrs[peerIDs[1]]])
	require.Zero(t, pushed[addrs[peerIDs[2]]])
	require.Zero(t, pushed[addrs[peerIDs[3]]])
	require.Zero(t, pushed[cluster.leaderInfo.addr])

	for _, id := range peerIDs[2:] {
		require.NoError(t, master.handlePeerChange(&rproto.ConfChange{
			Type:    rproto.ConfAddNode,
			Peer:    rproto.Peer{ID: id},
			Context: []byte(addrs[id]),
		}))
	}
	transport.requests = nil
	transport.onRequest = func() {
		for _, id := range peerIDs[1:] {
			require.NoError(t, master.handlePeerChange(&rproto.ConfChange{
				Type: rproto.ConfRemoveNode,
				Peer: rproto.Peer{ID: id},
			}))
		}
	}
	newFollowerReadManager(cluster).sendFollowerVolumeDpView()
	pushed = make(map[string]int)
	for _, request := range transport.requests {
		pushed[request.URL.Host]++
	}
	removedPeerRequests := pushed[addrs[peerIDs[1]]] + pushed[addrs[peerIDs[2]]] + pushed[addrs[peerIDs[3]]]
	require.LessOrEqual(t, removedPeerRequests, 1, "only the request already in flight may finish")
	for _, id := range peerIDs[1:] {
		require.Empty(t, AddrDatabase.Get(id))
	}
}
