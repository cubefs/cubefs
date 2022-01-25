package master

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	rproto "github.com/tiglabs/raft/proto"
)

func TestHandleLeaderChange(t *testing.T) {
	leaderID := server.id
	newLeaderID := leaderID + 1
	server.handleLeaderChange(newLeaderID)
	if server.metaReady != false {
		t.Errorf("logic error,metaReady should be false,metaReady[%v]", server.metaReady)
		return
	}
	server.handleLeaderChange(leaderID)
	if server.metaReady == false {
		t.Errorf("logic error,metaReady should be true,metaReady[%v]", server.metaReady)
		return
	}
}

func TestHandlerPeerChange(t *testing.T) {
	addPeerTest(t)
	removePeerTest(t)
}

func addPeerTest(t *testing.T) {
	confChange := &rproto.ConfChange{
		Type:    rproto.ConfAddNode,
		Peer:    rproto.Peer{ID: 2},
		Context: []byte("127.0.0.2:9090"),
	}
	if err := server.handlePeerChange(confChange); err != nil {
		t.Error(err)
		return
	}
}

func removePeerTest(t *testing.T) {
	confChange := &rproto.ConfChange{
		Type:    rproto.ConfRemoveNode,
		Peer:    rproto.Peer{ID: 2},
		Context: []byte("127.0.0.2:9090"),
	}
	if err := server.handlePeerChange(confChange); err != nil {
		t.Error(err)
		return
	}
}

func TestRaft(t *testing.T) {
	addRaftServerTest("127.0.0.1:9001", 2, t)
	removeRaftServerTest("127.0.0.1:9001", 2, t)
	snapshotTest(t)
}

func snapshotTest(t *testing.T) {
	var err error
	mdSnapshot, err := server.cluster.fsm.Snapshot()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("snapshot apply index[%v]\n", mdSnapshot.ApplyIndex())
	s := &Server{}

	var dbStore *raftstore.RocksDBStore
	dbStore, err = raftstore.NewRocksDBStore("/tmp/chubaofs/raft2", LRUCacheSize, WriteBufferSize)
	if err != nil {
		t.Fatalf("init rocks db store fail cause: %v", err)
	}
	fsm := &MetadataFsm{
		rs:    server.fsm.rs,
		store: dbStore,
	}
	fsm.registerApplySnapshotHandler(func() {
		fsm.restore()
	})
	s.fsm = fsm
	peers := make([]rproto.Peer, 0, len(server.config.peers))
	for _, peer := range server.config.peers {
		peers = append(peers, peer.Peer)
	}
	if err = fsm.ApplySnapshot(peers, mdSnapshot); err != nil {
		t.Error(err)
		return
	}
	if fsm.applied != mdSnapshot.ApplyIndex() {
		t.Errorf("applied not equal,applied[%v],snapshot applied[%v]\n", fsm.applied, mdSnapshot.ApplyIndex())
		return
	}
	mdSnapshot.Close()
}

func addRaftServerTest(addRaftAddr string, id uint64, t *testing.T) {
	//don't pass id test
	reqURL := fmt.Sprintf("%v%v?id=&addr=%v", hostAddr, proto.AddRaftNode, addRaftAddr)
	fmt.Println(reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(string(body))
}

func removeRaftServerTest(removeRaftAddr string, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.RemoveRaftNode, id, removeRaftAddr)
	fmt.Println(reqURL)
	process(reqURL, t)
}
