package master

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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
	server.doLeaderChange(newLeaderID, 0, 0)
	if !assert.Falsef(t, server.cluster.isMetaReady(), "logic error,metaReady should be false,metaReady[%v]", server.cluster.isMetaReady()) {
		return
	}
	server.doLeaderChange(leaderID, 0, 0)
	if !assert.Truef(t, server.cluster.isMetaReady(), "logic error,metaReady should be true,metaReady[%v]", server.cluster.isMetaReady()) {
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
	err := server.handlePeerChange(confChange)
	assert.NoError(t, err)
}

func removePeerTest(t *testing.T) {
	confChange := &rproto.ConfChange{
		Type:    rproto.ConfRemoveNode,
		Peer:    rproto.Peer{ID: 2},
		Context: []byte("127.0.0.2:9090"),
	}
	err := server.handlePeerChange(confChange)
	assert.NoError(t, err)
}

func TestRaft(t *testing.T) {
	addRaftServerTest("127.0.0.1:9001", 2, t)
	removeRaftServerTest("127.0.0.1:9001", 2, t)
	snapshotTest(t)
}

func snapshotTest(t *testing.T) {
	var err error
	mdSnapshot, err := server.cluster.fsm.Snapshot(0)
	if !assert.NoError(t, err) {
		return
	}
	s := &Server{}

	var dbStore *raftstore.RocksDBStore
	dbStore, err = raftstore.NewRocksDBStore("/tmp/chubaofs/raft2", LRUCacheSize, WriteBufferSize)
	assert.NoErrorf(t, err, "init rocks db store fail cause: %v", err)
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
	err = fsm.ApplySnapshot(peers, mdSnapshot, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, mdSnapshot.ApplyIndex(), fsm.applied, "applied not equal,applied[%v],snapshot applied[%v]\n", fsm.applied, mdSnapshot.ApplyIndex()) {
		return
	}
	mdSnapshot.Close()
}

func TestSnapshotWithClusterName(t *testing.T) {
	defer func() {
		reqURL := fmt.Sprintf("%v%v?%s=%s",
			hostAddr, proto.AdminSetClusterName, forceKey, "true")
		process(reqURL, t)
	}()
	var err error
	reqURL := fmt.Sprintf("%v%v?%s=%s",
		hostAddr, proto.AdminSetClusterName, proto.ClusterNameKey, server.clusterName)
	processWithError(reqURL, t)
	mdSnapshot, err := server.cluster.fsm.Snapshot(0)
	if !assert.NoError(t, err) {
		return
	}
	s := &Server{}

	var dbStore *raftstore.RocksDBStore
	dbStore, err = raftstore.NewRocksDBStore("/tmp/chubaofs/raft3", LRUCacheSize, WriteBufferSize)
	assert.NoErrorf(t, err, "init rocks db store fail cause: %v", err)
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
	err = fsm.ApplySnapshot(peers, mdSnapshot, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, mdSnapshot.ApplyIndex(), fsm.applied, "applied not equal,applied[%v],snapshot applied[%v]\n", fsm.applied, mdSnapshot.ApplyIndex()) {
		return
	}
	//check cluster name exist?
	var cv *clusterValue
	if cv, err = server.cluster.getFsmClusterCfg(fsm.store); err != nil {
		assert.NoError(t, err)
		t.Errorf("get cluster name failed,%s", err.Error())
		return
	}
	if len(cv.ClusterName) == 0 || cv.ClusterName != server.cluster.cfg.ClusterName {
		err = fmt.Errorf("cfg cluster name err, expect:%s, but now:%s", server.cluster.cfg.ClusterName, cv.ClusterName)
		assert.NoError(t, err)
		t.Errorf("%s", err.Error())
	}
	mdSnapshot.Close()
}

func addRaftServerTest(addRaftAddr string, id uint64, t *testing.T) {
	//don't pass id test
	reqURL := fmt.Sprintf("%v%v?id=&addr=%v", hostAddr, proto.AddRaftNode, addRaftAddr)
	resp, err := http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
}

func removeRaftServerTest(removeRaftAddr string, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?id=%v&addr=%v", hostAddr, proto.RemoveRaftNode, id, removeRaftAddr)
	process(reqURL, t)
}
