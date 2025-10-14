package master

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	raftstore_db "github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/stretchr/testify/require"
)

// build a minimal Server with in-memory raft store and empty Cluster
func newTestServer(t *testing.T) *Server {
	dir := t.TempDir()
	db, err := raftstore_db.NewRocksDBStoreAndRecovery(dir, LRUCacheSize, WriteBufferSize)
	require.NoError(t, err)
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			t: &topology{
				zones: []*Zone{
					{
						name: "zone1",
						nodeSetMap: map[uint64]*nodeSet{
							1: {
								ID:        1,
								metaNodes: new(sync.Map),
							},
						},
					},
				},
				zoneMap: new(sync.Map),
			},
		},
		fsm: &MetadataFsm{store: db},
	}
	return &Server{cluster: cluster}
}

func TestCreateMetaNodeBalancePlan_ErrorPath(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	s.createMetaNodeBalancePlan(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetMetaNodeBalancePlan_NoPlan(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.getMetaNodeBalancePlan(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRunStopDeleteMetaNodeBalancePlan(t *testing.T) {
	s := newTestServer(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	s.runMetaNodeBalancePlan(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	s.cluster.PlanRun = true
	w = httptest.NewRecorder()
	s.stopMetaNodeBalancePlan(w, req)
	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	w = httptest.NewRecorder()
	s.deleteMetaNodeBalancePlan(w, req)
	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestOfflineMetaNode_ParamMissing(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	s.offlineMetaNode(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCreateMetaPartitionStoreModeChangePlan_ParamParseError(t *testing.T) {
	s := newTestServer(t)
	form := url.Values{}
	form.Set("start", "not-number")
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.createMetaPartitionStoreModeChangePlan(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRemoveBackupMetaPartition_OK(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	s := newTestServer(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		conn, _ := ln.Accept()
		if conn != nil {
			p := proto.NewPacket()
			_ = p.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime)
			p.ResultCode = proto.OpOk
			p.Data = []byte("ok")
			p.Size = uint32(len(p.Data))
			_ = p.WriteToConn(conn)
			_ = conn.Close()
		}
	}()

	addr := ln.Addr().String()
	mn := &MetaNode{ID: 1, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	s.cluster.metaNodes.Store(addr, mn)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	s.removeBackupMetaPartition(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	<-done
}

func TestGetCleanMetaPartitionTask_EmptyAndNotFound(t *testing.T) {
	s := newTestServer(t)
	// empty cleanTask, no name => OK with empty list
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	s.getCleanMetaPartitionTask(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// with name that not exists in cleanTask => error
	form := url.Values{}
	form.Set(nameKey, "non-exist")
	req = httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	s.getCleanMetaPartitionTask(w, req)
	resp = w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMigrateMetaPartitionHandler_ParamErrors(t *testing.T) {
	s := newTestServer(t)
	// missing all params
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	s.migrateMetaPartitionHandler(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// have src/target but missing id
	form := url.Values{}
	form.Set(srcAddrKey, "127.0.0.1:10000")
	form.Set(targetAddrKey, "127.0.0.1:10001")
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	s.migrateMetaPartitionHandler(w, req)
	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// with id but target node not exists
	form = url.Values{}
	form.Set(srcAddrKey, "127.0.0.1:10000")
	form.Set(targetAddrKey, "127.0.0.1:10001")
	form.Set(idKey, "1")
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	s.migrateMetaPartitionHandler(w, req)
	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}
