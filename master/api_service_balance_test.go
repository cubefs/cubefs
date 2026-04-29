package master

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	raftstore_db "github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/stretchr/testify/require"
)

// The balance API tests intentionally exercise request parsing and shallow handler
// branches with small in-memory fixtures. Full migration execution is covered by
// lower-level cluster balance tests because those paths need more cluster state.

// newTestServer builds a minimal Server with an in-memory raft store and an
// empty Cluster. Handler tests use it when the code path reaches raft-backed
// plan loading or syncing.
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

	s.cluster.SetClusterPlanRunning()
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

// newBalanceFormRequest keeps parser tests concise and ensures every request
// looks like the form submissions accepted by these admin APIs.
func newBalanceFormRequest(values url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

func TestParseFreeEmptyMetaPartitionParam(t *testing.T) {
	t.Run("valid with minimum reserve", func(t *testing.T) {
		form := url.Values{}
		form.Set(nameKey, "vol1")
		form.Set(countKey, "1")

		name, count, err := parseFreeEmptyMetaPartitionParam(newBalanceFormRequest(form))
		require.NoError(t, err)
		require.Equal(t, "vol1", name)
		require.Equal(t, RsvEmptyMetaPartitionCnt, count)
	})

	t.Run("valid with explicit reserve", func(t *testing.T) {
		form := url.Values{}
		form.Set(nameKey, "vol1")
		form.Set(countKey, strconv.Itoa(RsvEmptyMetaPartitionCnt+2))

		name, count, err := parseFreeEmptyMetaPartitionParam(newBalanceFormRequest(form))
		require.NoError(t, err)
		require.Equal(t, "vol1", name)
		require.Equal(t, RsvEmptyMetaPartitionCnt+2, count)
	})

	t.Run("missing name", func(t *testing.T) {
		form := url.Values{}
		form.Set(countKey, "2")

		_, _, err := parseFreeEmptyMetaPartitionParam(newBalanceFormRequest(form))
		require.Error(t, err)
	})

	t.Run("invalid count", func(t *testing.T) {
		form := url.Values{}
		form.Set(nameKey, "vol1")
		form.Set(countKey, "-1")

		_, _, err := parseFreeEmptyMetaPartitionParam(newBalanceFormRequest(form))
		require.Error(t, err)
	})
}

func TestParseMigratePartitionParam(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		form := url.Values{}
		form.Set(srcAddrKey, "127.0.0.1:10000")
		form.Set(targetAddrKey, "127.0.0.2:10000")
		form.Set(idKey, "100")

		src, target, id, err := parseMigratePartitionParam(newBalanceFormRequest(form))
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:10000", src)
		require.Equal(t, "127.0.0.2:10000", target)
		require.Equal(t, uint64(100), id)
	})

	t.Run("missing source", func(t *testing.T) {
		form := url.Values{}
		form.Set(targetAddrKey, "127.0.0.2:10000")
		form.Set(idKey, "100")

		_, _, _, err := parseMigratePartitionParam(newBalanceFormRequest(form))
		require.Error(t, err)
	})

	t.Run("same source and target", func(t *testing.T) {
		form := url.Values{}
		form.Set(srcAddrKey, "127.0.0.1:10000")
		form.Set(targetAddrKey, "127.0.0.1:10000")
		form.Set(idKey, "100")

		_, _, _, err := parseMigratePartitionParam(newBalanceFormRequest(form))
		require.Error(t, err)
	})

	t.Run("invalid id", func(t *testing.T) {
		form := url.Values{}
		form.Set(srcAddrKey, "127.0.0.1:10000")
		form.Set(targetAddrKey, "127.0.0.2:10000")
		form.Set(idKey, "invalid")

		_, _, _, err := parseMigratePartitionParam(newBalanceFormRequest(form))
		require.Error(t, err)
	})
}

func TestParseMetaPartitionPlanUserParams(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		param, err := parseMetaPartitionPlanUserParams(newBalanceFormRequest(url.Values{}))
		require.NoError(t, err)
		require.Equal(t, proto.StoreModeRocksDb, param.Mode)
		require.Equal(t, 1, param.Count)
		require.False(t, param.AutoPromoteLearner)
	})

	t.Run("valid full params", func(t *testing.T) {
		form := url.Values{}
		form.Set(nameKey, "vol1")
		form.Set(StartIdKey, "10")
		form.Set(EndIdKey, "20")
		form.Set(StoreModeKey, strconv.Itoa(int(proto.StoreModeMem)))
		form.Set(countKey, "3")
		form.Set(PromoteKey, "true")
		form.Set(SelectTypeKey, strconv.Itoa(SelectTypeNodeSetId))
		form.Set(nodesetIdKey, "7")
		form.Set(addrKey, "127.0.0.1:17210")
		form.Set(RocksdbDirKey, "/data/rocksdb")

		param, err := parseMetaPartitionPlanUserParams(newBalanceFormRequest(form))
		require.NoError(t, err)
		require.Equal(t, "vol1", param.Name)
		require.Equal(t, uint64(10), param.StartID)
		require.Equal(t, uint64(20), param.EndID)
		require.Equal(t, proto.StoreModeMem, param.Mode)
		require.Equal(t, 3, param.Count)
		require.True(t, param.AutoPromoteLearner)
		require.Equal(t, SelectTypeNodeSetId, param.SelectType)
		require.Equal(t, uint64(7), param.NodeSetID)
		require.Equal(t, "127.0.0.1:17210", param.MetaNodeAddr)
		require.Equal(t, "/data/rocksdb", param.RocksdbDir)
	})

	tests := []struct {
		name string
		form url.Values
	}{
		{
			name: "invalid volume name",
			form: url.Values{nameKey: {"_bad"}},
		},
		{
			name: "invalid start id",
			form: url.Values{StartIdKey: {"bad"}},
		},
		{
			name: "start greater than end",
			form: url.Values{StartIdKey: {"20"}, EndIdKey: {"10"}},
		},
		{
			name: "invalid store mode",
			form: url.Values{StoreModeKey: {"99"}},
		},
		{
			name: "invalid promote",
			form: url.Values{PromoteKey: {"not-bool"}},
		},
		{
			name: "invalid select type",
			form: url.Values{SelectTypeKey: {"not-int"}},
		},
		{
			name: "missing zone name",
			form: url.Values{SelectTypeKey: {strconv.Itoa(SelectTypeZoneName)}},
		},
		{
			name: "missing nodeset id",
			form: url.Values{SelectTypeKey: {strconv.Itoa(SelectTypeNodeSetId)}},
		},
		{
			name: "missing select tag",
			form: url.Values{SelectTypeKey: {strconv.Itoa(SelectTypeNodeAddrs)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseMetaPartitionPlanUserParams(newBalanceFormRequest(tt.form))
			require.Error(t, err)
		})
	}
}
