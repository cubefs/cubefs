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

package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	hostAddr   string
	consulAddr string
	consulKV   api.KVPairs = nil

	stableCluster = &atomic.Value{}
	codeModeRaw   = &atomic.Value{}
	configRawMap  = &atomic.Value{}
	stopChs       []closer.Closer

	// Keep a global reference so mock Consul server
	// won't be GC'ed during package init.
	testServer *httptest.Server

	cc, cc1, cc2, cc3, cc19 controller.ClusterController
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	for _, stop := range stopChs {
		stop.Close()
	}
	os.Exit(exitCode)
}

func initCluster() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", consul)
	mux.HandleFunc("/service/get", serviceGet)
	mux.HandleFunc("/stat", stat)
	mux.HandleFunc("/config/get", configGet)

	// Keep a global reference to avoid GC closing the server
	// during package init (tests rely on Consul API calls).
	testServer = httptest.NewServer(mux)
	hostAddr = testServer.URL
	u, err := url.Parse(hostAddr)
	if err != nil {
		panic(err)
	}
	// Consul client expects host:port (no scheme) for Address.
	consulAddr = u.Host

	// Provide default codemode config for controller init.
	// Tests may override via codeModeRaw/configRawMap in individual cases.
	policies := []codemode.Policy{
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
	}
	rawPolicies, _ := json.Marshal(policies)
	codeModeRaw.Store(string(rawPolicies))
	configRawMap.Store(map[string]string{proto.CodeModeExtendKey: "[]"})

	cluster1 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 1,
		Capacity:  1 << 50,
		Available: 1 << 45,
		Readonly:  false,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data1, _ := json.Marshal(cluster1)

	cluster2 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 2,
		Capacity:  1 << 50,
		Available: 1 << 45,
		Readonly:  true,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data2, _ := json.Marshal(cluster2)

	cluster3 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 3,
		Capacity:  1 << 50,
		Available: -1024,
		Readonly:  false,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data3, _ := json.Marshal(cluster3)

	cluster9 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 9,
		Capacity:  1 << 50,
		Available: 1 << 40,
		Readonly:  false,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data9, _ := json.Marshal(cluster9)

	consulKV = api.KVPairs{
		&api.KVPair{Key: "1", Value: data1},
		&api.KVPair{Key: "2", Value: data2},
		&api.KVPair{Key: "3", Value: data3},
		&api.KVPair{Key: "9", Value: data9},
		&api.KVPair{Key: "4", Value: data1},
		&api.KVPair{Key: "5", Value: []byte("{invalid json")},
		&api.KVPair{Key: "cannot-parse-key", Value: []byte("{}")},
	}

	time.Sleep(time.Millisecond * 200)
	initCC()
}

func consul(w http.ResponseWriter, req *http.Request) {
	if val := stableCluster.Load(); val != nil {
		if b := val.([]byte); b != nil {
			w.Write(b)
			return
		}
	}
	data, _ := json.Marshal(consulKV)
	w.Write(data)
}

func serviceGet(w http.ResponseWriter, req *http.Request) {
	if val := stableCluster.Load(); val != nil {
		if b := val.([]byte); b != nil {
			w.Write([]byte("{}"))
			return
		}
	}
	if rand.Int31()%2 == 0 {
		w.Write([]byte("{cannot unmarshal"))
	} else {
		w.Write([]byte("{}"))
	}
}

func stat(w http.ResponseWriter, req *http.Request) {
	info := &clustermgr.StatInfo{
		LeaderHost: hostAddr,
		ReadOnly:   false,
		BlobNodeSpaceStat: clustermgr.SpaceStatInfo{
			TotalSpace:    1 << 40,
			WritableSpace: 1 << 20,
		},
	}
	bytes, _ := json.Marshal(info)
	w.Write(bytes)
}

func configGet(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query().Get("key")
	var raw string
	if key == proto.CodeModeConfigKey {
		raw, _ = codeModeRaw.Load().(string)
	} else {
		if kv, ok := configRawMap.Load().(map[string]string); ok && kv != nil {
			raw = kv[key]
		}
	}
	if raw == "" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`"config not found"`))
		return
	}

	resp, _ := json.Marshal(raw) // ConfigGet returns JSON string
	_, _ = w.Write(resp)
}

func initCC() {
	defer func() {
		var data []byte
		stableCluster.Store(data)
	}()

	count := 0
	// Scheme A: construction fails when no enabled policy is loaded (including an
	// empty Consul snapshot). Retry until Consul yields a cluster with policy.
	for cc == nil || cc1 == nil {
		c, stop, err := newCCStopErr()
		if err != nil {
			if errors.Is(err, controller.ErrNoCodeModeAvailable) {
				count++
				continue
			}
			panic(err)
		}
		if cc == nil {
			cc = c
			stop = func() {
				// do nothing
			}
		}

		clusters := c.All()
		switch len(clusters) {
		case 1:
			cc1, cc2, cc3 = handleClusterId(c, cc1, cc2, cc3, clusters[0].ClusterID, stop)
		default:
			stop()
		}

		count++
	}
	log.Debug("init clusters run:", count)

	// init cluster 2
	if cc2 == nil {
		data, _ := json.Marshal(api.KVPairs{consulKV[1]})
		stableCluster.Store(data)
		cc2 = newCC()
	}
	// init cluster 3
	if cc3 == nil {
		data, _ := json.Marshal(api.KVPairs{consulKV[2]})
		stableCluster.Store(data)
		cc3 = newCC()
	}
	// init cluster 1 and 9
	if cc19 == nil {
		data, _ := json.Marshal(api.KVPairs{consulKV[0], consulKV[3]})
		stableCluster.Store(data)
		cc19 = newCC()
	}
}

func handleClusterId(c, cc1, cc2, cc3 controller.ClusterController, clusterId proto.ClusterID, stop func()) (c1, c2, c3 controller.ClusterController) {
	c1, c2, c3 = cc1, cc2, cc3
	switch clusterId {
	case 1:
		c1 = c
	case 2:
		c2 = c
	case 3:
		c3 = c
	default:
		stop()
	}
	return
}

func newCC() controller.ClusterController {
	cc, _ := newCCStop()
	return cc
}

func newCCStopErr() (controller.ClusterController, func(), error) {
	cfg := controller.ClusterConfig{
		Region:            region,
		ClusterReloadSecs: 0,
		ConsulAgentAddr:   consulAddr,
	}
	cfg.CMClientConfig.LbConfig.Config.Tc = rpc.TransportConfig{
		MaxIdleConns:      4,
		IdleConnTimeoutMs: 2,
	}

	stop := closer.New()
	stopChs = append(stopChs, stop)
	cc, err := controller.NewClusterController(&cfg, proxycli, stop.Done())
	return cc, stop.Close, err
}

func newCCStop() (controller.ClusterController, func()) {
	cc, stop, err := newCCStopErr()
	if err != nil {
		panic(err)
	}
	return cc, stop
}

func newIsolatedCCErr() (controller.ClusterController, error) {
	// Make /service/get deterministic for this setup.
	stableCluster.Store([]byte("{}"))
	defer func() {
		var data []byte
		stableCluster.Store(data)
	}()

	cfg := controller.ClusterConfig{
		Region:            region,
		ClusterReloadSecs: 0,
		Clusters: []controller.Cluster{
			{ClusterID: 1, Hosts: []string{hostAddr, hostAddr}},
		},
	}
	cfg.CMClientConfig.LbConfig.Config.Tc = rpc.TransportConfig{
		MaxIdleConns:      4,
		IdleConnTimeoutMs: 2,
	}
	stop := closer.New()
	stopChs = append(stopChs, stop)
	return controller.NewClusterController(&cfg, proxycli, stop.Done())
}

func newIsolatedCC() controller.ClusterController {
	c, err := newIsolatedCCErr()
	if err != nil {
		panic(err)
	}
	return c
}

func TestAccessClusterNew(t *testing.T) {
	stableCluster.Store([]byte("{}"))
	defer func() {
		var data []byte
		stableCluster.Store(data)
	}()

	cfg := controller.ClusterConfig{
		Region: region,
		CMClientConfig: clustermgr.Config{
			LbConfig: rpc.LbConfig{Hosts: []string{"http://localhost"}},
		},
		Clusters: []controller.Cluster{
			{ClusterID: 1, Hosts: []string{hostAddr, hostAddr}},
		},
	}
	clusterController, err := controller.NewClusterController(&cfg, proxycli, nil)
	require.NoError(t, err)
	require.NotNil(t, clusterController)
	require.Equal(t, region, clusterController.Region())
}

func TestAccessClusterAll(t *testing.T) {
	// serviceGet randomly fails; a snapshot with every cluster dropped now
	// fails construction (no enabled policy) instead of returning All()==0.
	checked := 0
	for i := 0; i < 32 && checked < 5; i++ {
		c, _, err := newCCStopErr()
		if err != nil {
			require.ErrorIs(t, err, controller.ErrNoCodeModeAvailable)
			continue
		}
		require.LessOrEqual(t, len(c.All()), 4)
		checked++
	}
	require.NotZero(t, checked)
}

func TestAccessClusterChooseOne(t *testing.T) {
	const testSize = int64(1 << 20) // 1 MB

	singlePolicy := []codemode.Policy{
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
	}
	singleRaw, err := json.Marshal(singlePolicy)
	require.NoError(t, err)
	codeModeRaw.Store(string(singleRaw))
	defer codeModeRaw.Store("")

	data, _ := json.Marshal(api.KVPairs{consulKV[0], consulKV[3]})
	stableCluster.Store(data)
	cc19 = newCC()

	t.Run("(0,mode) picks a cluster that enables the mode", func(t *testing.T) {
		cid, mode, e := cc19.ChooseOne(testSize, codemode.EC6P6, 0)
		require.NoError(t, e)
		require.Contains(t, []proto.ClusterID{1, 9}, cid)
		require.Equal(t, codemode.EC6P6, mode)
	})

	t.Run("(cid,mode) stays on the assigned cluster", func(t *testing.T) {
		cid, mode, e := cc19.ChooseOne(testSize, codemode.EC6P6, 1)
		require.NoError(t, e)
		require.Equal(t, proto.ClusterID(1), cid)
		require.Equal(t, codemode.EC6P6, mode)
	})

	t.Run("readonly cluster is rejected", func(t *testing.T) {
		kv, err := json.Marshal(api.KVPairs{consulKV[1]}) // cluster 2: readonly
		require.NoError(t, err)
		stableCluster.Store(kv)
		readonlyCC := newCC()

		_, _, e := readonlyCC.ChooseOne(testSize, codemode.EC6P6, 2)
		require.ErrorIs(t, e, errcode.ErrAccessNoClusterAvail)
	})

	t.Run("invalid or unknown mode is rejected", func(t *testing.T) {
		_, _, e := cc19.ChooseOne(testSize, codemode.CodeMode(255), 0)
		require.ErrorIs(t, e, errcode.ErrIllegalArguments)
		_, _, e = cc19.ChooseOne(testSize, codemode.EC6P10L2, 0)
		require.ErrorIs(t, e, errcode.ErrIllegalArguments)
		_, _, e = cc19.ChooseOne(testSize, codemode.EC6P6, 999)
		require.ErrorIs(t, e, errcode.ErrIllegalArguments)
	})

	smallMax := int64(1 << 20)
	largeMax := int64(1 << 30)
	multiPolicy := []codemode.Policy{
		{ModeName: codemode.EC3P3.Name(), MinSize: 0, MaxSize: smallMax, SizeRatio: 0.3, Enable: true},
		{ModeName: codemode.EC6P6.Name(), MinSize: smallMax + 1, MaxSize: largeMax, SizeRatio: 0.7, Enable: true},
	}
	multiRaw, err := json.Marshal(multiPolicy)
	require.NoError(t, err)
	codeModeRaw.Store(string(multiRaw))

	t.Run("(0,None) routes by size", func(t *testing.T) {
		fresh := newIsolatedCC()

		_, mode, e := fresh.ChooseOne(smallMax, codemode.CodeModeNone, 0)
		require.NoError(t, e)
		require.Equal(t, codemode.EC3P3, mode)

		_, mode, e = fresh.ChooseOne(smallMax+1, codemode.CodeModeNone, 0)
		require.NoError(t, e)
		require.Equal(t, codemode.EC6P6, mode)

		_, _, e = fresh.ChooseOne(largeMax+1, codemode.CodeModeNone, 0)
		require.ErrorIs(t, e, errcode.ErrAccessNoClusterAvail)
	})

	t.Run("(cid,None) stays on cluster and still routes by size", func(t *testing.T) {
		fresh := newIsolatedCC()

		cid, mode, e := fresh.ChooseOne(smallMax, codemode.CodeModeNone, 1)
		require.NoError(t, e)
		require.Equal(t, proto.ClusterID(1), cid)
		require.Equal(t, codemode.EC3P3, mode)

		_, _, e = fresh.ChooseOne(largeMax+1, codemode.CodeModeNone, 1)
		require.ErrorIs(t, e, errcode.ErrAccessNoClusterAvail)
	})

	t.Run("specified mode ignores size", func(t *testing.T) {
		fresh := newIsolatedCC()

		_, mode, e := fresh.ChooseOne(smallMax+1, codemode.EC3P3, 0)
		require.NoError(t, e)
		require.Equal(t, codemode.EC3P3, mode)

		cid, mode, e := fresh.ChooseOne(smallMax, codemode.EC6P6, 1)
		require.NoError(t, e)
		require.Equal(t, proto.ClusterID(1), cid)
		require.Equal(t, codemode.EC6P6, mode)
	})

	t.Run("disabled policy is not selectable", func(t *testing.T) {
		policies := []codemode.Policy{
			{ModeName: codemode.EC3P3.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 0.8, Enable: false},
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 0.2, Enable: true},
		}
		raw, marshErr := json.Marshal(policies)
		require.NoError(t, marshErr)
		codeModeRaw.Store(string(raw))

		fresh := newIsolatedCC()
		_, mode, e := fresh.ChooseOne(testSize, codemode.CodeModeNone, 0)
		require.NoError(t, e)
		require.Equal(t, codemode.EC6P6, mode)
		_, _, e = fresh.ChooseOne(testSize, codemode.EC3P3, 0)
		require.ErrorIs(t, e, errcode.ErrAccessNoClusterAvail)
	})
}

func TestNewClusterControllerRequiresEnabledPolicy(t *testing.T) {
	t.Run("no policy loaded from any cluster", func(t *testing.T) {
		codeModeRaw.Store("")
		defer codeModeRaw.Store("")

		_, err := newIsolatedCCErr()
		require.ErrorIs(t, err, controller.ErrNoCodeModeAvailable)
	})

	t.Run("disabled policy: construction fails", func(t *testing.T) {
		disabledPolicies := []codemode.Policy{
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: false},
		}
		raw, err := json.Marshal(disabledPolicies)
		require.NoError(t, err)
		codeModeRaw.Store(string(raw))
		defer codeModeRaw.Store("")

		_, err = newIsolatedCCErr()
		require.ErrorIs(t, err, controller.ErrNoCodeModeAvailable)
	})

	t.Run("one invalid mode_name refuses the whole cluster", func(t *testing.T) {
		mixed := []codemode.Policy{
			{ModeName: "UNKNOWN_MODE", MinSize: 0, MaxSize: 1 << 30, SizeRatio: 0.5, Enable: true},
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 0.5, Enable: true},
		}
		raw, err := json.Marshal(mixed)
		require.NoError(t, err)
		codeModeRaw.Store(string(raw))
		defer codeModeRaw.Store("")

		_, err = newIsolatedCCErr()
		require.ErrorIs(t, err, controller.ErrNoCodeModeAvailable)
	})
}

func TestAccessClusterGetHandler(t *testing.T) {
	{
		service, err := cc2.GetServiceController(1)
		require.Error(t, err)
		require.Equal(t, nil, service)

		getter, err := cc2.GetVolumeGetter(1)
		require.Error(t, err)
		require.Equal(t, nil, getter)

		_, err = cc2.GetConfig(context.TODO(), "key")
		require.Error(t, err)
	}
	{
		service, err := cc1.GetServiceController(1)
		require.NoError(t, err)
		require.NotNil(t, service)

		getter, err := cc1.GetVolumeGetter(1)
		require.NoError(t, err)
		require.NotNil(t, getter)

		_, err = cc1.GetConfig(context.TODO(), "key")
		require.Error(t, err)
	}
}

func TestAccessClusterMaxPolicySize(t *testing.T) {
	t.Run("returns enabled policy MaxSize", func(t *testing.T) {
		maxSize := int64(50 << 30)
		policies := []codemode.Policy{
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: maxSize, SizeRatio: 1, Enable: true},
		}
		raw, err := json.Marshal(policies)
		require.NoError(t, err)
		codeModeRaw.Store(string(raw))
		defer codeModeRaw.Store("")

		fresh := newIsolatedCC()
		require.Equal(t, maxSize, fresh.MaxPolicySize())
	})

	t.Run("uses max policy MaxSize across clusters", func(t *testing.T) {
		smallMax := int64(1 << 30)
		largeMax := int64(50 << 30)
		policy := func(max int64) []codemode.Policy {
			return []codemode.Policy{{
				ModeName:  codemode.EC6P6.Name(),
				MinSize:   0,
				MaxSize:   max,
				SizeRatio: 1,
				Enable:    true,
			}}
		}

		srvA := newMockCMServer(t, policy(smallMax))
		srvB := newMockCMServer(t, policy(largeMax))
		cc := newCCFromClusters(t, []controller.Cluster{
			{ClusterID: 1, Hosts: []string{srvA.URL}},
			{ClusterID: 9, Hosts: []string{srvB.URL}},
		})
		require.Equal(t, largeMax, cc.MaxPolicySize())
	})

	t.Run("disabled policy MaxSize is ignored", func(t *testing.T) {
		enabledMax := int64(1 << 30)
		policies := []codemode.Policy{
			{ModeName: codemode.EC3P3.Name(), MinSize: 0, MaxSize: 50 << 30, SizeRatio: 0.5, Enable: false},
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: enabledMax, SizeRatio: 0.5, Enable: true},
		}
		raw, err := json.Marshal(policies)
		require.NoError(t, err)
		codeModeRaw.Store(string(raw))
		defer codeModeRaw.Store("")

		fresh := newIsolatedCC()
		require.Equal(t, enabledMax, fresh.MaxPolicySize())
	})
}

func TestAccessClusterGetEncoder(t *testing.T) {
	policies := []codemode.Policy{
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
	}
	raw, err := json.Marshal(policies)
	require.NoError(t, err)
	codeModeRaw.Store(string(raw))
	defer codeModeRaw.Store("")

	fresh := newIsolatedCC()
	enc := fresh.GetEncoder(codemode.EC6P6)
	require.NotNil(t, enc)

	require.Panics(t, func() { fresh.GetEncoder(codemode.EC16P20L2) })
}

func TestAccessClusterLoadExtendCodeModeWithPolicy(t *testing.T) {
	const extendStart = codemode.CodeMode(256 - 16)
	extends := []codemode.ExtendCodeMode{{
		CodeMode: extendStart,
		Name:     "EC3P31M_TEST",
		Tactic:   codemode.Tactic{N: 3, M: 3, AZCount: 3, PutQuorum: 5, MinShardSize: 1 << 20},
	}}
	extendRaw, err := json.Marshal(extends)
	require.NoError(t, err)

	configRawMap.Store(map[string]string{proto.CodeModeExtendKey: string(extendRaw)})
	defer configRawMap.Store(map[string]string{})

	policies := []codemode.Policy{
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
	}
	policyRaw, err := json.Marshal(policies)
	require.NoError(t, err)
	codeModeRaw.Store(string(policyRaw))
	defer codeModeRaw.Store("")

	fresh := newIsolatedCC()
	require.NotNil(t, fresh)
	require.True(t, extendStart.IsValid())
}

func TestParseClusterChooseAlg(t *testing.T) {
	cases := []struct {
		in   string
		want controller.AlgChoose
	}{
		{"", controller.AlgAvailable},
		{"  ", controller.AlgAvailable},
		{"available", controller.AlgAvailable},
		{"AVAILABLE", controller.AlgAvailable},
		{"roundrobin", controller.AlgRoundRobin},
		{"ROUNDROBIN", controller.AlgRoundRobin},
		{"random", controller.AlgRandom},
		{"unknown", controller.AlgAvailable},
	}
	for _, tc := range cases {
		got := controller.ParseClusterChooseAlg(tc.in)
		require.Equal(t, tc.want, got, tc.in)
	}
}

// newMockCMServer starts an isolated ClusterMgr HTTP stub that serves the given
// codemode policies. Existing package-level mocks share one host and one
// codeModeRaw, so they cannot express per-cluster policy differences.
func newMockCMServer(t *testing.T, policies []codemode.Policy) *httptest.Server {
	t.Helper()
	policyJSON, err := json.Marshal(policies)
	require.NoError(t, err)
	policyRaw := string(policyJSON)

	mux := http.NewServeMux()
	mux.HandleFunc("/stat", func(w http.ResponseWriter, _ *http.Request) {
		info := &clustermgr.StatInfo{
			ReadOnly: false,
			BlobNodeSpaceStat: clustermgr.SpaceStatInfo{
				TotalSpace:    1 << 40,
				WritableSpace: 1 << 30,
			},
		}
		_ = json.NewEncoder(w).Encode(info)
	})
	mux.HandleFunc("/service/get", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/config/get", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		var raw string
		switch key {
		case proto.CodeModeConfigKey:
			raw = policyRaw
		case proto.CodeModeExtendKey:
			raw = "[]"
		}
		if raw == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp, _ := json.Marshal(raw)
		_, _ = w.Write(resp)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newCCFromClusters(t *testing.T, clusters []controller.Cluster) controller.ClusterController {
	t.Helper()
	cfg := controller.ClusterConfig{
		Region:            region,
		ClusterReloadSecs: 0,
		Clusters:          clusters,
	}
	cfg.CMClientConfig.LbConfig.Config.Tc = rpc.TransportConfig{
		MaxIdleConns:      4,
		IdleConnTimeoutMs: 2,
	}
	stop := closer.New()
	stopChs = append(stopChs, stop)
	c, err := controller.NewClusterController(&cfg, proxycli, stop.Done())
	require.NoError(t, err)
	return c
}

func TestAccessClusterChooseOneDifferentPolicies(t *testing.T) {
	const (
		clusterA  = proto.ClusterID(1)
		clusterB  = proto.ClusterID(9)
		testSize  = int64(1 << 20)
		policyMax = int64(1 << 30)
		rounds    = 30
	)
	policyA := []codemode.Policy{{
		ModeName:  codemode.EC6P6.Name(),
		MinSize:   0,
		MaxSize:   policyMax,
		SizeRatio: 1,
		Enable:    true,
	}}
	policyB := []codemode.Policy{{
		ModeName:  codemode.EC15P12.Name(),
		MinSize:   0,
		MaxSize:   policyMax,
		SizeRatio: 1,
		Enable:    true,
	}}

	srvA := newMockCMServer(t, policyA)
	srvB := newMockCMServer(t, policyB)
	cc := newCCFromClusters(t, []controller.Cluster{
		{ClusterID: clusterA, Hosts: []string{srvA.URL}},
		{ClusterID: clusterB, Hosts: []string{srvB.URL}},
	})

	t.Run("encoder union covers both cluster modes", func(t *testing.T) {
		require.NotNil(t, cc.GetEncoder(codemode.EC6P6))
		require.NotNil(t, cc.GetEncoder(codemode.EC15P12))
	})

	t.Run("(0,None) never pairs a cluster with the other cluster mode", func(t *testing.T) {
		seenA, seenB := false, false
		for i := 0; i < rounds; i++ {
			cid, mode, err := cc.ChooseOne(testSize, codemode.CodeModeNone, 0)
			require.NoError(t, err)
			switch cid {
			case clusterA:
				require.Equal(t, codemode.EC6P6, mode)
				seenA = true
			case clusterB:
				require.Equal(t, codemode.EC15P12, mode)
				seenB = true
			default:
				t.Fatalf("unexpected cluster %d", cid)
			}
		}
		require.True(t, seenA, "cluster A should be chosen at least once")
		require.True(t, seenB, "cluster B should be chosen at least once")
	})

	t.Run("(0,mode) only selects the cluster that enables that mode", func(t *testing.T) {
		cid, mode, err := cc.ChooseOne(testSize, codemode.EC6P6, 0)
		require.NoError(t, err)
		require.Equal(t, clusterA, cid)
		require.Equal(t, codemode.EC6P6, mode)

		cid, mode, err = cc.ChooseOne(testSize, codemode.EC15P12, 0)
		require.NoError(t, err)
		require.Equal(t, clusterB, cid)
		require.Equal(t, codemode.EC15P12, mode)
	})

	t.Run("(cid,None) uses the assigned cluster policy", func(t *testing.T) {
		cid, mode, err := cc.ChooseOne(testSize, codemode.CodeModeNone, clusterA)
		require.NoError(t, err)
		require.Equal(t, clusterA, cid)
		require.Equal(t, codemode.EC6P6, mode)

		cid, mode, err = cc.ChooseOne(testSize, codemode.CodeModeNone, clusterB)
		require.NoError(t, err)
		require.Equal(t, clusterB, cid)
		require.Equal(t, codemode.EC15P12, mode)
	})

	t.Run("(cid,mode) rejects a mode the assigned cluster does not enable", func(t *testing.T) {
		_, _, err := cc.ChooseOne(testSize, codemode.EC15P12, clusterA)
		require.Error(t, err)
		require.Equal(t, errcode.ErrAccessNoClusterAvail, err)

		_, _, err = cc.ChooseOne(testSize, codemode.EC6P6, clusterB)
		require.Error(t, err)
		require.Equal(t, errcode.ErrAccessNoClusterAvail, err)
	})
}

func TestAccessClusterListCodeModes(t *testing.T) {
	const (
		clusterA = proto.ClusterID(1)
		clusterB = proto.ClusterID(9)
	)
	policyA := []codemode.Policy{
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
		{ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: false},
		{ModeName: codemode.EC3P3.Name(), MinSize: 0, MaxSize: 1 << 20, SizeRatio: 0, Enable: true},
	}
	policyB := []codemode.Policy{
		{ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
		{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 1 << 30, SizeRatio: 1, Enable: true},
	}

	srvA := newMockCMServer(t, policyA)
	srvB := newMockCMServer(t, policyB)
	cc := newCCFromClusters(t, []controller.Cluster{
		{ClusterID: clusterA, Hosts: []string{srvA.URL}},
		{ClusterID: clusterB, Hosts: []string{srvB.URL}},
	})

	got := cc.ListCodeModes()
	require.Len(t, got, 3)
	require.ElementsMatch(t, []proto.ClusterID{clusterB}, got[codemode.EC15P12])
	require.ElementsMatch(t, []proto.ClusterID{clusterA, clusterB}, got[codemode.EC6P6])
	require.ElementsMatch(t, []proto.ClusterID{clusterA}, got[codemode.EC3P3])
	require.NotContains(t, got, codemode.EC6P10L2)
}
