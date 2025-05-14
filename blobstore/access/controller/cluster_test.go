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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	hostAddr string
	consulKV api.KVPairs = nil

	stableCluster = &atomic.Value{}
	stopChs       []closer.Closer

	cc, cc0, cc1, cc2, cc3, cc19 controller.ClusterController
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

	testServer := httptest.NewServer(mux)
	hostAddr = testServer.URL

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

func initCC() {
	defer func() {
		var data []byte
		stableCluster.Store(data)
	}()

	count := 0
	for cc == nil || cc0 == nil || cc1 == nil {
		c, stop := newCCStop()
		if cc == nil {
			cc = c
			stop = func() {
				// do nothing
			}
		}

		clusters := c.All()
		switch len(clusters) {
		case 0:
			cc0 = c
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

func newCCStop() (controller.ClusterController, func()) {
	cfg := controller.ClusterConfig{
		Region:            region,
		ClusterReloadSecs: 0,
		ConsulAgentAddr:   hostAddr,
	}
	cfg.CMClientConfig.LbConfig.Config.Tc = rpc.TransportConfig{
		MaxIdleConns:      4,
		IdleConnTimeoutMs: 2,
	}

	stop := closer.New()
	stopChs = append(stopChs, stop)
	cc, err := controller.NewClusterController(&cfg, proxycli, stop.Done())
	if err != nil {
		panic(err)
	}
	return cc, stop.Close
}

func TestAccessClusterNew(t *testing.T) {
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
	require.NotNil(t, clusterController)
	require.Nil(t, err)
	require.Equal(t, region, clusterController.Region())
}

func TestAccessClusterAll(t *testing.T) {
	for range [5]struct{}{} {
		require.LessOrEqual(t, len(newCC().All()), 4)
	}
}

func TestAccessClusterChooseOne(t *testing.T) {
	{
		cluster, err := cc0.ChooseOne()
		require.Error(t, err)
		require.Equal(t, (*clustermgr.ClusterInfo)(nil), cluster)
	}
	{
		_, err := cc3.ChooseOne()
		require.Error(t, err)

		_, err = cc2.ChooseOne()
		require.Error(t, err)

		cc2.ChangeChooseAlg(controller.AlgRoundRobin)
		_, err = cc2.ChooseOne()
		require.Error(t, err)
	}
	{
		cluster, err := cc1.ChooseOne()
		require.NoError(t, err)
		require.NotNil(t, cluster)

		for _, alg := range []controller.AlgChoose{
			controller.AlgAvailable,
			controller.AlgRoundRobin,
			controller.AlgRandom,
		} {
			cc1.ChangeChooseAlg(alg)
			for range [100]struct{}{} {
				clusterx, err := cc1.ChooseOne()
				require.NoError(t, err)
				require.Equal(t, cluster, clusterx)
			}
		}
	}
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

func TestAccessClusterChangeChooseAlg(t *testing.T) {
	cases := []struct {
		alg controller.AlgChoose
		err error
	}{
		{0, controller.ErrInvalidChooseAlg},
		{controller.AlgAvailable, nil},
		{controller.AlgRoundRobin, nil},
		{controller.AlgRandom, nil},
		{1024, controller.ErrInvalidChooseAlg},
	}
	for _, cs := range cases {
		err := cc.ChangeChooseAlg(cs.alg)
		require.Equal(t, err, cs.err)
	}
}

func TestAccessClusterChooseBalance(t *testing.T) {
	cc := cc19
	for _, alg := range []controller.AlgChoose{
		controller.AlgAvailable,
		controller.AlgRoundRobin,
		controller.AlgRandom,
	} {
		err := cc.ChangeChooseAlg(alg)
		require.NoError(t, err)

		m := make(map[proto.ClusterID]int, 2)
		for range [10000]struct{}{} {
			cluster, err := cc.ChooseOne()
			require.NoError(t, err)
			m[cluster.ClusterID]++
		}

		t.Logf("balance with algorithm %s: %+v", alg, m)
	}
}
