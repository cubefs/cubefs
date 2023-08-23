// Copyright 2023 The CubeFS Authors.
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

package cmd

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/fake"
)

func TestClusterCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("cluster", "help")
	assert.NoError(t, err)
}

func TestClusterInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	clusterV1 := &proto.ClusterView{
		Name:               "cfs_dev",
		CreateTime:         "2023-04-29 16:59:54",
		LeaderAddr:         "172.16.1.101:17010",
		DisableAutoAlloc:   false,
		MetaNodeThreshold:  0.75,
		Applied:            123,
		MaxDataPartitionID: 20,
		MaxMetaNodeID:      20,
		MaxMetaPartitionID: 3,
		DataNodeStatInfo: &proto.NodeStatInfo{
			TotalGB:     215,
			UsedGB:      177,
			IncreasedGB: 0,
			UsedRatio:   "0.826",
		},
		MetaNodeStatInfo: &proto.NodeStatInfo{
			TotalGB:     9,
			UsedGB:      0,
			IncreasedGB: 0,
			UsedRatio:   "0.037",
		},
		VolStatInfo: []*proto.VolStatInfo{
			{
				Name:                  "vol1",
				TotalSize:             107374182400,
				UsedSize:              0,
				UsedRatio:             "0.000",
				CacheTotalSize:        0,
				CacheUsedSize:         0,
				CacheUsedRatio:        "0.00",
				EnableToken:           false,
				InodeCount:            1,
				DpReadOnlyWhenVolFull: false,
			},
		},
		BadPartitionIDs:     []proto.BadPartitionView{},
		BadMetaPartitionIDs: []proto.BadPartitionView{},
		MasterNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17010",
				Status:     true,
				DomainAddr: "",
				ID:         1,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.102:17010",
				Status:     true,
				DomainAddr: "",
				ID:         2,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.103:17010",
				Status:     true,
				DomainAddr: "",
				ID:         3,
				IsWritable: false,
			},
		},
		MetaNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17210",
				Status:     false,
				DomainAddr: "",
				ID:         2,
				IsWritable: true,
			},
			{
				Addr:       "172.16.1.102:17210",
				Status:     false,
				DomainAddr: "",
				ID:         3,
				IsWritable: true,
			},
			{
				Addr:       "172.16.1.103:17210",
				Status:     false,
				DomainAddr: "",
				ID:         4,
				IsWritable: true,
			},
		},
		DataNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17310",
				Status:     false,
				DomainAddr: "",
				ID:         2,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.102:17310",
				Status:     false,
				DomainAddr: "",
				ID:         3,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.103:17310",
				Status:     false,
				DomainAddr: "",
				ID:         4,
				IsWritable: false,
			},
		},
	}

	nodeV1 := &proto.ClusterNodeInfo{}

	ipV1 := &proto.ClusterIP{}
	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getCluster":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(clusterV1)}, nil

		case m == http.MethodGet && p == "/admin/getNodeInfo":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nodeV1)}, nil

		case m == http.MethodGet && p == "/admin/getIp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(ipV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "info")
	r.runTestCases(t, testCases)
}

func TestClusterStatCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := &proto.ClusterStatInfo{
		DataNodeStatInfo: &proto.NodeStatInfo{},
		MetaNodeStatInfo: &proto.NodeStatInfo{},
		ZoneStatInfo:     map[string]*proto.ZoneStat{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/cluster/stat":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "stat")
	r.runTestCases(t, testCases)
}

func TestClusterFreezeCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments enable",
			args:      []string{"true"},
			expectErr: false,
		},
		{
			name:      "Valid arguments disable",
			args:      []string{"false"},
			expectErr: false,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"invalid"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/cluster/freeze":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "freeze")
	r.runTestCases(t, testCases)
}

func TestClusterSetThresholdCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"0.5"},
			expectErr: false,
		},
		{
			name:      "missing arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"invalid"},
			expectErr: true,
		},
		{
			name:      "too big threshold",
			args:      []string{"1.1"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/threshold/set":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "threshold")
	r.runTestCases(t, testCases)
}

func TestClusterSetParasCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/setNodeInfo":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "set")
	r.runTestCases(t, testCases)
}

func TestClusterDisableMpDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"true"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"forbid-mp-decommission"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/cluster/forbidMetaPartitionDecommission":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("cluster", "forbid-mp-decommission")
	r.runTestCases(t, testCases)
}
