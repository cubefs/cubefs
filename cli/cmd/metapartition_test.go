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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/fake"
)

func TestMetaPartitionCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("metapartition", "help")
	assert.NoError(t, err)
}

func TestMetaPartitionGetCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"t"},
			expectErr: true,
		},
	}

	successV1 := &proto.MetaPartitionInfo{
		Replicas: []*proto.MetaReplicaInfo{
			{
				Addr:       "172.16.1.101:17210",
				DomainAddr: "172.16.1.101:17210",
				IsLeader:   false,
			},
			{
				Addr:       "172.16.1.102:17210",
				DomainAddr: "172.16.1.102:17210",
				IsLeader:   true,
			},
			{
				Addr:       "172.16.1.103:17210",
				DomainAddr: "172.16.1.103:17210",
				IsLeader:   false,
			},
		},
		ReplicaNum: 0,
		Status:     0,
		IsRecover:  false,
		Hosts:      []string{"172.16.1.101:17210", "172.16.1.102:17210", "172.16.1.103:17210"},
		Peers: []proto.Peer{
			{
				ID:   1,
				Addr: "172.16.1.101:17210",
			},
			{
				ID:   2,
				Addr: "172.16.1.102:17210",
			},
			{
				ID:   3,
				Addr: "172.16.1.103:17210",
			},
		},
		Zones:         []string{"default", "default", "default"},
		OfflinePeerID: 0,
		MissNodes: map[string]int64{
			"172.16.1.103:17210": 1690280680,
		},
		LoadResponse: []*proto.MetaPartitionLoadResponse{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaPartition/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metapartition", "info")
	r.runTestCases(t, testCases)
}

func TestListCorruptMetaPartitionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	diagnoseV1 := &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:           []string{"172.16.1.101:17310", "172.16.1.102:17310"},
		CorruptMetaPartitionIDs:     []uint64{1, 2},
		LackReplicaMetaPartitionIDs: []uint64{1, 2},
		BadMetaPartitionIDs: []proto.BadPartitionView{
			{
				Path:         "/test1",
				PartitionIDs: []uint64{1, 2},
			},
			{
				Path:         "/test2",
				PartitionIDs: []uint64{3, 4},
			},
		},
		BadReplicaMetaPartitionIDs:                 []uint64{1, 2},
		ExcessReplicaMetaPartitionIDs:              []uint64{1, 2},
		InodeCountNotEqualReplicaMetaPartitionIDs:  []uint64{1, 2},
		MaxInodeNotEqualReplicaMetaPartitionIDs:    []uint64{1, 2},
		DentryCountNotEqualReplicaMetaPartitionIDs: []uint64{1, 2},
	}

	metanodeV1 := &proto.MetaNodeInfo{
		ReportTime: time.Time{},
	}

	metaPartitionV1 := &proto.MetaPartitionInfo{
		Replicas: []*proto.MetaReplicaInfo{
			{
				Addr:       "172.16.1.101:17210",
				DomainAddr: "172.16.1.101:17210",
				IsLeader:   false,
			},
			{
				Addr:       "172.16.1.102:17210",
				DomainAddr: "172.16.1.102:17210",
				IsLeader:   true,
			},
			{
				Addr:       "172.16.1.103:17210",
				DomainAddr: "172.16.1.103:17210",
				IsLeader:   false,
			},
		},
		ReplicaNum: 0,
		Status:     0,
		IsRecover:  false,
		Hosts:      []string{"172.16.1.101:17210", "172.16.1.102:17210", "172.16.1.103:17210"},
		Peers: []proto.Peer{
			{
				ID:   1,
				Addr: "172.16.1.101:17210",
			},
			{
				ID:   2,
				Addr: "172.16.1.102:17210",
			},
			{
				ID:   3,
				Addr: "172.16.1.103:17210",
			},
		},
		Zones:         []string{"default", "default", "default"},
		OfflinePeerID: 0,
		MissNodes: map[string]int64{
			"172.16.1.103:17210": 1690280680,
		},
		LoadResponse: []*proto.MetaPartitionLoadResponse{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaPartition/diagnose":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(diagnoseV1)}, nil

		case m == http.MethodGet && p == "/metaNode/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(metanodeV1)}, nil

		case m == http.MethodGet && p == "/metaPartition/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(metaPartitionV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metapartition", "check")
	r.runTestCases(t, testCases)
}

func TestMetaPartitionDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaPartition/decommission":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metapartition", "decommission")
	r.runTestCases(t, testCases)
}

func TestMetaPartitionReplicateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaReplica/add":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metapartition", "add-replica")
	r.runTestCases(t, testCases)
}

func TestMetaPartitionDeleteReplicaCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaReplica/delete":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metapartition", "del-replica")
	r.runTestCases(t, testCases)
}
