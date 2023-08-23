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

func TestDataPartitionCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("datapartition", "help")
	assert.NoError(t, err)
}

func TestDataPartitionGetCmd(t *testing.T) {
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

	successV1 := &proto.DataPartitionInfo{
		Replicas:                []*proto.DataReplica{},
		Hosts:                   []string{},
		Peers:                   []proto.Peer{},
		Zones:                   []string{},
		MissingNodes:            map[string]int64{},
		FileInCoreMap:           map[string]*proto.FileInCore{},
		FilesWithMissingReplica: map[string]int64{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/dataPartition/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "info")
	r.runTestCases(t, testCases)
}

func TestListCorruptDataPartitionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	diagnosisV1 := &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           []string{"172.16.1.101:17310", "172.16.1.102:17310"},
		CorruptDataPartitionIDs:     []uint64{1, 2},
		LackReplicaDataPartitionIDs: []uint64{1, 2},
		BadDataPartitionIDs: []proto.BadPartitionView{
			{
				Path:         "/test1",
				PartitionIDs: []uint64{1, 2},
			},
			{
				Path:         "/test2",
				PartitionIDs: []uint64{3, 4},
			},
		},
		BadReplicaDataPartitionIDs: []uint64{1, 2},
		RepFileCountDifferDpIDs:    []uint64{1, 2},
		RepUsedSizeDifferDpIDs:     []uint64{1, 2},
		ExcessReplicaDpIDs:         []uint64{1, 2},
	}

	dataNodeV1 := &proto.DataNodeInfo{
		ReportTime:                time.Time{},
		DataPartitionReports:      []*proto.PartitionReport{},
		PersistenceDataPartitions: []uint64{},
		BadDisks:                  []string{},
	}

	dataPartitionV1 := &proto.DataPartitionInfo{
		Replicas:                []*proto.DataReplica{},
		Hosts:                   []string{},
		Peers:                   []proto.Peer{},
		Zones:                   []string{},
		MissingNodes:            map[string]int64{},
		FileInCoreMap:           map[string]*proto.FileInCore{},
		FilesWithMissingReplica: map[string]int64{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/dataPartition/diagnose":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(diagnosisV1)}, nil

		case m == http.MethodGet && p == "/dataNode/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(dataNodeV1)}, nil

		case m == http.MethodGet && p == "/dataPartition/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(dataPartitionV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "check")
	r.runTestCases(t, testCases)
}

func TestDataPartitionDecommissionCmd(t *testing.T) {
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

		case m == http.MethodGet && p == "/dataPartition/decommission":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "decommission")
	r.runTestCases(t, testCases)
}

func TestDataPartitionReplicateCmd(t *testing.T) {
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

		case m == http.MethodGet && p == "/dataReplica/add":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "add-replica")
	r.runTestCases(t, testCases)
}

func TestDataPartitionDeleteReplicaCmd(t *testing.T) {
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

		case m == http.MethodGet && p == "/dataReplica/delete":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "del-replica")
	r.runTestCases(t, testCases)
}

func TestDataPartitionGetDiscardCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := &proto.DiscardDataPartitionInfos{
		DiscardDps: []proto.DataPartitionInfo{
			{
				PartitionID:             1,
				Replicas:                []*proto.DataReplica{},
				Hosts:                   []string{},
				Peers:                   []proto.Peer{},
				Zones:                   []string{},
				MissingNodes:            map[string]int64{},
				FileInCoreMap:           map[string]*proto.FileInCore{},
				FilesWithMissingReplica: map[string]int64{},
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getDiscardDp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})
	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("datapartition", "get-discard")
	r.runTestCases(t, testCases)
}
