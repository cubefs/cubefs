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

func TestMetaNodeCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("metanode", "help")
	assert.NoError(t, err)
}

func TestMetaNodeListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metanode", "list"},
			expectErr: false,
		},
	}

	successV1 := &proto.ClusterView{
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
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getCluster":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient)
	r.runTestCases(t, testCases)
}

func TestMetaNodeInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.110:17320"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := &proto.MetaNodeInfo{
		ReportTime: time.Time{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaNode/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metanode", "info")
	r.runTestCases(t, testCases)
}

func TestMetaNodeDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.110:17320", "--count", "1"},
			expectErr: false,
		},
		{
			name:      "Missing node address",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid migrate dp count",
			args:      []string{"172.16.1.110:17320", "--count", "-1"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaNode/decommission":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metanode", "decommission")
	r.runTestCases(t, testCases)
}

func TestMetaNodeMigrateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"172.16.1.101:17210", "172.16.1.101:17210"},
			expectErr: false,
		},
		{
			name:      "Missing 1 node address",
			args:      []string{"172.16.1.101:17320"},
			expectErr: true,
		},
		{
			name:      "Missing 2 node address",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "invalid migrate dp count",
			args:      []string{"172.16.1.101:17210", "172.16.1.101:17210", "--count", "-1"},
			expectErr: true,
		},
		{
			name:      "too much migrate dp count",
			args:      []string{"172.16.1.101:17210", "172.16.1.101:17210", "--count", "500"},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/metaNode/migrate":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("metanode", "migrate")
	r.runTestCases(t, testCases)
}
