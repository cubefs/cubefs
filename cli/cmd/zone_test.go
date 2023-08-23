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

func TestZoneCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("zone", "help")
	assert.NoError(t, err)
}

func TestZoneList(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := []*proto.ZoneView{
		{
			Name:   "zone1",
			Status: "available",
			NodeSet: map[uint64]*proto.NodeSetView{
				1: {
					DataNodeLen: 3,
					MetaNodeLen: 3,
					MetaNodes: []proto.NodeView{
						{
							Addr:       "172.16.1.101:17210",
							Status:     false,
							DomainAddr: "172.16.1.101:17010",
							ID:         1,
							IsWritable: false,
						},
					},
					DataNodes: []proto.NodeView{
						{
							Addr:       "172.16.1.101:17310",
							Status:     false,
							DomainAddr: "172.16.1.101:17010",
							ID:         1,
							IsWritable: false,
						},
					},
				},
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/zone/list":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("zone", "list")
	r.runTestCases(t, testCases)
}

func TestZoneInfo(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"zone1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := &proto.TopologyView{
		Zones: []*proto.ZoneView{
			{
				Name:   "zone1",
				Status: "available",
				NodeSet: map[uint64]*proto.NodeSetView{
					1: {
						DataNodeLen: 3,
						MetaNodeLen: 3,
						MetaNodes: []proto.NodeView{
							{
								Addr:       "172.16.1.101:17210",
								Status:     false,
								DomainAddr: "172.16.1.101:17010",
								ID:         1,
								IsWritable: false,
							},
						},
						DataNodes: []proto.NodeView{
							{
								Addr:       "172.16.1.101:17310",
								Status:     false,
								DomainAddr: "172.16.1.101:17010",
								ID:         1,
								IsWritable: false,
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/topo/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("zone", "info")
	r.runTestCases(t, testCases)
}
