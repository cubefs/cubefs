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

func TestAclCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("acl", "help")
	assert.NoError(t, err)
}

func TestAclAddCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"testVol", "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "missing arguments",
			args:      []string{"testVol"},
			expectErr: true,
		},
	}

	successV1 := &proto.AclRsp{
		OK: true,
		List: []*proto.AclIpInfo{
			{
				Ip:    "192.168.0.1",
				CTime: 1689091200,
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/aclOp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("acl", "add")
	r.runTestCases(t, testCases)
}

func TestAclListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"testVol"},
			expectErr: false,
		},
	}

	successV1 := &proto.AclRsp{
		OK: true,
		List: []*proto.AclIpInfo{
			{
				Ip:    "192.168.0.1",
				CTime: 1689091200,
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/aclOp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	runner := newCliTestRunner().setHttpClient(fakeClient).setCommand("acl", "list")
	runner.runTestCases(t, testCases)
}

func TestAclDelCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"testVol", "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "missing arguments",
			args:      []string{"testVol"},
			expectErr: true,
		},
	}

	successV1 := &proto.AclRsp{
		OK: true,
		List: []*proto.AclIpInfo{
			{
				Ip:    "192.168.0.1",
				CTime: 1689091200,
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/aclOp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	runner := newCliTestRunner().setHttpClient(fakeClient).setCommand("acl", "del")
	runner.runTestCases(t, testCases)
}

func TestAclCheckCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"testVol", "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "missing arguments",
			args:      []string{"testVol"},
			expectErr: true,
		},
	}

	successV1 := &proto.AclRsp{
		OK: true,
		List: []*proto.AclIpInfo{
			{
				Ip:    "192.168.0.1",
				CTime: 1689091200,
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/aclOp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})
	runner := newCliTestRunner().setHttpClient(fakeClient).setCommand("acl", "check")
	runner.runTestCases(t, testCases)
}
