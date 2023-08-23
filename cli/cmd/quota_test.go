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

func TestQuotaCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("quota", "help")
	assert.NoError(t, err)
}

func TestQuotaListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := []*proto.QuotaInfo{
		{
			VolName: "vol1",
			QuotaId: 1,
			CTime:   0,
			PathInfos: []proto.QuotaPathInfo{
				{
					FullPath:    "/path1",
					RootInode:   0,
					PartitionId: 0,
				},
			},
			LimitedInfo: proto.QuotaLimitedInfo{},
			UsedInfo:    proto.QuotaUsedInfo{},
			MaxFiles:    18446744073709551615,
			MaxBytes:    18446744073709551615,
			Rsv:         "",
		},
		{
			VolName: "vol1",
			QuotaId: 2,
			CTime:   0,
			PathInfos: []proto.QuotaPathInfo{
				{
					FullPath:    "/path2",
					RootInode:   0,
					PartitionId: 0,
				},
			},
			LimitedInfo: proto.QuotaLimitedInfo{},
			UsedInfo:    proto.QuotaUsedInfo{},
			MaxFiles:    18446744073709551615,
			MaxBytes:    18446744073709551615,
			Rsv:         "",
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/quota/list":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("quota", "list")
	r.runTestCases(t, testCases)
}

func TestQuotaListAllCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := []*proto.VolInfo{
		{
			Name:                  "vol1",
			Owner:                 "cfs",
			CreateTime:            0,
			Status:                0,
			TotalSize:             0,
			UsedSize:              0,
			DpReadOnlyWhenVolFull: false,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/quota/listAll":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("quota", "listAll")
	r.runTestCases(t, testCases)
}

func TestQuotaUpdateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := &proto.QuotaInfo{
		VolName:     "vol1",
		QuotaId:     1,
		CTime:       0,
		PathInfos:   nil,
		LimitedInfo: proto.QuotaLimitedInfo{},
		UsedInfo:    proto.QuotaUsedInfo{},
		MaxFiles:    18446744073709551615,
		MaxBytes:    18446744073709551615,
		Rsv:         "",
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/quota/get":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		case m == http.MethodGet && p == "/quota/update":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("quota", "update")
	r.runTestCases(t, testCases)
}

func TestQuotaDeleteCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/aclOp":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("quota", "delete")
	r.runTestCases(t, testCases)
}
