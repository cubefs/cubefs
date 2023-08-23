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

	"github.com/cubefs/cubefs/util/fake"
)

func TestConfigCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("config", "help")
	assert.NoError(t, err)
}

func TestConfigSetCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"--addr", "172.16.1.101:17010"},
			expectErr: false,
		},
		{
			name:      "missing addr",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "zero timeout",
			args:      []string{"--addr", "172.16.1.101:17010", "--timeout", "0"},
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

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("config", "set")
	r.runTestCases(t, testCases)
}

func TestConfigInfoCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("config", "info")
	assert.NoError(t, err)
}
