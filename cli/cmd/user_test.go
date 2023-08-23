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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/fake"
)

func TestUserCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("user", "help")
	assert.NoError(t, err)
}

func TestUserCreateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user1", "--password", "123456", "--access-key", "key1", "--secret-key", "key2", "--user-type", "admin"},
			expectErr: false,
		},
		{
			name:      "Valid arguments in default",
			args:      []string{"user1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodPost && p == "/user/create":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "create")
	r.runTestCases(t, testCases)
}

func TestUserUpdateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user1", "--access-key", "key1", "--secret-key", "key2", "--user-type", "admin"},
			expectErr: false,
		},
		{
			name:      "No update",
			args:      []string{"user1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodPost && p == "/user/update":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "update")
	r.runTestCases(t, testCases)
}

func TestUserDeleteCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user1", "-y"},
			expectErr: false,
		},
		{
			name:      "Delete without confirmation",
			args:      []string{"user1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodPost && p == "/user/delete":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "delete")
	r.runTestCases(t, testCases)
}

func TestUserInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "key1",
		SecretKey: "key2",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		Mu: sync.RWMutex{},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/user/info":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "info")
	r.runTestCases(t, testCases)
}

func TestUserPermCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user1", "vol1", "rw"},
			expectErr: false,
		},
		{
			name:      "Valid arguments in remove permission",
			args:      []string{"user1", "vol1", "none"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"user1", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 3 arguments",
			args:      []string{},
			expectErr: true,
		},
		{
			name:      "Invalid permission",
			args:      []string{"user1", "vol1", "invalid"},
			expectErr: true,
		},
	}

	successV1 := &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "key1",
		SecretKey: "key2",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/user/info":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		case m == http.MethodPost && p == "/user/updatePolicy":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		case m == http.MethodPost && p == "/user/removePolicy":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "perm")
	r.runTestCases(t, testCases)
}

func TestUserListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := []*proto.UserInfo{
		{
			UserID: "user1",
			Policy: &proto.UserPolicy{
				OwnVols:        []string{"vol1", "vol2"},
				AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
			},
			Mu: sync.RWMutex{},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/user/list":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("user", "list")
	r.runTestCases(t, testCases)
}
