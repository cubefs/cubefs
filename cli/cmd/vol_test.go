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

func TestVolCmd(t *testing.T) {
	r := newCliTestRunner()
	err := r.testRun("volume", "help")
	assert.NoError(t, err)
}

func TestVolListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{},
			expectErr: false,
		},
	}

	successV1 := []*proto.VolInfo{
		{
			Name:  "vol1",
			Owner: "cfs",
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/vol/list":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "list")
	r.runTestCases(t, testCases)
}

func TestVolCreateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "user1"},
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

		case m == http.MethodGet && p == "/admin/createVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "create")
	r.runTestCases(t, testCases)
}

func TestVolUpdateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1"},
			expectErr: false,
		},
		{
			name: "Valid arguments with more options",
			args: []string{"vol1",
				"--description", "test",
				"--zone-name", "zone1",
				"--capacity", "10",
				"--replica-num", "3",
				"--follower-read", "true",
				"--ebs-blk-size", "8388608",
				"--transaction-mask", "create|mkdir|remove|rename|mknod|symlink|link",
				"--transaction-timeout", "5",
				"--tx-conflict-retry-num", "2",
				"--tx-conflict-retry-Interval", "5",
				"--cache-capacity", "10",
				"--cache-action", "0",
				"--cache-high-water", "80",
				"--cache-low-water", "60",
				"--cache-lru-interval", "5",
				"--cache-rule", "rule",
				"--cache-threshold", "10485760",
				"--cache-ttl", "30",
				"--readonly-when-full", "true",
			},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/vol/update":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "update")
	r.runTestCases(t, testCases)
}

func TestVolInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1"},
			expectErr: false,
		},
		{
			name:      "Valid arguments with more options",
			args:      []string{"vol1", "--meta-partition", "--data-partition"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	metaPartitionV1 := []*proto.MetaPartitionView{
		{
			PartitionID: 1,
			Members:     []string{},
		},
	}

	dataPartitionV1 := &proto.DataPartitionsView{
		DataPartitions: []*proto.DataPartitionResponse{
			{
				PartitionID: 1,
				Hosts:       []string{},
			},
		},
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/client/metaPartitions":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(metaPartitionV1)}, nil

		case m == http.MethodGet && p == "/client/partitions":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(dataPartitionV1)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "info")
	r.runTestCases(t, testCases)
}

func TestVolDeleteCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "-y"},
			expectErr: false,
		},
		{
			name:      "Delete without confirmation",
			args:      []string{"vol1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{},
			expectErr: true,
		},
	}

	successV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(successV1)}, nil

		case m == http.MethodGet && p == "/vol/delete":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "delete")
	r.runTestCases(t, testCases)
}

func TestVolTransferCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "user1", "-y"},
			expectErr: false,
		},
		{
			name:      "Transfer without confirmation",
			args:      []string{"vol1", "user1"},
			expectErr: true,
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

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	userV1 := &proto.UserInfo{
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

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/user/info":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(userV1)}, nil

		case m == http.MethodPost && p == "/user/transferVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "transfer")
	r.runTestCases(t, testCases)
}

func TestVolAddDPCmd(t *testing.T) {
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
		{
			name:      "count too small",
			args:      []string{"vol1", "0"},
			expectErr: true,
		},
		{
			name:      "invalid count",
			args:      []string{"vol1", "t"},
			expectErr: true,
		},
	}

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/dataPartition/create":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "add-dp")
	r.runTestCases(t, testCases)
}

func TestVolExpandCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"vol1", "400"},
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
		{
			name:      "Invalid capacity",
			args:      []string{"vol1", "t"},
			expectErr: true,
		},
		{
			name:      "Capacity smaller than before",
			args:      []string{"vol1", "1"},
			expectErr: true,
		},
	}

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/vol/expand":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "expand")
	r.runTestCases(t, testCases)
}

func TestVolShrinkCmd(t *testing.T) {
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
		{
			name:      "Invalid capacity",
			args:      []string{"vol1", "t"},
			expectErr: true,
		},
		{
			name:      "Capacity bigger than before",
			args:      []string{"vol1", "400"},
			expectErr: true,
		},
	}

	volumeV1 := &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		CreateTime:              "2023-04-29 17:27:17",
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		VolType:                 1,
		Uids:                    nil,
	}

	fakeClient := fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch m, p := req.Method, req.URL.Path; {

		case m == http.MethodGet && p == "/admin/getVol":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(volumeV1)}, nil

		case m == http.MethodGet && p == "/vol/shrink":
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeader(), Body: fake.SuccessJsonBody(nil)}, nil

		default:
			t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
			return nil, nil
		}
	})

	r := newCliTestRunner().setHttpClient(fakeClient).setCommand("volume", "shrink")
	r.runTestCases(t, testCases)
}
