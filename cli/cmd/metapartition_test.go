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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaPartitionCmd(t *testing.T) {
	err := testRun("metapartition", "help")
	assert.NoError(t, err)
}

func TestMetaPartitionGetCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "info", "1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"metapartition", "info"},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"metapartition", "info", "t"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestListCorruptMetaPartitionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "check"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaPartitionDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "decommission", "172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"metapartition", "decommission", "172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"metapartition", "decommission"},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"metapartition", "decommission", "172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaPartitionReplicateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "add-replica", "172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"metapartition", "add-replica", "172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"metapartition", "add-replica"},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"metapartition", "add-replica", "172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaPartitionDeleteReplicaCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "del-replica", "172.16.1.101:17310", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"metapartition", "del-replica", "172.16.1.101:17310"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"metapartition", "del-replica"},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"metapartition", "del-replica", "172.16.1.101:17310", "t"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaPartitionGetDiscardCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metapartition", "get-discard"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}
