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

func TestMetaNodeCmd(t *testing.T) {
	err := testRun("metanode", "help")
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

	runTestCases(t, testCases)
}

func TestMetaNodeInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metanode", "info", "172.16.1.110:17320"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"metanode", "info"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaNodeDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metanode", "decommission", "172.16.1.110:17320", "--count", "1"},
			expectErr: false,
		},
		{
			name:      "Missing node address",
			args:      []string{"metanode", "decommission"},
			expectErr: true,
		},
		{
			name:      "Invalid migrate dp count",
			args:      []string{"metanode", "decommission", "172.16.1.110:17320", "--count", "-1"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestMetaNodeMigrateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"metanode", "migrate", "172.16.1.101:17210", "172.16.1.101:17210"},
			expectErr: false,
		},
		{
			name:      "Missing 1 node address",
			args:      []string{"metanode", "migrate", "172.16.1.101:17320"},
			expectErr: true,
		},
		{
			name:      "Missing 2 node address",
			args:      []string{"metanode", "migrate"},
			expectErr: true,
		},
		{
			name:      "invalid migrate dp count",
			args:      []string{"metanode", "migrate", "172.16.1.101:17210", "172.16.1.101:17210", "--count", "-1"},
			expectErr: true,
		},
		{
			name:      "too much migrate dp count",
			args:      []string{"metanode", "migrate", "172.16.1.101:17210", "172.16.1.101:17210", "--count", "500"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
