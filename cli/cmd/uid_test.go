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

	"github.com/cubefs/cubefs/cli/cmd/mocktest"
	"github.com/stretchr/testify/assert"
)

func TestUidCmd(t *testing.T) {
	err := testRun("uid", "help")
	assert.NoError(t, err)
}

func TestUidAddCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"uid", "add", mocktest.CommonVolName, "1", "20"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"uid", "add"},
			expectErr: true,
		},
		{
			name:      "invalid VolName",
			args:      []string{"uid", "add", "invalidVolName", "1", "20"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUidListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"uid", "list", mocktest.CommonVolName},
			expectErr: false,
		},
		{
			name:      "List all volumes",
			args:      []string{"uid", "list", mocktest.CommonVolName, "all"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"uid", "list"},
			expectErr: true,
		},
		{
			name:      "invalid VolName",
			args:      []string{"uid", "list", "invalidVolName"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUidDelCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"uid", "del", mocktest.CommonVolName, "1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"uid", "del"},
			expectErr: true,
		},
		{
			name:      "invalid VolName",
			args:      []string{"uid", "del", "invalidVolName", "1"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
func TestUidCheckCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"uid", "check", mocktest.CommonVolName, "1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"uid", "check"},
			expectErr: true,
		},
		{
			name:      "invalid VolName",
			args:      []string{"uid", "check", "invalidVolName", "1"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
