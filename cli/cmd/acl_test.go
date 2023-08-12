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

func TestAclCmd(t *testing.T) {
	err := testRun("acl", "help")
	assert.NoError(t, err)
}

func TestAclAddCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"acl", "add", mocktest.CommonVolName, "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "invalid VolName",
			args:      []string{"acl", "add", "invalidVolName", "192.168.0.1"},
			expectErr: true,
		},
		{
			name:      "missing arguments",
			args:      []string{"acl", "add", mocktest.CommonVolName},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestAclListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"acl", "list", mocktest.CommonVolName},
			expectErr: false,
		},
		{
			name:      "invalid VolName",
			args:      []string{"acl", "list", "invalidVolName"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestAclDelCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"acl", "del", mocktest.CommonVolName, "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "invalid VolName",
			args:      []string{"acl", "del", "invalidVolName", "192.168.0.1"},
			expectErr: true,
		},
		{
			name:      "missing arguments",
			args:      []string{"acl", "del", mocktest.CommonVolName},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestAclCheckCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"acl", "check", mocktest.CommonVolName, "192.168.0.1"},
			expectErr: false,
		},
		{
			name:      "invalid VolName",
			args:      []string{"acl", "check", "invalidVolName", "192.168.0.1"},
			expectErr: true,
		},
		{
			name:      "missing arguments",
			args:      []string{"acl", "check", mocktest.CommonVolName},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
