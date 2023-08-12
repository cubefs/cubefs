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

func TestClusterCmd(t *testing.T) {
	err := testRun("cluster", "help")
	assert.NoError(t, err)
}

func TestClusterInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"cluster", "info"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestClusterStatCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"cluster", "stat"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestClusterFreezeCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments enable",
			args:      []string{"cluster", "freeze", "true"},
			expectErr: false,
		},
		{
			name:      "Valid arguments disable",
			args:      []string{"cluster", "freeze", "false"},
			expectErr: false,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"cluster", "freeze", "invalid"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestClusterSetThresholdCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"cluster", "threshold", "0.5"},
			expectErr: false,
		},
		{
			name:      "missing arguments",
			args:      []string{"cluster", "threshold"},
			expectErr: true,
		},
		{
			name:      "Invalid arguments",
			args:      []string{"cluster", "threshold", "invalid"},
			expectErr: true,
		},
		{
			name:      "too big threshold",
			args:      []string{"cluster", "threshold", "1.1"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestClusterSetParasCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"cluster", "set"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestClusterDisableMpDecommissionCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"cluster", "forbid-mp-decommission", "true"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"cluster", "forbid-mp-decommission"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
