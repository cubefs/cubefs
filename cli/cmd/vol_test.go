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

func TestVolCmd(t *testing.T) {
	err := testRun("volume", "help")
	assert.NoError(t, err)
}

func TestVolListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "list"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestVolCreateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "create", "vol1", "user1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"volume", "create", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"volume", "create"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolUpdateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "update", "vol1"},
			expectErr: false,
		},
		{
			name: "Valid arguments with more options",
			args: []string{"volume", "update", "vol1",
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
			args:      []string{"volume", "update"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "info", "vol1"},
			expectErr: false,
		},
		{
			name:      "Valid arguments with more options",
			args:      []string{"volume", "info", "vol1", "--meta-partition", "--data-partition"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"volume", "info"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolDeleteCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "delete", "vol1", "-y"},
			expectErr: false,
		},
		{
			name:      "Delete without confirmation",
			args:      []string{"volume", "delete", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{"volume", "delete"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolTransferCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "transfer", "vol1", "user1", "-y"},
			expectErr: false,
		},
		{
			name:      "Transfer without confirmation",
			args:      []string{"volume", "transfer", "vol1", "user1"},
			expectErr: true,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"volume", "transfer", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"volume", "transfer"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolAddDPCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "add-dp", "vol1", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"volume", "add-dp", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"volume", "add-dp"},
			expectErr: true,
		},
		{
			name:      "count too small",
			args:      []string{"volume", "add-dp", "vol1", "0"},
			expectErr: true,
		},
		{
			name:      "invalid count",
			args:      []string{"volume", "add-dp", "vol1", "t"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolExpandCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "expand", "vol1", "400"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"volume", "expand", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"volume", "expand"},
			expectErr: true,
		},
		{
			name:      "Invalid capacity",
			args:      []string{"volume", "expand", "vol1", "t"},
			expectErr: true,
		},
		{
			name:      "Capacity smaller than before",
			args:      []string{"volume", "expand", "vol1", "1"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestVolShrinkCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"volume", "shrink", "vol1", "1"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"volume", "shrink", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 2 arguments",
			args:      []string{"volume", "shrink"},
			expectErr: true,
		},
		{
			name:      "Invalid capacity",
			args:      []string{"volume", "shrink", "vol1", "t"},
			expectErr: true,
		},
		{
			name:      "Capacity bigger than before",
			args:      []string{"volume", "shrink", "vol1", "400"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
