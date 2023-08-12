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

func TestUserCmd(t *testing.T) {
	err := testRun("user", "help")
	assert.NoError(t, err)
}

func TestUserCreateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "create", "user1", "--password", "123456", "--access-key", "key1", "--secret-key", "key2", "--user-type", "admin"},
			expectErr: false,
		},
		{
			name:      "Valid arguments in default",
			args:      []string{"user", "create", "user1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"user", "create"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUserUpdateCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "update", "user1", "--access-key", "key1", "--secret-key", "key2", "--user-type", "admin"},
			expectErr: false,
		},
		{
			name:      "No update",
			args:      []string{"user", "update", "user1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{"user", "update"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUserDeleteCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "delete", "user1", "-y"},
			expectErr: false,
		},
		{
			name:      "Delete without confirmation",
			args:      []string{"user", "delete", "user1"},
			expectErr: true,
		},
		{
			name:      "Missing arguments",
			args:      []string{"user", "delete"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUserInfoCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "info", "user1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"user", "info"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUserPermCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "perm", "user1", "vol1", "rw"},
			expectErr: false,
		},
		{
			name:      "Valid arguments in remove permission",
			args:      []string{"user", "perm", "user1", "vol1", "none"},
			expectErr: false,
		},
		{
			name:      "Missing 1 arguments",
			args:      []string{"user", "perm", "user1", "vol1"},
			expectErr: true,
		},
		{
			name:      "Missing 3 arguments",
			args:      []string{"user", "perm"},
			expectErr: true,
		},
		{
			name:      "Invalid permission",
			args:      []string{"user", "perm", "user1", "vol1", "invalid"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}

func TestUserListCmd(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"user", "list"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}
