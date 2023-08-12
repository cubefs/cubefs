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

func TestZoneCmd(t *testing.T) {
	err := testRun("zone", "help")
	assert.NoError(t, err)
}

func TestZoneList(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"zone", "list"},
			expectErr: false,
		},
	}

	runTestCases(t, testCases)
}

func TestZoneInfo(t *testing.T) {
	testCases := []*TestCase{
		{
			name:      "Valid arguments",
			args:      []string{"zone", "info", "zone1"},
			expectErr: false,
		},
		{
			name:      "Missing arguments",
			args:      []string{"zone", "info"},
			expectErr: true,
		},
	}

	runTestCases(t, testCases)
}
