// Copyright 2026 The CubeFS Authors.
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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// `quota getInode [volname] [inode]` takes exactly two positional arguments,
// so a malformed inode id must be reported, not crash the CLI.
func TestQuotaGetInodeIllegalInodeID(t *testing.T) {
	var out strings.Builder
	origStdout := stdout
	stdout = func(format string, a ...interface{}) {
		fmt.Fprintf(&out, format, a...)
	}
	defer func() { stdout = origStdout }()

	// The inode id is parsed before the master client is used,
	// so no cluster is needed to reach the error path.
	cmd := newQuotaGetInode(nil)
	cmd.SetArgs([]string{"vol", "not-a-number"})

	require.NotPanics(t, func() { _ = cmd.Execute() })
	require.Contains(t, out.String(), "inodeId not-a-number is illegal")
}
