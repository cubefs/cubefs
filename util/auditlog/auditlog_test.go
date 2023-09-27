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

package auditlog_test

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/stretchr/testify/require"
)

const testLogModule = "test"

const testLogMax = 1024

const testLogCount = 1024

const testBufSize = 0

const testResetBufSize = 1024

const testPrefix = "test"

func AuditLogTest(audit *auditlog.Audit, baseDir string, t *testing.T) {
	audit.ResetWriterBufferSize(testBufSize)
	dir, module, max := audit.GetInfo()
	t.Logf("log dir in %v\n", dir)
	require.Equal(t, path.Join(baseDir, testLogModule), dir)
	require.Equal(t, testLogModule, module)
	require.EqualValues(t, testLogMax, max)
	for i := 0; i < testLogCount; i++ {
		audit.LogClientOp("nil", "nil", "nil", nil, 0, 0, 0)
	}
	// NOTE: wait for flush
	time.Sleep(2 * time.Second)
	dentries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, dentry := range dentries {
		t.Logf("log file %v\n", dentry.Name())
	}
	// NOTE: we have prefix, so shiftfile()
	// must be invoked once
	require.NotEqualValues(t, 1, len(dentries))
	audit.ResetWriterBufferSize(testResetBufSize)
}

func TestAuditLog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	audit, err := auditlog.NewAuditWithPrefix(tmpDir, testLogModule, testLogMax, auditlog.NewAuditPrefix(testPrefix))
	require.NoError(t, err)
	defer audit.Stop()
	AuditLogTest(audit, tmpDir, t)
}

func TestGlobalAuditLog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	_, err = auditlog.InitAuditWithPrefix(tmpDir, testLogModule, testLogMax, auditlog.NewAuditPrefix(testPrefix))
	require.NoError(t, err)
	defer auditlog.StopAudit()
	auditlog.ResetWriterBufferSize(testBufSize)
	dir, module, max, err := auditlog.GetAuditLogInfo()
	require.NoError(t, err)
	require.Equal(t, path.Join(tmpDir, testLogModule), dir)
	require.Equal(t, testLogModule, module)
	require.EqualValues(t, testLogMax, max)
	for i := 0; i < testLogCount; i++ {
		auditlog.LogClientOp("nil", "nil", "nil", nil, 0, 0, 0)
	}
	// NOTE: wait for flush
	time.Sleep(2 * time.Second)
	dentries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, dentry := range dentries {
		t.Logf("log file %v\n", dentry.Name())
	}
	require.NotEqualValues(t, 1, len(dentries))
	auditlog.ResetWriterBufferSize(testResetBufSize)
}
