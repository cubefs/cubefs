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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/stretchr/testify/require"
)

const testLogModule = "test"

const testLogMax = 1024

const testLogCount = 1024000

const testBufSize = 0

const testResetBufSize = 1024

const testPrefix = "test"

const testSleepTime = 5 * time.Second

func getFileSize(t *testing.T, file string) (size uint64) {
	tmp, err := os.Stat(file)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	size = uint64(tmp.Size())
	return
}

func AuditLogTest(audit *auditlog.Audit, baseDir string, t *testing.T) {
	audit.ResetWriterBufferSize(testBufSize)
	dir, module, max := audit.GetInfo()
	t.Logf("log dir in %v\n", dir)
	require.Equal(t, path.Join(baseDir, testLogModule), dir)
	require.Equal(t, testLogModule, module)
	require.EqualValues(t, testLogMax, max)
	auditLogName := path.Join(dir, "audit.log")
	maxSize := uint64(0)
	for i := 0; i < testLogCount; i++ {
		audit.LogClientOp("nil", "nil", "nil", nil, 0, 0, 0)
		curr := getFileSize(t, auditLogName)
		if curr > maxSize {
			maxSize = curr
		}
	}
	// NOTE: wait for flush
	token := int32(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&token) == 1 {
			curr := getFileSize(t, auditLogName)
			if curr > maxSize {
				maxSize = curr
			}
		}
	}()
	time.Sleep(testSleepTime)
	atomic.StoreInt32(&token, 0)
	wg.Wait()
	dentries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, dentry := range dentries {
		t.Logf("log file %v, size %v\n", dentry.Name(), getFileSize(t, dentry.Name()))
	}
	// NOTE: we have prefix, so shiftfile()
	// must be invoked once
	nowSize := getFileSize(t, auditLogName)
	require.Less(t, nowSize, maxSize)
	t.Logf("log file count %v", len(dentries))
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
	auditLogName := path.Join(dir, "audit.log")
	maxSize := uint64(0)
	for i := 0; i < testLogCount; i++ {
		auditlog.LogClientOp("nil", "nil", "nil", nil, 0, 0, 0)
		size := getFileSize(t, auditLogName)
		if size > maxSize {
			maxSize = size
		}
	}
	// NOTE: wait for flush
	token := int32(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&token) == 1 {
			curr := getFileSize(t, auditLogName)
			if curr > maxSize {
				maxSize = curr
			}
		}
	}()
	time.Sleep(testSleepTime)
	atomic.StoreInt32(&token, 0)
	wg.Wait()
	dentries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, dentry := range dentries {
		t.Logf("log file %v, size %v\n", dentry.Name(), getFileSize(t, auditLogName))
	}
	nowSize := getFileSize(t, auditLogName)
	require.Less(t, nowSize, maxSize)
	t.Logf("log file count %v", len(dentries))
	auditlog.ResetWriterBufferSize(testResetBufSize)
}
