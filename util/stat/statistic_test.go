// Copyright 2022 The CubeFS Authors.
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

package stat

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/stretchr/testify/require"
)

func TestStatistic(t *testing.T) {
	DefaultStatInterval = 1 * time.Second

	statLogPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	statLogSize := 20000000
	statLogModule := "TestStatistic"
	timeOutUs := [MaxTimeoutLevel]uint32{100000, 500000, 1000000}

	_, err = NewStatistic(statLogPath, statLogModule, int64(statLogSize), timeOutUs, true)
	require.NoError(t, err)
	defer os.RemoveAll(path.Join(statLogPath, statLogModule))
	defer ClearStat()

	bgTime := BeginStat()
	EndStat("test1", nil, bgTime, 1)
	time.Sleep(time.Second)
	err = errors.New("EIO")
	EndStat("test2", err, bgTime, 100)
	time.Sleep(3 * time.Second)
}

func TestDataNodeOpStatsRotate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_op_*")
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(tmpDir)
	}()

	logName := "test_op.log"
	fileSize := 1024 * 1024
	count := 30240 // every file is 1.6M
	tickCnt := 10  // generate 10 files
	expireInterval := 6

	opLog, err := NewOpLogger(tmpDir, logName, 0, time.Second, 0)
	require.NoError(t, err)

	// set rotate size as 1M
	opLog.SetArgs(time.Second*time.Duration(expireInterval), int64(fileSize))

	start := time.Now()
	for {
		if time.Since(start) > time.Second*time.Duration(tickCnt) {
			break
		}

		for i := 0; i < count; i++ {
			key := fmt.Sprintf("op_name_%d", i)
			msg := fmt.Sprintf("tt_op_xxxx_xxx_%d", i)
			opLog.RecordOp(key, msg)
		}
	}

	items, err := os.ReadDir(opLog.dir)
	require.NoError(t, err)
	for _, e := range items {
		t.Logf("got log files, e %s", e.Name())
	}
	// assert there are rotate files, and expire is vaild.
	require.True(t, len(items) > 1 && len(items) <= expireInterval+1)
}

func TestDataNodeOpStatsDiskFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_op_*")
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(tmpDir)
	}()

	logName := "test_op.log"
	fileSize := 1024 * 1024 // rotate over 1m
	count := 30240          // every file is 1.6M
	leftCnt := 4
	tickCnt := 10

	opLog, _ := NewOpLogger(tmpDir, logName, 0, time.Second, 0)
	require.NoError(t, err)

	fs, err := fileutil.Statfs(tmpDir)
	require.NoError(t, err)

	avail := fs.Bavail * uint64(fs.Bsize)
	opLog.leftSpace = int64(avail) + int64(leftCnt*fileSize)
	// set rotate size as 1M
	opLog.SetArgs(time.Hour, int64(fileSize))

	start := time.Now()
	for {
		if time.Since(start) > time.Second*time.Duration(tickCnt) {
			break
		}

		for i := 0; i < count; i++ {
			key := fmt.Sprintf("op_name_%d", i)
			msg := fmt.Sprintf("tt_op_xxxx_xxx_%d", i)
			opLog.RecordOp(key, msg)
		}
	}

	items, err := os.ReadDir(opLog.dir)
	require.NoError(t, err)
	for _, e := range items {
		t.Logf("got log files, e %s", e.Name())
	}

	require.True(t, len(items) <= 3)
}

func countLeftSpace(dir string) (diskSpaceLeft int) {
	fs := syscall.Statfs_t{}
	syscall.Statfs(dir, &fs)
	diskSpaceLeft = int(fs.Bavail * uint64(fs.Bsize))
	return diskSpaceLeft
}

func TestStatisticRotate(t *testing.T) {
	time.Sleep(1 * time.Second)
	DefaultStatInterval = 100 * time.Millisecond

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	statLogModule := "test_stat"
	fileSize := 1024 * 1024
	timeOutUs := [MaxTimeoutLevel]uint32{100000, 500000, 1000000}
	count := 10000 // every file is 1.12M
	tickCnt := 5   // generate 50 files
	expectLeftFileCount := 30

	st, err := NewStatistic(tmpDir, statLogModule, int64(fileSize), timeOutUs, true)
	st.headRoom = int64(countLeftSpace(st.logDir))/1024/1024 - int64(expectLeftFileCount)
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(st.logDir)
	}()
	defer ClearStat()

	start := time.Now()
	for {
		if time.Since(start) > time.Second*time.Duration(tickCnt) {
			break
		}

		for i := 0; i < count; i++ {
			bgTime := BeginStat()
			typename := "test" + strconv.Itoa(i)
			EndStat(typename, nil, bgTime, 1)
		}
		time.Sleep(DefaultStatInterval)
	}

	time.Sleep(2 * time.Second)

	items, err := os.ReadDir(st.logDir)
	require.NoError(t, err)
	for _, e := range items {
		info, _ := e.Info()
		t.Logf("got log files, e %s size = %v", e.Name(), info.Size())
	}
	// assert there are rotate files, and expire is vaild.
	require.True(t, len(items) <= expectLeftFileCount+1)
}
