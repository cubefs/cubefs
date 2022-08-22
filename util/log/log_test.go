// Copyright 2018 The CubeFS Authors.
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

package log

// These tests are too simple.

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	go func() {
		http.ListenAndServe(":10000", nil)
	}()

	dir := path.Join("/tmp/cfs", "cfs")
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}

	logFilePath1 := path.Join(dir, "log_info.log.old")
	if err = createFile(logFilePath1, true); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath1, err)
		return
	}
	logFilePath2 := path.Join(dir, "log_err.log")
	if err = createFile(logFilePath2, false); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath2, err)
		return
	}
	logFilePath3 := path.Join(dir, "log_info.log")
	if err = createFile(logFilePath3, true); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath3, err)
		return
	}

	InitLog("/tmp/cfs", "cfs", DebugLevel, nil)
	for i := 0; i < 10; i++ {
		LogDebugf("[debug] current time %v.", time.Now())
		LogWarnf("[warn] current time %v.", time.Now())
		LogErrorf("[error] current time %v.", time.Now())
		LogInfof("[info] current time %v.", time.Now())
		time.Sleep(200 * time.Millisecond)
	}

	_, err = os.Stat(logFilePath1)
	if !os.IsNotExist(err) {
		//t.Errorf("expect file[%v] doesn't exist but err is [%v]", logFilePath1, err)
		//return
	}
	_, err = os.Stat(logFilePath2)
	if err != nil {
		t.Errorf("expect file[%v] exists but err is [%v]", logFilePath2, err)
		return
	}
	_, err = os.Stat(logFilePath3)
	if err != nil {
		t.Errorf("expect file[%v] exists but err is [%v]", logFilePath3, err)
		return
	}
}

// create file and modify modTime to 7 days ago
func createFile(logFilePath string, modTime bool) (err error) {
	_, err = os.Create(logFilePath)
	if err != nil {
		return
	}
	info, err := os.Stat(logFilePath)
	if err != nil {
		return
	}
	if modTime {
		err = os.Chtimes(logFilePath, info.ModTime().AddDate(0, 0, -7), info.ModTime().AddDate(0, 0, -7))
		if err != nil {
			return
		}
	}
	return
}
