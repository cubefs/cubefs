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

package iopool_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/iopool"
	"github.com/cubefs/cubefs/blobstore/util/sys"
)

func SchedulerTest(t *testing.T, scheduler iopool.IoScheduler) {
	name := fmt.Sprintf("%s/tmp_file.txt", t.TempDir())
	t.Logf("Test file in %v", name)
	file, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o777)
	if err != nil {
		t.Fail()
		t.Errorf("Error:%v", err)
		return
	}
	defer os.Remove(name)
	defer file.Close()
	task := iopool.NewAllocIoTask(file, uint64(file.Fd()), sys.FALLOC_FL_DEFAULT, 0, 4096)
	scheduler.Submit(task)
	_, err = task.WaitAndClose()
	if err != nil {
		t.Fail()
		t.Errorf("Error:%v", err)
		return
	}
	content := "Hello World"
	data := []byte(content)
	task = iopool.NewWriteIoTask(file, uint64(file.Fd()), 0, data, false)
	scheduler.Submit(task)
	_, err = task.WaitAndClose()
	if err != nil {
		t.Fail()
		t.Errorf("Error:%v", err)
		return
	}
	task = iopool.NewSyncIoTask(file, uint64(file.Fd()))
	scheduler.Submit(task)
	_, err = task.WaitAndClose()
	if err != nil {
		t.Fail()
		t.Errorf("Error:%v", err)
		return
	}
	task = iopool.NewReadIoTask(file, uint64(file.Fd()), 0, data)
	scheduler.Submit(task)
	_, err = task.WaitAndClose()
	if err != nil {
		t.Fail()
		t.Errorf("Error:%v", err)
		return
	}
	if string(data) != content {
		t.Fail()
		t.Errorf("Error:data != \"%v\"", content)
	}
}

func TestSharedScheduler(t *testing.T) {
	scheduler := iopool.NewSharedIoScheduler(4, 128)
	defer scheduler.Close()
	SchedulerTest(t, scheduler)
}

func TestPartitionScheduler(t *testing.T) {
	scheduler := iopool.NewPartitionIoScheduler(2, 128)
	defer scheduler.Close()
	SchedulerTest(t, scheduler)
}
