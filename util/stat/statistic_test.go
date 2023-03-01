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
	"os"
	"path"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/errors"
)

func TestStatistic(t *testing.T) {
	DefaultStatInterval = 1 * time.Second

	statLogPath := "./"
	statLogSize := 20000000
	statLogModule := "TestStatistic"
	timeOutUs := [MaxTimeoutLevel]uint32{100000, 500000, 1000000}

	_, err := NewStatistic(statLogPath, statLogModule, int64(statLogSize), timeOutUs, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path.Join(statLogPath, statLogModule))
	defer ClearStat()

	bgTime := BeginStat()
	EndStat("test1", nil, bgTime, 1)
	time.Sleep(time.Second)
	err = errors.New("EIO")
	EndStat("test2", err, bgTime, 100)
	time.Sleep(3 * time.Second)
}
