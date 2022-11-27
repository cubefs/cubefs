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
	"github.com/cubefs/cubefs/util/errors"
	"testing"
	"time"
)

func TestStatistic(t *testing.T) {
	statLogPath := "./"
	statLogSize := 20000000
	timeOutUs := [MaxTimeoutLevel]uint32{100000, 500000, 1000000}

	NewStatistic(statLogPath, "TestStatistic", int64(statLogSize), timeOutUs, true)
	bgTime := BeginStat()
	EndStat("test1", nil, bgTime, 1)
	time.Sleep(10 * time.Second)
	err := errors.New("EIO")
	EndStat("test2", err, bgTime, 100)
	time.Sleep(10 * time.Second)
	time.Sleep(50 * time.Second)
}
