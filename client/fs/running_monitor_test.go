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

package fs

import (
	"math/rand"
	"testing"
	"time"
)

func random(start int, end int) (randomNum int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNum = r.Intn(end-start+1) + start
	return
}

func TestRunningMonitor(t *testing.T) {
	times := 1
	clientOpTimeOut := 1
	opNum := 20000

	monitor := NewRunningMonitor(int64(clientOpTimeOut))
	monitor.Start()

	stopTicker := time.NewTicker(time.Duration(clientOpTimeOut*times) * time.Second)
	for {
		select {
		case <-stopTicker.C:
			stopTicker.Stop()
			monitor.Stop()
			return
		default:
			for i := 0; i < opNum; i++ {
				opName := getOpName(uint8(random(1, getOpNum())))
				pid := random(1000, 999999)
				runningStat := monitor.AddClientOp(opName, uint32(pid))
				monitor.SubClientOp(runningStat, nil)
			}
		}
	}
}

func TestRunningMonitorInHighLoad(t *testing.T) {
	times := 1
	clientOpTimeOut := 1

	monitor := NewRunningMonitor(int64(clientOpTimeOut))
	monitor.Start()

	stopTicker := time.NewTicker(time.Duration(clientOpTimeOut*times) * time.Second)
	for {
		select {
		case <-stopTicker.C:
			stopTicker.Stop()
			monitor.Stop()
			return
		default:
			for i := 0; i < getOpNum(); i++ {
				for j := 1000; j < 1010; j++ {
					opName := getOpName(uint8(i))
					pid := j
					runningStat := monitor.AddClientOp(opName, uint32(pid))
					monitor.SubClientOp(runningStat, nil)
				}
			}
		}
	}
}
