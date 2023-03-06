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

package test

import (
	"github.com/cubefs/cubefs/client/fs"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func random(start int, end int) (randomNum int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNum = r.Intn(end-start+1) + start
	return
}

func TestRunningMonitor(t *testing.T) {

	if os.Getenv("FullTest") == "" {
		t.Skip("Skipping testing in normal test")
	}

	if _, err := log.InitLog("/tmp/cubefs/Logs", "client", log.DebugLevel, nil); err != nil {
		panic(err)
	}

	times := 3
	clientOpTimeOut := 10
	opNum := random(20000, 20000)
	oneRoundTime := 1000

	monitor := fs.NewRunningMonitor(int64(clientOpTimeOut))
	monitor.Start()

	stopTicker := time.NewTicker(time.Duration(clientOpTimeOut*times) * time.Second)
	for {
		select {
		case <-stopTicker.C:
			stopTicker.Stop()
			goto end
		default:
			start := time.Now()
			for i := 0; i < opNum; i++ {
				opName := fs.GetOpName(uint8(random(1, fs.GetOpNum())))
				pid := random(1000, 999999)
				runningStat := monitor.AddClientOp(opName, uint32(pid))
				monitor.SubClientOp(runningStat, nil)
			}
			elapsed := time.Since(start)
			log.LogInfof("action[TestRunningMonitor] elapsed[%v]ms", elapsed.Milliseconds())
			sleepTime := util.Max(oneRoundTime-int(elapsed.Milliseconds()), 0)
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}
	}
end:
	time.Sleep(time.Duration(clientOpTimeOut*3) * time.Second)
	monitor.Stop()
	time.Sleep(time.Duration(clientOpTimeOut) * time.Second)
}

func TestRunningMonitorInHighLoad(t *testing.T) {

	if os.Getenv("FullTest") == "" {
		t.Skip("Skipping testing in normal test")
	}

	if _, err := log.InitLog("/tmp/cubefs/Logs", "client", log.DebugLevel, nil); err != nil {
		panic(err)
	}

	times := 3
	clientOpTimeOut := 60

	monitor := fs.NewRunningMonitor(int64(clientOpTimeOut))
	monitor.Start()

	stopTicker := time.NewTicker(time.Duration(clientOpTimeOut*times) * time.Second)
	for {
		select {
		case <-stopTicker.C:
			stopTicker.Stop()
			goto end
		default:
			start := time.Now()
			for i := 0; i < fs.GetOpNum(); i++ {
				for j := 1000; j < 999999; j++ {
					opName := fs.GetOpName(uint8(i))
					pid := j
					runningStat := monitor.AddClientOp(opName, uint32(pid))
					monitor.SubClientOp(runningStat, nil)
				}
			}
			elapsed := time.Since(start)
			log.LogInfof("action[TestRunningMonitorInHighLoad] elapsed[%v]ms", elapsed.Milliseconds())
		}
	}
end:
	time.Sleep(time.Duration(clientOpTimeOut*3) * time.Second)
	monitor.Stop()
	time.Sleep(time.Duration(clientOpTimeOut) * time.Second)
}
