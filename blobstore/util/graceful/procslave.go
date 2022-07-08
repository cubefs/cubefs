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

package graceful

import (
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

type procSlave struct {
	masterProc   *os.Process
	programEntry programEntry
	state        *State
	once         sync.Once
}

// get the listener fd by extra files
func newProcSlave(config *Config) process {
	state := &State{
		ListenAddresses: config.ListenAddresses,
		CloseCh:         make(chan interface{}),
	}
	fdNum, err := strconv.Atoi(os.Getenv(fdNumEnv))
	if err != nil {
		log.Fatal("get fd num failed, err: ", err)
	}
	log.Info("fdNum: ", fdNum)
	for i := 0; i < fdNum; i++ {
		f := os.NewFile(uintptr(listenFdStart+i), "")
		l, err := net.FileListener(f)
		if err != nil {
			log.Fatal("failed to inherit file descriptor: ", i)
		}
		state.ListenerFds = append(state.ListenerFds, l)
	}
	log.Info("listenerFds: ", state.ListenerFds)
	masterPid := os.Getppid()
	masterProc, err := os.FindProcess(masterPid)
	if err != nil {
		log.Fatal("find master process failed, err: ", err)
	}
	return &procSlave{
		masterProc:   masterProc,
		programEntry: config.Entry,
		state:        state,
	}
}

// run the program entry, watching signal and syncing the master process status
func (s *procSlave) run() {
	// shutdown when receiving any signal
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, receiveSigs...)
		sig := <-sigCh
		log.Info("slave process receive signal: ", sig)
		s.once.Do(func() {
			close(s.state.CloseCh)
		})
	}()
	// syncing master process status, shutdown when the master process down
	go func() {
		// send signal 0 to master process forever
		for {
			if err := s.masterProc.Signal(syscall.Signal(0)); err != nil {
				s.once.Do(func() {
					close(s.state.CloseCh)
				})
				break
			}
			time.Sleep(2 * time.Second)
		}
	}()
	s.programEntry(s.state)
}
