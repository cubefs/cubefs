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
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type procMaster struct {
	slaveProcs      []*exec.Cmd
	programEntry    programEntry
	extraFiles      []*os.File
	listenAddresses []string
	lock            sync.Mutex
}

func newProcMaster(config *Config) process {
	// generate the listen fds and hold them
	extraFiles := make([]*os.File, len(config.ListenAddresses))
	for i, addr := range config.ListenAddresses {
		/*tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			log.Fatalln("resolve tcp address failed: ", errors.Detail(err))
		}*/
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal("listen tcp failed: ", errors.Detail(err))
		}
		netListener, ok := l.(*net.TCPListener)
		if !ok {
			log.Fatal("Failed to assert listener for: ", addr, errors.Detail(err))
		}
		f, err := netListener.File()
		if err != nil {
			log.Fatal("Failed to retreive fd for: ", addr, errors.Detail(err))
		}
		if err := l.Close(); err != nil {
			log.Fatal("Failed to close listener for: ", addr, errors.Detail(err))
		}
		extraFiles[i] = f
	}

	return &procMaster{
		programEntry:    config.Entry,
		extraFiles:      extraFiles,
		listenAddresses: config.ListenAddresses,
	}
}

// fork new process to run the entry program, watching the signal
func (m *procMaster) run() {
	m.fork()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, receiveSigs...)
	for sig := range sigCh {
		log.Info("master process rececive signal: ", sig)
		if sig == syscall.SIGUSR2 {
			m.fork()
		} else if sig == syscall.SIGCHLD {
			// this signal occur every restarting, ignore it
			continue
		}
		// send signal to children process
		m.lock.Lock()
		log.Info(m.slaveProcs)
		for i := range m.slaveProcs {
			if i != len(m.slaveProcs)-1 || sig != syscall.SIGUSR2 {
				if err := m.slaveProcs[i].Process.Signal(sig); err != nil {
					log.Error("unexpected err when sending signal: ", err.Error())
				}
			}
		}
		m.slaveProcs = m.slaveProcs[len(m.slaveProcs)-1:]
		m.lock.Unlock()
		// exit the main process
		if sig != syscall.SIGUSR2 {
			log.Info("master process exit")
			os.Exit(0)
		}
	}
}

func (m *procMaster) fork() {
	log.Info("fork new process")
	cmd := exec.Command(os.Args[0])
	cmd.Args = os.Args
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = m.extraFiles
	e := os.Environ()
	e = append(e, forkEnv+"=1")
	e = append(e, fdNumEnv+"="+strconv.Itoa(len(m.extraFiles)))
	cmd.Env = e
	err := cmd.Start()
	if err != nil {
		log.Fatal("fork new process failed：", errors.Detail(err))
	}
	m.slaveProcs = append(m.slaveProcs, cmd)
	log.Info("fork new process success")
	go func() {
		err := cmd.Wait()
		m.lock.Lock()
		for i := range m.slaveProcs {
			if cmd.Process.Pid == m.slaveProcs[i].Process.Pid {
				m.slaveProcs = append(m.slaveProcs[:i], m.slaveProcs[i+1:]...)
				break
			}
		}
		if len(m.slaveProcs) == 0 {
			log.Fatal("all slave process exit")
		}
		m.lock.Unlock()
		if err != nil {
			log.Error("wait for children process exit error: ", errors.Detail(err))
		}
		log.Info("wait for children process exit success")
	}()
}
