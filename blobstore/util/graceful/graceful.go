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
	"syscall"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

/*
	usage:
	1.start process
		./main -f main.conf
	2.get process pid
		ps afxu|grep main
	3.reload command
		kill -USR2 <pid>
	4.shutdown command
		kill <pid>
*/

const (
	forkEnv  = "GRACEFUL-FORK"
	fdNumEnv = "GRACEFUL-FD-NUM"

	listenFdStart = 3
)

var receiveSigs = []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGUSR2}

type process interface {
	run()
}

type programEntry func(*State)

// State hold the server running info, such as listener fd and listen address, close channel that use for the shutdown notification
type State struct {
	ListenAddresses []string
	ListenerFds     []net.Listener
	CloseCh         chan interface{}
}

type Config struct {
	Entry           programEntry
	ListenAddresses []string
}

func Run(config *Config) {
	var process process
	checkParams(config)
	if fork := os.Getenv(forkEnv); fork == "1" {
		process = newProcSlave(config)
	} else {
		process = newProcMaster(config)
	}
	process.run()
}

func checkParams(config *Config) {
	if len(config.ListenAddresses) < 1 {
		log.Fatal("invalid params ListenAddresses")
	}
}
