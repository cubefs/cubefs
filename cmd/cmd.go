// Copyright 2018 The Chubao Authors.
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

package main

import (
	"strings"

	"github.com/chubaofs/chubaofs/datanode"
	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/metanode"
	"github.com/chubaofs/chubaofs/util/log"

	"flag"
	"fmt"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/ump"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

const (
	ConfigKeyRole       = "role"
	ConfigKeyLogDir     = "logDir"
	ConfigKeyLogLevel   = "logLevel"
	ConfigKeyProfPort   = "prof"
	ConfigKeyWarnLogDir = "warnLogDir"
)

const (
	RoleMaster = "master"
	RoleMeta   = "metanode"
	RoleData   = "datanode"
)

const (
	ModuleMaster = "master"
	ModuleMeta   = "metaNode"
	ModuleData   = "dataNode"
)

var (
	configFile    = flag.String("c", "", "config file path")
	configVersion = flag.Bool("v", false, "show version")
)

type Server interface {
	Start(cfg *config.Config) error
	Shutdown()
	// Sync will block invoker goroutine until this MetaNode shutdown.
	Sync()
}

func interceptSignal(s Server) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	log.LogInfo("action[interceptSignal] register system signal.")
	go func() {
		sig := <-sigC
		log.LogInfo("action[interceptSignal] received signal: %s.", sig.String())
		s.Shutdown()
	}()
}

func modifyOpenFiles() (err error) {
	var rLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Getting Rlimit %v", err.Error())
	}
	fmt.Println(rLimit)
	rLimit.Max = 1024000
	rLimit.Cur = 1024000
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Setting Rlimit %v", err.Error())
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Getting Rlimit %v", err.Error())
	}
	fmt.Println("Rlimit Final", rLimit)
	return
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("action[main] process panic detail: %v.", r)
			log.LogFlush()
			panic(r)
		}
	}()
	flag.Parse()

	Version := fmt.Sprintf("ChubaoFS Server\nBranch: %s\nCommit: %s\nBuild: %s %s %s %s\n", BranchName, CommitID, runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
	if *configVersion {
		fmt.Printf("%v", Version)
		os.Exit(0)
	}

	err := modifyOpenFiles()
	if err != nil {
		fmt.Println(fmt.Sprintf(err.Error()))
		os.Exit(1)
	}

	log.LogInfof("Hello, ChubaoFS Storage\n%s", Version)
	cfg := config.LoadConfigFile(*configFile)
	role := cfg.GetString(ConfigKeyRole)
	logDir := cfg.GetString(ConfigKeyLogDir)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	profPort := cfg.GetString(ConfigKeyProfPort)
	umpDatadir := cfg.GetString(ConfigKeyWarnLogDir)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())
	ump.InitUmp(role, umpDatadir)
	// Init server instance with specified role configuration.
	var (
		server Server
		module string
	)
	switch role {
	case RoleMeta:
		server = metanode.NewServer()
		module = ModuleMeta
	case RoleMaster:
		server = master.NewServer()
		module = ModuleMaster
	case RoleData:
		server = datanode.NewServer()
		module = ModuleData
	default:
		fmt.Println("Fatal: role mismatch: ", role)
		os.Exit(1)
		return
	}

	// Init logging
	var (
		level log.Level
	)
	switch strings.ToLower(logLevel) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}

	if profPort != "" {
		go func() {
			err = http.ListenAndServe(fmt.Sprintf(":%v", profPort), nil)
			if err != nil {
				fmt.Println(fmt.Sprintf("cannot listen pprof %v err %v", profPort, err.Error()))
				os.Exit(1)
			}
		}()
	}

	if _, err := log.InitLog(logDir, module, level, nil); err != nil {
		fmt.Println("Fatal: failed to init log - ", err)
		os.Exit(1)
		return
	}

	interceptSignal(server)
	err = server.Start(cfg)
	if err != nil {
		fmt.Println(fmt.Sprintf("Fatal: failed to start the ChubaoFS %v daemon err %v - ", role, err))
		log.LogFatal(fmt.Sprintf("Fatal: failed to start the ChubaoFS %v daemon err %v - ", role, err))
		log.LogFlush()
		os.Exit(1)
		return
	}
	// Block main goroutine until server shutdown.
	server.Sync()
	log.LogFlush()
	os.Exit(0)
}
