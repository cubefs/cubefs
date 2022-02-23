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
	"encoding/json"
	"flag"
	"fmt"
	syslog "log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/monitor"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/console"
	"github.com/chubaofs/chubaofs/datanode"
	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/metanode"
	"github.com/chubaofs/chubaofs/objectnode"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	_ "github.com/chubaofs/chubaofs/util/log/http" // HTTP API for logging control
	sysutil "github.com/chubaofs/chubaofs/util/sys"
	"github.com/chubaofs/chubaofs/util/tracing"
	_ "github.com/chubaofs/chubaofs/util/tracing/http" // HTTP API for tracing
	"github.com/chubaofs/chubaofs/util/ump"
	"github.com/chubaofs/chubaofs/util/version"
	"github.com/jacobsa/daemonize"
)

const (
	ConfigKeyRole       = "role"
	ConfigKeyLogDir     = "logDir"
	ConfigKeyLogLevel   = "logLevel"
	ConfigKeyProfPort   = "prof"
	ConfigKeyWarnLogDir = "warnLogDir"
)

const (
	RoleMaster  = "master"
	RoleMeta    = "metanode"
	RoleData    = "datanode"
	RoleObject  = "objectnode"
	RoleConsole = "console"
	RoleMonitor = "monitor"
)

const (
	ModuleMaster  = "master"
	ModuleMeta    = "metaNode"
	ModuleData    = "dataNode"
	ModuleObject  = "objectNode"
	ModuleConsole = "console"
	ModuleMonitor = "monitor"
)

const (
	LoggerOutput = "output.log"
)

const (
	MaxThread = 40000
)

const (
	HTTPAPIPATHStatus = "/status"
)

var (
	startComplete         = false
	startCost             time.Duration
	HttpAPIPathSetTracing = "/tracing/set"
	HttpAPIPathGetTracing = "/tracing/get"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	configFile       = flag.String("c", "", "config file path")
	configVersion    = flag.Bool("v", false, "show version")
	configForeground = flag.Bool("f", false, "run foreground")
)

func interceptSignal(s common.Server) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	syslog.Println("action[interceptSignal] register system signal.")
	go func() {
		sig := <-sigC
		syslog.Printf("action[interceptSignal] received signal: %s.", sig.String())
		s.Shutdown()
	}()
}

func modifyOpenFiles() (err error) {
	var rLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Getting Rlimit %v", err.Error())
	}
	syslog.Println(rLimit)
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
	syslog.Println("Rlimit Final", rLimit)
	return
}

func modifyMaxThreadLimit() {
	debug.SetMaxThreads(MaxThread)
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	Version := proto.DumpVersion("Server", BranchName, CommitID, BuildTime)
	if *configVersion {
		fmt.Printf("%v", Version)
		return nil
	}

	/*
	 * LoadConfigFile should be checked before start daemon, since it will
	 * call os.Exit() w/o notifying the parent process.
	 */
	cfg, err := config.LoadConfigFile(*configFile)
	if err != nil {
		_ = daemonize.SignalOutcome(err)
		return err
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Server start failed: %v\n", err)
			return err
		}
		return nil
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	role := cfg.GetString(ConfigKeyRole)
	logDir := cfg.GetString(ConfigKeyLogDir)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	profPort := cfg.GetString(ConfigKeyProfPort)

	//Init tracing
	closer := tracing.TraceInit(role, cfg.GetString(config.CfgTracingsamplerType), cfg.GetFloat(config.CfgTracingsamplerParam), cfg.GetString(config.CfgTracingReportAddr))
	defer func() {
		_ = closer.Close()
	}()

	// Init server instance with specified role configuration.
	var (
		server common.Server
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
	case RoleObject:
		server = objectnode.NewServer()
		module = ModuleObject
	case RoleConsole:
		server = console.NewServer()
		module = ModuleConsole
	case RoleMonitor:
		server = monitor.NewServer()
		module = ModuleMonitor
	default:
		_ = daemonize.SignalOutcome(fmt.Errorf("Fatal: role mismatch: %v", role))
		return fmt.Errorf("unknown role: %v", role)
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

	_, err = log.InitLog(logDir, module, level, nil)
	if err != nil {
		_ = daemonize.SignalOutcome(fmt.Errorf("Fatal: failed to init log - %v", err))
		return err
	}
	defer log.LogFlush()

	// Init output file
	outputFilePath := path.Join(logDir, module, LoggerOutput)
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		_ = daemonize.SignalOutcome(err)
		return err
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)

	if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		log.LogErrorf("redirect stderr to %v failed: %v", outputFilePath, err)
		log.LogFlush()
		syslog.Printf("Fatal: redirect stderr to %v failed: %v", outputFilePath, err)
		_ = daemonize.SignalOutcome(fmt.Errorf("refiect stderr to %v failed: %v", outputFilePath, err))
		return err
	}

	syslog.Printf("Hello, ChubaoFS Storage\n%s\n", Version)

	err = modifyOpenFiles()
	if err != nil {
		log.LogErrorf("modify open files limit failed: %v", err)
		log.LogFlush()
		syslog.Printf("Fatal: modify open files limit failed: %v ", err)
		_ = daemonize.SignalOutcome(fmt.Errorf("modify open files limit failed: %v", err))
		return err
	}
	// Setup thread limit from 10,000 to 40,000
	modifyMaxThreadLimit()

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err = ump.InitUmp(role, "jdos_chubaofs-node"); err != nil {
		log.LogErrorf("init ump failed: %v", err)
		log.LogFlush()
		syslog.Printf("Fatal: init ump failed: %v ", err)
		_ = daemonize.SignalOutcome(fmt.Errorf("init ump failed: %v ", err))
		return err
	}

	var profNetListener net.Listener = nil
	if profPort != "" {
		http.HandleFunc(HTTPAPIPATHStatus, statusHandler)
		// 监听prof端口
		if profNetListener, err = net.Listen("tcp", fmt.Sprintf(":%v", profPort)); err != nil {
			log.LogErrorf("listen prof port %v failed: %v", profPort, err)
			log.LogFlush()
			syslog.Printf("Fatal: listen prof port %v failed: %v", profPort, err)
			_ = daemonize.SignalOutcome(fmt.Errorf("listen prof port %v failed: %v", profPort, err))
			return err
		}
		// 在prof端口监听上启动http API.
		go func() {
			_ = http.Serve(profNetListener, http.DefaultServeMux)
		}()
	}

	interceptSignal(server)

	var startTime = time.Now()
	if err = server.Start(cfg); err != nil {
		log.LogErrorf("start service %v failed: %v", role, err)
		log.LogFlush()
		syslog.Printf("Fatal: failed to start the ChubaoFS %v daemon err %v - ", role, err)
		_ = daemonize.SignalOutcome(fmt.Errorf("Fatal: failed to start the ChubaoFS %v daemon err %v - ", role, err))
		return err
	}
	startComplete = true
	startCost = time.Since(startTime)

	_ = daemonize.SignalOutcome(nil)

	// report server version
	masters := cfg.GetStringSlice(proto.MasterAddr)
	versionInfo := proto.DumpVersion(module, BranchName, CommitID, BuildTime)
	go version.ReportVersionSchedule(cfg, masters, versionInfo)

	// Block main goroutine until server shutdown.
	server.Sync()
	log.LogFlush()

	if profNetListener != nil {
		// 关闭prof端口监听
		_ = profNetListener.Close()
	}

	return nil
}

func startDaemon() error {
	cmdPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path, err(%v)", err)
	}

	configPath, err := filepath.Abs(*configFile)
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path of config file(%v) , err(%v)", *configFile, err)
	}

	args := []string{"-f"}
	args = append(args, "-c")
	args = append(args, configPath)

	env := []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
	}

	err = daemonize.Run(cmdPath, args, env, os.Stdout)
	if err != nil {
		return fmt.Errorf("startDaemon failed: daemon start failed, cmd(%v) args(%v) env(%v) err(%v)\n", cmdPath, args, env, err)
	}

	return nil
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	var resp = struct {
		StartComplete bool
		StarCost      time.Duration
		Version       string
	}{
		StartComplete: startComplete,
		StarCost:      startCost,
		Version:       proto.Version,
	}
	if marshaled, err := json.Marshal(&resp); err == nil {
		_, _ = w.Write(marshaled)
	}
}
