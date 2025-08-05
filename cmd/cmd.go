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

package main

import (
	"flag"
	"fmt"
	syslog "log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/authnode"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/console"
	"github.com/cubefs/cubefs/datanode"
	"github.com/cubefs/cubefs/flashnode"
	"github.com/cubefs/cubefs/lcnode"
	"github.com/cubefs/cubefs/master"
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/objectnode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	sysutil "github.com/cubefs/cubefs/util/sys"
	"github.com/cubefs/cubefs/util/ump"
	"github.com/jacobsa/daemonize"
)

const (
	ConfigKeyRole                   = "role"
	ConfigKeyLocalIp                = "localIP"
	ConfigKeyBindIp                 = "bindIp"
	ConfigKeyLogDir                 = "logDir"
	ConfigKeyLogLevel               = "logLevel"
	ConfigKeyLogRotateSize          = "logRotateSize"
	ConfigKeyLogRotateHeadRoom      = "logRotateHeadRoom"
	ConfigKeyProfPort               = "prof"
	ConfigKeyWarnLogDir             = "warnLogDir"
	ConfigKeyBuffersTotalLimit      = "buffersTotalLimit"
	ConfigKeyLogLeftSpaceLimitRatio = "logLeftSpaceLimitRatio"
	ConfigKeyEnableLogPanicHook     = "enableLogPanicHook"
)

const (
	RoleMaster    = "master"
	RoleMeta      = "metanode"
	RoleData      = "datanode"
	RoleAuth      = "authnode"
	RoleObject    = "objectnode"
	RoleConsole   = "console"
	RoleLifeCycle = "lcnode"
	RoleFlash     = "flashnode"
)

const (
	ModuleMaster    = "master"
	ModuleMeta      = "metaNode"
	ModuleData      = "dataNode"
	ModuleAuth      = "authNode"
	ModuleObject    = "objectNode"
	ModuleConsole   = "console"
	ModuleLifeCycle = "lcnode"
	ModuleFlash     = "flashNode"
)

const (
	LoggerOutput = "output.log"
)

var (
	configFile       = flag.String("c", "", "config file path")
	configVersion    = flag.Bool("v", false, "show version")
	configForeground = flag.Bool("f", false, "run foreground")
	redirectSTD      = flag.Bool("redirect-std", true, "redirect standard output to file")
)

func interceptSignal(s common.Server) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	syslog.Println("action[interceptSignal] register system signal.")
	go func() {
		for {
			sig := <-sigC
			syslog.Printf("action[interceptSignal] received signal: %s. pid %d", sig.String(), os.Getpid())
			s.Shutdown()
		}
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

func releaseMemory(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	begin := time.Now()
	syslog.Println("releaseMemory begen")
	debug.FreeOSMemory()
	interval := time.Since(begin)
	msg := fmt.Sprintf("Success to release memory using %v", interval)
	syslog.Printf("releaseMemory success, using time: %v", interval)
	w.Write([]byte(msg))
}

func main() {
	flag.Parse()

	Version := proto.DumpVersion("Server")
	if *configVersion {
		fmt.Printf("%v", Version)
		os.Exit(0)
	}

	/*
	 * LoadConfigFile should be checked before start daemon, since it will
	 * call os.Exit() w/o notifying the parent process.
	 */
	cfg, err := config.LoadConfigFile(*configFile)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Server start failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	role := cfg.GetString(ConfigKeyRole)
	bindIp := cfg.GetBool(ConfigKeyBindIp)
	localIp := cfg.GetString(ConfigKeyLocalIp)
	logDir := cfg.GetString(ConfigKeyLogDir)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	logRotateSize := cfg.GetInt64(ConfigKeyLogRotateSize)
	logRotateHeadRoom := cfg.GetInt64(ConfigKeyLogRotateHeadRoom)
	profPort := cfg.GetString(ConfigKeyProfPort)
	umpDatadir := cfg.GetString(ConfigKeyWarnLogDir)
	buffersTotalLimit := cfg.GetInt64(ConfigKeyBuffersTotalLimit)
	logLeftSpaceLimitRatioStr := cfg.GetString(ConfigKeyLogLeftSpaceLimitRatio)
	logLeftSpaceLimitRatio, err := strconv.ParseFloat(logLeftSpaceLimitRatioStr, 64)
	enableLogPanicHook := cfg.GetBool(ConfigKeyEnableLogPanicHook)
	if err != nil || logLeftSpaceLimitRatio <= 0 || logLeftSpaceLimitRatio > 1.0 {
		log.LogWarnf("logLeftSpaceLimitRatio is not a legal float value: %v", err.Error())
		logLeftSpaceLimitRatio = log.DefaultLogLeftSpaceLimitRatio
	}
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
	case RoleAuth:
		server = authnode.NewServer()
		module = ModuleAuth
	case RoleObject:
		server = objectnode.NewServer()
		module = ModuleObject
	case RoleConsole:
		server = console.NewServer()
		module = ModuleConsole
	case RoleLifeCycle:
		server = lcnode.NewServer()
		module = ModuleLifeCycle
	case RoleFlash:
		server = flashnode.NewServer()
		module = ModuleFlash
	default:
		err = errors.NewErrorf("Fatal: role mismatch: %s", role)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
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
	case "critical":
		level = log.CriticalLevel
	default:
		level = log.ErrorLevel
	}
	rotate := log.NewLogRotate()
	if logRotateSize > 0 {
		rotate.SetRotateSizeMb(logRotateSize)
	}
	if logRotateHeadRoom > 0 {
		rotate.SetHeadRoomMb(logRotateHeadRoom)
	}
	_, err = log.InitLog(logDir, module, level, rotate, logLeftSpaceLimitRatio)
	if err != nil {
		err = errors.NewErrorf("Fatal: failed to init log - %v", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()
	if enableLogPanicHook && errors.SupportPanicHook() {
		log.LogWarnf("enable log panic hook")
		err = errors.AtPanic(func() {
			log.LogFlush()
		})
		if err != nil {
			log.LogErrorf("failed to hook go panic")
			err = nil
		}
	}

	_, err = auditlog.InitAuditWithHeadRoom(logDir, module, auditlog.DefaultAuditLogSize, logLeftSpaceLimitRatio, auditlog.DefaultHeadRoom)
	if err != nil {
		err = errors.NewErrorf("Fatal: failed to init audit log - %v", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer auditlog.StopAudit()

	if *redirectSTD {
		// Init output file
		outputFilePath := path.Join(logDir, module, LoggerOutput)
		outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
		if err != nil {
			err = errors.NewErrorf("Fatal: failed to open output path - %v", err)
			fmt.Println(err)
			daemonize.SignalOutcome(err)
			os.Exit(1)
		}
		defer func() {
			outputFile.Sync()
			outputFile.Close()
		}()

		syslog.SetOutput(outputFile)
		if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
			err = errors.NewErrorf("Fatal: failed to redirect fd - %v", err)
			syslog.Println(err)
			daemonize.SignalOutcome(err)
			os.Exit(1)
		}
	}

	if buffersTotalLimit < 0 {
		syslog.Printf("invalid fields, BuffersTotalLimit(%v) must larger or equal than 0\n", buffersTotalLimit)
		return
	}

	proto.InitBufferPool(buffersTotalLimit)
	syslog.Printf("Hello, CubeFS Storage\n%s\n", Version)
	err = modifyOpenFiles()
	if err != nil {
		err = errors.NewErrorf("Fatal: failed to modify open files - %v", err)
		syslog.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	// for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err = ump.InitUmp(role, umpDatadir); err != nil {
		log.LogFlush()
		err = errors.NewErrorf("Fatal: failed to init ump warnLogDir - %v", err)
		syslog.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	if profPort != "" {
		go func() {
			mainMux := http.NewServeMux()
			mux := http.NewServeMux()
			http.HandleFunc(log.SetLogLevelPath, log.SetLogLevel)
			mux.Handle("/debug/pprof", http.HandlerFunc(pprof.Index))
			mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
			mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
			mux.Handle("/debug/", http.HandlerFunc(pprof.Index))
			mux.Handle("/debug/releaseMemory", http.HandlerFunc(releaseMemory))
			mainHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasPrefix(req.URL.Path, "/debug/") {
					mux.ServeHTTP(w, req)
				} else {
					http.DefaultServeMux.ServeHTTP(w, req)
				}
			})
			mainMux.Handle("/", mainHandler)
			addr := fmt.Sprintf(":%v", profPort)
			if bindIp {
				addr = fmt.Sprintf("%v:%v", localIp, profPort)
			}
			e := http.ListenAndServe(fmt.Sprintf("%v", addr), mainMux)

			if e != nil {
				log.LogFlush()
				err = errors.NewErrorf("cannot listen pprof %v err %v", profPort, e)
				syslog.Println(err)
				daemonize.SignalOutcome(err)
				os.Exit(1)
			}
		}()
	}

	interceptSignal(server)
	metricRole := role
	if role == RoleData {
		metricRole = "dataNode"
	}
	exporter.Init(metricRole, cfg)
	versionMetric := exporter.NewVersionMetrics(metricRole)
	go versionMetric.Start()
	defer versionMetric.Stop()
	err = server.Start(cfg)
	if err != nil {
		log.LogFlush()
		err = errors.NewErrorf("Fatal: failed to start the CubeFS %s daemon err %v - ", role, err)
		syslog.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	syslog.Printf("server start success, pid %d, role %s", os.Getpid(), role)
	log.LogDisableStderrOutput()
	err = log.OutputPid(logDir, role)
	if err != nil {
		log.LogFlush()
		err = errors.NewErrorf("Fatal: failed to print pid %s err %v - ", role, err)
		syslog.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	daemonize.SignalOutcome(nil)

	// Block main goroutine until server shutdown.
	server.Sync()
	log.LogFlush()
	os.Exit(0)
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
