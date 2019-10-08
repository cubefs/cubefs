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

//
// Usage: ./client -c fuse.json &
//
// Default mountpoint is specified in fuse.json, which is "/mnt".
//

import (
	"context"
	"flag"
	"fmt"
	syslog "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/jacobsa/daemonize"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	cfs "github.com/chubaofs/chubaofs/clientv2/fs"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	MaxReadAhead = 512 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"
	LoggerOutput = "output.log"

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configVersion    = flag.Bool("v", false, "show version")
	configForeground = flag.Bool("f", false, "run foreground")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *configVersion {
		fmt.Printf("ChubaoFS Client v2\n")
		fmt.Printf("Branch: %s\n", BranchName)
		fmt.Printf("Commit: %s\n", CommitID)
		fmt.Printf("Build: %s %s %s %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
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
			fmt.Printf("Mount failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	opt, err := parseMountOption(cfg)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	exporter.Init(ModuleName, opt.Config)

	level := parseLogLevel(opt.Loglvl)
	_, err = log.InitLog(opt.Logpath, LoggerPrefix, level, nil)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	outputFilePath := path.Join(opt.Logpath, LoggerPrefix, LoggerOutput)
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)

	if err = syscall.Dup2(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	registerInterceptedSignal(opt.MountPoint)

	mfs, err := mount(opt)
	if err != nil {
		log.LogFlush()
		daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		daemonize.SignalOutcome(nil)
	}

	if err = mfs.Join(context.Background()); err != nil {
		log.LogFlush()
		syslog.Printf("mfs Joint returns error: %v", err)
		os.Exit(1)
	}
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

func mount(opt *cfs.MountOption) (*fuse.MountedFileSystem, error) {
	super, err := cfs.NewSuper(opt)
	if err != nil {
		log.LogError(errors.Stack(err))
		return nil, err
	}

	go func() {
		http.HandleFunc(log.SetLogLevelPath, log.SetLogLevel)
		fmt.Println(http.ListenAndServe(":"+opt.Profport, nil))
	}()

	exporter.RegistConsul(super.ClusterName(), ModuleName, opt.Config)

	server := fuseutil.NewFileSystemServer(super)
	mntcfg := &fuse.MountConfig{
		FSName:                  "chubaofs-" + opt.Volname,
		Subtype:                 "chubaofs",
		ReadOnly:                opt.Rdonly,
		DisableWritebackCaching: true,
	}

	if opt.WriteCache {
		mntcfg.DisableWritebackCaching = false
	}

	// define extra options
	mntcfg.Options = make(map[string]string)
	mntcfg.Options["allow_other"] = ""

	mfs, err := fuse.Mount(opt.MountPoint, server, mntcfg)
	if err != nil {
		return nil, err
	}

	return mfs, nil
}

func registerInterceptedSignal(mnt string) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigC
		syslog.Printf("Killed due to a received signal (%v)\n", sig)
	}()
}

func parseMountOption(cfg *config.Config) (*cfs.MountOption, error) {
	var err error
	opt := new(cfs.MountOption)
	opt.Config = cfg

	rawmnt := cfg.GetString(proto.MountPoint)
	opt.MountPoint, err = filepath.Abs(rawmnt)
	if err != nil {
		return nil, errors.Trace(err, "invalide mount point (%v) ", rawmnt)
	}

	opt.Volname = cfg.GetString(proto.VolName)
	opt.Owner = cfg.GetString(proto.Owner)
	opt.Master = cfg.GetString(proto.MasterAddr)
	opt.Logpath = cfg.GetString(proto.LogDir)
	opt.Loglvl = cfg.GetString(proto.LogLevel)
	opt.Profport = cfg.GetString(proto.ProfPort)
	opt.IcacheTimeout = parseConfigString(cfg, proto.IcacheTimeout)
	opt.LookupValid = parseConfigString(cfg, proto.LookupValid)
	opt.AttrValid = parseConfigString(cfg, proto.AttrValid)
	opt.EnSyncWrite = parseConfigString(cfg, proto.EnSyncWrite)
	opt.UmpDatadir = cfg.GetString(proto.WarnLogDir)
	opt.Rdonly = cfg.GetBool(proto.Rdonly)
	opt.WriteCache = cfg.GetBool(proto.WriteCache)
	opt.KeepCache = cfg.GetBool(proto.KeepCache)

	if opt.MountPoint == "" || opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", opt.MountPoint, opt.Volname, opt.Owner, opt.Master))
	}

	return opt, nil
}

func parseConfigString(cfg *config.Config, keyword string) int64 {
	var ret int64 = -1
	rawstr := cfg.GetString(keyword)
	if rawstr != "" {
		val, err := strconv.Atoi(rawstr)
		if err == nil {
			ret = int64(val)
			fmt.Println(fmt.Sprintf("keyword[%v] value[%v]", keyword, ret))
		}
	}
	return ret
}

// ParseLogLevel returns the log level based on the given string.
func parseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
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
	return level
}
