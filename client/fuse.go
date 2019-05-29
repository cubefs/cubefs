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
	"flag"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	cfs "github.com/chubaofs/chubaofs/client/fs"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

const (
	MaxReadAhead = 512 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	configFile    = flag.String("c", "", "FUSE client config file")
	configVersion = flag.Bool("v", false, "show version")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *configVersion {
		fmt.Printf("ChubaoFS Client\n")
		fmt.Printf("Branch: %s\n", BranchName)
		fmt.Printf("Commit: %s\n", CommitID)
		fmt.Printf("Build: %s %s %s %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
		os.Exit(0)
	}

	cfg := config.LoadConfigFile(*configFile)
	if err := Mount(cfg); err != nil {
		fmt.Println("Mount failed: ", err)
		os.Exit(1)
	}

	fmt.Println("Done!")
}

// Mount mounts the volume.
func Mount(cfg *config.Config) (err error) {
	mnt := cfg.GetString("mountPoint")
	volname := cfg.GetString("volName")
	owner := cfg.GetString("owner")
	master := cfg.GetString("masterAddr")
	logpath := cfg.GetString("logDir")
	loglvl := cfg.GetString("logLevel")
	profport := cfg.GetString("profPort")
	icacheTimeout := ParseConfigString(cfg, "icacheTimeout")
	lookupValid := ParseConfigString(cfg, "lookupValid")
	attrValid := ParseConfigString(cfg, "attrValid")
	enSyncWrite := ParseConfigString(cfg, "enSyncWrite")
	autoInvalData := ParseConfigString(cfg, "autoInvalData")
	umpDatadir := cfg.GetString("warnLogDir")

	if mnt == "" || volname == "" || owner == "" || master == "" {
		return errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", mnt, volname, owner, master))
	}

	level := ParseLogLevel(loglvl)
	_, err = log.InitLog(logpath, LoggerPrefix, level, nil)
	if err != nil {
		return err
	}
	defer log.LogFlush()

	super, err := cfs.NewSuper(volname, owner, master, icacheTimeout, lookupValid, attrValid, enSyncWrite)
	if err != nil {
		log.LogError(errors.Stack(err))
		return err
	}

	c, err := fuse.Mount(
		mnt,
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.AutoInvalData(autoInvalData),
		fuse.FSName("chubaofs-"+volname),
		fuse.LocalVolume(),
		fuse.VolumeName("chubaofs-"+volname))

	if err != nil {
		return err
	}

	defer c.Close()

	go func() {
		fmt.Println(http.ListenAndServe(":"+profport, nil))
	}()

	ump.InitUmp(fmt.Sprintf("%v_%v", super.ClusterName(), ModuleName), umpDatadir)
	exporter.Init(super.ClusterName(), ModuleName, cfg)

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}

// ParseConfigString returns the value of the given key in the config.
func ParseConfigString(cfg *config.Config, keyword string) int64 {
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
func ParseLogLevel(loglvl string) log.Level {
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
