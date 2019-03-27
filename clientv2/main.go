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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	cfs "github.com/chubaofs/chubaofs/clientv2/fs"
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

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"
)

var (
	Version = "0.01"
)

var (
	configFile    = flag.String("c", "", "FUSE client config file")
	configVersion = flag.Bool("v", false, "show version")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *configVersion {
		fmt.Printf("CFS client v2 verson: %s\n", Version)
		os.Exit(0)
	}

	cfg := config.LoadConfigFile(*configFile)
	if err := Mount(cfg); err != nil {
		fmt.Println("Failed: ", err)
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
	rdonly := cfg.GetBool("rdonly")
	icacheTimeout := ParseConfigString(cfg, "icacheTimeout")
	lookupValid := ParseConfigString(cfg, "lookupValid")
	attrValid := ParseConfigString(cfg, "attrValid")
	enSyncWrite := ParseConfigString(cfg, "enSyncWrite")
	//autoInvalData := ParseConfigString(cfg, "autoInvalData")

	if mnt == "" || volname == "" || owner == "" || master == "" {
		return errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", mnt, volname, owner, master))
	}

	level := ParseLogLevel(loglvl)
	_, err = log.InitLog(path.Join(logpath, LoggerDir), LoggerPrefix, level, nil)
	if err != nil {
		return err
	}
	defer log.LogFlush()

	super, err := cfs.NewSuper(volname, owner, master, icacheTimeout, lookupValid, attrValid, enSyncWrite)
	if err != nil {
		log.LogError(errors.Stack(err))
		return err
	}

	go func() {
		fmt.Println(http.ListenAndServe(":"+profport, nil))
	}()

	exporter.Init(super.ClusterName(), ModuleName, cfg)

	server := fuseutil.NewFileSystemServer(super)
	mntcfg := &fuse.MountConfig{
		FSName:                  "chubaofs-" + volname,
		Subtype:                 "chubaofs",
		ReadOnly:                rdonly,
		DisableWritebackCaching: true,
	}

	// define extra options
	mntcfg.Options = make(map[string]string)
	mntcfg.Options["allow_other"] = ""

	mfs, err := fuse.Mount(mnt, server, mntcfg)
	if err != nil {
		return err
	}

	return mfs.Join(context.Background())
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
