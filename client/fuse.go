// Copyright 2018 The Containerfs Authors.
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
// Therefore operations to "/mnt" are routed to the baudstorage.
//

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/tiglabs/containerfs/third_party/fuse"
	"github.com/tiglabs/containerfs/third_party/fuse/fs"

	bdfs "github.com/tiglabs/containerfs/client/fs"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	"strconv"
)

const (
	MaxReadAhead = 512 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"

	UmpModuleName = "fuseclient"
)

var (
	configFile = flag.String("c", "", "FUSE client config file")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	ump.InitUmp(UmpModuleName)
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)
	if err := Mount(cfg); err != nil {
		fmt.Println("Mount failed: ", err)
		os.Exit(1)
	}

	fmt.Println("Done!")
}

func Mount(cfg *config.Config) error {
	mnt := cfg.GetString("mountpoint")
	volname := cfg.GetString("volname")
	master := cfg.GetString("master")
	logpath := cfg.GetString("logpath")
	loglvl := cfg.GetString("loglvl")
	profport := cfg.GetString("profport")

	bufferSizeStr := cfg.GetString("bufferSize")
	var bufferSize int
	if bufferSizeStr == "" {
		bufferSize = 3 * util.MB
	} else {
		var err error
		bufferSize, err = strconv.Atoi(bufferSizeStr)
		if err != nil {
			bufferSize = 3 * util.MB
		}
	}
	fmt.Println(fmt.Sprintf("bufferSize [%v]", bufferSize))

	icacheTimeout := cfg.GetInt("icacheTimeout")
	fmt.Println(fmt.Sprintf("icacheTimeout [%v]", icacheTimeout))

	c, err := fuse.Mount(
		mnt,
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.FSName("cfs-"+volname),
		fuse.LocalVolume(),
		fuse.VolumeName("cfs-"+volname))

	if err != nil {
		return err
	}
	defer c.Close()

	level := ParseLogLevel(loglvl)
	_, err = log.InitLog(path.Join(logpath, LoggerDir), LoggerPrefix, level)
	if err != nil {
		return err
	}
	defer log.LogFlush()

	super, err := bdfs.NewSuper(volname, master, icacheTimeout)
	if err != nil {
		return err
	}

	go func() {
		fmt.Println(http.ListenAndServe(":"+profport, nil))
	}()

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}

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
