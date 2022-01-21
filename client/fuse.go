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

import (
	"bytes"
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
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	cfs "github.com/chubaofs/chubaofs/client/fs"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	_ "github.com/chubaofs/chubaofs/util/log/http" // HTTP APIs for logging control
	sysutil "github.com/chubaofs/chubaofs/util/sys"
	"github.com/chubaofs/chubaofs/util/tracing"
	_ "github.com/chubaofs/chubaofs/util/tracing/http" // HTTP APIs for tracing
	"github.com/chubaofs/chubaofs/util/ump"
	"github.com/chubaofs/chubaofs/util/version"
	"github.com/jacobsa/daemonize"
)

const (
	MaxReadAhead = 512 * 1024

	defaultRlimit uint64 = 1024000
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"
	LoggerOutput = "output.log"

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"

	ControlCommandSetRate      = "/rate/set"
	ControlCommandGetRate      = "/rate/get"
	ControlCommandGetOpRate    = "/opRate/get"
	ControlCommandFreeOSMemory = "/debug/freeosmemory"
	ControlCommandTracing      = "/tracing"
	Role                       = "Client"
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

var GlobalMountOptions []proto.MountOption

func init() {
	GlobalMountOptions = proto.NewMountOptions()
	proto.InitMountOptions(GlobalMountOptions)
}

func main() {
	flag.Parse()

	if *configVersion {
		fmt.Print(proto.DumpVersion(Role, BranchName, CommitID, BuildTime))
		os.Exit(0)
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Mount failed.\n%s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	cfg, _ := config.LoadConfigFile(*configFile)
	opt, err := parseMountOption(cfg)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	if opt.MaxCPUs > 0 {
		runtime.GOMAXPROCS(int(opt.MaxCPUs))
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	//Init tracing
	closer := tracing.TraceInit(ModuleName, cfg.GetString(config.CfgTracingsamplerType), cfg.GetFloat(config.CfgTracingsamplerParam), cfg.GetString(config.CfgTracingReportAddr))
	defer func() {
		_ = closer.Close()
	}()

	level := parseLogLevel(opt.Loglvl)
	_, err = log.InitLog(opt.Logpath, opt.Volname, level, log.NewClientLogRotate())
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	outputFilePath := path.Join(opt.Logpath, opt.Volname, LoggerOutput)
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

	syslog.Println(dumpVersion())
	syslog.Println("*** Final Mount Options ***")
	for _, o := range GlobalMountOptions {
		syslog.Println(o)
	}
	syslog.Println("*** End ***")

	changeRlimit(defaultRlimit)

	if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	registerInterceptedSignal(opt)

	if err = checkPermission(opt); err != nil {
		syslog.Println("check permission failed: ", err)
		log.LogFlush()
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	// check volume mutex is whether open, if true, apply volume mutex
	if err = checkVolWriteMutex(opt); err != nil {
		syslog.Println("check volume mutex permission failed: ", err)
		log.LogFlush()
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer releaseVolWriteMutex(opt)

	fsConn, super, err := mount(opt)
	if err != nil {
		syslog.Println("mount failed: ", err)
		log.LogFlush()
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		_ = daemonize.SignalOutcome(nil)
	}
	defer fsConn.Close()

	exporter.Init(super.ClusterName(), ModuleName, cfg)
	exporter.RegistConsul(cfg)

	// report client version
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	versionInfo := proto.DumpVersion(ModuleName, BranchName, CommitID, BuildTime)
	go version.ReportVersionSchedule(cfg, masters, versionInfo)

	if err = fs.Serve(fsConn, super); err != nil {
		log.LogFlush()
		syslog.Printf("fs Serve returns err(%v)", err)
		os.Exit(1)
	}

	<-fsConn.Ready
	if fsConn.MountError != nil {
		log.LogFlush()
		syslog.Printf("fs Serve returns err(%v)\n", err)
		os.Exit(1)
	}
}

func dumpVersion() string {
	return fmt.Sprintf("ChubaoFS Client\nBranch: %s\nCommit: %s\nBuild: %s %s %s %s\n", BranchName, CommitID, runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
}

func startDaemon() error {
	cmdPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path, err(%v)", err)
	}

	if len(os.Args) <= 1 {
		return fmt.Errorf("startDaemon failed: cannot use null arguments")
	}

	args := []string{"-f"}
	args = append(args, os.Args[1:]...)

	if *configFile != "" {
		configPath, err := filepath.Abs(*configFile)
		if err != nil {
			return fmt.Errorf("startDaemon failed: cannot get absolute command path of config file(%v) , err(%v)", *configFile, err)
		}
		for i := 0; i < len(args); i++ {
			if args[i] == "-c" {
				// Since *configFile is not "", the (i+1)th argument must be the config file path
				args[i+1] = configPath
				break
			}
		}
	}

	env := os.Environ()

	// add GODEBUG=madvdontneed=1 environ, to make sysUnused uses madvise(MADV_DONTNEED) to signal the kernel that a
	// range of allocated memory contains unneeded data.
	env = append(env, "GODEBUG=madvdontneed=1")
	buf := new(bytes.Buffer)
	err = daemonize.Run(cmdPath, args, env, buf)
	if err != nil {
		if buf.Len() > 0 {
			fmt.Println(buf.String())
		}
		return fmt.Errorf("startDaemon failed.\ncmd(%v)\nargs(%v)\nerr(%v)\n", cmdPath, args, err)
	}

	return nil
}

func mount(opt *proto.MountOptions) (fsConn *fuse.Conn, super *cfs.Super, err error) {
	super, err = cfs.NewSuper(opt)
	if err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	http.HandleFunc(ControlCommandSetRate, super.SetRate)
	http.HandleFunc(ControlCommandGetRate, super.GetRate)
	http.HandleFunc(ControlCommandGetOpRate, super.GetOpRate)
	http.HandleFunc(ControlCommandFreeOSMemory, freeOSMemory)
	http.HandleFunc(log.GetLogPath, log.GetLog)

	go func() {
		if opt.Profport != "" {
			syslog.Println("Start pprof with port:", opt.Profport)
			if err := http.ListenAndServe(":"+opt.Profport, nil); err == nil {
				return
			}
		}

		syslog.Printf("Start with config pprof[%v] falied, try %v to %v\n", opt.Profport, log.DefaultProfPort,
			log.MaxProfPort)

		for port := log.DefaultProfPort; port <= log.MaxProfPort; port++ {
			syslog.Println("Start pprof with port:", port)
			if err := http.ListenAndServe(":"+strconv.Itoa(port), nil); err != nil {
				syslog.Println("Start pprof err: ", err)
				continue
			}
			break
		}
	}()

	if err = ump.InitUmp(fmt.Sprintf("%v_%v_%v", super.ClusterName(), super.VolName(), ModuleName), "jdos_chubaofs_node"); err != nil {
		return
	}

	options := []fuse.MountOption{
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.AutoInvalData(opt.AutoInvalData),
		fuse.FSName("chubaofs-" + opt.Volname),
		fuse.LocalVolume(),
		fuse.VolumeName("chubaofs-" + opt.Volname)}

	if opt.Rdonly {
		options = append(options, fuse.ReadOnly())
	}

	if opt.WriteCache {
		options = append(options, fuse.WritebackCache())
	}

	if opt.EnablePosixACL {
		options = append(options, fuse.PosixACL())
	}

	fsConn, err = fuse.Mount(opt.MountPoint, options...)
	return
}

func registerInterceptedSignal(opt *proto.MountOptions) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigC
		syslog.Printf("Killed due to a received signal (%v)\n", sig)
		// release volume write mutex
		releaseVolWriteMutex(opt)
		os.Exit(1)
	}()
}

func parseMountOption(cfg *config.Config) (*proto.MountOptions, error) {
	var err error
	opt := new(proto.MountOptions)

	proto.ParseMountOptions(GlobalMountOptions, cfg)

	opt.MountPoint = GlobalMountOptions[proto.MountPoint].GetString()
	opt.Volname = GlobalMountOptions[proto.VolName].GetString()
	opt.Owner = GlobalMountOptions[proto.Owner].GetString()
	opt.Master = GlobalMountOptions[proto.Master].GetString()
	opt.Logpath = GlobalMountOptions[proto.LogDir].GetString()
	opt.Loglvl = GlobalMountOptions[proto.LogLevel].GetString()
	opt.Profport = GlobalMountOptions[proto.ProfPort].GetString()
	opt.IcacheTimeout = GlobalMountOptions[proto.IcacheTimeout].GetInt64()
	opt.LookupValid = GlobalMountOptions[proto.LookupValid].GetInt64()
	opt.AttrValid = GlobalMountOptions[proto.AttrValid].GetInt64()
	opt.ReadRate = GlobalMountOptions[proto.ReadRate].GetInt64()
	opt.WriteRate = GlobalMountOptions[proto.WriteRate].GetInt64()
	opt.EnSyncWrite = GlobalMountOptions[proto.EnSyncWrite].GetInt64()
	opt.AutoInvalData = GlobalMountOptions[proto.AutoInvalData].GetInt64()
	opt.Rdonly = GlobalMountOptions[proto.Rdonly].GetBool()
	opt.WriteCache = GlobalMountOptions[proto.WriteCache].GetBool()
	opt.KeepCache = GlobalMountOptions[proto.KeepCache].GetBool()
	opt.FollowerRead = GlobalMountOptions[proto.FollowerRead].GetBool()
	opt.Authenticate = GlobalMountOptions[proto.Authenticate].GetBool()
	if opt.Authenticate {
		opt.TicketMess.ClientKey = GlobalMountOptions[proto.ClientKey].GetString()
		ticketHostConfig := GlobalMountOptions[proto.TicketHost].GetString()
		ticketHosts := strings.Split(ticketHostConfig, ",")
		opt.TicketMess.TicketHosts = ticketHosts
		opt.TicketMess.EnableHTTPS = GlobalMountOptions[proto.EnableHTTPS].GetBool()
		if opt.TicketMess.EnableHTTPS {
			opt.TicketMess.CertFile = GlobalMountOptions[proto.CertFile].GetString()
		}
	}
	opt.TokenKey = GlobalMountOptions[proto.TokenKey].GetString()
	opt.AccessKey = GlobalMountOptions[proto.AccessKey].GetString()
	opt.SecretKey = GlobalMountOptions[proto.SecretKey].GetString()
	opt.DisableDcache = GlobalMountOptions[proto.DisableDcache].GetBool()
	opt.SubDir = GlobalMountOptions[proto.SubDir].GetString()
	opt.AutoMakeSubDir = GlobalMountOptions[proto.AutoMakeSubDir].GetBool()
	opt.FsyncOnClose = GlobalMountOptions[proto.FsyncOnClose].GetBool()
	opt.MaxCPUs = GlobalMountOptions[proto.MaxCPUs].GetInt64()
	opt.EnableXattr = GlobalMountOptions[proto.EnableXattr].GetBool()
	opt.NearRead = GlobalMountOptions[proto.NearRead].GetBool()
	//opt.AlignSize = GlobalMountOptions[proto.AlignSize].GetInt64()
	//opt.MaxExtentNumPerAlignArea = GlobalMountOptions[proto.MaxExtentNumPerAlignArea].GetInt64()
	//opt.ForceAlignMerge = GlobalMountOptions[proto.ForceAlignMerge].GetBool()
	opt.EnablePosixACL = GlobalMountOptions[proto.EnablePosixACL].GetBool()
	opt.ExtentSize = GlobalMountOptions[proto.ExtentSize].GetInt64()
	opt.AutoFlush = GlobalMountOptions[proto.AutoFlush].GetBool()
	opt.DelProcessPath = GlobalMountOptions[proto.DeleteProcessAbsoPath].GetString()
	opt.NoBatchGetInodeOnReaddir = GlobalMountOptions[proto.NoBatchGetInodeOnReaddir].GetBool()

	if opt.MountPoint == "" || opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", opt.MountPoint, opt.Volname, opt.Owner, opt.Master))
	}

	absMnt, err := filepath.Abs(opt.MountPoint)
	if err != nil {
		return nil, errors.Trace(err, "invalide mount point (%v) ", opt.MountPoint)
	}
	opt.MountPoint = absMnt
	return opt, nil
}

func checkPermission(opt *proto.MountOptions) (err error) {
	var mc = master.NewMasterClientFromString(opt.Master, false)

	// Check token permission
	var info *proto.VolStatInfo
	if info, err = mc.ClientAPI().GetVolumeStat(opt.Volname); err != nil {
		return
	}
	if info.EnableToken {
		var token *proto.Token
		if token, err = mc.ClientAPI().GetToken(opt.Volname, opt.TokenKey); err != nil {
			log.LogWarnf("checkPermission: get token type failed: volume(%v) tokenKey(%v) err(%v)",
				opt.Volname, opt.TokenKey, err)
			return
		}
		log.LogInfof("checkPermission: get token: token(%v)", token)
		opt.Rdonly = token.TokenType == int8(proto.ReadOnlyToken) || opt.Rdonly
	}

	// Check user access policy is enabled
	if opt.AccessKey != "" {
		var userInfo *proto.UserInfo
		if userInfo, err = mc.UserAPI().GetAKInfo(opt.AccessKey); err != nil {
			return
		}
		if userInfo.SecretKey != opt.SecretKey {
			err = proto.ErrNoPermission
			return
		}
		var policy = userInfo.Policy
		if policy.IsOwn(opt.Volname) {
			return
		}
		if policy.IsAuthorized(opt.Volname, "", proto.POSIXWriteAction) &&
			policy.IsAuthorized(opt.Volname, "", proto.POSIXReadAction) {
			return
		}
		if policy.IsAuthorized(opt.Volname, "", proto.POSIXReadAction) &&
			!policy.IsAuthorized(opt.Volname, "", proto.POSIXWriteAction) {
			opt.Rdonly = true
			return
		}
		err = proto.ErrNoPermission
		return
	}
	return
}

func checkVolWriteMutex(opt *proto.MountOptions) (err error) {
	if opt.Rdonly {
		return
	}
	var mc = master.NewMasterClientFromString(opt.Master, false)
	err = mc.ClientAPI().ApplyVolMutex(opt.Volname)
	if err == nil || err == proto.ErrVolWriteMutexUnable {
		return nil
	}
	return
}

func releaseVolWriteMutex(opt *proto.MountOptions) (err error) {
	if opt.Rdonly {
		return
	}
	var mc = master.NewMasterClientFromString(opt.Master, false)
	err = mc.ClientAPI().ReleaseVolMutex(opt.Volname)
	if err == nil || err == proto.ErrVolWriteMutexUnable {
		return nil
	}
	return
}

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

func changeRlimit(val uint64) {
	rlimit := &syscall.Rlimit{Max: val, Cur: val}
	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, rlimit)
	if err != nil {
		syslog.Printf("Failed to set rlimit to %v \n", val)
	} else {
		syslog.Printf("Successfully set rlimit to %v \n", val)
	}
}

func freeOSMemory(w http.ResponseWriter, r *http.Request) {
	debug.FreeOSMemory()
}
