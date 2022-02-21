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
	"io/ioutil"
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

	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"

	sysutil "github.com/cubefs/cubefs/util/sys"

	cfs "github.com/cubefs/cubefs/client/fs"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/ump"
	"github.com/jacobsa/daemonize"
)

const (
	MaxReadAhead = 512 * 1024

	defaultRlimit uint64 = 1024000

	UpdateConfInterval = 2 * time.Minute
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"
	LoggerOutput = "output.log"

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"

	ControlCommandSetRate      = "/rate/set"
	ControlCommandGetRate      = "/rate/get"
	ControlCommandFreeOSMemory = "/debug/freeosmemory"
	ControlCommandSuspend      = "/suspend"
	ControlCommandResume       = "/resume"
	Role                       = "Client"

	DefaultIP            = "127.0.0.1"
	DynamicUDSNameFormat = "/tmp/ChubaoFS-fdstore-%v.sock"
	DefaultUDSName       = "/tmp/ChubaoFS-fdstore.sock"

	DefaultLogPath = "/var/log/chubaofs"
)

var (
	configFile           = flag.String("c", "", "FUSE client config file")
	configVersion        = flag.Bool("v", false, "show version")
	configForeground     = flag.Bool("f", false, "run foreground")
	configDynamicUDSName = flag.Bool("n", false, "dynamic unix domain socket filename")
	configRestoreFuse    = flag.Bool("r", false, "restore FUSE instead of mounting")
	configRestoreFuseUDS = flag.String("s", "", "restore socket addr")
	configFuseHttpPort   = flag.String("p", "", "fuse http service port")
)

var GlobalMountOptions []proto.MountOption

func init() {
	GlobalMountOptions = proto.NewMountOptions()
	proto.InitMountOptions(GlobalMountOptions)
}

func createUDS(sockAddr string) (listener net.Listener, err error) {
	var addr *net.UnixAddr

	log.LogInfof("sockaddr: %s\n", sockAddr)

	os.Remove(sockAddr)
	if addr, err = net.ResolveUnixAddr("unix", sockAddr); err != nil {
		log.LogErrorf("cannot resolve unix addr: %v\n", err)
		return
	}

	if listener, err = net.ListenUnix("unix", addr); err != nil {
		log.LogErrorf("cannot create unix domain: %v\n", err)
		return
	}

	if err = os.Chmod(sockAddr, 0666); err != nil {
		log.LogErrorf("failed to chmod socket file: %v\n", err)
		listener.Close()
		return
	}

	return
}

func destroyUDS(listener net.Listener) {
	sockAddr := listener.Addr().String()
	listener.Close()
	os.Remove(sockAddr)
}

func recvFuseFdFromOldClient(udsListener net.Listener) (file *os.File, err error) {
	var conn net.Conn
	var socket *os.File

	if conn, err = udsListener.Accept(); err != nil {
		log.LogErrorf("unix domain accepts fail: %v\n", err)
		return
	}
	defer conn.Close()

	log.LogInfof("a new connection accepted\n")
	unixconn := conn.(*net.UnixConn)
	if socket, err = unixconn.File(); err != nil {
		log.LogErrorf("failed to get socket file: %v\n", err)
		return
	}
	defer socket.Close()

	if file, err = util.RecvFd(socket); err != nil {
		log.LogErrorf("failed to receive fd: %v\n", err)
		return
	}

	log.LogInfof("Received file %s fd %v\n", file.Name(), file.Fd())
	return
}

func sendSuspendRequest(port string, udsListener net.Listener) (err error) {
	var (
		req  *http.Request
		resp *http.Response
		data []byte
	)
	udsFilePath := udsListener.Addr().String()

	url := fmt.Sprintf("http://%s:%s/suspend?sock=%s", DefaultIP, port, udsFilePath)
	if req, err = http.NewRequest("POST", url, nil); err != nil {
		log.LogErrorf("Failed to get new request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/text")

	client := http.DefaultClient
	client.Timeout = 120 * time.Second
	if resp, err = client.Do(req); err != nil {
		log.LogErrorf("Failed to post request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("Failed to read response: %v\n", err)
		return err
	}

	if resp.StatusCode == http.StatusOK {
		log.LogInfof("\n==> %s\n==> Could restore cfs-client now with -r option.\n\n", string(data))
	} else {
		log.LogErrorf("\n==> %s\n==> Status: %s\n\n", string(data), resp.Status)
		return fmt.Errorf(resp.Status)
	}

	return nil
}

func sendResumeRequest(port string) (err error) {
	var (
		req  *http.Request
		resp *http.Response
		data []byte
	)

	url := fmt.Sprintf("http://%s:%s/resume", DefaultIP, port)
	if req, err = http.NewRequest("POST", url, nil); err != nil {
		log.LogErrorf("Failed to get new request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/text")

	client := http.DefaultClient
	if resp, err = client.Do(req); err != nil {
		log.LogErrorf("Failed to post request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("Failed to read response: %v\n", err)
		return err
	}

	log.LogInfof("data: %s\n", string(data))
	return nil
}

func doSuspend(uds string, port string) (*os.File, error) {
	var fud *os.File

	udsListener, err := createUDS(uds)
	if err != nil {
		log.LogErrorf("doSuspend: failed to create UDS: %v\n", err)
		return nil, err
	}
	defer destroyUDS(udsListener)

	if err = sendSuspendRequest(port, udsListener); err != nil {
		sendResumeRequest(port)
		return nil, err
	}

	if fud, err = recvFuseFdFromOldClient(udsListener); err != nil {
		sendResumeRequest(port)
		return nil, err
	}

	return fud, nil
}

func main() {
	flag.Parse()

	if *configVersion {
		fmt.Print(proto.DumpVersion(Role))
		os.Exit(0)
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

	cfg, _ := config.LoadConfigFile(*configFile)
	opt, err := parseMountOption(cfg)

	if err != nil {
		err = errors.NewErrorf("parse mount opt failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	//load  conf from master
	err = loadConfFromMaster(opt)
	if err != nil {
		err = errors.NewErrorf("parse mount opt from master failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	if opt.MaxCPUs > 0 {
		runtime.GOMAXPROCS(int(opt.MaxCPUs))
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	level := parseLogLevel(opt.Loglvl)
	_, err = log.InitLog(opt.Logpath, opt.Volname, level, nil)
	if err != nil {
		err = errors.NewErrorf("Init log dir fail: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	_, err = stat.NewStatistic(opt.Logpath, LoggerPrefix, int64(stat.DefaultStatLogSize),
		stat.DefaultTimeOutUs, true)
	if err != nil {
		err = errors.NewErrorf("Init stat log fail: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	stat.ClearStat()

	proto.InitBufferPool(opt.BuffersTotalLimit)

	outputFilePath := path.Join(opt.Logpath, LoggerPrefix, LoggerOutput)
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		err = errors.NewErrorf("Open output file failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)

	if *configRestoreFuse {
		syslog.Println("Need restore fuse")
		opt.NeedRestoreFuse = true
	}

	syslog.Println(proto.DumpVersion(Role))
	syslog.Println("*** Final Mount Options ***")
	for _, o := range GlobalMountOptions {
		syslog.Println(o)
	}
	syslog.Println("*** End ***")

	changeRlimit(defaultRlimit)

	if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		err = errors.NewErrorf("Redirect fd failed: %v\n", err)
		syslog.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	registerInterceptedSignal(opt.MountPoint)

	if err = checkPermission(opt); err != nil {
		err = errors.NewErrorf("check permission failed: %v", err)
		syslog.Println(err)
		log.LogFlush()
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	var fud *os.File
	if opt.NeedRestoreFuse && *configFuseHttpPort != "" {
		log.LogInfof("Suspend/Restore by self\n")
		var udsName string
		if *configDynamicUDSName {
			udsName = fmt.Sprintf(DynamicUDSNameFormat, os.Getpid())
		} else {
			udsName = DefaultUDSName
		}

		// Tell old cfs-client to suspend first. This should be done
		// before mount() to avoid pprof port conflict between old and
		// new cfs-clients.
		if fud, err = doSuspend(udsName, *configFuseHttpPort); err != nil {
			log.LogErrorf("Failed to tell old cfs-client to suspend: %v\n", err)
			syslog.Printf("Error: Failed to tell old cfs-client to suspend: %v\n", err)
			log.LogFlush()
			_ = daemonize.SignalOutcome(err)
			os.Exit(1)
		}
	}

	fsConn, super, err := mount(opt)
	if err != nil {
		err = errors.NewErrorf("mount failed: %v", err)
		syslog.Println(err)
		log.LogFlush()
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		_ = daemonize.SignalOutcome(nil)
	}
	defer fsConn.Close()

	exporter.Init(ModuleName, cfg)
	exporter.RegistConsul(super.ClusterName(), ModuleName, cfg)

	err = log.OutputPid(opt.Logpath, ModuleName)
	if err != nil {
		log.LogFlush()
		syslog.Printf("output pid err(%v)", err)
		os.Exit(1)
	}

	if opt.NeedRestoreFuse {
		if fud == nil {
			if *configRestoreFuseUDS == "" {
				super.SetSockAddr(DefaultUDSName)
			} else {
				super.SetSockAddr(*configRestoreFuseUDS)
			}
		} else {
			fsConn.SetFuseDevFile(fud)
		}
	}

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
	err = daemonize.Run(cmdPath, args, env, os.Stdout)
	if err != nil {
		return fmt.Errorf("startDaemon failed: daemon start failed, cmd(%v) args(%v) env(%v) err(%v)\n", cmdPath, args, env, err)
	}

	return nil
}

func waitListenAndServe(statusCh chan error, addr string, handler http.Handler) {
	var err error
	var loop int = 0
	var interval int = (1 << 17) - 1
	var listener net.Listener
	var dynamicPort bool

	if addr == ":" {
		addr = ":0"
	}

	// FIXME: 1 min timeout?
	timeout := time.Now().Add(time.Minute)
	for {
		if listener, err = net.Listen("tcp", addr); err == nil {
			break
		}

		// addr is not released for use
		if strings.Contains(err.Error(), "bind: address already in use") {
			if loop&interval == 0 {
				syslog.Printf("address %v is still in use\n", addr)
			}
			runtime.Gosched()
		} else {
			break
		}
		if time.Now().After(timeout) {
			msg := fmt.Sprintf("address %v is still in use after "+
				"timeout, choose port automatically\n", addr)
			syslog.Print(msg)
			msg = "Warning: " + msg
			daemonize.StatusWriter.Write([]byte(msg))
			dynamicPort = true
			break
		}
		loop++
	}
	syslog.Printf("address %v wait loop %v\n", addr, loop)

	if dynamicPort {
		ipport := strings.Split(addr, ":")
		addr = ipport[0] + ":0"
		listener, err = net.Listen("tcp", addr)
	}

	if err != nil {
		statusCh <- err
		return
	}

	statusCh <- nil
	msg := fmt.Sprintf("Start pprof with port: %v\n",
		listener.Addr().(*net.TCPAddr).Port)
	syslog.Print(msg)
	if dynamicPort {
		msg = "Warning: " + msg
		daemonize.StatusWriter.Write([]byte(msg))
	}
	http.Serve(listener, handler)
	// unreachable
}

func mount(opt *proto.MountOptions) (fsConn *fuse.Conn, super *cfs.Super, err error) {
	super, err = cfs.NewSuper(opt)
	if err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	http.HandleFunc(ControlCommandSetRate, super.SetRate)
	http.HandleFunc(ControlCommandGetRate, super.GetRate)
	http.HandleFunc(log.SetLogLevelPath, log.SetLogLevel)
	http.HandleFunc(ControlCommandFreeOSMemory, freeOSMemory)
	http.HandleFunc(log.GetLogPath, log.GetLog)
	http.HandleFunc(ControlCommandSuspend, super.SetSuspend)
	http.HandleFunc(ControlCommandResume, super.SetResume)

	statusCh := make(chan error)
	go waitListenAndServe(statusCh, ":"+opt.Profport, nil)
	if err = <-statusCh; err != nil {
		daemonize.SignalOutcome(err)
		return
	}

	go func() {
		t := time.NewTicker(UpdateConfInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				log.LogDebugf("UpdateVolConf: start load conf from master")
				if proto.IsCold(opt.VolType) {
					var mc = master.NewMasterClientFromString(opt.Master, false)
					var volumeInfo *proto.SimpleVolView
					volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(opt.Volname)
					if err != nil {
						return
					}
					super.CacheAction = volumeInfo.CacheAction
					super.CacheThreshold = volumeInfo.CacheThreshold
					super.EbsBlockSize = volumeInfo.ObjBlockSize
				}
			}
		}
	}()

	if err = ump.InitUmp(fmt.Sprintf("%v_%v", super.ClusterName(), ModuleName), opt.UmpDatadir); err != nil {
		return
	}

	options := []fuse.MountOption{
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.AutoInvalData(opt.AutoInvalData),
		fuse.FSName("chubaofs-" + opt.Volname),
		fuse.Subtype("chubaofs"),
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
		options = append(options, fuse.DefaultPermissions())
	}

	if opt.EnableUnixPermission {
		options = append(options, fuse.DefaultPermissions())
	}

	fsConn, err = fuse.Mount(opt.MountPoint, opt.NeedRestoreFuse, options...)
	return
}

func registerInterceptedSignal(mnt string) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigC
		syslog.Printf("Killed due to a received signal (%v)\n", sig)
		os.Exit(1)
	}()
}

func parseMountOption(cfg *config.Config) (*proto.MountOptions, error) {
	var err error
	opt := new(proto.MountOptions)

	proto.ParseMountOptions(GlobalMountOptions, cfg)

	rawmnt := GlobalMountOptions[proto.MountPoint].GetString()
	opt.MountPoint, err = filepath.Abs(rawmnt)
	if err != nil {
		return nil, errors.Trace(err, "invalide mount point (%v) ", rawmnt)
	}

	opt.Volname = GlobalMountOptions[proto.VolName].GetString()
	opt.Owner = GlobalMountOptions[proto.Owner].GetString()
	opt.Master = GlobalMountOptions[proto.Master].GetString()
	logPath := GlobalMountOptions[proto.LogDir].GetString()
	if len(logPath) == 0 {
		logPath = DefaultLogPath
	}
	opt.Logpath = path.Join(logPath, LoggerPrefix)
	opt.Loglvl = GlobalMountOptions[proto.LogLevel].GetString()
	opt.Profport = GlobalMountOptions[proto.ProfPort].GetString()
	opt.IcacheTimeout = GlobalMountOptions[proto.IcacheTimeout].GetInt64()
	opt.LookupValid = GlobalMountOptions[proto.LookupValid].GetInt64()
	opt.AttrValid = GlobalMountOptions[proto.AttrValid].GetInt64()
	opt.ReadRate = GlobalMountOptions[proto.ReadRate].GetInt64()
	opt.WriteRate = GlobalMountOptions[proto.WriteRate].GetInt64()
	opt.EnSyncWrite = GlobalMountOptions[proto.EnSyncWrite].GetInt64()
	opt.AutoInvalData = GlobalMountOptions[proto.AutoInvalData].GetInt64()
	opt.UmpDatadir = GlobalMountOptions[proto.WarnLogDir].GetString()
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
	opt.AccessKey = GlobalMountOptions[proto.AccessKey].GetString()
	opt.SecretKey = GlobalMountOptions[proto.SecretKey].GetString()
	opt.DisableDcache = GlobalMountOptions[proto.DisableDcache].GetBool()
	opt.SubDir = GlobalMountOptions[proto.SubDir].GetString()
	opt.FsyncOnClose = GlobalMountOptions[proto.FsyncOnClose].GetBool()
	opt.MaxCPUs = GlobalMountOptions[proto.MaxCPUs].GetInt64()
	opt.EnableXattr = GlobalMountOptions[proto.EnableXattr].GetBool()
	opt.NearRead = GlobalMountOptions[proto.NearRead].GetBool()
	opt.EnablePosixACL = GlobalMountOptions[proto.EnablePosixACL].GetBool()
	opt.EnableSummary = GlobalMountOptions[proto.EnableSummary].GetBool()
	opt.EnableUnixPermission = GlobalMountOptions[proto.EnableUnixPermission].GetBool()
	opt.ReadThreads = GlobalMountOptions[proto.ReadThreads].GetInt64()
	opt.WriteThreads = GlobalMountOptions[proto.WriteThreads].GetInt64()
	opt.EnableBcache = GlobalMountOptions[proto.EnableBcache].GetBool()
	opt.MetaSendTimeout = GlobalMountOptions[proto.MetaSendTimeout].GetInt64()
	opt.BuffersTotalLimit = GlobalMountOptions[proto.BuffersTotalLimit].GetInt64()

	if opt.MountPoint == "" || opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", opt.MountPoint, opt.Volname, opt.Owner, opt.Master))
	}

	if  opt.BuffersTotalLimit < 0 {
		return nil, errors.New(fmt.Sprintf("invalid fields, BuffersTotalLimit(%v) must larger or equal than 0", opt.BuffersTotalLimit))
	}
	return opt, nil
}

func checkPermission(opt *proto.MountOptions) (err error) {
	var mc = master.NewMasterClientFromString(opt.Master, false)

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
		if policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXWriteAction) &&
			policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXReadAction) {
			return
		}
		if policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXReadAction) &&
			!policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXWriteAction) {
			opt.Rdonly = true
			return
		}
		err = proto.ErrNoPermission
		return
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

func loadConfFromMaster(opt *proto.MountOptions) (err error) {
	var mc = master.NewMasterClientFromString(opt.Master, false)
	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(opt.Volname)
	if err != nil {
		return
	}
	opt.VolType = volumeInfo.VolType
	opt.EbsBlockSize = volumeInfo.ObjBlockSize
	opt.CacheAction = volumeInfo.CacheAction
	opt.CacheThreshold = volumeInfo.CacheThreshold

	var clusterInfo *proto.ClusterInfo
	clusterInfo, err = mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}
	opt.EbsEndpoint = clusterInfo.EbsAddr
	opt.EbsServicePath = clusterInfo.ServicePath
	if proto.IsCold(opt.VolType) {
		if opt.EbsBlockSize == 0 || opt.EbsEndpoint == "" || opt.EbsServicePath == "" {
			return errors.New("cold volume ebs config is empty.")
		}
		log.LogDebugf("ebs config: EbsEndpoint(%v) EbsServicePath(%v) EbsBlockSize(%v)", opt.EbsEndpoint, opt.EbsServicePath, opt.EbsBlockSize)
	}
	return
}
