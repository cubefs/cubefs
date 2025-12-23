// Copyright 2025 The CubeFS Authors.
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
	"math"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	cfslog "github.com/cubefs/cubefs/util/log"
	sysutil "github.com/cubefs/cubefs/util/sys"
)

const (
	LoggerPrefix = "client"
	LoggerOutput = "output.log"
)

var GlobalMountOptions []proto.MountOption

func init() {
	GlobalMountOptions = proto.NewMountOptions()
	proto.InitMountOptions(GlobalMountOptions)
}

// parseMountOption parses mount options from config file, similar to client/fuse.go
func parseMountOption(cfg *config.Config) (*proto.MountOptions, error) {
	var err error
	opt := new(proto.MountOptions)

	proto.ParseMountOptions(GlobalMountOptions, cfg)

	rawmnt := GlobalMountOptions[proto.MountPoint].GetString()
	if rawmnt == "" {
		rawmnt = "/" // Default mount point for NFS (not actually used)
	}
	opt.MountPoint, err = filepath.Abs(rawmnt)
	if err != nil {
		return nil, fmt.Errorf("invalid mount point (%v): %v", rawmnt, err)
	}
	opt.Volname = GlobalMountOptions[proto.VolName].GetString()
	opt.Owner = GlobalMountOptions[proto.Owner].GetString()
	opt.Master = GlobalMountOptions[proto.Master].GetString()
	logPath := GlobalMountOptions[proto.LogDir].GetString()
	if len(logPath) == 0 {
		logPath = "/var/log/chubaofs"
	}
	opt.Logpath = logPath
	// Note: We don't append LoggerPrefix here as NFS demo doesn't need it
	opt.Loglvl = GlobalMountOptions[proto.LogLevel].GetString()
	opt.Profport = GlobalMountOptions[proto.ProfPort].GetString()
	opt.LocallyProf = GlobalMountOptions[proto.LocallyProf].GetBool()
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
	opt.MaximallyRead = GlobalMountOptions[proto.MaximallyRead].GetBool()
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
	opt.ReqChanCnt = GlobalMountOptions[proto.ReqChanCnt].GetInt64()
	opt.EnableXattr = GlobalMountOptions[proto.EnableXattr].GetBool()
	opt.NearRead = GlobalMountOptions[proto.NearRead].GetBool()
	opt.EnablePosixACL = GlobalMountOptions[proto.EnablePosixACL].GetBool()
	opt.EnableUnixPermission = GlobalMountOptions[proto.EnableUnixPermission].GetBool()
	opt.ReadThreads = GlobalMountOptions[proto.ReadThreads].GetInt64()
	opt.WriteThreads = GlobalMountOptions[proto.WriteThreads].GetInt64()

	opt.BcacheDir = GlobalMountOptions[proto.BcacheDir].GetString()
	opt.BcacheFilterFiles = GlobalMountOptions[proto.BcacheFilterFiles].GetString()
	opt.BcacheBatchCnt = GlobalMountOptions[proto.BcacheBatchCnt].GetInt64()
	opt.BcacheCheckIntervalS = GlobalMountOptions[proto.BcacheCheckIntervalS].GetInt64()
	opt.EnableBcache = false // Disable bcache for NFS server

	opt.BcacheOnlyForNotSSD = GlobalMountOptions[proto.BcacheOnlyForNotSSD].GetBool()

	if opt.Rdonly {
		verReadSeq := GlobalMountOptions[proto.SnapshotReadVerSeq].GetInt64()
		if verReadSeq == -1 {
			opt.VerReadSeq = math.MaxUint64
		} else {
			opt.VerReadSeq = uint64(verReadSeq)
		}
	}
	opt.MetaSendTimeout = GlobalMountOptions[proto.MetaSendTimeout].GetInt64()
	if opt.MetaSendTimeout <= 0 {
		opt.MetaSendTimeout = 120 // Default timeout
	}

	opt.BuffersTotalLimit = GlobalMountOptions[proto.BuffersTotalLimit].GetInt64()
	opt.BufferChanSize = GlobalMountOptions[proto.BufferChanSize].GetInt64()
	opt.MaxStreamerLimit = GlobalMountOptions[proto.MaxStreamerLimit].GetInt64()
	if opt.MaxStreamerLimit <= 0 {
		opt.MaxStreamerLimit = 100000 // Default limit
	}
	opt.EnableAudit = GlobalMountOptions[proto.EnableAudit].GetBool()
	opt.RequestTimeout = GlobalMountOptions[proto.RequestTimeout].GetInt64()
	opt.ClientOpTimeOut = GlobalMountOptions[proto.ClientOpTimeOut].GetInt64()
	opt.TcpAliveTime = GlobalMountOptions[proto.TcpAliveTime].GetInt64()
	opt.FileSystemName = GlobalMountOptions[proto.FileSystemName].GetString()
	opt.DisableMountSubtype = GlobalMountOptions[proto.DisableMountSubtype].GetBool()
	opt.StreamRetryTimeout = int(GlobalMountOptions[proto.StreamRetryTimeOut].GetInt64())
	if opt.StreamRetryTimeout <= 0 {
		opt.StreamRetryTimeout = 60 // Default timeout
	}
	opt.ForceRemoteCache = GlobalMountOptions[proto.ForceRemoteCache].GetBool()
	opt.AheadReadEnable = GlobalMountOptions[proto.AheadReadEnable].GetBool()
	opt.EnableAsyncFlush = GlobalMountOptions[proto.EnableAsyncFlush].GetBool()
	if opt.AheadReadEnable {
		opt.AheadReadBlockTimeOut = int(GlobalMountOptions[proto.AheadReadBlockTimeOut].GetInt64())
		opt.AheadReadWindowCnt = int(GlobalMountOptions[proto.AheadReadWindowCnt].GetInt64())
		opt.AheadReadTotalMem = GlobalMountOptions[proto.AheadReadTotalMemGB].GetInt64() * 1024 * 1024 * 1024 // Convert GB to bytes
		if opt.AheadReadTotalMem <= 0 {
			opt.AheadReadTotalMem = 100 * 1024 * 1024 // Default 100MB
		}
	}
	opt.ReadDirLimit = GlobalMountOptions[proto.ReadDirLimit].GetInt64()
	opt.MaxWarmUpConcurrency = GlobalMountOptions[proto.MaxWarmUpConcurrency].GetInt64()
	opt.StopWarmMeta = GlobalMountOptions[proto.StopWarmMeta].GetBool()
	opt.MetaCacheAcceleration = GlobalMountOptions[proto.MetaCacheAcceleration].GetBool()
	opt.MinimumNlinkReadDir = GlobalMountOptions[proto.MinimumNlinkReadDir].GetInt64()
	opt.InodeLruLimit = GlobalMountOptions[proto.InodeLruLimit].GetInt64()
	opt.FuseServeThreads = GlobalMountOptions[proto.FuseServeThreads].GetInt64()

	if opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, fmt.Errorf("invalid config: lack of mandatory fields, volName(%v), owner(%v), masterAddr(%v)", opt.Volname, opt.Owner, opt.Master)
	}

	if opt.BuffersTotalLimit < 0 {
		return nil, fmt.Errorf("invalid fields, BuffersTotalLimit(%v) must larger or equal than 0", opt.BuffersTotalLimit)
	}
	if opt.BuffersTotalLimit == 0 {
		opt.BuffersTotalLimit = 32768 // Default buffer pool size
	}

	if opt.FileSystemName == "" {
		opt.FileSystemName = "cubefs-" + opt.Volname
	}

	return opt, nil
}

// loadConfFromMaster loads additional configuration from master server
// This is the same as client/fuse.go does
func loadConfFromMaster(opt *proto.MountOptions) (err error) {
	mc := master.NewMasterClientFromString(opt.Master, false)
	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(opt.Volname)
	if err != nil {
		return
	}
	opt.VolType = volumeInfo.VolType
	opt.EbsBlockSize = volumeInfo.ObjBlockSize
	opt.EnableQuota = volumeInfo.EnableQuota
	opt.EnableTransaction = volumeInfo.EnableTransactionV1
	opt.TxTimeout = volumeInfo.TxTimeout
	opt.TxConflictRetryNum = volumeInfo.TxConflictRetryNum
	opt.TxConflictRetryInterval = volumeInfo.TxConflictRetryInterval
	opt.VolStorageClass = volumeInfo.VolStorageClass
	opt.VolAllowedStorageClass = volumeInfo.AllowedStorageClass

	var clusterInfo *proto.ClusterInfo
	clusterInfo, err = mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}
	opt.EbsEndpoint = clusterInfo.EbsAddr
	opt.EbsServicePath = clusterInfo.ServicePath
	return
}

func main() {
	port := flag.Int("port", 20490, "NFS server port")
	configFile := flag.String("config", "", "CubeFS client config file (JSON format, same as fuse client)")
	useMemFS := flag.Bool("memfs", false, "Use in-memory filesystem instead of CubeFS (for testing)")
	flag.Parse()

	var fs billy.Filesystem
	var err error
	var volName string
	var opt *proto.MountOptions

	if *useMemFS {
		// Use in-memory filesystem for testing
		syslog.Printf("Using in-memory filesystem (memfs)")
		fs = memfs.New()
	} else {
		if *configFile == "" {
			syslog.Fatalf("Config file is required. Use -config to specify CubeFS client config file")
		}

		// Load config file using the same method as fuse client
		cfg, err := config.LoadConfigFile(*configFile)
		if err != nil {
			syslog.Fatalf("Failed to load config file: %v", err)
		}

		// Parse mount options using the same logic as fuse client
		opt, err = parseMountOption(cfg)
		if err != nil {
			syslog.Fatalf("Failed to parse mount options: %v", err)
		}

		// Setup output.log for logging before log system initialization
		if opt.Logpath != "" {
			// Ensure log directory exists
			_, err = os.Stat(opt.Logpath)
			if err != nil {
				os.MkdirAll(opt.Logpath, 0o755)
			}
			outputFilePath := path.Join(opt.Logpath, LoggerPrefix, LoggerOutput)
			// Ensure client subdirectory exists
			clientDir := path.Join(opt.Logpath, LoggerPrefix)
			_, err = os.Stat(clientDir)
			if err != nil {
				os.MkdirAll(clientDir, 0o755)
			}
			outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
			if err != nil {
				err = fmt.Errorf("Open output file failed: %v", err)
				fmt.Println(err)
				os.Exit(1)
			}
			defer func() {
				outputFile.Sync()
				outputFile.Close()
			}()
			syslog.SetOutput(outputFile)

			// Redirect stderr to output.log
			if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
				err = fmt.Errorf("Redirect fd failed: %v", err)
				syslog.Println(err)
				os.Exit(1)
			}
		}

		// Load additional config from master (VolStorageClass, VolType, etc.)
		// This is the same as fuse client does
		err = loadConfFromMaster(opt)
		if err != nil {
			syslog.Fatalf("Failed to load config from master: %v", err)
		}

		volName = opt.Volname

		// Initialize CubeFS SDK log if LogDir is specified
		// After this point, use cfslog functions instead of syslog
		if opt.Logpath != "" {
			logLevel := opt.Loglvl
			if logLevel == "" {
				logLevel = "INFO"
			}
			level := cfslog.ParseLogLevel(logLevel)
			_, err := cfslog.InitLog(opt.Logpath, "nfs_gateway", level, nil, cfslog.DefaultLogLeftSpaceLimitRatio)
			if err != nil {
				syslog.Printf("Failed to init CubeFS log: %v (continuing without SDK logs)", err)
			} else {
				cfslog.LogInfof("CubeFS SDK log initialized: dir=%s, level=%s", opt.Logpath, logLevel)
			}
		}

		cfslog.LogInfof("Connecting to CubeFS: volume=%s, owner=%s, master=%s", opt.Volname, opt.Owner, opt.Master)
		cfsFS, err := NewCubefsFS(opt)
		if err != nil {
			cfslog.LogErrorf("Failed to create CubeFS filesystem: %v", err)
			cfslog.LogFlush()
			os.Exit(1)
		}
		defer cfsFS.Close()
		fs = cfsFS
		cfslog.LogInfof("Successfully connected to CubeFS volume: %s", volName)
	}

	// Create CubeFS NFS handler with null authentication
	// This handler wraps the CubeFS filesystem and provides NFS protocol interface
	cubefsHandler := helpers.NewNullAuthHandler(fs)
	// Wrap CubeFS handler with caching layer for better performance and file handle management
	// Use cache size of 100 to support directory listing and better performance
	nfsHandler := helpers.NewCachingHandler(cubefsHandler, 100)

	// Listen on specified port
	addr := fmt.Sprintf(":%d", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		if opt != nil && opt.Logpath != "" {
			cfslog.LogErrorf("Failed to listen on %s: %v", addr, err)
			cfslog.LogFlush()
		} else {
			syslog.Fatalf("Failed to listen on %s: %v", addr, err)
		}
		os.Exit(1)
	}
	defer listener.Close()

	if opt != nil && opt.Logpath != "" {
		cfslog.LogInfof("NFS server (using go-nfs library) listening on port %d", *port)
		if volName != "" {
			cfslog.LogInfof("Exporting CubeFS volume: %s", volName)
		}
		cfslog.LogInfof("To mount: sudo mount -t nfs -o vers=3,port=%d,mountport=%d,proto=tcp localhost:/ /tmp/nfs_test_mount", *port, *port)
		cfslog.LogInfof("Alternative mount: sudo mount -o port=%d,mountport=%d,nfsvers=3,noacl,tcp -t nfs localhost:/ /tmp/nfs_test_mount", *port, *port)
		cfslog.LogInfof("Note: If mount fails with error 521, this may be a go-nfs library MOUNT protocol limitation")
	} else {
		syslog.Printf("NFS server (using go-nfs library) listening on port %d", *port)
		if volName != "" {
			syslog.Printf("Exporting CubeFS volume: %s", volName)
		}
		syslog.Printf("To mount: sudo mount -t nfs -o vers=3,port=%d,mountport=%d,proto=tcp localhost:/ /tmp/nfs_test_mount", *port, *port)
		syslog.Printf("Alternative mount: sudo mount -o port=%d,mountport=%d,nfsvers=3,noacl,tcp -t nfs localhost:/ /tmp/nfs_test_mount", *port, *port)
		syslog.Printf("Note: If mount fails with error 521, this may be a go-nfs library MOUNT protocol limitation")
	}

	// Use nfs.Serve to handle connections with CubeFS handler (wrapped with caching)
	if err := nfs.Serve(listener, nfsHandler); err != nil {
		if opt != nil && opt.Logpath != "" {
			cfslog.LogErrorf("Failed to serve: %v", err)
			cfslog.LogFlush()
		} else {
			syslog.Fatalf("Failed to serve: %v", err)
		}
		os.Exit(1)
	}
}
