// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
	"golang.org/x/time/rate"
)

type LcNode struct {
	listen           string
	httpListen       string
	localServerAddr  string
	clusterID        string
	nodeID           uint64
	masters          []string
	ebsAddr          string
	logDir           string
	mc               *master.MasterClient
	scannerMutex     sync.RWMutex
	stopC            chan bool
	lastHeartbeat    time.Time
	control          common.Control
	lcScanners       map[string]*LcScanner
	snapshotScanners map[string]*SnapshotScanner
}

func NewServer() *LcNode {
	return &LcNode{
		lcScanners:       make(map[string]*LcScanner),
		snapshotScanners: make(map[string]*SnapshotScanner),
	}
}

func (l *LcNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return l.control.Start(l, cfg, doStart)
}

func (l *LcNode) Shutdown() {
	l.control.Shutdown(l, doShutdown)
}

func (l *LcNode) Sync() {
	l.control.Sync()
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	l, ok := s.(*LcNode)
	if !ok {
		return errors.New("Invalid node Type!")
	}
	l.stopC = make(chan bool)

	if err = l.parseConfig(cfg); err != nil {
		return
	}
	l.register()
	l.lastHeartbeat = time.Now()

	exporter.RegistConsul(l.clusterID, ModuleName, cfg)

	go l.checkRegister()
	if err = l.startServer(); err != nil {
		return
	}

	l.httpServiceStart()

	log.LogInfo("lcnode start successfully")

	return
}

func doShutdown(s common.Server) {
	l, ok := s.(*LcNode)
	if !ok {
		return
	}
	l.stopServer()
}

func (l *LcNode) parseConfig(cfg *config.Config) (err error) {
	l.logDir = cfg.GetString("logDir")
	// parse listen
	listen := cfg.GetString(configListen)
	if len(listen) == 0 {
		listen = defaultListen
	}
	if match := regexpListen.MatchString(listen); !match {
		err = errors.New("invalid listen configuration")
		return
	}
	l.listen = listen
	log.LogWarnf("loadConfig: setup config: %v(%v)", configListen, listen)

	var listenInt int
	if listenInt, err = strconv.Atoi(listen); err != nil {
		log.LogErrorf("parseConfig err: %v", err)
		return
	}
	l.httpListen = strconv.Itoa(listenInt + 1)
	log.LogWarnf("loadConfig: setup config: httpListen(%v)", l.httpListen)

	// parse master config
	masters := cfg.GetStringSlice(configMasterAddr)
	if len(masters) == 0 {
		return config.NewIllegalConfigError(configMasterAddr)
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configMasterAddr, strings.Join(masters, ","))
	l.masters = masters
	l.mc = master.NewMasterClient(masters, false)

	// parse scanCheckInterval
	scanCheckInterval = cfg.GetInt64(configScanCheckIntervalStr)
	if scanCheckInterval <= 0 {
		scanCheckInterval = defaultScanCheckInterval
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configScanCheckIntervalStr, scanCheckInterval)

	// parse lcScanRoutineNumPerTask
	lcScanRoutineNumPerTask = cfg.GetInt(configLcScanRoutineNumPerTaskStr)
	if lcScanRoutineNumPerTask <= 0 || lcScanRoutineNumPerTask > maxLcScanRoutineNumPerTask {
		lcScanRoutineNumPerTask = defaultLcScanRoutineNumPerTask
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configLcScanRoutineNumPerTaskStr, lcScanRoutineNumPerTask)

	// parse simpleQueueInitCapacity
	simpleQueueInitCapacity = cfg.GetInt(configSimpleQueueInitCapacityStr)
	if simpleQueueInitCapacity <= lcScanRoutineNumPerTask*1000 {
		simpleQueueInitCapacity = defaultSimpleQueueInitCapacity
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configSimpleQueueInitCapacityStr, simpleQueueInitCapacity)

	// parse snapshotRoutineNumPerTask
	snapshotRoutineNumPerTask = cfg.GetInt(configSnapshotRoutineNumPerTaskStr)
	if snapshotRoutineNumPerTask <= 0 || snapshotRoutineNumPerTask > maxLcScanRoutineNumPerTask {
		snapshotRoutineNumPerTask = defaultLcScanRoutineNumPerTask
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configSnapshotRoutineNumPerTaskStr, snapshotRoutineNumPerTask)

	// parse lcScanLimitPerSecond
	limitNum := cfg.GetInt64(configLcScanLimitPerSecondStr)
	if limitNum <= 0 {
		lcScanLimitPerSecond = defaultLcScanLimitPerSecond
	} else {
		lcScanLimitPerSecond = rate.Limit(limitNum)
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configLcScanLimitPerSecondStr, lcScanLimitPerSecond)

	// parse lcNodeTaskCount
	count := cfg.GetInt(configLcNodeTaskCountLimit)
	if count <= 0 || count > maxLcNodeTaskCountLimit {
		lcNodeTaskCountLimit = defaultLcNodeTaskCountLimit
	} else {
		lcNodeTaskCountLimit = count
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configLcNodeTaskCountLimit, lcNodeTaskCountLimit)

	// parse delayDelMinute
	delay := cfg.GetInt64(configDelayDelMinute)
	if delay <= 0 {
		delayDelMinute = defaultDelayDelMinute
	} else {
		delayDelMinute = uint64(delay)
	}
	log.LogWarnf("loadConfig: setup config: %v(%v)", configDelayDelMinute, delayDelMinute)

	// parse useCreateTime
	useCreateTime = cfg.GetBool(configUseCreateTime)
	log.LogWarnf("loadConfig: setup config: %v(%v)", configUseCreateTime, useCreateTime)

	stream.SetExentRetryArgs(defaultAllocRetryInterval, defaultWriteRetryInterval, defaultExtenthandlerMaxRetryMin, true)

	return
}

func (l *LcNode) register() {
	var err error
	timer := time.NewTimer(0)

	// get the IsIPV4 address, cluster ID and node ID from the master
	for {
		select {
		case <-timer.C:
			var ci *proto.ClusterInfo
			if ci, err = l.mc.AdminAPI().GetClusterInfo(); err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).",
					l.mc.Leader(), err)
				timer.Reset(2 * time.Second)
				continue
			}
			masterAddr := l.mc.Leader()
			l.clusterID = ci.Cluster
			localIP := ci.Ip
			l.localServerAddr = fmt.Sprintf("%s:%v", localIP, l.listen)
			if !util.IsIPV4(localIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).",
					localIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}

			// register this lcnode on the master
			var nodeID uint64
			if nodeID, err = l.mc.NodeAPI().AddLcNode(l.localServerAddr); err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master[%v] err(%v).",
					masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			l.nodeID = nodeID
			log.LogInfof("register: register LcNode: nodeID(%v)", l.nodeID)
			l.ebsAddr = ci.EbsAddr
			log.LogInfof("register: register success: %v", l)
			return
		case <-l.stopC:
			timer.Stop()
			return
		}
	}
}

func (l *LcNode) checkRegister() {
	for {
		if time.Since(l.lastHeartbeat) > time.Minute*10 {
			log.LogWarnf("lcnode might be deregistered from master, stop scanners...")
			l.stopScanners()
			log.LogWarnf("lcnode might be deregistered from master, retry registering...")
			l.register()
			l.lastHeartbeat = time.Now()
		}
		time.Sleep(time.Minute)
	}
}

func (l *LcNode) startServer() (err error) {
	log.LogInfo("Start: startServer")
	addr := fmt.Sprintf(":%v", l.listen)
	listener, err := net.Listen("tcp", addr)
	log.LogInfof("action[startServer] listen tcp address(%v).", addr)
	if err != nil {
		log.LogErrorf("action[startServer] failed to listen, err: %v", err)
		return
	}
	go func(stopC chan bool) {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			log.LogDebugf("action[startServer] accept connection from %s", conn.RemoteAddr().String())
			select {
			case <-stopC:
				return
			default:
			}
			if err != nil {
				log.LogErrorf("action[startServer] failed to accept, err: %s", err.Error())
				continue
			}
			go l.serveConn(conn, stopC)
		}
	}(l.stopC)
	return
}

func (l *LcNode) serveConn(conn net.Conn, stopC chan bool) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	remoteAddr := conn.RemoteAddr().String()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := &proto.Packet{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.LogErrorf("serveConn ReadFromConn remoteAddr: %v, err: %v", remoteAddr, err)
			}
			return
		}
		if err := l.handlePacket(conn, p, remoteAddr); err != nil {
			log.LogErrorf("serveConn handlePacket remoteAddr: %v, err: %v", remoteAddr, err)
		}
	}
}

func (l *LcNode) handlePacket(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	log.LogInfof("handlePacket input info op (%s), remote %s", p.String(), remoteAddr)
	switch p.Opcode {
	case proto.OpLcNodeHeartbeat:
		err = l.opMasterHeartbeat(conn, p, remoteAddr)
	case proto.OpLcNodeScan:
		err = l.opLcScan(conn, p)
	case proto.OpLcNodeSnapshotVerDel:
		err = l.opSnapshotVerDel(conn, p)
	default:
		err = fmt.Errorf("%s unknown Opcode: %d, reqId: %d", remoteAddr,
			p.Opcode, p.GetReqID())
	}
	if err != nil {
		err = errors.NewErrorf("%s [%s] req: %d - %s", remoteAddr, p.GetOpMsg(),
			p.GetReqID(), err.Error())
	}
	return
}

func (l *LcNode) stopServer() {
	if l.stopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[StopTcpServer],err:%v", r)
			}
		}()
		close(l.stopC)
		log.LogInfo("LcNode Stop!")
	}
}

func (l *LcNode) stopScanners() {
	l.scannerMutex.Lock()
	defer l.scannerMutex.Unlock()
	for _, s := range l.lcScanners {
		s.Stop()
		delete(l.lcScanners, s.ID)
	}
	for _, s := range l.snapshotScanners {
		s.Stop()
		delete(l.snapshotScanners, s.ID)
	}
}

func (l *LcNode) httpServiceStart() {
	router := mux.NewRouter().SkipClean(true)
	router.NewRoute().Methods(http.MethodGet).
		Path("/stopScanner").
		HandlerFunc(l.httpServiceStopScanner)
	router.NewRoute().Methods(http.MethodGet).
		Path("/getFile").
		HandlerFunc(l.httpServiceGetFile)

	addr := fmt.Sprintf(":%v", l.httpListen)
	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogFatalf("httpServiceStart addr(%v) err: %v", addr, err)
			return
		}
	}()
	log.LogInfof("httpServiceStart addr(%v) success", addr)
}

func (l *LcNode) httpServiceStopScanner(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		msg := fmt.Sprintf("httpServiceStopScanner ParseForm failed: %v", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	id := r.FormValue("id")
	if id == "" {
		http.Error(w, "invalid task id", http.StatusBadRequest)
		return
	}
	log.LogInfof("receive httpServiceStopScanner id: %v", id)

	l.scannerMutex.RLock()
	scanner, ok := l.lcScanners[id]
	if !ok {
		msg := fmt.Sprintf("task id(%v) not exist", id)
		http.Error(w, msg, http.StatusNotFound)
		l.scannerMutex.RUnlock()
		return
	}
	l.scannerMutex.RUnlock()
	if !scanner.receiveStop {
		log.LogInfof("receive httpServiceStopScanner: %v, close receiveStop", scanner.ID)
		close(scanner.receiveStopC)
	} else {
		log.LogInfof("receive httpServiceStopScanner: %v, already receiveStop", scanner.ID)
	}

	w.WriteHeader(http.StatusOK)
}

func (l *LcNode) httpServiceGetFile(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	var isMigrationExtent bool
	mek := r.FormValue("mek")
	if mek == "true" {
		isMigrationExtent = true
	}

	var ino uint64
	if ino, err = strconv.ParseUint(r.FormValue("ino"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint ino err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	var size uint64
	if size, err = strconv.ParseUint(r.FormValue("size"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint size err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	var sc uint64
	if sc, err = strconv.ParseUint(r.FormValue("sc"), 10, 32); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint sc err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	var vsc uint64
	if vsc, err = strconv.ParseUint(r.FormValue("vsc"), 10, 32); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint vsc err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	var asc []uint32
	ascStr := strings.Split(r.FormValue("asc"), ",")
	for _, scStr := range ascStr {
		var scUint64 uint64
		if scUint64, err = strconv.ParseUint(scStr, 10, 32); err != nil {
			http.Error(w, fmt.Sprintf("ParseUint asc err: %v", err.Error()), http.StatusBadRequest)
			return
		}
		asc = append(asc, uint32(scUint64))
	}

	metaConfig := &meta.MetaConfig{
		Volume:               vol,
		Masters:              l.masters,
		Authenticate:         false,
		ValidateOwner:        false,
		InnerReq:             true,
		MetaSendTimeout:      600,
		DisableTrashByClient: true,
	}
	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		http.Error(w, fmt.Sprintf("NewMetaWrapper err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	extentConfig := &stream.ExtentConfig{
		Volume:                      vol,
		Masters:                     l.masters,
		OnAppendExtentKey:           metaWrapper.AppendExtentKey,
		OnSplitExtentKey:            metaWrapper.SplitExtentKey,
		OnGetExtents:                metaWrapper.GetExtents,
		OnTruncate:                  metaWrapper.Truncate,
		OnRenewalForbiddenMigration: metaWrapper.RenewalForbiddenMigration,
		VolStorageClass:             uint32(vsc),
		VolAllowedStorageClass:      asc,
		OnForbiddenMigration:        metaWrapper.ForbiddenMigration,
		InnerReq:                    true,
		MetaWrapper:                 metaWrapper,
	}
	var extentClient *stream.ExtentClient
	if extentClient, err = stream.NewExtentClient(extentConfig); err != nil {
		http.Error(w, fmt.Sprintf("NewExtentClient err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	defer extentClient.Close()

	if err = extentClient.OpenStream(ino, false, false); err != nil {
		http.Error(w, fmt.Sprintf("OpenStream err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	defer extentClient.CloseStream(ino)

	t := &TransitionMgr{
		ec:     extentClient,
		ecForW: extentClient,
	}
	e := &proto.ScanDentry{
		Size:         size,
		Inode:        ino,
		StorageClass: uint32(sc),
	}
	if err = t.readFromExtentClient(e, w, isMigrationExtent, 0, 0); err != nil {
		http.Error(w, fmt.Sprintf("readFromExtentClient err: %v", err.Error()), http.StatusBadRequest)
		return
	}
	log.LogInfof("httpServiceGetFile success, vol(%v), ino(%v), size(%v)", vol, ino, size)
}
