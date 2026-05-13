// Copyright 2026 The CubeFS Authors.
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

package syncnode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

// SyncNode is the main service struct (Phase A: skeleton only; later phases
// populate scheduler / executor / state store fields).
type SyncNode struct {
	cfg *SyncConfig

	// Network
	tcpListener  net.Listener
	httpServer   *http.Server
	httpListener net.Listener

	// Identity (filled after master registration in Phase B; left empty in Phase A)
	localServerAddr string
	clusterID       string
	nodeID          uint64

	// Lifecycle
	stopC   chan struct{}
	control common.Control

	// Synchronisation for background goroutines started by doStart.
	wg sync.WaitGroup
}

// Build / version info reported by /admin/syncnode/version. Populated at link
// time in production builds; sane defaults for local builds.
var (
	BuildVersion = "dev"
	BuildCommit  = "unknown"
	BuildTime    = "unknown"
)

// NewServer constructs an empty SyncNode. cmd/cmd.go calls this on
// `cfs-server -c sync.json` when role=sync.
func NewServer() *SyncNode {
	return &SyncNode{}
}

// Start fulfils common.Server. Delegates to common.Control which handles the
// state machine (Standby → Start → Running) and goroutine accounting.
func (s *SyncNode) Start(cfg *config.Config) error {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return s.control.Start(s, cfg, doStart)
}

// Shutdown fulfils common.Server.
func (s *SyncNode) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

// Sync fulfils common.Server. Blocks until shutdown.
func (s *SyncNode) Sync() {
	s.control.Sync()
}

func doStart(srv common.Server, cfg *config.Config) (err error) {
	s, ok := srv.(*SyncNode)
	if !ok {
		return errors.New("invalid server type for syncnode")
	}
	s.stopC = make(chan struct{})

	if err = s.parseConfig(cfg); err != nil {
		return err
	}

	// Logging (parseConfig has already loaded LogDir / LogLevel into s.cfg).
	level := log.ParseLogLevel(s.cfg.LogLevel)
	if _, err = log.InitLog(s.cfg.LogDir, ModuleName, level, nil, log.DefaultLogLeftSpaceLimitRatio); err != nil {
		return fmt.Errorf("init log: %w", err)
	}

	// Metrics: initialise gauges + start the refresh loop.
	initMetrics()
	startMetricsLoop(s.stopC)

	// Exporter registration with consul / Prometheus pull endpoint.
	exporter.Init(ModuleName, cfg)
	exporter.RegistConsul("", ModuleName, cfg)

	// TCP server for future master-dispatched task packets (no-op in Phase A).
	if err = s.startTCPServer(); err != nil {
		return fmt.Errorf("start tcp server: %w", err)
	}

	// HTTP admin server (includes /admin/syncnode/version, /metrics-friendly
	// endpoints; full admin API lands in Phase E).
	if err = s.startHTTPServer(); err != nil {
		return fmt.Errorf("start http server: %w", err)
	}

	// Master registration stub: in Phase B this becomes a real register loop
	// + heartbeat goroutine. In Phase A it's a no-op so single-node smoke
	// tests work without a running master.
	s.registerStub()

	log.LogInfof("syncnode started: listen=%s httpListen=%s exporterPort=%d",
		s.cfg.Listen, s.cfg.HTTPListen, s.cfg.ExporterPort)
	return nil
}

func doShutdown(srv common.Server) {
	s, ok := srv.(*SyncNode)
	if !ok {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("syncnode doShutdown panic: %v", r)
		}
	}()
	if s.stopC != nil {
		close(s.stopC)
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	s.wg.Wait()
	log.LogInfo("syncnode shutdown complete")
}

// parseConfig loads the raw config.Config into a typed SyncConfig and runs
// validateConfig. On failure the returned error includes ConfigError code +
// field for operators / tests.
func (s *SyncNode) parseConfig(cfg *config.Config) error {
	// config.Config keeps the original JSON bytes in Raw; we parse with our
	// own typed schema + validator rather than fishing fields out one by one.
	raw := cfg.Raw
	if len(raw) == 0 {
		return errors.New("empty config: cfg.Raw is nil — was the file loaded?")
	}
	sc, err := ParseSyncConfig(raw)
	if err != nil {
		return err
	}
	// masterAddr is allowed to come either from a single string or comma-
	// separated multi-master form; both shapes are valid for cfs-server.
	if strings.Contains(sc.MasterAddr, ",") {
		// just validate it's non-empty; full parsing in Phase B's register().
	}
	s.cfg = sc
	return nil
}

func (s *SyncNode) startTCPServer() error {
	addr := ":" + s.cfg.Listen
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.tcpListener = l
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, aerr := l.Accept()
			if aerr != nil {
				select {
				case <-s.stopC:
					return
				default:
					log.LogWarnf("tcp accept: %v", aerr)
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
			// Phase A: no master-dispatched packets handled yet. Close conn
			// so peers that mistakenly point at us don't pile up.
			_ = conn.Close()
		}
	}()
	return nil
}

func (s *SyncNode) startHTTPServer() error {
	router := mux.NewRouter().SkipClean(true)
	router.HandleFunc("/admin/syncnode/version", s.handleVersion).Methods(http.MethodGet)
	router.HandleFunc("/admin/syncnode/stat", s.handleStat).Methods(http.MethodGet)

	addr := ":" + s.cfg.HTTPListen
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", addr, err)
	}
	s.httpListener = l
	s.httpServer = &http.Server{
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if serr := s.httpServer.Serve(l); serr != nil && serr != http.ErrServerClosed {
			log.LogWarnf("http server: %v", serr)
		}
	}()
	return nil
}

// handleVersion responds with build info + role identity. AC for A-1.
func (s *SyncNode) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"code": 0,
		"msg":  "OK",
		"data": map[string]string{
			"role":        ModuleName,
			"version":     BuildVersion,
			"commit":      BuildCommit,
			"buildTime":   BuildTime,
			"nodeAddress": s.localServerAddr,
		},
	})
}

// handleStat is a Phase-A minimal endpoint that returns node-level state.
// More detailed fields land in Phase E.
func (s *SyncNode) handleStat(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"code": 0,
		"msg":  "OK",
		"data": map[string]interface{}{
			"role":            ModuleName,
			"uptimeSeconds":   time.Since(startedAt).Seconds(),
			"concurrentTasks": concurrentTasks.Load(),
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// registerStub is a placeholder for Phase B's real register() loop. In
// Phase A this only records localServerAddr for /admin/syncnode/version
// to display; no network call to master.
func (s *SyncNode) registerStub() {
	host, _, _ := net.SplitHostPort(s.cfg.MasterAddr)
	port, _ := strconv.Atoi(s.cfg.Listen)
	if host == "" {
		host = "127.0.0.1"
	}
	s.localServerAddr = fmt.Sprintf("%s:%d", host, port)
}

// _ = proto.* is a build-time sanity check that proto package is reachable.
// Keeps the import grouped with other CubeFS deps for future expansion.
var _ = proto.ListenPort
