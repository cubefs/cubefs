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
	stderrors "errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/bolt"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/scheduler"
	"github.com/cubefs/cubefs/syncnode/tasks"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

// SyncNode is the main service struct. After Phase F it owns:
//
//   - BoltDB-backed state (rules, tasks_active, tasks_history, in_progress)
//   - The executor pool (per-task data movement)
//   - tasks.Runner (Trigger / Cancel / Retry HTTP dispatch + lifecycle)
//   - scheduler.Scheduler (cron-driven rule firing)
//   - tasks.TTLRunner (move terminal records to history; purge expired)
//   - Admin HTTP server (rules + tasks + reload endpoints)
type SyncNode struct {
	// Config + reload state.
	cfgMu   sync.RWMutex
	cfg     *SyncConfig
	cfgPath string // recorded by parseConfig from config.Config.FilePath

	// Network
	tcpListener  net.Listener
	httpServer   *http.Server
	httpListener net.Listener

	// Identity (filled after master registration in Phase B; left empty in
	// Phase A).
	localServerAddr string
	clusterID       string
	nodeID          uint64

	// State store (Phase F-2). One BoltDB underlying every persisted
	// surface: ruleStore + taskStore + inProgress all derived from boltDB.
	boltDB     *bolt.DB
	ruleStore  rules.Store
	taskStore  tasks.Store
	inProgress bolt.InProgressStore

	// Backend pool + executor + tasks subsystem (Phase D + E + F).
	backendPool *backend.Pool
	executor    *executor.Executor
	runner      *tasks.Runner

	// Background loops (Phase F-1 + F-4).
	scheduler *scheduler.Scheduler
	ttlRunner *tasks.TTLRunner

	// HTTP handler bundles (Phase E-2 + E-3 + F-4).
	ruleHandlers *rules.Handlers
	taskHandlers *tasks.Handlers

	// Signal handling for SIGHUP reload (Phase F-3).
	sighupCh chan os.Signal

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

	// Phase F: BoltDB + executor + runner + scheduler + TTL. Order is
	// important: state store first (so we can recover interrupted tasks
	// before any new ones land), then executor/runner, then scheduler/TTL.
	if err = s.initStateStore(); err != nil {
		return fmt.Errorf("init state store: %w", err)
	}
	if err = s.initExecutorAndRunner(); err != nil {
		return fmt.Errorf("init executor: %w", err)
	}
	if err = s.bootstrapRulesFromConfig(); err != nil {
		return fmt.Errorf("bootstrap rules: %w", err)
	}
	if err = s.validateRuleConflicts(); err != nil {
		return err
	}
	if err = s.initSchedulerAndTTL(); err != nil {
		return fmt.Errorf("init scheduler / ttl: %w", err)
	}

	// HTTP admin server — everything above is wired so handlers' backends
	// are ready by the time we accept the first request.
	if err = s.startHTTPServer(); err != nil {
		return fmt.Errorf("start http server: %w", err)
	}

	// SIGHUP → reload. Lives for the life of the process.
	s.installSIGHUPHandler()

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
	// Stop the periodic loops first so they don't try to use stores we're
	// about to close.
	if s.scheduler != nil {
		_ = s.scheduler.Stop()
	}
	if s.ttlRunner != nil {
		_ = s.ttlRunner.Stop()
	}
	if s.sighupCh != nil {
		signal.Stop(s.sighupCh)
		close(s.sighupCh)
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	// Drain the executor: cancel any still-running tasks, then close.
	if s.executor != nil {
		_ = s.executor.Close()
	}
	if s.backendPool != nil {
		_ = s.backendPool.Close()
	}
	if s.boltDB != nil {
		_ = s.boltDB.Close()
	}
	s.wg.Wait()
	log.LogInfo("syncnode shutdown complete")
}

// parseConfig loads the raw config.Config into a typed SyncConfig and runs
// validateConfig. On failure the returned error includes ConfigError code +
// field for operators / tests. Records cfg.FilePath into s.cfgPath so reload
// can re-read the same file.
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
	s.cfgMu.Lock()
	s.cfg = sc
	// Record the file path from the standard cfs-server "-c" flag. SIGHUP
	// reload reads from the same file. Empty when the service is invoked
	// without -c (rare; production always uses it).
	if f := flag.Lookup("c"); f != nil {
		if p := f.Value.String(); p != "" {
			abs, err := filepath.Abs(p)
			if err == nil {
				s.cfgPath = abs
			} else {
				s.cfgPath = p
			}
		}
	}
	s.cfgMu.Unlock()
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
	router.HandleFunc("/admin/syncnode/version", api.ToHTTPHandler(s.handleVersion, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/syncnode/stat", api.ToHTTPHandler(s.handleStat, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/syncnode/reload", api.ToHTTPHandler(s.handleReload, api.AuthMiddleware)).Methods(http.MethodPost)

	// Admin API subsystems register their own routes.
	if s.ruleHandlers != nil {
		s.ruleHandlers.Register(router)
	}
	if s.taskHandlers != nil {
		s.taskHandlers.Register(router)
	}

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

// initStateStore opens the BoltDB at {dataDir}/syncnode.db, derives the
// rule / task / in-progress stores from it, and runs the crash-recovery
// sweep (any pending/running tasks left over from a previous run are
// marked failed with "interrupted by node restart").
func (s *SyncNode) initStateStore() error {
	if s.cfg.DataDir == "" {
		return errors.New("dataDir is required for BoltDB state store")
	}
	if err := os.MkdirAll(s.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir dataDir %q: %w", s.cfg.DataDir, err)
	}
	dbPath := filepath.Join(s.cfg.DataDir, "syncnode.db")
	db, err := bolt.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open bolt %q: %w", dbPath, err)
	}
	if hErr := db.Health(); hErr != nil {
		_ = db.Close()
		return fmt.Errorf("bolt health check: %w", hErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	interrupted, err := db.Recover(ctx)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("bolt recover: %w", err)
	}
	if interrupted > 0 {
		log.LogWarnf("syncnode crash recovery: marked %d task(s) interrupted", interrupted)
	}
	s.boltDB = db
	s.ruleStore = db.RuleStore()
	s.taskStore = db.TaskStore()
	s.inProgress = db.InProgress()
	s.ruleHandlers = rules.NewHandlers(s.ruleStore)
	return nil
}

// initExecutorAndRunner constructs the per-task executor pool and the
// tasks.Runner that fronts it over HTTP / scheduler triggers. The
// BackendBuilder adapter (backend_builder.go) plugs in the shared pool so
// rules pointing at the same (kind, endpoint, region) reuse one HTTP/2
// connection pool.
func (s *SyncNode) initExecutorAndRunner() error {
	s.backendPool = backend.NewPool()

	execOpts := []executor.Option{}
	if s.cfg.Concurrency.TransfersPerTask > 0 {
		execOpts = append(execOpts, executor.WithTransfersPerTask(s.cfg.Concurrency.TransfersPerTask))
	}
	if s.cfg.Concurrency.BandwidthLimitMBps > 0 {
		execOpts = append(execOpts, executor.WithBandwidthLimit(s.cfg.Concurrency.BandwidthLimitMBps))
	}
	s.executor = executor.New(execOpts...)

	builder := newBackendBuilder(s.backendPool, s.cfg)
	s.runner = tasks.NewRunner(s.executor, s.taskStore, s.ruleStore, builder)
	s.taskHandlers = tasks.NewHandlers(s.runner, s.taskStore)
	return nil
}

// bootstrapRulesFromConfig upserts every rule declared in sync.json into
// the rule store. Pre-existing rules with the same ID keep their runtime
// state (CreatedAt, State, last-run summary) and only get their Config
// portion overwritten — mirrors the SIGHUP reload semantics.
func (s *SyncNode) bootstrapRulesFromConfig() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range s.cfg.Rules {
		cfg := s.cfg.Rules[i]
		existing, err := s.ruleStore.Get(ctx, cfg.ID)
		if err != nil && !stderrors.Is(err, rules.ErrRuleNotFound) {
			return fmt.Errorf("get %q: %w", cfg.ID, err)
		}
		if existing == nil {
			if cErr := s.ruleStore.Create(ctx, rules.NewRule(cfg)); cErr != nil {
				return fmt.Errorf("create %q: %w", cfg.ID, cErr)
			}
			continue
		}
		updated := *existing
		updated.Config = cfg
		updated.UpdatedAt = time.Now()
		if uErr := s.ruleStore.Update(ctx, &updated); uErr != nil {
			return fmt.Errorf("update %q: %w", cfg.ID, uErr)
		}
	}
	return nil
}

// validateRuleConflicts runs the E-4 validator over the full persisted rule
// set after bootstrap. Failures at startup mean operators have edited
// sync.json into a conflicting state and need to fix it before the node
// will accept work.
func (s *SyncNode) validateRuleConflicts() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stored, err := s.ruleStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list rules for startup validation: %w", err)
	}
	if vErr := rules.Validate(stored); vErr != nil {
		return fmt.Errorf("rule conflict at startup: %w", vErr)
	}
	return nil
}

// initSchedulerAndTTL builds the cron scheduler + the TTL Runner and
// arms both. ApplyRules is called once after Start with the current store
// snapshot; subsequent rule changes (HTTP create/update/delete/pause/resume,
// SIGHUP reload) re-call ApplyRules via the reload path.
func (s *SyncNode) initSchedulerAndTTL() error {
	s.scheduler = scheduler.New(s.ruleStore, s.runner)
	if err := s.scheduler.Start(context.Background()); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stored, err := s.ruleStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list rules for scheduler: %w", err)
	}
	if err := s.scheduler.ApplyRules(stored); err != nil {
		// Non-fatal: some rules had bad cron expressions. Already-good
		// rules are armed; operators fix the bad ones via the API.
		log.LogWarnf("syncnode: scheduler.ApplyRules partial: %v", err)
	}

	s.ttlRunner = tasks.NewTTLRunner(s.taskStore)
	if err := s.ttlRunner.Start(context.Background()); err != nil {
		return fmt.Errorf("start ttl runner: %w", err)
	}
	return nil
}

// installSIGHUPHandler arms a goroutine that calls reload on every SIGHUP.
// Reload failures are logged and reflected in reloadFailuresTotal but do
// NOT crash the process — the in-flight config stays active.
func (s *SyncNode) installSIGHUPHandler() {
	s.sighupCh = make(chan os.Signal, 1)
	signal.Notify(s.sighupCh, syscall.SIGHUP)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.stopC:
				return
			case _, ok := <-s.sighupCh:
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := s.reload(ctx); err != nil {
					log.LogErrorf("syncnode SIGHUP reload failed: %v", err)
				} else {
					log.LogInfof("syncnode SIGHUP reload OK")
				}
				cancel()
			}
		}
	}()
}

// handleVersion responds with build info + role identity. AC for A-1.
func (s *SyncNode) handleVersion(r *http.Request) (interface{}, error) {
	return map[string]string{
		"role":        ModuleName,
		"version":     BuildVersion,
		"commit":      BuildCommit,
		"buildTime":   BuildTime,
		"nodeAddress": s.localServerAddr,
	}, nil
}

// handleStat is the runtime snapshot endpoint. Now exposes scheduler size +
// reload failure count so operators can sanity-check the live state.
func (s *SyncNode) handleStat(r *http.Request) (interface{}, error) {
	out := map[string]interface{}{
		"role":                ModuleName,
		"uptimeSeconds":       time.Since(startedAt).Seconds(),
		"concurrentTasks":     concurrentTasks.Load(),
		"reloadFailuresTotal": reloadFailuresTotal.Load(),
	}
	if s.scheduler != nil {
		out["scheduledRules"] = s.scheduler.RegisteredCount()
	}
	if s.executor != nil {
		out["runningTasks"] = s.executor.RunningCount()
	}
	if s.boltDB != nil {
		if err := s.boltDB.Health(); err == nil {
			out["boltdbHealthy"] = true
		} else {
			out["boltdbHealthy"] = false
		}
	}
	return out, nil
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
