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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/barrier"
	"github.com/cubefs/cubefs/syncnode/bolt"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/ratelimit"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/tasks"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Master client (Phase B-3 + B-4). Owns the register + heartbeat
	// goroutines. nil until doStart wires it.
	masterClient *SyncMasterClient

	// State store (Phase F-2). One BoltDB underlying every persisted
	// surface: ruleStore + taskStore + inProgress all derived from boltDB.
	boltDB     *bolt.DB
	ruleStore  rules.Store
	taskStore  tasks.Store
	inProgress bolt.InProgressStore

	// Backend pool + executor + tasks subsystem (Phase D + E + F).
	backendPool    *backend.Pool
	backendBuilder *backendBuilder // P2-5: shared with TaskHandler for auto-prefix probe
	executor       *executor.Executor
	runner         *tasks.Runner
	rateLimits     *ratelimit.Registry

	// taskHandler dispatches OpSyncNodeRunTask / OpSyncNodeCancelTask packets
	// pushed by master onto the TCP listener (Phase P1-3). Built after
	// initExecutorAndRunner so it can wrap the runner.
	taskHandler *TaskHandler

	// Background loops (Phase F-4). P2-6 deleted the local cron
	// scheduler — master is now the authoritative scheduler. ttlRunner
	// stays because terminal record purging is a local-bolt concern.
	ttlRunner *tasks.TTLRunner

	// snapshotCache holds the heartbeat-input gauges that would otherwise
	// require a BoltDB scan per Snapshot() call (computeRecentFailureRate
	// + advertiseRules). Refreshed every snapshotCacheRefresh by a
	// background goroutine started in doStart; Snapshot() does
	// atomic-only reads on the hot path. nil before startSnapshotCacheLoop
	// runs (Snapshot returns zero gauges in that window).
	snapshotCache *snapshotCache

	// HTTP handler bundles. P2-6 removed the rule + task admin
	// surfaces from syncnode; they live on master now. Only the
	// /admin/syncnode/{version,stat,reload} endpoints stay.

	// Signal handling for SIGHUP reload (Phase F-3).
	sighupCh chan os.Signal

	// Lifecycle
	stopC   chan struct{}
	control common.Control

	// Synchronisation for background goroutines started by doStart.
	wg sync.WaitGroup
}

// Build / version info reported by /admin/syncnode/version. Delegated to
// proto.Version/CommitID/BuildTime which are injected at link time via ldflags
// in build.sh. No separate vars needed here.

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

	// Expose the bench metrics on a SEPARATE path (/metrics/bench) over the
	// same listener that cmd/cmd.go's exporter.Init has already opened. The
	// exporter does `http.Serve(l, nil)` against DefaultServeMux, so any
	// http.Handle call here lands on the same socket the scraper hits.
	// Independent registry keeps high-cardinality task_id/shard/stage/op
	// series out of the node-level /metrics endpoint.
	http.Handle("/metrics/bench", promhttp.HandlerFor(executor.BenchRegistry(), promhttp.HandlerOpts{}))

	// Sprint 3 / S3.1: launch the client-side resource sampler (CPU, RSS,
	// host NIC bytes, host disk bytes, fd count, goroutines) on the same
	// isolated registry. Pairs the syncnode bench worker's own resource
	// footprint with the bench op metrics so dashboards can attribute
	// throughput stalls to client-side saturation.
	executor.StartClientMetricsSampler(s.stopC)

	// Register with Consul once master returns our clusterID. cmd/cmd.go calls
	// exporter.Init (which mounts /metrics + creates the prom HTTP server) but
	// NOT RegistConsul — every other role (master/datanode/lcnode/flashnode/
	// objectnode) does that itself in its own server.go after it knows the
	// cluster. We mirror datanode's "after first successful register" pattern:
	// fire a one-shot goroutine that waits for masterClient to populate
	// clusterID, then calls RegistConsul once. Without this, Prometheus's
	// Consul SD never discovers syncnode targets.
	go s.registerConsulOnce(cfg)

	// Phase F: BoltDB + executor + runner + scheduler + TTL. Order is
	// important: state store first (so we can recover interrupted tasks
	// before any new ones land), then executor/runner, then scheduler/TTL.
	if err = s.initStateStore(); err != nil {
		return fmt.Errorf("init state store: %w", err)
	}
	if err = s.initExecutorAndRunner(); err != nil {
		return fmt.Errorf("init executor: %w", err)
	}
	// S1.6: install the cross-shard bench barrier. ConsulAddr empty or
	// unreachable degrades to a process-local MemBarrier so the
	// executor never has to nil-check. Boot must succeed even if
	// Consul is temporarily down.
	s.initBenchBarrier()
	if err = s.bootstrapRulesFromConfig(); err != nil {
		return fmt.Errorf("bootstrap rules: %w", err)
	}
	if err = s.validateRuleConflicts(); err != nil {
		return err
	}
	if err = s.initTTLRunner(); err != nil {
		return fmt.Errorf("init ttl runner: %w", err)
	}

	// Start the snapshot cache loop AFTER initStateStore (taskStore +
	// ruleStore are wired) AND bootstrapRulesFromConfig (rules are
	// seeded) so the immediate seed reads non-empty stores. Lifts the
	// per-heartbeat BoltDB scan off the hot path.
	s.startSnapshotCacheLoop()

	// Phase B-3 + B-4 + P1-3: construct the master client BEFORE the TCP
	// listener so the TaskHandler can carry it. Start() (which kicks off the
	// register + heartbeat goroutines) still happens at the end of doStart,
	// after the HTTP admin surface is up, so the syncnode is fully ready to
	// service requests before it announces itself to master.
	s.masterClient = NewSyncMasterClient(s.cfg.MasterAddr, s.cfg.Listen,
		WithSnapshotProvider(s),
		WithRateLimitRegistry(s.rateLimits))
	s.taskHandler = NewTaskHandler(s.runner, s.masterClient,
		WithReadIdleTimeout(s.cfg.TCP.ResolvedReadIdleTimeout()),
		withBackendBuilder(s.backendBuilder))

	// SEC4: install the admin auth token before the HTTP listener comes
	// up. An empty token disables auth (preserves pre-fix behaviour for
	// tests + dev). Operators rotating the token should restart the
	// process — the slot is threadsafe but the bootstrap path only
	// reads cfg once.
	api.SetAdminToken(s.cfg.AdminToken)

	// TCP server for master-dispatched task packets (Phase P1-3). Must be
	// after runner + taskHandler are wired so the accept loop has somewhere
	// to dispatch.
	if err = s.startTCPServer(); err != nil {
		return fmt.Errorf("start tcp server: %w", err)
	}

	// HTTP admin server — everything above is wired so handlers' backends
	// are ready by the time we accept the first request.
	if err = s.startHTTPServer(); err != nil {
		return fmt.Errorf("start http server: %w", err)
	}

	// SIGHUP → reload. Lives for the life of the process.
	s.installSIGHUPHandler()

	// Phase B-3 + B-4: start the register + heartbeat goroutines now that
	// every other subsystem is alive. Start() returns immediately; the first
	// register attempt happens in the background so a missing master does
	// not block syncnode boot.
	if err = s.masterClient.Start(context.Background()); err != nil {
		return fmt.Errorf("start master client: %w", err)
	}
	s.localServerAddr = s.masterClient.LocalServerAddr() // may be empty if first register pending

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
	// Stop the master client first so register/heartbeat goroutines exit
	// before we tear down the stores they snapshot.
	if s.masterClient != nil {
		_ = s.masterClient.Stop()
	}
	// Stop the periodic loops first so they don't try to use stores we're
	// about to close. P2-6: local cron scheduler removed; master is the
	// scheduler authority. Only ttlRunner stays.
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
	// FIX Q2 — drain Runner BEFORE Executor. Runner.Close cancels every
	// queued task ctx so runAfterWait goroutines exit cleanly before the
	// executor goes away. Otherwise a queued goroutine could call
	// executor.Run on a torn-down running map → panic.
	if s.runner != nil {
		_ = s.runner.Close()
	}
	// Drain the executor: cancel any still-running tasks, then close.
	if s.executor != nil {
		_ = s.executor.Close()
	}
	if s.backendPool != nil {
		_ = s.backendPool.Close()
	}
	// S6: stop the rules NotifyStore worker (if wrapped) before the
	// BoltDB underneath is closed so an in-flight onChange can't fire
	// against a torn-down scheduler / closed boltDB. ruleStore.Close()
	// is a no-op for non-NotifyStore implementations.
	if s.ruleStore != nil {
		_ = s.ruleStore.Close()
	}
	if s.boltDB != nil {
		_ = s.boltDB.Close()
	}
	s.wg.Wait()
	log.LogInfo("syncnode shutdown complete")
}

// registerConsulOnce mirrors the datanode/lcnode/objectnode pattern: wait
// for masterClient to populate clusterID after the first successful master
// register, then call exporter.RegistConsul exactly once. The Init call in
// cmd/cmd.go only mounts /metrics; without RegistConsul, Prometheus's
// Consul SD never lists syncnode as a target. Exits silently on shutdown.
func (s *SyncNode) registerConsulOnce(cfg *config.Config) {
	const pollInterval = time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopC:
			return
		case <-ticker.C:
		}
		if s.masterClient == nil {
			continue
		}
		cid := s.masterClient.ClusterID()
		if cid == "" {
			continue
		}
		s.clusterID = cid
		exporter.RegistConsul(cid, ModuleName, cfg)
		log.LogInfof("syncnode: registered with consul as cluster=%s role=%s", cid, ModuleName)
		return
	}
}

// parseConfig loads the raw config.Config into a typed SyncConfig and runs
// validateConfig. On failure the returned error includes ConfigError code +
// currentConfig returns the live *SyncConfig under cfgMu so callers that
// need to read masterAddr / s3Defaults / posix / concurrency get whatever
// the most recent SIGHUP reload landed. nil during the brief startup
// window before parseConfig fires.
func (s *SyncNode) currentConfig() *SyncConfig {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

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
	maxConns := s.cfg.TCP.ResolvedMaxConnections()
	// SEC2: cap the in-flight HandleConn goroutines via a buffered
	// channel semaphore. A flood of accepts can no longer spawn an
	// unbounded number of goroutines or hold open FDs forever —
	// over-cap connections are accepted and immediately closed so the
	// master sees a fast signal rather than a queued goroutine that
	// may never get to read.
	sem := make(chan struct{}, maxConns)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// One ctx for the whole accept loop; cancelled when stopC closes so
		// in-flight HandleConn goroutines exit promptly on shutdown.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-s.stopC:
				cancel()
			case <-ctx.Done():
			}
		}()
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
			// SEC2: try to reserve a slot. Fail-fast on rejection
			// rather than queueing — master should retry against a
			// less-loaded peer rather than wait on us.
			select {
			case sem <- struct{}{}:
			default:
				log.LogWarnf("tcp listener: max %d in-flight conns, rejecting %s",
					maxConns, conn.RemoteAddr())
				_ = conn.Close()
				continue
			}
			// P1-3: delegate to the TaskHandler. Each connection drains its
			// own goroutine. HandleConn closes the conn before returning.
			s.wg.Add(1)
			go func(c net.Conn) {
				defer func() {
					<-sem
					s.wg.Done()
				}()
				s.taskHandler.HandleConn(ctx, c)
			}(conn)
		}
	}()
	return nil
}

func (s *SyncNode) startHTTPServer() error {
	router := mux.NewRouter().SkipClean(true)
	router.HandleFunc("/admin/syncnode/version", api.ToHTTPHandler(s.handleVersion, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/syncnode/stat", api.ToHTTPHandler(s.handleStat, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/syncnode/reload", api.ToHTTPHandler(s.handleReload, api.AuthMiddleware)).Methods(http.MethodPost)

	// P2-6: /admin/sync/rule/* and /admin/sync/task/* moved to master.
	// Console + ops talk to master at /syncRule/* and /syncTask/*.

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
	db, err := bolt.Open(s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open bolt %q: %w", s.cfg.DataDir, err)
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
	// Wrap the persisted rule store in a NotifyStore so every CRUD /
	// auto-degrade lands at the scheduler within milliseconds. The
	// OnChange callback is wired later in initSchedulerAndTTL (chicken-
	// and-egg: the scheduler doesn't exist yet).
	s.ruleStore = rules.NewNotifyStore(db.RuleStore(), nil)
	s.taskStore = db.TaskStore()
	s.inProgress = db.InProgress()
	// P2-6: rule HTTP handlers removed; master owns the rule API.
	return nil
}

// initExecutorAndRunner constructs the per-task executor pool and the
// tasks.Runner that fronts it over HTTP / scheduler triggers. The
// BackendBuilder adapter (backend_builder.go) plugs in the shared pool so
// rules pointing at the same (kind, endpoint, region) reuse one HTTP/2
// connection pool.
func (s *SyncNode) initExecutorAndRunner() error {
	s.backendPool = backend.NewPool()

	// G-2: the rate-limit registry holds layer-3 (node) + layer-4
	// (per-backend) buckets. We always construct it so future SetBackendLimit
	// calls (e.g. via SIGHUP reload) have somewhere to land; a zero node
	// bandwidth means "unlimited" inside the Bucket.
	s.rateLimits = ratelimit.NewRegistry(s.cfg.Concurrency.BandwidthLimitMBps)

	execOpts := []executor.Option{
		executor.WithRateLimitRegistry(s.rateLimits),
	}
	if s.cfg.Concurrency.TransfersPerTask > 0 {
		execOpts = append(execOpts, executor.WithTransfersPerTask(s.cfg.Concurrency.TransfersPerTask))
	}
	if s.cfg.Concurrency.BandwidthLimitMBps > 0 {
		execOpts = append(execOpts, executor.WithBandwidthLimit(s.cfg.Concurrency.BandwidthLimitMBps))
	}
	// Wire the bolt-backed in-progress store into the executor so P2 resume
	// works in production. The adapter bridges the two package-local
	// Breakpoint structs without introducing an import cycle.
	if s.inProgress != nil {
		execOpts = append(execOpts, executor.WithInProgressStore(bolt.AdaptForExecutor(s.inProgress)))
	}
	s.executor = executor.New(execOpts...)

	// FIX D: pass a cfg-provider closure rather than the current pointer
	// so SIGHUP reload's atomic cfg swap takes effect for the next
	// Backend construction. The closure reads under cfgMu so a half-swap
	// can't race a Build.
	builder := newBackendBuilder(s.backendPool, s.currentConfig)
	s.backendBuilder = builder // P2-5: stash so TaskHandler can reuse for ListPrefixes probe
	runnerOpts := []tasks.RunnerOption{
		tasks.WithOnTerminal(s.onTaskTerminal),
	}
	// Wire the concurrency-gate options from cfg.Concurrency. Both fields
	// already exist (master reads them for the load score); this is the
	// first place the syncnode itself enforces them. A zero value keeps
	// the prior unlimited behavior so existing operators with empty caps
	// see no change.
	if n := s.cfg.Concurrency.MaxConcurrentTasks; n > 0 {
		runnerOpts = append(runnerOpts, tasks.WithMaxConcurrent(n))
	}
	if n := s.cfg.Concurrency.MaxQueueSize; n > 0 {
		runnerOpts = append(runnerOpts, tasks.WithQueueSize(n))
	}
	s.runner = tasks.NewRunner(s.executor, s.taskStore, s.ruleStore, builder, runnerOpts...)
	// P2-6: task HTTP handlers removed; master owns the task API.
	return nil
}

// initBenchBarrier wires the cross-shard bench barrier into the
// executor (S1.6). When ConsulAddr is set we build a Consul-backed
// barrier; otherwise — or when the Consul client cannot be constructed
// at all (DNS / config error) — we fall back to a process-local
// MemBarrier so the executor never has to nil-check.
//
// We do NOT block startup if Consul is unreachable: NewConsulBarrier
// logs a warning but returns a usable client whose first Ready() call
// surfaces the error per-stage. The boot path stays cheap.
func (s *SyncNode) initBenchBarrier() {
	addr := s.cfg.ConsulAddr
	if addr == "" {
		log.LogInfof("bench barrier: consulAddr empty, using in-process MemBarrier fallback")
		executor.SetBarrier(barrier.NewMemBarrier(1))
		return
	}
	b, err := barrier.NewConsulBarrier(addr)
	if err != nil {
		log.LogWarnf("bench barrier: consul client init failed (addr=%q): %v — falling back to MemBarrier", addr, err)
		executor.SetBarrier(barrier.NewMemBarrier(1))
		return
	}
	executor.SetBarrier(b)
	log.LogInfof("bench barrier: consul-backed barrier installed (addr=%q)", addr)
}

// onTaskTerminal pushes a task lifecycle update to master via the
// ResponseTask SDK path. Wired into the Runner via WithOnTerminal in
// initExecutorAndRunner. Best-effort: any error logs + drops on the
// floor — the master will eventually deduce terminal via heartbeat
// timeout if it can't be reached now.
//
// Fixes Bug #3: syncnode → master terminal signalling. Without this,
// master never learns when a task finishes and
// syncFailover.payloads / syncDispatcher.taskOwner grow unbounded.
func (s *SyncNode) onTaskTerminal(rec *tasks.Record) {
	if s == nil || s.masterClient == nil || rec == nil {
		return
	}
	report := &proto.TaskTerminalReport{
		TaskID: rec.TaskID,
		Status: string(rec.Status),
		Error:  rec.Error,
		Progress: proto.TaskTerminalProgress{
			FilesTotal:     rec.Progress.FilesTotal,
			FilesDone:      rec.Progress.FilesDone,
			FilesSkipped:   rec.Progress.FilesSkipped,
			FilesFailed:    rec.Progress.FilesFailed,
			BytesTotal:     rec.Progress.BytesTotal,
			BytesDone:      rec.Progress.BytesDone,
			BytesSkipped:   rec.Progress.BytesSkipped,
			ThroughputMBps: rec.Progress.ThroughputMBps,
		},
	}
	if rec.BenchResult != nil {
		// Fill in the local node address on terminal report so the master
		// ledger / dashboard can attribute results to a specific syncnode
		// (the bench executor itself has no notion of "self").
		br := *rec.BenchResult
		if br.NodeAddr == "" {
			br.NodeAddr = s.masterClient.LocalServerAddr()
		}
		report.BenchResult = &br
	}
	task := proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, s.masterClient.LocalServerAddr(), nil, rec.TaskID)
	task.Response = report
	if err := s.masterClient.ResponseTask(task); err != nil {
		log.LogWarnf("syncnode: push terminal %q status=%q: %v", rec.TaskID, rec.Status, err)
	} else {
		log.LogInfof("syncnode: pushed terminal %q status=%q", rec.TaskID, rec.Status)
	}
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

// initTTLRunner builds the TTL Runner for terminal-record purging.
// P2-6: deleted the local cron scheduler. Master is now the authoritative
// scheduler; this routine only wires up the local TTL cleanup loop.
func (s *SyncNode) initTTLRunner() error {
	s.ttlRunner = tasks.NewTTLRunner(s.taskStore)
	if err := s.ttlRunner.Start(context.Background()); err != nil {
		return fmt.Errorf("start ttl runner: %w", err)
	}
	return nil
}

// applyRulesToScheduler is retained as a no-op stub so the few callsites
// that haven't been deleted (SIGHUP reload + bootstrap) compile without
// edits. Master is the scheduler authority post P2-6.
func (s *SyncNode) applyRulesToScheduler() {}

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
		"version":     proto.Version,
		"commit":      proto.CommitID,
		"buildTime":   proto.BuildTime,
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
	// P2-6: scheduledRules always 0 — master is the cron authority.
	// Field kept on the wire so console doesn't see a schema break.
	out["scheduledRules"] = 0
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

// _ = proto.* is a build-time sanity check that proto package is reachable.
// Keeps the import grouped with other CubeFS deps for future expansion.
var _ = proto.ListenPort
