// Copyright 2022 The CubeFS Authors.
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

package cmd

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
	"github.com/cubefs/cubefs/blobstore/util/graceful"
	"github.com/cubefs/cubefs/blobstore/util/log"

	// module release init functions
	_ "github.com/cubefs/cubefs/blobstore/util/version"
)

const (
	defaultShutdownTimeoutS = 30
)

type LogConfig struct {
	Level      log.Level `json:"level"`
	Filename   string    `json:"filename"`
	MaxSize    int       `json:"maxsize"`
	MaxAge     int       `json:"maxage"`
	MaxBackups int       `json:"maxbackups"`
}

type Config struct {
	MaxProcs         int       `json:"max_procs"`
	LogConf          LogConfig `json:"log"`
	BindAddr         string    `json:"bind_addr"`
	ShutdownTimeoutS int       `json:"shutdown_timeout_s"`

	AuditLog auditlog.Config `json:"auditlog"`
	Auth     auth.Config     `json:"auth"`
}

type Module struct {
	Name       string
	InitConfig func(args []string) (*Config, error)
	SetUp      func() (*rpc.Router, []rpc.ProgressHandler)
	TearDown   func()
	graceful   bool
}

var mod *Module

func RegisterModule(m *Module) {
	mod = m
	mod.graceful = false
}

func RegisterGracefulModule(m *Module) {
	mod = m
	mod.graceful = true
}

func newLogWriter(cfg *LogConfig) io.Writer {
	maxsize := cfg.MaxSize
	if maxsize == 0 {
		maxsize = 1024
	}
	maxage := cfg.MaxAge
	if maxage == 0 {
		maxage = 7
	}
	maxbackups := cfg.MaxBackups
	if maxbackups == 0 {
		maxbackups = 7
	}
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    maxsize,
		MaxAge:     maxage,
		MaxBackups: maxbackups,
		LocalTime:  true,
	}
}

func Main(args []string) {
	cfg, err := mod.InitConfig(args)
	if err != nil {
		log.Fatalf("init config error: %v", err)
	}
	if cfg.MaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.MaxProcs)
	}
	log.SetOutputLevel(cfg.LogConf.Level)
	registerLogLevel()
	if cfg.LogConf.Filename != "" {
		log.SetOutput(newLogWriter(&cfg.LogConf))
	}
	if cfg.ShutdownTimeoutS <= 0 {
		cfg.ShutdownTimeoutS = defaultShutdownTimeoutS
	}

	lh, logf, err := auditlog.Open(mod.Name, &cfg.AuditLog)
	if err != nil {
		log.Fatal("failed to open auditlog:", err)
	}
	if logf != nil {
		defer logf.Close()
	}

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	config.HotReload(ctx, config.ConfName())

	if mod.graceful {
		programEntry := func(state *graceful.State) {
			router, handlers := mod.SetUp()

			httpServer := &http.Server{
				Addr:    cfg.BindAddr,
				Handler: reorderMiddleWareHandlers(router, lh, cfg.BindAddr, cfg.Auth, handlers),
			}

			log.Info("server is running at:", cfg.BindAddr)
			go func() {
				if err := httpServer.Serve(state.ListenerFds[0].(*net.TCPListener)); err != nil && err != http.ErrServerClosed {
					log.Fatal("server exits:", err)
				}
			}()

			// wait for signal
			<-state.CloseCh
			log.Info("graceful shutdown...")
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutS)*time.Second)
			defer cancel()
			httpServer.Shutdown(ctx)

			if mod.TearDown != nil {
				mod.TearDown()
			}
		}
		graceful.Run(&graceful.Config{
			Entry:           programEntry,
			ListenAddresses: []string{cfg.BindAddr},
		})
		return
	}

	router, handlers := mod.SetUp()

	httpServer := &http.Server{
		Addr:    cfg.BindAddr,
		Handler: reorderMiddleWareHandlers(router, lh, cfg.BindAddr, cfg.Auth, handlers),
	}

	log.Info("Server is running at", cfg.BindAddr)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server exits, err: %v", err)
		}
	}()

	// wait for signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	sig := <-ch
	log.Infof("receive signal: %s, stop service...", sig.String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutS)*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)

	if mod.TearDown != nil {
		mod.TearDown()
	}
}

// reorderMiddleWareHandlers
//	the order of handlers in MiddlewareHandler has some constraints:
//	1. the first is router,
//	2. the second is AuditLog handler,
//	3. the third is profile handler if config,
//	4. the fourth is Auth handler if config,
//	5. others self define handlers by modules.
func reorderMiddleWareHandlers(r *rpc.Router, lh rpc.ProgressHandler, profileAddr string, authCfg auth.Config, handlers []rpc.ProgressHandler) (mux http.Handler) {
	var hs []rpc.ProgressHandler
	if lh != nil {
		hs = append(hs, lh)
	}
	if profileHandler := profile.NewProfileHandler(profileAddr); profileHandler != nil {
		hs = append(hs, profileHandler)
	}
	if authCfg.EnableAuth && authCfg.Secret != "" {
		hs = append(hs, auth.NewAuthHandler(&authCfg))
	}
	hs = append(hs, handlers...)

	return rpc.MiddlewareHandlerWith(r, hs...)
}

func registerLogLevel() {
	logLevelPath, logLevelHandler := log.ChangeDefaultLevelHandler()
	profile.HandleFunc(http.MethodPost, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
	profile.HandleFunc(http.MethodGet, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
}
