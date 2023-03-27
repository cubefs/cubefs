package main

import (
	"flag"
	"fmt"
	syslog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/cli/repaircrc/repaircrc_server"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ump"
)

var (
	configFile = flag.String("c", "", "config file path")
)
var (
	configKeyLogDir   = "logDir"
	configKeyLogLevel = "logLevel"
	configKeyProfPort = "prof"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()
	cfg, err := config.LoadConfigFile(*configFile)
	if err != nil {
		return err
	}

	logDir := cfg.GetString(configKeyLogDir)
	logLevel := cfg.GetString(configKeyLogLevel)
	profPort := cfg.GetString(configKeyProfPort)

	var (
		server *repaircrc_server.RepairServer
	)
	server = repaircrc_server.NewServer()

	// Init logging
	var (
		level log.Level
	)
	switch strings.ToLower(logLevel) {
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

	_, err = log.InitLog(logDir, "repair_server", level, nil)
	if err != nil {
		return err
	}
	defer log.LogFlush()

	if err = ump.InitUmp("check_crc", "jdos_chubaofs-node"); err != nil {
		log.LogErrorf("init ump failed: %v", err)
		log.LogFlush()
		syslog.Printf("Fatal: init ump failed: %v ", err)
		return err
	}

	var profNetListener net.Listener = nil
	if profPort != "" {
		// 监听prof端口
		if profNetListener, err = net.Listen("tcp", fmt.Sprintf(":%v", profPort)); err != nil {
			log.LogErrorf("listen prof port %v failed: %v", profPort, err)
			log.LogFlush()
			syslog.Printf("Fatal: listen prof port %v failed: %v", profPort, err)
			return err
		}
		// 在prof端口监听上启动http API.
		go func() {
			_ = http.Serve(profNetListener, http.DefaultServeMux)
		}()
	}
	interceptSignal(server)
	if err = server.DoStart(cfg); err != nil {
		log.LogErrorf("start service failed: %v", err)
		log.LogFlush()
		syslog.Printf("Fatal: failed to start %v - ", err)
		return err
	}
	log.LogInfof("repair server started")
	// Block main goroutine until server shutdown.
	server.Sync()
	log.LogFlush()
	if profNetListener != nil {
		// 关闭prof端口监听
		_ = profNetListener.Close()
	}

	return nil
}

func interceptSignal(s *repaircrc_server.RepairServer) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	syslog.Println("action[interceptSignal] register system signal.")
	go func() {
		sig := <-sigC
		syslog.Printf("action[interceptSignal] received signal: %s.", sig.String())
		s.Shutdown()
	}()
}
