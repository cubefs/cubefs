package checktool

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestServer(t *testing.T) {
	cfg, err := config.LoadConfigFile("config/cfg.json")
	if err != nil {
		t.Fatal(err)
	}
	logDir := "log"
	logLevel := "debug"
	runtime.GOMAXPROCS(runtime.NumCPU())
	ump.InitUmp("checktool")
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
	if _, err = log.InitLog(logDir, "checktool", level, nil); err != nil {
		fmt.Println("Fatal: failed to start the baud storage daemon - ", err)
		os.Exit(1)
		return
	}

	cw := NewChecktoolWorker()
	if err = cw.Start(cfg); err != nil {
		t.Fatal(err)
	}
	cw.Sync()
}
