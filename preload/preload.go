package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/preload/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
)

var (
	configFile    = flag.String("c", "", "config file path")
	configVersion = flag.Bool("v", false, "show version")
)

const (
	Role = "Client"
)

func main() {
	defer sdk.FlushLog()
	flag.Parse()

	if *configVersion {
		fmt.Print(proto.DumpVersion(Role))
		os.Exit(0)
	}
	//TODO: why *configFlie
	cfg, err := config.LoadConfigFile(*configFile)
	if err != nil {
		fmt.Println("LoadConfigFile failed")
		os.Exit(1)
	}

	if !checkConfig(cfg) {
		os.Exit(1)
	}
	masters := strings.Split(cfg.GetString("masterAddr"), ",")
	travereDirConcurrency, _ := strconv.ParseInt(cfg.GetString("traverseDirConcurrency"), 10, 64)
	preloadFileConcurrency, _ := strconv.ParseInt(cfg.GetString("preloadFileConcurrency"), 10, 64)
	preloadFileSizeLimit, _ := strconv.ParseInt(cfg.GetString("preloadFileSizeLimit"), 10, 64)
	readBlockConcurrency, _ := strconv.ParseInt(cfg.GetString("readBlockConcurrency"), 10, 64)
	clearFileConcurrency, _ := strconv.ParseInt(cfg.GetString("clearFileConcurrency"), 10, 64)
	buffersTotalLimit, _ := strconv.ParseInt(cfg.GetString("buffersTotalLimit"), 10, 64)
	replicaNum, _ := strconv.ParseInt(cfg.GetString("replicaNum"), 10, 64)
	ttl, _ := strconv.ParseInt(cfg.GetString("ttl"), 10, 64)

	if replicaNum > 16 || replicaNum < 1 {
		fmt.Println("replicaNum must between [1,16]")
		os.Exit(1)
	}
	if buffersTotalLimit < 0 {
		fmt.Println("buffersTotalLimit cannot less than 0")
		os.Exit(1)
	} else if buffersTotalLimit == 0{
		buffersTotalLimit = int64(32768)
	}
	proto.InitBufferPool(buffersTotalLimit)
	config := sdk.PreloadConfig{
		Volume:   cfg.GetString("volumeName"),
		Masters:  masters,
		LogDir:   cfg.GetString("logDir"),
		LogLevel: cfg.GetString("logLevel"),
		ProfPort: cfg.GetString("prof"),
		LimitParam: sdk.LimitParameters{TraverseDirConcurrency: travereDirConcurrency,
			PreloadFileConcurrency: preloadFileConcurrency,
			ReadBlockConcurrency:   readBlockConcurrency,
			PreloadFileSizeLimit:   preloadFileSizeLimit,
			ClearFileConcurrency:   clearFileConcurrency},
	}

	cli := sdk.NewClient(config)

	if cli == nil {
		fmt.Println("Preload client created failed")
		os.Exit(1)
	}

	if cli.CheckColdVolume() == false {
		fmt.Println("Preload only work in cold volume")
		os.Exit(1)
	}
	fmt.Printf("conf is %v\n", config)
	action := cfg.GetString("action")
	if action == "preload" {
		if err := cli.PreloadDir(cfg.GetString("target"), int(replicaNum), uint64(ttl), cfg.GetString("zones")); err != nil {
			total, succeed := cli.GetPreloadResult()
			fmt.Printf("Preload failed:%v\n",err)
			fmt.Printf("Result: total[%v], succeed[%v]\n", total, succeed)
		} else {
			fmt.Println("Preload succeed")
		}
	} else if action == "clear" {
		cli.ClearPreloadDP(cfg.GetString("target"))
	} else {
		fmt.Printf("action[%v] is not support\n", action)
		os.Exit(1)
	}

}

func checkConfig(cfg *config.Config) bool {
	var masters = cfg.GetString("masterAddr")
	var target = cfg.GetString("target")
	var vol = cfg.GetString("volumeName")
	var logDir = cfg.GetString("logDir")
	var logLevel = cfg.GetString("logLevel")
	var ttl = cfg.GetString("ttl")
	var action = cfg.GetString("action")

	if len(masters) == 0 || len(target) == 0 || len(vol) == 0 || len(logDir) == 0 ||
		len(logLevel) == 0 || len(ttl) == 0 || len(action) == 0 {
		fmt.Println("masterAddr, target, volumeName, logDir, logLevel, ttl , action cannot be empty")
		return false
	}

	return true
}
