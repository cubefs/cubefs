package lcnode

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	// String type configuration type, used to configure the listening port number of the service.
	// Example:
	//		{
	//			"listen": "80"
	//		}
	configListen = proto.ListenPort
	// String array configuration item, used to configure the hostname or IP address of the cluster master node.
	// The ObjectNode needs to communicate with the Master during the startup and running process to update the
	// cluster, user and volume information.
	// Example:
	//		{
	//			"masterAddr":[
	//				"master1.chubao.io",
	//				"master2.chubao.io",
	//				"master3.chubao.io"
	//			]
	//		}
	configMasterAddr         = proto.MasterAddr
	batchExpirationGetNumStr = "batchExpirationGetNum"
	scanCheckIntervalStr     = "scanCheckInterval"
)

// Default of configuration value
const (
	defaultListen                = "80"
	ModuleName                   = "lcNode"
	defaultBatchExpirationGetNum = 100
	defaultScanCheckInterval     = 60

	defaultMasterIntervalToCheckHeartbeat = 6
	noHeartBeatTimes                      = 3 // number of times that no heartbeat reported
	defaultLcNodeTimeOutSec               = noHeartBeatTimes * defaultMasterIntervalToCheckHeartbeat
	defaultIntervalToCheckRegister        = 2 * defaultLcNodeTimeOutSec
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen          = regexp.MustCompile("^(\\d)+$")
	batchExpirationGetNum int
	scanCheckInterval     int64
)

type LcNode struct {
	listen          string
	localServerAddr string
	clusterID       string
	nodeID          uint64
	httpServer      *http.Server
	//tcpListener     net.Listener
	masters       []string
	mc            *master.MasterClient
	scannerMutex  sync.RWMutex
	scanners      map[string]*TaskScanner
	stopC         chan bool
	lastHeartbeat time.Time
	control       common.Control
}

func NewServer() *LcNode {
	return &LcNode{}
}

func (l *LcNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return l.control.Start(l, cfg, handleStart)
}

// Shutdown shuts down the current lcnode.
func (l *LcNode) Shutdown() {
	l.control.Shutdown(l, handleShutdown)
}

// Sync keeps lcnode in sync.
func (l *LcNode) Sync() {
	l.control.Sync()
}

func (l *LcNode) loadConfig(cfg *config.Config) (err error) {
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
	log.LogInfof("loadConfig: setup config: %v(%v)", configListen, listen)
	// parse master config
	masters := cfg.GetStringSlice(configMasterAddr)
	if len(masters) == 0 {
		return config.NewIllegalConfigError(configMasterAddr)
	}
	log.LogInfof("loadConfig: setup config: %v(%v)", configMasterAddr, strings.Join(masters, ","))
	l.masters = masters
	l.mc = master.NewMasterClient(masters, false)

	l.scanners = make(map[string]*TaskScanner, 0)

	batchExpirationGetNum = int(cfg.GetInt(batchExpirationGetNumStr))
	if batchExpirationGetNum == 0 {
		batchExpirationGetNum = defaultBatchExpirationGetNum
	}

	scanCheckInterval = cfg.GetInt64(scanCheckIntervalStr)
	if scanCheckInterval == 0 {
		scanCheckInterval = defaultScanCheckInterval
	}
	return
}

// register the lcnode on the master to report the information such as IsIPV4 address.
// The startup of a lcnode will be blocked until the registration succeeds.
func (l *LcNode) register(cfg *config.Config) {
	var (
		err error
	)

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
			localIP := string(ci.Ip)
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
			//exporter.RegistConsul(l.clusterID, ModuleName, cfg)
			l.nodeID = nodeID
			log.LogInfof("register: register LcNode: nodeID(%v)", l.nodeID)
			return
		case <-l.stopC:
			timer.Stop()
			return
		}
	}
}

func (l *LcNode) checkRegister(cfg *config.Config) {
	for {
		if time.Since(l.lastHeartbeat) > time.Second*time.Duration(defaultLcNodeTimeOutSec) {
			l.StopScanners()
			log.LogWarnf("Lcnode might be deregistered from master, retry registering...")
			l.register(cfg)
			l.lastHeartbeat = time.Now()
		}
		time.Sleep(time.Second * defaultIntervalToCheckRegister)
	}
}

func handleStart(s common.Server, cfg *config.Config) (err error) {
	l, ok := s.(*LcNode)
	if !ok {
		return errors.New("Invalid node Type!")
	}
	l.stopC = make(chan bool, 0)
	// parse config
	if err = l.loadConfig(cfg); err != nil {
		return
	}
	l.register(cfg)
	l.lastHeartbeat = time.Now()

	go l.checkRegister(cfg)
	if err = l.startServer(); err != nil {
		return
	}

	exporter.Init(ModuleName, cfg)
	exporter.RegistConsul(l.clusterID, ModuleName, cfg)

	log.LogInfo("lcnode start successfully")
	return
}

func handleShutdown(s common.Server) {
	l, ok := s.(*LcNode)
	if !ok {
		return
	}
	//close(l.stopC)
	l.stopServer()
}
