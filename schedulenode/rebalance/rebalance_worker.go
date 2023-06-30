package rebalance

import (
	"encoding/json"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm"
	"net/http"
	"sync"
)

type ReBalanceWorker struct {
	worker.BaseWorker
	masterAddr       map[string][]string
	mcw              map[string]*master.MasterClient
	mcwRWMutex       sync.RWMutex
	reBalanceCtrlMap sync.Map
	dbHandle         *gorm.DB
}

func NewReBalanceWorker() *ReBalanceWorker {
	return &ReBalanceWorker{}
}

func (rw *ReBalanceWorker) Start(cfg *config.Config) (err error) {
	return rw.Control.Start(rw, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	rw, ok := s.(*ReBalanceWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	rw.StopC = make(chan struct{}, 0)
	rw.mcw = make(map[string]*master.MasterClient)
	rw.reBalanceCtrlMap = sync.Map{}
	if err = rw.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	masterClient := make(map[string]*master.MasterClient)
	for cluster, addresses := range rw.masterAddr {
		mc := master.NewMasterClient(addresses, false)
		masterClient[cluster] = mc
	}

	err = rw.OpenSql()
	if err != nil {
		return err
	}
	rw.registerHandler()
	return nil
}

func (rw *ReBalanceWorker) Shutdown() {
	rw.Control.Shutdown(rw, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*ReBalanceWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (rw *ReBalanceWorker) Sync() {
	rw.Control.Sync()
}

func (rw *ReBalanceWorker) parseConfig(cfg *config.Config) (err error) {
	err = rw.ParseBaseConfig(cfg)
	if err != nil {
		return
	}
	// parse cluster master address
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	var masterAddr []string
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		if len(masterAddr) == 0 {
			masterAddr = addresses
		}
		masters[clusterName] = addresses
	}
	rw.masterAddr = masters
	// used for cmd to report version
	if len(masterAddr) == 0 {
		cfg.SetStringSlice(proto.MasterAddr, masterAddr)
	}
	return
}

func (rw *ReBalanceWorker) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("rebalance")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc(RBStart, responseHandler(rw.handleStart))
	http.HandleFunc(RBStop, responseHandler(rw.handleStop))
	http.HandleFunc(RBStatus, responseHandler(rw.handleStatus))
	http.HandleFunc(RBReset, responseHandler(rw.handleReset))
}
