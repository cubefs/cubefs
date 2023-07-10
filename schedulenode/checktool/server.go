package checktool

import (
	"errors"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs"
	"github.com/cubefs/cubefs/schedulenode/checktool/img"
	"github.com/cubefs/cubefs/schedulenode/checktool/jfs"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"sync"
)

type ChecktoolWorker struct {
	worker.BaseWorker
	vsm  *img.VolStoreMonitor
	cfsm *cfs.ChubaoFSMonitor
	jfsm *jfs.JFSMonitor
	wg   sync.WaitGroup
}

func NewChecktoolWorker() *ChecktoolWorker {
	return &ChecktoolWorker{}
}

func (cw *ChecktoolWorker) Start(cfg *config.Config) error {
	return cw.Control.Start(cw, cfg, doStart)
}

func (cw *ChecktoolWorker) Shutdown() {
	cw.Control.Shutdown(cw, doShutdown)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	cw, ok := s.(*ChecktoolWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	cw.StopC = make(chan struct{}, 0)
	cw.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	cw.cfsm = cfs.NewChubaoFSMonitor()
	if err = cw.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	cw.vsm = img.NewVolStoreMonitor()
	if err = cw.vsm.Start(cfg); err != nil {
		return
	}
	cw.cfsm = cfs.NewChubaoFSMonitor()
	if err = cw.cfsm.Start(cfg); err != nil {
		return
	}
	cw.jfsm = jfs.NewJFSMonitor()
	if err = cw.jfsm.Start(cfg); err != nil {
		return
	}
	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*ChecktoolWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (cw *ChecktoolWorker) parseConfig(cfg *config.Config) (err error) {
	err = cw.ParseBaseConfig(cfg)
	if err != nil {
		return
	}
	return
}
