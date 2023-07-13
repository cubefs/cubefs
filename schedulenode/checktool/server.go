package checktool

import (
	"context"
	"errors"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"sync"
)

type ChecktoolWorker struct {
	worker.BaseWorker
	cfsm   *cfs.ChubaoFSMonitor
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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
	if err = cw.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	cw.ctx, cw.cancel = context.WithCancel(context.Background())
	cw.cfsm = cfs.NewChubaoFSMonitor(cw.ctx)
	if err = cw.cfsm.Start(cfg); err != nil {
		return
	}
	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*ChecktoolWorker)
	if !ok {
		return
	}
	m.cancel()
	close(m.StopC)
}

func (cw *ChecktoolWorker) parseConfig(cfg *config.Config) (err error) {
	err = cw.ParseBaseConfig(cfg)
	if err != nil {
		return
	}
	return
}
