package compact

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/schedulenode/worker"
	"github.com/chubaofs/chubaofs/util/config"
)

type CompactWorker struct {
	worker.BaseWorker

}

func NewCompactWorker() *CompactWorker {
	return &CompactWorker{}
}

func (sv *CompactWorker) CreateTask(cluster string, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	return
}

func (sv *CompactWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	return
}

func (sv *CompactWorker) GetCreatorDuration() int {
	return 0
}

func (sv *CompactWorker) Start(cfg *config.Config) (err error) {

	return
}

func (sv *CompactWorker) Shutdown() {

	return
}

func (sv *CompactWorker) Sync() {

	return
}
