package convertnode

import (
	"container/list"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"sync/atomic"
	"time"
)

type OPType		byte

const (
	ProcessorOPAdd  OPType= iota
	ProcessorOPDel
)

type ProcessOp	struct {
	optype		OPType
	task 		*ConvertTask
}

type ProcessorInfo struct {
	sync.RWMutex
	info  		*proto.ConvertProcessorInfo
	stopC       chan bool
	runTimer    *time.Timer
	runningList *list.List
	opList      *list.List
	node        *ConvertNode
}

func NewProcessor(id uint32, node *ConvertNode) *ProcessorInfo {
	processor := new(ProcessorInfo)
	info := new(proto.ConvertProcessorInfo)
	info.Id = id
	info.State = proto.ProcessorStopped

	processor.info = info
	processor.runTimer = time.NewTimer(time.Minute * defaultProcessorInterval)
	processor.runTimer.Stop()
	processor.node = node
	processor.runningList = list.New()
	processor.opList = list.New()

	return processor
}

func (processor *ProcessorInfo) init() (err error){
	return
}

func (processor *ProcessorInfo) start() (err error){
	if atomic.CompareAndSwapUint32(&processor.info.State, proto.ProcessorStopped, proto.ProcessorStarting) {
		processor.info.State = proto.ProcessorRunning
		processor.stopC = make(chan bool)
		//set task state to init, range running list and op list
		go processor.run()
	}
	return
}

func (processor *ProcessorInfo) stop() (err error){
	if atomic.CompareAndSwapUint32(&processor.info.State, proto.ProcessorRunning, proto.ProcessorStopping) {
		log.LogInfof("stop processor[%d]", processor.info.Id)
		close(processor.stopC)
	}
	return
}

func (processor *ProcessorInfo) dealOpList() {
	processor.Lock()
	defer processor.Unlock()
	for elem := processor.opList.Front(); elem != nil; elem = elem.Next() {
		op := elem.Value.(*ProcessOp)
		switch op.optype {
		case ProcessorOPAdd:
			log.LogInfof("add task [%s] to processor %v", op.task.taskName(), processor.info.Id)
			op.task.elemP = processor.runningList.PushBack(op.task)
		case ProcessorOPDel:
			log.LogInfof("remove task [%s] from processor %v", op.task.taskName(), processor.info.Id)
			processor.runningList.Remove(op.task.elemP)
			atomic.StoreInt32(&(op.task.info.ProcessorID), -1)
		default:
			panic(fmt.Sprintf("op[%v] has unknown optype[%v]", op, op.optype))
		}
	}

	processor.opList = list.New()

	return
}

func (processor *ProcessorInfo) ResetTaskState() {

}

func (processor *ProcessorInfo) delTask(task *ConvertTask) {
	processor.Lock()
	defer processor.Unlock()
	//add list wait next run to del
	log.LogInfof("processor[%d] del task[%s]", processor.info.Id, task.taskName())
	op := &ProcessOp{optype:ProcessorOPDel, task:task}
	processor.opList.PushBack(op)
	return
}

func (processor *ProcessorInfo) addTask(task *ConvertTask) {
	processor.Lock()
	defer processor.Unlock()
	//add list wait next run to add
	log.LogInfof("processor[%d] add task[%s]", processor.info.Id, task.taskName())
	op := &ProcessOp{optype:ProcessorOPAdd, task:task}
	processor.opList.PushBack(op)
	return
}

func (processor *ProcessorInfo) getTaskCount() uint32 {
	processor.RLock()
	defer processor.RUnlock()
	return uint32(processor.runningList.Len())
}

func (processor *ProcessorInfo) run() {
	defer func() {
		if atomic.CompareAndSwapUint32(&processor.info.State, proto.ProcessorStopping, proto.ProcessorStopped) {
			processor.runTimer.Stop()
			log.LogInfof("processor[%d] stop finished", processor.info.Id)
		}
	}()
	processor.runTimer.Reset(time.Second * 30)
	for ;; {
		select {
		case <-processor.node.stopC:
			processor.stop()
			return
		case <-processor.stopC:
			return
		case <-processor.runTimer.C:
			start := time.Now()
			interval := time.Second
			processor.dealOpList()
			for elem := processor.runningList.Front(); elem != nil; elem = elem.Next() {
				task := elem.Value.(*ConvertTask)
				taskFinished, err := task.runOnce()
				if err = task.dealActionErr(err); err != nil {
					log.LogInfof("action[processor run] remove task[%s] from processor[%v]", task.taskName(), processor.info.Id)
					processor.delTask(task)
				}
				if taskFinished {
					log.LogInfof("action[processor run] delete task[%s] from processor[%v]", task.taskName(), processor.info.Id)
					processor.node.delTask(task.info.ClusterName, task.info.VolName)
				}
			}
			cost := time.Since(start)
			if cost < time.Second * 3 {
				interval = time.Second * 3 - cost
			}
			processor.runTimer.Reset(interval)
		}
	}
}

func (processor *ProcessorInfo) GenProcessorDetailView() (view *proto.ConvertProcessorDetailInfo){
	processor.RLock()
	defer processor.RUnlock()

	view = &proto.ConvertProcessorDetailInfo{}
	view.Processor = processor.info
	view.Processor.Count = processor.getTaskCount()
	view.Tasks = make([]*proto.ConvertTaskInfo, 0)

	for elem := processor.runningList.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(*ConvertTask)
		view.Tasks = append(view.Tasks, task.genTaskViewInfo())
	}
	return
}
