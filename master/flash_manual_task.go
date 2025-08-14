package master

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

const (
	minRequiredTTLSeconds       = 30 * 60 // 30 minute
	FlashTaskInteractiveTimeout = time.Minute * time.Duration(5)
	ScanManualInterval          = time.Minute * time.Duration(2)
	CheckInteractiveInterval    = time.Minute * time.Duration(2)
)

type flashManualTaskManager struct {
	mu                  sync.RWMutex
	cluster             *Cluster
	flashManualTasks    sync.Map //[string]*proto.FlashManualTask
	exitCh              chan struct{}
	flashNodeTaskStatus *flashNodeTaskStatus
	taskTotalLimit      int
}

type flashNodeTaskStatus struct {
	mu           sync.RWMutex
	WorkingCount map[string]int //ip:count, number of tasks being processed on this node
}

func newFlashManualTaskManager(c *Cluster) *flashManualTaskManager {
	log.LogInfof("action[newFlashManualTaskManager] construct")
	fltMgr := &flashManualTaskManager{
		exitCh: make(chan struct{}),
		flashNodeTaskStatus: &flashNodeTaskStatus{
			WorkingCount: make(map[string]int),
		},
		cluster:        c,
		taskTotalLimit: 8,
	}
	return fltMgr
}

func checkManualConfig(flt *proto.FlashManualTask, vol *Vol, fltMgr *flashManualTaskManager) error {
	var err error
	switch flt.Action {
	case proto.FlashManualWarmupAction:
		if vol.remoteCacheTTL < minRequiredTTLSeconds && vol.remoteCacheTTL != 0 {
			return fmt.Errorf("cache ttl %v of vol[%v] is too short and warm up can not work", vol.remoteCacheTTL, vol.Name)
		}
		if err = isDuplicated(flt, fltMgr); err != nil {
			return err
		}
	case proto.FlashManualCheckAction:

	case proto.FlashManualClearAction:

	default:
		return fmt.Errorf("invalid flash manual action: %v", flt.Action)
	}
	return nil
}

func isDuplicated(flt *proto.FlashManualTask, fltMgr *flashManualTaskManager) (err error) {
	if _, exists := fltMgr.flashManualTasks.Load(flt.Id); exists {
		return fmt.Errorf("flash manual task[%v] already exists", flt.Id)
	}
	volName := flt.VolName
	rootDir := flt.GetPathPrefix()

	var tmpDir string
	fltMgr.flashManualTasks.Range(func(_, v interface{}) bool {
		t := v.(*proto.FlashManualTask)
		if t.VolName != volName {
			return true
		}
		tmpDir = t.GetPathPrefix()
		if rootDir != tmpDir {
			return true
		}
		if !proto.ManualTaskDone(t.Status) {
			err = fmt.Errorf("manual task[%v] is running with the same directory", t.Id)
			return false
		}
		return true
	})
	return
}

func (fltMgr *flashManualTaskManager) SetFlashManualTask(flt *proto.FlashManualTask) error {
	flt.Lock()
	defer flt.Unlock()
	if proto.ManualTaskIsRunning(flt.Status) {
		currentTime := time.Now()
		flt.UpdateTime = &currentTime
	}
	fltMgr.flashManualTasks.Store(flt.Id, flt)
	return nil
}

func (fltMgr *flashManualTaskManager) startFlashScanHandleLeaderChange() {
	go func() {
		log.LogInfof("startFlashScanHandleLeaderChange start, wait 1 min ")
		go fltMgr.process()
		go fltMgr.checkInteractiveTimeoutTask()
	}()
}

func (fltMgr *flashManualTaskManager) process() {
	log.LogInfo("flashManualTaskManager process start")
	var err error
	ticker := time.NewTicker(ScanManualInterval)
	defer func() {
		defer ticker.Stop()
		log.LogInfo("flashManualTaskManager process stop")
	}()
	for {
		select {
		case <-fltMgr.exitCh:
			log.LogInfo("exitCh notified, flashManualTaskManager process exit")
			return
		case <-ticker.C:
			log.LogInfof("process flash manual task scan")
			task := fltMgr.getOneTodoTask()
			if task == nil {
				ticker.Reset(time.Minute * 2)
				log.LogInfof("getOneTodoTask, no task")
				continue
			}
			if fltMgr.getTotalWorkCount() >= fltMgr.taskTotalLimit {
				log.LogInfof("current working tasks equal gather than %v", fltMgr.taskTotalLimit)
				ticker.Reset(time.Minute)
				continue
			}
			lowLoadNode := fltMgr.findLowLoadNode()
			if lowLoadNode == "" {
				log.LogWarnf("insufficient flash node resources")
				continue
			}
			val, ok := fltMgr.cluster.flashNodeTopo.flashNodeMap.Load(lowLoadNode)
			if !ok {
				log.LogErrorf("process(%v), flashNode.Load, nodeAddr is not available, redo task", lowLoadNode)
				continue
			}
			task.Status = int(proto.Flash_Task_Running)
			task.ManualTaskStatistics = &proto.ManualTaskStatistics{
				FlashNode: lowLoadNode,
			}
			err = fltMgr.cluster.syncAddFlashManualTask(task)
			if err != nil {
				log.LogErrorf("sync task[%v] to status[%v] error %v", task.Id, task.Status, err)
				continue
			}
			_ = fltMgr.SetFlashManualTask(task)
			node := val.(*FlashNode)

			adminTask := node.createFnScanTask(fltMgr.cluster.masterAddr(), task)
			node.TaskManager.AddTask(adminTask)
			ticker.Reset(time.Minute)
			log.LogInfof("add flashnode manual scan task(%v) to flashnode(%v)", adminTask.ToString(), lowLoadNode)
		}
	}
}

func (fltMgr *flashManualTaskManager) getTotalWorkCount() int {
	fltMgr.flashNodeTaskStatus.mu.Lock()
	defer fltMgr.flashNodeTaskStatus.mu.Unlock()
	var cnt int
	for _, v := range fltMgr.flashNodeTaskStatus.WorkingCount {
		cnt = cnt + v
	}
	return cnt
}

func (fltMgr *flashManualTaskManager) findLowLoadNode() string {
	var (
		nodeAddr  string
		scanNodes []string
		allNodes  = make(map[string]int)
	)
	fltMgr.cluster.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		allNodes[node.Addr] = node.TaskCountLimit
		if node.WorkRole == proto.FlashNodeTaskWorker {
			scanNodes = append(scanNodes, node.Addr)
		}
		return true
	})
	if len(scanNodes) == 0 {
		for addr := range allNodes {
			scanNodes = append(scanNodes, addr)
		}
	}
	fltMgr.flashNodeTaskStatus.mu.Lock()
	defer fltMgr.flashNodeTaskStatus.mu.Unlock()
	min := math.MaxInt
	var workingCount, workerLimitCount int
	for _, addr := range scanNodes {
		workingCount = fltMgr.flashNodeTaskStatus.WorkingCount[addr]
		if workingCount == 0 {
			nodeAddr = addr
			break
		}
		workerLimitCount = allNodes[addr]
		if workingCount+1 > workerLimitCount {
			continue
		}
		if workingCount < min {
			nodeAddr = addr
			min = workingCount
		}
	}
	if nodeAddr != "" {
		fltMgr.flashNodeTaskStatus.WorkingCount[nodeAddr]++
	}
	return nodeAddr
}

func (fltMgr *flashManualTaskManager) checkInteractiveTimeoutTask() {
	log.LogInfo("flashManualTaskManager checkInteractiveTimeoutTask start")
	ticker := time.NewTicker(CheckInteractiveInterval)
	defer func() {
		defer ticker.Stop()
		log.LogInfo("flashManualTaskManager checkInteractiveTimeoutTask stop")
	}()
	var (
		err         error
		runningTask bool
	)
	for {
		select {
		case <-fltMgr.exitCh:
			log.LogInfo("exitCh notified, checkInteractiveTimeoutTask exit")
			return
		case <-ticker.C:
			log.LogInfof("check if the task scan has an interactive timeout")
			now := time.Now()
			runningTask = false
			fltMgr.flashManualTasks.Range(func(_, value interface{}) bool {
				t := value.(*proto.FlashManualTask)
				t.Lock()
				defer t.Unlock()
				if !proto.ManualTaskIsRunning(t.Status) {
					return true
				}
				runningTask = true
				if now.After(t.UpdateTime.Add(FlashTaskInteractiveTimeout)) {
					err = fltMgr.stopInteractiveTask(t)
					if err != nil {
						log.LogErrorf("flashManualTaskManager stopInteractiveTask err: %v", err)
					}
				}
				return true
			})
			if runningTask {
				ticker.Reset(time.Minute)
			} else {
				ticker.Reset(5 * time.Minute)
			}
		}
	}
}

func (fltMgr *flashManualTaskManager) dispatchTaskOp(tid string, opCode string) error {
	if tid == "" {
		return fmt.Errorf("req param miss tid info")
	}
	var (
		task *proto.FlashManualTask
		ok   bool
	)
	task, ok = fltMgr.LoadManualTaskById(tid)
	if !ok {
		return fmt.Errorf("no record found for tid[%v]", tid)
	}
	task.Lock()
	if task.ManualTaskStatistics == nil || task.ManualTaskStatistics.FlashNode == "" {
		task.Unlock()
		return fmt.Errorf("tid[%v] is not currently running in flashnode worker", tid)
	}
	if proto.ManualTaskDone(task.Status) {
		task.Unlock()
		return fmt.Errorf("task[%v] has already ended. No further actions can be performed", tid)
	}
	task.Unlock()
	val, ok := fltMgr.cluster.flashNodeTopo.flashNodeMap.Load(task.ManualTaskStatistics.FlashNode)
	if !ok {
		return fmt.Errorf("nodeAddr[%v] is not available", task.ManualTaskStatistics.FlashNode)
	}
	command := &proto.FlashNodeManualTaskCommand{
		ID:      task.Id,
		Command: opCode,
	}
	t := proto.NewAdminTask(proto.OpFlashNodeTaskCommand, task.ManualTaskStatistics.FlashNode, command)
	node := val.(*FlashNode)
	_, err := node.TaskManager.syncSendAdminTask(t)
	return err
}

func (fltMgr *flashManualTaskManager) stopInteractiveTask(task *proto.FlashManualTask) error {
	// send stop signal
	var err error
	task.Status = int(proto.Flash_Task_Failed)
	if err = fltMgr.cluster.syncAddFlashManualTask(task); err != nil {
		return err
	}
	return nil
}

func (fltMgr *flashManualTaskManager) getOneTodoTask() (mt *proto.FlashManualTask) {
	fltMgr.flashManualTasks.Range(func(_, v interface{}) bool {
		t := v.(*proto.FlashManualTask)
		if t.Status == int(proto.Flash_Task_Init) {
			if mt == nil {
				mt = t
				return true
			}
			if mt.StartTime.After(*t.StartTime) {
				mt = t
			}
		}
		return true
	})
	return
}

func (fltMgr *flashManualTaskManager) findMatchTasks(vol string, tid string) []*proto.FlashManualTask {
	var rets []*proto.FlashManualTask
	fltMgr.flashManualTasks.Range(func(id, v interface{}) bool {
		t := v.(*proto.FlashManualTask)
		if tid != "" && id != tid {
			return true
		}
		if vol != "" && t.VolName != vol {
			return true
		}
		rets = append(rets, t)
		return true
	})
	return rets
}

func (c *Cluster) handleFlashNodeScanResp(nodeAddr string, resp *proto.FlashNodeManualTaskResponse) (err error) {
	log.LogDebugf("action[handleFlashNodeScanResp] flashNode[%v] task[%v] Enter", nodeAddr, resp.ID)
	defer func() {
		log.LogDebugf("action[handleFlashNodeScanResp] flashNode[%v] task[%v] Exit", nodeAddr, resp.ID)
	}()
	var (
		manualTask *proto.FlashManualTask
		ok         bool
	)
	switch resp.Status {
	case proto.TaskFailed:
		log.LogWarnf("action[handleFlashNodeScanResp] scanning failed, resp(%v), no redo", resp)
		if manualTask, ok = c.flashManMgr.LoadManualTaskById(resp.ID); !ok {
			log.LogWarnf("action[handleFlashNodeScanResp] LoadManualTaskById %v not exsit", resp.ID)
			return
		}
		manualTask.Lock()
		manualTask.SetResponse(resp)
		manualTask.EndTime = resp.EndTime
		manualTask.Status = int(proto.Flash_Task_Failed)
		manualTask.Unlock()
		if e := c.syncAddFlashManualTask(manualTask); e != nil {
			log.LogWarnf("action[handleFlashNodeScanResp] syncAddFlashManualTask %v err(%v)", resp, e)
		}
		msg := fmt.Sprintf("scanning failed: %+v", resp)
		auditlog.LogMasterOp("handleFlashNodeScanResp", msg, nil)
		return
	case proto.TaskSucceeds, proto.TaskStop:
		log.LogInfof("action[handleFlashNodeScanResp] scanning completed, resp(%v)", resp)
		if manualTask, ok = c.flashManMgr.LoadManualTaskById(resp.ID); !ok {
			log.LogWarnf("action[handleFlashNodeScanResp] LoadManualTaskById %v not exsit", resp.ID)
			return
		}
		manualTask.Lock()
		manualTask.SetResponse(resp)
		manualTask.EndTime = resp.EndTime
		manualTask.Status = int(proto.Flash_Task_Success)
		if resp.Status == proto.TaskStop {
			manualTask.Status = int(proto.Flash_Task_Stop)
		}
		manualTask.Unlock()

		if e := c.syncAddFlashManualTask(manualTask); e != nil {
			log.LogWarnf("action[handleFlashNodeScanResp] syncAddLcResult %v err(%v)", resp, e)
		}
		msg := fmt.Sprintf("scanning completed: %+v", resp)
		auditlog.LogMasterOp("handleFlashNodeScanResp", msg, nil)
		return
	default:
		msg := fmt.Sprintf("scanning received, resp(%+v)", resp)
		auditlog.LogMasterOp("handleFlashNodeScanResp", msg, nil)
	}
	return
}

func (fltMgr *flashManualTaskManager) LoadManualTaskById(id string) (t *proto.FlashManualTask, ok bool) {
	var tmp interface{}
	tmp, ok = fltMgr.flashManualTasks.Load(id)
	if !ok || tmp == nil {
		return nil, false
	}
	t = tmp.(*proto.FlashManualTask)
	return
}
