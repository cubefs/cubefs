package flashgroupmanager

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/cubefs/cubefs/util/log"
)

type Cluster struct {
	Name          string
	CreateTime    int64
	flashNodeTopo *FlashNodeTopology
	idAlloc       *IDAllocator
	stopc         chan bool
	stopFlag      int32
	wg            sync.WaitGroup //run task?
	cfg           *clusterConfig
}

func newCluster(name string, cfg *clusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.flashNodeTopo = NewFlashNodeTopology()
	c.stopc = make(chan bool)
	c.idAlloc = newIDAllocator()
	c.cfg = cfg
	return
}

func (c *Cluster) createFlashGroup(setSlots []uint32, setWeight uint32, gradualFlag bool, step uint32) (fg *FlashGroup, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addFlashGroup],clusterID[%v] err:%v ", c.Name, err.Error())
		}
	}()
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	if gradualFlag {
		if fg, err = c.flashNodeTopo.gradualCreateFlashGroup(id, c, setSlots, setWeight, step); err != nil {
			return
		}
	} else {
		if fg, err = c.flashNodeTopo.createFlashGroup(id, c, setSlots, setWeight); err != nil {
			return
		}
	}

	c.flashNodeTopo.updateClientCache()
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Weight:%v Slots:%v success", c.Name, fg.ID, fg.Weight, fg.getSlots())
	return
}

func (c *Cluster) removeFlashGroup(flashGroup *FlashGroup, gradualFlag bool, step uint32) (err error) {
	remainingSlotsNum := uint32(flashGroup.getSlotsCount()) - step
	if gradualFlag && remainingSlotsNum > 0 {
		err = c.flashNodeTopo.gradualRemoveFlashGroup(flashGroup, c, step)
		return
	}

	// remove flash nodes then del the flash group
	err = c.removeAllFlashNodeFromFlashGroup(flashGroup)
	if err != nil {
		return
	}
	err = c.flashNodeTopo.removeFlashGroup(flashGroup, c)
	return
}

func (c *Cluster) removeAllFlashNodeFromFlashGroup(flashGroup *FlashGroup) (err error) {
	flashNodeHosts := flashGroup.getFlashNodeHosts(false)
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = c.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup); err != nil {
			log.LogErrorf("remove flashNode from flashGroup failed, successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
	}
	log.LogInfof("action[RemoveAllFlashNodeFromFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost)
	return
}

func (c *Cluster) removeFlashNodeFromFlashGroup(addr string, flashGroup *FlashGroup) (err error) {
	var flashNode *FlashNode
	if flashNode, err = c.setFlashNodeToUnused(addr, flashGroup.ID); err != nil {
		return
	}
	flashGroup.removeFlashNode(flashNode.Addr)
	log.LogInfo(fmt.Sprintf("action[removeFlashNodeFromFlashGroup] node:%v flashGroup:%v, success", flashNode.Addr, flashGroup.ID))
	return
}

func (c *Cluster) removeFlashNodesFromTargetZone(zoneName string, count int, flashGroup *FlashGroup) (err error) {
	flashNodeHosts := flashGroup.getTargetZoneFlashNodeHosts(zoneName)
	if len(flashNodeHosts) < count {
		return fmt.Errorf("flashNodeHostsCount:%v less than expectCount:%v,flashNodeHosts:%v", len(flashNodeHosts), count, flashNodeHosts)
	}
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = c.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup); err != nil {
			err = fmt.Errorf("successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
		if len(successHost) >= count {
			break
		}
	}
	log.LogInfo(fmt.Sprintf("action[removeFlashNodesFromTargetZone] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	return
}

func (c *Cluster) setFlashNodeToUnused(addr string, flashGroupID uint64) (flashNode *FlashNode, err error) {
	if flashNode, err = c.peekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != flashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] not equal to target flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}

	// oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = UnusedFlashNodeFlashGroupID
	// TODO
	//if err = c.syncUpdateFlashNode(flashNode); err != nil {
	//	flashNode.FlashGroupID = oldFgID
	//	return
	//}

	go func() {
		time.Sleep(time.Duration(DefaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr = fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()

	return
}

func (c *Cluster) addFlashNode(nodeAddr, zoneName, version string) (id uint64, err error) {
	c.flashNodeTopo.mu.Lock()
	defer func() {
		c.flashNodeTopo.mu.Unlock()
		if err != nil {
			log.LogErrorf("action[addFlashNode],clusterID[%v] Addr:%v err:%v ", c.Name, nodeAddr, err.Error())
		}
	}()

	var flashNode *FlashNode
	flashNode, err = c.peekFlashNode(nodeAddr)
	if err == nil {
		return flashNode.ID, nil
	}
	flashNode = NewFlashNode(nodeAddr, zoneName, c.Name, version, true)
	_, err = c.flashNodeTopo.getZone(zoneName)
	if err != nil {
		c.flashNodeTopo.putZoneIfAbsent(NewFlashNodeZone(zoneName))
	}
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		return
	}
	flashNode.ID = id
	//TODO
	//if err = c.syncAddFlashNode(flashNode); err != nil {
	//	return
	//}
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	if err = c.flashNodeTopo.putFlashNode(flashNode); err != nil {
		return
	}
	log.LogInfof("action[addFlashNode],clusterID[%v] Addr:%v ZoneName:%v success", c.Name, nodeAddr, zoneName)
	return
}

func (c *Cluster) updateFlashNode(flashNode *FlashNode, enable bool) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.IsEnable != enable {
		//oldState := flashNode.IsEnable
		flashNode.IsEnable = enable
		// TODO
		//if err = c.syncUpdateFlashNode(flashNode); err != nil {
		//	flashNode.IsEnable = oldState
		//	return
		//}
		if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
			c.flashNodeTopo.updateClientCache()
		}
	}
	return
}

func (c *Cluster) updateFlashNodeWorkRole(flashNode *FlashNode, workRole string) error {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.WorkRole = workRole
	// TODO
	//if err := c.syncUpdateFlashNode(flashNode); err != nil {
	//	return err
	//}
	return nil
}

func (c *Cluster) removeFlashNode(flashNode *FlashNode) (err error) {
	log.LogWarnf("action[removeFlashNode], ZoneName[%s] Node[%s] offline", flashNode.ZoneName, flashNode.Addr)
	var flashGroupID uint64
	if flashGroupID, err = c.deleteFlashNode(flashNode); err != nil {
		return
	}
	if flashGroupID != UnusedFlashNodeFlashGroupID {
		var flashGroup *FlashGroup
		if flashGroup, err = c.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
			return
		}
		flashGroup.removeFlashNode(flashNode.Addr)
		c.flashNodeTopo.updateClientCache()
	}

	go func() {
		time.Sleep(time.Duration(DefaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(flashNode.Addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr := fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()

	log.LogInfof("action[removeFlashNode], clusterID[%s] node[%s] flashGroupID[%d] offline success",
		c.Name, flashNode.Addr, flashGroupID)
	return
}

func (c *Cluster) deleteFlashNode(flashNode *FlashNode) (oldFlashGroupID uint64, err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	oldFlashGroupID = flashNode.FlashGroupID
	flashNode.FlashGroupID = UnusedFlashNodeFlashGroupID
	//TODO
	//if err = c.syncDeleteFlashNode(flashNode); err != nil {
	//	log.LogErrorf("action[deleteFlashNode],clusterID[%v] node[%v] offline failed,err[%v]",
	//		c.Name, flashNode.Addr, err)
	//	flashNode.FlashGroupID = oldFlashGroupID
	//	return
	//}
	c.delFlashNodeFromCache(flashNode)
	return
}

func (c *Cluster) delFlashNodeFromCache(flashNode *FlashNode) {
	c.flashNodeTopo.deleteFlashNode(flashNode)
	go flashNode.clean()
}

func (c *Cluster) addFlashNodeToFlashGroup(addr string, flashGroup *FlashGroup) (err error) {
	var flashNode *FlashNode
	if flashNode, err = c.setFlashNodeToFlashGroup(addr, flashGroup.ID); err != nil {
		return
	}
	flashGroup.putFlashNode(flashNode)
	return
}

func (c *Cluster) setFlashNodeToFlashGroup(addr string, flashGroupID uint64) (flashNode *FlashNode, err error) {
	if flashNode, err = c.peekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] can not add to flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}
	if time.Since(flashNode.ReportTime) > DefaultNodeTimeoutDuration {
		flashNode.IsActive = false
		err = fmt.Errorf("flashNode[%v] is inactive lastReportTime:%v", flashNode.Addr, flashNode.ReportTime)
		return
	}
	//oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = flashGroupID
	// TODO
	//if err = c.syncUpdateFlashNode(flashNode); err != nil {
	//	flashNode.FlashGroupID = oldFgID
	//	return
	//}
	log.LogInfo(fmt.Sprintf("action[setFlashNodeToFlashGroup] add flash node:%v to flashGroup:%v success", addr, flashGroupID))
	return
}

func (c *Cluster) selectFlashNodesFromZoneAddToFlashGroup(zoneName string, count int, excludeHosts []string, flashGroup *FlashGroup) (err error) {
	flashNodeZone, err := c.flashNodeTopo.getZone(zoneName)
	if err != nil {
		return
	}
	newHosts, err := flashNodeZone.selectFlashNodes(count, excludeHosts)
	if err != nil {
		return
	}
	successHost := make([]string, 0)
	for _, newHost := range newHosts {
		if err = c.addFlashNodeToFlashGroup(newHost, flashGroup); err != nil {
			err = fmt.Errorf("successHost:%v, newHosts:%v err:%v", successHost, newHosts, err)
			return
		}
		successHost = append(successHost, newHost)
	}
	log.LogInfo(fmt.Sprintf("action[selectFlashNodesFromZoneAddToFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	return
}

func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		dur := time.Second * time.Duration(5)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for {
			// TODO
			//if c.partition != nil && c.partition.IsRaftLeader() {
			//	c.flashNodeTopo.updateClientResponse()
			//}
			select {
			case <-c.stopc:
				return
			case <-c.flashNodeTopo.clientUpdateCh:
				ticker.Reset(dur)
			case <-ticker.C:
			}
		}
	}()
}

func (c *Cluster) scheduleTask() {
	c.scheduleToUpdateFlashGroupRespCache()
	c.scheduleToCheckHeartbeat()
}

func (c *Cluster) peekFlashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := c.flashNodeTopo.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(notFoundMsg(fmt.Sprintf("flashnode[%v]", addr)), "")
		return
	}
	flashNode = value.(*FlashNode)
	return
}

func (c *Cluster) handleFlashNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("flash action[handleFlashNodeTaskResponse] receive addr[%v] task response, but task is nil", nodeAddr)
		return
	}
	log.LogInfof("flash action[handleFlashNodeTaskResponse] receive addr[%v] task: %v", nodeAddr, task.ToString())
	var (
		err       error
		flashNode *FlashNode
	)

	if flashNode, err = c.peekFlashNode(nodeAddr); err != nil {
		goto errHandler
	}
	flashNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	//TODO
	//case proto.OpFlashNodeScan:
	//	response := task.Response.(*proto.FlashNodeManualTaskResponse)
	//	err = c.handleFlashNodeScanResp(task.OperatorAddr, response)
	case proto.OpFlashNodeHeartbeat:
		response := task.Response.(*proto.FlashNodeHeartbeatResponse)
		err = c.handleFlashNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("flash unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogWarnf("flash handleFlashNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
}

func (c *Cluster) handleFlashNodeHeartbeatResp(nodeAddr string, resp *proto.FlashNodeHeartbeatResponse) (err error) {
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleFlashNodeHeartbeatResp] clusterID[%v] flashNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}
	var node *FlashNode
	if node, err = c.peekFlashNode(nodeAddr); err != nil {
		log.LogErrorf("action[handleFlashNodeHeartbeatResp], flashNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	node.setActive()
	node.updateFlashNodeStatHeartbeat(resp)
	//TODO: preload
	// c.handleManualTaskProcessing(node, resp)
	return
}

func (c *Cluster) checkFlashNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		task := node.createHeartbeatTask(c.masterAddr(), c.cfg.flashNodeHandleReadTimeout, c.cfg.flashNodeReadDataNodeTimeout)
		tasks = append(tasks, task)
		return true
	})
	c.addFlashNodeHeartbeatTasks(tasks)
}

func (c *Cluster) addFlashNodeHeartbeatTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.TaskManager.AddTask(t)
	}
}

func (c *Cluster) scheduleToCheckHeartbeat() {
	c.runTask(
		&cTask{
			tickTime: time.Second * defaultIntervalToCheckHeartbeat,
			name:     "scheduleToCheckHeartbeat_checkFlashNodeHeartbeat",
			function: func() (fin bool) {
				//TODO
				//if c.partition != nil && c.partition.IsRaftLeader() {
				//	c.checkLcNodeHeartbeat()
				//}
				c.checkFlashNodeHeartbeat()
				return
			},
		})
}

type cTask struct {
	name     string
	tickTime time.Duration
	function func() bool
	noWait   bool
}

func (c *Cluster) runTask(task *cTask) {
	if !task.noWait {
		c.wg.Add(1)
	}
	go func() {
		if !task.noWait {
			defer c.wg.Done()
		}
		log.LogWarnf("runTask %v start!", task.name)
		currTickTm := task.tickTime
		ticker := time.NewTicker(currTickTm)
		for {
			select {
			case <-ticker.C:
				if task.function() {
					log.LogWarnf("runTask %v exit!", task.name)
					ticker.Stop()
					return
				}
				if currTickTm != task.tickTime { // there's no conflict, thus no need consider consistency between tickTime and currTickTm
					ticker.Reset(task.tickTime)
					currTickTm = task.tickTime
				}
			case <-c.stopc:
				log.LogWarnf("runTask %v exit!", task.name)
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cluster) masterAddr() (addr string) {
	//TODO
	//return c.leaderInfo.addr
	return "tmp"
}
