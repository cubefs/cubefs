package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type Cluster struct {
	Name          string
	CreateTime    int64
	flashNodeTopo *FlashNodeTopology
	idAlloc       *IDAllocator
	stopc         chan bool
	stopFlag      int32
	wg            sync.WaitGroup
	cfg           *clusterConfig
	leaderInfo    *LeaderInfo
	fsm           *MetadataFsm
	partition     raftstore.Partition
}

func newCluster(name string, cfg *clusterConfig, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.flashNodeTopo = NewFlashNodeTopology()
	c.flashNodeTopo.SyncFlashGroupFunc = c.syncUpdateFlashGroup
	c.stopc = make(chan bool)
	c.fsm = fsm
	c.partition = partition
	c.cfg = cfg
	c.leaderInfo = leaderInfo
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
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
	fg, err = c.flashNodeTopo.CreateFlashGroup(id, c.syncUpdateFlashGroup, c.syncAddFlashGroup, setSlots, setWeight, gradualFlag, step)
	log.LogInfof("action[addFlashGroup],clusterID[%v] id:%v Weight:%v Slots:%v success", c.Name, fg.ID, fg.Weight, fg.GetSlots())
	return
}

func (c *Cluster) addFlashNode(nodeAddr, zoneName, version string) (id uint64, err error) {
	return c.flashNodeTopo.AddFlashNode(c.Name, nodeAddr, zoneName, version,
		c.idAlloc.allocateCommonID, c.syncAddFlashNode)
}

func (c *Cluster) updateFlashNode(flashNode *FlashNode, enable bool) (err error) {
	return c.flashNodeTopo.UpdateFlashNode(flashNode, enable, c.syncUpdateFlashNode)
}

func (c *Cluster) updateFlashNodeWorkRole(flashNode *FlashNode, workRole string) error {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.WorkRole = workRole
	if err := c.syncUpdateFlashNode(flashNode); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) removeFlashNode(flashNode *FlashNode) (err error) {
	return c.flashNodeTopo.RemoveFlashNode(c.Name, flashNode, c.syncDeleteFlashNode)
}

func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		dur := time.Second * time.Duration(5)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		clientUpdateCh := c.flashNodeTopo.ClientUpdateChannel()
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.flashNodeTopo.UpdateClientResponse()
			}
			select {
			case <-c.stopc:
				return
			case <-clientUpdateCh:
				ticker.Reset(dur)
			case <-ticker.C:
			}
		}
	}()
}

func (c *Cluster) scheduleTask() {
	c.scheduleToUpdateFlashGroupRespCache()
	c.scheduleToCheckHeartbeat()
	c.scheduleToUpdateFlashGroupSlots()
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
	node.SetActive()
	node.UpdateFlashNodeStatHeartbeat(resp)
	return
}

func (c *Cluster) checkFlashNodeHeartbeat() {
	tasks := c.flashNodeTopo.CreateFlashNodeHeartBeatTasks(c.masterAddr(), int(c.cfg.RemoteCacheReadTimeout),
		c.cfg.FlashNodeReadDataNodeTimeout, c.cfg.FlashHotKeyMissCount, c.cfg.FlashReadFlowLimit, c.cfg.FlashWriteFlowLimit, c.cfg.FlashKeyFlowLimit)
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
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkFlashNodeHeartbeat()
				}
				return
			},
		})
}

func (c *Cluster) scheduleToUpdateFlashGroupSlots() {
	c.runTask(
		&cTask{
			tickTime: time.Minute,
			name:     "scheduleToUpdateFlashGroupSlots",
			function: func() (fin bool) {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.flashNodeTopo.UpdateFlashGroupSlots(c.syncDeleteFlashGroup, c.syncUpdateFlashGroup, c.syncUpdateFlashNode)
				}
				return
			},
		})
}

func getNewSlots(slots []uint32, pendingSlots []uint32, flag proto.SlotStatus) (newSlots []uint32) {
	if flag == proto.SlotStatus_Creating { // expand flashGroup slots
		newSlots = append(slots, pendingSlots...)
		sort.Slice(newSlots, func(i, j int) bool { return newSlots[i] < newSlots[j] })
		return
	} else { // shrink flashGroup slots
		slotMap := make(map[uint32]struct{})
		for _, val := range pendingSlots {
			if _, ok := slotMap[val]; !ok {
				slotMap[val] = struct{}{}
			}
		}
		for _, val := range slots {
			if _, ok := slotMap[val]; !ok {
				newSlots = append(newSlots, val)
			}
		}
		return
	}
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
	return c.leaderInfo.addr
}

func (c *Cluster) allMasterNodes() (masterNodes []proto.NodeView) {
	masterNodes = make([]proto.NodeView, 0)

	for _, addr := range c.cfg.peerAddrs {
		split := strings.Split(addr, colonSplit)
		id, _ := strconv.ParseUint(split[0], 10, 64)
		masterNode := proto.NodeView{ID: id, Addr: split[1] + ":" + split[2], Status: true}
		masterNodes = append(masterNodes, masterNode)
	}
	return masterNodes
}

func (c *Cluster) allFlashNodes() (flashNodes []proto.NodeView) {
	return c.flashNodeTopo.GetAllFlashNodes()
}

func (c *Cluster) syncAddFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncAddFlashGroup, flashGroup)
}

func (c *Cluster) syncDeleteFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncDeleteFlashGroup, flashGroup)
}

func (c *Cluster) syncUpdateFlashGroup(flashGroup *FlashGroup) (err error) {
	return c.syncPutFlashGroupInfo(opSyncUpdateFlashGroup, flashGroup)
}

func (c *Cluster) syncPutFlashGroupInfo(opType uint32, flashGroup *FlashGroup) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashGroupPrefix + strconv.FormatUint(flashGroup.ID, 10)
	metadata.V, err = json.Marshal(flashGroup.FlashGroupValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncPutFlashNodeInfo(opType uint32, flashNode *FlashNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashNodePrefix + strconv.FormatUint(flashNode.ID, 10) + keySeparator + flashNode.Addr
	metadata.V, err = json.Marshal(flashNode.FlashNodeValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncAddFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncAddFlashNode, flashNode)
}

func (c *Cluster) syncDeleteFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncDeleteFlashNode, flashNode)
}

func (c *Cluster) syncUpdateFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncUpdateFlashNode, flashNode)
}

func (c *Cluster) tryToChangeLeaderByHost() error {
	return c.partition.TryToLeader(1)
}

func (c *Cluster) syncFlashNodeSetIOLimitTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		if _, err = node.SyncSendAdminTask(t); err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
	}
}
