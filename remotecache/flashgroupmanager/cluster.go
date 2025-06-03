package flashgroupmanager

import (
	"fmt"
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
}

func newCluster(name string) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.flashNodeTopo = NewFlashNodeTopology()
	c.stopc = make(chan bool)
	c.idAlloc = newIDAllocator()
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
