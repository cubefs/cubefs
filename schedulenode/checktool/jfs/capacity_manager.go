package jfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/tiglabs/raft/util"

	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	OssClusterName         = "jss"
	OssClusterAvailSpaceTB = 100
)

func (s *JFSMonitor) checkClusterCapacity() {
	for _, cluster := range s.clusters.Clusters {
		cluster.checkCapacity()
	}
}

func (c *JFSCluster) checkCapacity() {
	go func() {
		defer func() {
			checktool.HandleCrash()
		}()
		if c.Name == "image1.0" {
			return
		}
		c.checkTFNodeCapacity()
		for {
			time.Sleep(time.Hour * 24)
			c.checkTFNodeCapacity()
		}
	}()

	go func() {
		defer func() {
			checktool.HandleCrash()
		}()
		if !(c.Name == "jss" || c.Name == "id.jss" || c.Name == "th.jss" || c.Name == "kepler") {
			return
		}
		c.checkFlowNodeCapacity()
		for {
			time.Sleep(time.Hour * 24)
			c.checkFlowNodeCapacity()
		}
	}()
}

func (c *JFSCluster) checkTFNodeCapacity() {
	log.LogInfof("checkTFNodeCapacity [%v] begin", c)
	c.tnAvailSpace = 0
	c.writeableTFGroups = make(map[uint64]int64, 0)
	c.checkNodesCapacity(0, "checkTFNodeCapacity")
	// tel alarm when less than 3TB
	//对象存储集群 阈值100TB
	if c.Name == OssClusterName && c.tnAvailSpace < OssClusterAvailSpaceTB*unit.TB {
		msg := fmt.Sprintf("cluster[%v],less than %vTB,tnAvailSpace[%v]TB", c.Name, OssClusterAvailSpaceTB, c.tnAvailSpace/unit.TB)
		c.WarnBySpecialUmpKey(umpKeyJFSAvailSpace, msg)
	}
	if c.tnAvailSpace < 3*unit.TB {
		msg := fmt.Sprintf("cluster[%v],less than 3TB,tnAvailSpace[%v]TB", c.Name, c.tnAvailSpace/unit.TB)
		c.WarnBySpecialUmpKey(umpKeyJFSAvailSpace, msg)
	}
	msg := fmt.Sprintf("cluster[%v],tnAvailSpace[%v]TB,writeableTFGroupsCount:%v", c.Name, c.tnAvailSpace/unit.TB, len(c.writeableTFGroups))
	log.LogInfo(msg)
	log.LogInfof("checkTFNodeCapacity [%v] end", c)
	log.LogDebug(fmt.Sprintf("%v,writeableTFGroups:%v", msg, c.writeableTFGroups))
}

func (c *JFSCluster) checkFlowNodeCapacity() {
	log.LogInfof("checkFlowNodeCapacity [%v] begin", c)
	c.fnAvailSpace = 0
	c.writeableFLGroups = make(map[uint64]int64, 0)
	c.checkNodesCapacity(1, "checkFlowNodeCapacity")
	// tel alarm when less than 3TB
	//对象存储集群 阈值100TB
	if c.Name == OssClusterName && c.fnAvailSpace < OssClusterAvailSpaceTB*unit.TB {
		msg := fmt.Sprintf("cluster[%v],less than %vTB,fnAvailSpace[%v]TB", c.Name, OssClusterAvailSpaceTB, c.fnAvailSpace/unit.TB)
		c.WarnBySpecialUmpKey(umpKeyJFSAvailSpace, msg)
	}
	if c.fnAvailSpace < 3*unit.TB {
		if c.Name == "th.jss" && c.fnAvailSpace > 1.5*unit.TB {
			log.LogInfof("cluster[%v],fnAvailSpace[%v]TB", c.Name, c.fnAvailSpace/unit.TB)
			return
		}
		msg := fmt.Sprintf("cluster[%v],less than 3TB,fnAvailSpace[%v]TB", c.Name, c.fnAvailSpace/unit.TB)
		c.WarnBySpecialUmpKey(umpKeyJFSAvailSpace, msg)
	}
	msg := fmt.Sprintf("cluster[%v],fnAvailSpace[%v]TB,writeableFLGroupsCount:%v", c.Name, c.fnAvailSpace/unit.TB, len(c.writeableFLGroups))
	log.LogInfo(msg)
	log.LogInfof("checkFlowNodeCapacity [%v] end", c)
	log.LogDebug(fmt.Sprintf("%v,writeableFLGroups:%v", msg, c.writeableFLGroups))
}

func (c *JFSCluster) checkNodesCapacity(index int, actionName string) {
	startTime := time.Now()
	conn, err := c.zkConnPool.Get(c.ZkAddrs[0], c.IpAddrs)
	log.LogDebugf("action[%v] start cluster[%v],zkAddr[%v]", actionName, c.Name, c.ZkAddrs[0])
	if err != nil {
		msg := fmt.Sprintf("action[%v] connect zk(%v) err(%v)", actionName, c.ZkAddrs, err)
		c.WarnBySpecialUmpKey(umpKeyJFSNodeAlive, msg)
		return
	}
	defer func() {
		log.LogDebugf("action[%v] c[%v],zk[%v] total cost [%v]", actionName, c.Name, c.ZkAddrs[0], time.Since(startTime))
		c.zkConnPool.Put(c.ZkAddrs[0], conn, err != nil)
	}()
	nodesZkChildren, _, err := conn.Children(ZkRgPaths[index])
	if err != nil {
		msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v] GetChildren[%v] error(%v)",
			actionName, c.Name, c.ZkAddrs[0], ZkRgPaths[index], err)
		c.WarnBySpecialUmpKey(umpKeyJFSNodeAlive, msg)
		return
	}
	nodesLen := len(nodesZkChildren)
	log.LogInfof("action[%v] c[%v],zk[%v] len(nodesZkChildren)=[%v] ", actionName, c.Name, c.ZkAddrs[0], nodesLen)
	nodesCh := make(chan string, nodesLen)
	for _, rgIDPath := range nodesZkChildren {
		nodesCh <- rgIDPath
	}
	var goroutineLen int
	if nodesLen > maxConcurrent {
		goroutineLen = maxConcurrent
	} else {
		goroutineLen = nodesLen
	}
	var wg sync.WaitGroup
	for i := 0; i < goroutineLen; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				checktool.HandleCrash()
			}()
			for {
				select {
				case rgIDPath := <-nodesCh:
					c.doCheckCapacity(index, actionName, rgIDPath)
				default:
					return
				}
			}
		}()
	}
	wg.Wait()
	close(nodesCh)
	return
}

func (c *JFSCluster) doCheckCapacity(index int, actionName, rgIDPath string) {
	var (
		rgID      uint64
		rgMembers []string
		msg       string
		err       error
	)
	conn, err := c.zkConnPool.Get(c.ZkAddrs[0], c.IpAddrs)
	if err != nil {
		msg := fmt.Sprintf("action[%v] connect zk(%v) err(%v)", actionName, c.ZkAddrs, err)
		c.WarnBySpecialUmpKey(umpKeyJFSNodeAlive, msg)
		return
	}
	defer func() {
		c.zkConnPool.Put(c.ZkAddrs[0], conn, err != nil)
	}()
	if rgID, err = strconv.ParseUint(rgIDPath, 10, 64); err != nil {
		msg = fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v] parse to rgID(%v) failed, err(%v)",
			actionName, c.Name, c.ZkAddrs[0], rgIDPath, err)
		c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
		return
	}
	rgIDAbsPath := path.Join(ZkRgPaths[index], rgIDPath)
	rgMembers, _, err = conn.Children(rgIDAbsPath)
	if err != nil {
		msg = fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v] get rgID(%v) err(%v)",
			actionName, c.Name, c.ZkAddrs[0], rgIDAbsPath, err)
		c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
		return
	}
	if len(rgMembers) < 3 {
		return
	}
	var (
		minAvailSpace  int64
		availSpace     int64
		writeableCount int
	)
	minAvailSpace = math.MaxInt64
	for _, nodeID := range rgMembers {
		id, e1 := strconv.ParseUint(nodeID, 10, 64)
		if e1 != nil {
			msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v], parse nodeID[%v] failed,err[%v]",
				actionName, c.Name, c.ZkAddrs[0], nodeID, err)
			c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
			break
		}
		if id == 0 {
			continue
		}
		gpID := GpID(id)
		if gpID != rgID {
			msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v],invalid node ID[%v],this ID doesn't belong rgID[%v]",
				actionName, c.Name, c.ZkAddrs[0], id, rgID)
			c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
			break
		}
		if index == 0 {
			availSpace, err = c.getNodeWeightSpace(rgIDAbsPath, nodeID, conn)
		} else if index == 1 {

			availSpace, err = c.getFlowNodeWeightSpace(ZkCtlPaths[index], nodeID, conn)
		}
		if err != nil {
			return
		}
		if availSpace >= 32*util.MB {
			writeableCount++
		}
		if availSpace < minAvailSpace {
			minAvailSpace = availSpace
		}
	}
	if minAvailSpace == math.MaxInt64 {
		log.LogError(fmt.Sprintf("doCheckCapacity CalErr index:%v zkAddr=%v,rgID=%v,rgMembers=%v", index, c.ZkAddrs, rgID, rgMembers))
		return
	}
	availSpace = minAvailSpace - 32*1024*1024*1024
	if writeableCount < 3 {
		return
	}
	c.writeableGroups(index, rgID, availSpace)
	if index == 0 {
		c.addTNAvailSpace(availSpace)
	} else {
		c.addFNAvailSpace(availSpace)
	}
}

func (c *JFSCluster) writeableGroups(index int, rgID uint64, availSpace int64) {
	if index == 0 {
		c.tfGroupsLock.Lock()
		c.writeableTFGroups[rgID] = availSpace
		c.tfGroupsLock.Unlock()
	} else {
		c.flGroupsLock.Lock()
		c.writeableFLGroups[rgID] = availSpace
		c.flGroupsLock.Unlock()
	}
}

func (c *JFSCluster) getFlowNodeWeightSpace(parentPath, nodeID string, conn *zk.Conn) (restSpace int64, err error) {
	nodeIDPath := path.Join(parentPath, nodeID)
	nodeDataBytes, _, err := conn.Get(nodeIDPath)
	if err != nil {
		return
	}
	nodeDataStr := string(nodeDataBytes)
	//log.LogWarnf("getFlowNodeWeightSpace zkAddr=%v,rgID=%v,nodeID=%v,nodeDataStr=%v", c.ZkAddrs, parentPath, nodeID, nodeDataStr)
	wdInfo := strings.Split(nodeDataStr, ",")
	if len(wdInfo) != 2 {
		log.LogWarnf("getFlowNodeWeightSpace invalid data,zkAddr=%v,rgID=%v,nodeID=%v,nodeDataStr=%v", c.ZkAddrs, parentPath, nodeID, nodeDataStr)
		return
	}
	weightIDC := strings.Split(wdInfo[1], "|")
	restSpace, err = strconv.ParseInt(weightIDC[0], 10, 64)
	return
}

func (c *JFSCluster) getNodeWeightSpace(parentPath, nodeID string, conn *zk.Conn) (restSpace int64, err error) {
	nodeIDPath := path.Join(parentPath, nodeID)
	nodeDataBytes, _, err := conn.Get(nodeIDPath)
	if err != nil {
		return
	}
	nodeDataStr := string(nodeDataBytes)
	//log.LogWarnf("getNodeWeightSpace zkAddr=%v,rgID=%v,nodeID=%v,nodeDataStr=%v", c.ZkAddrs, parentPath, nodeID, nodeDataStr)
	wdInfo := strings.Split(nodeDataStr, ",")
	if len(wdInfo) != 2 {
		log.LogWarnf("getNodeWeightSpace invalid data,zkAddr=%v,rgID=%v,nodeID=%v,nodeDataStr=%v", c.ZkAddrs, parentPath, nodeID, nodeDataStr)
		return
	}
	restSpace, err = strconv.ParseInt(wdInfo[1], 10, 64)
	return
}
