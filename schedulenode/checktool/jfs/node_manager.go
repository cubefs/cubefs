package jfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	maxConcurrent    = 50
	nodeTypeTFNode   = 0
	nodeTypeFlowNode = 1
)

var (
	ZkRgPaths  = []string{"/jfs-root/tfnode-rg", "/jfs-root/flownode-rg"}
	ZkCtlPaths = []string{"/jfs-root/tfnode", "/jfs-root/flownode"}
	RegStr     = `^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[1-9]\d*`
)

func (s *JFSMonitor) checkNodesAlive() {
	for _, cluster := range s.clusters.Clusters {
		cluster.checkNodesAlive()
	}
}

func (c *JFSCluster) checkNodesAlive() {
	go func() {
		defer func() {
			checktool.HandleCrash()
		}()
		c.checkTFNode()
		for {
			time.Sleep(time.Duration(c.scheduleInternal) * time.Second)
			c.checkTFNode()
		}
	}()

	go func() {
		defer func() {
			checktool.HandleCrash()
		}()
		c.checkFlowNode()
		for {
			time.Sleep(time.Duration(c.scheduleInternal) * time.Second)
			c.checkFlowNode()
		}
	}()

	go func() {
		defer func() {
			checktool.HandleCrash()
		}()
		c.checkZkAlive()
		for {
			time.Sleep(time.Duration(c.scheduleInternal) * time.Second)
			c.checkZkAlive()
		}
	}()
}

func (c *JFSCluster) checkZkAlive() {
	log.LogInfof("checkZkAlive [%v] begin", c)
	defer func() {
		log.LogInfof("checkZkAlive [%v] end", c)
	}()
	zkAddrs := make([]string, 1)
	for _, zkAddr := range c.ZkAddrs {
		zkAddrs[0] = zkAddr
		conn, _, err := zk.Connect(zkAddrs, time.Second*10, zk.WithLogInfo(false))
		if err != nil {
			msg := fmt.Sprintf("action[%v] connect zk(%v) err(%v)", "checkZkAlive", c.ZkAddrs, err)
			c.WarnBySpecialUmpKey(umpKeyJFSZkAlive, msg)
			continue
		}
		conn.Close()
	}
}

func (c *JFSCluster) checkTFNode() {
	log.LogInfof("checkTFNode [%v] begin", c)
	c.checkNodes(0, "checkTFNodes")
	log.LogInfof("checkTFNode [%v] end", c)
}

func (c *JFSCluster) checkFlowNode() {
	log.LogInfof("checkFlowNode [%v] begin", c)
	c.checkNodes(1, "checkFlowNodes")
	log.LogInfof("checkFlowNode [%v] end", c)
}

func (c *JFSCluster) checkNodes(index int, actionName string) {
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
					c.doCheck(index, actionName, rgIDPath)
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

func (c *JFSCluster) doCheck(index int, actionName, rgIDPath string) {
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
	if len(rgMembers) < defaultReplicas-1 {
		c.doProcessInvalidReplGroup(rgID, len(rgMembers), index, actionName)
		return
	}
	invalidNodes := make(map[string]error)
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
		nodeIDPath := path.Join(ZkCtlPaths[index], nodeID)
		nodeDataBytes, _, e := conn.Get(nodeIDPath)
		if e != nil {
			invalidNodes[nodeID] = e
			continue
		}
		isMatch := c.regValidator.Match(nodeDataBytes)
		if !isMatch {
			msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v], get node data(%v) unavaliData(%v)",
				actionName, c.Name, c.ZkAddrs[0], nodeIDPath, string(nodeDataBytes))
			c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
			break
		}
		if c.isNodeOnDiskCorruptOrShutdown(index, nodeID, rgIDAbsPath, conn) {
			msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v], node(%v-%v) is diskCorrupted or shutdown",
				actionName, c.Name, c.ZkAddrs[0], nodeID, string(nodeDataBytes))
			if time.Now().UnixNano()-c.lastDiskCorruptReportTime > int64(time.Hour) {
				c.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
				c.lastDiskCorruptReportTime = time.Now().UnixNano()
			} else {
				log.LogWarn(msg)
			}
		}
	}

	if len(invalidNodes) > 1 {
		msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v],",
			actionName, c.Name, c.ZkAddrs[0])
		for nodeID, e1 := range invalidNodes {
			msg = msg + fmt.Sprintf("get node[%v] data failed,err[%v],", nodeID, e1)
		}
		c.WarnBySpecialUmpKey(umpKeyJFSNodeAlive, msg)
		return
	}
}

func (c *JFSCluster) isNodeOnDiskCorruptOrShutdown(index int, nodeID, rgIDAbsPath string, conn *zk.Conn) bool {
	var (
		availSpace int64
	)
	if index == 0 {
		availSpace, _ = c.getNodeWeightSpace(rgIDAbsPath, nodeID, conn)
	} else if index == 1 {

		availSpace, _ = c.getFlowNodeWeightSpace(ZkCtlPaths[index], nodeID, conn)
	}
	if availSpace < 0 {
		return true
	}
	return false
}

func (c *JFSCluster) doProcessInvalidReplGroup(rgID uint64, curNodesCount, nodeType int, actionName string) {
	c.Lock()
	defer c.Unlock()
	var (
		group *ReplGroup
		ok    bool
	)
	switch nodeType {
	case nodeTypeTFNode:
		group, ok = c.invalidTFGroups[rgID]
		if !ok {
			group = &ReplGroup{rgID: rgID, lastCheckedTime: time.Now().Unix(), count: 1}
			c.invalidTFGroups[rgID] = group
		}
	case nodeTypeFlowNode:
		group, ok = c.invalidFlowGroups[rgID]
		if !ok {
			group = &ReplGroup{rgID: rgID, lastCheckedTime: time.Now().Unix(), count: 1}
			c.invalidFlowGroups[rgID] = group
		}
	default:
		log.LogErrorf("action[doProcessInvalidReplGroup] unknown node type[%v],cluster[%v],rgID[%v],curNodesCount[%v]", nodeType, c.Name, rgID, curNodesCount)
		return
	}
	if group == nil {
		msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v],rgID[%v] miss nodes,current nodes count[%v],expect node count[%v]",
			actionName, c.Name, c.ZkAddrs[0], rgID, curNodesCount, defaultReplicas)
		log.LogErrorf(msg)
	}
	inOneCycle := time.Now().Unix()-group.lastCheckedTime < checktool.DefaultWarnInternal
	if inOneCycle && group.count >= checktool.DefaultMinCount {
		msg := fmt.Sprintf("action[%v] cluster[%v],zkAddr[%v],rgID[%v] miss nodes,current nodes count[%v],expect node count[%v]",
			actionName, c.Name, c.ZkAddrs[0], rgID, curNodesCount, defaultReplicas)
		c.WarnBySpecialUmpKey(umpKeyJFSNodeAlive, msg)
	}
	if !inOneCycle {
		group.count = 1
		group.lastCheckedTime = time.Now().Unix()
	}
}

func GpID(nodeID uint64) uint64 {
	return (nodeID-1)/3 + 1
}
