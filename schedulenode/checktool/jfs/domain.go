package jfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/checktool/zkconn"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

type JFSCluster struct {
	Name                         string
	ID                           string
	ZkAddrs                      []string
	invalidTFNodes               map[uint64]*Node
	invalidFlowNodes             map[uint64]*Node
	invalidTFGroups              map[uint64]*ReplGroup
	invalidFlowGroups            map[uint64]*ReplGroup
	WriteableTFNodes             map[string]string
	WriteableFlowNodes           map[string]string
	regValidator                 *regexp.Regexp
	scheduleInternal             int
	tnAvailSpace                 int64
	fnAvailSpace                 int64
	lastDiskCorruptReportTime    int64
	lastZkConnClosedCheckedTime  int64
	lastZkConnClosedCheckedCount int
	zkConnPool                   *zkconn.ConnectPool
	IpAddrs                      []string
	capLock                      sync.RWMutex
	sync.RWMutex
	writeableTFGroups map[uint64]int64
	writeableFLGroups map[uint64]int64
	tfGroupsLock      sync.RWMutex
	flGroupsLock      sync.RWMutex
}

func (c *JFSCluster) initConnection() {
	if strings.Contains(c.ZkAddrs[0], "jd.local") {
		addrs, err := net.LookupHost(c.ZkAddrs[0])
		if err != nil {
			panic(err)
		}
		c.IpAddrs = addrs
	} else {
		c.IpAddrs = c.ZkAddrs
	}
	c.zkConnPool = zkconn.NewConnectPool()
}

func (c *JFSCluster) WarnBySpecialUmpKey(key, msg string) {
	if strings.Contains(msg, zkConnClosedMsg) {
		c.lastZkConnClosedCheckedCount++
		if time.Now().Unix()-c.lastZkConnClosedCheckedTime > 180 && c.lastZkConnClosedCheckedCount >= 2 {
			checktool.WarnBySpecialUmpKey(key, msg)
		}
		if time.Now().Unix()-c.lastZkConnClosedCheckedTime > 180 {
			c.lastZkConnClosedCheckedCount = 0
			c.lastZkConnClosedCheckedTime = time.Now().Unix()
		}
	} else {
		checktool.WarnBySpecialUmpKey(key, msg)
	}
}

func (c *JFSCluster) addTNAvailSpace(space int64) {
	if space <= 0 {
		return
	}
	c.capLock.Lock()
	defer c.capLock.Unlock()
	c.tnAvailSpace = c.tnAvailSpace + space
}

func (c *JFSCluster) addFNAvailSpace(space int64) {
	if space <= 0 {
		return
	}
	c.capLock.Lock()
	defer c.capLock.Unlock()
	c.fnAvailSpace = c.fnAvailSpace + space
}

type Node struct {
	id              int
	rg              int
	addr            string
	lastCheckedTime int64
	count           int
}

type ReplGroup struct {
	rgID            uint64
	lastCheckedTime int64
	count           int
}

func (c *JFSCluster) String() string {
	return fmt.Sprintf("Name[%v],ID[%v],ZkAddrs[%v]", c.Name, c.ID, c.ZkAddrs[0])
}

type JFSClusters struct {
	Clusters []*JFSCluster
}
