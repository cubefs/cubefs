package jfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"regexp"
	"strconv"
	"strings"
)

const (
	cfgKeyJfsAddrsPath  = "jfsAddrsPath"
	cfgKeyInterval      = "interval"
	defaultReplicas     = 3
	umpKeyJFSNodeAlive  = checktool.UmpKeyStorageBotPrefix + "jfs.node.alive"
	umpKeyJFSZkAlive    = checktool.UmpKeyStorageBotPrefix + "jfs.zk.alive"
	umpKeyJFSAvailSpace = checktool.UmpKeyStorageBotPrefix + "jfs.avail.space"
	zkConnClosedMsg     = "zk: connection closed"
)

type JFSMonitor struct {
	clusters         *JFSClusters
	scheduleInterval int
	region           string
	regValidator     *regexp.Regexp
}

func NewJFSMonitor() *JFSMonitor {
	return &JFSMonitor{}
}

func (s *JFSMonitor) Start(cfg *config.Config) (err error) {
	s.regValidator, err = regexp.Compile(RegStr)
	if err != nil {
		return
	}
	if err = s.parseConfig(cfg); err != nil {
		return
	}
	go s.schedule()
	fmt.Println("starting JFSMonitor finished")
	return
}

func (s *JFSMonitor) parseConfig(cfg *config.Config) (err error) {
	interval := cfg.GetString(cfgKeyInterval)
	s.region = cfg.GetString(config.CfgRegion)
	if interval == "" {
		return fmt.Errorf("parse interval failed,interval can't be nil")
	}

	if s.scheduleInterval, err = strconv.Atoi(interval); err != nil {
		return err
	}
	filePath := cfg.GetString(cfgKeyJfsAddrsPath)
	if err = s.extractClusterInfo(filePath); err != nil {
		return
	}
	fmt.Printf("scheduleInterval[%v]\n,cluster[%v] \n", s.scheduleInterval, s.clusters)
	return
}

func (s *JFSMonitor) extractClusterInfo(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	clusters := &JFSClusters{}
	if err = json.Unmarshal(cfg.Raw, clusters); err != nil {
		return
	}

	selectClusters := new(JFSClusters)
	for _, cluster := range clusters.Clusters {
		if s.region == config.IDRegion && !strings.Contains(cluster.ZkAddrs[0], config.IDRegion) {
			continue
		}
		if s.region != config.IDRegion && strings.Contains(cluster.ZkAddrs[0], config.IDRegion) {
			continue
		}
		selectClusters.Clusters = append(selectClusters.Clusters, cluster)
	}
	s.clusters = selectClusters

	for _, cluster := range s.clusters.Clusters {

		cluster.regValidator = s.regValidator
		cluster.scheduleInternal = s.scheduleInterval
		cluster.invalidFlowGroups = make(map[uint64]*ReplGroup, 0)
		cluster.invalidTFGroups = make(map[uint64]*ReplGroup, 0)
		cluster.invalidFlowNodes = make(map[uint64]*Node, 0)
		cluster.invalidTFNodes = make(map[uint64]*Node, 0)
		cluster.WriteableFlowNodes = make(map[string]string, 0)
		cluster.WriteableTFNodes = make(map[string]string, 0)
		cluster.writeableTFGroups = make(map[uint64]int64, 0)
		cluster.writeableFLGroups = make(map[uint64]int64, 0)
		cluster.initConnection()
	}
	return
}

func (s *JFSMonitor) schedule() {
	go s.checkClusterCapacity()
	s.checkNodesAlive()
}
