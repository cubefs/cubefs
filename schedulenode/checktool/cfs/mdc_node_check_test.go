package cfs

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/checktool/mdc"
	"github.com/cubefs/cubefs/util/config"
	"sort"
	"testing"
	"time"
)

var (
	now       = time.Now()
	endTime   = now.Unix() * 1000
	startTime = now.Add(-5*time.Minute).Unix() * 1000
	metrics   = []string{mdc.MinMemUsagePercent}
)

func TestGetMetricMonitorInfoByIpsFromMDC(t *testing.T) {
	ips := []string{"11.5.113.138", "11.3.117.130"}
	totalResults := getMetricMonitorInfoByIpsFromMDC(startTime, endTime, ips, metrics)
	t.Log(len(totalResults))
	for _, info := range totalResults {
		fmt.Println(*info)
	}
}

func TestGetCFSNodeInfos(t *testing.T) {
	host := &ClusterHost{
		host:             "cn.elasticdb.jd.local",
		isReleaseCluster: false,
	}
	mdcNodeInfos, err := getCFSNodeInfos(host, startTime, endTime, metrics)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(mdcNodeInfos, func(i, j int) bool {
		return mdcNodeInfos[i].Value > mdcNodeInfos[j].Value
	})
	t.Log(len(mdcNodeInfos))
	for _, info := range mdcNodeInfos {
		fmt.Printf("%s\n", info)
	}
}

func TestCheckCFSNodeLoadInfos(t *testing.T) {
	cfsm := NewChubaoFSMonitor(context.Background())
	cfg, _ := config.LoadConfigFile(checktool.ReDirPath("cfg.json"))
	cfsm.parseHighMemNodeWarnConfig(cfg)
	cfsm.hosts = []*ClusterHost{{host: "cn.elasticdb.jd.local", isReleaseCluster: false}}
	for _, host := range cfsm.hosts {
		host.nodeMemInfo = make(map[string]float64)
	}
	cfsm.scheduleToCheckCFSHighIncreaseMemNodes()
}
