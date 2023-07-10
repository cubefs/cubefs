package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"testing"
	"time"
)

func TestZoneDiskUsedRatio(t *testing.T) {
	s := &ChubaoFSMonitor{
		hosts: make([]*ClusterHost, 0),
	}
	domains := [2]string{
		"cn.chubaofs.jd.local",
		"cn.elasticdb.jd.local",
	}
	for _, domain := range domains {
		s.hosts = append(s.hosts, &ClusterHost{
			host: domain,
		})
	}
	s.checkZoneDiskUsedRatio()
	time.Sleep(time.Second * 5)
}

func TestIsSSDZone(t *testing.T) {
	hostStr := "cn.chubaofs.jd.local"
	ssdZones, hddZones, err := getClusterSSDAndHDDZones(hostStr)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v ssdZones:%v\n", hostStr, ssdZones)
	fmt.Printf("%v hddZones:%v\n", hostStr, hddZones)

	hostStr = "cn.elasticdb.jd.local"
	ssdZones, hddZones, err = getClusterSSDAndHDDZones(hostStr)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v ssdZones:%v\n", hostStr, ssdZones)
	fmt.Printf("%v hddZones:%v\n", hostStr, hddZones)
}

func getClusterSSDAndHDDZones(hostStr string) (ssdZones, hddZones []string, err error) {
	host := &ClusterHost{host: hostStr}
	csv, err := cfs.GetClusterStat(host.host, host.isReleaseCluster)
	if err != nil {
		return
	}
	for zoneName := range csv.ZoneStatInfo {
		if host.isSSDZone(zoneName) {
			ssdZones = append(ssdZones, zoneName)
		} else {
			hddZones = append(hddZones, zoneName)
		}
	}
	return
}
