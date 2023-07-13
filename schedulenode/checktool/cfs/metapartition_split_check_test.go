package cfs

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCheckMetaPartitionSplit(t *testing.T) {
	s := &ChubaoFSMonitor{
		hosts: make([]*ClusterHost, 0),
	}
	domains := []string{
		"cn.chubaofs.jd.local",
		"cn.elasticdb.jd.local",
		"cn.chubaofs-seqwrite.jd.local",
	}
	for _, domain := range domains {
		var isRelease bool
		if strings.Contains(domain, "seqwrite") {
			isRelease = true
		}
		s.hosts = append(s.hosts, &ClusterHost{
			host:             domain,
			isReleaseCluster: isRelease,
		})
	}
	for _, host := range s.hosts {
		fmt.Printf("%v:%v\n", host.host, host.isReleaseCluster)
	}
	//s.checkMetaPartitionSplit()
	time.Sleep(time.Hour)
}
