package cfs

import (
	"testing"
	"time"
)

func TestZoneMnDnWriteAbilityRate(t *testing.T) {
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
	s.checkZoneMnDnWriteAbilityRate()
	time.Sleep(time.Second * 5)
}
