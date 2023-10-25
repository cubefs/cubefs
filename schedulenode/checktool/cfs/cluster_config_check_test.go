package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	interval := 100
	clusterConfigCheck := fmt.Sprintf(`{
		"interval": %v,
		"cfsCluster": [
			{
			  "host": "11.13.113.232",
			  "clientPkgAddr": "http://storage.jd.local/20210200/cfs/fuseupdate"
			},
  			{
			  "host": "test.chubaofs.jd.local",
			  "clientPkgAddr": "http://storage.jd.local/dpgimage/libcfs_mysql/"
			}
		]
	}`, interval)
	monitor := NewChubaoFSMonitor(context.Background())
	monitor.clusterConfigCheck = new(ClusterConfigCheck)
	if err := json.Unmarshal([]byte(clusterConfigCheck), monitor.clusterConfigCheck); err != nil {
		fmt.Println(err)
		return
	}
	assert.Equal(t, interval, monitor.clusterConfigCheck.Interval)
	monitor.CheckClusterConfig()
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for {
		select {
		case <-ticker.C:
			monitor.CheckClusterConfig()
		}
	}
}
