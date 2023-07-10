package img

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/img"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft/util"
	"runtime/debug"
	"sync"
	"time"
)

func (s *VolStoreMonitor) scheduleToCheckDiskSpace() {
	for {
		for _, host := range s.hosts {
			log.LogInfof("clusterID[ds_image] checkDiskSpace [%v] begin", host)
			startTime := time.Now()
			s.checkDiskSpace("ds_image", host, UMPImageDiskSpaceKey)
			log.LogInfof("clusterID[ds_image] checkDiskSpace [%v] end,cost[%v]", host, time.Since(startTime))
		}
		time.Sleep(time.Duration(2) * time.Hour)
	}
}

func (s *VolStoreMonitor) checkDiskSpace(clusterId, host, umpKey string) {
	var (
		data []byte
		err  error
	)
	defer func() {
		if err != nil {
			log.LogFlush()
		}
	}()
	data, err = s.doRequest(clusterId, host, "checkDiskSpace")
	if err != nil {
		msg := fmt.Sprintf("action[checkDiskSpace],get cluster[%v] infomation from master[%v] occurred err[%v]", clusterId, host, err)
		s.doProcessAlarm(msg, clusterId, umpKey)
		return
	}
	//fmt.Println(string(msg))
	view := &ClusterTopologyView{}

	if err = json.Unmarshal(data, view); err != nil {
		log.LogErrorf("action[checkDiskSpace] clusterId[%v],host[%v],data[%v],err[%v]", clusterId, host, string(data), err)
		return
	}
	limitCh := make(chan bool, 5)
	wg := sync.WaitGroup{}
	for _, zone := range view.Topology.ZonesMap {
		for _, node := range zone.Nodes {
			limitCh <- true
			wg.Add(1)
			go func(h string, ch chan bool, w *sync.WaitGroup) {
				var (
					nodeData []byte
					url      string
					nodeErr  error
				)
				defer func() {
					if r := recover(); r != nil {
						msg := fmt.Sprintf("clusterId[%v] action[checkDiskSpace] panic r:%v url:%v data:%v", clusterId, r, url, string(nodeData))
						fmt.Println(msg)
						log.LogError(msg)
						debug.PrintStack()
					}
				}()
				defer func() {
					<-ch
					w.Done()
				}()
				n := new(Node)
				url = fmt.Sprintf("http://%v/admin/getnode?cluster=%v&node=%v", host, clusterId, h)
				for i := 1; i < 10; i++ {
					nodeData, nodeErr = img.DoRequest(url)
					if nodeErr == nil {
						break
					}
					time.Sleep(time.Second)
				}
				if nodeErr != nil {
					log.LogErrorf("clusterId[%v] get node failed, err:%v", clusterId, nodeErr)
					return
				}
				if nodeErr = json.Unmarshal(nodeData, n); nodeErr != nil {
					log.LogErrorf("clusterId[%v] unmarshal node[%v] failed,err[%v]", clusterId, h, nodeErr)
					return
				}
				for _, d := range n.DiskInfo {
					if d.RealAvail+int64(d.MinRestSize) < 1*util.GB {
						checktool.WarnBySpecialUmpKey(umpKey, fmt.Sprintf("disk[%v_%v] less than 1GB, can offline vols[%v]", n.Addr, d.Path, d.VolIDs))
					}
				}
				return
			}(node.TcpAddr, limitCh, &wg)
		}
	}
	wg.Wait()
}
