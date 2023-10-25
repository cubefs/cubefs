package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	HOST          = "host"
	ClientPkgAddr = "clientPkgAddr"
)

func (s *ChubaoFSMonitor) scheduleToCheckClusterConfig() {
	s.CheckClusterConfig()
	for {
		t := time.NewTimer(time.Duration(s.clusterConfigCheck.Interval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.CheckClusterConfig()
		}
	}
}

func (s *ChubaoFSMonitor) CheckClusterConfig() {
	var alarmMsg string
	for _, clusterMap := range s.clusterConfigCheck.CfsCluster {
		host, ok := clusterMap[HOST]
		if !ok {
			continue
		}
		mc := master.NewMasterClient([]string{host}, false)
		clientPkgAddr, ok := clusterMap[ClientPkgAddr]
		if ok {
			alarmMsg += s.CheckClientPkgAddr(mc, clientPkgAddr)
		}
	}
	if len(alarmMsg) > 0 {
		checktool.WarnBySpecialUmpKey(UMPKeyClusterConfigCheck, alarmMsg)
	} else {
		log.LogInfo("CheckClusterConfig finished")
	}
}

func (s *ChubaoFSMonitor) CheckClientPkgAddr(mc *master.MasterClient, expectClientPkgAddr string) (alarmMsg string) {
	var err error
	cv, err := mc.AdminAPI().GetCluster()
	if err != nil {
		_, ok := err.(*json.SyntaxError)
		if ok {
			return
		}
		msg := fmt.Sprintf("get cluster info from %v failed,err:%v", mc.Nodes(), err)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		return
	}
	if cv.ClientPkgAddr != expectClientPkgAddr {
		alarmMsg = fmt.Sprintf("host:%v clientPkgAddr expect:%v actual:%v\n", mc.Nodes(), expectClientPkgAddr, cv.ClientPkgAddr)
		if err = mc.ClientAPI().SetClientPkgAddr(expectClientPkgAddr); err != nil {
			msg := fmt.Sprintf("set client package addr from %v failed,err:%v", mc.Nodes(), err)
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		} else {
			msg := fmt.Sprintf("set client package addr from %v clientPkgAddr:%v success", mc.Nodes(), expectClientPkgAddr)
			checktool.WarnBySpecialUmpKey(UMPKeyClusterConfigCheck, msg)
		}
	}
	return
}
