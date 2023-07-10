package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/jdos"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	ObjectNodeAppName    = "sparkobject"
	ObjectNodeListen     = 1601
	MaxConnectivityRetry = 5
)

func (s *ChubaoFSMonitor) scheduleToCheckObjectNodeAlive(cfg *config.Config) {
	if cfg.GetString(config.CfgRegion) == config.IDRegion {
		log.LogInfo("action[scheduleToCheckMasterLbPodStatus] need not for id region")
		return
	}
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.checkObjectNodeAlive()
	for {
		select {
		case <-ticker.C:
			s.checkObjectNodeAlive()
		}
	}
}

func (s *ChubaoFSMonitor) checkObjectNodeAlive() {
	var err error
	startTime := time.Now()
	log.LogInfof("checkObjectNodeAlive start")
	jdosAPI := jdos.NewJDOSOpenApi(SysNameCFS, ObjectNodeAppName, jdos.OnlineSite, jdos.Erp, jdos.OnlineToken)
	var groups jdos.Groups
	if groups, err = jdosAPI.GetAllGroupsDetails(); err != nil {
		log.LogErrorf("get all ObjectNode groups details failed: %v", err)
		return
	}
	var badPodIpsMap = make(map[*jdos.Group][]string) // group name to bad pod ip addresses
	var totalPods int
	var badPodIpsMapMu sync.Mutex
	var wg = new(sync.WaitGroup)
	for _, group := range groups {
		if group.Environment == GroupEnvironmentPre {
			continue // 跳过预发分组
		}
		wg.Add(1)
		go func(group *jdos.Group) {
			defer wg.Done()
			var badPodIps []string
			var totalPodsInGroup int
			var checkErr error
			if badPodIps, totalPodsInGroup, checkErr = s.checkObjectNodeAliveByGroup(group); checkErr != nil {
				log.LogErrorf("check ObjectNode alive by group [%v %v] failed: %v", group.GroupName, group.Nickname, err)
				return
			}
			if len(badPodIps) == 0 {
				return
			}
			badPodIpsMapMu.Lock()
			badPodIpsMap[group] = badPodIps
			totalPods += totalPodsInGroup
			badPodIpsMapMu.Unlock()
		}(group)
	}
	wg.Wait()
	if badPods := len(badPodIpsMap); badPods > 0 && totalPods > 0 && float64(badPods)/float64(totalPods) >= 0.1 {
		detailBuilder := strings.Builder{}
		for group, badPodIps := range badPodIpsMap {
			if len(badPodIps) > 0 {
				detailBuilder.WriteString(fmt.Sprintf("%v (%v): %v\n", group.Nickname, len(badPodIps), strings.Join(badPodIps, ", ")))
			}
		}
		msg := fmt.Sprintf("Bad ObjectNode pods found！\n%v", detailBuilder.String())
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	}
	log.LogInfof("checkObjectNodeAlive end,totalPods:%v cost [%v]", totalPods, time.Since(startTime))
}

func (s *ChubaoFSMonitor) checkObjectNodeAliveByGroup(group *jdos.Group) (badPodIps []string, totalPods int, err error) {
	api := jdos.NewJDOSOpenApi(SysNameCFS, ObjectNodeAppName, jdos.OnlineSite, jdos.Erp, jdos.OnlineToken)
	var pods jdos.Pods
	if pods, err = api.GetGroupAllPods(group.GroupName); err != nil {
		return
	}
	totalPods = len(pods)
	for _, pod := range pods {
		if pod.Status == PodStatusTerminating {
			continue
		}
		if len(pod.PodIP) == 0 {
			continue
		}
		if pod.Status != PodStatusRunning {
			badPodIps = append(badPodIps, pod.PodIP)
			continue
		}
		if checkErr := checkDestinationPortConnectivity(pod.PodIP, ObjectNodeListen, MaxConnectivityRetry); checkErr != nil {
			badPodIps = append(badPodIps, pod.PodIP)
			log.LogWarnf("check objectnode %v failed: %v", pod.PodIP, checkErr)
		}
	}
	return
}

func checkDestinationPortConnectivity(podIp string, port int, maxRetry int) (err error) {
	var addr = fmt.Sprintf("%v:%v", podIp, port)
	var conn net.Conn
	for i := 0; i < maxRetry; i++ {
		conn, err = net.DialTimeout("tcp", addr, time.Second*5)
		if err != nil {
			if i < maxRetry {
				time.Sleep(time.Second * 1)
			}
			continue
		}
		_ = conn.Close()
		return
	}
	return
}
