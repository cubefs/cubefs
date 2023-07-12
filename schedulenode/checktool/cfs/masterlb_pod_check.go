package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/jdos"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const (
	PodStatusRunning       = "Running"
	PodStatusTerminating   = "Terminating"
	GroupEnvironmentPro    = "pro" // 分组类型,生产分组,对应jdos.Group中Environment字段
	GroupEnvironmentPre    = "pre" // 分组类型,预发分组,对应jdos.Group中Environment字段
	SysNameCFS             = "chubaofs"
	MasterLBAPPNameCFSpark = "overwrite-master-lb"
	MasterLBHostCFSpark    = "cn.chubaofs.jd.local"

	MasterLBAPPNameCFSDbbak = "dbbakmasterlb"
	MasterLBHostCFSDbbak    = "cn.chubaofs-seqwrite.jd.local"

	MasterLBAPPNameCFSMysql     = "elasticdb-master-lb"
	MasterLBHostCFSMysql        = "cn.elasticdb.jd.local"
	MasterLBPort                = "80"
	MasterLBPortHealthyCheckAPI = "/admin/getIp"
	PodStatusWarningThreshold   = 0.1
	minMasterLBFaultTelCount    = 5
	PodStatusBatchCheckCount    = 10
	minMasterLBWarnCount        = 3
)

func (s *ChubaoFSMonitor) scheduleToCheckMasterLbPodStatus(cfg *config.Config) {
	if cfg.GetString(config.CfgRegion) == config.IDRegion {
		log.LogInfo("action[scheduleToCheckMasterLbPodStatus] need not for id region")
		return
	}
	s.checkMasterLbPodStatus()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkMasterLbPodStatus()
		}
	}
}

func (s *ChubaoFSMonitor) checkMasterLbPodStatus() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("checkMasterLbPodStatus panic:%v", r)
		}
	}()
	// check cfs spark
	s.checkPodsStatusOfAppAndAlarm(SysNameCFS, MasterLBAPPNameCFSpark, MasterLBHostCFSpark, PodStatusWarningThreshold)
	// check cfs dbback
	s.checkPodsStatusOfAppAndAlarm(SysNameCFS, MasterLBAPPNameCFSDbbak, MasterLBHostCFSDbbak, PodStatusWarningThreshold)
	// check cfs mysql
	s.checkPodsStatusOfAppAndAlarm(SysNameCFS, MasterLBAPPNameCFSMysql, MasterLBHostCFSMysql, PodStatusWarningThreshold)
}

func (s *ChubaoFSMonitor) checkPodsStatusOfAppAndAlarm(systemName, appName, host string, threshold float32) {
	totalPodsCounts, notRunningPodIps, err := checkPodsStatFromJDOS(systemName, appName, host)
	if err != nil {
		log.LogErrorf("action[checkPodsStatusOfAppAndAlarm] err:%v", err)
		return
	}
	key := systemName + appName
	masterLBWarnInfo, ok := s.masterLbLastWarnInfo[key]
	if !ok || masterLBWarnInfo == nil {
		masterLBWarnInfo = &MasterLBWarnInfo{}
		s.masterLbLastWarnInfo[key] = masterLBWarnInfo
	}
	if totalPodsCounts == 0 || len(notRunningPodIps) == 0 {
		masterLBWarnInfo.ContinuedTimes = 0
		log.LogInfof("action[checkPodsStatusOfAppAndAlarm] masterlb check systemName:%v, appName:%v, PodsCounts:%v, notRunningPodIps:%v",
			systemName, appName, totalPodsCounts, notRunningPodIps)
		return
	}
	if float32(len(notRunningPodIps))/float32(totalPodsCounts) > threshold || len(notRunningPodIps) > minMasterLBFaultTelCount {
		msg := fmt.Sprintf("masterlb check systemName:%v, appName:%v, PodsCounts:%v, notRunningPodIps:%v",
			systemName, appName, totalPodsCounts, notRunningPodIps)
		checktool.WarnBySpecialUmpKey(UMPKeyMasterLbPodStatus, msg)
	} else {
		// 连续minMasterLBWarnCount次再执行普通告警, 每次告警时间间隔十分钟
		masterLBWarnInfo.ContinuedTimes++
		if time.Since(masterLBWarnInfo.LastWarnTime) >= time.Minute*5 && masterLBWarnInfo.ContinuedTimes >= minMasterLBWarnCount {
			masterLBWarnInfo.LastWarnTime = time.Now()
			masterLBWarnInfo.ContinuedTimes = 0
			msg := fmt.Sprintf("masterlb check systemName:%v, appName:%v, PodsCounts:%v, notRunningPodIps:%v",
				systemName, appName, totalPodsCounts, notRunningPodIps)
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		} else {
			msg := fmt.Sprintf("masterlb check systemName:%v, appName:%v, PodsCounts:%v, notRunningPodIps:%v, masterLBWarnInfo:%v",
				systemName, appName, totalPodsCounts, notRunningPodIps, *masterLBWarnInfo)
			log.LogInfo(msg)
		}
	}
}

func checkPodsStatFromJDOS(systemName, appName, host string) (totalPodsCount int, notRunningPodIps []string, err error) {
	notRunningPodIps = make([]string, 0)
	jdosOpenApi := jdos.NewJDOSOpenApi(systemName, appName, jdos.OnlineSite, jdos.Erp, jdos.OnlineToken)
	groupsDetails, err := jdosOpenApi.GetAllGroupsDetails()
	if err != nil {
		return
	}
	podIps := make([]string, 0, PodStatusBatchCheckCount)
	for _, group := range groupsDetails {
		groupAllPods, err1 := jdosOpenApi.GetGroupAllPods(group.GroupName)
		if err1 != nil {
			err = fmt.Errorf("action[GetGroupAllPods] from group:%v err:%v", group.GroupName, err1)
			return
		}
		totalPodsCount += len(groupAllPods)
		for _, pod := range groupAllPods {
			if pod.LbStatus != jdos.LbStatusActivce {
				continue
			}
			if pod.Status == PodStatusRunning {
				// pod状态正常 检查nginx实例
				podIps = append(podIps, pod.PodIP)
				if len(podIps) >= PodStatusBatchCheckCount {
					badIps := batchCheckPodHealth(podIps, MasterLBPort, MasterLBPortHealthyCheckAPI, host)
					notRunningPodIps = append(notRunningPodIps, badIps...)
					podIps = make([]string, 0, PodStatusBatchCheckCount)
				}
			} else {
				notRunningPodIps = append(notRunningPodIps, pod.PodIP)
			}
		}
	}
	return
}

func batchCheckPodHealth(podIps []string, port, api, host string) (badIps []string) {
	badIps = make([]string, 0)
	badIpsLock := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for _, podIp := range podIps {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			if _, err1 := doServiceHealthyRequest(ip, port, api, host); err1 != nil {
				log.LogErrorf("action[doServiceHealthyRequest] ip:%v port:%v api:%v err1:%v", ip, port, api, err1)
				badIpsLock.Lock()
				badIps = append(badIps, ip)
				badIpsLock.Unlock()
			}
		}(podIp)
	}
	wg.Wait()
	return
}

func doServiceHealthyRequest(podIp string, port string, api string, host string) (data []byte, err error) {
	var resp *http.Response
	url := fmt.Sprintf("http://%v:%v%v", podIp, port, api)
	// 设置3s超时
	client := http.Client{Timeout: time.Duration(3 * time.Second)}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Host = host
	if resp, err = client.Do(req); err != nil {
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("action[checkServiceHealthy] podIp[%v] port[%v] api[%v] host[%v],resp.Status[%v],body[%v],err[%v]", podIp, port, api, host, resp.Status, string(data), err)
		err = fmt.Errorf(msg)
		return
	}
	log.LogDebugf("masterlb check:%v,%v\n", url, string(data))
	return
}
