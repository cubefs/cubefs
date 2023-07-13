package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStartChubaoFSHighLoadNodeSolver_WithoutCfg(t *testing.T) {
	StartChubaoFSHighLoadNodeSolver(new(config.Config))
}

func TestStartChubaoFSHighLoadNodeSolver(t *testing.T) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	s := &ChubaoFSHighLoadNodeSolver{
		RestartNodeMaxCountIn24Hour: 1,
		ch:                          make(chan *NodeInfo, 500),
		LoadMinCurrent:              LoadMinCurrent,
		DBConfigDSN:                 "root:1qaz@WSX@tcp(11.13.125.198:80)/storage_sre?charset=utf8mb4&parseTime=True&loc=Local",
	}
	go s.startChubaoFSHighLoadNodeSolver()
	nodeInfo := &NodeInfo{
		NodeIp:         "192.179.20.58",
		Current:        1200,
		CompareType:    "",
		MatchCount:     10,
		ThresholdValue: 1000,
	}
	s.ch <- nodeInfo
	wg.Wait()
}

func TestConvertSysUpTimeInstanceStrToDuration(t *testing.T) {
	if totalStartupTime, err := convertSysUpTimeInstanceStrToDuration("DISMAN-EVENT-MIB::sysUpTimeInstance = Timeticks: (16520527) 1 day, 21:53:25.27"); err != nil {
		t.Error(err)
	} else {
		t.Log(fmt.Sprintf("sysUpTimeInstance:%v", totalStartupTime))
	}

	if totalStartupTime, err := convertSysUpTimeInstanceStrToDuration("DISMAN-EVENT-MIB::sysUpTimeInstance = Timeticks: (12) 4:35:17.92"); err != nil {
		t.Error(err)
	} else {
		t.Log(totalStartupTime)
	}

	if totalStartupTime, err := convertSysUpTimeInstanceStrToDuration("DISMAN-EVENT-MIB::sysUpTimeInstance = Timeticks: (123) 4:35:17.92"); err != nil {
		t.Error(err)
	} else {
		t.Log(totalStartupTime)
	}

	if _, err := convertSysUpTimeInstanceStrToDuration("DISMAN-EVENT-MIB"); !strings.Contains(err.Error(), "wrong str") {
		t.Errorf("should wrong str err, but get:%v", err)
	} else {
		t.Log(err)
	}
}

func TestGetContinueAlarmDuration(t *testing.T) {
	s := &ChubaoFSHighLoadNodeSolver{
		RestartNodeMaxCountIn24Hour: 1,
		nodeAlarmRecords:            make(map[string]*AlarmRecord, 0),
		LoadMinCurrent:              LoadMinCurrent,
	}
	nodeIP := "192.168.1.1"
	if alarmDuration, alarmCount := s.getContinueAlarmDurationAndCount(nodeIP); alarmDuration != 0 || alarmCount != 1 {
		t.Errorf("alarmDuration,alarmCount should be 0 1,but get alarmDuration:%v alarmCount:%v", alarmDuration, alarmCount)
		return
	}
	time.Sleep(mdcAlarmInterval)
	if alarmDuration, alarmCount := s.getContinueAlarmDurationAndCount(nodeIP); alarmDuration < mdcAlarmInterval || alarmCount != 2 {
		t.Errorf("alarmDuration should more than mdcAlarmInterval:%v,but get alarmDuration:%v, alarmCount should be 2,but get:%v",
			mdcAlarmInterval, alarmDuration, alarmCount)
		return
	} else {
		t.Logf("mdcAlarmInterval:%v alarmDuration:%v alarmCount:%v", mdcAlarmInterval, alarmDuration, alarmCount)
	}
	time.Sleep(mdcAlarmInterval * 2)
	if alarmDuration, alarmCount := s.getContinueAlarmDurationAndCount(nodeIP); alarmDuration != 0 || alarmCount != 1 {
		t.Errorf("alarmDuration,alarmCount should be 0 1,but get alarmDuration:%v alarmCount:%v", alarmDuration, alarmCount)
		return
	}
}

func TestCheckNodeStartStatus(t *testing.T) {
	nodeStatus, err := checkNodeStartStatus("192.168.0.33:17320", 60)
	t.Log(nodeStatus, err)
}

func TestUnmarshalMDCInfo(t *testing.T) {
	s := `{"data":{"min_load_avg":{"current":17115,"unit":"","compareType":"gt","thresholdValue":"4000.0","matchCount":5}},"level":"警告","alarmTime":1659390789476,"content":"【警告】【自定义分组】 分组: cfs_spark_独立_底层报警, ip: 11.5.208.175, 系统负载已连续7次大于4000.0[当前值:17,115]。报警时间: 2022-08-02 05:53:09","labels":{"level":["warning"],"ext_metric":["min_load_avg"],"g_type":["1"],"groupId":["238827"],"ip":["11.5.208.175"],"ext_groupId":["238827"],"hookUrl":["http://172.18.152.139:7000/checkToolHighLoadNodeSolver"],"ext_groupType":["1"],"metricUnit":[""],"groupName":["cfs_spark_独立_底层报警"],"metric":["min_load_avg"],"provider":["mdc"],"hostType":["h"],"ext_metricAlias":["系统负载"],"ruleId":["6698691613462527176"],"ext_groupName":["cfs_spark_独立_底层报警"]}}`
	var mdcInfo MDCInfo
	if err := json.Unmarshal([]byte(s), &mdcInfo); err != nil {
		t.Errorf("action[getMDCInfoFromReq] data:%v err:%v", s, err)
		return
	}
	if len(mdcInfo.Labels.Ip) == 0 {
		t.Fatal()
	}
	fmt.Println(mdcInfo)
}
