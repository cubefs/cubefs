package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"runtime/debug"
	"strings"
	"time"
)

type RestartNodeRecord struct {
	TimeStamp time.Time `gorm:"column:time_stamp;comment:时间"`
	NodeIloIp string    `gorm:"column:node_ilo_ip;comment:管理IP"`
	NodeIp    string    `gorm:"column:node_ip;comment:IP"`
	Addr      string    `gorm:"column:addr;comment:cluster"`
	Host      string    `gorm:"column:host;comment:cluster"`
}

func (RestartNodeRecord) TableName() string {
	return "check_tool_restart_node"
}

func (s *ChubaoFSMonitor) checkThenRestartNode(nodeAddr, host string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("action[checkThenRestartNode] panic r:%v", r)
			fmt.Println(msg)
			log.LogError(msg)
			debug.PrintStack()
		}
	}()
	if s.highLoadNodeSolver == nil || s.highLoadNodeSolver.db == nil {
		return
	}
	nodeIp := getIpFromIpPort(nodeAddr)
	log.LogDebug(fmt.Sprintf("action[checkThenRestartNode] nodeIp:%v nodeAddr:%v host:%v", nodeIp, nodeAddr, host))
	// 检查启动时间, 判断启动时间间隔 机器如果宕机，则获取不到
	totalStartupTime, err := GetNodeTotalStartupTime(nodeIp)
	if err != nil {
		//因为机器宕机获取不到，go command 会返回：exit status 1
		if strings.Contains(err.Error(), "exit") {
			log.LogErrorf("action[checkThenRestartNode] NodeIp:%v sysUpTimeInstance err:%v", nodeIp, err)
		} else {
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, fmt.Sprintf("IP:%v get sysUpTimeInstance failed", nodeIp))
			return
		}
	}
	if err == nil && totalStartupTime < minRestartDuration {
		err = fmt.Errorf("nodeIp:%v totalStartupTime:%v less than minRestartDuration:%v", nodeIp, totalStartupTime, minRestartDuration)
		return
	}
	if time.Since(s.lastRestartNodeTime) < RestartConnTimeoutNodeInterval {
		err = fmt.Errorf("lastRestartNodeTime:%v less than %v", s.lastRestartNodeTime, RestartConnTimeoutNodeInterval)
		return
	}
	countIn24Hour, err := s.getNodeRestartRecordsCountIn24Hour()
	if err != nil {
		return
	}
	if countIn24Hour >= s.RestartNodeMaxCountIn24Hour {
		err = fmt.Errorf("restartNodeCount:%v more than RestartNodeMaxCountIn24Hour:%v", countIn24Hour, s.RestartNodeMaxCountIn24Hour)
		return
	}
	// 根据IP 从数据库查询管理IP
	nodeIloIp, err := s.highLoadNodeSolver.getNodeIloIpFromDB(nodeIp)
	if err != nil {
		return
	}
	if err = s.SaveNodeRestartRecord(nodeIloIp, nodeIp, nodeAddr, host); err != nil {
		return
	}
	s.lastRestartNodeTime = time.Now()
	//告警 执行python脚本
	go DoRestartByPythonScript(nodeIp, nodeIloIp, RestartReasonConnTimeout)
	return
}

func (s *ChubaoFSMonitor) getNodeRestartRecordsCountIn24Hour() (count int, err error) {
	yesterdayTime := time.Now().AddDate(0, 0, -1)
	sqlStr := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE time_stamp >= '%v'", RestartNodeRecord{}.TableName(), yesterdayTime)
	if err = s.highLoadNodeSolver.db.Raw(sqlStr).Scan(&count).Error; err != nil {
		return
	}
	return
}

func (s *ChubaoFSMonitor) SaveNodeRestartRecord(nodeIloIp, nodeIp, Addr, host string) (err error) {
	if s.highLoadNodeSolver.db == nil {
		return fmt.Errorf("db is nil")
	}
	record := RestartNodeRecord{
		TimeStamp: time.Now(),
		NodeIloIp: nodeIloIp,
		NodeIp:    nodeIp,
		Addr:      Addr,
		Host:      host,
	}
	for i := 0; i < SaveToDBReTryTimes; i++ {
		if err = s.highLoadNodeSolver.db.Create(&record).Error; err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	return
}

func getIpFromIpPort(ipPort string) string {
	split := strings.Split(ipPort, ":")
	if len(split) == 0 {
		return ipPort
	}
	return split[0]
}
