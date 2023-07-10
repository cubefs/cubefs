package cfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/go-ping/ping"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	cfgKeyDbConfigDSNPort          = "dbConfigDSN"
	SaveToDBReTryTimes             = 3
	LoadMinCurrent                 = 4000
	maxRestartNodeMaxCountIn24Hour = 3
	minRestartDuration             = time.Minute * 60
	RestartHighLoadNodeInterval    = time.Minute * 30
	RestartConnTimeoutNodeInterval = time.Minute * 30
	mdcAlarmInterval               = time.Minute * 5
	ContinueAlarmDuration          = time.Minute * 30
	MinUptimeThreshold             = time.Minute * 30
	ContinueAlarmCount             = 6
	MinCriticalLoad                = 8000
)

type RestartReason string

const (
	RestartReasonHighLoad    RestartReason = "HighLoad"
	RestartReasonConnTimeout RestartReason = "ConnTimeout"
)

type NodeStartStatus struct {
	StartComplete bool
	StarCost      time.Duration
	Version       string
}

type MDCInfo struct {
	Data   `json:"data"`
	Level  string `json:"level"`
	Labels struct {
		Iface string   `json:"iface"`
		Ip    []string `json:"ip"`
	} `json:"labels"`
}
type Data struct {
	MinLoadAvg `json:"min_load_avg"`
}

type MinLoadAvg struct {
	Current     float64 `json:"current"` //当前值
	Unit        string  `json:"unit"`    //单位
	CompareType string  `json:"compareType"`
	MatchCount  float64 `json:"matchCount"` //匹配次数
	//ThresholdValue float64 `json:"thresholdValue"` //阈值
}

type HighLoadNodeSolverRecord struct {
	TimeStamp time.Time `gorm:"column:time_stamp;comment:时间"`
	NodeIloIp string    `gorm:"column:node_ilo_ip;comment:管理IP"`
	NodeInfo
}

func (HighLoadNodeSolverRecord) TableName() string {
	return "check_tool_high_load_node_solver"
}

type NodeInfo struct {
	NodeIp         string  `gorm:"column:node_ip;comment:IP"`
	Iface          string  `gorm:"column:iface;comment:"`
	Current        float64 `gorm:"column:current;comment:当前值"`
	Unit           string  `gorm:"column:unit;comment:单位"`
	CompareType    string  `gorm:"column:compareType;comment:"`
	MatchCount     float64 `gorm:"column:match_count;comment:匹配次数"`
	ThresholdValue float64 `gorm:"column:threshold_value;comment:阈值"`
	IsCritical     bool    `gorm:"column:is_critical;comment:是否为critical级别"`
}

type AlarmRecord struct {
	Ip              string
	FirstAlarmTime  time.Time
	LatestAlarmTime time.Time
	Count           int
}

type ChubaoFSHighLoadNodeSolver struct {
	DBConfigDSN                 string
	db                          *gorm.DB
	RestartNodeMaxCountIn24Hour int
	lastRestartNodeTime         time.Time
	LoadMinCurrent              float64 // 处理的最小值
	ch                          chan *NodeInfo
	nodeAlarmRecords            map[string]*AlarmRecord
}

func StartChubaoFSHighLoadNodeSolver(cfg *config.Config) (s *ChubaoFSHighLoadNodeSolver) {
	s = &ChubaoFSHighLoadNodeSolver{
		RestartNodeMaxCountIn24Hour: maxRestartNodeMaxCountIn24Hour,
		LoadMinCurrent:              LoadMinCurrent,
		ch:                          make(chan *NodeInfo, 500),
		nodeAlarmRecords:            make(map[string]*AlarmRecord, 0),
	}
	err := s.parseConfig(cfg)
	if err != nil {
		fmt.Printf("StartChubaoFSHighLoadNodeSolver err:%v\n", err)
		return
	}
	registerChubaoFSHighLoadNodeSolver(s)
	go s.startChubaoFSHighLoadNodeSolver()
	return
}

func (s *ChubaoFSHighLoadNodeSolver) parseConfig(cfg *config.Config) (err error) {
	s.DBConfigDSN = cfg.GetString(cfgKeyDbConfigDSNPort)
	log.LogInfof("action[parseConfig] DBConfigDSN[%v]", s.DBConfigDSN)
	if s.DBConfigDSN == "" {
		return fmt.Errorf("DBConfigDSN is empty, do not start this")
	}
	return
}

// 开发接口 报警内部逻辑请异步处理，报警接口响应超过500ms 会重新发送报警
// 参考文档： https://cf.jd.com/pages/viewpage.action?pageId=442248013
func (s *ChubaoFSHighLoadNodeSolver) highLoadNodeSolverHandle(w http.ResponseWriter, r *http.Request) {
	mdcInfo, err := getMDCInfoFromReq(r)
	if err != nil {
		BuildJSONRespWithDiffCode(w, http.StatusOK, 1, nil, "get MDCInfo from req failed")
		log.LogErrorf("action[highLoadNodeSolverHandle] err:%v", err)
		return
	}
	BuildJSONRespWithDiffCode(w, http.StatusOK, 0, nil, "调用成功")
	log.LogInfof("action[highLoadNodeSolverHandle] mdcInfo:%v", mdcInfo)
	go func() {
		if len(mdcInfo.Labels.Ip) == 0 {
			log.LogInfof("action[highLoadNodeSolverHandle] IpCount is 0 mdcInfo:%v", mdcInfo)
			return
		}
		info := &NodeInfo{
			NodeIp:      mdcInfo.Labels.Ip[0],
			Iface:       mdcInfo.Labels.Iface,
			Current:     mdcInfo.Data.MinLoadAvg.Current,
			Unit:        mdcInfo.Data.MinLoadAvg.Unit,
			CompareType: mdcInfo.Data.MinLoadAvg.CompareType,
			MatchCount:  mdcInfo.Data.MinLoadAvg.MatchCount,
			//ThresholdValue: mdcInfo.Data.MinLoadAvg.ThresholdValue,
		}
		s.ch <- info
	}()
}

func (s *ChubaoFSHighLoadNodeSolver) highLoadNodeSolverCriticalHandle(w http.ResponseWriter, r *http.Request) {
	mdcInfo, err := getMDCInfoFromReq(r)
	if err != nil {
		BuildJSONRespWithDiffCode(w, http.StatusOK, 1, nil, "get MDCInfo from req failed")
		log.LogErrorf("action[highLoadNodeSolverCriticalHandle] err:%v", err)
		return
	}
	BuildJSONRespWithDiffCode(w, http.StatusOK, 0, nil, "调用成功")
	// 告警恢复，但是负载依然大于4000，小于8000，需要直接忽略
	if mdcInfo.Data.MinLoadAvg.Current < MinCriticalLoad {
		log.LogInfof("action[highLoadNodeSolverCriticalHandle] ignore warn mdcInfo:%v, MinLoadAvg less than MinCriticalLoad:%v", mdcInfo, MinCriticalLoad)
		return
	}
	log.LogInfof("action[highLoadNodeSolverCriticalHandle] mdcInfo:%v", mdcInfo)
	go func() {
		if len(mdcInfo.Labels.Ip) == 0 {
			log.LogInfof("action[highLoadNodeSolverCriticalHandle] IpCount is 0 mdcInfo:%v", mdcInfo)
			return
		}
		info := &NodeInfo{
			NodeIp:      mdcInfo.Labels.Ip[0],
			Iface:       mdcInfo.Labels.Iface,
			Current:     mdcInfo.Data.MinLoadAvg.Current,
			Unit:        mdcInfo.Data.MinLoadAvg.Unit,
			CompareType: mdcInfo.Data.MinLoadAvg.CompareType,
			MatchCount:  mdcInfo.Data.MinLoadAvg.MatchCount,
			//ThresholdValue: mdcInfo.Data.MinLoadAvg.ThresholdValue,
			IsCritical: true,
		}
		s.ch <- info
	}()
}

func getMDCInfoFromReq(r *http.Request) (mdcInfo MDCInfo, err error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	fmt.Printf("getMDCInfoFromReq time:%v data[%v]\n", time.Now(), string(data))
	if err = json.Unmarshal(data, &mdcInfo); err != nil {
		err = fmt.Errorf("action[getMDCInfoFromReq] data:%v err:%v", string(data), err)
		return
	}
	return
}

func (s *ChubaoFSHighLoadNodeSolver) startChubaoFSHighLoadNodeSolver() {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("action[startChubaoFSHighLoadNodeSolver] panic r:%v", r)
			fmt.Println(msg)
			log.LogError(msg)
			debug.PrintStack()
		}
	}()
	if err := s.initDB(); err != nil {
		fmt.Printf("startChubaoFSHighLoadNodeSolver failed to initDB ,err %s db config:%s", err, s.DBConfigDSN)
		return
	}
	fmt.Printf("startChubaoFSHighLoadNodeSolver finished DBConfigDSN[%v]\n", s.DBConfigDSN)
	s.highLoadNodeSolver()
}

func (s *ChubaoFSHighLoadNodeSolver) highLoadNodeSolver() {
	for {
		select {
		case info := <-s.ch:
			s.recordAndRestartNode(info)
		}
	}
}

func (s *ChubaoFSHighLoadNodeSolver) recordAndRestartNode(nodeInfo *NodeInfo) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("action[recordAndRestartNode] nodeInfo:%v err:%v", nodeInfo, err)
		}
	}()
	if nodeInfo == nil {
		return
	}
	msg := fmt.Sprintf("action[recordAndRestartNode] nodeInfo:%v", nodeInfo)
	log.LogWarn(msg)
	if nodeInfo.Current < s.LoadMinCurrent {
		err = fmt.Errorf("current:%v less than LoadMinCurrent:%v", nodeInfo.Current, s.LoadMinCurrent)
		delete(s.nodeAlarmRecords, nodeInfo.NodeIp)
		return
	}
	// 如果是IsCritical级别的负载回调，无须进行ping以及持续时间检查
	if nodeInfo.IsCritical == false {
		//检查机器是否能ping通，如果不通或者获取失败，则机器可能宕机，继续处理;
		//如果能ping通，且高负载告警持续一定时间(ContinueAlarmDuration)则继续处理；如果ping通，但是高负载持续时间过短，只进行记录，不执行处理
		ok, err1 := ServerPing(nodeInfo.NodeIp)
		if err1 != nil {
			log.LogErrorf("action[recordAndRestartNode] NodeIp:%v ServerPing err:%v", nodeInfo.NodeIp, err1)
		}
		if ok {
			// 能ping通，记录并检查持续告警时间和次数，小于一定时间或次数 才不做处理
			continueAlarmDuration, alarmCount := s.getContinueAlarmDurationAndCount(nodeInfo.NodeIp)
			if continueAlarmDuration < ContinueAlarmDuration || alarmCount < ContinueAlarmCount {
				msg = fmt.Sprintf("高负载节点IP:%v 负载:%v ping ok continueAlarmDuration:%v alarmCount:%v",
					nodeInfo.NodeIp, nodeInfo.Current, continueAlarmDuration, alarmCount)
				log.LogInfo(msg)
				return
			}
		}
	}
	// 检查启动时间，启动时间小于minRestartDuration 不执行重启， 机器如果宕机，则获取不到
	totalStartupTime, err := GetNodeTotalStartupTime(nodeInfo.NodeIp)
	if err != nil {
		//因为机器宕机获取不到，go command 会返回：exit status 1
		if strings.Contains(err.Error(), "exit") {
			log.LogErrorf("action[recordAndRestartNode] NodeIp:%v sysUpTimeInstance err:%v", nodeInfo.NodeIp, err)
		} else {
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, fmt.Sprintf("高负载节点IP:%v 负载:%v, get sysUpTimeInstance failed",
				nodeInfo.NodeIp, nodeInfo.Current))
			return
		}
	}
	if err == nil && totalStartupTime < minRestartDuration {
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, fmt.Sprintf("高负载节点IP:%v 负载:%v sysUpTimeInstance:%v less than minRestartDuration:%v will not restart",
			nodeInfo.NodeIp, nodeInfo.Current, totalStartupTime, minRestartDuration))
		err = fmt.Errorf("totalStartupTime:%v less than minRestartDuration:%v", totalStartupTime, minRestartDuration)
		return
	}
	// 检查datanode进程的启动情况 如果正在启动中就不处理
	nodeStatus, err := checkNodeStartStatus(fmt.Sprintf("%s:6001", nodeInfo.NodeIp), 60)
	if err == nil && nodeStatus.StartComplete == false {
		log.LogWarnf("action[recordAndRestartNode] NodeIp:%v nodeStatus:%v", nodeInfo.NodeIp, nodeStatus)
		return
	} else {
		log.LogErrorf("action[recordAndRestartNode] NodeIp:%v nodeStatus:%v err:%v", nodeInfo.NodeIp, nodeStatus, err)
	}
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, fmt.Sprintf("高负载节点IP:%v 负载:%v sysUpTimeInstance:%v",
		nodeInfo.NodeIp, nodeInfo.Current, totalStartupTime))
	if time.Since(s.lastRestartNodeTime) < RestartHighLoadNodeInterval {
		err = fmt.Errorf("lastRestartNodeTime:%v less than %v", s.lastRestartNodeTime, RestartHighLoadNodeInterval)
		return
	}
	// 查询24小时内的记录数
	countIn24Hour, err := s.getHighLoadNodeSolverRecordsCountIn24Hour()
	if err != nil {
		return
	}
	if countIn24Hour >= s.RestartNodeMaxCountIn24Hour {
		err = fmt.Errorf("restartNodeCount:%v more than RestartNodeMaxCountIn24Hour:%v", countIn24Hour, s.RestartNodeMaxCountIn24Hour)
		return
	}
	// 根据IP 从数据库查询管理IP
	nodeIloIp, err := s.getNodeIloIpFromDB(nodeInfo.NodeIp)
	if err != nil {
		return
	}
	if err = s.SaveHighLoadNodeSolverRecord(nodeIloIp, nodeInfo); err != nil {
		return
	}
	s.lastRestartNodeTime = time.Now()
	//告警 执行python脚本
	go DoRestartByPythonScript(nodeInfo.NodeIp, nodeIloIp, RestartReasonHighLoad)
}

// GetNodeTotalStartupTime
// 命令：snmpwalk -v 1 -c 360buy 11.5.109.163 sysUpTimeInstance
// 结果：DISMAN-EVENT-MIB::sysUpTimeInstance = Timeticks: (16520527) 1 day, 21:53:25.27
// 16520527 ：最后两位为秒之后的单位，并非毫秒，只是100进的计数方式，直接去掉后两位，只保留到秒
func GetNodeTotalStartupTime(physicalIP string) (totalStartupTime time.Duration, err error) {
	cmd := exec.Command("snmpwalk", "-v", "1", "-c", "360buy", physicalIP, "sysUpTimeInstance")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout // 标准输出
	cmd.Stderr = &stderr // 标准错误
	if err = cmd.Run(); err != nil {
		return
	}
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
	msg := fmt.Sprintf("GetNodeLastStartTime physicalIP:%v outStr:%v, errStr:%v", physicalIP, outStr, errStr)
	fmt.Println(msg)
	log.LogInfo(msg)

	// 转换为时间
	totalStartupTime, err = convertSysUpTimeInstanceStrToDuration(outStr)
	return
}

func convertSysUpTimeInstanceStrToDuration(str string) (totalStartupTime time.Duration, err error) {
	reg := regexp.MustCompile(`\([0-9]*\)`)
	res := reg.FindString(str)
	if len(res) == 0 || !strings.HasPrefix(res, "(") || !strings.HasSuffix(res, ")") {
		err = fmt.Errorf("wrong str:%v res:%v", str, res)
		return
	}

	durationStr := res[1 : len(res)-1]
	duration, err := strconv.Atoi(durationStr)
	if err != nil {
		err = fmt.Errorf("strconv err:%v durationStr:%v res:%v str:%v", err, durationStr, res, str)
		return
	}
	totalStartupTime = time.Second * time.Duration(duration/100)
	return
}

func (s *ChubaoFSHighLoadNodeSolver) getHighLoadNodeSolverRecordsCountIn24Hour() (count int, err error) {
	if s.db == nil {
		err = fmt.Errorf("db is nil")
		return
	}
	yesterdayTime := time.Now().AddDate(0, 0, -1)
	sqlStr := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE time_stamp >= '%v'", HighLoadNodeSolverRecord{}.TableName(), yesterdayTime)
	if err = s.db.Raw(sqlStr).Scan(&count).Error; err != nil {
		return
	}
	return
}

func (s *ChubaoFSHighLoadNodeSolver) getNodeIloIpFromDB(ip string) (nodeIloIp string, err error) {
	if s.db == nil {
		return "", fmt.Errorf("db is nil")
	}
	sqlStr := " SELECT ilo_ip FROM tb_device_pool WHERE ip = '" + ip + "' and role_info LIKE '%国内全功能集群%' AND role_info LIKE '%cfs%' "
	results := make([]string, 0)
	if err = s.db.Raw(sqlStr).Scan(&results).Error; err != nil {
		return
	}
	if len(results) == 0 {
		err = fmt.Errorf("get no result sqlStr:%v", sqlStr)
		return
	}
	nodeIloIp = results[0]
	return
}

func (s *ChubaoFSHighLoadNodeSolver) SaveHighLoadNodeSolverRecord(nodeIloIp string, nodeInfo *NodeInfo) (err error) {
	if s.db == nil {
		return fmt.Errorf("db is nil")
	}
	record := HighLoadNodeSolverRecord{
		TimeStamp: time.Now(),
		NodeInfo:  *nodeInfo,
		NodeIloIp: nodeIloIp,
	}
	for i := 0; i < SaveToDBReTryTimes; i++ {
		if err = s.db.Create(&record).Error; err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	return
}

// DoRestartByPythonScript 负载高---》关机---》等60秒---》开机---》检查电源状态是否ON---》检查服务是否正常
func DoRestartByPythonScript(nodeIp, nodeIloIp string, restartReason RestartReason) {
	var (
		msg string
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[DoRestartByPythonScript] nodeIp:%v nodeIloIp:%v err:%v", nodeIp, nodeIloIp, err)
		}
	}()
	msg = fmt.Sprintf("RestartReason:%v nodeIp:%v nodeIloIp:%v will be restart", restartReason, nodeIp, nodeIloIp)
	checktool.WarnBySpecialUmpKey(UMPCFSNodeRestartWarnKey, msg)
	go recordBySre(nodeIp, restartReason)
	if _, _, err = DoCMDByPythonScript(nodeIloIp, nodeIp, "soft", restartReason); err != nil {
		msg = fmt.Sprintf("RestartReason:%v DoCMDByPythonScript_soft nodeIp:%v nodeIloIp:%v err:%v", restartReason, nodeIp, nodeIloIp, err)
		log.LogErrorf(msg)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		return
	}
	// wait 60s then power on, 重试几次，确保已经启动
	time.Sleep(time.Second * 60)
	if _, _, err = DoCMDByPythonScript(nodeIloIp, nodeIp, "on", restartReason); err != nil {
		msg = fmt.Sprintf("RestartReason:%v DoCMDByPythonScript_on nodeIp:%v nodeIloIp:%v err:%v", restartReason, nodeIp, nodeIloIp, err)
		log.LogErrorf(msg)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	}
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second * 60)
		if CheckIsPowerOn(nodeIloIp, nodeIp, restartReason) {
			log.LogWarnf("nodeIloIp:%v Power is on, need not start", nodeIloIp)
			break
		}
		if _, _, err = DoCMDByPythonScript(nodeIloIp, nodeIp, "on", restartReason); err != nil {
			//错误记录日志 直接重试开机，开机状态下重试不会有影响
			msg = fmt.Sprintf("RestartReason:%v DoCMDByPythonScript_on nodeIp:%v nodeIloIp:%v err:%v", restartReason, nodeIp, nodeIloIp, err)
			log.LogErrorf(msg)
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
			continue
		}
	}
	return
}

// CheckIsPowerOn
// python ipmi.py ip status -- > ip Chassis Power is on
func CheckIsPowerOn(nodeIloIp, nodeIP string, restartReason RestartReason) bool {
	outStr, _, err := DoCMDByPythonScript(nodeIloIp, nodeIP, "status", restartReason)
	if err != nil {
		return false
	}
	return strings.Contains(outStr, "Power is on")
}

// DoCMDByPythonScript 1、关机 python ipmi.py iloip off
// 2、启动 python ipmi.py iloip on
// 3、查询电源状态 python ipmi.py iloip status
// 4、软关机python ipmi.py iloip soft
func DoCMDByPythonScript(nodeIloIp, nodeIP, cmdStr string, restartReason RestartReason) (outStr, errStr string, err error) {
	cmd := exec.Command("python", "ipmi.py", nodeIloIp, cmdStr)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout // 标准输出
	cmd.Stderr = &stderr // 标准错误
	if err = cmd.Run(); err != nil {
		return
	}
	outStr, errStr = string(stdout.Bytes()), string(stderr.Bytes())
	var msg string
	if errStr == "" {
		msg = fmt.Sprintf("RestartReason:%v Power_%v nodeIloIp:%v nodeIP:%v outStr:%v", restartReason, cmdStr, nodeIloIp, nodeIP, outStr)
	} else {
		msg = fmt.Sprintf("RestartReason:%v Power_%v nodeIloIp:%v nodeIP:%v outStr:%v, errStr:%v", restartReason, cmdStr, nodeIloIp, nodeIP, outStr, errStr)
	}
	fmt.Println(msg)
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func (s *ChubaoFSHighLoadNodeSolver) initDB() (err error) {
	s.db, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                       s.DBConfigDSN,
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}), &gorm.Config{})
	if err != nil {
		log.LogErrorf("initDB failed err:%v", err)
		s.db = nil
		return
	}
	return
}

func (s *ChubaoFSHighLoadNodeSolver) closeDB() {
	if s.db == nil {
		return
	}
	db, _ := s.db.DB()
	if db != nil {
		db.Close()
	}
}

func ServerPing(target string) (ok bool, err error) {
	pinger, err := ping.NewPinger(target)
	if err != nil {
		return
	}
	pinger.Count = 4
	pinger.Timeout = 3 * time.Second
	pinger.SetPrivileged(true)
	// blocks until finished
	if err = pinger.Run(); err != nil {
		return
	}
	stats := pinger.Statistics()
	log.LogDebug(fmt.Sprintf("ServerPing stats:%v", stats))
	// 有回包，就是说明IP是可用的
	if stats.PacketsRecv >= 1 {
		ok = true
		return
	}
	return
}

func (s *ChubaoFSHighLoadNodeSolver) getContinueAlarmDurationAndCount(nodeIp string) (continueAlarmDuration time.Duration, alarmCount int) {
	alarmRecord, ok := s.nodeAlarmRecords[nodeIp]
	// 不存在，或者最近一次告警时间大于9分钟（两次告警的时间-1min）则初始化新的
	if !ok || time.Since(alarmRecord.LatestAlarmTime) > (mdcAlarmInterval*2-1) {
		now := time.Now()
		alarmRecord = &AlarmRecord{
			Ip:              nodeIp,
			FirstAlarmTime:  now,
			LatestAlarmTime: now,
			Count:           1,
		}
	} else {
		alarmRecord.LatestAlarmTime = time.Now()
		alarmRecord.Count++
	}
	s.nodeAlarmRecords[nodeIp] = alarmRecord
	continueAlarmDuration = alarmRecord.LatestAlarmTime.Sub(alarmRecord.FirstAlarmTime)
	alarmCount = alarmRecord.Count
	log.LogDebug(*alarmRecord)
	return
}

func checkNodeStartStatus(nodeIpPort string, overtimeSecond time.Duration) (nodeStatus NodeStartStatus, err error) {
	var data []byte
	defer func() {
		msg := fmt.Sprintf("action[checkNodeStartStatus] nodeIpPort:%v data:%v nodeStatus:%v err:%v", nodeIpPort, string(data), nodeStatus, err)
		if err != nil {
			log.LogError(msg)
		} else {
			log.LogDebug(msg)
		}
	}()
	url := fmt.Sprintf("http://%v/status", nodeIpPort)
	data, err = doRequestWithTimeOut(url, overtimeSecond)
	if err != nil {
		return
	}
	if err = json.Unmarshal(data, &nodeStatus); err != nil {
		return
	}
	return
}

func recordBySre(nodeIp string, restartReason RestartReason) {
	//http://api.storage.sre.jd.local/openApi/opsRecordIP?ip=10.199.146.186&type=故障类型测试222&message=信息测试222&common=测试222
	url := fmt.Sprintf("http://api.storage.sre.jd.local/openApi/opsRecordIP?ip=%v&type=%s", nodeIp, restartReason)
	_, err := doRequestWithTimeOut(url, 10)
	if err != nil {
		log.LogError(fmt.Sprintf("action[recordBySre] ip:%v,type:%v err:%v", nodeIp, restartReason, err))
		return
	}
	return
}
