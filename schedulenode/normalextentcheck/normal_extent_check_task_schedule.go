package normalextentcheck

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/notify"
	"strings"
	"sync"
	"time"
)

const (
	checkResultTitle = "<tr><th width>卷名</th><th width>InodeID</th><th width>DP ID</th><th width>Extent ID</th>" +
	"<th width>ExtentOffset</th><th width>Size</th></tr>"
	checkFailedResultTitle = "<tr><th width>卷名</th><th width>检查失败原因</th></tr>"
	searchInodeFailedResultTitle = "<tr><th width>卷名</th><th width>DP ID</th><th width>Extent ID</th></tr>"
	extentAllocateConflictResultTitle = "<tr><th width>卷名</th><th width>DP ID</th><th width>Extent ID</th>" +
		"<th width>Owner Inodes</th></tr>"
	ekCountOverThresholdResultTitle = "<tr><th width>卷名</th><th width>MP ID</th><th width>Inode ID</th>" +
		"<th width>Extent Count</th></tr>"
)

func formatCheckResultContent(result *proto.NormalEKCheckResult) string {
	return fmt.Sprintf("<tr><td width>%v</td><td width>%v</td><td width>%v</td><td width>%v</td>" +
		"<td width>%v</td><td width>%v</td></tr>", result.VolName, result.ExtInfo.Inode, result.ExtInfo.EK.PartitionId,
		result.ExtInfo.EK.ExtentId, result.ExtInfo.EK.ExtentOffset, result.ExtInfo.EK.Size)
}

func formatCheckFailedResultContent(result *proto.NormalEKCheckFailed) string {
	return fmt.Sprintf("<tr><th width>%v</th><th width>%v</th></tr>", result.VolName, result.FailedInfo)
}

func formatSearchInodeFailedResultContent(volName string, dpID, extentID uint64) string {
	return fmt.Sprintf("<tr><th width>%v</th><th width>%v</th><th width>%v</th></tr>", volName, dpID, extentID)
}

func formatAllocatConflictExtentResultContent(result *proto.NormalEKAllocateConflict) string {
	ownerInodesStr := make([]string, 0, len(result.OwnerInodes))
	for _, inode := range result.OwnerInodes {
		ownerInodesStr = append(ownerInodesStr, fmt.Sprintf("%v", inode))
	}
	return fmt.Sprintf("<tr><th width>%v</th><th width>%v</th><th width>%v</th>" +
		"<th width>%v</th></tr>", result.VolName, result.DataPartitionID, result.ExtentID, ownerInodesStr)
}

func formatEKCountOverThresholdResultContent(result *proto.InodeEKCountRecord) string {
	return fmt.Sprintf("<tr><th width>%v</th><th width>%v</th><th width>%v</th>" +
		"<th width>%v</th></tr>", result.VolName, result.PartitionID, result.InodeID, result.EKCount)
}

type NormalExtentCheckTaskSchedule struct {
	sync.RWMutex
	worker.BaseWorker
	port          string
	masterAddr    map[string][]string
	mcw           map[string]*master.MasterClient
	mcwRWMutex    sync.RWMutex
}

func NewNormalExtentCheckTaskSchedule(cfg *config.Config) (normalExtentCheckTaskSchedule *NormalExtentCheckTaskSchedule, err error) {
	normalExtentCheckTaskSchedule = &NormalExtentCheckTaskSchedule{}
	if err = normalExtentCheckTaskSchedule.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewNormalExtentCheckTaskSchedule] parse config info failed, error(%v)", err)
		return
	}
	if err = normalExtentCheckTaskSchedule.initNormalEKCheckTaskScheduler(); err != nil {
		log.LogErrorf("[NewNormalExtentCheckTaskSchedule] init compact worker failed, error(%v)", err)
		return
	}
	go normalExtentCheckTaskSchedule.UpdateNotifyMembers()
	return
}

func (s *NormalExtentCheckTaskSchedule) parseConfig(cfg *config.Config) (err error) {
	err = s.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	// parse cluster master address
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		masters[clusterName] = addresses
	}
	s.masterAddr = masters
	s.port = s.Port
	return
}

func (s *NormalExtentCheckTaskSchedule) initNormalEKCheckTaskScheduler() (err error) {
	s.WorkerType = proto.WorkerTypeNormalExtentMistakeDelCheck
	s.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	s.mcw = make(map[string]*master.MasterClient)
	for cluster, addresses := range s.masterAddr {
		isDBBack := false
		for _, addr := range addresses {
			if strings.Contains(addr, "cn.chubaofs-seqwrite") || strings.Contains(addr, "dbbak") {
				isDBBack = true
				break
			}
		}
		if isDBBack {
			s.mcw[cluster] = master.NewMasterClientForDbBackCluster(addresses, false)
		} else {
			s.mcw[cluster] = master.NewMasterClient(addresses, false)
		}
	}

	if err = mysql.InitMysqlClient(s.MysqlConfig); err != nil {
		log.LogErrorf("[initBlockTaskScheduler] init mysql client failed, error(%v)", err)
		return
	}
	return
}

func (s *NormalExtentCheckTaskSchedule) GetCreatorDuration() int {
	return s.WorkerConfig.TaskCreatePeriod
}

func (s *NormalExtentCheckTaskSchedule) CreateTask(clusterID string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	s.RLock()
	defer s.RUnlock()

	var (
		mainCheckTask        *proto.MainCheckTask
		notFinishedTaskCount int
		alarmDongdongContent string
		alarmEmailContent    string
		notifyService        *notify.Notify
	)
	//初始化通知服务
	notifyService = notify.NewNotify(s.NotifyConfig)
	notifyService.SetAlarmEmails(s.NotifyMembers.MailTo)
	notifyService.SetAlarmErps(s.NotifyMembers.AlarmERPs)

	mainCheckTask, err = mysql.SelectMainCheckTask(clusterID, int(proto.WorkerTypeNormalExtentMistakeDelCheck))
	if err != nil {
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask select main check task failed, cluster(%v), taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}

	if mainCheckTask == nil || (mainCheckTask.Status == proto.MainCheckTaskStatusFinished && time.Since(mainCheckTask.CreateTime) >= time.Hour * 24) {
		//创建子任务任务
		newTasks, err = s.CreateSubTask(clusterID, runningTasks)
		if err != nil {
			log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask create sub tasks failed, cluster(%v), taskType(%v), err(%v)",
				clusterID, proto.WorkerTypeToName(s.WorkerType), err)
			return
		}

		//记录主任务到数据库
		mainCheckTask = proto.NewMainCheckTask(s.WorkerType, clusterID)
		if _, err = mysql.AddMainCheckTask(mainCheckTask); err != nil {
			log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask add main task failed, cluster(%v), taskType(%v), err(%v)",
				clusterID, proto.WorkerTypeToName(s.WorkerType), err)
			return
		}
		log.LogDebugf("NormalExtentCheckTaskSchedule CreateTask create task finished, cluster(%v), taskType(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType))
		return
	}

	if mainCheckTask.Status == proto.MainCheckTaskStatusFinished && time.Since(mainCheckTask.CreateTime) < time.Hour * 24 {
		//完成，距离上次创建未超过24H
		log.LogDebugf("NormalExtentCheckTaskSchedule CreateMainTask main task less than 24H since the last creation, cluster(%v), taskType(%v)",
			clusterID, proto.WorkerTypeToName(proto.WorkerTypeNormalExtentMistakeDelCheck))
		return
	}

	//未完成,计算未完成的个数
	notFinishedTaskCount, err = mysql.SelectNotFinishedTaskCount(s.WorkerType, clusterID)
	if err != nil {
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask getNotFinishedTaskCount failed, cluster(%v) taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}

	if notFinishedTaskCount != 0 {
		if time.Since(mainCheckTask.CreateTime) > time.Hour * 24 {
			//有超过24小时未完成的子任务
			notifyService.AlarmToDongdong("误删除EK检查任务", fmt.Sprintf("集群%v误删除EK检查任务超过24H未完成", clusterID), "")
			log.LogCriticalf("NormalExtentCheckTaskSchedule CreateTask sub tasks not finished more than 24H," +
				" clusterID(%v) taskType(%v), count(%v)", clusterID, s.WorkerType, notFinishedTaskCount)
		}
		log.LogDebugf("NormalExtentCheckTaskSchedule CreateTask sub task not finished, cluster(%v), taskType(%v), count(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), notFinishedTaskCount)
		return
	}

	log.LogDebugf("NormalExtentCheckTaskSchedule CreateTask latest main task finished at %s, cluster(%v), taskType(%v)",
		time.Now().Format(proto.TimeFormat), clusterID, proto.WorkerTypeToName(s.WorkerType))

	//子任务完成，发送邮件通知
	if alarmDongdongContent, alarmEmailContent, err = s.formatCheckResult(clusterID); err != nil {
		notifyService.AlarmToDongdong("误删除EK检查任务", fmt.Sprintf("集群%v误删除EK检查完成，格式化检查结果失败", clusterID), "")
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask format check result failed, cluster(%v) taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}
	notifyService.AlarmToDongdong("误删除EK检查任务", alarmDongdongContent, "")

	if alarmEmailContent != "" {
		notifyService.AlarmToEmailWithHtmlContent("误删除EK检查结果通知", alarmEmailContent)
	}

	if err = mysql.UpdateAlreadyNotifyNormalEKCheckResult(clusterID); err != nil {
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask update already notify check result status failed, cluster(%v) taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}

	if err = mysql.CleanNormalEKCheckResult(clusterID); err != nil {
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask clean check result failed, cluster(%v) taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}

	if err = mysql.UpdateMainCheckTaskStatusToFinished(mainCheckTask.TaskId); err != nil {
		log.LogErrorf("NormalExtentCheckTaskSchedule CreateTask update main task to finished failed, cluster(%v) taskType(%v), err(%v)",
			clusterID, proto.WorkerTypeToName(s.WorkerType), err)
		return
	}
	return
}

func (s *NormalExtentCheckTaskSchedule) CreateSubTask(clusterID string, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	_, ok := s.mcw[clusterID]
	if !ok {
		log.LogInfof("NormalExtentCheckTaskSchedule CreateSubTask:cluster %s not exist", clusterID)
		return
	}
	masterClient := s.mcw[clusterID]

	var checkRules []*proto.CheckRule
	checkRules, err = mysql.SelectCheckRule(int(s.WorkerType), clusterID)
	if err != nil {
		return
	}
	ruleMap := make(map[string]string, len(checkRules))
	for _, rule := range checkRules {
		ruleMap[rule.RuleType] = rule.RuleValue
	}

	var needCheckVols []string
	checkAll, checkVolumes, skipVolumes := common.ParseCheckAllRules(ruleMap)
	if checkAll {
		var vols []*proto.VolInfo
		vols, err = masterClient.AdminAPI().ListVols("")
		if err != nil {
			return
		}
		for _, vol := range vols {
			if _, ok = skipVolumes[vol.Name]; ok {
				continue
			}
			needCheckVols = append(needCheckVols, vol.Name)
		}
	} else {
		for volName := range checkVolumes {
			needCheckVols = append(needCheckVols, volName)
		}
	}

	for _, volName := range needCheckVols {
		newTask := proto.NewDataTask(proto.WorkerTypeNormalExtentMistakeDelCheck, clusterID, volName, 0, 0, "")
		if alreadyExist, _, _ := s.ContainTask(newTask, runningTasks); alreadyExist {
			log.LogInfof("NormalExtentCheckTaskSchedule CreateSubTask cluster(%v) volume(%v) task(%v) already exist",
				clusterID, volName, newTask)
			continue
		}

		var taskId uint64
		if taskId, err = s.AddTask(newTask); err != nil {
			log.LogErrorf("NormalExtentCheckTaskSchedule CreateSubTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				clusterID, volName, newTask, err)
			continue
		}

		newTask.TaskId = taskId
		newTasks = append(newTasks, newTask)
	}
	return
}

func (s *NormalExtentCheckTaskSchedule) formatCheckResult(clusterID string) (alarmDongDongContent, emailContent string, err error) {
	var (
		checkResult                    []*proto.NormalEKCheckResult
		checkFailedResult              []*proto.NormalEKCheckFailed
		normalEKSearchFailedResult     []*proto.NormalEKOwnerInodeSearchFailedResult
		normalEKAllocateConflictResult []*proto.NormalEKAllocateConflict
		ekCountOverThresholdResult     []*proto.InodeEKCountRecord
	)
	checkResult, err = mysql.SelectNormalEKCheckResult(clusterID)
	if err != nil {
		return
	}

	checkFailedResult, err = mysql.SelectNormalEkCheckFailedResult(clusterID)
	if err != nil {
		return
	}

	normalEKSearchFailedResult, err = mysql.SelectNormalEKSearchFailedResult(clusterID)
	if err != nil {
		return
	}

	normalEKAllocateConflictResult, err = mysql.SelectNormalEKAllocateConflict(clusterID)
	if err != nil {
		return
	}

	ekCountOverThresholdResult, err = mysql.SelectEKCountOverThresholdResult(clusterID)
	if err != nil {
		return
	}

	//alarm dongdong, check finished
	if len(checkResult) == 0 && len(checkFailedResult) == 0 && len(normalEKSearchFailedResult) == 0 &&
		len(normalEKAllocateConflictResult) == 0 && len(ekCountOverThresholdResult) == 0 {
		msg := fmt.Sprintf("集群%v误删除EK检查完成，所有卷都正常", clusterID)
		alarmDongDongContent = msg
		emailContent = msg
		return
	}

	alarmDongDongContent = fmt.Sprintf("集群%v误删除EK检查完成，存在异常卷，请查看邮件通知", clusterID)
	emailContent = fmt.Sprintf("集群%v误删除EK检查结果:<br>", clusterID)
	if len(checkResult) != 0 {
		emailContent += fmt.Sprintf("异常Extents结果:<br>")
		strBuilder := strings.Builder{}
		for _, r := range checkResult {
			strBuilder.WriteString(formatCheckResultContent(r))
		}
		emailContent += fmt.Sprintf("<table border rules=all>%s%s</table>", checkResultTitle, strBuilder.String())
		emailContent += "<br>"
	}

	if len(normalEKSearchFailedResult) != 0 {
		emailContent += fmt.Sprintf("Extent查找InodeID失败结果:<br>")
		strBuilder := strings.Builder{}
		for _, r := range normalEKSearchFailedResult {
			strBuilder.WriteString(formatSearchInodeFailedResultContent(r.VolName, r.DataPartitionID, r.ExtentID))
		}
		emailContent += fmt.Sprintf("<table border rules=all>%s%s</table>", searchInodeFailedResultTitle, strBuilder.String())
		emailContent += "<br>"
	}

	if len(checkFailedResult) != 0 {
		emailContent += fmt.Sprintf("误删除EK检查失败的卷:<br>")
		strBuilder := strings.Builder{}
		for _, r := range checkFailedResult {
			strBuilder.WriteString(formatCheckFailedResultContent(r))
		}
		emailContent += fmt.Sprintf("<table border rules=all>%s%s</table>", checkFailedResultTitle, strBuilder.String())
		emailContent += "<br>"
	}

	if len(normalEKAllocateConflictResult) != 0 {
		emailContent += fmt.Sprintf("重复分配的Extents:<br>")
		strBuilder := strings.Builder{}
		for _, r := range normalEKAllocateConflictResult {
			strBuilder.WriteString(formatAllocatConflictExtentResultContent(r))
		}
		emailContent += fmt.Sprintf("<table border rules=all>%s%s</table>", extentAllocateConflictResultTitle, strBuilder.String())
		emailContent += "<br>"
	}

	if len(ekCountOverThresholdResult) != 0 {
		emailContent += fmt.Sprintf("EK个数超过阈值的Inode:<br>")
		strBuilder := strings.Builder{}
		for _, r := range ekCountOverThresholdResult {
			strBuilder.WriteString(formatEKCountOverThresholdResultContent(r))
		}
		emailContent += fmt.Sprintf("<table border rules=all>%s%s</table>", ekCountOverThresholdResultTitle, strBuilder.String())
		emailContent += "<br>"
	}
	return
}