package img

import (
	"bufio"
	"fmt"
	"github.com/ThomasRooney/gexpect"
	"github.com/cubefs/cubefs/schedulenode/common/jdos"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultComConfPath      = "/export/servers/nginx/conf/conf.d/servers/com"
	defaultLocalConfPath    = "/export/servers/nginx/conf/conf.d/servers/local/"
	defaultDownloadInterval = 60 * 10
	defaultLbNotMatchCount  = 2
)

const (
	LbTypeCom   = "download_com"
	LbTypeLocal = "download_local"
)

func (s *VolStoreMonitor) scheduleToCheckLb() {
	ticker := time.NewTicker(time.Duration(s.lbDownload.ScheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.checkDownloadLb()
	for {
		select {
		case <-ticker.C:
			s.checkDownloadLb()
		}
	}
}

func (s *VolStoreMonitor) checkDownloadLb() {
	s.checkDownloadLbConfToPodIP()
}

// COM/LOCAL分别单独考虑 具体逻辑相同
// JDOS 获取所有PODIP--内存里面 map[ip] = bool // bool 用于最后判断是否被匹配过
func (s *VolStoreMonitor) checkDownloadLbConfToPodIP() {
	// 根据分组的标识 分别得到 COM/LOCAL分组
	comGroups, localGroups, err := s.getAllGroupsFromApp()
	if err != nil {
		log.LogErrorf("action[checkDownloadLbConfToPodIP] err:%v", err)
		return
	}

	for _, group := range comGroups {
		log.LogDebugf("comGroups:%+v\n", *group)
	}
	for _, group := range localGroups {
		log.LogDebugf("localGroups:%+v\n", *group)
	}

	dir, err := os.Getwd()
	if err != nil {
		log.LogErrorf("get current dir failed err:%v", err)
		return
	}
	basePath := fmt.Sprintf("%v/image_lb_conf", dir)
	if err = removeAllOldConfFile(basePath); err != nil {
		log.LogErrorf("remove all old conf file failed err:%v", err)
		return
	}
	// 分别获取pod map , 需要确保获取所有的集合
	comPods, err := s.getAllPodsFromAppGroups(comGroups, LbTypeCom)
	if err != nil {
		log.LogErrorf("action[checkDownloadLbConfToPodIP] err:%v", err)
		return
	}
	localPods, err := s.getAllPodsFromAppGroups(localGroups, LbTypeLocal)
	if err != nil {
		log.LogErrorf("action[checkDownloadLbConfToPodIP] err:%v", err)
		return
	}
	go s.checkGroupPods(comPods, localPods, basePath, LbTypeCom, s.lbDownload.ComIpPath, s.lbDownload.ComConfPath)
	go s.checkGroupPods(localPods, comPods, basePath, LbTypeLocal, s.lbDownload.LocalIpPath, s.lbDownload.LocalConfPath)
}

func (s *VolStoreMonitor) getAllGroupsFromApp() (comGroups, localGroups []*AppGroups, err error) {
	comGroups = make([]*AppGroups, 0)
	localGroups = make([]*AppGroups, 0)
	for _, systemApp := range s.lbDownload.SystemAppInfo {
		comGroupNames := make([]string, 0)
		localGroupNames := make([]string, 0)
		jdosOpenApi := jdos.NewJDOSOpenApi(systemApp.System, systemApp.App, s.lbDownload.JDOS.Site, s.lbDownload.JDOS.Erp, s.lbDownload.JDOS.Token)
		groups, err1 := jdosOpenApi.GetAllGroupsNames()
		if err1 != nil {
			msg := fmt.Sprintf("image lb check get System:%v App:%v from jdos err:%v", systemApp.System, systemApp.App, err1)
			checktool.WarnBySpecialUmpKey(checktool.UmpKeyNormalWarn, msg)
			continue
		}
		for i := 0; i < len(groups); i++ {
			nickName := strings.ToLower(groups[i].Nickname)
			if strings.Contains(nickName, "com") {
				comGroupNames = append(comGroupNames, groups[i].GroupName)
			} else if strings.Contains(nickName, "local") {
				localGroupNames = append(localGroupNames, groups[i].GroupName)
			} else if strings.Contains(nickName, "test") {
				log.LogDebugf("test group nickName:%v,GroupName:%v", nickName, groups[i].GroupName)
				continue
			} else {
				log.LogDebugf("group may be not local and com, nickName:%v,GroupName:%v", nickName, groups[i].GroupName)
			}
		}
		comGroup := &AppGroups{systemApp: systemApp, groupNames: comGroupNames}
		localGroup := &AppGroups{systemApp: systemApp, groupNames: localGroupNames}
		comGroups = append(comGroups, comGroup)
		localGroups = append(localGroups, localGroup)
	}
	return
}

func removeAllOldConfFile(path string) (err error) {
	if strings.Contains(path, "*") {
		return fmt.Errorf("path:%v should not contine *", path)
	}
	cmd := exec.Command(`rm`, "-rf", path)
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("cmd.Run() failed err:%v", err)
	}
	return
}

func (s *VolStoreMonitor) getAllPodsFromAppGroups(appGroups []*AppGroups, lbType string) (pods *sync.Map, err error) {
	pods = new(sync.Map)
	podCount := 0
	for _, appGroup := range appGroups {
		jdosOpenApi := jdos.NewJDOSOpenApi(appGroup.systemApp.System, appGroup.systemApp.App, s.lbDownload.JDOS.Site, s.lbDownload.JDOS.Erp, s.lbDownload.JDOS.Token)
		for _, groupName := range appGroup.groupNames {
			groupAllPodIPs, err1 := jdosOpenApi.GetGroupAllPods(groupName)
			if err1 != nil {
				err = fmt.Errorf("action[getAllPodsFromAppGroups] GetGroupAllPods failed lbType:%v groupName:%v err:%v", lbType, groupName, err1)
				return
			}
			for _, pod := range groupAllPodIPs {
				pods.Store(pod.PodIP, false)
				podCount++
			}
		}
	}
	log.LogInfof("action[getAllPodsFromAppGroups] lbType:%v get pod ok, pods count:%v", lbType, podCount)
	if podCount == 0 {
		return nil, fmt.Errorf("action[getAllPodsFromAppGroups] lbType:%v get pod err, podCount is 0", lbType)
	}
	return
}

func (s *VolStoreMonitor) checkGroupPods(pods, extraPods *sync.Map, basePath, lbType, lbIpFilePath, lbConfPath string) {
	// 机器挨个进行匹配 根据LB机器列表 对每个机器下面的 每个正常的 配置文件 进行匹配
	scpConfFailedCount := 0
	lbIPs := getAllLbIPs(lbIpFilePath)
	localPath := fmt.Sprintf("%v/%v", basePath, lbType)
	for _, lbIP := range lbIPs {
		//对于每个机器 将文件拷贝到目录下  + 时间 + com/local + ip/
		dirname := fmt.Sprintf("%v/%v", localPath, lbIP)
		err := s.doScp(lbIP, lbConfPath, dirname, false)
		if err != nil {
			log.LogInfof("action[checkGroupPods] type:%v lbIP:%v err:%v will try send yes cmd", lbType, lbIP, err)
			if err = s.doScp(lbIP, lbConfPath, dirname, true); err != nil {
				scpConfFailedCount++
				log.LogWarnf("action[checkGroupPods] type:%v lbIP:%v err:%v", lbType, lbIP, err)
				continue
			}
		}
		dirList, err := ioutil.ReadDir(dirname)
		if err != nil {
			log.LogWarnf("action[checkGroupPods] type:%v lbIP:%v dirList:%v read dir err:%v", lbType, lbIP, dirList, err)
			continue
		}
		for _, file := range dirList {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".conf") {
				continue
			}
			checkConfFileIps(lbType, basePath, fmt.Sprintf("%v/%v/%v", lbType, lbIP, file.Name()), s.lbDownload.IgnoreFileName, pods, extraPods, s.lbDownload.LbNotMatchCnt)
		}
	}
	//4.COM/LOCAL检测完之后，对于map最后进行一次轮询
	notMatchedIp := make([]string, 0)
	podsCount := 0
	pods.Range(func(k, v interface{}) bool {
		podsCount++
		ip := k.(string)
		value := v.(bool)
		if value == false {
			notMatchedIp = append(notMatchedIp, ip)
		}
		return true
	})
	if len(notMatchedIp) != 0 {
		if podsCount == len(notMatchedIp) || scpConfFailedCount > 0 {
			log.LogInfof("may have err in scp conf file, image lb:%v scpConfFailedCount:%v pod total count:%v,not matched count:%v ips:%v",
				lbType, scpConfFailedCount, podsCount, len(notMatchedIp), notMatchedIp)
			return
		}
		msg := fmt.Sprintf("image lb:%v pod total count:%v,not matched count:%v ips:%v ",
			lbType, podsCount, len(notMatchedIp), notMatchedIp)
		if len(notMatchedIp) >= s.lbDownload.LbNotMatchCnt {
			checktool.WarnBySpecialUmpKey(UMPImageLbCriticalKey, msg)
		}
		log.LogInfof(msg)
		return
	}
	log.LogInfof("image lb:%v pod total count:%v, all of them is in lb conf file", lbType, podsCount)
}

func getAllLbIPs(fileName string) (lbIPs []string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.LogErrorf("action[getAllLbIPs] Open file:%v err:%v", fileName, err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineText := scanner.Text()
		str := strings.TrimSpace(lineText)
		if len(str) == 0 || str[0] == '#' {
			continue
		}
		lbIPs = append(lbIPs, lineText)
	}
	return
}

// scp将远程目录下的conf文件保存到本地
func (s *VolStoreMonitor) doScp(ip, remotePath, localPath string, needYesCMD bool) (err error) {
	pidCh := make(chan map[string]int, 1)
	go func() {
		// 超时控制
		var pidInfo map[string]int
		defer func() {
			if pidInfo != nil && len(pidInfo) == 1 {
				if pid, ok := pidInfo[ip]; !ok || pid == 0 {
					return
				}
				pidFind, err1 := os.FindProcess(pidInfo[ip])
				if err1 != nil {
					return
				}
				pidFind.Kill()
			}
		}()
		for {
			select {
			case pidInfo = <-pidCh:
			case <-time.After(time.Second * 20):
				return
			}
		}
	}()
	if err = scpRemoteConfFilesToLocal(ip, remotePath, localPath, s.lbDownload.LbPwd, pidCh, needYesCMD); err != nil {
		return
	}
	return
}

func scpRemoteConfFilesToLocal(ip, remotePath, localPath, lbPwd string, pidCh chan map[string]int, needYesCMD bool) (err error) {
	if err = os.MkdirAll(localPath, 0766); err != nil {
		err = fmt.Errorf("action[scpRemoteConfFilesToLocal] Mkdir path:%v err:%v", localPath, err)
		return
	}
	var child *gexpect.ExpectSubprocess
	defer func() {
		if child != nil {
			child.Close()
		}
	}()
	cmd := fmt.Sprintf("scp -r root@%v:%v/*.conf %v", ip, remotePath, localPath)
	child, err = gexpect.Spawn(cmd)
	if child != nil {
		pidCh <- map[string]int{ip: child.Cmd.Process.Pid}
	}
	if err != nil {
		err = fmt.Errorf("action[scpRemoteConfFilesToLocal] cmd:%v Spawn cmd err:%v", cmd, err)
		return
	}
	time.Sleep(time.Millisecond * 500)
	if needYesCMD {
		time.Sleep(time.Second * 2)
		// 可能需要 同意 authenticity 的场景 以及 或许是因为 上次超时之后重试
		if err = child.SendLine("yes"); err != nil {
			err = fmt.Errorf("action[scpRemoteConfFilesToLocal] cmd:%v sendLine yes err:%v", cmd, err)
			return
		}
		time.Sleep(time.Second * 3)
	}
	if err = child.SendLine(lbPwd); err != nil {
		err = fmt.Errorf("action[scpRemoteConfFilesToLocal] cmd:%v sendLine password err:%v", cmd, err)
		return
	}
	if err = child.Wait(); err != nil {
		err = fmt.Errorf("action[scpRemoteConfFilesToLocal] cmd:%v wait err:%v", cmd, err)
		return
	}
	return
}

//3.配置文件挨个匹配 对于每个配置文件的数据 每一行每一行处理
func checkConfFileIps(lbType, basePath, fileName, ignoreFileName string, pods, extraPods *sync.Map, lbNotMatchCnt int) {
	file, err := os.Open(fmt.Sprintf("%v/%v", basePath, fileName))
	if err != nil {
		log.LogErrorf("action[checkConfFileIps] open file err:%v", err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	notMatchedFileAndIps := make(map[string][]string, 0)
	otherDomainPodFileAndIps := make(map[string][]string, 0) // 配置文件中的IP非本模块
	for scanner.Scan() {
		lineText := scanner.Text()
		str := strings.TrimSpace(lineText)
		if len(str) == 0 || str[0] == '#' {
			continue
		}
		//  用正则式，匹配获得ip
		reg := regexp.MustCompile("((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}")
		ips := reg.FindStringSubmatch(str)
		if len(ips) == 0 {
			continue
		}
		ipInConfFile := ips[0]
		value, ok := pods.Load(ipInConfFile)
		if !ok {
			// 如果不在，就从另一个中查找, 都不存在再进行告警
			if _, findInExtraPods := extraPods.Load(ipInConfFile); findInExtraPods {
				otherDomainPodFileAndIps[fileName] = append(otherDomainPodFileAndIps[fileName], ipInConfFile)
				continue
			}
			notMatchedIps, ok1 := notMatchedFileAndIps[fileName]
			if !ok1 {
				notMatchedIps = make([]string, 0)
			}
			notMatchedIps = append(notMatchedIps, ipInConfFile)
			notMatchedFileAndIps[fileName] = notMatchedIps
			continue
		}
		if !value.(bool) {
			pods.Store(ips[0], true)
		}
	}
	// 排除指定的特殊文件
	for k, v := range notMatchedFileAndIps {
		if strings.Contains(k, ignoreFileName) {
			log.LogInfof("image lb ignoreFileName:%v path:%v ip:%v", ignoreFileName, k, v)
			delete(notMatchedFileAndIps, k)
		}
	}
	if len(notMatchedFileAndIps) != 0 {
		log.LogErrorf("image lb not matched file count:%v, detail:%v", len(notMatchedFileAndIps), &notMatchedFileAndIps)
		msg := fmt.Sprintf("image lb not matched file count:%v, detail:%v", len(notMatchedFileAndIps), &notMatchedFileAndIps)
		if len(notMatchedFileAndIps) >= lbNotMatchCnt {
			checktool.WarnBySpecialUmpKey(UMPImageLbCriticalKey, msg)
		} else {
			checktool.WarnBySpecialUmpKey(UMPImageLbKey, msg)
		}

	}
	// 排除指定的特殊文件
	for k, v := range otherDomainPodFileAndIps {
		if strings.Contains(k, ignoreFileName) {
			log.LogInfof("image lb ignoreFileName:%v path:%v ip:%v", ignoreFileName, k, v)
			delete(otherDomainPodFileAndIps, k)
		}
	}
	if len(otherDomainPodFileAndIps) != 0 {
		msg := fmt.Sprintf("imageLBType:%v other domain file count:%v, detail:%v", lbType, len(otherDomainPodFileAndIps), &otherDomainPodFileAndIps)
		if len(otherDomainPodFileAndIps) >= lbNotMatchCnt {
			checktool.WarnBySpecialUmpKey(UMPImageLbOtherDomainCriticalKey, msg)
		} else {
			checktool.WarnBySpecialUmpKey(UMPImageLbOtherDomainKey, msg)
		}
	}
}
