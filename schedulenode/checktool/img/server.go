package img

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	cfgKeyDomains                    = "imgDomains"
	cfgKeyInterval                   = "interval"
	cfgKeyCount                      = "count"
	cfgKeyMinRwVolCount              = "imgMinRWVolCount"
	cfgKeyMinAvailOfAllVols          = "imgAvailOfAllVolsTB"
	domainSeparator                  = ","
	UMPImageNodesAliveKey            = checktool.UmpKeyStorageBotPrefix + "image"
	UMPImageLbKey                    = checktool.UmpKeyStorageBotPrefix + "image.lb.check"
	UMPImageLbCriticalKey            = checktool.UmpKeyStorageBotPrefix + "image.lb.critical.check"
	UMPImageLbOtherDomainKey         = checktool.UmpKeyStorageBotPrefix + "image.lb.other.domain"
	UMPImageLbOtherDomainCriticalKey = checktool.UmpKeyStorageBotPrefix + "image.lb.other.domain.critical"
	UMPImageDiskSpaceKey             = checktool.UmpKeyStorageBotPrefix + "image.disk"
	UMPImageRwVolsKey                = checktool.UmpKeyStorageBotPrefix + "image.rw.vols"
	UMPMCAImageNodesAliveKey         = checktool.UmpKeyStorageBotPrefix + "MCAImage"
	UMPMCAImageDiskSpaceKey          = checktool.UmpKeyStorageBotPrefix + "MCAImage.disk"
	cfgKeyLbDownloadJsonPath         = "imgLbDownloadJsonPath"
	cfgKeyMCAImageJsonPath           = "MCAImageConfigPath"
	cfgKeyMCAImageMaster             = "master"
	cfgKeyMCAImageClusterID          = "ClusterID"
	cfgKeyMCAImageMinRWVolCount      = "minRWVolCount"
)

type MCAImgInfo struct {
	master        []string
	clusterID     string
	minRWVolCount int
	er            *ExceptionRequest
}

type VolStoreMonitor struct {
	hosts               []string
	minRWVolCount       int
	minAvailOfAllVolsTB float64
	inactiveNodes       int
	scheduleInterval    int
	lbDownload          *LbDownload
	er                  *ExceptionRequest
	mcaImg              *MCAImgInfo
}

func NewVolStoreMonitor() *VolStoreMonitor {
	return &VolStoreMonitor{
		mcaImg: &MCAImgInfo{
			er: &ExceptionRequest{},
		},
	}
}

func (s *VolStoreMonitor) Start(cfg *config.Config) (err error) {
	if cfg.GetString(config.CfgRegion) == config.IDRegion {
		return
	}
	err = s.parseConfig(cfg)
	if err != nil {
		return
	}
	s.er = &ExceptionRequest{}
	go s.schedule()
	go s.scheduleToCheckVols()
	go s.scheduleToCheckLb()
	go s.scheduleToCheckDiskSpace()
	go s.scheduleToMCAImage()

	fmt.Println("starting VolStoreMonitor finished")
	return
}

func (s *VolStoreMonitor) parseConfig(cfg *config.Config) (err error) {
	if err = s.extractCheckVolInfo(cfg); err != nil {
		return
	}
	countStr := cfg.GetString(cfgKeyCount)
	if countStr == "" {
		return fmt.Errorf("parse count failed,count can't be nil")
	}
	if s.inactiveNodes, err = strconv.Atoi(countStr); err != nil {
		return err
	}
	domains := cfg.GetString(cfgKeyDomains)
	if domains == "" {
		return fmt.Errorf("parse imgDomains failed,imgDomains can't be nil")
	}
	s.hosts = strings.Split(domains, domainSeparator)
	interval := cfg.GetString(cfgKeyInterval)
	if interval == "" {
		return fmt.Errorf("parse interval failed,interval can't be nil")
	}

	if s.scheduleInterval, err = strconv.Atoi(interval); err != nil {
		return err
	}
	lbDownloadJsonPath := cfg.GetString(cfgKeyLbDownloadJsonPath)
	if err = s.extractLbDownloadInfo(lbDownloadJsonPath); err != nil {
		return fmt.Errorf("parse lbDownloadJsonPath failed,lbDownloadJsonPath can't be nil err:%v", err)
	}
	MCAImageJsonPath := cfg.GetString(cfgKeyMCAImageJsonPath)
	if err = s.extractMCAImgInfo(MCAImageJsonPath); err != nil {
		return fmt.Errorf("extractMCAImgInfo failed,err:%v", err)
	}
	fmt.Printf("count[%v],domains[%v],scheduleInterval[%v]\n", s.inactiveNodes, s.hosts, s.scheduleInterval)
	fmt.Printf("lbDownload:%v\n", s.lbDownload.LbInfo())
	return
}

func (s *VolStoreMonitor) extractMCAImgInfo(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	masterStr := cfg.GetString(cfgKeyMCAImageMaster)
	if masterStr == "" {
		return fmt.Errorf("parse MCAImg master failed")
	}
	s.mcaImg.master = strings.Split(masterStr, domainSeparator)
	s.mcaImg.clusterID = cfg.GetString(cfgKeyMCAImageClusterID)
	if s.mcaImg.clusterID == "" {
		return fmt.Errorf("parse MCAImg clusterID failed")
	}
	minRWVolCountStr := cfg.GetString(cfgKeyMCAImageMinRWVolCount)
	if s.mcaImg.minRWVolCount, err = strconv.Atoi(minRWVolCountStr); err != nil {
		return fmt.Errorf("parse MCAImg minRWVolCount failed")
	}
	fmt.Printf("MCAImgInfo:%v\n", s.mcaImg)
	return
}

func (s *VolStoreMonitor) extractLbDownloadInfo(filePath string) (err error) {
	cfg, _ := config.LoadConfigFile(filePath)
	lbDownload := &LbDownload{}
	if err = json.Unmarshal(cfg.Raw, lbDownload); err != nil {
		return
	}
	s.lbDownload = lbDownload
	if s.lbDownload.ComIpPath == "" {
		return fmt.Errorf("parse imgComIpPath failed,imgComIpPath can't be nil")
	}
	if s.lbDownload.LocalIpPath == "" {
		return fmt.Errorf("parse imgLocalIpPath failed,imgLocalIpPath can't be nil")
	}
	if s.lbDownload.LbPwd == "" {
		return fmt.Errorf("parse imgLbPwd failed,imgLbPwd can't be nil")
	}
	if s.lbDownload.ComConfPath == "" {
		s.lbDownload.ComConfPath = defaultComConfPath
	}
	if s.lbDownload.LocalConfPath == "" {
		s.lbDownload.LocalConfPath = defaultLocalConfPath
	}
	if s.lbDownload.ScheduleInterval < defaultDownloadInterval {
		s.lbDownload.ScheduleInterval = defaultDownloadInterval
	}
	if s.lbDownload.LbNotMatchCnt == 0 {
		s.lbDownload.LbNotMatchCnt = defaultLbNotMatchCount
	}
	return
}

func (s *VolStoreMonitor) extractCheckVolInfo(cfg *config.Config) (err error) {
	minRWVolCountStr := cfg.GetString(cfgKeyMinRwVolCount)
	if minRWVolCountStr == "" {
		return fmt.Errorf("parse %v failed,count can't be nil", cfgKeyMinRwVolCount)
	}
	if s.minRWVolCount, err = strconv.Atoi(minRWVolCountStr); err != nil {
		return err
	}
	if s.minRWVolCount < 5 {
		return fmt.Errorf("parse %v failed,can't less than 5", cfgKeyMinRwVolCount)
	}
	s.minAvailOfAllVolsTB = cfg.GetFloat(cfgKeyMinAvailOfAllVols)
	if s.minAvailOfAllVolsTB < 0 {
		fmt.Printf("parse %v failed, should be more than 0\n", cfgKeyMinAvailOfAllVols)
	}
	fmt.Printf("image extractCheckVolInfo minRWVolCount:%v,minAvailOfAllVolsTB:%v\n", s.minRWVolCount, s.minAvailOfAllVolsTB)
	return
}

func (s *VolStoreMonitor) schedule() {

	for {
		for _, host := range s.hosts {
			log.LogInfof("cluster[ds_image] checkNodesAlive [%v] begin", host)
			startTime := time.Now()
			s.checkNodeAlive("ds_image", host, UMPImageNodesAliveKey)
			log.LogInfof("cluster[ds_image] checkNodesAlive [%v] end,cost[%v]", host, time.Since(startTime))
		}
		time.Sleep(time.Duration(s.scheduleInterval) * time.Second)
	}
}

func (s *VolStoreMonitor) doRequest(clusterId, host, action string) (data []byte, err error) {
	var resp *http.Response
	url := fmt.Sprintf("http://%v/admin/getcluster?cluster=%v", host, clusterId)
	if resp, err = http.Get(url); err != nil {
		log.LogErrorf("action[%v] host[%v],err[%v]", action, host, err)
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[%v] host[%v],err[%v]", action, host, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("action[%v] host[%v],resp.Status[%v],body[%v]",
			action, host, resp.Status, string(data))
		log.LogError(msg)
		err = fmt.Errorf(msg)
		return
	}
	return
}

func (s *VolStoreMonitor) checkNodeAlive(clusterId, host, umpKey string) {
	var (
		data []byte
		err  error
	)
	defer func() {
		if err != nil {
			log.LogFlush()
		}
	}()
	data, err = s.doRequest(clusterId, host, "checkNodeAlive")
	if err != nil {
		msg := fmt.Sprintf("action[checkNodeAlive],get cluster[%v] infomation from master[%v] occurred err[%v]", clusterId, host, err)
		s.doProcessAlarm(msg, clusterId, umpKey)
		return
	}
	//fmt.Println(string(msg))
	view := &ClusterTopologyView{}

	if err = json.Unmarshal(data, view); err != nil {
		log.LogErrorf("action[checkNodeAlive] clusterId[%v] host[%v],data[%v],err[%v]", clusterId, s.hosts[0], string(data), err)
		return
	}
	var count int
	for _, zone := range view.Topology.ZonesMap {
		for _, node := range zone.Nodes {
			if node.IsActive == false {
				count++
			}
		}
	}

	if count > s.inactiveNodes {
		checktool.WarnBySpecialUmpKey(umpKey, fmt.Sprintf("%v has %v inactive nodes", host, count))
	} else {
		log.LogInfof("clusterId[%v] %v has %v inactive nodes", clusterId, host, count)
	}

	if count > 4 {
		for _, zone := range view.Topology.ZonesMap {
			for _, node := range zone.Nodes {
				if node.IsActive == false {
					s.resetNodeReportTime(host, node.TcpAddr, node.IsActive)
				}
			}
		}
	}
}

func (s *VolStoreMonitor) resetNodeReportTime(host, node string, isActive bool) {
	url := fmt.Sprintf("http://%v/node/setStatus?cluster=ds_image&node=%v&isActive=%v", host, node, isActive)
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("action[resetNodeReportTime] host[%v],err[%v]", host, err)
		return
	}
	if resp.Body != nil {
		resp.Body.Close()
	}
}

func (s *VolStoreMonitor) doProcessAlarm(msg, clusterId, umpKey string) {
	needAlarm := false
	switch clusterId {
	case "ds_image":
		needAlarm = doErrCountAlarm(s.er)
	case "MCA_image":
		needAlarm = doErrCountAlarm(s.mcaImg.er)
	default:
		log.LogErrorf("unknown clusterID(%v)", clusterId)
	}
	if needAlarm {
		checktool.WarnBySpecialUmpKey(umpKey, msg)
	}
	return
}

func doErrCountAlarm(er *ExceptionRequest) (needAlarm bool) {
	er.count++
	inOneCycle := time.Now().Unix()-er.lastTime < checktool.DefaultWarnInternal
	if er.count >= checktool.DefaultMinCount && inOneCycle {
		needAlarm = true
		er.count = 0
	}
	if !inOneCycle {
		er.count = 1
		er.lastTime = time.Now().Unix()
	}
	return
}

func (s *VolStoreMonitor) scheduleToMCAImage() {
	go s.mcaImageCheckNodeAlive()
	go s.mcaImageCheckDiskSpace()
}

func (s *VolStoreMonitor) mcaImageCheckNodeAlive() {
	for {
		for _, host := range s.mcaImg.master {
			log.LogInfof("cluster[%v] checkNodesAlive [%v] begin", s.mcaImg.clusterID, host)
			startTime := time.Now()
			s.checkNodeAlive(s.mcaImg.clusterID, host, UMPMCAImageNodesAliveKey)
			log.LogInfof("cluster[%v] checkNodesAlive [%v] end,cost[%v]", s.mcaImg.clusterID, host, time.Since(startTime))
		}
		time.Sleep(time.Duration(s.scheduleInterval) * time.Second)
	}
}

func (s *VolStoreMonitor) mcaImageCheckDiskSpace() {
	for {
		for _, host := range s.mcaImg.master {
			log.LogInfof("clusterID[%v] checkDiskSpace [%v] begin", s.mcaImg.clusterID, host)
			startTime := time.Now()
			s.checkDiskSpace(s.mcaImg.clusterID, host, UMPMCAImageDiskSpaceKey)
			log.LogInfof("clusterID[%v] checkDiskSpace [%v] end,cost[%v]", s.mcaImg.clusterID, host, time.Since(startTime))
		}
		time.Sleep(time.Duration(2) * time.Hour)
	}
}
