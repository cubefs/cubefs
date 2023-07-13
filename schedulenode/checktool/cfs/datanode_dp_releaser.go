package cfs

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	cfgKeyReleaserDomainsPath = "cfsReleaserDomainsJsonPath"
)

type ChubaoFSDPReleaser struct {
	Hosts                []*ClusterReleaserHost
	ScheduleIntervalHour int  `json:"intervalHour"`
	IsEnable             bool `json:"isEnable"` // 是否开启
}

type ClusterReleaserHost struct {
	host             *ClusterHost
	Host             string `json:"host"`
	DataNodeHttpPort string `json:"dataNodeHttpPort"`
	IsEnable         bool   `json:"isEnable"`
	TimeLocation     string `json:"timeLocation"`
}

func StartChubaoFSDPReleaser(cfg *config.Config) (s *ChubaoFSDPReleaser) {
	s = &ChubaoFSDPReleaser{
		Hosts: make([]*ClusterReleaserHost, 0),
	}
	registerChubaoFSDPReleaserServer(s)
	err := s.parseConfig(cfg)
	if err != nil {
		log.LogWarnf("action[StartChubaoFSDPReleaser] err:%v", err)
		return
	}
	s.scheduleTask()
	fmt.Println("StartChubaoFSDPReleaser finished")
	return
}

func (s *ChubaoFSDPReleaser) scheduleTask() {
	go s.scheduleToReleaseDataNodeDp()
}

func (s *ChubaoFSDPReleaser) parseConfig(cfg *config.Config) (err error) {
	if err = s.extractCFSReleaserDomains(cfg); err != nil {
		return fmt.Errorf("parse cfgKeyReleaserDomainsPath failed,cfgKeyReleaserDomainsPath can't be nil err:%v", err)
	}
	bytes, _ := json.Marshal(s)
	fmt.Printf("ChubaoFSDPReleaser:%s\n", string(bytes))
	return
}

func (s *ChubaoFSDPReleaser) extractCFSReleaserDomains(cfg *config.Config) (err error) {
	cfgReleaserDomainsPath := cfg.GetString(cfgKeyReleaserDomainsPath)
	if cfgReleaserDomainsPath == "" {
		return fmt.Errorf("cfgReleaserDomainsPath is empty")
	}
	cfgReleaserDomain, _ := config.LoadConfigFile(cfgReleaserDomainsPath)
	type ChubaoFSDPReleaserDetail struct {
		IntervalHour int  `json:"intervalHour"`
		IsEnable     bool `json:"isEnable"`
		CfsDomains   []struct {
			Host             string `json:"host"`
			IsEnable         bool   `json:"isEnable"`
			DataNodeHTTPPort string `json:"dataNodeHttpPort"`
			TimeLocation     string `json:"timeLocation"`
		} `json:"cfsDomains"`
	}

	detail := ChubaoFSDPReleaserDetail{}
	if err = json.Unmarshal(cfgReleaserDomain.Raw, &detail); err != nil {
		return fmt.Errorf("extractCFSReleaserDomains cfgReleaserDomain:%v err:%v", string(cfgReleaserDomain.Raw), err)
	}
	s.IsEnable = detail.IsEnable
	s.ScheduleIntervalHour = detail.IntervalHour
	if s.ScheduleIntervalHour < 4 {
		s.ScheduleIntervalHour = 4
	}

	clusterHosts := make([]*ClusterReleaserHost, 0)
	for _, cfsDomain := range detail.CfsDomains {
		clusterHosts = append(clusterHosts, &ClusterReleaserHost{
			host:             newClusterHost(cfsDomain.Host),
			Host:             cfsDomain.Host,
			DataNodeHttpPort: cfsDomain.DataNodeHTTPPort,
			IsEnable:         cfsDomain.IsEnable,
			TimeLocation:     cfsDomain.TimeLocation,
		})
	}

	s.Hosts = clusterHosts
	return
}

func (s *ChubaoFSDPReleaser) scheduleToReleaseDataNodeDp() {
	if s.ScheduleIntervalHour < 4 {
		s.ScheduleIntervalHour = 4
	}
	s.releaseDataNodeDp()
	timer := time.NewTimer(time.Duration(s.ScheduleIntervalHour) * time.Hour)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if !s.IsEnable {
				log.LogWarn("action[scheduleToReleaseDataNodeDp] IsEnable false so stop")
				continue
			}
			s.releaseDataNodeDp()
			timer.Reset(time.Duration(s.ScheduleIntervalHour) * time.Hour)
		}
	}
}

func (s *ChubaoFSDPReleaser) releaseDataNodeDp() {
	defer checktool.HandleCrash()
	wg := new(sync.WaitGroup)
	for _, h := range s.Hosts {
		wg.Add(1)
		go func(host *ClusterReleaserHost) {
			defer wg.Done()
			if !host.IsEnable {
				log.LogWarnf("action[releaseDataNodeDp] host:%v IsEnable false so stop", host.host)
				return
			}
			log.LogInfof("releaseDataNodeDp [%v] begin", host.host)
			startTime := time.Now()
			cv, err := getCluster(host.host)
			if err != nil {
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v ", host.host, err)
				log.LogWarn(msg)
				return
			}
			host.releaseDataNodeDp(cv)
			log.LogInfof("releaseDataNodeDp [%v] end,cost[%v]", host.host, time.Since(startTime))
		}(h)
	}
	wg.Wait()
	log.LogInfof("releaseDataNodeDp all hosts are finished")
}

func (ch *ClusterReleaserHost) releaseDataNodeDp(cv *ClusterView) {
	if ch.DataNodeHttpPort == "" {
		log.LogWarnf("action[releaseDataNodeDp] host:%v DataNodeHttpPort is empty so return", ch.host)
		return
	}
	successCount := 0
	for _, dn := range cv.DataNodes {
		if dn.Status != false {
			split := strings.Split(dn.Addr, ":")
			if len(split) != 2 {
				continue
			}
			dataNodeHttpAddr := fmt.Sprintf("%s:%s", split[0], ch.DataNodeHttpPort)
			if err := doReleaseDataNodePartitions(dataNodeHttpAddr, ch.TimeLocation); err != nil {
				log.LogError(fmt.Sprintf("action[releaseDataNodeDp] err:%v", err))
				continue
			}
			successCount++
		}
	}
	msg := fmt.Sprintf("action[releaseDataNodeDp] host[%v] DataNodeCount[%v] successCount[%v]", ch.host, len(cv.DataNodes), successCount)
	log.LogWarn(msg)
}

func doReleaseDataNodePartitions(dataNodeHttpAddr, timeLocation string) (err error) {
	var (
		data   []byte
		reqURL string
		key    string
	)
	if timeLocation == "" {
		key = generateAuthKey()
	} else {
		key = generateAuthKeyWithTimeZone(timeLocation)
	}
	reqURL = fmt.Sprintf("http://%v/releasePartitions?key=%s", dataNodeHttpAddr, key)
	data, err = doRequest(reqURL, false)
	if err != nil {
		return fmt.Errorf("url[%v],err %v resp[%v]", reqURL, err, string(data))
	}
	log.LogInfo(fmt.Sprintf("action[doReleaseDataNodePartitions] url[%v] resp[%v]", reqURL, string(data)))
	return
}

func (s *ChubaoFSDPReleaser) getChubaoFSDPReleaser(w http.ResponseWriter, r *http.Request) {
	BuildSuccessResp(w, s)
}

func (s *ChubaoFSDPReleaser) setChubaoFSDPReleaser(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		hostStr string
		enable  bool
	)
	if err = r.ParseForm(); err != nil {
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if hostStr = r.FormValue("host"); hostStr == "" {
		BuildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("key host not found"))
		return
	}
	if enable, err = parseEnable(r); err != nil {
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.ToLower(hostStr) == "all" {
		s.IsEnable = enable
	} else {
		for _, host := range s.Hosts {
			if strings.ToLower(hostStr) == host.host.String() {
				host.IsEnable = enable
			}
		}
	}
	BuildSuccessResp(w, s)
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func generateAuthKeyWithTimeZone(timeLocation string) string {
	var t time.Time
	if timeLocation == "" {
		t = time.Now()
	} else {
		l, _ := time.LoadLocation(timeLocation)
		t = time.Now().In(l)
	}
	date := t.Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}
