package version

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/iputil"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	clientId   string
	cluster    string
	reportAddr string
)

var (
	ConfigKeyReportAddr = "reportVersionAddr"
)

const (
	DefaultReportAddr = "http://jfs.report.jd.local/version/report"
)

func ReportVersionSchedule(cfg *config.Config, masterAddr []string, version, volName string, stopC chan struct{}, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()
	reportAddr = cfg.GetString(ConfigKeyReportAddr)
	if reportAddr == "" {
		reportAddr = DefaultReportAddr
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-stopC:
			return
		case <-timer.C:
			err := reportVersion(cfg, masterAddr, version, volName)
			if err != nil {
				log.LogErrorf("[reportVersionSchedule] report version failed, errorInfo(%v)", err)
			}
			timer.Reset(24 * time.Hour)
		}
	}
}

func reportVersion(cfg *config.Config, masterAddr []string, version, volName string) (err error) {
	var (
		localIp string
	)

	// get cluster info
	if cluster == "" {
		cluster = getCluster(cfg, masterAddr)
	}

	// compute client id
	if localIp == "" || localIp == "unknown" {
		localIp, err = iputil.GetLocalIPByDial()
		if err != nil || localIp == "" {
			localIp = "unknown"
			log.LogErrorf("[reportVersion] get local ip failed, errorInfo(%v)", err)
		}
	}
	timestamp := time.Now().Unix()
	clientId = fmt.Sprintf("%s@%d", localIp, timestamp)

	versionInfo := &proto.VersionInfo{
		ClientId: clientId,
		Version:  version,
		ZkAddr:   cluster,
		VolName:  volName,
	}
	data, err := json.Marshal(versionInfo)
	if err != nil {
		return
	}

	client := &http.Client{}
	req, err := http.NewRequest("PUT", reportAddr, strings.NewReader(string(data)))
	if err != nil {
		log.LogErrorf("[reportVersion] create request failed, errorInfo(%v)", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.LogErrorf("[reportVersion] execute request failed, errorInfo(%v)", err)
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.LogErrorf("[reportVersion] StatusCode(%v), errorInfo(%v)", resp.StatusCode, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("[reportVersion]: report version failed, statusCode(%v) body(%s).",
			resp.StatusCode, strings.Replace(string(respData), "\n", "", -1))
		return
	}
	log.LogInfof("[reportVersion] report version success, respData(%v)", string(respData))
	return
}

const (
	ClusterName = "clusterName"
)

func getCluster(cfg *config.Config, masterAddr []string) string {
	cluster := cfg.GetString(ClusterName)
	if cluster == "" {
		cluster = strings.Join(masterAddr, ",")
	}
	return cluster
}
