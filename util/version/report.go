package version

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/iputil"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var (
	clientId   string
	version    string
	cluster    string
	reportAddr string
)

var (
	ConfigKeyReportAddr = "reportVersionAddr"
)

const (
	DefaultReportAddr = "http://jfs.report.jd.local/version/report"
)

func ReportVersionSchedule(cfg *config.Config, masterAddr []string, versionFunc func() string) {
	reportAddr = cfg.GetString(ConfigKeyReportAddr)
	if reportAddr == "" {
		reportAddr = DefaultReportAddr
	}

	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			err := reportVersion(cfg, masterAddr, versionFunc)
			if err != nil {
				log.LogErrorf("[reportVersionSchedule] report version failed, errorInfo(%v)", err)
			}
			timer.Reset(24 * time.Hour)
		}
	}
}

func reportVersion(cfg *config.Config, masterAddr []string, versionFunc func() string) (err error) {
	// compute client id
	var (
		localIp string
	)
	if localIp == "" || cluster == "" {
		cluster = getCluster(cfg, masterAddr)
		localIp, err = iputil.GetLocalIPByDial()
		if err != nil {
			log.LogErrorf("[reportVersion] get local ip failed, errorInfo(%v)", err)
			return
		}
		if localIp == "" {
			localIp = "unknown"
		}
	}
	timestamp := time.Now().Unix()
	clientId = fmt.Sprintf("%s@%d", localIp, timestamp)

	// compute version
	if version == "" {
		version = versionFunc()
	}

	versionInfo := &proto.VersionInfo{
		ClientId: clientId,
		Version:  version,
		ZkAddr:   cluster,
	}
	data, err := json.Marshal(versionInfo)
	if err != nil {
		return
	}

	client := &http.Client{}
	req, err := http.NewRequest("PUT", reportAddr, bytes.NewBuffer(data))
	if err != nil {
		log.LogErrorf("[reportVersion] create request failed, errorInfo(%v)", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		log.LogErrorf("[reportVersion] execute request failed, errorInfo(%v)", err)
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	log.LogErrorf("[reportVersion] StatusCode(%v) respData(%v)", resp.StatusCode, string(respData))

	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("[reportVersion]: report version failed, statusCode(%v) body(%s).",
			resp.StatusCode, strings.Replace(string(respData), "\n", "", -1))
		return
	}
	return
}

func getCluster(cfg *config.Config, masterAddr []string) string {
	cluster := cfg.GetString(master.ClusterName)
	if cluster == "" {
		cluster = strings.Join(masterAddr, ",")
	}
	return cluster
}