package worker

import (
	"github.com/chubaofs/chubaofs/util/config"
	"testing"
)

func TestParseConfig(t *testing.T) {
	cfgJSON := `{
		"role": "schedulenode",
		"localIP": "11.97.57.231",
		"prof": "17330",
		"workerTaskPeriod": 5,
		"logDir": "/export/Logs/chubaofs/scheduleNode1/",
		"logLevel": "debug",
		"mysql": {
			"url": "10.170.240.34",
			"userName": "jfsops",
			"password": "Lizhendong7&",
			"database": "storage_object",
			"port": 3306
		},
		"hBaseUrl": "api.storage.hbase.jd.local/",
		"clusterAddr": {
			"smart_vol_test": [
				"172.20.81.30:17010",
				"172.20.81.31:17010",
				"11.7.139.135:17010"
			]
		}
    }`
	cfg := config.LoadConfigString(cfgJSON)
	baseWorker := &BaseWorker{}
	err := baseWorker.ParseBaseConfig(cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if baseWorker.MysqlConfig.Url != "10.170.240.34" {
		t.Errorf("invalid mysql url")
	}
	if baseWorker.MysqlConfig.Database != "storage_object" {
		t.Errorf("invalid mysql database")
	}
}
