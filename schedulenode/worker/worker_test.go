package worker

import (
	"github.com/cubefs/cubefs/util/config"
	"testing"
)

func TestParseConfig(t *testing.T) {
	cfgJSON := `{
		"role": "schedulenode",
		"localIP": "192.168.0.100",
		"prof": "17330",
		"workerTaskPeriod": 5,
		"logDir": "/export/Logs/chubaofs/scheduleNode1/",
		"logLevel": "debug",
		"mysql": {
			"url": "192.168.0.101",
			"userName": "test",
			"password": "test",
			"database": "storage_object",
			"port": 3306
		},
		"hBaseUrl": "",
		"clusterAddr": {
			"smart_vol_test": [
				"192.168.0.201:9000",
				"192.168.0.202:9000",
				"192.168.0.203:9000"
			]
		}
    }`
	cfg := config.LoadConfigString(cfgJSON)
	baseWorker := &BaseWorker{}
	err := baseWorker.ParseBaseConfig(cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if baseWorker.MysqlConfig.Url != "192.168.0.101" {
		t.Errorf("invalid mysql url")
	}
	if baseWorker.MysqlConfig.Database != "storage_object" {
		t.Errorf("invalid mysql database")
	}
}
