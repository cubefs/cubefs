package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func initRW() (*ReBalanceWorker, error) {
	cfgJSON := `{
                "role": "schedulenode",
                "localIP": "192.168.0.13",
                "prof": "17330",
                "workerTaskPeriod": 5,
                "logDir": "/export/Logs/chubaofs/scheduleNode1/",
                "logLevel": "debug",
                "mysql": {
                        "url": "11.13.125.198",
                        "userName": "root",
                        "password": "1qaz@WSX",
                        "database": "rebalance_dp_record",
                        "port": 80
                }
    }`
	cfg := config.LoadConfigString(cfgJSON)
	rw := NewReBalanceWorker()
	err := rw.parseConfig(cfg)
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func TestParseConfig(t *testing.T) {
	rw, err := initRW()
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, "17330", rw.Port)
	assert.Equal(t, "11.13.125.198", rw.MysqlConfig.Url)
	assert.Equal(t, "root", rw.MysqlConfig.Username)
	assert.Equal(t, "rebalance_dp_record", rw.MysqlConfig.Database)
}

func TestDataBase(t *testing.T) {
	rw, err := initRW()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = rw.OpenSql()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Run("test insert", func(t *testing.T) {
		err = rw.PutMigrateInfoToDB(&MigrateRecordTable{
			ClusterName: "test_cluster",
			ZoneName:    "test_zone",
			VolName:     "test_vol",
			PartitionID: 1,
			SrcAddr:     "192.168.0.1:6000",
			SrcDisk:     "/data0",
			DstAddr:     "192.168.0.2:6000",
		})
		if err != nil {
			t.Fatal(err.Error())
		}
	})
	t.Run("test get by zoneName", func(t *testing.T) {
		res, err := rw.GetMigrateRecordsByZoneName("test_cluster", "test_zone")
		if err != nil {
			t.Fatal(err.Error())
		}
		for _, v := range res {
			fmt.Println(v)
		}
	})
	t.Run("test get by host", func(t *testing.T) {
		res, err := rw.GetMigrateRecordsBySrcNode("192.168.0.1")
		if err != nil {
			t.Fatal(err.Error())
		}
		for _, v := range res {
			fmt.Println(v)
		}
	})
}

func TestHandle(t *testing.T) {
	rw, err := initRW()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = rw.OpenSql()
	if err != nil {
		t.Fatal(err.Error())
	}
	rw.registerHandler()
	fmt.Println("listen at 18080")
	err = http.ListenAndServe(":18080", nil)
	if err != nil {
		t.Fatal(err)
	}
}

// example:
// curl "http://localhost:18080/rebalance/start?cluster=sparkchubaofs.jd.local&zoneName=default&highRatio=0.90&lowRatio=0.50&goalRatio=0.85&migrateLimit=10"
// curl "http://localhost:18080/rebalance/status?cluster=sparkchubaofs.jd.local&zoneName=default"
// curl "http://localhost:18080/rebalance/stop?cluster=sparkchubaofs.jd.local&zoneName=default"
