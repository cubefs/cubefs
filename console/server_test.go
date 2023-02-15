package console

import (
	// "github.com/cubefs/cubefs/master"
	// "github.com/cubefs/cubefs/util/config"
	// "github.com/cubefs/cubefs/util/log"
	"testing"
)

func TestServer(t *testing.T) {

	return

	// if _, err := log.InitLog("/tmp", "console", log.DebugLevel, nil); err != nil {
	// 	panic(err)
	// }

	// // start master server
	// cfg := config.LoadConfigString(`{
	// 	"role": "master",
	// 	"ip": "127.0.0.1",
	// 	"listen": "8080",
	// 	"prof":"10088",
	// 	"id":"1",
	// 	"peers": "1:127.0.0.1:8080",
	// 	"retainLogs":"20000",
	// 	"tickInterval":500,
	// 	"electionTick":6,
	// 	"logDir": "/tmp/chubaofs/Logs",
	// 	"walDir":"/tmp/chubaofs/raft",
	// 	"storeDir":"/tmp/chubaofs/rocksdbstore",
	// 	"clusterName":"chubaofs"
	// }`)

	// mserver := master.NewServer()
	// if err := mserver.Start(cfg); err != nil {
	// 	panic(err)
	// }

	// go func() {
	// 	mserver.Sync()
	// }()

	// // start console server
	// cfg = config.LoadConfigString(`{
	// 	"role": "master",
	// 	"listen": "8989",
	// 	"masterAddr": ["127.0.0.1:8080"],
	// 	"monitor_addr" :"http://cfsthanos-yf01.cfsmon.svc.zyx02.n.jd.local",
	// 	"monitor_app": "cfs",
	// 	"monitor_cluster": "spark"
	// }`)

	// server := NewServer()
	// if err := server.Start(cfg); err != nil {
	// 	panic(err)
	// }
	// server.Sync()
}

type graphErr struct {
	Message string
}
type graphResponse struct {
	Data   interface{}
	Errors []graphErr
}
