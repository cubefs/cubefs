package auditlog

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestOpen1(t *testing.T) {
	dsl, err := os.ReadFile("config.txt")
	if err != nil {
		fmt.Println("读取文件错误")
	}
	var q Query
	err1 := json.Unmarshal(dsl, &q)
	if err1 != nil {
		fmt.Println("Error parsing DSL:", err)
		return
	}

	logs := []Log{
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	//t1 := Log{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56}
	//if FilterLog(t1, q) {
	//	fmt.Println(t1)
	//}

	for _, log := range logs {
		if FilterLog(log, q) {
			fmt.Println(log)
		}
	}
}

func Benchmark_LogFilter(b *testing.B) {
	dsl, err := os.ReadFile("config.txt")
	if err != nil {
		fmt.Println("读取文件错误")
	}
	var q Query
	err1 := json.Unmarshal(dsl, &q)
	if err1 != nil {
		fmt.Println("Error parsing DSL:", err)
		return
	}
	b.ResetTimer()
	t1 := Log{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56}
	for i := 0; i < b.N; i++ {
		FilterLog(t1, q)
	}

}
