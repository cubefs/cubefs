package auditlog

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestFilter(t *testing.T) {
	que := Query{
		MustNot: []map[string]interface{}{
			{
				"match": []map[string]interface{}{
					{
						"path": "my_service",
					},
				},
			},
			{
				"range": []map[string]interface{}{
					{
						"start_time": map[string]interface{}{
							"gte": "16894926383996679",
							"lte": "16894926383996679",
						},
					},
				},
			},
		},
		Must: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"module": "RPC",
					},
					{
						"method": "GET",
					},
				},
			},
			{
				"regexp": []map[string]interface{}{
					{
						"req_header": "^.*$",
					},
				},
			},
			{
				"match": []map[string]interface{}{
					{
						"path":       "get",
						"req_header": "gzip",
					},
				},
			},
		},
	}
	logs := []AuditLog{
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	q := Query{}
	arr, err := json.Marshal(que)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(arr, &q)
	q.Init()
	for _, log := range logs {
		if ok, err := q.FilterLogWithPriority(log); !ok {
			t.Log(err.Error())
		}
	}
}

func Benchmark_Filter(b *testing.B) {
	que := Query{
		MustNot: []map[string]interface{}{
			{
				"match": []map[string]interface{}{
					{
						"path": "my_service",
					},
					{
						"req_type": "REQ11",
					},
				},
			},
			{
				"range": []map[string]interface{}{
					{
						"start_time": map[string]interface{}{
							"gte": "16894926383996679",
							"lte": "16894926383996679",
						},
					},
				},
			},
			{
				"term": []map[string]interface{}{
					{
						"req_params": "RPC",
					},
					{
						"resp_header": "utf-8",
					},
					{
						"resp_length": "0",
					},
					{
						"duration": "10",
					},
				},
			},
		},
		Must: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"module": "RPC",
					},
					{
						"method": "GET",
					},
					{
						"duration": "56",
					},
				},
			},
			{
				"regexp": []map[string]interface{}{
					{
						"req_header": "^.*$",
					},
				},
			},
			{
				"match": []map[string]interface{}{
					{
						"path":       "get",
						"req_header": "gzip",
					},
				},
			},
		},
		Should: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"status_code": 404,
					},
					{
						"status_code": 200,
					},
					{
						"status_code": 403,
					},
				},
			},
		},
	}
	q := Query{}
	arr, err := json.Marshal(que)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	logs := []AuditLog{
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		//{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	q.Init()
	for i := 0; i < b.N; i++ {
		for _, log := range logs {
			if ok, err := q.FilterLogWithPriority(log); !ok {
				b.Log(err)
			}
		}
	}
}

func Benchmark_FilterWithPriority(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{
				"match": []map[string]interface{}{
					{
						"path": "my_service",
					},
				},
			},
			{
				"range": []map[string]interface{}{
					{
						"start_time": map[string]interface{}{
							"gte": "16894926383996679",
						},
					},
				},
			},
		},
		Must: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"module": "RPC",
					},
					{
						"method": "GET",
					},
				},
			},
			{
				"regexp": []map[string]interface{}{
					{
						"req_header": "^.*$",
					},
				},
			},
			{
				"match": []map[string]interface{}{
					{
						"path":       "get",
						"req_header": "gzip",
					},
				},
			},
		},
		Should: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"status_code": 404,
					},
					{
						"status_code": 200,
					},
					{
						"status_code": 403,
						"resp_length": 19,
					},
				},
			},
		},
	}
	q := Query{}
	arr, err := json.Marshal(que)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	logs := []AuditLog{
		//{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	q.Init()
	for i := 0; i < b.N; i++ {
		for _, log := range logs {
			if ok, err := q.FilterLogWithPriority(log); !ok {
				b.Log(err.Error())
			}
		}
	}
}

func Benchmark_FilterWithPriorityB(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{
				"match": []map[string]interface{}{
					{
						"path": "my_service",
					},
				},
			},
			{
				"range": []map[string]interface{}{
					{
						"start_time": map[string]interface{}{
							"gte": "16894926383996679",
						},
					},
				},
			},
		},
		Must: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"module": "RPC",
					},
					{
						"method": "GET",
					},
				},
			},
			{
				"regexp": []map[string]interface{}{
					{
						"req_header": "^.*$",
					},
				},
			},
			{
				"match": []map[string]interface{}{
					{
						"path":       "get",
						"req_header": "gzip",
					},
				},
			},
		},
		Should: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"status_code": 404,
					},
					{
						"status_code": 200,
					},
					{
						"status_code": 403,
					},
				},
			},
		},
	}
	q := Query{}
	arr, err := json.Marshal(que)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	logs := []AuditLog{
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 40, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		//{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	q.Init()
	for i := 0; i < b.N; i++ {
		for _, log := range logs {
			q.FilterLogWithPriority(log)
		}
	}
}

func Benchmark_FilterWithPriorityC(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{
				"match": []map[string]interface{}{
					{
						"path": "my_service",
					},
				},
			},
			{
				"range": []map[string]interface{}{
					{
						"start_time": map[string]interface{}{
							"gte": "16894926383996679",
						},
					},
				},
			},
		},
		Must: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"module": "RPC",
					},
					{
						"method": "GET",
					},
				},
			},
			{
				"regexp": []map[string]interface{}{
					{
						"req_header": "^.*$",
					},
				},
			},
			{
				"match": []map[string]interface{}{
					{
						"path":       "get",
						"req_header": "gzip",
					},
				},
			},
		},
		Should: []map[string]interface{}{
			{
				"term": []map[string]interface{}{
					{
						"status_code": 404,
					},
					{
						"status_code": 200,
					},
					{
						"status_code": 403,
					},
				},
			},
		},
	}
	q := Query{}
	arr, err := json.Marshal(que)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	logs := []AuditLog{
		{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/get", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
		//{ReqType: "REQ", Module: "RPC", StartTime: 16894926383996678, Method: "GET", Path: "/service/set", ReqHeader: M{"Accept-Encoding": "gzip", "Host": "127.0.0.1:9999", "IP": "127.0.0.1", "RawQuery": "name=PROXY", "User-Agent": "access/master/f55e969506182e516281b910b8c61f9bea121854 (linux/amd64; go1.20.5) koordinator-virtual-machine/8896"}, ReqParams: " ", StatusCode: 404, RespHeader: M{"Blobstore-Tracer-Traceid": "2e9c917595bea348", "Content-Type": "text/plain; charset=utf-8", "Trace-Log": []string{"RPC"}, "Trace-Tags": []string{"span.kind:server"}, "X-Content-Type-Options": "nosniff"}, RespLength: 19, Duration: 56},
	}
	q.Init()
	for i := 0; i < b.N; i++ {
		for _, log := range logs {
			if ok, err := q.FilterLogWithPriority(log); !ok {
				b.Log(err.Error())
			}
		}
	}
}
