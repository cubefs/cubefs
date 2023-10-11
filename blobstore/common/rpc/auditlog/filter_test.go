package auditlog

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

var auditlog = AuditLog{
	ReqType:   "REQ",
	Module:    "RPC",
	StartTime: 16894926383996678,
	Method:    "GET",
	Path:      "/service/get",
	ReqHeader: M{
		"Accept-Encoding": "gzip",
		"Host":            "127.0.0.1:9999",
		"IP":              "127.0.0.1",
		"RawQuery":        "name=PROXY",
		"User-Agent":      "access/master (linux/amd64; go1.20.5) koordinator-virtual-machine/8896",
	},
	ReqParams:  "",
	StatusCode: 404,
	RespHeader: M{
		"Blobstore-Tracer-Traceid": "2e9c917595bea348",
		"Content-Type":             "text/plain; charset=utf-8",
		"Trace-Log":                []string{"RPC"},
		"Trace-Tags":               []string{"span.kind:server"},
		"X-Content-Type-Options":   "nosniff",
	},
	RespLength: 19,
	Duration:   56,
}

func TestFilter(t *testing.T) {
	que := Query{
		MustNot: []map[string]interface{}{
			{"match": []map[string]interface{}{{"path": "my_service"}}},
			{"range": []map[string]interface{}{{"start_time": "16894926383996679-16894926383996679"}}},
		},
		Must: []map[string]interface{}{
			{"term": []map[string]interface{}{{"module": "RPC"}, {"method": "GET"}}},
			{"regexp": []map[string]interface{}{{"req_header": "^.*$"}}},
			{"match": []map[string]interface{}{{"path": "get", "req_header": "gzip"}}},
		},
	}
	arr, _ := json.Marshal(que)
	q := Query{}
	json.Unmarshal(arr, &q)
	require.NoError(t, q.Init())
	ok, _ := q.FilterLogWithPriority(&auditlog)
	require.True(t, ok)
}

func Benchmark_Filter(b *testing.B) {
	que := Query{
		MustNot: []map[string]interface{}{
			{"match": []map[string]interface{}{{"path": "my_service"}, {"req_type": "REQ11"}}},
			{"range": []map[string]interface{}{{"start_time": "16894926383996679-16894926383996679"}}},
			{"term": []map[string]interface{}{
				{"req_params": "RPC"},
				{"resp_header": "utf-8"},
				{"resp_length": "0"},
				{"duration": "10"},
			}},
		},
		Must: []map[string]interface{}{
			{"term": []map[string]interface{}{
				{"module": "RPC"},
				{"method": "GET"},
				{"duration": "56"},
			}},
			{"regexp": []map[string]interface{}{{"req_header": "^.*$"}}},
			{"match": []map[string]interface{}{{"path": "get", "req_header": "gzip"}}},
		},
		Should: []map[string]interface{}{
			{"term": []map[string]interface{}{{"status_code": 404}, {"status_code": 200}, {"status_code": 403}}},
		},
	}
	arr, _ := json.Marshal(que)
	q := Query{}
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	require.NoError(b, q.Init())
	for i := 0; i < b.N; i++ {
		q.FilterLogWithPriority(&auditlog)
	}
}

func Benchmark_FilterWithPriority(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{"match": []map[string]interface{}{{"path": "my_service"}}},
			{"range": []map[string]interface{}{{"start_time": "16894926383996679-"}}},
		},
		Must: []map[string]interface{}{
			{"term": []map[string]interface{}{{"module": "RPC"}, {"method": "GET"}}},
			{"regexp": []map[string]interface{}{{"req_header": "^.*$"}}},
			{"match": []map[string]interface{}{{"path": "get", "req_header": "gzip"}}},
		},
		Should: []map[string]interface{}{
			{"term": []map[string]interface{}{{"status_code": 404}, {"status_code": 200}, {"status_code": 403, "resp_length": 19}}},
		},
	}
	q := Query{}
	arr, _ := json.Marshal(que)
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	require.NoError(b, q.Init())
	for i := 0; i < b.N; i++ {
		q.FilterLogWithPriority(&auditlog)
	}
}

func Benchmark_FilterWithPriorityB(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{"match": []map[string]interface{}{{"path": "my_service"}}},
			{"range": []map[string]interface{}{{"start_time": "16894926383996679-"}}},
		},
		Must: []map[string]interface{}{
			{"term": []map[string]interface{}{{"module": "RPC"}, {"method": "GET"}}},
			{"regexp": []map[string]interface{}{{"req_header": "^.*$"}}},
			{"match": []map[string]interface{}{{"path": "get", "req_header": "gzip"}}},
		},
		Should: []map[string]interface{}{
			{"term": []map[string]interface{}{{"status_code": 404}, {"status_code": 200}, {"status_code": 403}}},
		},
	}
	q := Query{}
	arr, _ := json.Marshal(que)
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	require.NoError(b, q.Init())
	for i := 0; i < b.N; i++ {
		q.FilterLogWithPriority(&auditlog)
	}
}

func Benchmark_FilterWithPriorityC(b *testing.B) {
	var que Query = Query{
		MustNot: []map[string]interface{}{
			{"match": []map[string]interface{}{{"path": "my_service"}}},
			{"range": []map[string]interface{}{{"start_time": "16894926383996679-"}}},
		},
		Must: []map[string]interface{}{
			{"term": []map[string]interface{}{{"module": "RPC"}, {"method": "GET"}}},
			{"regexp": []map[string]interface{}{{"req_header": "^.*$"}}},
			{"match": []map[string]interface{}{{"path": "get", "req_header": "gzip"}}},
		},
		Should: []map[string]interface{}{
			{"term": []map[string]interface{}{{"status_code": 404}, {"status_code": 200}, {"status_code": 403}}},
		},
	}
	q := Query{}
	arr, _ := json.Marshal(que)
	json.Unmarshal(arr, &q)
	b.ResetTimer()
	require.NoError(b, q.Init())
	for i := 0; i < b.N; i++ {
		q.FilterLogWithPriority(&auditlog)
	}
}
