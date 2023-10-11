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
	RespBody:   "",
	RespLength: 19,
	Duration:   56,
}

func TestFilterNewError(t *testing.T) {
	cases := []FilterConfig{
		{Must: Conditions{"term": {"no-field": "x"}}},
		{MustNot: Conditions{"term": {"StatusCode": "xxx"}}},
		{Must: Conditions{"term-match": {"method": "GET"}}},
		{Must: Conditions{"term-match": {"duration": "GET"}}},
		{MustNot: Conditions{"regexp": {"Method": `(\d`}}},
		{Should: Conditions{"range": {"status_code": "1-2-3"}}},
		{Should: Conditions{"range": {"status_code": "x-"}}},
		{Should: Conditions{"range": {"status_code": "-x"}}},
		{Should: Conditions{"range": {"status_code": 100}}},
	}
	for _, cfg := range cases {
		_, err := newFilter(cfg)
		require.Error(t, err)
		_, err = newLogFilter([]FilterConfig{cfg})
		require.Error(t, err)
	}
}

func TestFilterRun(t *testing.T) {
	cases := []struct {
		cfg FilterConfig
		b   bool
	}{
		{FilterConfig{Must: Conditions{"term": {"ReqType": "REQx"}}}, false},
		{FilterConfig{Must: Conditions{"term": {"ReqType": "REQ"}}}, true},
		{FilterConfig{Must: Conditions{"term": {"ReqType": []string{"REQ", "QER"}}}}, false},
		{FilterConfig{Must: Conditions{"term": {"ReqType": "REQ", "RespLength": "19"}}}, true},
		{FilterConfig{
			Must:    Conditions{"term": {"ReqType": "REQ", "RespLength": "19"}},
			MustNot: Conditions{"term": {"module": "rpc"}, "match": {"path": "//service"}},
		}, true},
		{FilterConfig{
			MustNot: Conditions{"term": {"module": "rpc"}, "match": {"path": "/service"}},
		}, false},
		{FilterConfig{
			Should: Conditions{
				"match":  {"path": "//service"},
				"regexp": {"req_header": "access.master"},
			},
		}, true},
		{FilterConfig{
			Should: Conditions{
				"match":  {"path": "//service"},
				"regexp": {"req_header": "access.master", "RespHeader": ".*"},
			},
		}, true},
		{FilterConfig{
			Must: Conditions{
				"term":  {"ReqType": "REQ", "RespLength": "19", "req_params": "", "start_time": "16894926383996678"},
				"range": {"Duration": "1-20,30-100"},
			},
			MustNot: Conditions{
				"term":  {"module": []string{"rpc", "Rpc", "RPc"}, "method": "get"},
				"match": {"path": "//service"},
			},
			Should: Conditions{
				"term":   {"resp_body": "body"},
				"match":  {"path": "//service"},
				"regexp": {"req_header": "access..master", "RespHeader": "Blobstore-Traceid"},
				"range":  {"status_code": "100-403,405-500"},
			},
		}, false},
	}
	for _, cs := range cases {
		f, err := newLogFilter([]FilterConfig{cs.cfg})
		require.NoError(t, err)
		if cs.b {
			require.True(t, f.Filter(&auditlog))
		} else {
			require.False(t, f.Filter(&auditlog))
		}
	}

	cfg := FilterConfig{
		Must: Conditions{
			"term":  {"ReqType": "REQ", "RespLength": "19", "req_params": "", "start_time": "16894926383996678"},
			"range": {"Duration": "1-20,30-100"},
		},
		MustNot: Conditions{
			"term":  {"module": []string{"rpc", "Rpc", "RPc"}, "method": "GET"},
			"match": {"path": "//service"},
		},
	}
	f, err := newFilter(cfg)
	require.NoError(t, err)
	for range [100000]struct{}{} {
		require.False(t, f.Filter(&auditlog))
	}
	require.Equal(t, int64(1), f.reset)
	f.and.Push(f.and.Pop())

	cfgString := `{
		"must":{
			"term":{"ReqType":"REQ","RespLength":"19","req_params":"","start_time":"16894926383996678"},
			"range":{"Duration":"1-20,30-100"}
		},
		"must_not":{
			"term":{"method":"GET","module":["rpc","Rpc","RPc", 10]},
			"match":{"path":"//service"}
		}
	}`
	var cfgLoad FilterConfig
	require.NoError(t, json.Unmarshal([]byte(cfgString), &cfgLoad))
	require.Equal(t, cfg.Must, cfgLoad.Must)
	f, err = newFilter(cfgLoad)
	require.NoError(t, err)
	for range [100000]struct{}{} {
		require.False(t, f.Filter(&auditlog))
	}
	require.Equal(t, int64(1), f.reset)
}

func Benchmark_FilterSimple(b *testing.B) {
	f, err := newFilter(FilterConfig{Must: Conditions{"term": {"ReqType": "REQ"}}})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}

func Benchmark_FilterPriority(b *testing.B) {
	f, err := newFilter(FilterConfig{
		Must:    Conditions{"range": {"Duration": "1-20,30-100"}},
		MustNot: Conditions{"term": {"module": "rpc", "method": "GET"}, "match": {"path": "//service"}},
	})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}

func Benchmark_FilterComplex(b *testing.B) {
	f, err := newFilter(FilterConfig{
		Must: Conditions{
			"term":  {"ReqType": "REQ", "RespLength": "19", "req_params": "", "start_time": "16894926383996678"},
			"range": {"Duration": "1-20,30-100"},
		},
		MustNot: Conditions{"term": {"module": "rpc", "method": "get"}, "match": {"path": "//service"}},
		Should: Conditions{
			"term":  {"resp_body": "body"},
			"match": {"path": "//service"},
			"range": {"status_code": "100-403,405-500"},
		},
	})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}

func Benchmark_FilterMatch(b *testing.B) {
	f, err := newFilter(FilterConfig{Must: Conditions{"match": {"path": "vice"}}})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}

func Benchmark_FilterRegxp(b *testing.B) {
	f, err := newFilter(FilterConfig{Must: Conditions{"regexp": {"path": "^/service.get$"}}})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}

func Benchmark_FilterHeader(b *testing.B) {
	f, err := newFilter(FilterConfig{Should: Conditions{"regexp": {"req_header": "access..master"}}})
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Filter(&auditlog)
	}
}
