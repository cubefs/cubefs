# 纠删码通用管理

## 查看服务通用接口

请求服务接口，可以看到一些比较通用接口方便调试，比如日志级别、指标等

```bash
curl http://127.0.0.1:9500
```

**响应示例**

```text
usage:
        /

vars:
        /debug/vars
        /debug/var/*

pprof:
        /debug/pprof/
        /debug/pprof/cmdline
        /debug/pprof/profile
        /debug/pprof/symbol
        /debug/pprof/trace

metrics:
        /metrics

users:
        /access/status
        /access/stream/controller/alg/:alg
        /log/level
        /log/level
```

## 日志级别查看

```bash
curl http://127.0.0.1:9500/log/level
```

**响应示例**

```text
{"level": "[DEBUG]"}
```

## 日志级别变更

| 级别    | 值   |
|-------|-----|
| debug | 0   |
| info  | 1   | 
| warn  | 2   | 
| error | 3   |
| panic | 4   |
| fatal | 5   |

```bash
# 以下为设置日志级别为warn
curl -XPOST -d 'level=2' http://127.0.0.1:9500/log/level
```

## metrics信息采集

```bash
curl http://127.0.0.1:9500/metrics
```
**响应示例**

```text
# HELP go_gc_duration_seconds A summary of the pause duration of garbage collection cycles.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 5.449e-05
go_gc_duration_seconds{quantile="0.25"} 6.633e-05
go_gc_duration_seconds{quantile="0.5"} 8.525e-05
go_gc_duration_seconds{quantile="0.75"} 0.000107266
go_gc_duration_seconds{quantile="1"} 0.000441444
go_gc_duration_seconds_sum 1.6007172
go_gc_duration_seconds_count 14311
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 45
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.17.1"} 1
......
```
