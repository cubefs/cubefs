# Reed-Solomon Code General Management

## View Service General Interface

Request the service interface to view some common interfaces for debugging, such as log level, metrics, etc.

```bash
curl http://127.0.0.1:9500
```

**Response Example**

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

## View Log Level

```bash
curl http://127.0.0.1:9500/log/level
```

**Response Example**
```text
{"level": "[DEBUG]"}
```

## Change Log Level

| Level | Value |
|-------|-------|
| debug | 0     |
| info  | 1     | 
| warn  | 2     | 
| error | 3     |
| panic | 4     |
| fatal | 5     |

```bash
# The following command sets the log level to warn
curl -XPOST -d 'level=2' http://127.0.0.1:9500/log/level
```

## Collect Metrics

```bash
curl http://127.0.0.1:9500/metrics
```

**Response Example**

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
