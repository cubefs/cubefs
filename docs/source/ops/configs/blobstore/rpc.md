# Erasure Code RPC Configuration

## Configuration Instructions
### Single-point Configuration Client
```json
{
  "client_timeout_ms": "Request timeout time",
  "body_bandwidth_mbps": "Read body bandwidth, default is 1MBps",
  "body_base_timeout_ms": "Read body benchmark time, so the maximum time to read body is body_base_timeout_ms+size/body_bandwidth_mbps(converted to ms)",
  "transport_config": {
    "...": "See the detailed configuration of the golang http library transport. In general, it can be ignored, and the default configuration is provided in the code"
  }
}
```

#### Default transport configuration

::: tip Note
This default configuration is supported starting from version v3.2.1.
:::

The following default configuration is only enabled when all items in `transport_config` are default values.

```json
{
  "max_conns_per_host": 10,
  "max_idle_conns": 1000,
  "max_idle_conns_per_host": 10,
  "idle_conn_timeout_ms": 10000
}

```

### Multi-point Configuration LbClient

The Lb version mainly implements load balancing, failure node removal and reuse of multiple nodes. Its configuration is based on single-point configuration, with the following additional configuration items.

```json
{
  "hosts": "List of destination hosts for requests",
  "backup_hosts": "List of backup destination hosts. When all hosts are unavailable, they will be used",
  "host_try_times": "Number of retries for each node failure, used in conjunction with node removal. When a target host fails continuously for host_try_times times, if the failure removal mechanism is enabled, the node will be removed from the available list",
  "try_times": "Number of retries for each request failure",
  "fail_retry_interval_s": "Used in conjunction with node removal to implement the time interval for failed nodes to be reused. If this value is less than or equal to 0, no removal will be performed. The default value is -1",
  "MaxFailsPeriodS": "Time interval for recording consecutive failures. For example, if the current node has failed N times, when the time interval between the N+1th failure and the Nth failure is less than this value, the node will be recorded as the N+1th failure. Otherwise, it will be recorded as the first failure"
}