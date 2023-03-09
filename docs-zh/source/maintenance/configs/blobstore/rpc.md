## RPC 配置

### 单点配置Client
```json
{
  "client_timeout_ms": "请求超时时间",
  "body_bandwidth_mbps": "读body带宽，默认1MBps",
  "body_base_timeout_ms": "读body基准时间，从而读body的最大时间为body_base_timeout_ms+size/body_bandwidth_mbps(转换为ms)",
  "transport_config": {
    "...": "详见golang http库transport配置，一般情况下可以忽略，代码内也提供了默认配置"
  }
}
```
### 多点配置LbClient
> Lb版本主要实现多节点的负载均衡、失败节点剔除与复用。其配置以单点配置为基础，额外增添以下配置项
```json
{
  "hosts": "请求目的主机列表",
  "backup_hosts": "备用目的主机列表，当hosts均不可用时投入使用",
  "host_try_times": "每个节点失败重试次数，配合节点剔除使用，当某个目标主机连续失败host_try_times次，若开启失败剔除机制，将会把这个节点从可用列表中剔除",
  "try_times": "每个请求失败重试次数",
  "fail_retry_interval_s": "配合节点剔除，实现失败节点重新投入使用的时间间隔，当该值小于或等于0不剔除，默认为-1",
  "MaxFailsPeriodS": "记录为连续失败次数时间间隔，例如当前节点已经失败N次，当第N+1次失败时间与第N次间隔小于该值，则记该节点为第N+1次失败，否则重新记为第1次失败"
}
```
