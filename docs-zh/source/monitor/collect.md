# 监控指标采集

## 开启指标

- `Master` 指标监听端口为服务端口，默认开启指标监控模块
- `Blobstore` 指标监听端口为服务端口，默认开启服务自定义监控指标项，公共指标项需要修改配置文件进行打开，可参考 [这里](./metrics.md)。

- `其他模块` 需要配置指标监听端口，默认关闭
    - `exporterPort`: 指标监听端口。
    - `consulAddr`: consul 注册服务器地址。设置后, 可配合 prometheus 的自动发现机制实现 CubeFS 节点 exporter 的自动发现服务，若不设置，将不会启用 consul 自动注册服务。
    - `consulMeta`：consul 元数据配置。 非必填项, 在注册到 consul 时设置元数据信息。
    - `ipFilter`: 基于正则表达式的过滤器。 非必填项，默认为空。暴露给 consul, 当机器存在多个 ip 时使用. 支持正向和反向过滤。
    - `enablePid`：是否上报 partition id, 默认为 false; 如果想在集群展示 dp 或者 mp 的信息, 可以配置为 true。

```json
{
  "exporterPort": 9505,
  "consulAddr": "http://consul.prometheus-cfs.local",
  "consulMeta": "k1=v1;k2=v2",
  "ipFilter": "10.17.*",
  "enablePid": "false"
}
```

请求服务对应指标监听接口可以获取到监控指标，如 `curl localhost:port/metrics`

## 采集指标

对于 `Master`、`MetaNode`、`DataNode`、`ObjectNode` 而言，有两种方式实现指标采集：

- 配置 prometheus 的 consul 地址（或者是支持 prometheus 标准语法的 consul 地址），配置生效后 prometheus 会主动拉取监控指标
- 不配置 consul 地址，示例如下：

修改 prometheus 的 yaml 配置文件，添加采集指标源

```yaml
# prometheus.yml
- job_name: 'cubefs01'
file_sd_configs:
  - files: [ '/home/service/app/prometheus/cubefs01/*.yml' ]
  refresh_interval: 10s
```

接入 exporter，在上述配置目录下创建 exporter 文件，以 master 为例，创建 master_exporter.yaml 文件

```yaml
# master_exporter.yaml
- targets: [ 'master1_ip:17010' ]
  labels:
    cluster: cubefs01
```

配置完成之后启动 prometheus 即可。

`纠删码子系统（Blobstore）`相关服务暂时只支持上述第二种方式采集指标。
