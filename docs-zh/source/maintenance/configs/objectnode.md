# ObjectNode
## 配置说明

| 参数         | 类型         | 描述                                                                                             | 是否必需 |
|--------------|--------------|--------------------------------------------------------------------------------------------------|----------|
| role         | string       | 进程角色，必须设置为 `objectnode`                                                                | 是       |
| listen       | string       | \| http服务监听的IP地址和端口号. \| 格式: `IP:PORT` 或者 `:PORT` \| 默认: `:80`                  | 是       |
| domains      | string slice | \| 为S3兼容接口配置域名以支持DNS风格访问资源 \| 格式: `DOMAIN`                                   | 否       |
| logDir       | string       | 日志存放路径                                                                                     | 是       |
| logLevel     | string       | \| 日志级别. \| 默认: `error`                                                                    | 否       |
| masterAddr   | string slice | \| 格式: `HOST:PORT`. \| HOST: 资源管理节点IP（Master）. \| PORT: 资源管理节点服务端口（Master） | 是       |
| exporterPort | string       | prometheus获取监控数据端口                                                                       | 否       |
| prof         | string       | 调试和管理员API接口                                                                              | 是       |

## 配置示例

``` json
{
     "role": "objectnode",
     "listen": "17410",
     "domains": [
         "object.cfs.local"
     ],
     "logDir": "/cfs/Logs/objectnode",
     "logLevel": "info",
     "masterAddr": [
         "10.196.59.198:17010",
         "10.196.59.199:17010",
         "10.196.59.200:17010"
     ],
     "exporterPort": 9503,
     "prof": "7013"
}
```