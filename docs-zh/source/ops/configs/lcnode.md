# LcNode 配置
## 配置说明

| 参数           | 类型           | 描述                                                              | 必需  | 默认值     |
|:--------------|:--------------|:-----------------------------------------------------------------|:-------| :--------- |
| role         | string       | 进程角色，必须设置为 `lcnode`                                         | 是   |          |
| listen       | string       | http 服务监听的端口号. 格式: `PORT`         | 是   |     80     |
| logDir       | string       | 日志存放路径                                                          | 是   |          |
| logLevel     | string       | 日志级别                                              | 否   |    error      |
| masterAddr   | string slice | 格式: `HOST:PORT`，HOST: 资源管理节点IP（Master），PORT: 资源管理节点服务端口（Master） | 是   |          |
| prof         | string       | 调试和管理员 API 接口                                                     | 否   |          |
| lcScanRoutineNumPerTask | int       | 生命周期迁移文件并发数                                                     | 否   |     20     |
| lcScanLimitPerSecond    | int       | 生命周期迁移文件QPS限制                                                     | 否   |    0 （不限制）      |
| delayDelMinute | int       | 生命周期迁移文件后源数据保留时间                                                     | 否   |     1440     |
| useCreateTime | bool       | 生命周期使用文件创建时间判断过期，默认使用文件访问时间判断过期                    | 否   |     false     |

## 配置示例

``` json
{
    "role": "lcnode",
    "listen": "17510",
    "logDir": "./logs",
    "logLevel": "info",
    "masterAddr": [
        "xxx",
        "xxx",
        "xxx"
    ]
}
```