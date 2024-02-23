# 授权节点

authnode负责授权客户端对CubeFS的Master节点的访问。通过此文档可以创建一个authnode docker-compose试用集群。

authnode功能的整体流程是：创建key –> 使用key获取访问指定服务的ticket –> 使用ticket访问服务。

## 编译构建

使用如下命令构建authtool及相关的依赖：

```bash
$ git clone http://github.com/cubefs/cubefs.git
$ cd cubefs
$ make build
```

如果构建成功，将在 build/bin 目录中生成可执行文件 cfs-authtool 。

## 配置文件

创建anthnode的key：

```bash
$ ./cfs-authtool authkey
```

执行命令后，将在当前目录下生成 `authroot.json` 和 `authservice.json` 两个key文件。

示例 authservice.json ：

```json
{
    "id": "AuthService",
    "key": "9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
    "create_ts": 1573801212,
    "role": "AuthService",
    "caps": "{\"*\"}"
}
```

在 docker/conf 目录下，编辑 `authnode.json` 配置文件：

- 将 `authroot.json` 文件中的 key 值作为 `authRootKey` 的值。

- 将 `authservice.json` 文件中的 key 值作为 `authServiceKey` 的值。

示例 `authnode.json` ：

```json
{
     "role": "authnode",
     "ip": "192.168.0.14",
     "port": "8080",
     "prof":"10088",
     "id":"1",
     "peers": "1:192.168.0.14:8080,2:192.168.0.15:8081,3:192.168.0.16:8082",
     "retainLogs":"2",
     "logDir": "/export/Logs/authnode",
     "logLevel":"info",
     "walDir":"/export/Data/authnode/raft",
     "storeDir":"/export/Data/authnode/rocksdbstore",
     "exporterPort": 9510,
     "consulAddr": "http://consul.prometheus-cfs.local",
     "clusterName":"test",
     "authServiceKey":"9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
     "authRootKey":"MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE=",
     "enableHTTPS":false
}
```

## 启动集群

在 docker/authnode 目录下，执行以下命令创建authnode集群

```bash
$ docker-compose up -d
```

## 使用授权功能

### 授权准备

**获取authService的ticket**

```bash
$ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=authservice.json -output=ticket_auth.json getticket AuthService
```

参数说明

| 字段    | 说明                                                         |
|---------|------------------------------------------------------------|
| host    | authnode的访问地址                                           |
| keyfile | 需要获取ticket的用户key文件路径，是“创建key”操作输出的key文件 |
| output  | 输出存放ticket的文件路径                                     |


示例 `ticket_auth.json` ：

```json
{
    "id": "AuthService",
    "session_key": "A9CSOGEN9CFYhnFnGwSMd4WFDBVbGmRNjaqGOhOinJE=",
    "service_id": "AuthService",
    "ticket": "RDzEiRLX1xjoUyp2TDFviE/eQzXGlPO83siNJ3QguUrtpwiHIA3PLv4edyKzZdKcEb3wikni8UxBoIJRhKzS00+nB7/9CjRToAJdT9Glhr24RyzoN8psBAk82KEDWJhnl+Y785Av3f8CkNpKv+kvNjYVnNKxs7f3x+Ze7glCPlQjyGSxqARyLisoXoXbiE6gXR1KRT44u7ENKcUjWZ2ZqKEBML9U4h0o58d3IWT+n4atWKtfaIdp6zBIqnInq0iUueRzrRlFEhzyrvi0vErw+iU8w3oPXgTi+um/PpUyto20c1NQ3XbnkWZb/1ccx4U0"
}
```

**创建管理员用户**

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_auth.json -data=data_admin.json -output=key_admin.json AuthService createkey
```

参数说明

| 字段       | 说明                                                         |
|------------|------------------------------------------------------------|
| ticketfile | 上一步骤所得ticket文件的路径，使用ticket才能访问相关服务      |
| data       | 需要注册的管理员用户信息                                     |
| output     | 管理员用户的key文件路径，key文件格式同前述操作所输出的key文件 |


示例 `data_admin.json` ：

```json
{
  "id": "admin",
  "role": "client",
  "caps": "{\"API\":[\"*:*:*\"]}"
}
```

### 管理员授权用户

**管理员获取ticket**

```bash
$ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_admin.json -output=ticket_admin.json getticket AuthService
```

**管理员创建新的授权用户**

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey
```

**授权用户获取访问服务的ticket**

例如，访问MasterService，可以执行以下命令获取ticket：

```bash
$ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_client.json -output=ticket_client.json getticket MasterService
```

## 在CubeFS集群中添加授权功能

### 为Master节点创建key

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_master.json -output=key_master.json AuthService createkey
```

示例 `data_master` ：

```json
{
    "id": "MasterService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

执行命令后，将 `key_master.json` 中 `key` 的值作为 `masterServiceKey` 的值写入配置文件 `master.json` 中。

### 为客户端创建key

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey
```

示例 `data_client` ：

```json
{
    "id": "ltptest",
    "role": "client",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

参数说明

| 字段 | 说明                                                              |
|------|-----------------------------------------------------------------|
| id   | volname名称                                                       |
| role | 有client和service两种                                             |
| caps | 格式为”{“API”:[“master:getVol:access”]}”，设为*表示所有API均可访问 |


执行命令后，将 `key_client.json` 中 `key` 的值作为 `clientKey` 的值写入配置文件 `client.json` 中。

示例 `client.json` ：

```json
{
    "masterAddr": "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010",
    "mountPoint": "/cfs/mnt",
    "volName": "ltptest",
    "owner": "ltptest",
    "logDir": "/cfs/log",
    "logLevel": "info",
    "consulAddr": "http://192.168.0.100:8500",
    "exporterPort": 9500,
    "profPort": "17410",
    "authenticate": true,
    "ticketHost": "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082",
    "clientKey": "jgBGSNQp6mLbu7snU8wKIdEkytzl+pO5/OZOJPpIgH4=",
    "enableHTTPS": "false"
}
```

参数说明

| 字段         | 说明                                                  |
|--------------|-----------------------------------------------------|
| authenticate | 是否需要权限认证。设为true表示当前Vol需要进行权限认证。 |
| ticketHost   | authnode集群的节点信息                                |
| clientKey    | 分发给client的key                                     |
| enableHTTPS  | 是否使用https协议传输                                 |


### 启动CubeFS集群

```bash
$ docker/run_docker.sh -r -d /data/disk
```

在客户端的启动过程中，会先使用clientKey从authnode节点处获取访问Master节点的ticket，再使用ticket访问Master API。因此，只有被受权的客户端才能成功启动并挂载

## Master API鉴权

master有众多API，比如创建卷，删除卷，因此我们有必要对master API的访问进行鉴权，以提高集群安全性。

借助authnode优秀的鉴权能力，在原有鉴权机制的基础上，我们进行了优化，以达到简化鉴权流程的目的。

### 开启Master API鉴权

示例 `master.json` ：
```json
{
  "role": "master",
  "ip": "127.0.0.1",
  "listen": "17010",
  "prof":"17020",
  "id":"1",
  "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log",
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal",
  "storeDir":"/cfs/master/data/store",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824",
  "masterServiceKey": "jgBGSNQp6mLbu7snU8wKIdEkytzl+pO5/OZOJPpIgH4=",
  "authenticate": true,
  "authNodeHost": "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082",
  "authNodeEnableHTTPS": false
}
```
参数说明

| 字段         | 说明                                                  |
|--------------|-----------------------------------------------------|
| authenticate | 是否需要权限认证。设为true表示Master API需要进行权限认证。 |
| authNodeHost   | authnode集群的节点信息                                |
| authNodeEnableHTTPS  | 是否使用https协议传输    |

### 附带鉴权参数

访问Master API时，必须带上用于鉴权的参数clientIDKey。

使用authtool创建key时，会生成auth_id_key。此key在访问master API时将作为clientIDKey。

#### 示例1
以HTTP方式访问master API，写入参数clientIDKey，比如扩容卷：

```bash
curl --location 'http://127.0.0.1:17010/vol/update?name=ltptest&authKey=0e20229116d5a9a4a9e876806b514a85&capacity=100&clientIDKey=eyJpZCI6Imx0cHRlc3QiLCJhdXRoX2tleSI6ImpnQkdTTlFwNm1MYnU3c25VOHdLSWRFa3l0emwrcE81L09aT0pQcElnSDQ9In0='
```

#### 示例2
以cfs-cli方式访问master API，把clientIDKey写到.cfs-cli.json配置文件中，这样任何集群管理命令都进行了权限认证。

示例 `.cfs-cli.json` ：
```json
{
  "masterAddr": [
    "127.0.0.1:17010",
    "127.0.0.2:17010",
    "127.0.0.3:17010"
  ],
  "timeout": 60,
  "clientIDKey": "eyJpZCI6Imx0cHRlc3QiLCJhdXRoX2tleSI6ImpnQkdTTlFwNm1MYnU3c25VOHdLSWRFa3l0emwrcE81L09aT0pQcElnSDQ9In0="
}
```

#### 示例3
datanode和metanode在启动时，会分别调用AddDataNode和AddMetaNode API，因此也需要为它们准备serviceIDKey。

同样的，使用authtool分别为datanode和metanode创建key，把该key作为serviceIDKey写入到配置文件中，当它们启动时，就会进行权限认证。

#### 为datanode节点创建key

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_datanode.json -output=key_datanode.json AuthService createkey
```

示例 `data_datanode` ：

```json
{
    "id": "DatanodeService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

执行命令后，将 `key_datanode.json` 中 `auth_id_key` 的值作为 `serviceIDKey` 的值写入配置文件 `datanode.json` 中。

#### 为metanode节点创建key

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_metanode.json -output=key_metanode.json AuthService createkey
```

示例 `data_metanode` ：

```json
{
    "id": "MetanodeService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

执行命令后，将 `key_metanode.json` 中 `auth_id_key` 的值作为 `serviceIDKey` 的值写入配置文件 `metanode.json` 中。