# QOS管理

::: warning 注意
QOS为release-3.2.1版本新增feature
:::

## Master接口限流
Master的稳定性对于整个集群非常重要，所以为了防止意外（失败大量重试）或者恶意攻击，需要对Master的接口进行qps限流管理。

Master的接口限流为qps限流（每秒接受多少个请求），对于没有设置限流的接口，不做任何限制。对于设置了限流的接口，有一个限流等待超时时间可以设置，防止雪崩效应。

### 查看Master上的所有接口信息
在设置接口限流前，可以查询Master支持哪些接口。

```bash
curl -v "http://192.168.0.11:17010/admin/getMasterApiList"
```

::: tip 提示
`192.168.0.11`为Master的ip地址，下同
:::

响应如下：
```json
{
    "code": 0,
    "data": {
        "adddatanode": "/dataNode/add",
        "addmetanode": "/metaNode/add",
        "addraftnode": "/raftNode/add",
        "adminadddatareplica": "/dataReplica/add",
        "adminaddmetareplica": "/metaReplica/add",
        "adminchangemetapartitionleader": "/metaPartition/changeleader",
        "adminclusterfreeze": "/cluster/freeze",
        "adminclusterstat": "/cluster/stat",
        "admincreatedatapartition": "/dataPartition/create",
        "admincreatemetapartition": "/metaPartition/create",
        "admincreatepreloaddatapartition": "/dataPartition/createPreLoad",
        "admincreatevol": "/admin/createVol",
        "admindatapartitionchangeleader": "/dataPartition/changeleader",
        "admindecommissiondatapartition": "/dataPartition/decommission",
        "admindecommissionmetapartition": "/metaPartition/decommission",
        "admindeletedatareplica": "/dataReplica/delete",
        "admindeletemetareplica": "/metaReplica/delete",
        "admindeletevol": "/vol/delete",
        "admindiagnosedatapartition": "/dataPartition/diagnose",
        "admindiagnosemetapartition": "/metaPartition/diagnose",
        "admingetallnodesetgrpinfo": "/admin/getDomainInfo",
        "admingetcluster": "/admin/getCluster",
        "admingetdatapartition": "/dataPartition/get",
        "admingetinvalidnodes": "/invalid/nodes",
        "admingetip": "/admin/getIp",
        "admingetisdomainon": "/admin/getIsDomainOn",
        "admingetmasterapilist": "/admin/getMasterApiList",
        "admingetnodeinfo": "/admin/getNodeInfo",
        "admingetnodesetgrpinfo": "/admin/getDomainNodeSetGrpInfo",
        "admingetvol": "/admin/getVol",
        "adminlistvols": "/vol/list",
        "adminloaddatapartition": "/dataPartition/load",
        "adminloadmetapartition": "/metaPartition/load",
        "adminsetapiqpslimit": "/admin/setApiQpsLimit",
        "adminsetclusterinfo": "/admin/setClusterInfo",
        "adminsetdprdonly": "/admin/setDpRdOnly",
        "adminsetmetanodethreshold": "/threshold/set",
        "adminsetnodeinfo": "/admin/setNodeInfo",
        "adminsetnoderdonly": "/admin/setNodeRdOnly",
        "adminupdatedatanode": "/dataNode/update",
        "adminupdatedomaindatauseratio": "/admin/updateDomainDataRatio",
        "adminupdatemetanode": "/metaNode/update",
        "adminupdatenodesetcapcity": "/admin/updateNodeSetCapcity",
        "adminupdatenodesetid": "/admin/updateNodeSetId",
        "adminupdatevol": "/vol/update",
        "adminupdatezoneexcluderatio": "/admin/updateZoneExcludeRatio",
        "adminvolexpand": "/vol/expand",
        "adminvolshrink": "/vol/shrink",
        "canceldecommissiondatanode": "/dataNode/cancelDecommission",
        "clientdatapartitions": "/client/partitions",
        "clientmetapartition": "/metaPartition/get",
        "clientmetapartitions": "/client/metaPartitions",
        "clientvol": "/client/vol",
        "clientvolstat": "/client/volStat",
        "decommissiondatanode": "/dataNode/decommission",
        "decommissiondisk": "/disk/decommission",
        "decommissionmetanode": "/metaNode/decommission",
        "getallzones": "/zone/list",
        "getdatanode": "/dataNode/get",
        "getdatanodetaskresponse": "/dataNode/response",
        "getmetanode": "/metaNode/get",
        "getmetanodetaskresponse": "/metaNode/response",
        "gettopologyview": "/topo/get",
        "migratedatanode": "/dataNode/migrate",
        "migratemetanode": "/metaNode/migrate",
        "qosgetclientslimitinfo": "/qos/getClientsInfo",
        "qosgetstatus": "/qos/getStatus",
        "qosgetzonelimitinfo": "/qos/getZoneLimit",
        "qosupdate": "/qos/update",
        "qosupdateclientparam": "/qos/updateClientParam",
        "qosupdatemasterlimit": "/qos/masterLimit",
        "qosupdatezonelimit": "/qos/updateZoneLimit",
        "qosupload": "/admin/qosUpload",
        "raftstatus": "/get/raftStatus",
        "removeraftnode": "/raftNode/remove",
        "updatezone": "/zone/update",
        "usercreate": "/user/create",
        "userdelete": "/user/delete",
        "userdeletevolpolicy": "/user/deleteVolPolicy",
        "usergetakinfo": "/user/akInfo",
        "usergetinfo": "/user/info",
        "userlist": "/user/list",
        "userremovepolicy": "/user/removePolicy",
        "usersofvol": "/vol/users",
        "usertransfervol": "/user/transferVol",
        "userupdate": "/user/update",
        "userupdatepolicy": "/user/updatePolicy"
    },
    "msg": "success"
}
```

### 设置接口限流

以接口`/dataPartition/get`为例，从接口查询命令响应中可以得到，接口的名字为`admingetdatapartition`，接口限流命令如下：
```bash
curl -v "http://192.168.0.11:17010/admin/setApiQpsLimit?name=AdminGetDataPartition&limit=2000&timeout=5"
```

::: tip 提示
name参数的值为接口查询命令响应中`data`下的key，如`admingetdatapartition`，值的字母大小写不区分，可以写为`AdminGetDataPartition`
:::

| 参数   | 类型     | 描述               |
|------|--------|------------------|
| name | string | 接口名称（字母不区分大小写）   |
| timeout | uint   | 接口限流等待超时时间(单位：秒) |

当触发该接口限流时（达到qps限流值），后续的请求将进行等待，默认超时时间为5秒。超过5秒仍然没有得到处理，则返回429响应码。

### 查询接口限流信息

```bash
curl -v "http://192.168.0.11:17010/admin/getMasterApiList"
```
响应如下：

```json
{
    "code": 0,
    "msg": "success",
    "data": {
        "/admin/getIp": {
            "api_name": "admingetip",
            "query_path": "/admin/getIp",
            "limit": 1,
            "limiter_timeout": 5
        },
        "/dataPartition/get": {
            "api_name": "admingetdatapartition",
            "query_path": "/dataPartition/get",
            "limit": 2000,
            "limiter_timeout": 5
        }
    }
}
```

### 删除接口限流

```bash
curl -v "http://192.168.0.11:17010/admin/rmApiQpsLimit?name=AdminGetDataPartition"
```

| 参数   | 类型     | 描述           |
|------|--------|--------------|
| name | string | 接口名称（字母不区分大小写） |