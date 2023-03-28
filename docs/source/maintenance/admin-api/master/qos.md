# QoS Management

::: warning Note
QoS is a new feature added in v3.2.1.
:::

## Master Interface Throttling
The stability of the master is very important for the entire cluster, so in order to prevent accidents (a large number of retries) or malicious attacks, the interfaces of the master need to be managed by QPS throttling.

The QPS throttling for the master interface is based on QPS (how many requests are accepted per second). For interfaces that have not set throttling, no restrictions are imposed. For interfaces that have set throttling, there is a throttling wait timeout to prevent the avalanche effect.

### View All Interface Information on the Master
Before setting interface throttling, you can query which interfaces the master supports.

```bash
curl -v "http://192.168.0.11:17010/admin/getMasterApiList"
```

::: tip Note
`192.168.0.11` is the IP address of the master, and the same applies below.
:::

The response is as follows:

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

### Set Interface Throttling

Taking the `/dataPartition/get` interface as an example, from the response of the interface query command, you can see that the name of the interface is `admingetdatapartition`, and the command to set the interface throttling is as follows:

```bash
curl -v "http://192.168.0.11:17010/admin/setApiQpsLimit?name=AdminGetDataPartition&limit=2000&timeout=5"
```

::: tip Note
The value of the `name` parameter is the key under `data` in the response of the interface query command, such as `admingetdatapartition`. The letter case of the value is not case-sensitive and can be written as `AdminGetDataPartition`.
:::

| Parameter | Type   | Description                                    |
|-----------|--------|------------------------------------------------|
| name      | string | Interface name (case-insensitive)              |
| timeout   | uint   | Interface throttling wait timeout (in seconds) |

When the interface throttling is triggered (the QPS limit is reached), subsequent requests will be queued. The default timeout is 5 seconds. If the request is not processed after 5 seconds, a 429 response code will be returned.

### Query Interface Throttling Information

```bash
curl -v "http://192.168.0.11:17010/admin/getMasterApiList"
```

The response is as follows:

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

### Remove Interface Throttling

```bash
curl -v "http://192.168.0.11:17010/admin/rmApiQpsLimit?name=AdminGetDataPartition"
```

| Parameter | Type   | Description                       |
|-----------|--------|-----------------------------------|
| name      | string | Interface name (case-insensitive) |