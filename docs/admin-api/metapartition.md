### Create

```bash
curl -v "http://127.0.0.1/metaPartition/create?name=test&start=10000"
```

split meta parition manully,if max meta partition of the vol which range is [0,end),end larger than start parameter,old meta paritioin range is[0,start], new meta partition is [start+1,end)

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|start|uint64| the start value of meta partition which will be create

### Get

```bash
curl -v "http://127.0.0.1/client/metaPartition?name=test&id=1" | python -m json.tool
```

show base information of meta partition,such as id,start,end and so on.

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|id|uint64| the id of meta partition

response

``` json
{
    "PartitionID": 1,
    "Start": 0,
    "End": 9223372036854776000,
    "MaxNodeID": 1,
    "Replicas": [...],
    "ReplicaNum": 3,
    "Status": 2,
    "PersistenceHosts": [...],
    "Peers": [...],
    "MissNodes": {}
}
```

### Offline

```bash
curl -v "http://127.0.0.1/metaPartition/offline?name=test&id=13&addr=127.0.0.1:9021"
```

remove the replica of meta parition,and create new replica asynchronous

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|id|uint64| the id of meta partition
|addr|string|the addr of replica which will be offfline
