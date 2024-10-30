# Resource Management

## Add Node

``` bash
curl -v "http://192.168.0.1:17010/raftNode/add?addr=10.196.59.197:17010&id=3"
```

Adds a new master node to the raft replication group.

Parameter List

| Parameter | Type   | Description                                        |
|-----------|--------|----------------------------------------------------|
| addr      | string | IP address of the master, in the format of ip:port |
| id        | uint64 | Node identifier of the master                      |

## Remove Node

``` bash
curl -v "http://192.168.0.1:17010/raftNode/remove?addr=10.196.59.197:17010&id=3"
```

Removes a node from the raft replication group.

Parameter List

| Parameter | Type   | Description                                        |
|-----------|--------|----------------------------------------------------|
| addr      | string | IP address of the master, in the format of ip:port |
| id        | uint64 | Node identifier of the master                      |

## Retrieve the nodeset list of the cluster

``` bash
curl -v "http://192.168.0.1:17010/nodeSet/list"
```
Retrieve the nodeset list of the cluster.

## Retrieve detailed information about the nodeset

``` bash
curl -v "http://192.168.0.1:17010//nodeSet/get?nodesetId=1"
```

Retrieve detailed information about the nodeset

Parameter List

| Parameter | Type   | Description                                        |
|------|--------|-------------------------|
| nodesetId | string | odeset identifier |