# Resource Management

## Add Node

``` bash
curl -v "http://10.196.59.198:17010/raftNode/add?addr=10.196.59.197:17010&id=3"
```

Adds a new master node to the raft replication group.

Parameter List

| Parameter | Type   | Description                                        |
|-----------|--------|----------------------------------------------------|
| addr      | string | IP address of the master, in the format of ip:port |
| id        | uint64 | Node identifier of the master                      |

## Remove Node

``` bash
curl -v "http://10.196.59.198:17010/raftNode/remove?addr=10.196.59.197:17010&id=3"
```

Removes a node from the raft replication group.

Parameter List

| Parameter | Type   | Description                                        |
|-----------|--------|----------------------------------------------------|
| addr      | string | IP address of the master, in the format of ip:port |
| id        | uint64 | Node identifier of the master                      |
