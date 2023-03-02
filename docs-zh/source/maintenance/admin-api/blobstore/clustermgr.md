# Clustermgr管理命令

## 服务状态

展示集群状态，包括raft状态、空间状态、卷信息统计等等。

```bash
curl "http://127.0.0.1:9998/stat"
```

**响应示例**

```json
{
    "leader_host": "127.0.0.1:9998",
    "raft_status": {
        "applied": 291968826,
        "commit": 291968826,
        "leader": 3,
        "nodeId": 2,
        "peers": null,
        "raftApplied": 291968826,
        "raftState": "StateFollower",
        "term": 24,
        "transferee": 0,
        "vote": 3
    },
    "read_only": false,
    "space_stat": {
        "disk_stat_infos": [
            {
                "available": 107,
                "broken": 0,
                "dropped": 10,
                "dropping": 0,
                "expired": 0,
                "idc": "z0",
                "readonly": 22,
                "repaired": 17,
                "repairing": 0,
                "total": 134,
                "total_chunk": 55619,
                "total_free_chunk": 37979
            },
            {
                "available": 93,
                "broken": 0,
                "dropped": 10,
                "dropping": 0,
                "expired": 0,
                "idc": "z1",
                "readonly": 22,
                "repaired": 19,
                "repairing": 0,
                "total": 122,
                "total_chunk": 51523,
                "total_free_chunk": 33425
            },
            {
                "available": 96,
                "broken": 0,
                "dropped": 53,
                "dropping": 1,
                "expired": 0,
                "idc": "z2",
                "readonly": 46,
                "repaired": 3,
                "repairing": 0,
                "total": 152,
                "total_chunk": 58123,
                "total_free_chunk": 40173
            }
        ],
        "free_space": 1774492930453504,
        "total_blob_node": 17,
        "total_disk": 408,
        "total_space": 2155017090891776,
        "used_space": 380524160438272,
        "writable_space": 923847465369600
    },
    "volume_stat": {
        "active_volume": 345,
        "can_alloc_volume": 1651,
        "idle_volume": 1651,
        "lock_volume": 0,
        "total_volume": 1996,
        "unlocking_volume": 0
    }
}
```

## 节点添加

添加节点，指定节点类型，地址和id。

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1, "host": "127.0.0.1:9998", "member_type": 2}' "http://127.0.0.1:9998/member/add" 
```

**参数列表**

| 参数        | 类型   | 描述                               |
|-------------|--------|------------------------------------|
| peer_id     | uint64 | raft节点id，不可重复               |
| host        | string | 主机地址                           |
| member_type | uint8  | 节点类型，1表示leaner，2表示normal |


## 节点移除

根据id移除节点。

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/member/remove"
```

**参数列表**

| 参数    | 类型   | 描述                 |
|---------|--------|----------------------|
| peer_id | uint64 | raft节点id，不可重复 |


## 切主

根据id切换主节点。

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/leadership/transfer"
```

**参数列表**

| 参数    | 类型   | 描述                 |
|---------|--------|----------------------|
| peer_id | uint64 | raft节点id，不可重复 |


## 启动或禁用后台任务

| 任务类型(type) | 任务名(key)  | 开关(value) |
|----------------|--------------|-------------|
| 磁盘修复       | disk_repair  | true/false  |
| 数据均衡       | balance      | true/false  |
| 磁盘下线       | disk_drop    | true/false  |
| 数据删除       | blob_delete  | true/false  |
| 数据修补       | shard_repair | true/false  |
| 数据巡检       | vol_inspect  | true/false  |

查看任务状态
```bash
curl http://127.0.0.1:9998/config/get?key=balance
```

开启任务
```bash
curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"true"}' --header 'Content-Type: application/json'
```

关闭任务
```bash
curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"false"}' --header 'Content-Type: application/json'
```
