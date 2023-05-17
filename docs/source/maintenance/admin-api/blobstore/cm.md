# Clustermgr Management

## Service Status

Displays the cluster status, including raft status, space status, volume information statistics, and more.

```bash
curl "http://127.0.0.1:9998/stat"
```

**Response Example**

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

## Node Management

### Add Node

Add a node by specifying the node type, address, and ID.

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1, "host": "127.0.0.1:10110","node_host": "127.0.0.1:9998", "member_type": 2}' "http://127.0.0.1:9998/member/add"
```

**Parameter List**

| Parameter   | Type   | Description                           |
|-------------|--------|---------------------------------------|
| peer_id     | uint64 | Raft node ID, must be unique          |
| host        | string | Raft address                          |
| node_host   | string | Service address                       |
| member_type | uint8  | Node type, 1 for leaner, 2 for normal |

### Remove Node

Remove a node by ID.

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/member/remove"
```

**Parameter List**

| Parameter | Type   | Description                  |
|-----------|--------|------------------------------|
| peer_id   | uint64 | Raft node ID, must be unique |


### Switch Leader

Switch the leader node based on the ID.

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/leadership/transfer"
```

**Parameter List**

| Parameter | Type   | Description                  |
|-----------|--------|------------------------------|
| peer_id   | uint64 | Raft node ID, must be unique |

## Disk Management

### Get Disk Information

```bash
curl "http://127.0.0.1:9998/disk/info?disk_id=1"
```

**Parameter List**

| Parameter | Type   | Description |
|-----------|--------|-------------|
| disk_id   | uint32 | Disk ID     |

**Response Example**

```
{
    "cluster_id": 10001,
    "create_time": "2022-05-07T15:22:01.627271402+08:00",
    "disk_id": 1,
    "free": 1910475022336,
    "free_chunk_cnt": 106,
    "host": "http://127.0.0.1:8889",
    "idc": "bjht",
    "last_update_time": "2022-05-07T15:22:01.627271402+08:00",
    "max_chunk_cnt": 1037,
    "path": "/home/service/var/data21",
    "rack": "HT02-B11-F4-402-0203",
    "readonly": false,
    "size": 17828005326848,
    "status": 1,
    "used": 15917530304512,
    "used_chunk_cnt": 931
}
```

### Set Disk Status

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"disk_id":2,"status":2}' "http://127.0.0.1:9998/disk/set"
```

| Parameter | Type   | Description                                                                            |
|-----------|--------|----------------------------------------------------------------------------------------|
| disk_id   | uint32 | Disk ID                                                                                |
| status    | uint8  | Disk status can only be increased, numerical description refers to the following table |

| Disk Status Value | Description |
|-------------------|-------------|
| 1                 | normal      |
| 2                 | broken      |
| 3                 | repairing   |
| 4                 | repaired    |
| 5                 | dropped     |

### Set Disk Read and Write

Set the disk to read-only or read-write.

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"disk_id":2,"readonly":false}' "http://127.0.0.1:9998/disk/access"
```

**Parameter List**

| Parameter | Type   | Description                                                           |
|-----------|--------|-----------------------------------------------------------------------|
| disk_id   | uint32 | Disk ID                                                               |
| readonly  | bool   | Whether it is read-only, true means read-only, false means read-write |

### Set Disk Drop

For the scenario where the machine or disk is over-protected, we can set the disk drop for data migration. During the migration, the offline disk data will be read first. If the reading fails, the recovery and reading process will be followed.

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"disk_id":2}' "http://127.0.0.1:9998/disk/drop"
```

## Volume Management

### Get Volume Information

Get information for a single volume.

```bash
curl "http://127.0.0.1:9998/volume/get?vid=1"
```

**Parameter List**

| Parameter | Type   | Description |
|-----------|--------|-------------|
| vid       | uint32 | Volume ID   |

**Response Example**

```
{
    "code_mode": 12,
    "create_by_node_id": 1,
    "free": 1061027840,
    "health_score": 0,
    "status": 1,
    "total": 171798691840,
    "units": [
        {
            "disk_id": 112,
            "host": "http://127.0.0.1:8889",
            "vuid": 4294967654
        },
        ...
        {
            "disk_id": 401,
            "host": "http://127.0.0.1:8889",
            "vuid": 4513071462
        }
    ],
    "used": 170737664000,
    "vid": 1
}
```

## Background Tasks

| Task Type (type) | Task Name (key) | Switch (value) |
|------------------|-----------------|----------------|
| Disk Repair      | disk_repair     | true/false     |
| Data Balancing   | balance         | true/false     |
| Disk Offline     | disk_drop       | true/false     |
| Data Deletion    | blob_delete     | true/false     |
| Data Repair      | shard_repair    | true/false     |
| Data Inspection  | vol_inspect     | true/false     |

View task status

```bash
curl http://127.0.0.1:9998/config/get?key=balance
# or use blobstore-cli
blobstore-cli cm config background status balance
```

Enable task

```bash
curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"true"}' --header 'Content-Type: application/json'
# or use blobstore-cli
blobstore-cli cm config background enable balance
```

Disable task

```bash
curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"false"}' --header 'Content-Type: application/json'
# or use blobstore-cli
blobstore-cli cm config background disable balance
```
