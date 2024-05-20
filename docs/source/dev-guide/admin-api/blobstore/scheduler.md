# Scheduler Management

## Service Status

By default, the background tasks are not enabled after initialization and deployment. If needed, they can be enabled in the [Clustermgr Management section](./cm.md).

::: tip Note
Only the status of balancing, offline, repairing, inspection, and manual migration tasks will be displayed on the main node. The status of all Scheduler node includes deletion and repair tasks.
:::

```bash
curl http://127.0.0.1:9800/stats # View the node status of the local machine
curl http://127.0.0.1:9800/stats/leader # View the status of the main node
```

::: tip Note
v3.3.0 use `stats/leader` and older is `leader/stats`
:::

**Response Example**

```json
{
  "disk_repair":{
    "enable":true,
    "repairing_disks":[],
    "total_tasks_cnt":0,
    "repaired_tasks_cnt":0,
    "preparing_cnt":0,
    "worker_doing_cnt":0,
    "finishing_cnt":0,
    "stats_per_min":{
      "finished_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "shard_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "data_amount_byte":"[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
    }
  },
  "disk_drop":{
    "enable":true,
    "dropping_disks":[],
    "total_tasks_cnt":0,
    "dropped_tasks_cnt":0,
    "preparing_cnt":0,
    "worker_doing_cnt":0,
    "finishing_cnt":0,
    "stats_per_min":{
      "finished_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "shard_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "data_amount_byte":"[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
    }
  },
  "balance":{
    "enable":true,
    "preparing_cnt":1,
    "worker_doing_cnt":0,
    "finishing_cnt":0,
    "stats_per_min":{
      "finished_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "shard_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "data_amount_byte":"[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
    }
  },
  "manual_migrate":{
    "preparing_cnt":0,
    "worker_doing_cnt":0,
    "finishing_cnt":0,
    "stats_per_min":{
      "finished_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "shard_cnt":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
      "data_amount_byte":"[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
    }
  },
  "volume_inspect":{
    "enable":false,
    "finished_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
    "time_out_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]"
  },
  "shard_repair":{
    "enable":true,
    "success_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
    "failed_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
    "total_err_cnt":0,
    "err_stats":null
  },
  "blob_delete":{
    "enable":true,
    "success_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
    "failed_per_min":"[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
    "total_err_cnt":0,
    "err_stats":null
  }
}
```

## Manual Chunk Migration

In special cases, you can manually migrate a chunk.

::: warning Note
You can only specify the data source for migration, and the target is assigned by Clustermgr.
:::

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"vuid": 4395630596,"direct_download": false}' "http://127.0.0.1:9800/manual/migrate/task/add" 
```

**Parameter Description**

| Parameter       | Type   | Description                                                                                                                                             |
|-----------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| vuid            | uint64 | Chunk ID                                                                                                                                                |
| direct_download | bool   | Whether the source chunk can be downloaded directly (if the data where the source VUID is located is damaged, it will be repaired by Reed-Solomon code) |

## Query Background Tasks

You can use this command to query detailed information about a background task, such as task basic information and task execution status information.

```bash
curl http://127.0.0.1:9800/task/detail/:type/:id

# curl http://127.0.0.1:9800/task/detail/balance/balance-2678-387752-cg08egoi5d8une4a6cp0
```

**Parameter Description**

| Parameter | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| type      | string | disk_repair/balance/disk_drop/manual_migrate task ID |
| id        | string | Background task ID                                   |

**Response Example**

```json
{
    "stat": {
        "done_count": 1045,
        "done_size": 856426665,
        "progress": 5,
        "total_count": 17552,
        "total_size": 14473209599
    },
    "task": {
        "code_mode": 12,
        "ctime": "2023-03-02 19:23:47.09638336 +0800 CST m=+80.601918285",
        "destination": {
            "disk_id": 6027,
            "host": "http://127.0.0.1:8889",
            "vuid": 1665382175735812
        },
        "finish_advance_reason": "",
        "forbidden_direct_download": false,
        "mtime": "2023-03-02 19:23:48.251012267 +0800 CST m=+81.756547201",
        "source_disk_id": 2678,
        "source_idc": "test",
        "source_vuid": 1665382175735811,
        "sources": [
            {
                "disk_id": 2939,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382158958593
            },
            {
                "disk_id": 2678,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382175735811
            },
            {
                "disk_id": 4467,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382192513026
            },
            {
                "disk_id": 5987,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382209290243
            },
            {
                "disk_id": 6190,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382226067459
            },
            {
                "disk_id": 2744,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382242844678
            },
            {
                "disk_id": 6276,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382259621892
            },
            {
                "disk_id": 6095,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382276399106
            },
            {
                "disk_id": 5823,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382293176325
            },
            {
                "disk_id": 6315,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382309953539
            },
            {
                "disk_id": 5918,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382326730756
            },
            {
                "disk_id": 6278,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382343507971
            },
            {
                "disk_id": 6138,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382360285186
            },
            {
                "disk_id": 5411,
                "host": "http://127.0.0.1:8889",
                "vuid": 1665382377062403
            }
        ],
        "state": 2,
        "task_id": "balance-2678-387752-cg08egoi5d8une4a6cp0",
        "task_type": "balance",
        "worker_redo_cnt": 0
    }
}
```

## Query Offline or Repair Task Progress

::: tip Note
New interface in v3.3.0
:::

```bash
curl http://127.0.0.1:9800/stats/disk/migrating?task_type=xx&disk_id=xxx
```

| Parameter | Type   | Description           |
|-----------|--------|-----------------------|
| task_type | string | disk_repair/disk_drop |
| disk_id   | int    | Disk ID               |

Example

```json
{
    "total_tasks_cnt": 10,
    "migrated_tasks_cnt":1
}
```

- total_tasks_cnt: Total number of tasks
- migrated_tasks_cnt: Number of completed tasks
