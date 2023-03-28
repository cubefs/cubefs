# Scheduler管理

## 服务状态

初始化部署后默认后台任务是没有打开的，如果需要可以[Clustermgr管理章节](./cm.md)打开。

::: tip 提示
只有主节点才会显示均衡、下线、修盘、巡检以及手动迁移任务状态，所有Scheduler节点状态都包含删除、修补任务。
:::

```bash
curl http://127.0.0.1:9800/stats # 查看本机的节点状态
curl http://127.0.0.1:9800/stats/leader # 查看主节点状态
```

**响应示例**

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

## 手动迁移chunk

特殊情况下可以设置手动迁移某个chunk。

::: warning 注意
这里只能指定迁移的数据源，目标由Clustermgr分配。
:::

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"vuid": 4395630596,"direct_download": false}' "http://127.0.0.1:9800/manual/migrate/task/add" 
```

**参数说明**

| 参数              | 类型     | 描述                                         |
|-----------------|--------|--------------------------------------------|
| vuid            | uint64 | chunk id                                   |
| direct_download | bool   | 源chunk是否允许直接下载（源vuid所在数据如果损坏，则会通过纠删码修复的方式） |

## 查询后台任务

可以通过此命名查询某个后台任务的详细信息，如任务基本信息以及任务的执行状态信息。

```bash
curl http://127.0.0.1:9800/task/detail/:type/:id

# curl http://127.0.0.1:9800/task/detail/balance/balance-2678-387752-cg08egoi5d8une4a6cp0
```

**参数说明**

| 参数   | 类型     | 描述                                              |
|------|--------|-------------------------------------------------|
| type | string | disk_repair/balance/disk_drop/manual_migrate id |
| id   | string | 后台任务id                                          |

**响应示例**

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

## 查询下线或修盘任务进度

```bash
curl http://127.0.0.1:9800/stats/disk/migrating?task_type=xx&disk_id=xxx
```

| 参数        | 类型     | 描述                    |
|-----------|--------|-----------------------|
| task_type | string | disk_repair/disk_drop |
| disk_id   | int    | 磁盘id                  |

示例

```json
{
    "total_tasks_cnt": 10,
    "migrated_tasks_cnt":1
}
```

- total_tasks_cnt，表示总体任务数
- migrated_tasks_cnt，表示已完成任务数
