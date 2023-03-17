# Blobnode管理

## 服务状态

查看指定管理的所有磁盘可用空间，已用空间，读写状态，已用chunk数等状态信息。

```bash
curl http://127.0.0.1:8889/stat
```

**响应示例**

```json
[
    {
        "cluster_id": 100,
        "create_time": "2021-10-12T17:31:59.159436462+08:00",
        "disk_id": 259,
        "free": 6534303547392,
        "free_chunk_cnt": 0,
        "host": "http://127.0.0.1:8889",
        "idc": "z2",
        "last_update_time": "2021-10-12T17:31:59.159436462+08:00",
        "max_chunk_cnt": 0,
        "path": "/home/service/var/data6",
        "rack": "testrack",
        "readonly": false,
        "size": 7833890668544,
        "status": 1,
        "used": 1299587121152,
        "used_chunk_cnt": 129
    }
]

```

## 查看指定磁盘信息

```bash
curl http://127.0.0.1:8889/disk/stat/diskid/259
```

**响应示例**

```json
{
  "cluster_id": 100,
  "create_time": "2021-10-12T17:31:59.159436462+08:00",
  "disk_id": 259,
  "free": 6534319898624,
  "free_chunk_cnt": 0,
  "host": "http://127.0.0.1:8889",
  "idc": "z2",
  "last_update_time": "2021-10-12T17:31:59.159436462+08:00",
  "max_chunk_cnt": 0,
  "path": "/home/service/var/data6",
  "rack": "testrack",
  "readonly": false,
  "size": 7833890668544,
  "status": 1,
  "used": 1299570769920,
  "used_chunk_cnt": 129
}
```

## 注册磁盘

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"path":"/home/service/disks/data11"}' "http://127.0.0.1:8889/disk/probe" 
```