# BlobNode Management

## Service Status

View the status information of all disks managed by the specified management, including available space, used space, read and write status, and the number of used chunks.

```bash
curl http://127.0.0.1:8889/stat
```

**Response Example**

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

## View Information of Specified Disk

```bash
curl http://127.0.0.1:8889/disk/stat/diskid/259
```

**Response Example**

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

## Register Disk

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"path":"/home/service/disks/data11"}' "http://127.0.0.1:8889/disk/probe" 
```