# Inode 管理

## 获取指定 Inode 基本信息

``` bash
curl -v "http://192.168.0.22:17220/getInode?pid=1&ino=1024"
```

请求参数：

| 参数  | 类型  | 描述       |
|-----|-----|----------|
| pid | int  | 分片 id     |
| ino | int  | inode id |

## 获取指定 Inode 的数据存储信息

``` bash
curl -v "http://192.168.0.22:17220/getExtentsByInode?pid=1&ino=1024"
```

请求参数：

| 参数  | 类型  | 描述       |
|-----|-----|----------|
| pid | int  | 分片 id     |
| ino | int  | inode id |

## 获取指定元数据分片的全部 inode 信息

``` bash
curl -v "http://192.168.0.22:17220/getAllInodes?pid=1"
```

请求参数：

| 参数  | 类型  | 描述    |
|-----|-----|-------|
| pid | int  | 分片 id |

## 获取inode上的ebs分片信息

``` bash
curl -v "192.168.0.22:17220/getEbsExtentsByInode?pid=282&ino=16797167"
```

请求参数：

| 参数  | 类型  | 描述       |
|-----|-----|----------|
| pid | int  | 分片 id    |
| ino | int  | inode id |