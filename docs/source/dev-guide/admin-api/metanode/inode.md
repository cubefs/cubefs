# Inode Management

## Obtaining Basic Information for a Specified Inode

``` bash
curl -v "http://192.168.0.22:17220/getInode?pid=1&ino=1024"
```

Request Parameters:

| Parameter | Type    | Description |
|-----------|---------|-------------|
| pid       | Integer | Shard ID    |
| ino       | Integer | Inode ID    |

## Obtaining Data Storage Information for a Specified Inode

``` bash
curl -v "http://192.168.0.22:17220/getExtentsByInode?pid=1&ino=1024"
```

Request Parameters:

| Parameter | Type    | Description |
|-----------|---------|-------------|
| pid       | Integer | Shard ID    |
| ino       | Integer | Inode ID    |

## Obtaining All Inode Information for a Specified Metadata Shard

``` bash
curl -v "http://192.168.0.22:17220/getAllInodes?pid=1"
```

Request Parameters:

| Parameter | Type    | Description |
|-----------|---------|-------------|
| pid       | Integer | Shard ID    |

## Obtaining EBS Shard Information for an Inode

``` bash
curl -v "192.168.0.22:17220/getEbsExtentsByInode?pid=282&ino=16797167"
```

Request Parameters:

| Parameter | Type    | Description |
|-----------|---------|-------------|
| pid       | Integer | Shard ID    |
| ino       | Integer | Inode ID    |