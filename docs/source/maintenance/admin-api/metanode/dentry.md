# Debugging Dentries

## Obtaining Dentry Information

``` bash
curl -v 'http://10.196.59.202:17210/getDentry?pid=100&name="aa.txt"&parentIno=1024'
```

| Parameter | Type    | Description               |
|-----------|---------|---------------------------|
| pid       | integer | meta partition id         |
| name      | string  | directory or file name    |
| parentIno | integer | parent directory inode id |

## Obtaining All Files in a Specified Directory

``` bash
curl -v 'http://10.196.59.202:17210/getDirectory?pid=100&parentIno=1024'
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | integer | partition id |
| ino       | integer | inode id     |


## Obtaining All Directory Information for a Specified Shard

``` bash
curl -v 'http://10.196.59.202:17210/getAllDentry?pid=100'
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | integer | partition id |
