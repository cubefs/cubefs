# Dentry调试

## 获取Dentry信息

``` bash
curl -v 'http://10.196.59.202:17220/getDentry?pid=100&name="aa.txt"&parentIno=1024'
```

| Parameter | Type    | Description               |
|-----------|---------|---------------------------|
| pid       | integer | meta partition id         |
| name      | string  | directory or file name    |
| parentIno | integer | parent directory inode id |

## 获取指定目录下全部文件

``` bash
curl -v 'http://10.196.59.202:17220/getDirectory?pid=100&parentIno=1024'
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | integer | partition id |
| ino       | integer | inode id     |


## 获取指定分片的全部目录信息

``` bash
curl -v 'http://10.196.59.202:17220/getAllDentry?pid=100'
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | integer | partition id |
