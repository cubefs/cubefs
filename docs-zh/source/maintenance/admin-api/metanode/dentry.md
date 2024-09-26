# Dentry调试

## 获取Dentry信息

``` bash
curl -v "http://10.196.59.202:17220/getDentry?pid=100&name="aa.txt"&parentIno=1024"
```

| Parameter | Type    | Description               |
|-----------|---------|---------------------------|
| pid       | int | 元数据分片 ID         |
| name      | string  | 目录或文件名    |
| parentIno | int | 父目录 inode id |

## 获取指定目录下全部文件

``` bash
curl -v "http://10.196.59.202:17220/getDirectory?pid=100&parentIno=1024"
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | int | 元数据分片 ID |
| ino       | int | inode ID     |


## 获取指定分片的全部目录信息

``` bash
curl -v "http://10.196.59.202:17220/getAllDentry?pid=100"
```

| Parameter | Type    | Description  |
|-----------|---------|--------------|
| pid       | integer | 元数据分片 ID |
