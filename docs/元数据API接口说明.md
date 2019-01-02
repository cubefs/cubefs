#元数据REST API接口说明
##元数据服务提供了部分REST API，便于用户快速查询元数据信息及目录信息及debug调试信息接口。REST API接口的监听端口是配置文件中prof指定的，详细的API接口即内容如下：
* 获取本机上的所有metaPartition信息
  >GET http://127.0.0.1:9092/getPartitions
  
  响应类型： application/json
  响应内容：
   ```json
    [{
        "partition_id": 1,
        "vol_name": "test",
        "start": 0,
        "end": 1000000,
        "peers": [{"node_id": 10, "addr":"192.168.1.1"},...]
    },...]
   ```

* 获取指定partition的meta信息
  >GET http://127.0.0.1:9092/getPartitionById?pid=100
 
  参数说明:
  
  |参数|类型|说明|
  |--:|--:|--:|
  |pid | integer| partition id|

  响应类型: application/json
  响应内容:
  ```json
  {
      "cursor":3472582,
      "leaderAddr":"10.196.31.173:9021",
      "nodeId":2,
      "peers":[
          {"id":1,"addr":"10.196.31.141:9021"},
          {"id":2,"addr":"10.196.31.173:9021"},
          {"id":3,"addr":"10.196.30.200:9021"}]
  }
  ```

* 获取指定Inode基本信息
  >GET http://127.0.0.1:9092/getInode?pid=100&ino=1024

  参数说明:
  |参数|类型|说明|
  |:---:|:---:|:---:|
  |pid | integer | partition id |
  |ino | integer | inode id |
  
  响应内容:
  ```json
  {
    "code": 200,
    "msg": "OK",
    "Data": {
      "Inode": 1024,
      "Type": 1,
      "Size": 1024,
      ...
    }
  }
  ```

* 获取指定Inode的数据存储信息
>GET http://127.0.0.1:9092/getExtetnsByInode?pid=100&ino=1024

参数说明:
|参数|类型|说明|
|:---:|:---:|:---:|
|pid | integer | partition id |
|ino | integer | inode id |

响应内容:
```json
{
  "code": 200,
  "msg": "OK",
  "data": {
    "gen": 100,
    "size": 1024,
    "eks": [
      {
        "FileOffset": 0,
        "PartitionId": 1000000,
        "ExtentId": 5000001,
        "ExtentOffset": 20,
        "Size": 100,
        "CRC": 1243},...]
  }
}
```
  
* 获取指定partition的所有Inode信息
  >GET http://127.0.0.1:9092/getAllInode?pid=100

  参数说明：

  |参数|类型|说明|
  |--:|--:|--:|
  |pid |integer| partition id|

* 获取指定partititon和指定inode的元信息
  >GET http://127.0.0.1:9092/getDentry?pid=100&parentIno=22&name="cfs"

  参数说明:
  |参数|类型|说明|
  |--:|--:|--:|
  |pid|integer| partition id|
  | parentIno| integer | parent Inode id |
  |name|string| directory/file name|

* 获取指定partition的所有dentry信息
>GET http://127.0.0.1:9092/getAllDentry?pid=100

参数说明:
| 参数 | 类型 | 说明 |
| ---:| ---:| ---:|
| pid | integer| partition id|