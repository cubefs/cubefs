#元数据REST API接口说明
##元数据服务提供了部分REST API，便于用户快速查询元数据信息及目录信息及debug调试信息接口。REST API接口的监听端口是配置文件中prof指定的，详细的API接口即内容如下：
* 获取本机上的所有metaPartition信息
  >GET http://127.0.0.1:9092/getAllPartitions
  
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
  >GET http://127.0.0.1:9092/getInodeInfo?id=100
 
  参数说明:
  
  |参数|类型|说明|
  |--:|--:|--:|
  |id | integer| partition id|

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
  
* 获取指定partition的所有Inode信息
  >GET http://127.0.0.1:9092/getInodeRange?id=100

  参数说明：

  |参数|类型|说明|
  |--:|--:|--:|
  |id |integer| partition id|

  响应内容:
  ```json
      {
          "Inode":1,
          "Type":1,
          "Size":0,
          "Generation":1,
          "CreateTime":1543718745,
          "AccessTime":1543718745,
          "ModifyTime":1543718745,
          "LinkTarget":null,
          "NLink":2,
          "MarkDelete":0,
          "Extents":{"Inode":1,"Extents":null}
    }
  ...
  ```
* 获取指定partititon和指定inode的元信息
  >GET http://127.0.0.1:9092/getExtents?pid=100&ino=203

  参数说明:
  |参数|类型|说明|
  |--:|--:|--:|
  |pid|integer| partition id|
  |ino|integer| inode id|

  响应类型: application/json
  响应内容:
  ```json
    {
        "Inode":1,
        "Type":1,
        "Size":0,
        "Generation":1,
        "CreateTime":1543718745,
        "AccessTime":1543718745,
        "ModifyTime":1543718745,
        "LinkTarget":null,
        "NLink":2,
        "MarkDelete":0,
        "Extents":{"Inode":1,"Extents":null}
    }
  ```
* 获取指定partition的所有dentry信息
>GET http://127.0.0.1:9092/getDentry?pid=100

参数说明:
| 参数 | 类型 | 说明 |
| ---:| ---:| ---:|
| pid | integer| partition id|

响应内容：
```json
{"ParentId":3,"Name":"1_8125","Inode":49761,"Type":0}
...
```