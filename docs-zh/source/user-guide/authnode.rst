授权节点
====================

authnode负责授权客户端对CubeFS的Master节点的访问。通过此文档可以创建一个authnode docker-compose试用集群。

authnode功能的整体流程是：创建key --> 使用key获取访问指定服务的ticket --> 使用ticket访问服务。

编译构建
---------------

使用如下命令构建authtool及相关的依赖：

.. code-block:: bash

   $ git clone http://github.com/cubeFS/cubefs.git
   $ cd cubefs
   $ make build

如果构建成功，将在 `build/bin` 目录中生成可执行文件 `cfs-authtool` 。


配置文件
--------------

- 创建anthnode的key：

  .. code-block:: bash

    $ ./cfs-authtool authkey

  执行命令后，将在当前目录下生成 ``authroot.json`` 和 ``authservice.json`` 两个key文件。
  
  示例 ``authservice.json`` ：

  .. code-block:: json
  
    {
        "id": "AuthService",
        "key": "9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
        "create_ts": 1573801212,
        "role": "AuthService",
        "caps": "{\"*\"}"
    }

- 在 `docker/conf` 目录下，编辑 ``authnode.json`` 配置文件：
  
  将 ``authroot.json`` 文件中的 ``key`` 值作为 ``authRootKey`` 的值。
  
  将 ``authservice.json`` 文件中的 ``key`` 值作为 ``authServiceKey`` 的值。

  示例 ``authnode.json`` ：

  .. code-block:: json

    {
         "role": "authnode",
         "ip": "192.168.0.14",
         "port": "8080",
         "prof":"10088",
         "id":"1",
         "peers": "1:192.168.0.14:8080,2:192.168.0.15:8081,3:192.168.0.16:8082",
         "retainLogs":"2",
         "logDir": "/export/Logs/authnode",
         "logLevel":"info",
         "walDir":"/export/Data/authnode/raft",
         "storeDir":"/export/Data/authnode/rocksdbstore",
         "exporterPort": 9510,
         "consulAddr": "http://consul.prometheus-cfs.local",
         "clusterName":"test",
         "authServiceKey":"9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
         "authRootKey":"MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE=",
         "enableHTTPS":false
    }

启动集群
------------

在 `docker/authnode` 目录下，执行以下命令创建authnode集群

.. code-block:: bash

  $ docker-compose up -d

使用授权功能
-------------

- 授权准备


  * 获取authService的ticket
  
    .. code-block:: bash

      $ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=authservice.json -output=ticket_auth.json getticket AuthService

    输入：

        host：authnode的访问地址

        keyfile：需要获取ticket的用户key文件路径，是“创建key”操作输出的key文件

    输出：

        output：存放ticket的文件路径
    
    示例 ``ticket_auth.json`` ：
    
    .. code-block:: json

      {
          "id": "AuthService",
          "session_key": "A9CSOGEN9CFYhnFnGwSMd4WFDBVbGmRNjaqGOhOinJE=",
          "service_id": "AuthService",
          "ticket": "RDzEiRLX1xjoUyp2TDFviE/eQzXGlPO83siNJ3QguUrtpwiHIA3PLv4edyKzZdKcEb3wikni8UxBoIJRhKzS00+nB7/9CjRToAJdT9Glhr24RyzoN8psBAk82KEDWJhnl+Y785Av3f8CkNpKv+kvNjYVnNKxs7f3x+Ze7glCPlQjyGSxqARyLisoXoXbiE6gXR1KRT44u7ENKcUjWZ2ZqKEBML9U4h0o58d3IWT+n4atWKtfaIdp6zBIqnInq0iUueRzrRlFEhzyrvi0vErw+iU8w3oPXgTi+um/PpUyto20c1NQ3XbnkWZb/1ccx4U0"
      }

  * 创建管理员用户
  
    .. code-block:: bash
  
      $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_auth.json -data=data_admin.json -output=key_admin.json AuthService createkey
    
    输入：
    
        ticketfile：上一步骤所得ticket文件的路径，使用ticket才能访问相关服务
        
        data：需要注册的管理员用户信息
    
    示例 ``data_admin.json`` ：
    
    .. code-block:: json
    
      {
        "id": "admin",
        "role": "client",
        "caps": "{\"API\":[\"*:*:*\"]}"
      }
    
    输出：
    
        output：管理员用户的key文件路径，key文件格式同前述操作所输出的key文件
    
- 管理员授权用户


  * 管理员获取ticket
  
    .. code-block:: bash

      $ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_admin.json -output=ticket_admin.json getticket AuthService
  
  * 管理员创建新的授权用户
  
    .. code-block:: bash

      $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey
  
  * 授权用户获取访问服务的ticket
  
  
    例如，访问MasterService，可以执行以下命令获取ticket：
    
    .. code-block:: bash

      $ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_client.json -output=ticket_client.json getticket MasterService
 
在CubeFS集群中添加授权功能
-------------------------------

- 为Master节点创建key

  .. code-block:: bash

    $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_master.json -output=key_master.json AuthService createkey

  示例 ``data_master`` ：
  
  .. code-block:: json
    
    {
        "id": "MasterService",
        "role": "service",
        "caps": "{\"API\":[\"*:*:*\"]}"
    }
  
  执行命令后，将 ``key_master.json`` 中 ``key`` 的值作为 ``masterServiceKey`` 的值写入配置文件 ``master.json`` 中。

- 为客户端创建key

  .. code-block:: bash

    $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey

  示例 ``data_client`` ：
    
  .. code-block:: json
  
    {
        "id": "ltptest",
        "role": "client",
        "caps": "{\"API\":[\"*:*:*\"]}"
    }

  参数说明：
  
      id：volname名称。
      
      role：有client和service两种。
      
      caps：格式为"{\"API\":[\"master:getVol:access\"]}"，设为*表示所有API均可访问。
  
  执行命令后，将 ``key_client.json`` 中 ``key`` 的值作为 ``clientKey`` 的值写入配置文件 ``client.json`` 中。
  
  示例 ``client.json`` ：
  
  .. code-block:: json
  
    {
        "masterAddr": "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010",
        "mountPoint": "/cfs/mnt",
        "volName": "ltptest",
        "owner": "ltptest",
        "logDir": "/cfs/log",
        "logLevel": "info",
        "consulAddr": "http://192.168.0.100:8500",
        "exporterPort": 9500,
        "profPort": "17410",
        "authenticate": true,
        "ticketHost": "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082",
        "clientKey": "jgBGSNQp6mLbu7snU8wKIdEkytzl+pO5/OZOJPpIgH4=",
        "enableHTTPS": "false"
    }
    
  参数说明：
  
      authenticate：是否需要权限认证。设为true表示当前Vol需要进行权限认证。
      
      ticketHost：authnode集群的节点信息。
      
      clientKey：分发给client的key。
      
      enableHTTPS：是否使用https协议传输。
  
- 启动CubeFS集群

  .. code-block:: bash
  
    $ docker/run_docker.sh -r -d /data/disk

  在客户端的启动过程中，会先使用clientKey从authnode节点处获取访问Master节点的ticket，再使用ticket访问Master API。因此，只有被受权的客户端才能成功启动并挂载。
