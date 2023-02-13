对象存储(ObjectNode)
==============================

如何部署对象存储服务
-------------------------------------------------------------------------

通过执行CubeFS的二进制文件并用“-c”参数指定的配置文件来启动一个ObjectNode进程。

.. code-block:: bash

   nohup cfs-server -c objectnode.json &

如果不打算使用对象存储功能，无需启动ObjectNode节点。

配置
-----------------------
对象管理节点使用 `JSON` 合适的配置文件


**属性**

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "role", "string", "进程角色，必须设置为 ``objectnode``", "是"
   "listen", "string", "
   | http服务监听的IP地址和端口号.
   | 格式: ``IP:PORT`` 或者 ``:PORT``
   | 默认: ``:80``", "是"
   "domains", "string slice", "
   | 为S3兼容接口配置域名以支持DNS风格访问资源
   | 格式: ``DOMAIN``", "否"
   "logDir", "string", "日志存放路径", "是"
   "logLevel", "string", "
   | 日志级别.
   | 默认: ``error``", "否"
   "masterAddr", "string slice", "
   | 格式: ``HOST:PORT``.
   | HOST: 资源管理节点IP（Master）.
   | PORT: 资源管理节点服务端口（Master）", "是"
   "exporterPort", "string", "prometheus获取监控数据端口", "否"
   "prof", "string", "调试和管理员API接口", "是"


**示例:**

.. code-block:: json

   {
        "role": "objectnode",
        "listen": "17410",
        "domains": [
            "object.cfs.local"
        ],
        "logDir": "/cfs/Logs/objectnode",
        "logLevel": "info",
        "masterAddr": [
	        "10.196.59.198:17010",
            "10.196.59.199:17010",
            "10.196.59.200:17010"
        ],
        "exporterPort": 9503,
        "prof": "7013"
   }

获取鉴权密钥
----------------------------
鉴权秘钥由各用户所有，存储于用户信息中。

创建用户可以参见链接：:doc:`/admin-api/master/user`。

如已创建用户，用户可以通过链接中的相关API获取用户信息，以获取鉴权密钥 *Access Key* 和 *Secret Key* 。

对象存储接口使用方法
-------------------------------
对象子系统（ObjectNode）提供S3兼容的对象存储接口，所以可以直接使用原生的Amazon S3 SDKs来使用系统。

对象存储功能中，使用的 ``Region`` 变量为 **集群名称** 。

通过 **Supported S3-compatible APIs** 获取更详细的信息，地址： :doc:`/design/objectnode`

通过 **Supported SDKs** 获取详细的SDK信息，地址： :doc:`/design/objectnode`
