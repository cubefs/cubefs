Object Subsystem (ObjectNode)
==============================

How To Provide Object Storage Service with Object Subsystem (ObjectNode)
-------------------------------------------------------------------------

Start a ObjectNode process by execute the server binary of ChubaoFS you built with ``-c`` argument and specify configuration file.

.. code-block:: bash

   nohup cfs-server -c objectnode.json &


Configurations
-----------------------
Object Node using `JSON` format configuration file.


**Properties**

.. csv-table::
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to ``objectnode``", "Yes"
   "listen", "string", "
   | Listen and accept ip address and port of this server.
   | Format: ``IP:PORT`` or ``:PORT``
   | Default: ``:80``", "Yes"
   "domains", "string slice", "
   | Domain of S3-like interface which makes wildcard domain support
   | Format: ``DOMAIN``", "No"
   "logDir", "string", "Log directory", "Yes"
   "logLevel", "string", "
   | Level operation for logging.
   | Default: ``error``", "No"
   "masterAddr", "string slice", "
   | Format: ``HOST:PORT``.
   | HOST: Hostname, domain or IP address of master (resource manager).
   | PORT: port number which listened by this master", "Yes"
   "authNodes", "string slice", "
   | Format: *HOST:PORT*.
   | HOST: Hostname, domain or IP address of AuthNode.
   | PORT: port number which listened by this AuthNode", "Yes"
   "exporterPort", "string", "Port for monitor system", "No"
   "prof", "string", "Pprof port", "Yes"


**Example:**

.. code-block:: json

   {
        "role": "objectnode",
        "listen": "17410",
        "region": "cn_bj",
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

Fetch Authentication Keys
----------------------------
Authentication keys owned by volume and stored with volume view (volume topology) by Resource Manager (Master).
User can fetch it by using administration API, see **Get Volume Information** at :doc:`/admin-api/master/volume`

Using Object Storage Interface
-------------------------------
Object Subsystem (ObjectNode) provides S3-compatible object storage interface, so that you can operate files by using native Amazon S3 SDKs.

For detail about list of supported APIs, see **Supported S3-compatible APIs** at :doc:`/design/objectnode`

For detail about list of supported SDKs, see **Supported SDKs** at :doc:`/design/objectnode`
