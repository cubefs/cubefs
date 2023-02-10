Console
======================

How To Start Console
---------------------

Start a Console process by execute the server binary of CubeFS you built with ``-c`` argument and specify configuration file.

.. code-block:: bash

   nohup cfs-server -c console.json &


Configurations
--------------

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to *console*", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "listen", "string", "Port of TCP network to be listen, default is 80", "Yes"
   "masterAddr", "string slice", "Addresses of master server", "Yes"
   "objectNodeDomain", "string", "object domain for sign url for down", "Yes"
   "monitor_addr", "string", "Prometheus the address", "Yes"
   "dashboard_addr", "string", "console menu forward to Grafana", "Yes"
   "monitor_app", "string", "the tag for monitor, it same as master config", "Yes"
   "monitor_cluster", "string", "the tag for monitor, it same as master config", "Yes"
   
**Example:**

.. code-block:: json

    {
      "role": "console",
      "logDir": "/cfs/log/",
      "logLevel": "debug",
      "listen": "80",
      "masterAddr": [
        "192.168.0.11:17010",
        "192.168.0.12:17010",
        "192.168.0.13:17010"
      ],
      "monitor_addr": "http://192.168.0.102:9090",
      "dashboard_addr": "http://grafana.cube.io",
      "monitor_app": "cfs",
      "monitor_cluster": "spark"
    }

Notice
-------------

  * add `127.0.0.1 console.cube.io grafana.cube.io` in `/etc/hosts`
  * you can visit it by `http://127.0.0.1:80`
  * in console default user is `root` default password is `CubeFSRoot`
  * If you are upgrading your CubeFS from v2.0.0 or earlier version, the password may not be compatible, you can use `curl -H "Content-Type:application/json" -X POST --data '{"id":"testuser","pwd":"12345","type":2}' "http://10.196.59.198:17010/user/create"` to create new user to use it
