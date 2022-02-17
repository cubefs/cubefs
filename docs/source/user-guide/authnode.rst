Authnode
====================

`Authnode` provides a general authentication & authorization service among `ChubaoFS` nodes. `Client`, `Master`, `Meta` and `Data` node are required to be authenticated and authorized before any resource access in another node.

Initially, each node (`Auth`, `Client`, `Master`, `Meta` or `Data` node) is launched with a secure key which is distributed by a authenticated person (for instance, cluster admin). With a valid key, a node can be identified in `Authnode` service and granted a ticket for resource access.

The overall work flow is: key creation and distribution --> ticket retrieval with key --> resource access with ticket.

Concepts
----------
- Key: a bit of secret shared data between a node and `Authnode` that asserts identity of a client.

- Ticket: a bit of data that cryptographically asserts identity and authorization (through a list of capabilities) for a service for a period of time.

- Capability: a capability is defined in the format of `node:object:action` where `node` refers to a service node (such as `auth`, `master`, `meta` or `data`), `object` refers to the resource and `action` refers to permitted activities (such as read, write and access). See examples below.

.. csv-table:: Capability Example
   :header: "Capability", "Specifications"

   "auth:createkey:access", "Access permission for createkey in Authnode"
   "master:\*:\*", "Any permissions for any objects in Master node"
   "\*:\*:\*", "Any permissions for any objects in any nodes"



Client Tool
------------

cfs-authtool is a client-side utility of `Authnode`, providing key managements (creation, view and modification) and ticket retrieval in and from `Authnode` keystore.
In particular, Each key is associated with an entity name or id, secret key string, creation time, role and capability specification.


.. csv-table:: Key structure
   :header: "Key", "Type", "Description"

   "id", "string", "Unique key identifier composed of letters and digits"
   "key", "string", "Base64 encoded secret key"
   "role", "string", "The role of the key (either client or service)"
   "caps", "string", "The capabilities of the key"


Build
~~~~~~~~
Use the following commands to build client side tool for `Authnode`:

.. code-block:: bash

   $ git clone http://github.com/cubefs/cubefs.git
   $ cd chubaofs
   $ make build

If successful, the tool `cfs-authtool` can be found in `build/bin`.


Synopsis
~~~~~~~~~~~
cfs-authtool ticket -host=AuthNodeAddress [-keyfile=Keyfile] [-output=TicketOutput] [-https=TrueOrFalse -certfile=AuthNodeCertfile] TicketService Service

cfs-authtool api -host=AuthNodeAddress -ticketfile=TicketFile [-data=RequestDataFile] [-output=KeyFile] [-https=TrueOrFalse -certfile=AuthNodeCertfile] Service Request

cfs-authtool authkey [-keylen=KeyLength]

TicketService := [getticket]

Service := [AuthService | MasterService | MetaService | DataService]

Request := [createkey | deletekey | getkey | addcaps | deletecaps | getcaps | addraftnode | removeraftnode]



AuthNode Configurations
------------------------

`Authnode` use **JSON** as configuration file format.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to master", "Yes"
   "ip", "string", "host ip", "Yes"
   "port", "string", "Http port which api service listen on", "Yes"
   "prof", "string", "golang pprof port", "Yes"
   "id", "string", "identy different master node", "Yes"
   "peers", "string", "the member information of raft group", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*.", "No"
   "retainLogs", "string", "the number of raft logs will be retain.", "Yes"
   "walDir", "string", "Path for raft log file storage.", "Yes"
   "storeDir", "string", "Path for RocksDB file storage,path must be exist", "Yes"
   "clusterName", "string", "The cluster identifier", "Yes"
   "exporterPort", "int", "The prometheus exporter port", "No"
   "authServiceKey", "string", "The secret key used for authentication of AuthNode", "Yes"
   "authRootKey", "string", "The secret key used for key derivation (session and client secret key)", "Yes"
   "enableHTTPS", "bool", "Option whether enable HTTPS protocol", "No"


**Example:**

   .. code-block:: json

      {
        "role": "authnode",
        "ip": "192.168.0.14",
        "port": "8080",
        "prof":"10088",
        "id":"1",
        "peers": "1:192.168.0.14:8080,2:192.168.0.15:8081,3:192.168.0.16:8082",
        "logDir": "/export/Logs/authnode",
        "logLevel":"info",
        "retainLogs":"100",
        "walDir":"/export/Data/authnode/raft",
        "storeDir":"/export/Data/authnode/rocksdbstore",
        "exporterPort": 9510,
        "consulAddr": "http://consul.prometheus-cfs.local",
        "clusterName":"test",
        "authServiceKey":"9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
        "authRootKey":"wbpvIcHT/bLxLNZhfo5IhuNtdnw1n8kom+TimS2jpzs=",
        "enableHTTPS":false
      }


Steps for Starting ChubaoFS with AuthNode
------------------------------------------

Create Authnode key
~~~~~~~~~~~~~~~~~~~~~~
Run the command:

  .. code-block:: bash

    $ ./cfs-authtool authkey

If successful, two key files can be generated ``authroot.json`` and ``authservice.json`` under current directory.
They represent `authServiceKey` and `authRootKey` respectively.
 
example ``authservice.json`` :

  .. code-block:: json

    {
         "id": "AuthService",
         "key": "9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
         "create_ts": 1573801212,
         "role": "AuthService",
         "caps": "{\"*\"}"
    }


Edit ``authnode.json`` in `docker/conf` as following:

  - ``authRootKey``: use the value of ``key`` in ``authroot.json``
  - ``authServiceKey``: use the value of ``key`` in ``authService.json``

  example ``authnode.json`` :

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
         "authRootKey":"wbpvIcHT/bLxLNZhfo5IhuNtdnw1n8kom+TimS2jpzs=",
         "enableHTTPS":false
    }

Start Authnode Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~

In directory `docker/authnode`, run the following command to start a `Authnode` cluster.

.. code-block:: bash

  $ docker-compose up -d


Create `admin` in Authnode
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get `Authnode` ticket using `authServiceKey`:

  .. code-block:: bash

    $ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=authservice.json -output=ticket_auth.json getticket AuthService

   
    example ``ticket_auth.json`` :

    .. code-block:: json

      {
          "id": "AuthService",
          "session_key": "A9CSOGEN9CFYhnFnGwSMd4WFDBVbGmRNjaqGOhOinJE=",
          "service_id": "AuthService",
          "ticket": "RDzEiRLX1xjoUyp2TDFviE/eQzXGlPO83siNJ3QguUrtpwiHIA3PLv4edyKzZdKcEb3wikni8UxBoIJRhKzS00+nB7/9CjRToAJdT9Glhr24RyzoN8psBAk82KEDWJhnl+Y785Av3f8CkNpKv+kvNjYVnNKxs7f3x+Ze7glCPlQjyGSxqARyLisoXoXbiE6gXR1KRT44u7ENKcUjWZ2ZqKEBML9U4h0o58d3IWT+n4atWKtfaIdp6zBIqnInq0iUueRzrRlFEhzyrvi0vErw+iU8w3oPXgTi+um/PpUyto20c1NQ3XbnkWZb/1ccx4U0"
      }

Create `admin` using `Authnode` ticket:

  .. code-block:: bash

    $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_auth.json -data=data_admin.json -output=key_admin.json AuthService createkey


    example ``data_admin.json`` :

    .. code-block:: json

      {
          "id": "admin",
          "role": "service",
          "caps": "{\"API\":[\"*:*:*\"]}"
      }

Create key for ChubaoFS cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


- Get `Authnode` ticket using `admin` key:

 .. code-block:: bash

  $ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_admin.json -output=ticket_admin.json getticket AuthService


- Create key for Master

 .. code-block:: bash

   $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_master.json -output=key_master.json AuthService createkey

    example ``data_master.json`` :

    .. code-block:: json

      {
          "id": "MasterService",
          "role": "service",
          "caps": "{\"API\":[\"*:*:*\"]}"
      }

   Specifications:
        id: will set `Client` ID

        role: will set the role of id

        caps: will set the capabilities of id

 Edit ``master.json`` as following:
  - ``masterServiceKey``: use the value of ``key`` in ``key_master.json``

- Create key for Client

  .. code-block:: bash

    $ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey

  example ``data_client.json``:

  .. code-block:: json

    {
        "id": "ltptest",
        "role": "client",
        "caps": "{\"API\":[\"*:*:*\"], \"Vol\":[\"*:*:*\"]}"
    }

  Edit ``client.json`` as following:
   ``clientKey``: use the value of ``key`` in ``key_client.json``

  example ``client.json`` ：

  .. code-block:: json

    {
        "masterAddr": "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010",
        "mountPoint": "/cfs/mnt",
        "volName": "ltptest",
        "owner": "ltptest",
        "logDir": "/cfs/log",
        "logLevel": "info",
        "consulAddr": "http://192.168.0.101:8500",
        "exporterPort": 9500,
        "profPort": "17410",
        "authenticate": true,
        "ticketHost": "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082",
        "clientKey": "jgBGSNQp6mLbu7snU8wKIdEkytzl+pO5/OZOJPpIgH4=",
        "enableHTTPS": "false"
    }

 Specifications:
      authenticate: will enable authentication flow if set true.

      ticketHost: will set the IP/URL of `Authnode` cluster.

      clientKey: will set the key generated by `Authnode`

      enableHTTPS: will enable HTTPS if set true.


Start ChubaoFS cluster
~~~~~~~~~~~~~~~~~~~~~~~
 Run the following to launch ChubaoFS cluster with `AuthNode` enabled:

  .. code-block:: bash

    $ docker/run_docker.sh -r -d /data/disk

Generate Certificate
---------------------

To prevent `MITM` (Man In The Middle) attacks, `HTTPS` is required for the communication between client and service.
The following steps show the generation of self-sign a certificate with a private (`.key`) and public key.

- Generating Key and Self Signed Cert:

.. code-block:: bash

  $ openssl req \
    -x509 \
    -nodes \
    -newkey rsa:2048 \
    -keyout server.key \
    -out server.crt \
    -days 3650 \
    -subj "/C=GB/ST=China/L=Beijing/O=jd.com/OU=Infra/CN=*"

  - `server.crt`: `AuthNode` public certificate needed to be sent to `Client`
  - `server.key`: `AuthNode` private key needed to be securely placed in `/app` folder in `Authnode`

For easy deployment, current implementation of `AuthNode` uses TLS option `insecure_skip_verify` and `tls.RequireAndVerifyClientCert`, which would skip secure verification of both client and server.
For environment with high security command, these options should be turned off.
