# Authnode

Authnode provides a general authentication & authorization service among CubeFS nodes. Client, Master, Meta and Data node are required to be authenticated and authorized before any resource access in another node.

Initially, each node (Auth, Client, Master, Meta or Data node) is launched with a secure key which is distributed by a authenticated person (for instance, cluster admin). With a valid key, a node can be identified in Authnode service and granted a ticket for resource access.

The overall work flow is: key creation and distribution –> ticket retrieval with key –> resource access with ticket.

## Concepts

- Key: a bit of secret shared data between a node and Authnode that asserts identity of a client.

- Ticket: a bit of data that cryptographically asserts identity and authorization (through a list of capabilities) for a service for a period of time.

- Capability: a capability is defined in the format of node:object:action where node refers to a service node (such as auth, master, meta or data), object refers to the resource and action refers to permitted activities (such as read, write and access). See examples below.

*Capability Example*

| Capability            | Specifications                                 |
|-----------------------|------------------------------------------------|
| auth:createkey:access | Access permission for createkey in Authnode    |
| master:\*:\*          | Any permissions for any objects in Master node |
| \*:\*:\*              | Any permissions for any objects in any nodes   |

## Client Tool

cfs-authtool is a client-side utility of Authnode, providing key managements (creation, view and modification) and ticket retrieval in and from Authnode keystore. In particular, Each key is associated with an entity name or id, secret key string, creation time, role and capability specification.

*Key structure*

| Key  | Type   | Description                                          |
|------|--------|------------------------------------------------------|
| id   | string | Unique key identifier composed of letters and digits |
| key  | string | Base64 encoded secret key                            |
| role | string | The role of the key (either client or service)       |
| caps | string | The capabilities of the key                          |

### Build

Use the following commands to build client side tool for Authnode:

```bash
$ git clone http://github.com/cubefs/cubefs.git
$ cd cubefs
$ make build
```

If successful, the tool cfs-authtool can be found in *build/bin*.


### Synopsis

cfs-authtool ticket -host=AuthNodeAddress [-keyfile=Keyfile] [-output=TicketOutput] [-https=TrueOrFalse -certfile=AuthNodeCertfile] TicketService Service

cfs-authtool api -host=AuthNodeAddress -ticketfile=TicketFile [-data=RequestDataFile] [-output=KeyFile] [-https=TrueOrFalse -certfile=AuthNodeCertfile] Service Request

cfs-authtool authkey [-keylen=KeyLength]

TicketService := [getticket]

Service := [AuthService | MasterService | MetaService | DataService]

Request := [createkey | deletekey | getkey | addcaps | deletecaps | getcaps | addraftnode | removeraftnode]


## AuthNode Configurations

*Authnode* use **JSON** as configuration file format.

*Properties*

| Key            | Type   | Description                                                            | Mandatory |
|----------------|--------|------------------------------------------------------------------------|-----------|
| role           | string | Role of process and must be set to master                              | Yes       |
| ip             | string | host ip                                                                | Yes       |
| port           | string | Http port which api service listen on                                  | Yes       |
| prof           | string | golang pprof port                                                      | Yes       |
| id             | string | identy different master node                                           | Yes       |
| peers          | string | the member information of raft group                                   | Yes       |
| logDir         | string | Path for log file storage                                              | Yes       |
| logLevel       | string | Level operation for logging. Default is error.                         | No        |
| retainLogs     | string | the number of raft logs will be retain.                                | Yes       |
| walDir         | string | Path for raft log file storage.                                        | Yes       |
| storeDir       | string | Path for RocksDB file storage,path must be exist                       | Yes       |
| clusterName    | string | The cluster identifier                                                 | Yes       |
| exporterPort   | int    | The prometheus exporter port                                           | No        |
| authServiceKey | string | The secret key used for authentication of AuthNode                     | Yes       |
| authRootKey    | string | The secret key used for key derivation (session and client secret key) | Yes       |
| enableHTTPS    | bool   | Option whether enable HTTPS protocol                                   | No        |

**Example:**

```json
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
```

## Steps for Starting CubeFS with AuthNode

### Create Authnode key

Run the command:

```bash
$ ./cfs-authtool authkey
```

If successful, two key files can be generated `authroot.json` and `authservice.json` under current directory. They represent authServiceKey and authRootKey respectively.

example `authservice.json` :

```json
{
     "id": "AuthService",
     "key": "9h/sNq4+5CUAyCnAZM927Y/gubgmSixh5hpsYQzZG20=",
     "create_ts": 1573801212,
     "role": "AuthService",
     "caps": "{\"*\"}"
}
```

Edit `authnode.json` in *docker/conf* as following:

- `authRootKey`: use the value of `key` in `authroot.json`

- `authServiceKey`: use the value of `key` in `authService.json`

example `authnode.json` :

```json
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
```

### Start Authnode Cluster

In directory *docker/authnode*, run the following command to start a *Authnode cluster*.

```bash
$ docker-compose up -d
```

### Create admin in Authnode

Get Authnode ticket using authServiceKey:

```bash
$ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=authservice.json -output=ticket_auth.json getticket AuthService
```

example `ticket_auth.json` :

```json
{
    "id": "AuthService",
    "session_key": "A9CSOGEN9CFYhnFnGwSMd4WFDBVbGmRNjaqGOhOinJE=",
    "service_id": "AuthService",
    "ticket": "RDzEiRLX1xjoUyp2TDFviE/eQzXGlPO83siNJ3QguUrtpwiHIA3PLv4edyKzZdKcEb3wikni8UxBoIJRhKzS00+nB7/9CjRToAJdT9Glhr24RyzoN8psBAk82KEDWJhnl+Y785Av3f8CkNpKv+kvNjYVnNKxs7f3x+Ze7glCPlQjyGSxqARyLisoXoXbiE6gXR1KRT44u7ENKcUjWZ2ZqKEBML9U4h0o58d3IWT+n4atWKtfaIdp6zBIqnInq0iUueRzrRlFEhzyrvi0vErw+iU8w3oPXgTi+um/PpUyto20c1NQ3XbnkWZb/1ccx4U0"
}
```

Create admin using Authnode ticket:

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_auth.json -data=data_admin.json -output=key_admin.json AuthService createkey
```

example `data_admin.json` :

```json
{
    "id": "admin",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

### Create key for CubeFS cluster

- Get Authnode ticket using admin key:

```bash
$ ./cfs-authtool ticket -host=192.168.0.14:8080 -keyfile=key_admin.json -output=ticket_admin.json getticket AuthService
```

- Create key for Master

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_master.json -output=key_master.json AuthService createkey
```

example `data_master.json` :

```json
{
    "id": "MasterService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```

*Specifications:*

| Key  | Description                     |
|------|---------------------------------|
| id   | will set Client ID              |
| role | will set the role of id         |
| caps | will set the capabilities of id |

Edit `master.json as` following:

- `masterServiceKey`: use the value of `key` in `key_master.json`

Create key for Client

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_client.json -output=key_client.json AuthService createkey
```

example `data_client.json`:

```json
{
    "id": "ltptest",
    "role": "client",
    "caps": "{\"API\":[\"*:*:*\"], \"Vol\":[\"*:*:*\"]}"
}
```

Edit `client.json` as following:

- `clientKey`: use the value of `key` in `key_client.json`

example `client.json`：

```json
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
```

*Specifications:*

| Key          | Description                                 |
|--------------|---------------------------------------------|
| authenticate | will enable authentication flow if set true |
| ticketHost   | will set the IP/URL of Authnode cluster     |
| clientKey    | will set the key generated by Authnode      |
| enableHTTPS  | will enable HTTPS if set true               |


### Start CubeFS cluster

Run the following to launch CubeFS cluster with AuthNode enabled:

```bash
$ docker/run_docker.sh -r -d /data/disk
```

## Generate Certificate

To prevent MITM (Man In The Middle) attacks, HTTPS is required for the communication between client and service. The following steps show the generation of self-sign a certificate with a private (.key) and public key.

Generating Key and Self Signed Cert:

```bash
$ openssl req \
  -x509 \
  -nodes \
  -newkey rsa:2048 \
  -keyout server.key \
  -out server.crt \
  -days 3650 \
  -subj "/C=GB/ST=China/L=Beijing/O=jd.com/OU=Infra/CN=*"
```

*Specifications:*

- `server.crt`: `AuthNode` public certificate needed to be sent to `Client`

- `server.key`: `AuthNode` private key needed to be securely placed in `/app` folder in `Authnode`

For easy deployment, current implementation of AuthNode uses TLS option insecure_skip_verify and tls.RequireAndVerifyClientCert, which would skip secure verification of both client and server. For environment with high security command, these options should be turned off.


## Master API Authentication

Master has numerous APIs, such as creating and deleting volumes, so it is necessary to authenticate access to the master API to improve cluster security.

With the excellent authentication capability of authnode, we have optimized the existing authentication mechanism to achieve the goal of simplifying the authentication process.

### Enable Master API Authentication

example `master.json` ：
```json
{
  "role": "master",
  "ip": "127.0.0.1",
  "listen": "17010",
  "prof":"17020",
  "id":"1",
  "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log",
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal",
  "storeDir":"/cfs/master/data/store",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824",
  "masterServiceKey": "jgBGSNQp6mLbu7snU8wKIdEkytzl+pO5/OZOJPpIgH4=",
  "authenticate": true,
  "authNodeHost": "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082",
  "authNodeEnableHTTPS": false
}
```
Properties

| Key         | Description                                                  |
|--------------|-----------------------------------------------------|
| authenticate | will enable Master API Authentication if set true |
| authNodeHost   | will set the IP/URL of Authnode cluster                               |
| authNodeEnableHTTPS  | will enable HTTPS if set true     |

### Use Authentication Parameter
When accessing the Master API, the parameter clientIDKey for authentication must be included.

When using authtool to create a key, auth_id_key is generated. This key will be used as the clientIDKey when accessing the master API.

#### First Example
Access the master API via HTTP and write the parameter clientIDKey, such as expanding a volume:

```bash
curl --location 'http://127.0.0.1:17010/vol/update?name=ltptest&authKey=0e20229116d5a9a4a9e876806b514a85&capacity=100&clientIDKey=eyJpZCI6Imx0cHRlc3QiLCJhdXRoX2tleSI6ImpnQkdTTlFwNm1MYnU3c25VOHdLSWRFa3l0emwrcE81L09aT0pQcElnSDQ9In0='
```

#### Second Example
Access the master API via cfs-cli and write the clientIDKey to configuration file .cfs-cli.json, so that any cluster management command is authenticated for permissions.

example `.cfs-cli.json` ：
```json
{
  "masterAddr": [
    "127.0.0.1:17010",
    "127.0.0.2:17010",
    "127.0.0.3:17010"
  ],
  "timeout": 60,
  "clientIDKey": "eyJpZCI6Imx0cHRlc3QiLCJhdXRoX2tleSI6ImpnQkdTTlFwNm1MYnU3c25VOHdLSWRFa3l0emwrcE81L09aT0pQcElnSDQ9In0="
}
```

#### Third Example
When datanode and metanode are started, they will respectively call the AddDataNode and AddMetaNode APIs, so it is also necessary to prepare serviceIDKey for them.

Similarly, use authtool to create keys for datanode and metanode respectively, and write the key as the serviceIDKey to the configuration file. When they start, permission authentication will be performed.

#### Create Key For datanode

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_datanode.json -output=key_datanode.json AuthService createkey
```

example `data_datanode` ：

```json
{
    "id": "DatanodeService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```
Edit `datanode.json as` following:

- `serviceIDKey`: use the value of `auth_id_key` in `key_datanode.json`

#### Create Key For metanode

```bash
$ ./cfs-authtool api -host=192.168.0.14:8080 -ticketfile=ticket_admin.json -data=data_metanode.json -output=key_metanode.json AuthService createkey
```

example `data_metanode` ：

```json
{
    "id": "MetanodeService",
    "role": "service",
    "caps": "{\"API\":[\"*:*:*\"]}"
}
```
Edit `metanode.json as` following:

- `serviceIDKey`: use the value of `auth_id_key` in `key_metanode.json`