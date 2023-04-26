# Log Processing

## Client Logs

### Audit Logs
The operation audit trail of the mount point is stored in the specified directory on the client, making it easy to access third-party log collection platforms.

- You can enable or disable local audit log function in the client configuration [Client Configuration](../maintenance/configs/client.md).
- You can send commands to the client through HTTP to actively enable or disable the log function without remounting.
- The client audit logs are recorded locally. When the log file exceeds 200MB, it will be rolled over, and the stale logs after rolling over will be deleted after 7 days. That is, the audit logs are kept for 7 days by default, and stale log files are scanned and deleted every hour.

### Audit Log Format

```text
[Cluster Name (Master Domain Name or IP), Volume Name, Subdir, Mount Point, Timestamp, Client IP, Client Hostname, Operation Type (Create, Delete, Rename), Source Path, Target Path, Error Message, Operation Time, Source File Inode, Destination File Inode]
```

### Audit Operation Types

The currently supported audit operations are as follows:

- Create, create a file
- Mkdir, create a directory
- Remove, delete a file or directory
- Rename, mv operation

### Log Writing Method

Audit logs are asynchronously written to disk and do not block client operations synchronously.

### Audit Log Interface

#### Enable Audit

```bash
curl -v "http://192.168.0.2:17410/auditlog/enable?path=/cfs/log&prefix=client2&logsize=1024"
```
::: tip Note
`192.168.0.2` is the IP address of the mounted client, and the same below.
:::

| Parameter | Type   | Description                                                                                                                                   |
|-----------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| path      | string | Audit log directory                                                                                                                           |
| prefix    | string | Specify the prefix directory of the audit log, which can be a module name or "audit" to distinguish the directory of flow logs and audit logs |
| logsize   | uint32 | Used to set the size threshold for log rolling. If not set, the default is 200MB                                                              |

#### Disable Audit Log

```bash
curl -v "http://192.168.0.2:17410/auditlog/disable"
```

## Server Logs

### Types of Logs
1. MetaNode, DataNode, and Master all have two types of logs: service running logs and raft logs. Since the client does not use raft, there is only a process service log.
2. The log paths of each service log and raft log can be configured in the startup configuration file as follows:
```
{
    "logDir": "/cfs/log",
    "raftDir": "/cfs/log",
    ......
}
```
3. ObjectNode has one type of log, which is the service running log.
4. The various modules of the erasure coding subsystem have two types of logs: service running logs and audit logs. The audit logs are disabled by default. If you want to enable them, please refer to [Basic Service Configuration](./configs/blobstore/base.md).

### Log Settings

1. If you are a developer or tester and want to debug, you can set the log level to Debug or Info.
2. If it is a production environment, you can set the log level to Warn or Error, which will greatly reduce the amount of logs.
3. The supported log levels are Debug, Info, Warn, Error, Fatal, and Critical (the erasure coding subsystem does not support the Critical level).

There are two ways to set the log:

- Set in the configuration file, as follows:
```
"logLevel": "debug"
```
- You can dynamically modify it through the command. The command is as follows:
```
http://127.0.0.1:{profPort}/loglevel/set?level={log-level}
```

::: tip Note
The log settings for the erasure coding subsystem are slightly different.
:::

- Set in the configuration file, please refer to [Basic Service Configuration](./configs/blobstore/base.md).
- Modify through the command, please refer to [Erasure Coding Common Management Commands](./admin-api/blobstore/base.md).

### Log Format

The log format is as follows:

```text
[Time][Log Level][Log Path and Line Number][Detailed Information]
For example:
2023/03/08 18:38:06.628192 [ERROR] partition.go:664: action[LaunchRepair] partition(113300) err(no valid master).
```

::: tip Note
The format of the erasure coding system is slightly different. Here, the running log and audit log are introduced separately.
:::

The format of the service running log is as follows:

```test
[Time][Log Level][Log Path and Line Number][TraceID:SpanID][Detailed Information]
2023/03/15 18:59:10.350557 [DEBUG] scheduler/blob_deleter.go:540 [tBICACl6si0FREwX:522f47d329a9961d] delete shards: location[{Vuid:94489280515 Host:http://127.0.0.1:8889 DiskID:297}]
```

The format of the audit log is as follows:

```text
[Request][Service Name][Request Time][Request Type][Request Interface][Request Header][Request Parameters][Response Status Code][Response Length][Request Time, in microseconds]
REQ	SCHEDULER	16793641137770897	POST	/inspect/complete	{"Accept-Encoding":"gzip","Content-Length":"90","Content-Type":"application/json","User-Agent":"blobnode/cm_1.2.0/5616eb3c957a01d189765cf004cd2df50bc618a8 (linux/amd64; go1.16.13)}	{"task_id":"inspect-45800-cgch04ehrnv40jlcqio0","inspect_err_str":"","missed_shards":null}	200	{"Blobstore-Tracer-Traceid":"0c5ebc85d3dba21b","Content-Length":"0","Trace-Log":["SCHEDULER"],"Trace-Tags":["span.kind:server"]}		0	68
```