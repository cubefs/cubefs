# Atomic management

CubeFS supports atomic operation. After turning on the atomic function, the metadata atomicity of the file operation is met, and the metadata uses the final consistency, that is, the metadata modification of the file operation is ether successfully committed, or rolled back.

## Fuse interface supported by atomicity

- Create
- Mkdir
- Remove
- Rename
- Mknod
- Symlink
- Link

::: tip tip
Atomicity supports the Fuse interface, and S3 interface is not supported for now.
:::

## atomicity switch
CubeFS implements part of the characteristics of transaction, but it is different from the traditional strict sense of transaction. For the convenience, the operation of files is referred to as transaction.

The transaction is effective in the volume, and a cluster can have multiple volumes. Each volume can switch on or off transaction separately. The client get configuration of transaction from the master when started. 

The switch can control all supported interfaces, or only open some interfaces, the default transaction time out is 1 minute.

```
curl "192.168.0.11:17010/vol/update?name=ltptest&enableTxMask=all&txForceReset=true&txConflictRetryNum=13&txConflictRetryInterval=30&txTimeout=1&authKey=0e20229116d5a9a4a9e876806b514a85"
```

::: tip tip
After modifying the parameter of the transaction, new config will be synchronized from Master within 2 minutes.
:::

| Parameters | Type | Description                                                                                                                                |
|---------|--------|--------------------------------------------------------------------------------------------------------------------------------------------|
| enabletxamask | String | Value can be: Create, MKDIR, Remove, RENAME, MKNod, Symlink, Link, off, all. off and all and other values are mutually exclusive           |
| txtimeout | uint32 | the default transaction time out is 1 minute, the maximum 60 minutes                                                                       |
| txForceReset | uint32 | The difference from enabletxamask is that value of enabletxmask will be merged, and txforcereset is forced to reset to the specified value |
| txConflictRetryNum | uint32 | Value range [1-100], defaults to 10                                                                                                        |
| txConflictRetryInterval | uint32 | Unit: milliseconds, the range of value [10-1000], the default is 20ms                                                                      |

You can check transaction config by getting the volume information
```
curl "192.168.0.11:17010/admin/getvol?name=ltptest"
```

You can also specify transaction parameters when creating the volume

```
curl "192.168.0.11:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3&enableTxMask=all&txTimeout=5"
```