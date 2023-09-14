# Quota Management

::: warning note
Directory quota management is a new feature for v3.3.0
:::

## Create Quota

To create a quota, you need to specify the volume name and one or more path directories.

Note: Paths cannot be repeated or nested.

```bash
cfs-cli quota create [volname] [fullpath1,fullpath2] [flags]
```

```bash
Flags:
  -h, --help            help for create
      --maxBytes uint   Specify quota max bytes (default 18446744073709551615)
      --maxFiles uint   Specify quota max files (default 18446744073709551615)
```

## Apply Quota

Apply quota needs to specify the volume name and quotaId. This interface is executed after the quota is created. The purpose is to make the quotaId effective for the existing files and directories under the quota directory (including the quota directory itself).
The entire process of creating a quota is: first execute the quota create command, and then execute the quota apply command.

Note: If there are many files in the quota directory, the return time of this interface will be longer

```bash
cfs-cli quota apply [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help                       help for apply
      --maxConcurrencyInode uint   max concurrency set Inodes (default 1000)
```

## Revoke Quota

Revoke quota needs to specify the volume name and quotaId. This interface is executed when the quota is ready to be deleted. The purpose is to invalidate the quotaId of the existing files and directories under the quota directory (including the quota directory itself).
The entire process of deleting quota is: execute quota revoke first, then confirm the values of USEDFILES and USEDBYTES are 0 through quota list query, and then perform quota delete operation.

```bash
cfs-cli quota revoke [volname] [quotaId] [flags]
```

```bash
Flags:
      --forceInode uint            force revoke quota inode
  -h, --help                       help for revoke
      --maxConcurrencyInode uint   max concurrency delete Inodes (default 1000)
```

## Delete Quota

Delete quota needs to specify volume name and quotaId

```bash
cfs-cli quota delete [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help   help for delete
  -y, --yes    Do not prompt to clear the quota of inodes
```

## Update Quota

The update quota needs to specify the volume name and quotaId. Currently, the only values that can be updated are maxBytes and maxFiles

```bash
cfs-cli quota update [volname] [quotaId] [flags]
```

```bash
Flags:
  -h, --help            help for update
      --maxBytes uint   Specify quota max bytes
      --maxFiles uint   Specify quota max files
```

## List Quota of A Volume

List quota needs to specify the volume name, and traverse all the quota information of the volume

``` bash
cfs-cli quota list [volname] [flags]
```

```bash
Flags:
  -h, --help   help for list
```

## List Quota of All Volumes

Without any parameters, traverse all the volume information with quota

```bash
cfs-cli quota listAll [flags]
```

```bash
Flags:
  -h, --help   help for listAll
```

## Get Quota of A Inode

Check whether a specific inode has quota information

``` bash
cfs-cli quota getInode [volname] [inode] [flags]
```

```bash
Flags:
  -h, --help   help for getInode
```
