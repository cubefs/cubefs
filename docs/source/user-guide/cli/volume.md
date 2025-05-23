# Volume Management

## Create Volume

```bash
cfs-cli volume create [VOLUME NAME] [USER ID] [flags]
```

```bash
Create a new volume

Usage:
  cfs-cli volume create [VOLUME NAME] [USER ID] [flags]

Flags:
      --allowedStorageClass string         Specify which StorageClasses the vol will support,
                                           format is comma separated uint32:"StorageClass1, StorageClass2",
                                           1:SSD, 2:HDD, empty value means determine by master
      --cache-action int                   Specify low volume cacheAction (default 0)
      --cache-capacity int                 Specify low volume capacity[Unit: GB]
      --cache-high-water int                (default 80)
      --cache-low-water int                 (default 60)
      --cache-lru-interval int             Specify interval expiration time[Unit: min] (default 5)
      --cache-rule-key string              Anything that match this field will be written to the cache
      --cache-threshold int                Specify cache threshold[Unit: byte] (default 10485760)
      --cache-ttl int                      Specify cache expiration time[Unit: day] (default 30)
      --capacity uint                      Specify volume capacity (default 10)
      --clientIDKey string                 needed if cluster authentication is on
      --crossZone string                   Disable cross zone (default "false")
      --delete-lock-time int               Specify delete lock time[Unit: hour] for volume
      --description string                 Description
      --dp-count int                       Specify init data partition count (default 10)
      --dp-size int                        Specify data partition size[Unit: GB] (default 120)
      --ebs-blk-size int                   Specify ebsBlk Size[Unit: byte] (default 8388608)
      --enableQuota string                 Enable quota (default false) (default "false")
      --flashNodeTimeoutCount int          FlashNode timeout count, flashNode will be removed by client if it's timeout count exceeds this value (default 5)
      --follower-read string               Enable read form replica follower
  -h, --help                               help for create
      --maximally-read string              Enable read form mp follower, (true|false), default false
      --meta-follower-read string          Enable read form more hosts, (true|false), default false
      --mp-count int                       Specify init meta partition count (default 3)
      --normalZonesFirst string            Write to normal zone first (default "false")
      --readonly-when-full string          Enable volume becomes read only when it is full (default "false")
      --remoteCacheAutoPrepare string      Remote cache auto prepare, let flashnode read ahead when client append ek
      --remoteCacheEnable string           Remote cache enable
      --remoteCacheMaxFileSizeGB int       Remote cache max file size[Unit: GB](must > 0) (default 128)
      --remoteCacheMultiRead string        Remote cache follower read(true|false) (default "false")
      --remoteCacheOnlyForNotSSD string    Remote cache only for not ssd(true|false) (default "false")
      --remoteCachePath string             Remote cache path, split with (,)
      --remoteCacheReadTimeout int         Remote cache read timeout millisecond(must > 0) (default 100)
      --remoteCacheSameRegionTimeout int   Remote cache same region timeout millisecond(must > 0) (default 2)
      --remoteCacheSameZoneTimeout int     Remote cache same zone timeout microsecond(must > 0) (default 400)
      --remoteCacheTTL int                 Remote cache ttl[Unit: s](must >= 10min, default 5day) (default 432000)
      --replica-num string                 Specify data partition replicas number(default 3 for normal volume,1 for low volume)
      --transaction-mask string            Enable transaction for specified operation: "create|mkdir|remove|rename|mknod|symlink|link" or "off" or "all"
      --transaction-timeout uint32         Specify timeout[Unit: minute] for transaction [1-60] (default 1)
      --tx-conflict-retry-Interval int     Specify retry interval[Unit: ms] for transaction conflict [10-1000]
      --tx-conflict-retry-num int          Specify retry times for transaction conflict [1-100]
      --volStorageClass uint32             Specify which StorageClass the clients mounts this vol should write to: [1:SSD | 2:HDD | 3:Blobstore]
  -y, --yes                                Answer yes for all questions
      --zone-name string                   Specify volume zone name
```

## Delete Volume

Delete the specified volume [VOLUME NAME]. The size of the ec volume must be 0 to be deleted. When enable delay deletion, volume will be deleted after `volDeletionDelayTime` hours, and `status=false` can be used to cancel volume deletion.

```bash
cfs-cli volume delete [VOLUME NAME] [flags]
```

```bash
Flags:
  -h, --help     help for delete
  -s, --status   Decide whether to delete or undelete (default true)
  -y, --yes      Answer yes for all questions
```

## Show Volume

Get information of the volume [VOLUME NAME].

```bash
cfs-cli volume info [VOLUME NAME] [flags]
```

```bash
Flags:
    -d, --data-partition                                # Show detailed information of the data Partition.
    -m, --meta-partition                                # Show detailed information of the meta Partition.
```

## Create and Add Data Partitions to the Volume

Create and add [NUMBER] data partitions to the volume [VOLUME].

```bash
cfs-cli volume add-dp [VOLUME] [NUMBER OF DATA PARTITIONS]
```

## List Volumes

```bash
cfs-cli volume list
```

## Transfer Volume

Transfer the volume [VOLUME NAME] to another user [USER ID].

```bash
cfs-cli volume transfer [VOLUME NAME] [USER ID] [flags]
```

```bash
Flags:
    -f, --force                                         # Force transfer.
    -y, --yes                                           # Skip all questions and set the answer to "yes".
```

## Volume Configuration Setup

Update the configurations of the volume.

```bash
Update configuration of the volume

Usage:
  cfs-cli volume update [VOLUME NAME] [flags]

Flags:
      --accessTimeValidInterval int           Effective time interval for accesstime, at least 43200 [Unit: second] (default -1)
      --autoDpMetaRepair string               Enable or disable dp auto meta repair
      --cache-action string                   Specify low volume cacheAction (default 0)
      --cache-capacity string                 Specify low volume capacity[Unit: GB]
      --cache-high-water int                   (default 80)
      --cache-low-water int                    (default 60)
      --cache-lru-interval int                Specify interval expiration time[Unit: min] (default 5)
      --cache-rule string                     Specify cache rule
      --cache-threshold int                   Specify cache threshold[Unit: byte] (default 10M)
      --cache-ttl int                         Specify cache expiration time[Unit: day] (default 30)
      --capacity uint                         Specify volume datanode capacity [Unit: GB]
      --clientIDKey string                    needed if cluster authentication is on
      --cross-zone string                     Enable cross zone
      --delete-lock-time int                  Specify delete lock time[Unit: hour] for volume (default -1)
      --description string                    The description of volume
      --directRead string                     Enable read direct from disk (true|false, default false)
      --ebs-blk-size int                      Specify ebsBlk Size[Unit: byte]
      --enablePersistAccessTime string        true/false to enable/disable persisting access time
      --enableQuota string                    Enable quota
      --flashNodeTimeoutCount int             FlashNode timeout count, flashNode will be removed by client if it's timeout count exceeds this value(default 5)
      --follower-read string                  Enable read form replica follower (default false)
      --forbidWriteOpOfProtoVersion0 string   set volume forbid write operates of packet whose protocol version is version-0: [true | false]
  -h, --help                                  help for update
      --leader-retry-timeout int              Specify leader retry timeout for mp read [Unit: second] for volume, default 0 (default -1)
      --maximally-read string                 Enable read more hosts (true|false, default false)
      --meta-follower-read string             Enable read form mp follower (true|false, default false)
      --quotaClass int                        specify target storage class for quota, 1(SSD), 2(HDD)
      --quotaOfStorageClass int               specify quota of target storage class, GB (default -1)
      --readonly-when-full string             Enable volume becomes read only when it is full
      --remoteCacheAutoPrepare string         Remote cache auto prepare, let flashnode read ahead when client append ek
      --remoteCacheEnable string              Remote cache enable
      --remoteCacheMaxFileSizeGB int          Remote cache max file size[Unit: GB](must > 0)
      --remoteCacheMultiRead string           Remote cache follower read(true|false), default true
      --remoteCacheOnlyForNotSSD string       Remote cache only for not ssd(true|false), default false
      --remoteCachePath string                Remote cache path, split with (,)
      --remoteCacheReadTimeout int            Remote cache read timeout millisecond(must > 0)
      --remoteCacheSameRegionTimeout int      Remote cache same region timeout millisecond(must > 0),default 2
      --remoteCacheSameZoneTimeout int        Remote cache same zone timeout microsecond(must > 0),default 400
      --remoteCacheTTL int                    Remote cache ttl[Unit:second](must >= 10min, default 5day)
      --replica-num string                    Specify data partition replicas number(default 3 for normal volume,1 for low volume)
      --transaction-force-reset               Reset transaction mask to the specified value of "transaction-mask"
      --transaction-limit int                 Specify limitation[Unit: second] for transaction(default 0 unlimited)
      --transaction-mask string               Enable transaction for specified operation: "create|mkdir|remove|rename|mknod|symlink|link" or "off" or "all"
      --transaction-timeout int               Specify timeout[Unit: minute] for transaction (0-60]
      --trashInterval int                     The retention period for files in trash (default -1)
      --tx-conflict-retry-Interval int        Specify retry interval[Unit: ms] for transaction conflict [10-1000]
      --tx-conflict-retry-num int             Specify retry times for transaction conflict [1-100]
      --volStorageClass int                   specify volStorageClass
  -y, --yes                                   Answer yes for all questions
      --zone-name string                      Specify volume zone name
```

## Forbid Volume

Set volume forbidden mark

```bash
cfs-cli vol set-forbidden [VOLUME] [FORBIDDEN]
```

The following commands forbid volume `ltptest`:

```bash
cfs-cli vol set-forbidden ltptest true
```

## Enable/Disable Volume Auditlog

Enable or disable auditlog of volume

```bash
cfs-cli volume set-auditlog [VOLUME] [STATUS]
```

The following commands disable auditlog for `ltptest`:

```bash
cfs-cli volume set-auditlog ltptest false
```