# 卷管理

## 创建卷

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

## 删除指定卷

删除指定卷[VOLUME NAME], ec卷大小为0才能删除。开启延迟删除后，volume将会在`volDeletionDelayTime`小时后被真正删除，在此期间可以通过`status=false`取消删除操作。

```bash
cfs-cli volume delete [VOLUME NAME] [flags]
```

```bash
 Flags:
    -h, --help     help for delete
    -s, --status   Decide whether to delete or undelete (default true)
    -y, --yes      Answer yes for all questions
```

## 获取卷信息

获取卷 [VOLUME NAME] 的信息

```bash
cfs-cli volume info [VOLUME NAME] [flags]
```

```bash
Flags:
    -d, --data-partition                                # 显示数据分片的详细信息
    -m, --meta-partition                                # 显示元数据分片的详细信息
```

## 创建并添加的数据分片至卷

创建并添加个数为 [NUMBER] 的数据分片至卷 [VOLUME]

```bash
cfs-cli volume add-dp [VOLUME] [NUMBER]
```

## 列出所有卷信息

获取包含当前所有卷信息的列表

```bash
cfs-cli volume list
```

## 将卷转交给其他用户

将卷 [VOLUME NAME] 转交给其他用户 [USER ID]

```bash
cfs-cli volume transfer [VOLUME NAME] [USER ID] [flags]
```

```bash
Flags:
    -f, --force                                         # 强制转交
    -y, --yes                                           # 跳过所有问题并设置回答为"yes"
```

## 更新卷配置

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

## 禁用卷

```bash
cfs-cli vol set-forbidden [VOLUME] [FORBIDDEN]
```

以下命令禁用卷 `ltptest`:

```bash
cfs-cli vol set-forbidden ltptest true
```

## 开启/关闭卷审计日志

```bash
cfs-cli volume set-auditlog [VOLUME] [STATUS]
```

以下命令关闭卷 `ltptest` 的审计日志:

```bash
cfs-cli volume set-auditlog ltptest false
```