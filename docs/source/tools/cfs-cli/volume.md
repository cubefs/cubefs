# Volume Management

## Create Volume

```bash
cfs-cli volume create [VOLUME NAME] [USER ID] [flags]
```

```bash
Flags:
     --cache-action int          Specify low volume cacheAction (default 0)
     --cache-capacity int        Specify low volume capacity[Unit: GB]
     --cache-high-water int       (default 80)
     --cache-low-water int        (default 60)
     --cache-lru-interval int    Specify interval expiration time[Unit: min] (default 5)
     --cache-rule-key string     Anything that match this field will be written to the cache
     --cache-threshold int       Specify cache threshold[Unit: byte] (default 10485760)
     --cache-ttl int             Specify cache expiration time[Unit: day] (default 30)
     --capacity uint             Specify volume capacity (default 10)
     --crossZone string          Disable cross zone (default "false")
     --description string        Description
     --ebs-blk-size int          Specify ebsBlk Size[Unit: byte] (default 8388608)
     --follower-read string      Enable read form replica follower (default "true")
 -h, --help                      help for create
     --mp-count int              Specify init meta partition count (default 3)
     --normalZonesFirst string   Write to normal zone first (default "false")
     --replica-num string        Specify data partition replicas number(default 3 for normal volume,1 for low volume)
     --size int                  Specify data partition size[Unit: GB] (default 120)
     --vol-type int              Type of volume (default 0)
 -y, --yes                       Answer yes for all questions
     --zone-name string          Specify volume zone name
```

## Delete Volume

Delete the specified volume [VOLUME NAME]. The size of the ec volume must be 0 to be deleted.

```bash
cfs-cli volume delete [VOLUME NAME] [flags]
```

```bash
Flags:
    -y, --yes                                           # Skip all questions and set the answer to "yes".
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
cfs-cli volume update
```

```bash
Flags:
    --cache-action string      Specify low volume cacheAction (default 0)
    --cache-capacity string    Specify low volume capacity[Unit: GB]
    --cache-high-water int      (default 80)
    --cache-low-water int       (default 60)
    --cache-lru-interval int   Specify interval expiration time[Unit: min] (default 5)
    --cache-rule string        Specify cache rule
    --cache-threshold int      Specify cache threshold[Unit: byte] (default 10M)
    --cache-ttl int            Specify cache expiration time[Unit: day] (default 30)
    --capacity uint            Specify volume datanode capacity [Unit: GB]
    --description string       The description of volume
    --ebs-blk-size int         Specify ebsBlk Size[Unit: byte]
    --follower-read string     Enable read form replica follower (default false)
    -y, --yes               Answer yes for all questions
    --zonename string   Specify volume zone name
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