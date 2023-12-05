# 卷管理

## 创建卷

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

## 删除指定卷

删除指定卷[VOLUME NAME], ec卷大小为0才能删除

```bash
cfs-cli volume delete [VOLUME NAME] [flags]
```

```bash
Flags:
    -y, --yes                                           # 跳过所有问题并设置回答为"yes"
```

## 获取卷信息

获取卷[VOLUME NAME]的信息

```bash
cfs-cli volume info [VOLUME NAME] [flags]
```

```bash
Flags:
    -d, --data-partition                                # 显示数据分片的详细信息
    -m, --meta-partition                                # 显示元数据分片的详细信息
```

## 创建并添加的数据分片至卷

创建并添加个数为[NUMBER]的数据分片至卷[VOLUME]

```bash
cfs-cli volume add-dp [VOLUME] [NUMBER]
```

## 列出所有卷信息

获取包含当前所有卷信息的列表

```bash
cfs-cli volume list
```

## 将卷转交给其他用户

将卷[VOLUME NAME]转交给其他用户[USER ID]

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

## 禁用卷

```bash
cfs-cli vol set-forbidden [VOLUME] [FORBIDDEN]
```

以下命令禁用卷`ltptest`:

```bash
cfs-cli vol set-forbidden ltptest true
```

## 开启/关闭卷审计日志

```bash
cfs-cli volume set-auditlog [VOLUME] [STATUS]
```

以下命令关闭卷`ltptest`的审计日志:

```bash
cfs-cli volume set-auditlog ltptest false
```