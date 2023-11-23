# cfsauto

automount tool for [cubefs](https://github.com/cubefs/cubefs)

## build

```bash
go build -v -ldflags="-X main.buildVersion=1.1 -X 'main.buildDate=$(date '+%Y-%m-%d %H:%M:%S')'" -o /usr/local/bin
```

## quick start

### mount

cfsauto mountpoint -o options
mount -t fuse cfsauto mountpoint -o options

### show mounts

cfsauto

### version

cfsauto -V

example:

```bash
Version: 1.1, BuildDate: 2023-05-18 15:08:00
```


## LDAP automount example

automountkey=cubefs-test

automountInformation=-fstype=fuse,subdir=test/sub,volName=projectA,owner=1234567,accessKey=abcdxxxefg,secretKey=uvwxxxxyz,masterAddr=10.0.0.1:17010,logDir=/var/logs/cfs/log,enablePosixACL,logLevel=debug :cfsauto


## releases

- v1.1: support all options of cfs-client v3.2