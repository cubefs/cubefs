## Prerequisite

Insert FUSE kernel module and install libfuse.

```bash
modprobe fuse

yum install -y fuse
```

## Prepare config file

fuse.json

```json
{
  "mountpoint": "/mnt/fuse",
  "volname": "test",
  "master": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
  "logpath": "/export/Logs/cfs",
  "loglvl": "info",
  "profport": "10094"
}
```

## Mount the client

Use the example *fuse.json*, and client is mounted on the directory */mnt/fuse*. All operations to */mnt/fuse* would be performed on the backing baudstorage.

```bash
nohup ./client -c fuse.json &
```
