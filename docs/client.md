## Prerequisite

Insert FUSE kernel module and install libfuse.

```bash
modprobe fuse

rpm -i fuse-2.9.2-8.el7.x86_64.rpm
```

## Prepare config file

fuse.json

```json
{
  "mountpoint": "/mnt/fuse",
  "volname": "intest",
  "master": "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80",
  "logpath": "/export/Logs/baudstorage",
  "loglvl": "info",
  "profport": "10094"
}
```

## Mount the client

Use the example *fuse.json*, and client is mounted on the directory */mnt/fuse*. All operations to */mnt/fuse* would be performed on the backing baudstorage.

```bash
nohup ./client -c fuse.json &
```
