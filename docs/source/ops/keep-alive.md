# keep-alive script
The keepalive script will be available in version 3.5.1.

## Running Guide
Enter the/tool/keepalive directory, take out all three scripts and place them in the designated location，execute the installation script.
```
./cbfs_auto_install.sh --mountPoint arg1 -- volName arg2 --owner arg3 -- accessKey arg4 --secretKey arg5 --masterAddr arg6 --logDir arg7 ---logLevel arg8 --rdOnly arg9 --subdir arg10 --clientPath arg11
```

* Before executing commands, users and volumes need to be created first
* Execute without root privileges, you need to add `user_allow_other` to `/etc/fuse.conf` before executing the command

## Parameter description
Necessary parameters
* mountPoint: mountPoint The local directory to be mounted (if multiple instances need to be launched, they need to be distinguished)
* volName: The volume name applied for, such as ltptest
* owner: The account applied for, such as prod_xxx
* accessKey
* secretKey
* master Address
* logdir: Client log directory (if multiple instances need to be launched, they need to be distinguished)
* clientPath

Optional parameters
* logLevel: Log level, default is warn
* rdOnly: read-only mount, default is false
* subdir: Mounts in the provided subdir, default is /

## Running instructions
`cbfs_auto_install.sh` will use the provided parameters to create `client.conf` in the `./volName` path, and mount the volume, add the restart rule to `crontab`.By default, the cbfs_restart script is executed once per minute.

cbfs_restart.sh: used to restart the crashed client, automatically mount at a scheduled time
cbfs_clean.sh: used to clean up the mount point and crontab rules, needs to be executed manually