# Fuse Client Issues

## Memory and Performance Optimization

- The Fuse client occupies too much memory, exceeding 2GB, which has a significant impact on other businesses.
  - Offline modification: Set the `readRate` and `writeRate` parameters in the configuration file, restart the client. [For details, please refer to](../maintenance/config.md)
  - Online modification: `http://{clientIP}:{profPort} /rate/set?write=800&read=800`
- Fuse client performance optimization, [please refer to Fuse optimization](../maintenance/fuse.md)

## Mounting Issues

1. Does CubeFS support subdirectory mounting?

Yes, it does. Set the `subdir` parameter in the configuration file.

2. What are the reasons for mounting failure?

After mounting failure, the following information is output:

```bash
$ ... err(readFromProcess: sub-process: fusermount: exec: "fusermount": executable file not found in $PATH)
```

- Check whether fuse is installed. If not, install it.

```bash
$ rpm –qa|grep fuse
$ yum install fuse
```

- Check whether the mount directory exists.
- Check whether the mount point directory is empty.
- Check whether the mount point has been unmounted.
- Check whether the mount point status is normal. If the mount point `mnt` displays the following information, you need to unmount it first and then start the client.

```bash
$ ls -lih
ls: cannot access 'mnt': Transport endpoint is not connected
total 0
6443448706 drwxr-xr-x 2 root root 73 Jul 29 06:19 bin
 811671493 drwxr-xr-x 2 root root 43 Jul 29 06:19 conf
6444590114 drwxr-xr-x 3 root root 28 Jul 29 06:20 log
         ? d????????? ? ?    ?     ?            ? mnt
 540443904 drwxr-xr-x 2 root root 45 Jul 29 06:19 script
```

- Check whether the configuration file is correct, including the Master address, volume name, and other information.
- If none of the above problems exist, locate the error through the client error log to see if the mounting failure is caused by the MetaNode or Master service.

## IO Issues

1. The IOPS is too high, causing the client to occupy more than 3GB or even higher memory. Is there a way to limit the IOPS?

You can limit the client's response to IO request frequency by modifying the client's rate limit.

```bash
# View the current IOPS:
$ http://[ClientIP]:[profPort]/rate/get
# Set the IOPS. The default value of -1 means no limit on IOPS.
$ http://[ClientIP]:[profPort]/rate/set?write=800&read=800
```

2. The IO delay of operations such as `ls` is too high?

- Because the client reads and writes files through the HTTP protocol, please check whether the network is healthy.
- Check whether there is an overloaded MetaNode, whether the MetaNode process is hung, and you can restart the MetaNode or expand new MetaNodes to the cluster and take some MetaNodes offline on the overloaded MetaNode to relieve the pressure on the MetaNode.

## Strong Consistency for Concurrent Read and Write by Multiple Clients

No. CubeFS relaxes the POSIX consistency semantics, which can only ensure the order consistency of file/directory operations and does not prevent multiple clients from writing to the same file/directory leasing mechanism. This is because in a containerized environment, many cases do not require strict POSIX semantics, that is, applications rarely rely on the file system to provide strong consistency guarantees. And in a multi-tenant system, it is rare for two independent tasks to write to a shared file at the same time, so the upper-layer application needs to provide stricter consistency guarantees.

## Can the client process be killed directly?

It is not recommended. It is best to go through the `umount` process. After unmounting, the client process will automatically stop.