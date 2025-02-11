# FAQ

## Question 1
_Description_: Using MinIO API to create a bucket results in a permission denied error. How should the permissions for the 'testuser' account be configured? The exception log in the Java program shows "Access Denied."

**Answer**: This issue likely requires creating a volume first, as the bucket corresponds to a volume in cubefs. Creating a bucket using the S3 interface may hang due to the backend volume creation. It's recommended to create the volume using cfs-cli first.

## Question 2
_Description_: A hard drive has failed, and the replica cannot be taken offline normally. How should unmanaged data partitions be repaired?

**Answer**: You can force the deletion and removal of the replica, then add new replicas.
```bash
curl "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```

## Question 3
Description: The metanode container is reporting error messages: CrashLoopBackOff and OOMKilled.

**Answer**: This is an OOM issue with metanode; increase the memory allocation.

## Question 4
Description: When normally deleting an object in S3, the key is deleted. However, cubefs simulates S3 using a file system, so even if the key’s directory is empty, it won’t be automatically deleted.
For example: If a bucket contains only one object, test1/path2/obj.jpg, after normal S3 deletion, the bucket should be empty. However, cubefs only deletes the obj.jpg file and does not automatically delete the test1 and path2 directories.

**Answer**: This is because objectnode uses fuse to virtually create S3 keys based on paths, and metanode lacks corresponding semantics. If deletion should also recursively delete upper-level empty directories, the logic becomes complex and can lead to concurrency issues. Users can mount the client and write a script to periodically query and clean up empty directories. A depth-first search algorithm can solve the problem of searching for empty directories.

## Question 5
Description: Two out of three datanode 3 nodes have failed. Is there any way to recover?

**Answer**: Yes, back up the bad data partition replicas.
```bash
curl "127.0.0.1:17010/dataReplica/delete?raftForceDel=true&addr=datanodeAddr:17310&id=47128"
```
Force delete, then add two good datanodes.
```bash
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```
## Question 6
Description: A directory was mistakenly deleted, and a metanode reports a lost partition. How should this be handled? Can data be copied from other nodes?

**Answer**: You can take the node offline and then restart it. This will trigger the migration of the meta partition to other nodes.

## Question 7
Description: The default behavior of cfs-client tends to overload machines that are already expanded, writing up to 90% of disk capacity, causing high IO wait on some machines. Higher-capacity machines are more likely to experience high client concurrency, leading to disk IO throughput not matching requests, forming hotspots. Is there a way to handle this issue?

**Answer**: Prioritize storing on machines with more available space:
```bash
curl -v "http://127.0.0.1:17010/nodeSet/update?nodesetId=id&dataNodeSelector=AvailableSpaceFirst"
```
Or set the data node to read-only mode to prevent hot nodes from continuing to write:
```bash
curl "masterip:17010/admin/setNodeRdOnly?addr=datanodeip:17310&nodeType=2&rdOnly=true"
```

## Question 8
Description: After upgrading to 3.4, the number of metanode meta partitions gradually increases, and the meta partition limit must be increased accordingly. It seems to become insufficient over time.

**Answer**: Adjust the inode number interval for large meta partitions to 100 million, so new meta partitions are less likely to be created.
```bash
curl -v "http://192.168.1.1:17010/admin/setConfig?metaPartitionInodeIdStep=100000000"
```

## Question 9
Description: If I want to migrate files from Ceph or MinIO to cubefs, which tools and methods are recommended?

**Answer**: Write a Python script using the S3 interface to copy concurrently, or use tools like juicefs_sync, rclone.

## Question 10
Description: What is the process for offline blobstore disks? After setting a disk as bad, its status remains repaired and does not go offline. The offline API call requires the disk status to be normal and read-only to proceed. Is only a normal disk allowed to go offline? How should a failed disk be handled? For example, if disk3 fails, a new disk is added, and disk3 is marked as bad. After restarting, disk3 has a new disk ID, but the old disk ID remains. How do I delete the old disk ID?

**Answer**: The old disk ID record will persist for traceability of disk replacement. Therefore, the old disk ID is not deleted.

## Question 11
Description: How well does cubefs support large file scenarios, such as large model files in the tens of gigabytes?

**Answer**: It is OK and supported well.

## Question 12
Description: After forcefully deleting an abnormal replica, the remaining replicas do not automatically become leaders, preventing the addition of new replicas. The cfs-cli datapartition check command shows the dp is in a no leader state. How should this abnormal replica be handled?

**Answer**: Check the raft logs to query the partition’s election information. If there are nodes outside the replica group requesting votes, they need to be forcefully removed from the replica group.
```bash
curl "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```