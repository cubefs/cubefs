# FAQ

## Question 1
_Description_: When using the object storage interface to create a bucket, it prompts "Access Denied." How should the permissions for the account be set? The account used is testuser. The exception log in the Java program is Access Denied.

**Answer**: This issue likely requires creating a volume first, as the bucket is essentially a volume. When using the object storage interface to create a bucket, Cubefs creates the corresponding volume in the background, which might get stuck. It is suggested to create the volume first using cfs-cli.

## Question 2
_Description_: The hard drive is faulty, and the replica cannot be taken offline normally. How should this orphaned data partition be repaired?

**Answer**: You can force the deletion of the replica and then add a new replica.
```bash
curl -v "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```

## Question 3
_Description_: If there is only one object in a bucket, test1/path2/obj.jpg, it should be deleted along with test1/path2 after normal deletion. However, Cubefs only deletes the obj.jpg file and does not automatically delete the test1 and path2 directories.

**Answer**: This is essentially because ObjectNode is based on fuse to virtually create S3 keys, and MetaNode itself does not have corresponding semantics. If deletion is to recursively delete upper-level empty directories, the judgment logic would become complex and there would also be concurrency issues. Users can mount the client and write a script to periodically recursively query and clean up empty directories. Using a depth-first search algorithm can solve the problem of searching for empty directories.

## Question 4
_Description_: Two DataNode3 nodes are faulty. Is there any way to save them?

**Answer**: Yes, it can be saved. First, back up the faulty dp replica, then force delete the faulty replica, and finally add two good DataNodes.
```bash
curl -v "127.0.0.1:17010/dataReplica/delete?raftForceDel=true&addr=datanodeAddr:17310&id=47128"
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```

## Question 5
_Description_: A directory was mistakenly deleted, and one MetaNode reports a lost partition. How should this be handled? Can data be copied from other nodes?

**Answer**: The node can be taken offline and then restarted. This will trigger the migration of the meta partition to other nodes, and the copy can be completed automatically through migration.

## Question 6
_Description_: The default data partition method of the client cfs-client tends to increase the load on machines that are already heavily loaded, especially those that were expanded first, leading to disk usage above 90%. This causes high IO wait on some machines. The higher the machine’s capacity, the more likely it is to experience client concurrent access, leading to disk IO throughput not matching requests, forming local hotspots. Is there any way to handle this issue?

**Answer**: Choose to store on nodes with more available space:
```bash
curl -v "http://127.0.0.1:17010/nodeSet/update?nodesetId=id&dataNodeSelector=AvailableSpaceFirst"
```
Or set the DataNode to read-only mode to prevent the hot node from continuing to write:
```bash
curl -v "masterip:17010/admin/setNodeRdOnly?addr=datanodeip:17310&nodeType=2&rdOnly=true"
```

## Question 7
_Description_: After upgrading to 3.4, the number of MetaNode meta partitions gradually increases, and the mp quantity limit must be increased accordingly. It seems to become insufficient over time.

**Answer**: Adjust the inode number interval of the large meta partition to 100 million, so it is less likely to create new meta partitions.
```bash
curl -v "http://192.168.1.1:17010/admin/setConfig?metaPartitionInodeIdStep=100000000"
```

## Question 8
_Description_: What is the process for the disk damage and offline process of Blobstore? After setting Blobstore to a faulty disk, the disk status remains in repaired and never goes offline. When calling the offline API actively, it shows that the disk status needs to be normal and read-only to go offline. Does this mean only normal disks can be taken offline? How should faulty disks be handled? For example, if disk3 is faulty and a new disk is replaced, after setting it to faulty and restarting, disk3 gets a new disk ID, but the old disk ID of disk3 still exists. How do you delete the old disk ID?

**Answer**: The record of the old disk will always exist for traceability of disk replacement. In other words, we do not delete the old disk ID.

## Question 9
_Description_: How does Cubefs support large file scenarios, such as large model files in the tens of gigabytes?

**Answer**: There is no problem, it can be supported.

## Question 10
_Description_: After forcefully deleting an abnormal replica, the remaining replicas did not automatically become leaders, resulting in the inability to add new replicas. The cfs-cli datapartition check command returns that this dp is in a no leader state. How should this abnormal replica be handled?

**Answer**: Check the raft logs to query the election information of this partition to see if there are nodes outside the replica group requesting votes. If so, it needs to be forcibly removed from the replica group.
```bash
curl -v "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```