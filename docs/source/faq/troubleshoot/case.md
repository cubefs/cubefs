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

## Question 11
_Description_: Is it possible not to deploy the BlobStore system? If deployed, will there be an automatic error correction function if part of the data for an image file is lost when accessing it via the S3 interface?

**Answer**: If not deployed, the system will use the 3-replica mode. If deployed, it will use the EC (Erasure Coding) mode. For disk failures, both the 3-replica and EC modes have repair capabilities. The difference is that the 3-replica mode uses a good replica for repair, while the EC mode splits data based on erasure coding technology and uses this technology to repair the data.

## Question 12
_Description_: Is there a commercial version of CubeFS?

**Answer**：CubeFS is an open-source project and does not have a commercial version.

## Question 13
_Description_: How to solve the following error message encountered in docker deployment: 
`docker pull cubefs/cbfs-base:1.1-golang-1.17.13 Error response from daemon: Get "https://registry-1.docker.io/v2/": net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)`

**Answer**: You can use an accelerated mirror

## Question 14
_Description_: How can I view the information of the volumes created by CubeFS?

**Answer**: You can use the `cfs-cli` tool to view it. The command is `./cfs-cli volume info volName`.

## Question 15
_Description_: What is the function of lcnode? What does "lc" stand for?

**Answer**: "lc" stands for Life Cycle, which refers to the lifecycle component. It is used for executing periodic tasks at the system level

## Question 16
_Description_: What does "mediaType" mean and how should it be configured?

**Answer**: This refers to the type of storage medium. For example, SSD is 1, HDD is 2. It is a new feature for hybrid cloud added in version 3.5. After upgrading to version 3.5, this must be configured. The configuration method is as follows:
- Add in the master configuration file: `"legacyDataMediaType": 1`
- Add in the datanode configuration file: `"mediaType": 1`
- Run in the terminal: `./cfs-cli cluster set dataMediaType=1`

## Question 17
_Description_: Is there any performance data for CubeFS?

**Answer**: Yes, it is available on the official website documentation: https://cubefs.io/zh/docs/master/evaluation/tiny.html.

## Question 18
_Description_: In production environment, does CubeFS generally use replica mode or EC mode?

**Answer**: Both are used. EC mode is chosen for cost considerations, while replica mode is chosen for performance considerations.

## Question 19
_Description_: What is the endpoint in object storage corresponding to the address of CubeFS? What corresponds to the bucket name?

**Answer**: The endpoint defaults to the address and port 17410 of the objectnode, such as "127.0.0.1:17410". The volume in CubeFS corresponds to the bucket in S3.

## Question 20
_Description_: How should the region in object storage be filled out?

**Answer**: You can fill in the cluster name, such as "cfs_dev"

## Question 21
_Description_: Does CubeFS support mounting multiple volumes at the same time?

**Answer**: CubeFS does not support a single client process mounting multiple volumes. However, it does support running multiple clients on the same machine, with each client mounting its own volume. In this way, multiple volumes (including duplicate volumes) can be mounted.

## Question 22
_Description_: What are the default username and password for the GUI platform?

**Answer**: After the GUI backend is deployed, an initial account with the highest privileges will be generated: `admin/Admin@1234`. The password must be changed upon the first login. For more details, see https://cubefs.io/zh/docs/master/user-guide/gui.html.

## Question 23
_Description_: Can the master, meta, data, and object nodes in the container each be started on just one machine?

**Answer**: The objectnode is stateless and can be started on just one machine. The others need to form a Raft group and require multiple machines to be started.

## Question 24
_Description_: How to query the Raft status related to different components？

**Answer**: You can use commands to query the Raft status. Only the leader will display information about the group members, while others will show their own information.
``` bash
curl 127.0.0.1:17320/raftStatus?raftID=1624  # For datanode
curl "127.0.0.1:17010/get/raftStatus" | python -m json.tool  # For master
curl 127.0.0.1:17220/getRaftStatus?id=400  # For metanode
```
 
## Question 25
_Description_: When using the command cfs-cli user create, an error message is displayed, indicating invalid access key. How can I solve this problem?

**Answer**: Generally, there is a problem with the length of the AK/SK entered. The length of AK is 16 characters, and the length of SK is 32 characters. If you are not sure, you can remove the AK/SK setting. The system will generate an AK/SK for each account by default.

## Question 26
_Description_：Does cubefs support overwriting in EC mode?

**Answer**：Not currently supported

## Question 27
_Description_：Have you ever practiced bcache? Following the official documentation to start the test, found that the cache directory will not cache files at all

**Answer**：Check that bcacheDir is configured correctly in the bcache configuration file. bcacheDir should be configured as the mount directory.

## Question 28
_Description_：Tested the performance before and after enabling bcache with fio command, and found that the read performance decreased after enabling bcache.

**Answer**：The bcache cache space configuration is too small. It has a cacheFree configuration that defaults to 0.15 and evicts data when cache space usage exceeds 1-cacheFree.

## Question 29
_Description_：Client cache bcache, what kind of client does this kind of client refer to?

**Answer**：Currently only fuse clients are supported.

## Question 30
_Description_：Does metadata use btree or rocksdb?

**Answer**：The current version uses btree. Using rocksdb's version is in development.

## Question 31
_Description_：Does cubefs support mixing storage?

**Answer**：cubefs supports hot and cold storage in SSD replica mode -> HDD replica mode -> HDD erasure code mode.

## Question 32
_Description_： When using cubefs, attempting to dd a large file will cause a no space on device error, which will be recovered after a while. What might be causing this? Check the cluster status.Both the metanode and datanode are fine.

**Answer**：The amount of writing is relatively large, and dp can be used faster. Use the following command to create more dp without affecting the data on the volume:
```bash
./cfs-cli volume add-dp [VOLUME] [NUMBER]'
```

## Question 33
_Description_：Does the metaNode support deployment by zone? What Raft mechanism is used?

**Answer**： Supports deployment by zone. You can configure the zoneName parameter in the startup process configuration file. The Raft mechanism used is Multi-Raft.

## Question 34
_Description_：Is there any documentation compiled for arm environment?

**Answer**： See the community documentation. https://cubefs.io/zh/docs/master/faq/build.html#arm%E7%89%88%E6%9C%AC%E7%BC%96%E8%AF%91

## Question 35
_Description_：Is cubefs an online or offline erasure code? Do you support offline erasure codes?

**Answer**：Online. Write three replicas first and then cool down to ec offline, which is supported by the lifetime cost reduction capability in version 3.5.0.

## Question 36
_Description_：What is the difference between fuse client and cfs-client?

**Answer**：They are the same concept. cfs-client is mounted using fuse.

## Question 37
_Description_：Why are erasure codes and object gateways deployed so that files uploaded via the object storage api are written below the data node of the replica collection subsystem?

**Answer**：Volumes were created without erasure codes. To create a volume, use the following command, where volType is the volume type, 0 is the replica volume, 1 is the erasure code volume, and the default is 0.
```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test&capacity=100&owner=cfs&volType=1'
```

## Question 38
_Description_：Can the s3 api be used to access the erasure code subsystem?

**Answer**：If you configure the ObjectNode.

## Question 39
_Description_：What is the order in which metanode,master,datanode are started?

**Answer**：The master starts before the datanode and metanode. Datanodes and metanodes need to be registered with the master and validated.

## Question 40
_Description_：Why doesn't cubefs s3 put api do qos?

**Answer**：The s3 api has separate flow control functionality, including the put api.

## Question 41
_Description_：Where are the accessKey and secretKey generated when the client mounts?

**Answer**：This is automatically created when the user is created. This can be seen by cfs-cli user info.

## Question 42
_Description_：Do you have an offline deployment solution?

**Answer**：Yes. You just start the server manually. See the documentation. https://cubefs.io/zh/docs/master/quickstart/cluster-deploy.html

## Question 43
_Description_：There are 3 ObjectNodes in the cluster, how do clients use them? There is three ips, which one should you use?

**Answer**：You can build an nginx and backend node mount objectnode.

## Question 44
_Description_：Can nodeSet groups of metanode cross zones in multi-AZ scenario?

**Answer**：No. Nodeset is a zone concept. Cross zone, that is, multiple replicas are not in a nodeset. Currently there is a concept of nodesetgrp, which is not enabled by default and large clusters can be considered.

## Question 45
_Description_：The metanode's nodeset is also at the zone level, multi-raft restricts it to the nodeset level to prevent heartbeat storm, and the cross-zone volume metanode selection mechanism selects some machines in each zone to put. Is there any problem with this cross zone storage pool functionality?

**Answer**：Currently supported.