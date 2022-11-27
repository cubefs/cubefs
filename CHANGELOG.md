## Release v2.5.0 - 2022/01/06

### **UPGRAGDE NOTICE**
If your CubeFS version is v2.3.x or before, please refer to the UPGRADE NOTICE in v2.4.0 for upgrading steps. And also please make sure that your fuse client or objectnode version is equal to or older than the servers, i.e. master, metanode and datanode. In another word, newer versioned client can not be used in a cluster with older versioned servers.

### **Main Feature**
* `fuse client`:support auto push data to push gateway
* `master`:reduce flow from interface of "client/partiton" that follower support it and will not redirect to leader
* `raft`:the receive buffer channel size of raft support to be configurable
* `metanode`:implement the content summary
* `master`:domain for cross zone

### **Bugfix**
* `fuse client`:fix: currAddr of stream conn is empty
* `fuse client`:fix: update extent cache no matter whether eh is dirty or not
* `metanode`:when kill metanode(mean while mp stoped firstly) and if apply snapshot happen at the same time, snapshot will be block, causeing metanode can't be killed
* `metanode`:meta node migration be recorded in badmetapartitions concurrently without lock which lead to mp id miss
* `metanode`:return file nodes for statfs
* `metanode`:meta not use warn when consulMeta not set
* `metanode`:makes AppendExtentKeyWithCheck request idempotent
* `fuse client`:push addr shadowed if export port is not set
* `objectnode`:readdir gets insufficient dentries if prefix larger than marker
* `raft`:remove redundant memcopy when reading raft snapshot
* `raft`start tcp listen before starting raft
* ` ltp test`:broken test case ftest01，update and unlock ltp test cases
* ` ltp test`:fix ltptest ci bug
* `docker`:finstall killall command in the docker image
* `cli`:fix cli tool typo

### **Enhance**
* `master`: support disk, datanode, metanode decommission and assign target node
* `master`:return response cache directly, not copy again
* `master`:add vol usedRatio warn log when usage > 90%
* `master`:add master metrics vol_meta_count to export vol dp/mp/inode/dentry
* `master`:del replica with force if decommission hang
* `metanode`:add log for loading meta partitions
* `fuse client`:increase client retry times to avoid mp raft election timeout.
* `grafna`: change grafana volume used size rate from rate to deriv
* `grafna`:support push monitor data to gateway
* `objectnode`: add subdir authorized check for objectnode
* `objectnode`: tracking objectnode modification for cfs-server
* `raft`:check nil when get leader term info
* `raft`: check size when read data from heartbeat port
* `raft`: add getRaftStatus for metanode
* `cli`:update gitlab-ci
* `cli`:Enhancement: add ci check for ltptest result
* `cli`:enhance: upload docker_data when ci tests finish
* `cli`:Create and Update ci.yml
* `style`:rename cfs to cbfs & optimize the libsdk module
* `docker`:change: use ghcr instead of dockerhub
* `build`: add version information and rules for building fsck
* `build`:change: update build status badge

### **New feature brief introduction**

### **Fault domain(Failure domain)**

**1. Purpose**
  In the cross zone scenario, the reliability need to be improved. Compared with the 2.5 version before, the number of copysets in probability can be reduced. The key point is to use fault domains to group nodesets between multiple zones.

* Reliable papers
https://www.usenix.org/conference/atc13/technical-sessions/presentation/cidon

* Chinese can refer to
https://zhuanlan.zhihu.com/p/28417779

**2. configuration** 

**1) Master**

* Config file：master.json
   Enable faultDomain set item "faultDomain": true
* Zone count to build domain
  faultDomainGrpBatchCnt，default count:3，can also set 2 or 1
    
  If zone is unavaliable caused by network partition interruption，create nodeset group according to usable zone
Set “faultDomainBuildAsPossible”  true, default is false

  The distribution of nodesets under the number of different faultDomainGrpBatchCnt
  3 zone（1 nodeset per zone）
  2 zone（2 nodesets zone，1 nodeset zone，Take the size of the space as the weight, and build 2 nodeset with the larger space remaining）
  1 zone（3 nodeset in 1 zone）

* Ratio
  **1) The use space threshold of the non-fault domain（origin zone）**
  After the upgrade, the zone used by the previous volume can be expanded, or operated and maintained in the previous way, or the space of 
  the fault domain can be used, but the original space usage ratio needs to reach a threshold, that is, the current configuration item
  The proportion of the overall space used by meta or data
  Default：0.90
  UpdateInterface：
      AdminUpdateZoneExcludeRatio   = "/admin/updateZoneExcludeRatio"
 
  **2) the use space threshold of the nodeset group in the domain**
  Nodeset group will not be used in dp or mp allocation
  default：0.75
  Update interface：
  AdminUpdateDataUseRatio   = "/admin/updateDomainDataRatio"

**2) Datanode && metanode**

  After the fault domain is enabled, a minimum configuration of the fault domain is constructed under the default configuration:
  Each zone contains 1 datanode and 1 metanode, and the zone name needs to be specified in the configuration file
  There are 3 datanodes and 3 metanodes in 3 zones

  For example, three datanodes (metanode) are configured separately:
  "zoneName": "z1",
  "zoneName": "z2",
  "zoneName": "z3",
  Start after configuration, the master will build a nodeset for z1, z2, and z3, and component a nodesetgrp

**3.  Note**
**1) After the fault domain is enabled, all devices in the new zone will join the fault domain**
**2) The created volume will preferentially select the resources of the original zone**
**3) Need add configuration items to use domain resources when creating a new volume according to the table below. By default, the original  zone resources are used first if it’s avaliable**

| Cluster:faultDomain | Vol:crossZone | Vol:defaultPriority | Rules for volume to use domain |
| ------ | ------ | ------ |------ |
| N | N/A | N/A | Do not support domain |
| Y | N | N/A | Write origin resources first before fault domain until origin reach threshold |
| Y | Y |  N | Write fault domain only |
| Y | Y |  Y | Write origin resources first before fault domain until origin reach threshold |

Note: the fault domain is designed for cross zone by default. The fault domain of a single zone is considered as a special case of cross zone, and the options are consistent

example :` curl "http://10.177.200.119:17010/admin/createVol?name=vol_cross5&capacity=1000&owner=cfs&crossZone=true&defaultPriority=true"|jq .`

### **Content Summary**

**1. Purpose**
In order to query the content summary information of a directory efficiently, e.g. total file size, total files and total directories, v2.5 stores such information as the parent directory’s xattr. 

The parent directory stores the files, directories and total file size of the current directory. Then only need to make recursive of the sub directories, and accumulate the information stored by the directories to query the content summary information of a directory.

**2. Configuration**
  Client config file: fuse.json
  **1) Enable XAttr**
    ”enableXattr”:”true”
  **2) Enable Summary**
    ”enableSummary”:”true”
  Both of xattr and summay have to be set if you want to mount a volume to the local disk. 
  Set summary is enough if you want to access the volume via libsdk.so.

**3. How to use**
  There are two different ways to get the content summary of a directory.
  **1) Fuse mount**
    getfattr -n DirStat yourDirPath
   getfattr can be installed by: yum install attr or apt install attr
  **2) libsdk.so**
    cfs_getsummary (libsdk/libsdk.go)

  **4. Note**
  1)The incremental files’ summary information will be held by their parent directories. But the old files will not. Use cfs_refreshsummary 
    (libsdk/libsdk.go) interface to rebuild the content summary information.
  2)The files, directories and total file size are updated asynchronously in the background. Users are not aware of these operations, but it does 
    increase the requests to meta servers (usually doubled). You are recommended to evaluate the impact to your cluster before using this 
    feature.

## Release v2.4.1 - 2021/12/31

### _**UPGRAGDE NOTICE**_

If your CubeFS version is v2.3.x or before, please refer to the UPGRADE NOTICE in v2.4.0 for upgrading steps. And also please make sure that your fuse client or objectnode version is equal to or older than the servers, i.e. master, metanode and datanode. In another word, newer versioned client can not be used in a cluster with older versioned servers.

### Feature

* `meta&object` introduce ReadDirLimit interface to retrieve partial results [#1234](https://github.com/cubefs/cubefs/pull/1234)
* `fuse client` use ReadDirLimit in fuse client [#1244](https://github.com/cubefs/cubefs/pull/1244)

### Bugfix

* `sdk` makes AppendExtentKeyWithCheck request idempotent [#1224](https://github.com/cubefs/cubefs/pull/1224)
* `meta` start tcp listen before starting raft [1256](https://github.com/cubefs/cubefs/pull/1256)
* `raft` remove redundant memcopy when reading raft snapshot to avoid snapshot hanging [1264](https://github.com/cubefs/cubefs/pull/1264)
* `object` handling range read request in a behavior compatible with S3 [#1286](https://github.com/cubefs/cubefs/pull/1286) [#1298](https://github.com/cubefs/cubefs/pull/1298)

### Enhance

* `sdk` add version in the http requests issued to master to collect client info [1262](https://github.com/cubefs/cubefs/pull/1262)
* `sdk` mitigate the pain of extents fragmentation [1282](https://github.com/cubefs/cubefs/pull/1282)

## Release v2.4.0 - 2021/05/14

### _**UPGRAGDE NOTICE**_

If your CubeFS version is v2.3.x or before, and need to upgrade to v2.4.x , please follow the following upgrade steps:

1. first of all, firewall open two more port (17710 & 17810) in your machine to support tcp multiplexing；

   ```
   17710 is calculated by 17210, the port listened by metanode, add 500
   17810 is calculated by 17310, the port listened by datanode, add 500
   ```

2. then add item '"enableSmuxConnPool"=false' in your datanode's config file, upgrade all datanodes; detail steps as follow:

   ```
   a. using new dataNode bin file replace the old version
   b. add  '"enableSmuxConnPool"=false' in your datanode's config file
   c. restart datanode process
   d. update all the datanodes in the custer
   ```

3. update '"enableSmuxConnPool"=true' in your datanode's config file, restart all datanode again;

   ```
   notes: before set enableSmuxConnPool as true, you must finsh step 2. 
   ```

4. upgrade all metanode bin file and restart meta process;

5. upgrade all master  bin file and restart master process;

6. upgrade all client bin file and restart client process; 

The key point of upgrading from previous versions to v2.4.0 is to make sure ALL datanodes are upgraded to the new version with "enableSmuxConnPool" set to "false" before upgrading other components, such as metanode, master and client. Then datanode can be configured to enable smux conn pool, i.e. "enableSmuxConnPool" set to "true".

### Feature

* `dataNode` smux support [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `metaNode` smux support [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `datanode` `metanode` support TickInterval configurable [#1117](https://github.com/cubefs/cubefs/pull/1117)

### Refector

* `master` remove token-based volume read-write permission control  [#1119](https://github.com/cubefs/cubefs/pull/1119)

### Bugfix

* `datanode` fix build repair task panic [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `metanode` too many open/close stream when delete limit [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `master` do split on metanode partion if inode count larger than maxinodecnt [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `datanode` dp.raftPartition nil pointer [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `datanode` fix datanode repair log print error [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `master` partition be split can be write if not full and less then on step [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `metanode` new a gauge instance every time to avoid a gauge instance modified concurrently [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `master` fix conflict for histogram data when report data concurrently [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `datanode` fix bug when packet opCode=OpExtentRepairRead and network is unstable [#1124](https://github.com/cubefs/cubefs/pull/1124)

### Enhance

* `client` export init should be after init log in fuse client [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `util` flush log before os.exit [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `datanode` format datanode log when delete [#1124](https://github.com/cubefs/cubefs/pull/1124)
* `metanode` update log level to info if mp is not exist [#1124](https://github.com/cubefs/cubefs/pull/1124)

## Release v2.3.0 - 2021/02/26

### _**UPGRAGDE NOTICE**_

Note that when upgrading from versions prior to `v2.3.0` to version `v2.3.0` or beyond, servers(i.e. resource manager, metanode and datanode) must be upgraded before client(i.e. fuse client and objectnode) due to [#1098](https://github.com/cubefs/cubefs/pull/1098).

### Feature
* `client`: support multiple subdir permissions for an individual user. [#1051](https://github.com/cubefs/cubefs/pull/1051)
* `sdk`: introducing libsdk so applications can embed the use of cubefs to a single binary and no additional process is required at runtime. [#1082](https://github.com/cubefs/cubefs/pull/1082)

### Enhancement

* `datanode`: improve the shut down process of datanode so the raft apply id is persisted. [#1030](https://github.com/cubefs/cubefs/pull/1030)
* `raft`: auto fix crc-mismatch raft log crc. [#1039](https://github.com/cubefs/cubefs/pull/1039)
* improve stability of the whole system. [#1098](https://github.com/cubefs/cubefs/pull/1098)

### Bug fix

* `objectnode`: fix key encoding issue of S3 ListObjects API. [#953](https://github.com/cubefs/cubefs/pull/953)
* `datanode`: fix data partition decommission timeout. [#972](https://github.com/cubefs/cubefs/pull/972)
* `metanode`: change mtime of the directory when creating or deleting dentry. [#1000](https://github.com/cubefs/cubefs/pull/1000)
* `datanode`: fix statfs deviation when using EXT4 as the underlying local filesystem for datanode. [#1031](https://github.com/cubefs/cubefs/pull/1031)
* `client`: func sortHostsByDistance change dp's hosts. [#1106](https://github.com/cubefs/cubefs/pull/1106)

## Release v2.2.2 - 2020/09/22

### Feature
* `datanode`: Introducing data partition selector which allow users to customize the client's selection logic for data partition when reading and writing, and can switch at any time. [#853](https://github.com/cubefs/cubefs/pull/853)

### Enhancement
* `client`: Add **enablePosixACL** configuration to enable POSIX ACL feature. [#906](https://github.com/cubefs/cubefs/pull/906) 
* `datanode`: Improved data recovery speed. [#899](https://github.com/cubefs/cubefs/pull/899)

### Bug fix
* `datanode`: Fix the issue that the information in the memory is not released immediately after deleting the extent, which causes the status information of the data partition to be incorrect. [#938](https://github.com/cubefs/cubefs/pull/937)
* `datanode`: Fix the issue that repeatedly sending requests to the failed data partitions. [#937](https://github.com/cubefs/cubefs/pull/937) 
* `objectnode`: Fix the issue that recursive making directory concurrent with sample multi-layer path prefix. [#904](https://github.com/cubefs/cubefs/pull/904)

## Release v2.2.1 - 2020/09/07

### Bug fix
* `master`: Fix the concurrency safe issue when removing raft member. [#893](https://github.com/cubefs/cubefs/pull/893)
* `sdk`: Fix the panic issue while remove data partition concurrently. [#894](https://github.com/cubefs/cubefs/pull/894)

## Release v2.2.0 - 2020/09/01

### Enhancement
* `object`: Implemented S3 api 'DeleteBucketPolicy' for bucket deletion. [#757](https://github.com/cubefs/cubefs/pull/757)
* `master`: Introducing management API for volume capacity expanding and shrinking. New capacity must be set more than 20% lager than used. [#764](https://github.com/cubefs/cubefs/pull/764)
* `master`: Introducing management API for updating node address. [#813](https://github.com/cubefs/cubefs/pull/813)
* `master`: Introducing management API for checking nodes. [#813](https://github.com/cubefs/cubefs/pull/813) 
* `metanode`: Add header and checksum verification for EXTENT_DEL files. [#813](https://github.com/cubefs/cubefs/pull/813)  
* `cli`: Add logging for CLI tool, and added several commands to call master API. [#764](https://github.com/cubefs/cubefs/pull/764) [#801](https://github.com/cubefs/cubefs/pull/801)
* `compile`:Support direct compile or docker cross compile on Arm64 platform. [#779](https://github.com/cubefs/cubefs/pull/779)
* `client`: Introducing **nearRead** option that allow client read data priority from the nearest DataNode to improve reading performance. [#810](https://github.com/cubefs/cubefs/pull/810)  
* `client`: Update volume follower read config from master periodicity. [#837](https://github.com/cubefs/cubefs/pull/837)  

### Refactor
* `object`: Improved compatibility for ListObjects and ListObjectsV2 interfaces. [#769](https://github.com/cubefs/cubefs/pull/769)
* `master`: Check whether the used space between the replicas is consistent when performing the load partition operation. [#813](https://github.com/cubefs/cubefs/pull/813)
* `metanode` `datanode`: Optimize batch delete Extent; add **autoRepairLimitRater** on **DataNode**. [#781](https://github.com/cubefs/cubefs/pull/781)
* `datanode`: Introducing **autoRepair** option for limit data repair speed on **DataNode**. [#842](https://github.com/cubefs/cubefs/pull/842)  
* `datanode`: The data repair task will skip process extent file which has been already deleted. [#842](https://github.com/cubefs/cubefs/pull/842) 

### Bug fix
* `master`: When loading metadata of cluster, using the current ID instead of the loaded ID. [#821](https://github.com/cubefs/cubefs/pull/821) 
* `metanode`: Fix the deadlock problem while **MetaNode** deleting **dentry**. [#785](https://github.com/cubefs/cubefs/pull/785) 
* `metanode`: Fixes [#760](https://github.com/cubefs/cubefs/issues/760) Add **inode** to freeList when NLink is 0, and delete **inode** 7 days later. [#767](https://github.com/cubefs/cubefs/issues/767)
* `datanode` `metanode`: When **DataNode** and **MetaNode** start, the partitions not exist in cluster view will be renamed to **expiredPartition** and skip loading. [#824](https://github.com/cubefs/cubefs/pull/824) 
* `datanode` `metanode`: Fixes issue[#698](https://github.com/cubefs/cubefs/issues/698), Raft instance delete itself by applying ConfChange raft log. [#866](https://github.com/cubefs/cubefs/pull/866)  
* `metanode`: **MetaNode** may not free space. [#838](https://github.com/cubefs/cubefs/pull/838)  
* `client`: Fix the problem that client opening file with outdated extents information. [#783](https://github.com/cubefs/cubefs/pull/783) 
* `client`: Batch inode get mechanism is out of service. [#847](https://github.com/cubefs/cubefs/pull/847) 
* `client`: Fix the problem that makes authorized user mount volume failure. [#828](https://github.com/cubefs/cubefs/pull/828) 
* `datanode`: Fix several issue in automatic repair process for tiny extent. [#855](https://github.com/cubefs/cubefs/pull/855) [#857](https://github.com/cubefs/cubefs/pull/857) 
* `datanode`: Fix the problem that the client still applies to create a new extent to a data partition that has no space. [#867](https://github.com/cubefs/cubefs/pull/867)

### Document
* Updated Q&A, environment&capacity planing documentation. [#801](https://github.com/cubefs/cubefs/pull/801) 

## Release v2.1.0 - 2020/07/09

### Feature
* `console`: CubeFS **Console** is a web application, which provides management for volume, file, s3, user, node, monitoring, and alarm. 
The CubeFS **Console** makes it much easier to access services provided by CubeFS. [#728](https://github.com/cubefs/cubefs/pull/728)  
Please refer to [https://cubefs.readthedocs.io/en/latest/user-guide/console.html](https://cubefs.readthedocs.io/en/latest/user-guide/console.html) to start the **Console**.
* `metanode`: Provide compatibility verification tool for meta partition. [#684](https://github.com/cubefs/cubefs/pull/684)
* `object`: CORS access control. [#507](https://github.com/cubefs/cubefs/pull/507)
* `fuse`: Introduce **fsyncOnClose** mount option. Choose if closing system call shall trigger fsync. [#494](https://github.com/cubefs/cubefs/pull/494)

### Optimize Memory Usage
Release2.1.0 did a lot of work to optimize memory usage.
* Modify the max write size to 128k; limit the rate of Forget requests. [#533](https://github.com/cubefs/cubefs/pull/533)
* If concurrent write operations are more than 256, bufferPool is no longer used. [#538](https://github.com/cubefs/cubefs/pull/538)
* Eliminates the unnecessary memory allocations for Inode struct by using proto InodeInfo struct directly. [#545](https://github.com/cubefs/cubefs/pull/545)
* Use sync Pool and rate limiter to limit the memory usage. [#639](https://github.com/cubefs/cubefs/pull/639)
* Use total count instead of rate. [#642](https://github.com/cubefs/cubefs/pull/642)
* Uses sorted extents instead of BTree. [#646](https://github.com/cubefs/cubefs/pull/646)

### Enhancement
* `master`: Add set/get deleteBatchCount interfaces. [#608](https://github.com/cubefs/cubefs/pull/608)
* `master`: Delete partitions on DataNode/MetaNode concurrently in decommission action. [#724](https://github.com/cubefs/cubefs/pull/724)
* `master`: Added api for getting & setting parameters for batch deleting extents. [#726](https://github.com/cubefs/cubefs/pull/726)
* `metanode`: Filter inode candidates that will be sent to a partition in **batch-inode-get** to save memory. [#481](https://github.com/cubefs/cubefs/pull/481)
* `metanode`: Batch delete inode, unlink&evict dentry. [#586](https://github.com/cubefs/cubefs/pull/586)
* `datanode`: Prioritize healthy data node when sending message to data partition. [#562](https://github.com/cubefs/cubefs/pull/562)
* `metanode` `datanode`: Check expired partition before loading. [#624](https://github.com/cubefs/cubefs/pull/624)
* `object`: Implemented several conditional parameters in **GetObject** action. [#471](https://github.com/cubefs/cubefs/pull/471)
* `object`: Making S3 credential compatible with earlier version of CubeFS. [#508](https://github.com/cubefs/cubefs/pull/508)
* `object`: Totally support presigned url; Partial support user-defined metadata; Making ETag versioned. [#540](https://github.com/cubefs/cubefs/pull/540)   
* `object`: **CopyObject** across volumes; Support coping directory; Support coping **xattr**. [#563](https://github.com/cubefs/cubefs/pull/563)
* `object`: Parallel downloading. [#548](https://github.com/cubefs/cubefs/pull/548)
* `object`: Add 'Expires' and 'Cache-Control' in **putObject/copyObject/getObject/headObject**. [#589](https://github.com/cubefs/cubefs/pull/589)
* `object`: Modify part system metadata. [#636](https://github.com/cubefs/cubefs/pull/636)
* `fuse`: Decrease request channel size to save memory. [#512](https://github.com/cubefs/cubefs/pull/512)
* `fuse`: Let the client daemon inherit all the environment variables but not just *PATH*. [#529](https://github.com/cubefs/cubefs/pull/529)
* `fuse`: Introduces a debug interface **/debug/freeosmemory** to trigger garbage collection manually. [#539](https://github.com/cubefs/cubefs/pull/539)
* `fuse`: Add configuration **MaxCPUs**. [#546](https://github.com/cubefs/cubefs/pull/546)
* `fuse`: View client log through http proto. [#552](https://github.com/cubefs/cubefs/pull/552)
* `fuse`: Support modifying **ModifyTime** of inode by setAttr. [#733](https://github.com/cubefs/cubefs/pull/733)
* `deployment`: Add log map in **docker-compose.yaml**. [#478](https://github.com/cubefs/cubefs/pull/478)
* `deployment`: Introduce nginx to improve compatibility. [#534](https://github.com/cubefs/cubefs/pull/534)
* `test`: Add testing script for S3 APIs in python. [#514](https://github.com/cubefs/cubefs/pull/514) [#595](https://github.com/cubefs/cubefs/pull/595)
* `monitor`: Add detailed vol info in grafana dashboard. [#522](https://github.com/cubefs/cubefs/pull/522)
* `cli`: Some new features for CLI tool: bash completions, configuration setting, decommission, diagnose etc. [#555](https://github.com/cubefs/cubefs/pull/555) [#695](https://github.com/cubefs/cubefs/pull/695)

### Refactor
* `metanode`: Accelerate deletion. [#582](https://github.com/cubefs/cubefs/pull/582) [#600](https://github.com/cubefs/cubefs/pull/600)
* `datanode`: Limit replica number of data partition to at least 3. [#587](https://github.com/cubefs/cubefs/pull/587)
* `datanode`: Set if auto-repair by http interface. [#672](https://github.com/cubefs/cubefs/pull/672)
* `metanode` `datanode` `fuse`: Optimizations on port checking. [#543](https://github.com/cubefs/cubefs/pull/543) [#531](https://github.com/cubefs/cubefs/pull/531)
* `metanode` `datanode`: Validate creating partition request from master. [#611](https://github.com/cubefs/cubefs/pull/611)
* `object`: Refactor copy object function. [#563](https://github.com/cubefs/cubefs/pull/563)
* `fuse`: Replace process of token validation from meta wrapper to client. [#498](https://github.com/cubefs/cubefs/pull/498)
* `fuse`: Adjust rlimit config for client. [#521](https://github.com/cubefs/cubefs/pull/521)

### Bug fix
* `metanode`: When overwriting a file, if the inode in a dentry is updated successfully, ignore unlink and evict inode errors. [#500](https://github.com/cubefs/cubefs/pull/500)
* `metanode`: Fix incorrect metrics collection for volume usedGB in monitor system. [#503](https://github.com/cubefs/cubefs/pull/503)
* `metanode`: **VolStat** can correctly update when force update metaPartitions frequently. [#537](https://github.com/cubefs/cubefs/pull/537)
* `metanode`: The **MaxInodeID** of meta partition is not synchronized when recovered from snapshot. [#571](https://github.com/cubefs/cubefs/pull/571)
* `metanode`: Free Inodes by raft protocol. [#582](https://github.com/cubefs/cubefs/pull/582)
* `metanode`: Painc with **deleteMarkedInodes**. [#597](https://github.com/cubefs/cubefs/pull/597)
* `object`: Fix the parsing issue of the two preconditions of **If-Match** and **If-None-Match**. [#516](https://github.com/cubefs/cubefs/pull/516)
* `object`: Change the time format in list buckets API to UTC time. [#525](https://github.com/cubefs/cubefs/pull/525)
* `object`: Fix xml element of **DeleteResult**. [#532](https://github.com/cubefs/cubefs/pull/532)
* `object`: Empty result in **list** operation when run in parallel with delete. [#509](https://github.com/cubefs/cubefs/pull/509)
* `object`: Change from hard link to soft link in **CopyObject** action. [#563](https://github.com/cubefs/cubefs/pull/563)
* `object`: Solved **parallel-safety** issue; Clean up useless data on failure in **upload** part. [#553](https://github.com/cubefs/cubefs/pull/553)
* `object`: Fixed a problem in listing multipart uploads. [#595](https://github.com/cubefs/cubefs/pull/595) 
* `object`: Solve the problem that back-end report “NotExistErr” error when uploading files with the same key in parallel. [#685](https://github.com/cubefs/cubefs/pull/685)
* `fuse`: Evict inode cache when dealing with forget. [#523](https://github.com/cubefs/cubefs/pull/523)

### Document
* Update related documents of ObjectNode. [#554](https://github.com/cubefs/cubefs/pull/554)
* Added user and CLI introduction, synchronize documentation according to the latest code. [#564](https://github.com/cubefs/cubefs/pull/564)
* Added F&Q section. [#573](https://github.com/cubefs/cubefs/pull/573)

## Release v2.0.0 - 2020/04/10

### Feature
* Multi-Zone replication & Create volume on a specified zone. [#407](https://github.com/cubefs/cubefs/pull/407) [#416](https://github.com/cubefs/cubefs/pull/416)
* Support token authentication for readwrite-mount & readonly-mount of fuse client. [#435](https://github.com/cubefs/cubefs/pull/435)
* A command line tool for cluster operations. [#441](https://github.com/cubefs/cubefs/pull/441) 
* Implemented user security and authorization system to improve resource access control. [#441](https://github.com/cubefs/cubefs/pull/441) 
* Implemented extend attributes (xattr) for metadata and posix-compatible file system interface (mountable client). [#441](https://github.com/cubefs/cubefs/pull/441)

### Enhancement

#### Object storage related
* Support folder operations in S3 APIs. [#450](https://github.com/cubefs/cubefs/pull/450)
* Reduce blocking under concurrency. [#458](https://github.com/cubefs/cubefs/pull/458)
* Implemented more Amazon S3-compatible object storage interfaces to improve compatibility. [#441](https://github.com/cubefs/cubefs/pull/441)

#### Master related
* Make replicas of the data partition a specific option when creating volume. [#377](https://github.com/cubefs/cubefs/pull/377)
* If meta node reaches threshold,set meta partition status to readonly. [#411](https://github.com/cubefs/cubefs/pull/411)
* Add cluster status API. [#457](https://github.com/cubefs/cubefs/pull/457) 

#### MetaNode related
* Checks only file type instead of the whole mode. [#381](https://github.com/cubefs/cubefs/pull/381)

#### Fuse related
* Make `followerRead` a client specific option. [#382](https://github.com/cubefs/cubefs/pull/382)
* Support command line argument for fuse client. [#418](https://github.com/cubefs/cubefs/pull/418)
* Introduce disable `dentry-cache` to client option. [#453](https://github.com/cubefs/cubefs/pull/453)
* Filter target meta partitions in batch iget. [#472](https://github.com/cubefs/cubefs/pull/472)

#### Others
* Yum tool for deploying CubeFS cluster. [#385](https://github.com/cubefs/cubefs/pull/385)

### Bug fix
* Fix the signature algorithm issues. [#369](https://github.com/cubefs/cubefs/pull/369) [#476](https://github.com/cubefs/cubefs/pull/476)
* Avoid inode unlink due to net error. [#402](https://github.com/cubefs/cubefs/pull/402)
* A map structure locked during serialization. [#413](https://github.com/cubefs/cubefs/pull/413)
* Wait for data sync in close syscall. [#419](https://github.com/cubefs/cubefs/pull/419)
* Fix empty result on list objects. [#433](https://github.com/cubefs/cubefs/pull/433)
* Set lookup valid duration for newly created file. [#437](https://github.com/cubefs/cubefs/pull/437)
* Fix `iget` error due to metapartition split. [#446](https://github.com/cubefs/cubefs/pull/446)
* Fix mount fail when volume is full. [#453](https://github.com/cubefs/cubefs/pull/453)
* Fix offline strategy for raft peers of `data partition` and `meta partition`. [#467](https://github.com/cubefs/cubefs/pull/467)

### Document
* Add guide for running CubeFS by yum tools. [#386](https://github.com/cubefs/cubefs/pull/386)
* Update FUSE client mount options. [#439](https://github.com/cubefs/cubefs/pull/439)
* Add documentation for client token. [#449](https://github.com/cubefs/cubefs/pull/449)

## Release v1.5.1 - 2020/01/19

### Enhancement
* Support building docker image that contains both cfs-server and cfs-client. [#353](https://github.com/cubefs/cubefs/pull/353)

### Bug fix
* Only one replica of meta partition can be taken offline at the same time. [#345](https://github.com/cubefs/cubefs/pull/345)
* Check if server port is open before raft leader change. [#348](https://github.com/cubefs/cubefs/pull/348)
* Solved several issue in signature algorithm version 2. [#357](https://github.com/cubefs/cubefs/pull/357)
* Solved the issue related to copying files across folder through object storage interface. [#361](https://github.com/cubefs/cubefs/pull/361)

### Refactoring
* Update dashboard configuration of grafana. [#347](https://github.com/cubefs/cubefs/pull/347)
* Unified the configuration of master address and listening port in documentation. [#362](https://github.com/cubefs/cubefs/pull/362)

### Document
* Added benchmark data and guidelines for deploying CubeFS cluster with Helm in README file. [#350](https://github.com/cubefs/cubefs/pull/350)

## Release v1.5.0 - 2020/01/08

### Feature
* Add a general Authentication & Authorization framework for CubeFS. [commit](https://github.com/cubefs/cubefs/commit/60a4977980e093d746d88e848dc3d1f87e17dfb5)
* Object storage interface. Add ObjectNode to provide S3-compatible APIs. [commit](https://github.com/cubefs/cubefs/commit/d609fedb5c031e27f79dce9c004fdbb101070ac1)

### Enhancement
* Check disk path size in run-docker script. [commit](https://github.com/cubefs/cubefs/commit/ba6f2e068f40515491066cfe349d0266f92d9d5c)
* Support building CubeFS docker image that contains cfs-server and cfs-client. [commit](https://github.com/cubefs/cubefs/commit/e1483f5ec531882d193c3ae8237012789f018831)
* Add go test in docker-compose. [commit](https://github.com/cubefs/cubefs/commit/7ec1e318be0601e2130d2279201541268a29a28b)
* Add authorization to master api getVol. [commit](https://github.com/cubefs/cubefs/commit/df6ff2a3285c2f77d90baa76ef352d3ec3f38bc4)
* Support building under the Darwin(Apple MacOS) and Microsoft Windows operating system environment. [commit](https://github.com/cubefs/cubefs/commit/03718f1ed5ed096b3a3f2b810abeffcaec743159)

### Bug fix
* Set DataNode disk size in docker script to be compatible with lower version of `df`. [commit](https://github.com/cubefs/cubefs/commit/874ea2db1d31b7fc716e9ff9d15c6cd8b60d7c78)
* The `reservedSpace` parameter invalid when gt 30GB. [commit](https://github.com/cubefs/cubefs/commit/c955e66b89239ae2875bb1116771cd77b816b665)
* The mtime of parent inode does not change when create or delete a `dentry`. [commit](https://github.com/cubefs/cubefs/commit/e8c2450c8d2fe4f5cb36a0fd3abfa1215646d559)
* MetaNode `opResponseLoadPartition` removes duplicate locks. [commit](https://github.com/cubefs/cubefs/commit/d1bf9f90cd336bc4d12259c9b5980f1e860d21fb)
* MetaNode leak memory on DeleteMetaPartition operator. [commit](https://github.com/cubefs/cubefs/commit/d09a832544d798c37f366d159bef0635a00e7acc)

### Refactoring
* Change configuration file of Master daemon. [commit](https://github.com/cubefs/cubefs/commit/8d505ed5ccff6fb597b20a1de82b6ba723e32fa5)
* Remove unnecessary function in raft store. [commit](https://github.com/cubefs/cubefs/commit/910d7e4b63f4f2e4642b864903a9ae9c4f048a2c)
* Change metaNode `loadSnapshotSign` command about `inodeCount` and `dentryCount`. [commit](https://github.com/cubefs/cubefs/commit/d50269f0a804656a26bd5f24d851202f47a2b4ad)
* Rename `rack` to `cell`. [commit](https://github.com/cubefs/cubefs/commit/ed269b65062534abbbaf255646fbc3749c66f3d6)
* Improve meta partition replicas verification by `MaxInode` and `DentryCnt`. [commit](https://github.com/cubefs/cubefs/commit/800685f337dd4b17a40340348b2f8cff64ee7769)
* Change parameter name in MetaNode configuration file. [commit](https://github.com/cubefs/cubefs/commit/f40266a183b55d205140bacf8190fc6baf448767)
* Using current applyID to replace snapshot applyID in MetaNode LoadMetaPartition response action. [commit](https://github.com/cubefs/cubefs/commit/a693321306085e94cfbce0f0721fd996d994a9b4)
* Delete data partition and meta partition synchronously. [commit](https://github.com/cubefs/cubefs/commit/77d92b16e040db7279b18f2c4769f476a39290fe)

### Document
* Add design and user guide document for AuthNode. [commit](https://github.com/cubefs/cubefs/commit/108e9986f256e8136cbf1b0ef7eb8bf273e3a0e2)
* Update configuration file sample in document. [commit](https://github.com/cubefs/cubefs/commit/7f869dbc15a1f78d94e67d0ca4b273357e22510d)
* Add design and user guide document for object subsystem. [commit](https://github.com/cubefs/cubefs/commit/360b538e0bff9447305026c36f2351cb7696f8e2)
* Add IO, small file and metadata performance benchmark data to document. [commit](https://github.com/cubefs/cubefs/commit/d2a012ad24f9b1c4d8ff233902ebd649b6d5f45d)

## Release v1.4.0 - 2019/11/13


### Feature
* Datanode : support read from follower and if packet is tinyExtent,then do write it once [commit](https://github.com/cubefs/cubefs/commit/c03e9f6ae70d31c6457612614b9236f686924602)
* Vol add followerRead field to support reading data from foll owner [commit](https://github.com/cubefs/cubefs/commit/7e19af029acadd5b17563c1754833fd289d21fb5)
* Support read from raft follower instead of just leader  [commit](https://github.com/cubefs/cubefs/commit/c03a7bcf15c135629946cd8a1e293787cc3abfa4)
* Support to modify whether vol supports reading data from a replica [commit](https://github.com/cubefs/cubefs/commit/99d8bafca4b5a70b4857436c3d9c6dcd388c850c)
* Introduce read and write iops rate limit [commit](https://github.com/cubefs/cubefs/commit/66205b324ab9d545a8546757d78ac1b9fd62c4a4)
* Add metrics [commit](https://github.com/cubefs/cubefs/commit/929785698d07232729074eca01f636d699e94fde)

### Enhancement
* If vol has been marked deleted,data partitions, meta partition information reported by heartbeat will no longer be accepted [commit](https://github.com/cubefs/cubefs/commit/4e573923f5e5d160b38ca6e4a13d724f45df7f93)
* Use static ip for meta and data nodes [commit](https://github.com/cubefs/cubefs/commit/7078944218268571ea00b43deae4ac930301d0b9)
* Improve debug environment using docker [commit](https://github.com/cubefs/cubefs/commit/a62899782dfa00da7773d9d2156b596d8cde6bc6)
* Support custom meta node reserved memory [commit](https://github.com/cubefs/cubefs/commit/a2b699fc8fdda77a6f6f8d0b4ee299beb9478f11)
* Data partition and meta partition must have three replicas except reducing replicas to 2 [commit](https://github.com/cubefs/cubefs/commit/be2d5f54b95f2ab9566d8a172f9ca151a411f5c0)
* Adjust demo config parameters [commit](https://github.com/cubefs/cubefs/commit/6ccdd06d691ab53f55ab4514e4f1c15f75525525)
* Update grafana dashbord for disk error metric [commit](https://github.com/cubefs/cubefs/commit/f8760cb99c2d44de977bf83c956ec9c74b3e6877)

### Bug fix
* OpFollowerRead if read eof,return error [commit](https://github.com/cubefs/cubefs/commit/3a28ee72df08a37461c927ec4f8e3baf3608112f)
* Get follower read option in init [commit](https://github.com/cubefs/cubefs/commit/d695718a332a580652a5397512abfd1f64c714fe)
* Stream traverse process never gets triggered in some situation [commit](https://github.com/cubefs/cubefs/commit/09de5abea9b675d142035000bbe67482016a42dd)
* Check LoadConfigFile before starting daemon [commit](https://github.com/cubefs/cubefs/commit/938c1075a327ac903ce33770f07b52f0ac6c415d)
* Return error from function LoadConfigFile to the caller [commit](https://github.com/cubefs/cubefs/commit/c8b46cc021550345af3ad0285be92ce81303992d)
* ExtentStorage engine :autoComputeCrc compute crc error [commit](https://github.com/cubefs/cubefs/commit/1b0305425e7cde77c3a2ccd6dd937cfc3afc928e)
* Clean up async delete process of metanode [commit](https://github.com/cubefs/cubefs/commit/7d8382577456e17718a3b235a58a2bbb84d99a84)
* Set default port to non-system reserved port  [commit](https://github.com/cubefs/cubefs/commit/506fac1f803912353dcacd658999166f63b6882d)

### Refactoring
* Leader change not warning on raft [commit](https://github.com/cubefs/cubefs/commit/4ea5b2510f4845c36a9878138e4974ffd69bb7ad)
* Remove go module files for now [commit](https://github.com/cubefs/cubefs/commit/2c9af0a2d62bfba692e3363fdca4d0616a1dc99d)
* Clean up response of get all inodes info [commit](https://github.com/cubefs/cubefs/commit/bc0d850ff30c964f03cc12d7cb58f8cf7976a891)
* Master, DataNode and MetaNode Fix dp or mp offline process [commit](https://github.com/cubefs/cubefs/commit/ced4b8b92482d910c8ab4494eb829d7af54bf02d)
* Use AddNodeWithPort replace AddNode,and delete AddNode API [commit](https://github.com/cubefs/cubefs/commit/b56c817c9572d1f78b36a85cb5a71ea156eed61b)
* Delete reserved space on DataNode config file [commit](https://github.com/cubefs/cubefs/commit/1f64bf46a8f0c25d02e12cfd197b66fb0bb70e91)
* Refine labels of the disk error metric [commit](https://github.com/cubefs/cubefs/commit/c4ee0aea946630d32c25bbc9b9aff7dd788c5314)
* Optimize auto compute crc [commit](https://github.com/cubefs/cubefs/commit/f9d3ba1d4c031c5b3b0f0162833aceb5606fefba)

### Document
* Add use cases [commit](https://github.com/cubefs/cubefs/commit/93a36485f510ceadba2ed819ac1c2f7dafa4a0e2)




## Release v1.3.0 - 2019/09/12


### Feature
* Introduce writecache mount option. [commit](https://github.com/cubefs/cubefs/commit/f86199564c6286828845c8c00adbc0e8b8a9ac7b)
* Introduce keepcache mount option. [commit](https://github.com/cubefs/cubefs/commit/eadf23331258a49218dee04423d60e8a129208c1)
* Add admin API for get all meta parititons under vol [commit](https://github.com/cubefs/cubefs/commit/3cd677b28d24211b258c259e99514f830ddd6be5)
* Support for truncating raft log. [commit](https://github.com/cubefs/cubefs/commit/3cd677b28d24211b258c259e99514f830ddd6be5)
* Dynamiclly reduce the num of replicas for vol. [commit](https://github.com/cubefs/cubefs/commit/07ddb382a9ad379f7393ae39f44f6ebdbb2dfad0)
* The specified number of replica num is supported when creating vol. [commit](https://github.com/cubefs/cubefs/commit/d0c5e78b08c3d3116570e226e388fd87ac23f11b)
* Feature: daemonize server [commit](https://github.com/cubefs/cubefs/commit/ad203059234a80c5d696754b57dd4cf750cb17d2)
* Support log module change loglevel on line. [commit](https://github.com/cubefs/cubefs/commit/9c1e104822672bc9965dfedac2eee4fb854b4880)

### Enhancement
* Extent_store LoadTinyDeleteFileOffset return s.baseTinyDeleteOffset. [commit](https://github.com/cubefs/cubefs/commit/e7800676fc43132f092ef7b011d9a459b784d2e3)
* Enable async read by default. [commit](https://github.com/cubefs/cubefs/commit/5e945554614c0deb4d56daf0f3b1c62c25bf93ec)
* Improve log message details for clientv2. [commit](https://github.com/cubefs/cubefs/commit/14e3dd7e02a04babb96d7112860b5a246e5faa45)
* Compatible with string when get bool config. [commit](https://github.com/cubefs/cubefs/commit/a485e82f69c0b99ab3581362b3a3002f58d7c211)
* Add performance tracepoint for clientv2. [commit](https://github.com/cubefs/cubefs/commit/9c5ee2e51c0a127a92f346f748f88ab65325ad9e)
* Align out message buffer size with max read size. [commit](https://github.com/cubefs/cubefs/commit/b0d82fb77f1db52a61f66f92c4cc4ee7e368aa7f)
* For splitting meta partition,updating meta partition and creating new meta partition are persisted within a single transaction. [commit](https://github.com/cubefs/cubefs/commit/5af7a9ebbcd4578e3a09cb4626a6c729e61f9e00)
* If metanode used memory is full,then the partition must set to readonly. [commit](https://github.com/cubefs/cubefs/commit/6d80fdfd4126a9a5ed4bb1cbf91d2baddf8227ce)
* Set report time to now after creating data partition. [commit](https://github.com/cubefs/cubefs/commit/01153377431fa3aa604b89a0e7c34c7aac456615)
* Set writeDeadLineTime to one minute,avoid metanode gc reset conn which snapshot used as much as possible. [commit](https://github.com/cubefs/cubefs/commit/d4a94ae1ecff9748c4c71b62c63e6ded6c7d1816)
* Add raft monitor. [commit](https://github.com/cubefs/cubefs/commit/9e2fce42a711571f903d905590d7cfba0cabf473)
* If the creation of a data partition fails, the successfully created replica is deleted. [commit](https://github.com/cubefs/cubefs/commit/3cd677b28d24211b258c259e99514f830ddd6be5)
* If the creation of a meta partition fails, the successfully created replica is deleted. [commit](https://github.com/cubefs/cubefs/commit/3cd677b28d24211b258c259e99514f830ddd6be5)
* Add unit test case. [commit](https://github.com/cubefs/cubefs/commit/3cd677b28d24211b258c259e99514f830ddd6be5)
* Passing create data partition type to datanode. [commit](https://github.com/cubefs/cubefs/commit/abffb1712b9d257abd2bb4737f5d596b99e1c115)
* If create dp is normal,must start Raft else backend start raft. [commit](https://github.com/cubefs/cubefs/commit/2701be38d85436650eb1e07c24b944c2847f4b5d)
* The tickInterval and electionTick support reading from a configuration file [commit](https://github.com/cubefs/cubefs/commit/6f0952fbd77a0cfa22df9852afbde02a6a2d86a1)

### Bugfix
* Fix: add del vol step after ltptest in travis-ci test script. [commit](https://github.com/cubefs/cubefs/commit/8274cd721ddc98e7ac3bbabd1c148232dcc62694)
* Clientv2 file handle memory leak. [commit](https://github.com/cubefs/cubefs/commit/62ecf860d1694cbe0020fd331235704bb905774a)
* Redirect stderr to an output file in daemon. [commit](https://github.com/cubefs/cubefs/commit/ead87acb7147594867832ebae5e61cacb30fe2d9)
* Exclude data partition only when connection refused. [commit](https://github.com/cubefs/cubefs/commit/a4f27cfd9be309c73c9fde8b45dd25fe821f79b7)
* When delete DataParittion,the forwardToLeader mayBe painc. [commit](https://github.com/cubefs/cubefs/commit/c4b0e9ee77db42d9e190b40cebaffead1be106de)
* Metanode load error mayme shield. [commit](https://github.com/cubefs/cubefs/commit/7186621728006ba92de9bbd2a94f2cc59a1d22ec)
* Truncate raft corrupt data. [commit](https://github.com/cubefs/cubefs/commit/fceb29fc08d534f03bedf0c0c56b591518f24cac)
* When meta node memory usage arrive threshold, split meta partition occurred dead lock. [commit](https://github.com/cubefs/cubefs/commit/6bb1aaf2460185e95904f1cd5df0ce0e8665f09a)
* SplitMetaPartition race lock with updateViewCache. [commit](https://github.com/cubefs/cubefs/commit/d0737f20de832d778b0a9b7b01d69b74c61be696)
* After the vol is created and before the heartbeat report, the status of the data partition is set to read only after the check dp operation is performed. [commit](https://github.com/cubefs/cubefs/commit/81435471a11a0866a6fe958f442e9d642de92779)
* When disk error,the raft cannot start on new data server first. [commit](https://github.com/cubefs/cubefs/commit/74b9f1b737d3fafc8c09c336ed91c8419cceb664)
* OpDecommissionDataPartition delete dataPartition on new server. [commit](https://github.com/cubefs/cubefs/commit/974e508bdbd161ab213acaad3788122dcba4b45d)
* Datanode may be painc. [commit](https://github.com/cubefs/cubefs/commit/ac47e9ad5659a0b0b78e697f1f50e4787775616f)
* Datanode auto compute crc. [commit](https://github.com/cubefs/cubefs/commit/84707a5f2c80499e6428f20509a610fa8f8efd97)
* DataNode: when dataPartition load,if applyId ==0 ,then start Raft. [commit](https://github.com/cubefs/cubefs/commit/042f939bf3a0b7c7ea4578cde94513157a5f23d5)
* The reported data partition usage decreased, and the statistical usage did not decrease accordingly. [commit](https://github.com/cubefs/cubefs/commit/2386b48ad9c1507ab939ec28042b643e5c55db4d)
* Docker metanode.cfg add totalMem parameter. [commit](https://github.com/cubefs/cubefs/commit/353ece00c3ef739ddd6b7f117a68d79e40d7ac27)
* Datanode deadlock on deletePartition. [commit](https://github.com/cubefs/cubefs/commit/4142d38ef9f46a243cc1b18f789a1c1ecfba4af2)
* DataNode may be painc. [commit](https://github.com/cubefs/cubefs/commit/4a3ba7403fac4d91b90d1a0c6f8fa13345152e00)
* Exclude dir inode in the orphan list. [commit](https://github.com/cubefs/cubefs/commit/7fff89b3c0efea31cc4d07c9267a1c3e89724888)
* Evict inode cache after successful deletion. [commit](https://github.com/cubefs/cubefs/commit/f63e5c657d0d45238309ec28fe3831993f402e20)
* The actual reduction in the number of replicas exceeds the expected reduction in the number of replicas. [commit](https://github.com/cubefs/cubefs/commit/d6b118864b134bdb8cec6ccb1b75b7699d1d8c08)
* Compatible with old heartbeat mode, old heartbeat mode does not report volname. [commit](https://github.com/cubefs/cubefs/commit/97da53f06540189bb79e5331b13c2fd097d2b96f)
* Metanode mistakenly delete empty dir inode. [commit](https://github.com/cubefs/cubefs/commit/54774529743033bfeaf4e1eb67fb511472ba1337)
* Treat ddelete not exist error as successful. [commit](https://github.com/cubefs/cubefs/commit/4f38ebad02d776d3faf4df594e7f17625755c7ab)
* Fuse directIO read size can exceeds buffer size. [commit](https://github.com/cubefs/cubefs/commit/b15b78293b5e10f1d43d2c1397d357cc341a8124)
* Fix Datanode retain RaftLog [commit](https://github.com/cubefs/cubefs/commit/4dd740c435d69964f57222ebc55dea665c101915)
* Fix: Datanode: when tinyExtentRepair auto repair,it has been [commit](https://github.com/cubefs/cubefs/commit/b370e8d220d314c03a4f40e518e7f8d101edab73)
* Fix: Storage :when write tinyExtent,if offset!=e.datasize,return error [commit](https://github.com/cubefs/cubefs/commit/73355d2f1f72550873fc32e95acaf6623aa3d6a5)
* The buf is not reset and the old data is repeatedly written to the ump warning log, resulting in the overall performance degradation of the cluster [commit](https://github.com/cubefs/cubefs/commit/d69db16252f8236acb6cb7348d6a625a460a5254)
* DataPartitionMap occurred error which is concurrent map iteration and map write [commit](https://github.com/cubefs/cubefs/commit/3cf7c68015b89858c24152fe890c5e733414908a)
* Client gets stale file size if streamer is auto evicted. [commit](https://github.com/cubefs/cubefs/commit/4831443a56a7342e1843db225e29645a1814007c)
* Update export init for consul register [commit](https://github.com/cubefs/cubefs/commit/c06e1dbaf7dac5cfdd1faff2d8c215a050944217)
* Err is shadowed in server main function [commit](https://github.com/cubefs/cubefs/commit/8ce0f64ad0f19cf8dcfab5cd1df9bf45de7d5017)
* If master only create DataPartitionCnt is 10,then cannnot mount [commit](https://github.com/cubefs/cubefs/commit/5e47ca00d35b8ca0fa62a9e4a52dbac6da9d5ae9)
* Update export init for consul register [commit](https://github.com/cubefs/cubefs/commit/dfca18fa1b394fcceee2bf0696737d7197d77ddc)
* DataPartition disk error ,not recvoery raft log on new datanode [commit](https://github.com/cubefs/cubefs/commit/d5febd10da6008dd4fca46d91b4e7d29ebadb9ec)
* Datanode register hang bug [commit](https://github.com/cubefs/cubefs/commit/8eaeabf5c1a74b6057a43bf0e856e94823c9ad3a)

### Refactoring
* Sdk When creating a datapartition, select the datapartition retry strategy. [commit](https://github.com/cubefs/cubefs/commit/226578551137d9655bf23aa3c8220c4fe0fc8957)
* Refactoring SDK: when write datapartition num greater 10 ,then trust master. [commit](https://github.com/cubefs/cubefs/commit/adfe95deef05b10d812736e613f88fbceadb260e)
* When disk error,the datapartition recover only recover avali data on tinyExtent. [commit](https://github.com/cubefs/cubefs/commit/1910bb9d8112acdaa87456f54abae648f817b60d)
* Decommission Meta or Data Partition must sync response to master. [commit](https://github.com/cubefs/cubefs/commit/b4350ade23b2d81bb5840199fcfe46d33a8cff5a)
* Synchronized decommission the data partition. [commit](https://github.com/cubefs/cubefs/commit/501b36a1fde217116f2d9ef37ae1a37a6812798c)
* Datanode api /partition add raftStatus. [commit](https://github.com/cubefs/cubefs/commit/2a2fa5d32de2ab42b7ce86da73f1171df5bc372e)
* StartRaftLoggingSchedule not use goroutine. [commit](https://github.com/cubefs/cubefs/commit/e65d12d5ddfdf8bad6a711412dd40c62c0b110e2)
* Metanode must config totalMem. [commit](https://github.com/cubefs/cubefs/commit/9e9ae6a74cc588aa5d58038d4b8582d71a8c5c97)
* Sync tinyDeleteExtent time change to 1 days. [commit](https://github.com/cubefs/cubefs/commit/bf4411dbd52ce03c470aab57f6b321a8de24080c)
* Change partitionId varliable to partitionID. [commit](https://github.com/cubefs/cubefs/commit/c366fcde54ef8e07226b7d118dbd40379e317749)
* Datanode delete SnapshotFile Pool. [commit](https://github.com/cubefs/cubefs/commit/b376e01732f97816bd18a6dc43f96603605e4501)
* Exporter add ump. [commit](https://github.com/cubefs/cubefs/commit/669df1f3d7632d161d87a1f9d9393def6702eab8)
* DataNode: compatible old dataPartition Meta info. [commit](https://github.com/cubefs/cubefs/commit/bcdfce265683109ef59250d095a71b9b1018ba31)
* Add ltptest log. [commit](https://github.com/cubefs/cubefs/commit/67ce466ce1aa63559820453a01ecb1281fad3bd6)
* Docker metanode config.json change totalMem to 6GB. [commit](https://github.com/cubefs/cubefs/commit/af066310afc265343346521f4db0c737d28a40d6)
* Datanode create dataPartition select disk function change. [commit](https://github.com/cubefs/cubefs/commit/07617e7f6f2903e326fba0de944f3ad1cd012eff)
* Datanode start must start StartRaftLoggingSchedule func. [commit](https://github.com/cubefs/cubefs/commit/78bb09f858a7dec007537172afb1b6d31c4dca48)
* Master create vol min default DataPartition set to 10. [commit](https://github.com/cubefs/cubefs/commit/f9062c618d0ad0a3046411348902326c90895739)
* Docker: run ltptest print errorinfo. [commit](https://github.com/cubefs/cubefs/commit/eee4553dfee29ec810f959730b224e0bb7935437)
* When load dp,if dp status is normal,then start raft ,else wait snapshot has recover. [commit](https://github.com/cubefs/cubefs/commit/cc68ec3266169bb1bf96f3f254dd55dcd7f93fc7)
* Datanode delete unused func. [commit](https://github.com/cubefs/cubefs/commit/f3297f91ec4482b3c6a6d03671319a15fc39f0df)
* If not config warnLogDir,then donnot write umplog to disk. [commit](https://github.com/cubefs/cubefs/commit/6a9dbc369b670b9c2d128cdd214dd56a528202f2)
* DataNode: if not config raftDir,then not start server. [commit](https://github.com/cubefs/cubefs/commit/480c44af49747dcb7ac37de99f23682391367e06)
* Add log on metanode delete extent. [commit](https://github.com/cubefs/cubefs/commit/dde79c0636a9907b4314613d15dbe7c61169f1bb)
* Doc :delete warnLogDir config. [commit](https://github.com/cubefs/cubefs/commit/6f05a78e23af638fd85015c09055e616fc6d0b4e)
* Keep mount point in error stataus when client is killed [commit](https://github.com/cubefs/cubefs/commit/df1a4d9cba3d656c14686164cb0e57380922a77d)
* Log checkroration checkTime change to 1 second [commit](https://github.com/cubefs/cubefs/commit/f732ade2bc70e5207163193f0ea0923b7621df39)
* Metanode change deleteDentry or deleteInode api to log.LogDebugf [commit](https://github.com/cubefs/cubefs/commit/149d661e63a2ce3393ad97be9e8ae02d2b2d141a)
* Increase the judgment condition of disk error,add syscall.EROFS [commit](https://github.com/cubefs/cubefs/commit/06a3ab3be294f198f2a7e37b6c254798b939bce2)
* Sync code from git.jd.com/cubefs/cubefs [commit](https://github.com/cubefs/cubefs/commit/2b2461598fdebe0abac152218cbbd83d48c19bf2)


## Release v1.2.1 - 2019/07/19

### Enhancement
* When datanode or metanode start,must compare partition with master and gofmt project. [commit](https://github.com/cubefs/cubefs/commit/02e1989065cd89691af0991b376527ef3a029a34)
* Datanode add backend check disk status func. [commit](https://github.com/cubefs/cubefs/commit/1b5ac0e674e37602410b2762254d093b62eb4cdf)


### Bugfix
* Update rocksdb build depends. [commit](https://github.com/cubefs/cubefs/commit/f665f2f33a63b55835a759c207fc0d7b00ca05a1)
* Metanode panic when deleting meta partition. [commit](https://github.com/cubefs/cubefs/commit/c228ced18d810ef527c8d7a36e278a826ed045a3)
* When raft member become leader,must apply from appliyID to commitID. [commit](https://github.com/cubefs/cubefs/commit/f5c2d3211717bb14664b32a4f6bbcaba7b1e7d18)


## Release v1.2.0 - 2019/07/16

### Feature

* Clientv2: add dentry cache. [commit](https://github.com/cubefs/cubefs/commit/add53510fd96b406572c99f781660aae29736981)
* Vendor: introduce jacobsa daemonize package. [commit](https://github.com/cubefs/cubefs/commit/97a1360de29fbe3007a32b80d601ea966d5140a2)
* Client: start client as a daemon. [commit](https://github.com/cubefs/cubefs/commit/f1efb1c0a7f41bb4673cf350fb636781c6321ca4)
* Clientv2: daemonize client.[commit](https://github.com/cubefs/cubefs/commit/2055e73092c34f86dc0982612063c3a33ceb0ff7)

### Enhancement:

* Update docker helper script. [commit](https://github.com/cubefs/cubefs/commit/5e00b4e9021a197f581c3344059aafbf549da080)
* Data sdk: use ip address instead of partition id to rule out unavailable data partitions. [commit](https://github.com/cubefs/cubefs/commit/568b40cd7d9169fe6d72882ef725971dce2a46fd)
* Add monitor add prometheus, grafana in docker helper script.[commit](https://github.com/cubefs/cubefs/commit/354a66cf660f4eaef1049045d7daa3b6b18565d7)
* Add GOPATH check in build.sh. [commit](https://github.com/cubefs/cubefs/commit/7b314c5f031aec16d8f5fcda5adf9ec089e2c4fb)
* Integrated rocksdb compilation. [commit](https://github.com/cubefs/cubefs/commit/5dbc0b2d8c906166bb2b592c4dd52206d096742c)
* Update makefile. [commit](https://github.com/cubefs/cubefs/commit/2ec8603e7300e8eb14778c5add90f5db795757ab)
* Docs: start client as a daemon. [commit](https://github.com/cubefs/cubefs/commit/714209a76dfed3c020df6485185f199ccb45d3a1)
* Update docs: add make build in readme and docs. [commit](https://github.com/cubefs/cubefs/commit/3c6ccae977ab4c2054fb49bb5fa65d4d22fbb783)
* Use one thread to send and recive from follower on primary backup replication protocol .[commit](https://github.com/cubefs/cubefs/commit/1009af799dc91a089adf70fca97ea3b473f2eaf6)
* Change random write raft serialize not use json. [commit](https://github.com/cubefs/cubefs/commit/6144a94c280cee34eae42bd9dc04d009969bea33)


### Bug fix:

* Fix: when datanode is killed, maxBaseExtentId is corrupted.[commit](https://github.com/cubefs/cubefs/commit/e8552fe1c194acc7d58db969dc8673c1a150bbc5)
* Once vol is created successfully, the state of the data partition it has should be writable. [commit](https://github.com/cubefs/cubefs/commit/2d954a376fb500cc6dc6d32f3191cfe2541cd3a6)
* Metanode: fix create dentry exist error. [commit](https://github.com/cubefs/cubefs/commit/f55846af3c2522b4773e5e2dd2c2871b88f80267)
* Add privileged for docker-compose server node. [commit](https://github.com/cubefs/cubefs/commit/718e26308c2ee19ca98c52abdef6c92718ebbbd6)
* The retain space field in config file is not allowed to be larger than the default value. [commit](https://github.com/cubefs/cubefs/commit/70a42d35369f6fb3fb5050539eee063e128cdb52)
* Fix: when data node gets a disk error, the disk status does not changed. [commit](https://github.com/cubefs/cubefs/commit/2b056234193351fad1dbedde2ea11626c1d6e97a)
* Fix: build error. [commit](https://github.com/cubefs/cubefs/commit/f3eead28cbb3640fb8a60abea16422f8dd4b54e9)
* Fix build with lua bug. [commit](https://github.com/cubefs/cubefs/commit/6207f7e7a8ddc46691b9058cad0662cb4c27f84d)
* Check whether vol is valid. If there is no meta partition with start being. [commit](https://github.com/cubefs/cubefs/commit/dd001daf37c6517685eb7515bc46c4ed85a0d7a0)
* Build: update build.sh for user permission. [commit](https://github.com/cubefs/cubefs/commit/7532a51ed150e95c69cae8b2b890f545608264f9)
* Clientv2: fix to get the latest file size. [commit](https://github.com/cubefs/cubefs/commit/fba924fb1ef0fb40a8cc25956812c222b7309fc2)
* When create dp failed,then Check all dp host if the host is alive. [commit](https://github.com/cubefs/cubefs/commit/7e0eed1b700e8b6c9f8a2a5b43a9432b52198219)
* Fix bug: when read tinyDeleteFile return eof error. [commit](https://github.com/cubefs/cubefs/commit/b7ee83f908024e042a296faf40090e218e462e4d)
* Build: fix multi users build bug. [commit](https://github.com/cubefs/cubefs/commit/a885777b9da97957cc1eddfee102be3707763b64[commit](https://github.com/cubefs/cubefs/commit/a885777b9da97957cc1eddfee102be3707763b64)
* Build: fix no root user permission bug. [commit](https://github.com/cubefs/cubefs/commit/abe6e1fb5a4193183729a2d4d06e395d51a567e6)
* Init replicas info from create data partition request. [commit](https://github.com/cubefs/cubefs/commit/63b6d11d721f8d03b2df07e5cbba10ffa83fc4a2)


#### Change/Refactoring

* Add cfs-base dockerfile. [commit](https://github.com/cubefs/cubefs/commit/7b240f21ba6a0037484dd5591bfce0b8f88f2844)
* Specification code and metanode config file. [commit](https://github.com/cubefs/cubefs/commit/03f79e737099cd3f6e782f6abbb781b8ab871aa7)
* Add go mod. [commit](https://github.com/cubefs/cubefs/commit/5735625b3c60bcba72403cd80ccf0238552f0db8)
* Docs: update according to recent changes. [commit](https://github.com/cubefs/cubefs/commit/65e0aa355568424b5fa6137f138be6e28ac9c647)
* If warnLogDir is not specified in the config files, server or client will return an error. [commit](https://github.com/cubefs/cubefs/commit/236e454c376e8edaa8216f1b384a8afd6268037c)
* Rename RestSize to ReservedSize. [commit](https://github.com/cubefs/cubefs/commit/f018e4b20f0f4828a39fe86996940ccfd1779932)
* Print more detailed error information when server starts failed. [commit](https://github.com/cubefs/cubefs/commit/0c1b9532f7b50bf7b0d6a5943996bf0f1a11badf)
* Remove docker client security opt. [commit](https://github.com/cubefs/cubefs/commit/d346a7d83548aec83590f94e1a788eeed6627455)
* Add disk config detailed description. [commit](https://github.com/cubefs/cubefs/commit/3142c54aa229451de02ae8059c248a9f89e6c835)
* By default create 10 data partitions and 3 meta partitions when creating volume. [commit](https://github.com/cubefs/cubefs/commit/06045cd2c8960fc4b23126e8c42b52557d3525c6)
* Doc: add clarification that resource manager is AKA master. [commit](https://github.com/cubefs/cubefs/commit/02781071b2dc7ceac751d746113b3cfb205d75ce)
* Normalize exporter cluster name. [commit](https://github.com/cubefs/cubefs/commit/8543c15380a8628962e54e1a3eff15bc28f425bf)
* Set the value of rack for data node to default. [commit](https://github.com/cubefs/cubefs/commit/39000f6d3240bed35de23f459e7c57ecef9a7c76)
* Refine decommission disk. [commit](https://github.com/cubefs/cubefs/commit/6b6ce51f62389fcfc6d078768fa6bdf621c2220f)
* Change document about metanode config. [commit](https://github.com/cubefs/cubefs/commit/f142311c8d9a5d7c7c66fcdbec2323d03f40e72d)
* Doc: update docker helper section. [commit](https://github.com/cubefs/cubefs/commit/851f90c85386af3b603480d009cdb708794120ec)
* Update quick-start-guide.rst. [commit](https://github.com/cubefs/cubefs/commit/ac3f176ea3e8a2157d66637abdd8d552bcafcb10)
* Change dataPartition IntervalToUpdateReplica to 600 seconds. [commit](https://github.com/cubefs/cubefs/commit/4c0521d7466950dcfdc33dad3e31c50e229c3d26)
* Add third-party directory. [commit](https://github.com/cubefs/cubefs/commit/8cb94afd07afdcfedb58227275f69dad13278283)
* Write data partition decommission url message to log. [commit](https://github.com/cubefs/cubefs/commit/2520f73b2b794b1206f3401f8316a736121d487e)
* Go fmt project. [commit](https://github.com/cubefs/cubefs/commit/e841bb8e4b01813ba3ba9f29ca9e6cdf4f501bd6)
* Data sdk: check amount of writable data partitions when mount. [commit](https://github.com/cubefs/cubefs/commit/9f55ccd37fafe2f989e75341117711ecb3a7dc88)
* Delete third-party directory. [commit](https://github.com/cubefs/cubefs/commit/6d7838a3ba8924c5d41661ff56737ec0d2ab4636)
* Update docs: remove server and client. [commit](https://github.com/cubefs/cubefs/commit/ff5e4c9951422bd5d4176efd2f004b10ef0f138b)
* Pass the hosts to data node when creating the data partition. [commit](https://github.com/cubefs/cubefs/commit/8cf3447de95224744eece033aa96d81dc2755727)
* Warn log message unicode encoding is converted to utf8. [commit](https://github.com/cubefs/cubefs/commit/1bbdc2bef584051cb71c09423f0e81aa8fb94f4c)
* Ignore generated build files. [commit](https://github.com/cubefs/cubefs/commit/e243fa79a8306e616b49b5cc87ebcaf120e0d703)
* Build: update build.sh and Makefile. [commit](https://github.com/cubefs/cubefs/commit/b6004b225f32df326b3dae7a664f8ff56c91bf81)
* Change packet attr func and ReadTinyDelete impl log. [commit](https://github.com/cubefs/cubefs/commit/e37c7d7ee0575d67f7758802a9f4e08c4bcfdecd)
* Add log when read tinyDeleteFile failed. [commit](https://github.com/cubefs/cubefs/commit/fc1e8f3b9fc55774267aea615c8c2904356cb0dc)
* Add log when streamRead and ExtentRepairRead. [commit](https://github.com/cubefs/cubefs/commit/dfa1ae5eb8a13df6b8b1dd9b144fa1018f08b71a)
* Remove unused source code. [commit](https://github.com/cubefs/cubefs/commit/9a91850c4cfa04cb321372a1008c0dfff2c0520a)
* Build: update build.sh. [commit](https://github.com/cubefs/cubefs/commit/1f65750588ada80c1908d9b85fb7de2bee901133)
* Update README.md. [commit](https://github.com/cubefs/cubefs/commit/bf8cd97f8979083b4832cabdb44db025fe39c22b)
* Build: remove client unused depends. [commit](https://github.com/cubefs/cubefs/commit/f613e804cc2b664b1f154fed66829a0fea9b933d)


## Release v1.1.1 - 2019/06/11

### Feature

* Client: enable support to fifo and socket file types. [commit](https://github.com/cubefs/cubefs/pull/80/commits/a1b118423334c075b0fbdc0b059b7225e8c2173f)
* Clientv2: use jacobsa fuse package instead of bazil. [commit](https://github.com/cubefs/cubefs/pull/68)
* Docker: introduce docker compose to create a local testing CubeFS cluster. [commit](https://github.com/cubefs/cubefs/pull/79)


### Bugfix

* Meta: update atime in inode get operation. [metanode] [commit](https://github.com/cubefs/cubefs/pull/80/commits/cf76479d251ee7214d0d27625fab95498ee1ae0c)
* Fix: potential panic if send returns nil resp with no error. [client] [commit](https://github.com/cubefs/cubefs/pull/80/commits/22f3623d5e24a84c7d1ec49fcb72be375d0d4b92)
* Fix: raft election takes a long time and timeout; issue a panic if raft server is down. [raft] [commit](https://github.com/cubefs/cubefs/pull/80/commits/26a1f2f826d5ddd3bb6803ec462f928d12597bdd)
* Fix: potential deadlock if applyHandler. [master] [commit](https://github.com/cubefs/cubefs/pull/80/commits/cb1eb6ebfcfedc4d1f8bd97e6b7d776bc8ecf4f4)
* Fix: put vol to cache after it is persistent. [master] [commit](https://github.com/cubefs/cubefs/pull/80/commits/f31c5d8e260a878b5bfe9d09d8ce196c9aa2abc8)
* Fix: partition is nil when apply remove raft node. [datanode] [commit](https://github.com/cubefs/cubefs/pull/80/commits/971cc4b9105af77b4ada52159125225e713754c0)
* Fix: metanode painc. [metanode] [commit](https://github.com/cubefs/cubefs/pull/80/commits/a3d8b1f19b2c3f52af561e46c0c8b3eea15472fa)
* Fix: panic when pprof does not start. [commit](https://github.com/cubefs/cubefs/pull/80/commits/77a0efe9aa35d68c039a05dc2780d6902ec08d53)

### Enhancement

* Sdk: retry if mount failed in case master is unavailable temporarily. [commit](https://github.com/cubefs/cubefs/pull/80/commits/d20732dbbc343dffe1893f3766305322ae8d05de)
* Build: add verbose build info. [commit](https://github.com/cubefs/cubefs/pull/80/commits/e5316f98429ed0b680cda4e4af994774c59ac8bd)
* Master: introduce data partition over-provision. [commit](https://github.com/cubefs/cubefs/pull/80/commits/642d1f15696b42d8470392c4dab40e4e3b6d3d8a)
* Master: reserve writable data partition amount according to capacity instead of a const. [commit](https://github.com/cubefs/cubefs/pull/80/commits/06186f0b62df534fae3d2f817ccfb61dba921c01)
* Monitor: Use UMP performance monitor if exporter is not enabled. [commit](https://github.com/cubefs/cubefs/pull/80/commits/ddf608f7e1705e79ba1c07285f4e61dbdf86189d)



## Release v1.1.0 - 2019/05/07

### Change / Refactoring

* Rename the repository from cfs to cubefs.
* Use own errors module instead of juju errors due to license incompatibility.
* Metanode: Change not raft leader error to tryOtherAddr error.

### Bugfix

* Master: Partition recovered but the status not changed.
* Datanode: Report to client with proto.OptryAgain resultcode when datapartition does not exsit.
* Raft: A member must apply playback from old apply id to commit id after elected as leader.
* Metanode: generate identical inode number under extreme conditions. [commit](https://github.com/cubefs/cubefs/commit/45b6daa88911eaaebabe299b05fad565761f97ed)

### Enhancement

* Master: Add ump warn packet.
* Master: Remove redundant calling of loadMetaData method.
* Master: Reload meta data after leader changed.
* Master: make dataPartition disk Path persistent. [commit](https://github.com/cubefs/cubefs/commit/6dce99a755d0e32828296a002c6aa50ebfe07c63)
* Master: Volume creation supports specifying the amount of meta partitions. [commit](https://github.com/cubefs/cubefs/commit/2e7bbf2dde555496f9476eb3a9e0ab8200d44d8f)
* Metanode: Add totalMem in configFile.
* Datanode: Change default disk reserved space.
* Datanode: Add volname in heartbeat report.
* Raft: Use raft.ErrNotLeader instead of ErrNotLeader.
* Client: Create a dummy node instance if inode does not exist.
* Client: Add UMP monitor alarms for read/write/fsync errors.
* Client: Suppress some error messages. [commit](https://github.com/cubefs/cubefs/commit/1f6062000d2049a875c3b16a5cc65d61cad1b367)
* Log: Automatically create subdirectory under the log directory.

## Release v1.0.0 - 2019/04/02

The initial release version.
