## Release v1.4.0 - 2019/11/13


### Feature
* datanode : support read from follower and if packet is tinyExtent,then do write it once https://github.com/chubaofs/chubaofs/commit/c03e9f6ae70d31c6457612614b9236f686924602
* vol add followerRead field to support reading data from foll owner https://github.com/chubaofs/chubaofs/commit/7e19af029acadd5b17563c1754833fd289d21fb5
* support read from raft follower instead of just leader  https://github.com/chubaofs/chubaofs/commit/c03a7bcf15c135629946cd8a1e293787cc3abfa4
* support to modify whether vol supports reading data from a replica https://github.com/chubaofs/chubaofs/commit/99d8bafca4b5a70b4857436c3d9c6dcd388c850c
* introduce read and write iops rate limit https://github.com/chubaofs/chubaofs/commit/66205b324ab9d545a8546757d78ac1b9fd62c4a4
* add metrics https://github.com/chubaofs/chubaofs/commit/929785698d07232729074eca01f636d699e94fde

### Enhancement
* if vol has been marked deleted,data partitions, meta partition information reported by heartbeat will no longer be accepted https://github.com/chubaofs/chubaofs/commit/4e573923f5e5d160b38ca6e4a13d724f45df7f93
* use static ip for meta and data nodes https://github.com/chubaofs/chubaofs/commit/7078944218268571ea00b43deae4ac930301d0b9
* improve debug environment using docker https://github.com/chubaofs/chubaofs/commit/a62899782dfa00da7773d9d2156b596d8cde6bc6
* support custom meta node reserved memory https://github.com/chubaofs/chubaofs/commit/a2b699fc8fdda77a6f6f8d0b4ee299beb9478f11
* data partition and meta partition must have three replicas except reducing replicas to 2 https://github.com/chubaofs/chubaofs/commit/be2d5f54b95f2ab9566d8a172f9ca151a411f5c0
* adjust demo config parameters https://github.com/chubaofs/chubaofs/commit/6ccdd06d691ab53f55ab4514e4f1c15f75525525
*  update grafana dashbord for disk error metric https://github.com/chubaofs/chubaofs/commit/f8760cb99c2d44de977bf83c956ec9c74b3e6877

### Bugfix
* OpFollowerRead if read eof,return error https://github.com/chubaofs/chubaofs/commit/3a28ee72df08a37461c927ec4f8e3baf3608112f
* get follower read option in init https://github.com/chubaofs/chubaofs/commit/d695718a332a580652a5397512abfd1f64c714fe
* stream traverse process never gets triggered in some situation https://github.com/chubaofs/chubaofs/commit/09de5abea9b675d142035000bbe67482016a42dd
* check LoadConfigFile before starting daemon https://github.com/chubaofs/chubaofs/commit/938c1075a327ac903ce33770f07b52f0ac6c415d
* return error from function LoadConfigFile to the caller https://github.com/chubaofs/chubaofs/commit/c8b46cc021550345af3ad0285be92ce81303992d
* extentStorage engine :autoComputeCrc compute crc error https://github.com/chubaofs/chubaofs/commit/1b0305425e7cde77c3a2ccd6dd937cfc3afc928e
* clean up async delete process of metanode https://github.com/chubaofs/chubaofs/commit/7d8382577456e17718a3b235a58a2bbb84d99a84
* set default port to non-system reserved port  https://github.com/chubaofs/chubaofs/commit/506fac1f803912353dcacd658999166f63b6882d

### Refacoring
* leader change not warning on raft https://github.com/chubaofs/chubaofs/commit/4ea5b2510f4845c36a9878138e4974ffd69bb7ad
* remove go module files for now https://github.com/chubaofs/chubaofs/commit/2c9af0a2d62bfba692e3363fdca4d0616a1dc99d
* clean up response of get all inodes info https://github.com/chubaofs/chubaofs/commit/bc0d850ff30c964f03cc12d7cb58f8cf7976a891
* master, datanode and metanode Fix dp or mp offline process https://github.com/chubaofs/chubaofs/commit/ced4b8b92482d910c8ab4494eb829d7af54bf02d
* delete resverd space on datanode config file https://github.com/chubaofs/chubaofs/commit/1f64bf46a8f0c25d02e12cfd197b66fb0bb70e91
* use AddNodeWithPort replace AddNode,and delete AddNode API https://github.com/chubaofs/chubaofs/commit/b56c817c9572d1f78b36a85cb5a71ea156eed61b
* delete resverd space on datanode config file https://github.com/chubaofs/chubaofs/commit/1f64bf46a8f0c25d02e12cfd197b66fb0bb70e91
* refine labels of the disk error metric https://github.com/chubaofs/chubaofs/commit/c4ee0aea946630d32c25bbc9b9aff7dd788c5314
* optimize auto compute crc https://github.com/chubaofs/chubaofs/commit/f9d3ba1d4c031c5b3b0f0162833aceb5606fefba

### Document
* add use cases https://github.com/chubaofs/chubaofs/commit/93a36485f510ceadba2ed819ac1c2f7dafa4a0e2



## Release v1.3.0 - 2019/09/12


### Feature
* introduce writecache mount option. https://github.com/chubaofs/chubaofs/commit/f86199564c6286828845c8c00adbc0e8b8a9ac7b
* introduce keepcache mount option. https://github.com/chubaofs/chubaofs/commit/eadf23331258a49218dee04423d60e8a129208c1
* add admin API for get all meta parititons under vol https://github.com/chubaofs/chubaofs/commit/3cd677b28d24211b258c259e99514f830ddd6be5
* support for truncating raft log. https://github.com/chubaofs/chubaofs/commit/3cd677b28d24211b258c259e99514f830ddd6be5
* dynamiclly reduce the num of replicas for vol. https://github.com/chubaofs/chubaofs/commit/07ddb382a9ad379f7393ae39f44f6ebdbb2dfad0
* the specified number of replica num is supported when creating vol. https://github.com/chubaofs/chubaofs/commit/d0c5e78b08c3d3116570e226e388fd87ac23f11b
* feature: daemonize server https://github.com/chubaofs/chubaofs/commit/ad203059234a80c5d696754b57dd4cf750cb17d2
* support log module change loglevel on line. https://github.com/chubaofs/chubaofs/commit/9c1e104822672bc9965dfedac2eee4fb854b4880

### Enhancement
* extent_store LoadTinyDeleteFileOffset return s.baseTinyDeleteOffset. https://github.com/chubaofs/chubaofs/commit/e7800676fc43132f092ef7b011d9a459b784d2e3
* enable async read by default. https://github.com/chubaofs/chubaofs/commit/5e945554614c0deb4d56daf0f3b1c62c25bf93ec
* improve log message details for clientv2. https://github.com/chubaofs/chubaofs/commit/14e3dd7e02a04babb96d7112860b5a246e5faa45
* compatible with string when get bool config. https://github.com/chubaofs/chubaofs/commit/a485e82f69c0b99ab3581362b3a3002f58d7c211
* add performance tracepoint for clientv2. https://github.com/chubaofs/chubaofs/commit/9c5ee2e51c0a127a92f346f748f88ab65325ad9e
* align out message buffer size with max read size. https://github.com/chubaofs/chubaofs/commit/b0d82fb77f1db52a61f66f92c4cc4ee7e368aa7f
* for splitting meta partition,updating meta partition and creating new meta partition are persisted within a single transaction. https://github.com/chubaofs/chubaofs/commit/5af7a9ebbcd4578e3a09cb4626a6c729e61f9e00
* if metanode used memory is full,then the partition must set to readonly. https://github.com/chubaofs/chubaofs/commit/6d80fdfd4126a9a5ed4bb1cbf91d2baddf8227ce
* set report time to now after creating data partition. https://github.com/chubaofs/chubaofs/commit/01153377431fa3aa604b89a0e7c34c7aac456615
* set writeDeadLineTime to one minute,avoid metanode gc reset conn which snapshot used as much as possible. https://github.com/chubaofs/chubaofs/commit/d4a94ae1ecff9748c4c71b62c63e6ded6c7d1816
* add raft monitor. https://github.com/chubaofs/chubaofs/commit/9e2fce42a711571f903d905590d7cfba0cabf473
* If the creation of a data partition fails, the successfully created replica is deleted. https://github.com/chubaofs/chubaofs/commit/3cd677b28d24211b258c259e99514f830ddd6be5
* If the creation of a meta partition fails, the successfully created replica is deleted. https://github.com/chubaofs/chubaofs/commit/3cd677b28d24211b258c259e99514f830ddd6be5
* add unit test case. https://github.com/chubaofs/chubaofs/commit/3cd677b28d24211b258c259e99514f830ddd6be5
* passing create data partition type to datanode. https://github.com/chubaofs/chubaofs/commit/abffb1712b9d257abd2bb4737f5d596b99e1c115
* if create dp is normal,must start Raft else backend start raft. https://github.com/chubaofs/chubaofs/commit/2701be38d85436650eb1e07c24b944c2847f4b5d
* the tickInterval and electionTick support reading from a configuration file https://github.com/chubaofs/chubaofs/commit/6f0952fbd77a0cfa22df9852afbde02a6a2d86a1

### Bugfix
* fix: add del vol step after ltptest in travis-ci test script. https://github.com/chubaofs/chubaofs/commit/8274cd721ddc98e7ac3bbabd1c148232dcc62694
* clientv2 file handle memory leak. https://github.com/chubaofs/chubaofs/commit/62ecf860d1694cbe0020fd331235704bb905774a
* redirect stderr to an output file in daemon. https://github.com/chubaofs/chubaofs/commit/ead87acb7147594867832ebae5e61cacb30fe2d9
* exclude data partition only when connection refused. https://github.com/chubaofs/chubaofs/commit/a4f27cfd9be309c73c9fde8b45dd25fe821f79b7
* when delete DataParittion,the forwardToLeader mayBe painc. https://github.com/chubaofs/chubaofs/commit/c4b0e9ee77db42d9e190b40cebaffead1be106de
* metanode load error mayme shield. https://github.com/chubaofs/chubaofs/commit/7186621728006ba92de9bbd2a94f2cc59a1d22ec
* truncate raft corrupt data. https://github.com/chubaofs/chubaofs/commit/fceb29fc08d534f03bedf0c0c56b591518f24cac
* when meta node memory usage arrive threshold, split meta partition occurred dead lock. https://github.com/chubaofs/chubaofs/commit/6bb1aaf2460185e95904f1cd5df0ce0e8665f09a
* splitMetaPartition race lock with updateViewCache. https://github.com/chubaofs/chubaofs/commit/d0737f20de832d778b0a9b7b01d69b74c61be696
* after the vol is created and before the heartbeat report, the status of the data partition is set to read only after the check dp operation is performed. https://github.com/chubaofs/chubaofs/commit/81435471a11a0866a6fe958f442e9d642de92779
* when disk error,the raft cannot start on new data server first. https://github.com/chubaofs/chubaofs/commit/74b9f1b737d3fafc8c09c336ed91c8419cceb664
* OpDecommissionDataPartition delete dataPartition on new server. https://github.com/chubaofs/chubaofs/commit/974e508bdbd161ab213acaad3788122dcba4b45d
* datanode may be painc. https://github.com/chubaofs/chubaofs/commit/ac47e9ad5659a0b0b78e697f1f50e4787775616f
* datanode auto compute crc. https://github.com/chubaofs/chubaofs/commit/84707a5f2c80499e6428f20509a610fa8f8efd97
* DataNode: when dataPartition load,if applyId ==0 ,then start Raft. https://github.com/chubaofs/chubaofs/commit/042f939bf3a0b7c7ea4578cde94513157a5f23d5
* the reported data partition usage decreased, and the statistical usage did not decrease accordingly. https://github.com/chubaofs/chubaofs/commit/2386b48ad9c1507ab939ec28042b643e5c55db4d
* docker metanode.cfg add totalMem parameter. https://github.com/chubaofs/chubaofs/commit/353ece00c3ef739ddd6b7f117a68d79e40d7ac27
* Datanode deadlock on deletePartition. https://github.com/chubaofs/chubaofs/commit/4142d38ef9f46a243cc1b18f789a1c1ecfba4af2
* DataNode may be painc. https://github.com/chubaofs/chubaofs/commit/4a3ba7403fac4d91b90d1a0c6f8fa13345152e00
* exclude dir inode in the orphan list. https://github.com/chubaofs/chubaofs/commit/7fff89b3c0efea31cc4d07c9267a1c3e89724888
* evict inode cache after successful deletion. https://github.com/chubaofs/chubaofs/commit/f63e5c657d0d45238309ec28fe3831993f402e20
* The actual reduction in the number of replicas exceeds the expected reduction in the number of replicas. https://github.com/chubaofs/chubaofs/commit/d6b118864b134bdb8cec6ccb1b75b7699d1d8c08
* compatible with old heartbeat mode, old heartbeat mode does not report volname. https://github.com/chubaofs/chubaofs/commit/97da53f06540189bb79e5331b13c2fd097d2b96f
* metanode mistakenly delete empty dir inode. https://github.com/chubaofs/chubaofs/commit/54774529743033bfeaf4e1eb67fb511472ba1337
* treat ddelete not exist error as successful. https://github.com/chubaofs/chubaofs/commit/4f38ebad02d776d3faf4df594e7f17625755c7ab
* fuse directIO read size can exceeds buffer size. https://github.com/chubaofs/chubaofs/commit/b15b78293b5e10f1d43d2c1397d357cc341a8124
* Fix Datanode retain RaftLog https://github.com/chubaofs/chubaofs/commit/4dd740c435d69964f57222ebc55dea665c101915
* Fix: Datanode: when tinyExtentRepair auto repair,it has been https://github.com/chubaofs/chubaofs/commit/b370e8d220d314c03a4f40e518e7f8d101edab73
* Fix: Storage :when write tinyExtent,if offset!=e.datasize,return error https://github.com/chubaofs/chubaofs/commit/73355d2f1f72550873fc32e95acaf6623aa3d6a5
* the buf is not reset and the old data is repeatedly written to the ump warning log, resulting in the overall performance degradation of the cluster https://github.com/chubaofs/chubaofs/commit/d69db16252f8236acb6cb7348d6a625a460a5254
* dataPartitionMap occurred error which is concurrent map iteration and map write https://github.com/chubaofs/chubaofs/commit/3cf7c68015b89858c24152fe890c5e733414908a
* client gets stale file size if streamer is auto evicted. https://github.com/chubaofs/chubaofs/commit/4831443a56a7342e1843db225e29645a1814007c
* update export init for consul register https://github.com/chubaofs/chubaofs/commit/c06e1dbaf7dac5cfdd1faff2d8c215a050944217
* err is shadowed in server main function https://github.com/chubaofs/chubaofs/commit/8ce0f64ad0f19cf8dcfab5cd1df9bf45de7d5017
* if master only create DataPartitionCnt is 10,then cannnot mount https://github.com/chubaofs/chubaofs/commit/5e47ca00d35b8ca0fa62a9e4a52dbac6da9d5ae9
* update export init for consul register https://github.com/chubaofs/chubaofs/commit/dfca18fa1b394fcceee2bf0696737d7197d77ddc
* dataPartition disk error ,not recvoery raft log on new datanode https://github.com/chubaofs/chubaofs/commit/d5febd10da6008dd4fca46d91b4e7d29ebadb9ec
* datanode register hang bug https://github.com/chubaofs/chubaofs/commit/8eaeabf5c1a74b6057a43bf0e856e94823c9ad3a

### Refacoring
* sdk When creating a datapartition, select the datapartition retry strategy. https://github.com/chubaofs/chubaofs/commit/226578551137d9655bf23aa3c8220c4fe0fc8957
* Refactoring SDK: when write datapartition num greater 10 ,then trust master. https://github.com/chubaofs/chubaofs/commit/adfe95deef05b10d812736e613f88fbceadb260e
* when disk error,the datapartition recover only recover avali data on tinyExtent. https://github.com/chubaofs/chubaofs/commit/1910bb9d8112acdaa87456f54abae648f817b60d
* Decommission Meta or Data Partition must sync response to master. https://github.com/chubaofs/chubaofs/commit/b4350ade23b2d81bb5840199fcfe46d33a8cff5a
* synchronized decommission the data partition. https://github.com/chubaofs/chubaofs/commit/501b36a1fde217116f2d9ef37ae1a37a6812798c
* datanode api /partition add raftStatus. https://github.com/chubaofs/chubaofs/commit/2a2fa5d32de2ab42b7ce86da73f1171df5bc372e
* StartRaftLoggingSchedule not use goroutine. https://github.com/chubaofs/chubaofs/commit/e65d12d5ddfdf8bad6a711412dd40c62c0b110e2
* metanode must config totalMem. https://github.com/chubaofs/chubaofs/commit/9e9ae6a74cc588aa5d58038d4b8582d71a8c5c97
* sync tinyDeleteExtent time change to 1 days. https://github.com/chubaofs/chubaofs/commit/bf4411dbd52ce03c470aab57f6b321a8de24080c
* change partitionId varliable to partitionID. https://github.com/chubaofs/chubaofs/commit/c366fcde54ef8e07226b7d118dbd40379e317749
* Datanode delete SnapshotFile Pool. https://github.com/chubaofs/chubaofs/commit/b376e01732f97816bd18a6dc43f96603605e4501
* exporter add ump. https://github.com/chubaofs/chubaofs/commit/669df1f3d7632d161d87a1f9d9393def6702eab8
* DataNode: compatible old dataPartition Meta info. https://github.com/chubaofs/chubaofs/commit/bcdfce265683109ef59250d095a71b9b1018ba31
* add ltptest log. https://github.com/chubaofs/chubaofs/commit/67ce466ce1aa63559820453a01ecb1281fad3bd6
* docker metanode config.json change totalMem to 6GB. https://github.com/chubaofs/chubaofs/commit/af066310afc265343346521f4db0c737d28a40d6
* datanode create dataPartition select disk function change. https://github.com/chubaofs/chubaofs/commit/07617e7f6f2903e326fba0de944f3ad1cd012eff
* datanode start must start StartRaftLoggingSchedule func. https://github.com/chubaofs/chubaofs/commit/78bb09f858a7dec007537172afb1b6d31c4dca48
* Master create vol min default DataPartition set to 10. https://github.com/chubaofs/chubaofs/commit/f9062c618d0ad0a3046411348902326c90895739
* docker: run ltptest print errorinfo. https://github.com/chubaofs/chubaofs/commit/eee4553dfee29ec810f959730b224e0bb7935437
* when load dp,if dp status is normal,then start raft ,else wait snapshot has recover. https://github.com/chubaofs/chubaofs/commit/cc68ec3266169bb1bf96f3f254dd55dcd7f93fc7
* Datanode delete unused func. https://github.com/chubaofs/chubaofs/commit/f3297f91ec4482b3c6a6d03671319a15fc39f0df
* if not config warnLogDir,then donnot write umplog to disk. https://github.com/chubaofs/chubaofs/commit/6a9dbc369b670b9c2d128cdd214dd56a528202f2
* DataNode: if not config raftDir,then not start server. https://github.com/chubaofs/chubaofs/commit/480c44af49747dcb7ac37de99f23682391367e06
* add log on metanode delete extent. https://github.com/chubaofs/chubaofs/commit/dde79c0636a9907b4314613d15dbe7c61169f1bb
* Doc :delete warnLogDir config. https://github.com/chubaofs/chubaofs/commit/6f05a78e23af638fd85015c09055e616fc6d0b4e
* keep mount point in error stataus when client is killed https://github.com/chubaofs/chubaofs/commit/df1a4d9cba3d656c14686164cb0e57380922a77d
* log checkroration checkTime change to 1 second https://github.com/chubaofs/chubaofs/commit/f732ade2bc70e5207163193f0ea0923b7621df39
* metanode change deleteDentry or deleteInode api to log.LogDebugf https://github.com/chubaofs/chubaofs/commit/149d661e63a2ce3393ad97be9e8ae02d2b2d141a
* Increase the judgment condition of disk error,add syscall.EROFS https://github.com/chubaofs/chubaofs/commit/06a3ab3be294f198f2a7e37b6c254798b939bce2
* sync code from git.jd.com/chubaofs/chubaofs https://github.com/chubaofs/chubaofs/commit/2b2461598fdebe0abac152218cbbd83d48c19bf2


## Release v1.2.1 - 2019/07/19

### Enhancement
* when datanode or metanode start,must compare partition with master and gofmt project. https://github.com/chubaofs/chubaofs/commit/02e1989065cd89691af0991b376527ef3a029a34
* datanode add backend check disk status func. https://github.com/chubaofs/chubaofs/commit/1b5ac0e674e37602410b2762254d093b62eb4cdf


### Bugfix
* update rocksdb build depends. https://github.com/chubaofs/chubaofs/commit/f665f2f33a63b55835a759c207fc0d7b00ca05a1
* metanode panic when deleting meta partition. https://github.com/chubaofs/chubaofs/commit/c228ced18d810ef527c8d7a36e278a826ed045a3
* when raft member become leader,must apply from appliyID to commitID. https://github.com/chubaofs/chubaofs/commit/f5c2d3211717bb14664b32a4f6bbcaba7b1e7d18


## Release v1.2.0 - 2019/07/16

### Feature

* clientv2: add dentry cache. https://github.com/chubaofs/chubaofs/commit/add53510fd96b406572c99f781660aae29736981
* vendor: introduce jacobsa daemonize package. https://github.com/chubaofs/chubaofs/commit/97a1360de29fbe3007a32b80d601ea966d5140a2
* client: start client as a daemon. https://github.com/chubaofs/chubaofs/commit/f1efb1c0a7f41bb4673cf350fb636781c6321ca4
* clientv2: daemonize client.https://github.com/chubaofs/chubaofs/commit/2055e73092c34f86dc0982612063c3a33ceb0ff7

### Enhancement:

* Update docker helper script. https://github.com/chubaofs/chubaofs/commit/5e00b4e9021a197f581c3344059aafbf549da080
* data sdk: use ip address instead of partition id to rule out unavailable data partitions. https://github.com/chubaofs/chubaofs/commit/568b40cd7d9169fe6d72882ef725971dce2a46fd
* add monitor add prometheus, grafana in docker helper script.https://github.com/chubaofs/chubaofs/commit/354a66cf660f4eaef1049045d7daa3b6b18565d7
* add GOPATH check in build.sh. https://github.com/chubaofs/chubaofs/commit/7b314c5f031aec16d8f5fcda5adf9ec089e2c4fb
* Integrated rocksdb compilation. https://github.com/chubaofs/chubaofs/commit/5dbc0b2d8c906166bb2b592c4dd52206d096742c
* update makefile. https://github.com/chubaofs/chubaofs/commit/2ec8603e7300e8eb14778c5add90f5db795757ab
* docs: start client as a daemon. https://github.com/chubaofs/chubaofs/commit/714209a76dfed3c020df6485185f199ccb45d3a1
* update docs: add make build in readme and docs. https://github.com/chubaofs/chubaofs/commit/3c6ccae977ab4c2054fb49bb5fa65d4d22fbb783
* use one thread to send and recive from follower on primary backup replication protocol .https://github.com/chubaofs/chubaofs/commit/1009af799dc91a089adf70fca97ea3b473f2eaf6
* change random write raft serialize not use json. https://github.com/chubaofs/chubaofs/commit/6144a94c280cee34eae42bd9dc04d009969bea33


### Bugfix:

* fix: when datanode is killed, maxBaseExtentId is corrupted.https://github.com/chubaofs/chubaofs/commit/e8552fe1c194acc7d58db969dc8673c1a150bbc5
* Once vol is created successfully, the state of the data partition it has should be writable. https://github.com/chubaofs/chubaofs/commit/2d954a376fb500cc6dc6d32f3191cfe2541cd3a6
* metanode: fix create dentry exist error. https://github.com/chubaofs/chubaofs/commit/f55846af3c2522b4773e5e2dd2c2871b88f80267
* add privileged for docker-compose server node. https://github.com/chubaofs/chubaofs/commit/718e26308c2ee19ca98c52abdef6c92718ebbbd6
* the retain space field in config file is not allowed to be larger than the default value. https://github.com/chubaofs/chubaofs/commit/70a42d35369f6fb3fb5050539eee063e128cdb52
* fix: when data node gets a disk error, the disk status does not changed. https://github.com/chubaofs/chubaofs/commit/2b056234193351fad1dbedde2ea11626c1d6e97a
* fix: build error. https://github.com/chubaofs/chubaofs/commit/f3eead28cbb3640fb8a60abea16422f8dd4b54e9
* fix build with lua bug. https://github.com/chubaofs/chubaofs/commit/6207f7e7a8ddc46691b9058cad0662cb4c27f84d
* Check whether vol is valid. If there is no meta partition with start being. https://github.com/chubaofs/chubaofs/commit/dd001daf37c6517685eb7515bc46c4ed85a0d7a0
* build: update build.sh for user permission. https://github.com/chubaofs/chubaofs/commit/7532a51ed150e95c69cae8b2b890f545608264f9
* clientv2: fix to get the latest file size. https://github.com/chubaofs/chubaofs/commit/fba924fb1ef0fb40a8cc25956812c222b7309fc2
* when create dp failed,then Check all dp host if the host is alive. https://github.com/chubaofs/chubaofs/commit/7e0eed1b700e8b6c9f8a2a5b43a9432b52198219
* fix bug: when read tinyDeleteFile return eof error. https://github.com/chubaofs/chubaofs/commit/b7ee83f908024e042a296faf40090e218e462e4d
* build: fix multi users build bug. https://github.com/chubaofs/chubaofs/commit/a885777b9da97957cc1eddfee102be3707763b64https://github.com/chubaofs/chubaofs/commit/a885777b9da97957cc1eddfee102be3707763b64
* build: fix no root user permission bug. https://github.com/chubaofs/chubaofs/commit/abe6e1fb5a4193183729a2d4d06e395d51a567e6
* init replicas info from create data partition request. https://github.com/chubaofs/chubaofs/commit/63b6d11d721f8d03b2df07e5cbba10ffa83fc4a2


#### Change/Refactoring

* add cfs-base dockerfile. https://github.com/chubaofs/chubaofs/commit/7b240f21ba6a0037484dd5591bfce0b8f88f2844
* Specification code and metanode config file. https://github.com/chubaofs/chubaofs/commit/03f79e737099cd3f6e782f6abbb781b8ab871aa7
* add go mod. https://github.com/chubaofs/chubaofs/commit/5735625b3c60bcba72403cd80ccf0238552f0db8
* docs: update according to recent changes. https://github.com/chubaofs/chubaofs/commit/65e0aa355568424b5fa6137f138be6e28ac9c647
* if warnLogDir is not specified in the config files, server or client will return an error. https://github.com/chubaofs/chubaofs/commit/236e454c376e8edaa8216f1b384a8afd6268037c
* rename RestSize to ReservedSize. https://github.com/chubaofs/chubaofs/commit/f018e4b20f0f4828a39fe86996940ccfd1779932
* print more detailed error information when server starts failed. https://github.com/chubaofs/chubaofs/commit/0c1b9532f7b50bf7b0d6a5943996bf0f1a11badf
* remove docker client security opt. https://github.com/chubaofs/chubaofs/commit/d346a7d83548aec83590f94e1a788eeed6627455
* add disk config detailed description. https://github.com/chubaofs/chubaofs/commit/3142c54aa229451de02ae8059c248a9f89e6c835
* by default create 10 data partitions and 3 meta partitions when creating volume. https://github.com/chubaofs/chubaofs/commit/06045cd2c8960fc4b23126e8c42b52557d3525c6
* doc: add clarification that resource manager is AKA master. https://github.com/chubaofs/chubaofs/commit/02781071b2dc7ceac751d746113b3cfb205d75ce
* normalize exporter cluster name. https://github.com/chubaofs/chubaofs/commit/8543c15380a8628962e54e1a3eff15bc28f425bf
* set the value of rack for data node to default. https://github.com/chubaofs/chubaofs/commit/39000f6d3240bed35de23f459e7c57ecef9a7c76
* refine decommission disk. https://github.com/chubaofs/chubaofs/commit/6b6ce51f62389fcfc6d078768fa6bdf621c2220f
* change document about metanode config. https://github.com/chubaofs/chubaofs/commit/f142311c8d9a5d7c7c66fcdbec2323d03f40e72d
* doc: update docker helper section. https://github.com/chubaofs/chubaofs/commit/851f90c85386af3b603480d009cdb708794120ec
* Update quick-start-guide.rst. https://github.com/chubaofs/chubaofs/commit/ac3f176ea3e8a2157d66637abdd8d552bcafcb10
* change dataPartition IntervalToUpdateReplica to 600 seconds. https://github.com/chubaofs/chubaofs/commit/4c0521d7466950dcfdc33dad3e31c50e229c3d26
* add third-party directory. https://github.com/chubaofs/chubaofs/commit/8cb94afd07afdcfedb58227275f69dad13278283
* write data partition decommission url message to log. https://github.com/chubaofs/chubaofs/commit/2520f73b2b794b1206f3401f8316a736121d487e
* go fmt project. https://github.com/chubaofs/chubaofs/commit/e841bb8e4b01813ba3ba9f29ca9e6cdf4f501bd6
* data sdk: check amount of writable data partitions when mount. https://github.com/chubaofs/chubaofs/commit/9f55ccd37fafe2f989e75341117711ecb3a7dc88
* delete third-party directory. https://github.com/chubaofs/chubaofs/commit/6d7838a3ba8924c5d41661ff56737ec0d2ab4636
* update docs: remove server and client. https://github.com/chubaofs/chubaofs/commit/ff5e4c9951422bd5d4176efd2f004b10ef0f138b
* pass the hosts to data node when creating the data partition. https://github.com/chubaofs/chubaofs/commit/8cf3447de95224744eece033aa96d81dc2755727
* warn log message unicode encoding is converted to utf8. https://github.com/chubaofs/chubaofs/commit/1bbdc2bef584051cb71c09423f0e81aa8fb94f4c
* Ignore generated build files. https://github.com/chubaofs/chubaofs/commit/e243fa79a8306e616b49b5cc87ebcaf120e0d703
* build: update build.sh and Makefile. https://github.com/chubaofs/chubaofs/commit/b6004b225f32df326b3dae7a664f8ff56c91bf81
* change packet attr func and ReadTinyDelete impl log. https://github.com/chubaofs/chubaofs/commit/e37c7d7ee0575d67f7758802a9f4e08c4bcfdecd
* add log when read tinyDeleteFile failed. https://github.com/chubaofs/chubaofs/commit/fc1e8f3b9fc55774267aea615c8c2904356cb0dc
* add log when streamRead and ExtentRepairRead. https://github.com/chubaofs/chubaofs/commit/dfa1ae5eb8a13df6b8b1dd9b144fa1018f08b71a
* remove unused source code. https://github.com/chubaofs/chubaofs/commit/9a91850c4cfa04cb321372a1008c0dfff2c0520a
* build: update build.sh. https://github.com/chubaofs/chubaofs/commit/1f65750588ada80c1908d9b85fb7de2bee901133
* Update README.md. https://github.com/chubaofs/chubaofs/commit/bf8cd97f8979083b4832cabdb44db025fe39c22b
* build: remove client unused depends. https://github.com/chubaofs/chubaofs/commit/f613e804cc2b664b1f154fed66829a0fea9b933d


## Release v1.1.1 - 2019/06/11

### Feature

* client: enable support to fifo and socket file types. https://github.com/chubaofs/chubaofs/pull/80/commits/a1b118423334c075b0fbdc0b059b7225e8c2173f
* clientv2: use jacobsa fuse package instead of bazil. https://github.com/chubaofs/chubaofs/pull/68
* docker: introduce docker compose to create a local testing ChubaoFS cluster. https://github.com/chubaofs/chubaofs/pull/79


### Bugfix

* meta: update atime in inode get operation. [metanode] https://github.com/chubaofs/chubaofs/pull/80/commits/cf76479d251ee7214d0d27625fab95498ee1ae0c
* fix: potential panic if send returns nil resp with no error. [client] https://github.com/chubaofs/chubaofs/pull/80/commits/22f3623d5e24a84c7d1ec49fcb72be375d0d4b92
* fix: raft election takes a long time and timeout; issue a panic if raft server is down. [raft] https://github.com/chubaofs/chubaofs/pull/80/commits/26a1f2f826d5ddd3bb6803ec462f928d12597bdd
* fix: potential deadlock if applyHandler. [master] https://github.com/chubaofs/chubaofs/pull/80/commits/cb1eb6ebfcfedc4d1f8bd97e6b7d776bc8ecf4f4
* fix: put vol to cache after it is persistent. [master] https://github.com/chubaofs/chubaofs/pull/80/commits/f31c5d8e260a878b5bfe9d09d8ce196c9aa2abc8
* fix: partition is nil when apply remove raft node. [datanode] https://github.com/chubaofs/chubaofs/pull/80/commits/971cc4b9105af77b4ada52159125225e713754c0
* fix: metanode painc. [metanode] https://github.com/chubaofs/chubaofs/pull/80/commits/a3d8b1f19b2c3f52af561e46c0c8b3eea15472fa
* fix: panic when pprof does not start. https://github.com/chubaofs/chubaofs/pull/80/commits/77a0efe9aa35d68c039a05dc2780d6902ec08d53

### Enhancement

* sdk: retry if mount failed in case master is unavailable temporarily. https://github.com/chubaofs/chubaofs/pull/80/commits/d20732dbbc343dffe1893f3766305322ae8d05de
* build: add verbose build info. https://github.com/chubaofs/chubaofs/pull/80/commits/e5316f98429ed0b680cda4e4af994774c59ac8bd
* master: introduce data partition over-provision. https://github.com/chubaofs/chubaofs/pull/80/commits/642d1f15696b42d8470392c4dab40e4e3b6d3d8a
* master: reserve writable data partition amount according to capacity instead of a const. https://github.com/chubaofs/chubaofs/pull/80/commits/06186f0b62df534fae3d2f817ccfb61dba921c01
* monitor: Use UMP performance monitor if exporter is not enabled. https://github.com/chubaofs/chubaofs/pull/80/commits/ddf608f7e1705e79ba1c07285f4e61dbdf86189d



## Release v1.1.0 - 2019/05/07

### Change / Refactoring

* Rename the repository from cfs to chubaofs.
* Use own errors module instead of juju errors due to license incompatibility.
* Metanode: Change not raft leader error to tryOtherAddr error.

### Bugfix

* Master: Partition recovered but the status not changed.
* Datanode: Report to client with proto.OptryAgain resultcode when datapartition does not exsit.
* Raft: A member must apply playback from old apply id to commit id after elected as leader.
* Metanode: generate identical inode number under extreme conditions. https://github.com/chubaofs/chubaofs/commit/45b6daa88911eaaebabe299b05fad565761f97ed

### Enhancement

* Master: Add ump warn packet.
* Master: Remove redundant calling of loadMetaData method.
* Master: Reload meta data after leader changed.
* Master: make dataPartition disk Path persistent. https://github.com/chubaofs/chubaofs/commit/6dce99a755d0e32828296a002c6aa50ebfe07c63
* Master: Volume creation supports specifying the amount of meta partitions. https://github.com/chubaofs/chubaofs/commit/2e7bbf2dde555496f9476eb3a9e0ab8200d44d8f
* Metanode: Add totalMem in configFile.
* Datanode: Change default disk reserved space.
* Datanode: Add volname in heartbeat report.
* Raft: Use raft.ErrNotLeader instead of ErrNotLeader.
* Client: Create a dummy node instance if inode does not exist.
* Client: Add UMP monitor alarms for read/write/fsync errors.
* Client: Suppress some error messages. https://github.com/chubaofs/chubaofs/commit/1f6062000d2049a875c3b16a5cc65d61cad1b367
* Log: Automatically create subdirectory under the log directory.

## Release v1.0.0 - 2019/04/02

The initial release version.
